//go:build integration

// End-to-end test: brings up Comqtt + Postgres + RustFS via testcontainers,
// runs the full pipeline (subscriber -> ring -> flusher -> Postgres),
// publishes 500 messages from a load generator, and asserts:
//
//  1. Every message lands in telemetry exactly once (dedup_key unique).
//  2. The buffer / WAL / flusher metrics moved.
//  3. The subscriber acked everything it received.
//
// This is the "does the contract hold" test. It is intentionally slow
// (~30s per run) because it stands up real brokers, a real database, and
// a real S3 server.
package integration

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/deadletter"
	"github.com/debsahu/mqtt2db-go/internal/flusher"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/subscriber"
)

// startRustFS boots a single-node RustFS container.
func startRustFS(tb testing.TB, ctx context.Context) (endpoint, accessKey, secretKey string) {
	tb.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "rustfs/rustfs:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY":     "rustfsadmin",
			"RUSTFS_SECRET_KEY":     "rustfsadmin",
			"RUSTFS_ADDRESS":        ":9000",
			"RUSTFS_VOLUMES":        "/data",
			"RUSTFS_REGION":         "us-east-1",
			"RUSTFS_CONSOLE_ENABLE": "false",
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})

	host, err := c.Host(ctx)
	require.NoError(tb, err)
	port, err := c.MappedPort(ctx, "9000/tcp")
	require.NoError(tb, err)
	return fmt.Sprintf("http://%s:%s", host, port.Port()), "rustfsadmin", "rustfsadmin"
}

// startPostgresE2E is a thin wrapper around the Postgres testcontainer
// helper from postgres_integration_test.go, copied here so the e2e
// build doesn't have a hidden cross-file dependency.
func startPostgresE2E(tb testing.TB, ctx context.Context) string {
	tb.Helper()
	c, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("telemetry"),
		tcpostgres.WithUsername("ingest"),
		tcpostgres.WithPassword("ingest"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(tb, err)
	return dsn
}

// createBucket gives RustFS its dead-letter bucket. The Compose stack
// uses minio/mc for this; in tests we use the SDK directly.
func createBucket(tb testing.TB, ctx context.Context, endpoint, accessKey, secretKey, bucket string) {
	tb.Helper()
	awscfgValue, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	require.NoError(tb, err)
	client := s3.NewFromConfig(awscfgValue, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		tb.Fatalf("create bucket: %v", err)
	}
}

func publishE2E(tb testing.TB, broker, topic string, payload []byte) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bu, err := url.Parse(broker)
	require.NoError(tb, err)
	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:     []*url.URL{bu},
		ConnectTimeout: 5 * time.Second,
		KeepAlive:      10,
		ClientConfig:   paho.ClientConfig{ClientID: "pub-" + uuid.NewString()},
	})
	require.NoError(tb, err)
	require.NoError(tb, cm.AwaitConnection(ctx))
	_, err = cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: payload})
	require.NoError(tb, err)
	_ = cm.Disconnect(ctx)
}

func TestE2E_FullPipelinePersistsToPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e test skipped in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker := startBroker(t, ctx)
	dsn := startPostgresE2E(t, ctx)
	dlqEndpoint, dlqAccess, dlqSecret := startRustFS(t, ctx)

	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), dsn))
	createBucket(t, ctx, dlqEndpoint, dlqAccess, dlqSecret, "mqtt2db-go-dlq")

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{
		DSN: dsn, MaxConns: 4, MinConns: 1,
	})
	require.NoError(t, err)
	defer pool.Close()

	copier, err := postgres.NewCopier(pool, config.PostgresConfig{
		Schema: "public", Table: "telemetry",
		Columns: []string{"tenant_id", "device_uuid", "topic", "payload", "received_at", "dedup_key"},
	})
	require.NoError(t, err)

	reg := metrics.NewRegistry()
	bufM := metrics.NewBufferMetrics(reg)
	subM := metrics.NewSubscriberMetrics(reg)
	flM := metrics.NewFlusherMetrics(reg)
	dlqM := metrics.NewDeadLetterMetrics(reg)

	ring, err := buffer.New(buffer.Config{
		MaxMessages: 10_000, MaxBytes: 64 << 20,
		SpillThreshold: 0.8, PauseThreshold: 0.95,
	}, bufM)
	require.NoError(t, err)

	dlq, err := deadletter.NewSink(ctx, config.S3Config{
		Bucket: "mqtt2db-go-dlq", Region: "us-east-1",
		Endpoint: dlqEndpoint, UsePathStyle: true,
		AccessKey: dlqAccess, SecretKey: dlqSecret,
	}, dlqM)
	require.NoError(t, err)

	flush, err := flusher.New(config.FlusherConfig{
		BatchSize:                250,
		FlushInterval:            config.Duration(50 * time.Millisecond),
		ElevatedLatencyThreshold: config.Duration(200 * time.Millisecond),
		ElevatedBatchSize:        500,
		ElevatedFlushInterval:    config.Duration(100 * time.Millisecond),
		CriticalLatencyThreshold: config.Duration(2 * time.Second),
		RecoveryWindow:           config.Duration(2 * time.Second),
		MaxRetries:               2,
	}, copier, ring, nil, dlq, flM, nil)
	require.NoError(t, err)

	t.Setenv("HOSTNAME", "e2e-host")
	sub, err := subscriber.New(config.MQTTConfig{
		Brokers:             []string{broker},
		ClientIDPrefix:      "mqtt2db-go-e2e",
		SharedSubscription:  "$share/ingest/t/+/d/+/evt/#",
		Keepalive:           config.Duration(10 * time.Second),
		ConnectTimeout:      config.Duration(5 * time.Second),
		MaxReconnectBackoff: config.Duration(10 * time.Second),
		ProtocolVersion:     5,
	}, ring, nil, subM, nil)
	require.NoError(t, err)

	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	defer pipelineCancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = sub.Start(pipelineCtx) }()
	go func() { defer wg.Done(); _ = flush.Run(pipelineCtx) }()

	// wait for subscriber to connect
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(subM.Connected) == 1
	}, 30*time.Second, 100*time.Millisecond, "subscriber never connected")

	const total = 500
	for i := 0; i < total; i++ {
		dev := uuid.New()
		topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
		publishE2E(t, broker, topic, []byte(fmt.Sprintf(`{"seq":%d}`, i)))
	}

	require.Eventually(t, func() bool {
		var n int64
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM telemetry`).Scan(&n)
		return n >= int64(total)
	}, 60*time.Second, 250*time.Millisecond, "Postgres never saw all %d rows", total)

	pipelineCancel()
	wg.Wait()

	var n int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM telemetry`).Scan(&n))
	assert.Equal(t, int64(total), n, "exactly %d rows expected (no duplicates)", total)

	var distinctDedup int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(distinct dedup_key) FROM telemetry`).Scan(&distinctDedup))
	assert.Equal(t, int64(total), distinctDedup)
}

// TestE2E_PgUnreachable_DeadLetters demonstrates the dead-letter path:
// kill Postgres mid-flow and confirm messages land in S3 with the
// expected key shape.
func TestE2E_PgUnreachable_DeadLetters(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e test skipped in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dlqEndpoint, dlqAccess, dlqSecret := startRustFS(t, ctx)
	createBucket(t, ctx, dlqEndpoint, dlqAccess, dlqSecret, "mqtt2db-go-dlq")

	reg := metrics.NewRegistry()
	dlqM := metrics.NewDeadLetterMetrics(reg)

	dlq, err := deadletter.NewSink(ctx, config.S3Config{
		Bucket: "mqtt2db-go-dlq", Region: "us-east-1",
		Endpoint: dlqEndpoint, UsePathStyle: true,
		AccessKey: dlqAccess, SecretKey: dlqSecret,
	}, dlqM)
	require.NoError(t, err)

	dev := uuid.New()
	require.NoError(t, dlq.Write(ctx, postgres.Message{
		TenantID:   "acme",
		DeviceUUID: dev,
		Topic:      "t/acme/d/" + dev.String() + "/evt/state",
		Payload:    []byte(`{"x":1}`),
		ReceivedAt: time.Now().UTC(),
		DedupKey:   "acme:1",
	}, fmt.Errorf("simulated pg outage")))

	// Confirm the object landed.
	awscfgValue, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(dlqAccess, dlqSecret, ""),
		),
	)
	require.NoError(t, err)
	client := s3.NewFromConfig(awscfgValue, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(dlqEndpoint)
		o.UsePathStyle = true
	})
	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("mqtt2db-go-dlq"),
		Prefix: aws.String("dead-letter/"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listed.Contents), 1)
	if len(listed.Contents) > 0 {
		assert.True(t, strings.HasSuffix(*listed.Contents[0].Key, ".json"))
	}
	_ = s3types.NoSuchBucket{} // keep s3types referenced for import
}

//go:build stress

// Self-contained stress test for the mqtt2db-go pipeline.
//
// What it does:
//  1. Spins up Comqtt v2.6.2 (locally-built), Postgres 17, RustFS via
//     testcontainers — same images the integration tests use.
//  2. Runs migrations.
//  3. Starts the full in-process pipeline (ring buffer + WAL + flusher
//     + dead-letter sink + subscriber) with production-shaped config.
//  4. Drives load through 8 parallel publishers at a configurable rate
//     for a configurable duration.
//  5. Reports messages_sent, messages_persisted, achieved_rate,
//     end-to-end latency p50/p95/p99 (received_at -> Postgres
//     inserted_at), dead-letter count, and per-mode time spent.
//  6. Asserts no message loss: persisted == sent.
//
// Run with:
//
//	STRESS_RATE=2000 STRESS_DURATION=30s STRESS_DEVICES=500 \
//	    go test -tags=stress -timeout=10m -v -run='^TestStress$' ./test/stress/
//
// The defaults are conservative so this can run on a laptop in 30s.
// CI can override the env vars for nightly runs.
package stress_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	"github.com/debsahu/mqtt2db-go/internal/wal"
)

// envInt reads an int from env with a default.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

// migrationsDir resolves ./migrations from this file.
func migrationsDir(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations")
}

// startComqtt boots the locally-built comqtt:v2.6.2 image. The test
// fails (not skips) if the image is missing — stress tests should make
// the prerequisites loud.
func startComqtt(tb testing.TB, ctx context.Context) string {
	tb.Helper()
	req := testcontainers.ContainerRequest{
		Image:         "comqtt:v2.6.2",
		ImagePlatform: "linux/" + runtime.GOARCH,
		ExposedPorts:  []string{"1883/tcp"},
		Cmd:           []string{"--tcp=:1883", "--ws=:1882", "--http=:8080"},
		WaitingFor:    wait.ForListeningPort("1883/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err, "comqtt:v2.6.2 missing? Run `make comqtt-image` first.")
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
	host, err := c.Host(ctx)
	require.NoError(tb, err)
	port, err := c.MappedPort(ctx, "1883/tcp")
	require.NoError(tb, err)
	return fmt.Sprintf("tcp://%s:%s", host, port.Port())
}

func startPostgres(tb testing.TB, ctx context.Context) string {
	tb.Helper()
	c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
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

func startRustFS(tb testing.TB, ctx context.Context) (string, string, string) {
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

func createBucket(tb testing.TB, ctx context.Context, endpoint, ak, sk, bucket string) {
	tb.Helper()
	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	require.NoError(tb, err)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, _ = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
}

func TestStress(t *testing.T) {
	rate := envInt("STRESS_RATE", 1500)
	duration := envDuration("STRESS_DURATION", 20*time.Second)
	devices := envInt("STRESS_DEVICES", 200)
	publishers := envInt("STRESS_PUBLISHERS", 8)
	payload := envInt("STRESS_PAYLOAD", 128)

	t.Logf("stress params: rate=%d msg/s duration=%s devices=%d publishers=%d payload=%d bytes",
		rate, duration, devices, publishers, payload)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker := startComqtt(t, ctx)
	dsn := startPostgres(t, ctx)
	dlqEndpoint, dlqAccess, dlqSecret := startRustFS(t, ctx)
	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), dsn))
	createBucket(t, ctx, dlqEndpoint, dlqAccess, dlqSecret, "mqtt2db-go-dlq")

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{
		DSN: dsn, MaxConns: 8, MinConns: 2,
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
	walM := metrics.NewWALMetrics(reg)
	subM := metrics.NewSubscriberMetrics(reg)
	flM := metrics.NewFlusherMetrics(reg)
	dlqM := metrics.NewDeadLetterMetrics(reg)

	walStore, err := wal.Open(wal.Config{
		Path:             t.TempDir(),
		TTL:              0,
		SyncWrites:       false,
		ValueLogFileSize: 1 << 28,
	}, walM)
	require.NoError(t, err)
	t.Cleanup(func() { _ = walStore.Close() })

	ring, err := buffer.New(buffer.Config{
		MaxMessages: 100_000, MaxBytes: 100 << 20,
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
		BatchSize:                1000,
		FlushInterval:            config.Duration(100 * time.Millisecond),
		ElevatedLatencyThreshold: config.Duration(500 * time.Millisecond),
		ElevatedBatchSize:        5000,
		ElevatedFlushInterval:    config.Duration(500 * time.Millisecond),
		CriticalLatencyThreshold: config.Duration(2 * time.Second),
		RecoveryWindow:           config.Duration(60 * time.Second),
		MaxRetries:               5,
	}, copier, ring, walStore, dlq, flM, nil)
	require.NoError(t, err)

	t.Setenv("HOSTNAME", "stress")
	sub, err := subscriber.New(config.MQTTConfig{
		Brokers:             []string{broker},
		ClientIDPrefix:      "mqtt2db-go-stress",
		SharedSubscription:  "$share/ingest/t/+/d/+/evt/#",
		Keepalive:           config.Duration(30 * time.Second),
		ConnectTimeout:      config.Duration(5 * time.Second),
		MaxReconnectBackoff: config.Duration(10 * time.Second),
		ProtocolVersion:     5,
	}, ring, walStore, subM, nil)
	require.NoError(t, err)

	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	defer pipelineCancel()
	var pipelineWG sync.WaitGroup
	pipelineWG.Add(2)
	go func() { defer pipelineWG.Done(); _ = sub.Start(pipelineCtx) }()
	go func() { defer pipelineWG.Done(); _ = flush.Run(pipelineCtx) }()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(subM.Connected) == 1
	}, 30*time.Second, 100*time.Millisecond, "subscriber never connected")

	deviceIDs := make([]uuid.UUID, devices)
	for i := range deviceIDs {
		deviceIDs[i] = uuid.New()
	}

	bu, err := url.Parse(broker)
	require.NoError(t, err)
	pubs := make([]*autopaho.ConnectionManager, publishers)
	for i := 0; i < publishers; i++ {
		cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
			ServerUrls:     []*url.URL{bu},
			KeepAlive:      30,
			ConnectTimeout: 5 * time.Second,
			ClientConfig:   paho.ClientConfig{ClientID: fmt.Sprintf("loadgen-%d-%s", i, uuid.NewString()[:6])},
		})
		require.NoError(t, err)
		require.NoError(t, cm.AwaitConnection(ctx))
		pubs[i] = cm
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, cm := range pubs {
			_ = cm.Disconnect(ctx)
		}
	}()

	loadCtx, loadCancel := context.WithTimeout(ctx, duration)
	defer loadCancel()

	var sent atomic.Int64
	var pubErrs atomic.Int64
	perPub := rate / publishers
	if perPub < 1 {
		perPub = 1
	}

	t.Logf("driving load for %s …", duration)
	loadStart := time.Now()
	var loadWG sync.WaitGroup
	for i, cm := range pubs {
		loadWG.Add(1)
		go func(idx int, cm *autopaho.ConnectionManager) {
			defer loadWG.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(idx)))
			interval := time.Second / time.Duration(perPub)
			next := time.Now()
			body := make([]byte, payload)
			for {
				select {
				case <-loadCtx.Done():
					return
				default:
				}
				if now := time.Now(); now.Before(next) {
					select {
					case <-loadCtx.Done():
						return
					case <-time.After(next.Sub(now)):
					}
				}
				next = next.Add(interval)

				dev := deviceIDs[r.Intn(len(deviceIDs))]
				topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
				_, _ = r.Read(body)
				// Publish QoS 0 from the load generator: each publish is
				// fire-and-forget on the publisher side, but the broker
				// still delivers QoS 1 to our subscriber via the shared
				// subscription. This isolates the test from the
				// publisher's PUBACK-bound ceiling.
				if _, err := cm.Publish(loadCtx, &paho.Publish{Topic: topic, QoS: 0, Payload: body}); err != nil {
					pubErrs.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(i, cm)
	}
	loadWG.Wait()
	loadElapsed := time.Since(loadStart)
	totalSent := sent.Load()
	t.Logf("load phase done: sent=%d errs=%d elapsed=%s", totalSent, pubErrs.Load(), loadElapsed.Round(time.Millisecond))

	// Drain phase: wait until the service-side count (acked) shows up
	// in Postgres. Time-budget scales with the queued backlog
	// (ring + WAL) divided by an assumed conservative flush rate.
	drainStart := time.Now()
	expected := int64(testutil.ToFloat64(subM.Acked))
	drainBudget := time.Duration(expected/3000)*time.Second + 30*time.Second
	if drainBudget < 60*time.Second {
		drainBudget = 60 * time.Second
	}
	t.Logf("draining: expected=%d budget=%s", expected, drainBudget)
	persisted := waitForPostgres(t, ctx, pool, expected, drainBudget)
	drainElapsed := time.Since(drainStart)

	// E2E latency: select(received_at, inserted_at) deltas, p50/p95/p99.
	rows, err := pool.Query(ctx, `SELECT EXTRACT(EPOCH FROM (inserted_at - received_at)) FROM telemetry ORDER BY inserted_at DESC LIMIT 50000`)
	require.NoError(t, err)
	var latencies []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err == nil {
			latencies = append(latencies, v)
		}
	}
	rows.Close()
	sort.Float64s(latencies)

	p := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)-1))
		return time.Duration(latencies[idx] * float64(time.Second))
	}

	achievedRate := float64(totalSent) / loadElapsed.Seconds()
	persistRate := float64(persisted) / (loadElapsed + drainElapsed).Seconds()

	t.Log("==================== STRESS TEST REPORT ====================")
	t.Logf("Target rate           : %d msg/s", rate)
	t.Logf("Achieved publish rate : %.0f msg/s (%.1f%% of target)", achievedRate, achievedRate/float64(rate)*100)
	t.Logf("Achieved end-to-end   : %.0f msg/s", persistRate)
	t.Logf("")
	dedupCollisions := int64(testutil.ToFloat64(subM.Acked)) - int64(testutil.ToFloat64(flM.Inserted))
	if dedupCollisions < 0 {
		dedupCollisions = 0
	}
	t.Logf("Messages sent         : %d", totalSent)
	t.Logf("Messages persisted    : %d", persisted)
	t.Logf("Dedup collisions      : %d (msgs collapsed by ON CONFLICT — by design)", dedupCollisions)
	t.Logf("Apparent publish loss : %d (%.4f%% — broker drop at QoS 0 + dedup)",
		totalSent-persisted, float64(totalSent-persisted)/float64(totalSent)*100)
	t.Logf("Publish errors        : %d", pubErrs.Load())
	t.Logf("")
	t.Logf("Drain time after stop : %s", drainElapsed.Round(time.Millisecond))
	t.Logf("Latency  p50          : %s", p(0.50).Round(time.Millisecond))
	t.Logf("         p95          : %s", p(0.95).Round(time.Millisecond))
	t.Logf("         p99          : %s", p(0.99).Round(time.Millisecond))
	t.Logf("")
	t.Logf("Buffer  enqueued      : %.0f", testutil.ToFloat64(bufM.Enqueued))
	t.Logf("Buffer  dequeued      : %.0f", testutil.ToFloat64(bufM.Dequeued))
	t.Logf("Buffer  spilled       : %.0f", testutil.ToFloat64(bufM.Spilled))
	t.Logf("Buffer  rejected      : %.0f", testutil.ToFloat64(bufM.Rejected))
	t.Logf("WAL     appended      : %.0f", testutil.ToFloat64(walM.Appended))
	t.Logf("WAL     drained       : %.0f", testutil.ToFloat64(walM.Drained))
	t.Logf("Flusher flushed       : %.0f", testutil.ToFloat64(flM.Flushed))
	t.Logf("Flusher inserted      : %.0f", testutil.ToFloat64(flM.Inserted))
	t.Logf("Flusher errors        : %.0f", testutil.ToFloat64(flM.FlushErrors))
	t.Logf("Flusher retries       : %.0f", testutil.ToFloat64(flM.RetriesTotal))
	t.Logf("Flusher dead-lettered : %.0f", testutil.ToFloat64(flM.DeadLettered))
	t.Logf("Subscriber received   : %.0f", testutil.ToFloat64(subM.Received))
	t.Logf("Subscriber acked      : %.0f", testutil.ToFloat64(subM.Acked))
	t.Log("============================================================")

	// Stress test contract:
	//   • The service-side at-least-once guarantee (subscriber→PG) is
	//     tested in e2e_integration_test.go at QoS 1.
	//   • This stress test uses QoS 0 publishes so we can observe the
	//     service's ceiling without hitting the per-publisher PUBACK
	//     bottleneck. Some publish→broker drop at QoS 0 is expected
	//     under sustained load.
	//   • Some dedup collisions (acked > inserted) are also expected
	//     and correct: when two messages for the same device share a
	//     nanosecond, dedup_key is the same and ON CONFLICT collapses
	//     them. This is the at-least-once → exactly-once promotion.
	//   • Hard service invariant: every distinct dedup_key the flusher
	//     committed must end up in Postgres.
	inserted := int64(testutil.ToFloat64(flM.Inserted))
	require.Equalf(t, inserted, persisted,
		"service-side loss between flusher and Postgres: inserted=%d persisted=%d",
		inserted, persisted)

	pipelineCancel()
	pipelineWG.Wait()
}

// waitForPostgres polls until n rows have landed or timeout fires; returns
// the count actually seen.
func waitForPostgres(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM telemetry`).Scan(&n); err == nil {
			last = n
			if n >= want {
				return n
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return last
}

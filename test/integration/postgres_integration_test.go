//go:build integration

// Integration tests for the postgres layer. These spin up a Postgres
// container via testcontainers-go and exercise the migration runner plus
// CopyMessages end-to-end. Gated behind the `integration` build tag so
// `go test ./...` and CI's unit-tests job stay fast and Docker-free.
//
// Run with:
//
//	go test -tags=integration ./test/integration/...
//	make test-integration
package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// migrationsDir resolves the absolute path to ./migrations from this test
// file's location, so the test runs regardless of CWD.
func migrationsDir(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations")
}

// startPostgres boots a clean Postgres 17 container and returns its DSN.
// The container is stopped at test cleanup. Accepts testing.TB so both
// tests and benchmarks can share setup.
func startPostgres(tb testing.TB, ctx context.Context) string {
	tb.Helper()
	container, err := tcpostgres.Run(ctx,
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
		_ = container.Terminate(ctx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(tb, err)

	// Hold-off until pgx itself can connect — BasicWaitStrategies is good
	// on most machines but can be flaky in CI.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			lastErr = pool.Ping(ctx)
			pool.Close()
			if lastErr == nil {
				break
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			tb.Fatalf("postgres never became ready: %v", lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return dsn
}

func TestMigrate_UpDownVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn := startPostgres(t, ctx)
	source := "file://" + migrationsDir(t)

	v, dirty, err := postgres.Version(source, dsn)
	require.NoError(t, err)
	assert.Equal(t, uint(0), v)
	assert.False(t, dirty)

	require.NoError(t, postgres.Migrate(source, dsn))

	v, dirty, err = postgres.Version(source, dsn)
	require.NoError(t, err)
	// Latest migration in ./migrations — bump when a new file is added.
	assert.Equal(t, uint(2), v)
	assert.False(t, dirty)

	// Idempotent re-up.
	require.NoError(t, postgres.Migrate(source, dsn))

	require.NoError(t, postgres.MigrateDown(source, dsn, 0))
	v, _, err = postgres.Version(source, dsn)
	require.NoError(t, err)
	assert.Equal(t, uint(0), v)
}

func TestCopyMessages_PersistsAndDedups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn := startPostgres(t, ctx)
	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), dsn))

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{
		DSN:      dsn,
		MaxConns: 4,
		MinConns: 1,
	})
	require.NoError(t, err)
	defer pool.Close()

	cp, err := postgres.NewCopier(pool, config.PostgresConfig{
		Schema:  "public",
		Table:   "telemetry",
		Columns: []string{"tenant_id", "device_uuid", "topic", "payload", "received_at", "dedup_key"},
	})
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Microsecond)
	makeBatch := func(start, n int) []postgres.Message {
		out := make([]postgres.Message, n)
		for i := 0; i < n; i++ {
			id := uuid.New()
			out[i] = postgres.Message{
				TenantID:   "acme",
				DeviceUUID: id,
				Topic:      fmt.Sprintf("t/acme/d/%s/evt/state", id),
				Payload:    []byte(fmt.Sprintf(`{"seq":%d}`, start+i)),
				ReceivedAt: now.Add(time.Duration(start+i) * time.Millisecond),
				DedupKey:   fmt.Sprintf("acme:%d", start+i),
			}
		}
		return out
	}

	first := makeBatch(0, 100)
	inserted, err := cp.CopyMessages(ctx, first)
	require.NoError(t, err)
	assert.Equal(t, int64(100), inserted)

	// Replay the same batch — every dedup_key conflicts, so 0 inserted.
	inserted, err = cp.CopyMessages(ctx, first)
	require.NoError(t, err)
	assert.Equal(t, int64(0), inserted, "duplicate batch must collapse via ON CONFLICT")

	// Mixed batch: 50 from the first set + 50 fresh => only 50 inserted.
	mixed := append(makeBatch(0, 50), makeBatch(100, 50)...)
	inserted, err = cp.CopyMessages(ctx, mixed)
	require.NoError(t, err)
	assert.Equal(t, int64(50), inserted)

	var rows int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM telemetry`).Scan(&rows))
	assert.Equal(t, int64(150), rows)

	// Empty batch is a no-op.
	inserted, err = cp.CopyMessages(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), inserted)
}

func TestMigrate_TelemetryUnparseableExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn := startPostgres(t, ctx)
	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), dsn))

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{DSN: dsn, MaxConns: 2, MinConns: 1})
	require.NoError(t, err)
	defer pool.Close()

	// to_regclass returns NULL when the table doesn't exist; a non-NULL
	// result confirms migration 0002 ran.
	var name *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT to_regclass('public.telemetry_unparseable')::text`).Scan(&name))
	require.NotNil(t, name)
	assert.Equal(t, "telemetry_unparseable", *name)
}

func TestInsertUnparseable_PersistsRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn := startPostgres(t, ctx)
	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), dsn))

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{DSN: dsn, MaxConns: 2, MinConns: 1})
	require.NoError(t, err)
	defer pool.Close()

	w, err := postgres.NewUnparseableWriter(pool, config.PostgresConfig{Schema: "public"})
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Microsecond)
	row := postgres.Unparseable{
		Topic:       "t/+/d/+/evt/#",
		Payload:     []byte(`{"junk":true}`),
		ErrorClass:  "tenant_invalid",
		ErrorDetail: "+",
		ReceivedAt:  now,
	}
	require.NoError(t, w.InsertUnparseable(ctx, row))

	var (
		topic, errorClass string
		payload           []byte
		errorDetail       *string
		receivedAt        time.Time
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT topic, payload, error_class, error_detail, received_at
		FROM telemetry_unparseable
		ORDER BY id DESC LIMIT 1`,
	).Scan(&topic, &payload, &errorClass, &errorDetail, &receivedAt))

	assert.Equal(t, row.Topic, topic)
	assert.Equal(t, row.Payload, payload)
	assert.Equal(t, row.ErrorClass, errorClass)
	require.NotNil(t, errorDetail)
	assert.Equal(t, "+", *errorDetail)
	assert.WithinDuration(t, now, receivedAt, time.Second)
}

func TestInsertUnparseable_EmptyDetailIsNull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn := startPostgres(t, ctx)
	require.NoError(t, postgres.Migrate("file://"+migrationsDir(t), dsn))

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{DSN: dsn, MaxConns: 2, MinConns: 1})
	require.NoError(t, err)
	defer pool.Close()

	w, err := postgres.NewUnparseableWriter(pool, config.PostgresConfig{Schema: "public"})
	require.NoError(t, err)

	now := time.Now().UTC()
	require.NoError(t, w.InsertUnparseable(ctx, postgres.Unparseable{
		Topic:      "garbage",
		Payload:    []byte(`p`),
		ErrorClass: "topic_structure",
		ReceivedAt: now,
	}))

	var detail *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT error_detail FROM telemetry_unparseable ORDER BY id DESC LIMIT 1`,
	).Scan(&detail))
	assert.Nil(t, detail, "empty ErrorDetail must persist as NULL")
}

// BenchmarkCopyMessages_100K reports throughput for a single 100K-row
// batch. Run with:
//
//	go test -tags=integration -run=^$ -bench=BenchmarkCopyMessages_100K \
//	    ./test/integration/...
func BenchmarkCopyMessages_100K(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	dsn := startPostgres(b, ctx)
	require.NoError(b, postgres.Migrate("file://"+migrationsDir(b), dsn))

	pool, err := postgres.NewPool(ctx, config.PostgresConfig{
		DSN: dsn, MaxConns: 4, MinConns: 1,
	})
	require.NoError(b, err)
	defer pool.Close()

	cp, err := postgres.NewCopier(pool, config.PostgresConfig{
		Schema:  "public",
		Table:   "telemetry",
		Columns: []string{"tenant_id", "device_uuid", "topic", "payload", "received_at", "dedup_key"},
	})
	require.NoError(b, err)

	const batchSize = 100_000
	batch := make([]postgres.Message, batchSize)
	now := time.Now().UTC()
	id := uuid.New()
	payload := []byte(`{"k":"v"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := i * batchSize
		for j := 0; j < batchSize; j++ {
			batch[j] = postgres.Message{
				TenantID:   "bench",
				DeviceUUID: id,
				Topic:      "t/bench/d/x/evt/state",
				Payload:    payload,
				ReceivedAt: now,
				DedupKey:   fmt.Sprintf("bench:%d", offset+j),
			}
		}
		if _, err := cp.CopyMessages(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(batchSize) * int64(len(payload)))
}

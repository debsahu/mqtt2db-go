package deadletter_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/deadletter"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

type fakeS3 struct {
	mu      sync.Mutex
	puts    []putRecord
	putErr  error
	headOK  bool
	headErr error
}

type putRecord struct {
	Bucket string
	Key    string
	Body   []byte
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return nil, f.putErr
	}
	body, _ := io.ReadAll(in.Body)
	f.puts = append(f.puts, putRecord{
		Bucket: aws.ToString(in.Bucket),
		Key:    aws.ToString(in.Key),
		Body:   body,
	})
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) HeadBucket(_ context.Context, _ *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeS3) snapshot() []putRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]putRecord, len(f.puts))
	copy(out, f.puts)
	return out
}

func newSink(t *testing.T, api deadletter.S3API) (*deadletter.Sink, *metrics.DeadLetterMetrics) {
	t.Helper()
	reg := metrics.NewRegistry()
	m := metrics.NewDeadLetterMetrics(reg)
	s := deadletter.NewSinkWithAPI(config.S3Config{Bucket: "mqtt2db-go-dlq"}, api, m)
	return s, m
}

func makeMsg(seq int) postgres.Message {
	return postgres.Message{
		TenantID:   "acme",
		DeviceUUID: uuid.New(),
		Topic:      "t/acme/d/x/evt/state",
		Payload:    []byte(`{"k":"v"}`),
		ReceivedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Millisecond),
		DedupKey:   "acme:" + uuid.NewString(),
	}
}

func TestWrite_PutsEnvelopeWithExpectedKeyShape(t *testing.T) {
	api := &fakeS3{}
	sink, m := newSink(t, api)

	msg := makeMsg(0)
	err := sink.Write(context.Background(), msg, errors.New("pg unavailable"))
	require.NoError(t, err)

	puts := api.snapshot()
	require.Len(t, puts, 1)
	assert.Equal(t, "mqtt2db-go-dlq", puts[0].Bucket)
	assert.True(t, strings.HasPrefix(puts[0].Key, "dead-letter/"), "key=%q", puts[0].Key)
	assert.Contains(t, puts[0].Key, msg.TenantID)
	assert.Contains(t, puts[0].Key, msg.DeviceUUID.String())
	assert.True(t, strings.HasSuffix(puts[0].Key, ".json"))

	var env map[string]any
	require.NoError(t, json.Unmarshal(puts[0].Body, &env))
	assert.Equal(t, "acme", env["tenant_id"])
	assert.Equal(t, msg.DeviceUUID.String(), env["device_uuid"])
	assert.Equal(t, "pg unavailable", env["last_error"])

	assert.Equal(t, float64(1), testutil.ToFloat64(m.Written))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.Errors))
}

func TestWrite_KeysAreUniqueAcrossCalls(t *testing.T) {
	api := &fakeS3{}
	sink, _ := newSink(t, api)

	for i := 0; i < 20; i++ {
		require.NoError(t, sink.Write(context.Background(), makeMsg(i), nil))
	}
	puts := api.snapshot()
	keys := make(map[string]struct{})
	for _, p := range puts {
		keys[p.Key] = struct{}{}
	}
	assert.Len(t, keys, len(puts), "all dead-letter keys must be unique")
}

func TestWrite_ReportsPutObjectErrors(t *testing.T) {
	api := &fakeS3{putErr: errors.New("disk full")}
	sink, m := newSink(t, api)

	err := sink.Write(context.Background(), makeMsg(0), nil)
	require.Error(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Errors))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.Written))
}

func TestHealthCheck_DelegatesToHeadBucket(t *testing.T) {
	api := &fakeS3{headOK: true}
	sink, _ := newSink(t, api)
	require.NoError(t, sink.HealthCheck(context.Background()))

	api2 := &fakeS3{headErr: errors.New("nope")}
	sink2, _ := newSink(t, api2)
	require.Error(t, sink2.HealthCheck(context.Background()))
}

func TestNewSink_RejectsMissingBucket(t *testing.T) {
	reg := metrics.NewRegistry()
	m := metrics.NewDeadLetterMetrics(reg)
	_, err := deadletter.NewSink(context.Background(), config.S3Config{}, m)
	require.Error(t, err)
}

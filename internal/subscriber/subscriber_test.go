package subscriber_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/config"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/subscriber"
)

// fakeRing implements subscriber.Ring with knobs for the rejection paths.
type fakeRing struct {
	mu        sync.Mutex
	msgs      []postgres.Message
	rejectN   int
	overState buffer.State
	spilled   atomic.Int64
}

func (f *fakeRing) Enqueue(m postgres.Message) (buffer.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rejectN > 0 {
		f.rejectN--
		return buffer.StatePause, buffer.ErrFull
	}
	f.msgs = append(f.msgs, m)
	return f.overState, nil
}
func (f *fakeRing) State() buffer.State { return f.overState }
func (f *fakeRing) MarkSpilled()        { f.spilled.Add(1) }
func (f *fakeRing) Snapshot() []postgres.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]postgres.Message(nil), f.msgs...)
}

// fakeWAL implements subscriber.WAL.
type fakeWAL struct {
	mu        sync.Mutex
	msgs      []postgres.Message
	AppendErr error
}

func (f *fakeWAL) Append(m postgres.Message) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.AppendErr != nil {
		return nil, f.AppendErr
	}
	f.msgs = append(f.msgs, m)
	return []byte(fmt.Sprintf("k%d", len(f.msgs))), nil
}
func (f *fakeWAL) Snapshot() []postgres.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]postgres.Message(nil), f.msgs...)
}

func newSub(t *testing.T, ring subscriber.Ring, w subscriber.WAL) (*subscriber.Subscriber, *metrics.SubscriberMetrics) {
	t.Helper()
	t.Setenv("HOSTNAME", "test-host")
	reg := metrics.NewRegistry()
	m := metrics.NewSubscriberMetrics(reg)
	sub, err := subscriber.New(config.MQTTConfig{
		Brokers:        []string{"tcp://localhost:1883"},
		ClientIDPrefix: "mqtt2db-go-test",
	}, ring, w, m, nil)
	require.NoError(t, err)
	return sub, m
}

func TestHandleMessage_NormalEnqueueAndAck(t *testing.T) {
	ring := &fakeRing{}
	w := &fakeWAL{}
	sub, m := newSub(t, ring, w)

	dev := uuid.New()
	topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
	var acked atomic.Int64
	sub.HandleMessage(topic, []byte(`{"x":1}`), func() { acked.Add(1) })

	require.Len(t, ring.Snapshot(), 1)
	got := ring.Snapshot()[0]
	assert.Equal(t, "acme", got.TenantID)
	assert.Equal(t, dev, got.DeviceUUID)
	assert.Equal(t, []byte(`{"x":1}`), got.Payload)
	assert.Contains(t, got.DedupKey, dev.String())

	assert.Equal(t, int64(1), acked.Load())
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Received))
	assert.Empty(t, w.Snapshot())
}

func TestHandleMessage_RingFullSpillsToWAL(t *testing.T) {
	ring := &fakeRing{rejectN: 1}
	w := &fakeWAL{}
	sub, _ := newSub(t, ring, w)

	dev := uuid.New()
	topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
	var acked atomic.Int64
	sub.HandleMessage(topic, []byte(`{"x":1}`), func() { acked.Add(1) })

	assert.Empty(t, ring.Snapshot())
	require.Len(t, w.Snapshot(), 1)
	assert.Equal(t, int64(1), ring.spilled.Load())
	assert.Equal(t, int64(1), acked.Load(), "spill path must still ack")
}

func TestHandleMessage_RingFullAndWALDownDoesNotAck(t *testing.T) {
	ring := &fakeRing{rejectN: 1}
	w := &fakeWAL{AppendErr: assertErr}
	sub, _ := newSub(t, ring, w)

	dev := uuid.New()
	topic := fmt.Sprintf("t/acme/d/%s/evt/state", dev)
	var acked atomic.Int64
	sub.HandleMessage(topic, []byte(`{"x":1}`), func() { acked.Add(1) })

	assert.Empty(t, ring.Snapshot())
	assert.Empty(t, w.Snapshot())
	assert.Equal(t, int64(0), acked.Load(), "no ack means broker redelivers")
}

func TestHandleMessage_BadTopicAcks(t *testing.T) {
	ring := &fakeRing{}
	w := &fakeWAL{}
	sub, m := newSub(t, ring, w)

	var acked atomic.Int64
	sub.HandleMessage("t/garbage", []byte(`x`), func() { acked.Add(1) })

	assert.Empty(t, ring.Snapshot())
	assert.Empty(t, w.Snapshot())
	assert.Equal(t, int64(1), acked.Load(), "bad topic acks so the broker drops it")
	assert.Equal(t, float64(1), testutil.ToFloat64(m.HandlerErrors))
}

func TestNew_RequiresDeps(t *testing.T) {
	t.Setenv("HOSTNAME", "test-host")
	cfg := config.MQTTConfig{Brokers: []string{"tcp://localhost:1883"}, ClientIDPrefix: "x"}
	reg := metrics.NewRegistry()
	m := metrics.NewSubscriberMetrics(reg)

	_, err := subscriber.New(cfg, nil, &fakeWAL{}, m, nil)
	require.Error(t, err)

	_, err = subscriber.New(cfg, &fakeRing{}, &fakeWAL{}, nil, nil)
	require.Error(t, err)

	noBrokers := cfg
	noBrokers.Brokers = nil
	_, err = subscriber.New(noBrokers, &fakeRing{}, &fakeWAL{}, m, nil)
	require.Error(t, err)
}

func TestClientID_StableAcrossCalls(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-7")
	cfg := config.MQTTConfig{Brokers: []string{"tcp://localhost:1883"}, ClientIDPrefix: "ingest"}
	reg := metrics.NewRegistry()
	m := metrics.NewSubscriberMetrics(reg)

	a, err := subscriber.New(cfg, &fakeRing{}, &fakeWAL{}, m, nil)
	require.NoError(t, err)
	b, err := subscriber.New(cfg, &fakeRing{}, &fakeWAL{}, metrics.NewSubscriberMetrics(metrics.NewRegistry()), nil)
	require.NoError(t, err)

	assert.Equal(t, "ingest-pod-7", a.ClientID())
	assert.Equal(t, a.ClientID(), b.ClientID())
}

// assertErr is a sentinel for the WAL-failure test.
var assertErr = fakeErr("wal down")

type fakeErr string

func (f fakeErr) Error() string { return string(f) }

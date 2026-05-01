package buffer_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/debsahu/mqtt2db-go/internal/buffer"
	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

func newRing(t *testing.T, cfg buffer.Config) (*buffer.Ring, *metrics.BufferMetrics) {
	t.Helper()
	reg := metrics.NewRegistry()
	m := metrics.NewBufferMetrics(reg)
	r, err := buffer.New(cfg, m)
	require.NoError(t, err)
	return r, m
}

func makeMsg(seq int) postgres.Message {
	id := uuid.New()
	return postgres.Message{
		TenantID:   "acme",
		DeviceUUID: id,
		Topic:      "t/acme/d/x/evt/state",
		Payload:    []byte(fmt.Sprintf(`{"seq":%d}`, seq)),
		ReceivedAt: time.Now(),
		DedupKey:   fmt.Sprintf("acme:%d", seq),
	}
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	reg := metrics.NewRegistry()
	m := metrics.NewBufferMetrics(reg)

	cases := []buffer.Config{
		{MaxMessages: 0, MaxBytes: 1024, SpillThreshold: 0.8, PauseThreshold: 0.95},
		{MaxMessages: 10, MaxBytes: 0, SpillThreshold: 0.8, PauseThreshold: 0.95},
		{MaxMessages: 10, MaxBytes: 1024, SpillThreshold: 0.95, PauseThreshold: 0.8},
		{MaxMessages: 10, MaxBytes: 1024, SpillThreshold: 0, PauseThreshold: 0.95},
		{MaxMessages: 10, MaxBytes: 1024, SpillThreshold: 0.8, PauseThreshold: 1.0},
	}
	for i, cfg := range cases {
		_, err := buffer.New(cfg, m)
		assert.Error(t, err, "case %d should fail", i)
	}

	_, err := buffer.New(buffer.Config{
		MaxMessages: 10, MaxBytes: 1024, SpillThreshold: 0.8, PauseThreshold: 0.95,
	}, nil)
	require.Error(t, err)
}

func TestEnqueueDequeue_PreservesFIFO(t *testing.T) {
	r, m := newRing(t, buffer.Config{
		MaxMessages: 100, MaxBytes: 1 << 20, SpillThreshold: 0.8, PauseThreshold: 0.95,
	})

	for i := 0; i < 50; i++ {
		state, err := r.Enqueue(makeMsg(i))
		require.NoError(t, err)
		assert.Equal(t, buffer.StateNormal, state)
	}
	assert.Equal(t, 50, r.Len())

	out := r.Dequeue(50)
	require.Len(t, out, 50)
	for i, msg := range out {
		assert.Equal(t, fmt.Sprintf("acme:%d", i), msg.DedupKey)
	}
	assert.Equal(t, 0, r.Len())
	assert.Equal(t, int64(0), r.Bytes())

	// Counter sanity: 50 in, 50 out.
	assert.Equal(t, float64(50), testutil.ToFloat64(m.Enqueued))
	assert.Equal(t, float64(50), testutil.ToFloat64(m.Dequeued))
}

func TestState_TransitionsAtSpillAndPauseThresholds(t *testing.T) {
	// Capacity 10, spill at 8, pause at 9.
	r, _ := newRing(t, buffer.Config{
		MaxMessages: 10, MaxBytes: 1 << 20, SpillThreshold: 0.8, PauseThreshold: 0.9,
	})

	for i := 0; i < 7; i++ {
		state, err := r.Enqueue(makeMsg(i))
		require.NoError(t, err)
		assert.Equal(t, buffer.StateNormal, state, "i=%d", i)
	}

	// 8th enqueue tips into spill.
	state, err := r.Enqueue(makeMsg(7))
	require.NoError(t, err)
	assert.Equal(t, buffer.StateSpill, state)

	// 9th tips into pause.
	state, err = r.Enqueue(makeMsg(8))
	require.NoError(t, err)
	assert.Equal(t, buffer.StatePause, state)

	// Subsequent enqueues stay accepted until ring is actually full.
	state, err = r.Enqueue(makeMsg(9))
	require.NoError(t, err)
	assert.Equal(t, buffer.StatePause, state)
}

func TestEnqueue_RejectsWhenFull(t *testing.T) {
	r, m := newRing(t, buffer.Config{
		MaxMessages: 4, MaxBytes: 1 << 20, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})
	for i := 0; i < 4; i++ {
		_, err := r.Enqueue(makeMsg(i))
		require.NoError(t, err)
	}
	state, err := r.Enqueue(makeMsg(99))
	assert.ErrorIs(t, err, buffer.ErrFull)
	assert.Equal(t, buffer.StatePause, state)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Rejected))
}

func TestEnqueue_RejectsWhenByteCapExceeded(t *testing.T) {
	// makeMsg charges len(Payload)+len(Topic)+len(TenantID)+len(DedupKey).
	// "acme" + "t/acme/d/x/evt/state" + `{"seq":N}` + "acme:N" ~= 39 bytes
	// per message, so cap=100 admits 2 (78 bytes total) and rejects the 3rd.
	r, m := newRing(t, buffer.Config{
		MaxMessages: 100, MaxBytes: 100, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})
	_, err := r.Enqueue(makeMsg(0))
	require.NoError(t, err)
	_, err = r.Enqueue(makeMsg(1))
	require.NoError(t, err)
	_, err = r.Enqueue(makeMsg(2))
	assert.ErrorIs(t, err, buffer.ErrFull)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Rejected))
}

func TestDequeue_PartialDrainAdvancesHead(t *testing.T) {
	r, _ := newRing(t, buffer.Config{
		MaxMessages: 10, MaxBytes: 1 << 20, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})
	for i := 0; i < 5; i++ {
		_, err := r.Enqueue(makeMsg(i))
		require.NoError(t, err)
	}
	first := r.Dequeue(3)
	require.Len(t, first, 3)
	assert.Equal(t, "acme:0", first[0].DedupKey)
	assert.Equal(t, "acme:2", first[2].DedupKey)

	rest := r.Dequeue(10)
	require.Len(t, rest, 2)
	assert.Equal(t, "acme:3", rest[0].DedupKey)
	assert.Equal(t, "acme:4", rest[1].DedupKey)

	assert.Nil(t, r.Dequeue(10))
}

func TestDequeue_ZeroAndNegative(t *testing.T) {
	r, _ := newRing(t, buffer.Config{
		MaxMessages: 4, MaxBytes: 1 << 20, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})
	_, err := r.Enqueue(makeMsg(0))
	require.NoError(t, err)
	assert.Nil(t, r.Dequeue(0))
	assert.Nil(t, r.Dequeue(-5))
	assert.Equal(t, 1, r.Len())
}

func TestRing_WrapsAround(t *testing.T) {
	r, _ := newRing(t, buffer.Config{
		MaxMessages: 4, MaxBytes: 1 << 20, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})
	for i := 0; i < 4; i++ {
		_, err := r.Enqueue(makeMsg(i))
		require.NoError(t, err)
	}
	out := r.Dequeue(3)
	require.Len(t, out, 3)

	// Now head=3, size=1; enqueue 3 more should wrap into slots 0,1,2.
	for i := 4; i < 7; i++ {
		_, err := r.Enqueue(makeMsg(i))
		require.NoError(t, err)
	}
	final := r.Dequeue(10)
	require.Len(t, final, 4)
	for i, m := range final {
		assert.Equal(t, fmt.Sprintf("acme:%d", 3+i), m.DedupKey)
	}
}

func TestMarkSpilled_IncrementsCounter(t *testing.T) {
	r, m := newRing(t, buffer.Config{
		MaxMessages: 4, MaxBytes: 1 << 20, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})
	r.MarkSpilled()
	r.MarkSpilled()
	assert.Equal(t, float64(2), testutil.ToFloat64(m.Spilled))
}

func TestRing_ConcurrentEnqueueDequeue(t *testing.T) {
	r, m := newRing(t, buffer.Config{
		MaxMessages: 1024, MaxBytes: 1 << 24, SpillThreshold: 0.5, PauseThreshold: 0.75,
	})

	const producers = 4
	const perProducer = 5_000

	var wg sync.WaitGroup
	wg.Add(producers + 1)

	var produced atomic.Int64
	for p := 0; p < producers; p++ {
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				for {
					_, err := r.Enqueue(makeMsg(pid*perProducer + i))
					if err == nil {
						produced.Add(1)
						break
					}
					// ErrFull: backoff while consumer drains.
					time.Sleep(time.Microsecond)
				}
			}
		}(p)
	}

	var consumed atomic.Int64
	go func() {
		defer wg.Done()
		want := int64(producers * perProducer)
		for consumed.Load() < want {
			batch := r.Dequeue(256)
			if len(batch) == 0 {
				time.Sleep(10 * time.Microsecond)
				continue
			}
			consumed.Add(int64(len(batch)))
		}
	}()

	wg.Wait()

	assert.Equal(t, int64(producers*perProducer), produced.Load())
	assert.Equal(t, int64(producers*perProducer), consumed.Load())
	assert.Equal(t, float64(producers*perProducer), testutil.ToFloat64(m.Enqueued))
	assert.Equal(t, float64(producers*perProducer), testutil.ToFloat64(m.Dequeued))
	assert.Equal(t, 0, r.Len())
}

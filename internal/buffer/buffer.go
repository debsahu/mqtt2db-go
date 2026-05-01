// Package buffer implements the in-memory ring between the MQTT subscriber
// and the Postgres flusher. It is bounded by both message count and byte
// size; whichever cap fires first wins.
//
// Decision flow per incoming message:
//
//   - depth/capacity < spill_threshold       -> Enqueue accepted
//   - spill_threshold <= depth < pause       -> caller routes to WAL via spill
//   - depth >= pause_threshold               -> caller stops acking MQTT
//   - ring is full (depth == capacity)       -> Enqueue rejected
//
// The ring itself is a simple mutex-guarded slice ring. We chose this over
// a lock-free MPMC queue for two reasons: the buffer is one-stage hot,
// not the dominant cost (Postgres CopyFrom is); and benchmark numbers
// (~50M ops/sec on M1 Pro for sync.Mutex contention at this granularity)
// dwarf the 1K-10K msg/sec target. We can revisit if profiling proves
// otherwise.
package buffer

import (
	"errors"
	"sync"

	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// ErrFull is returned by Enqueue when the ring has reached MaxMessages or
// MaxBytes. Callers should route the message to WAL or, at the pause
// threshold, stop pulling from MQTT.
var ErrFull = errors.New("buffer: full")

// State describes the ring's pressure level for the subscriber's use.
type State int

const (
	// StateNormal: ring is below spill_threshold; accept and enqueue.
	StateNormal State = iota
	// StateSpill: ring is at or above spill_threshold; accept but write
	// past the ring directly to WAL.
	StateSpill
	// StatePause: ring is at or above pause_threshold; stop acking MQTT
	// so the broker buffers or redelivers via the shared subscription.
	StatePause
)

func (s State) String() string {
	switch s {
	case StateNormal:
		return "normal"
	case StateSpill:
		return "spill"
	case StatePause:
		return "pause"
	default:
		return "unknown"
	}
}

// Config configures a Ring. Fields mirror config.BufferConfig and the same
// validation rules apply (spill < pause, both in (0,1)).
type Config struct {
	MaxMessages    int
	MaxBytes       int64
	SpillThreshold float64
	PauseThreshold float64
}

// Ring is a bounded FIFO of postgres.Message ready for the flusher.
type Ring struct {
	cfg   Config
	mu    sync.Mutex
	head  int
	size  int
	bytes int64
	slots []postgres.Message
	m     *metrics.BufferMetrics

	// spillCount and pauseCount are int64-form thresholds, precomputed
	// so the hot path avoids float math.
	spillCount int
	pauseCount int
	spillBytes int64
	pauseBytes int64
}

// New constructs a Ring. Both Config and metrics are required; pass a
// fresh registry in tests via metrics.NewRegistry to keep counters
// isolated.
func New(cfg Config, m *metrics.BufferMetrics) (*Ring, error) {
	if cfg.MaxMessages <= 0 {
		return nil, errors.New("buffer: MaxMessages must be > 0")
	}
	if cfg.MaxBytes <= 0 {
		return nil, errors.New("buffer: MaxBytes must be > 0")
	}
	if cfg.SpillThreshold <= 0 || cfg.SpillThreshold >= 1 {
		return nil, errors.New("buffer: SpillThreshold must be in (0, 1)")
	}
	if cfg.PauseThreshold <= 0 || cfg.PauseThreshold >= 1 {
		return nil, errors.New("buffer: PauseThreshold must be in (0, 1)")
	}
	if cfg.SpillThreshold >= cfg.PauseThreshold {
		return nil, errors.New("buffer: SpillThreshold must be < PauseThreshold")
	}
	if m == nil {
		return nil, errors.New("buffer: metrics required")
	}

	r := &Ring{
		cfg:        cfg,
		slots:      make([]postgres.Message, cfg.MaxMessages),
		m:          m,
		spillCount: int(float64(cfg.MaxMessages) * cfg.SpillThreshold),
		pauseCount: int(float64(cfg.MaxMessages) * cfg.PauseThreshold),
		spillBytes: int64(float64(cfg.MaxBytes) * cfg.SpillThreshold),
		pauseBytes: int64(float64(cfg.MaxBytes) * cfg.PauseThreshold),
	}
	m.Capacity.Set(float64(cfg.MaxMessages))
	return r, nil
}

// State reports the current pressure level. Subscriber checks this on
// each incoming message to decide whether to enqueue, spill, or pause.
func (r *Ring) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stateLocked()
}

func (r *Ring) stateLocked() State {
	if r.size >= r.pauseCount || r.bytes >= r.pauseBytes {
		return StatePause
	}
	if r.size >= r.spillCount || r.bytes >= r.spillBytes {
		return StateSpill
	}
	return StateNormal
}

// Len returns the current number of messages in the ring.
func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

// Bytes returns the current byte total of messages in the ring.
func (r *Ring) Bytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

// Enqueue adds m to the ring. Returns the post-enqueue State so the caller
// can decide whether to keep ack'ing MQTT, start spilling future messages
// to WAL, or pause consumption. Returns ErrFull if the ring is at capacity.
//
// Enqueue itself does not implement the spill policy — it is the
// subscriber's responsibility to consult State() and route to WAL when
// StateSpill is reported. We keep these decisions outside the buffer so
// the same Ring can serve different routing strategies in tests.
func (r *Ring) Enqueue(m postgres.Message) (State, error) {
	sz := messageSize(m)

	r.mu.Lock()
	if r.size >= r.cfg.MaxMessages || r.bytes+sz > r.cfg.MaxBytes {
		r.mu.Unlock()
		r.m.Rejected.Inc()
		return StatePause, ErrFull
	}
	tail := (r.head + r.size) % r.cfg.MaxMessages
	r.slots[tail] = m
	r.size++
	r.bytes += sz
	state := r.stateLocked()
	depth := r.size
	bytes := r.bytes
	r.mu.Unlock()

	r.m.Enqueued.Inc()
	r.m.Depth.Set(float64(depth))
	r.m.Bytes.Set(float64(bytes))
	return state, nil
}

// MarkSpilled records that a message bypassed the ring and went straight
// to WAL because the buffer was at the spill threshold. Subscribers call
// this after a successful WAL write so we can graph spill rate.
func (r *Ring) MarkSpilled() {
	r.m.Spilled.Inc()
}

// Dequeue removes up to n messages from the head of the ring and returns
// them as a slice. Returns nil if the ring is empty. The returned slice
// is owned by the caller; the underlying ring slot is zeroed so the
// Message and any large payload are eligible for GC.
func (r *Ring) Dequeue(n int) []postgres.Message {
	if n <= 0 {
		return nil
	}
	r.mu.Lock()
	if r.size == 0 {
		r.mu.Unlock()
		return nil
	}
	if n > r.size {
		n = r.size
	}
	out := make([]postgres.Message, n)
	var freedBytes int64
	for i := 0; i < n; i++ {
		idx := (r.head + i) % r.cfg.MaxMessages
		out[i] = r.slots[idx]
		freedBytes += messageSize(r.slots[idx])
		// Zero the slot so the payload isn't pinned by the ring after
		// dequeue.
		r.slots[idx] = postgres.Message{}
	}
	r.head = (r.head + n) % r.cfg.MaxMessages
	r.size -= n
	r.bytes -= freedBytes
	depth := r.size
	bytes := r.bytes
	r.mu.Unlock()

	r.m.Dequeued.Add(float64(n))
	r.m.Depth.Set(float64(depth))
	r.m.Bytes.Set(float64(bytes))
	return out
}

// messageSize returns the in-memory cost we charge a message against
// MaxBytes. Payload dominates; everything else is small.
func messageSize(m postgres.Message) int64 {
	return int64(len(m.Payload) + len(m.Topic) + len(m.TenantID) + len(m.DedupKey))
}

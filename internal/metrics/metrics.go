// Package metrics owns the Prometheus registry for mqtt2db-go.
//
// Components ask this package for the metrics they need rather than
// touching prometheus.DefaultRegisterer directly. Two reasons:
//
//  1. Tests that exercise a component shouldn't pollute the global
//     registry. Constructors take a *prometheus.Registry so tests can
//     pass a fresh one.
//  2. The set of buffer/flusher/subscriber metrics is small enough that
//     keeping every name and label in one place prevents drift in label
//     cardinality.
//
// All metric names use the `mqtt2db_` prefix so they don't collide with
// metrics from PgBouncer, Comqtt, or other services scraped by the same
// Prometheus.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Namespace is the Prometheus name prefix for everything this service
// exports.
const Namespace = "mqtt2db"

// NewRegistry returns a fresh, isolated registry. Production code calls
// this once at boot; tests call it per-test.
func NewRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

// BufferMetrics is the metric set the ring buffer exports. Every field is
// non-nil and pre-registered.
type BufferMetrics struct {
	Depth    prometheus.Gauge   // current message count
	Bytes    prometheus.Gauge   // current byte usage
	Capacity prometheus.Gauge   // configured max_messages, set once at init
	Enqueued prometheus.Counter // accepted into the ring
	Dequeued prometheus.Counter // pulled by the flusher
	Spilled  prometheus.Counter // bypassed ring -> WAL because spill threshold hit
	Rejected prometheus.Counter // refused (ring full, pause threshold)
}

// NewBufferMetrics registers the buffer metric set on reg.
func NewBufferMetrics(reg prometheus.Registerer) *BufferMetrics {
	m := &BufferMetrics{
		Depth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "depth_messages",
			Help: "Current number of messages held in the in-memory ring buffer.",
		}),
		Bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "depth_bytes",
			Help: "Current bytes held in the in-memory ring buffer.",
		}),
		Capacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "capacity_messages",
			Help: "Configured max_messages for the ring buffer.",
		}),
		Enqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "enqueued_total",
			Help: "Messages accepted into the ring buffer.",
		}),
		Dequeued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "dequeued_total",
			Help: "Messages drained from the ring buffer.",
		}),
		Spilled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "spilled_total",
			Help: "Messages routed past the ring directly to WAL (spill threshold reached).",
		}),
		Rejected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "buffer",
			Name: "rejected_total",
			Help: "Messages refused by the ring (full or pause threshold reached).",
		}),
	}
	reg.MustRegister(m.Depth, m.Bytes, m.Capacity, m.Enqueued, m.Dequeued, m.Spilled, m.Rejected)
	return m
}

// WALMetrics is the metric set the Badger-backed overflow log exports.
type WALMetrics struct {
	Entries     prometheus.Gauge   // current entry count (best-effort; computed via Badger LSM stats)
	BytesOnDisk prometheus.Gauge   // approximate on-disk size in bytes
	OldestAge   prometheus.Gauge   // age of the oldest live entry in seconds (0 when empty)
	Appended    prometheus.Counter // entries appended
	Drained     prometheus.Counter // entries successfully drained (and deleted)
	Expired     prometheus.Counter // entries dropped by TTL
}

// NewWALMetrics registers the WAL metric set on reg.
func NewWALMetrics(reg prometheus.Registerer) *WALMetrics {
	m := &WALMetrics{
		Entries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "wal",
			Name: "entries",
			Help: "Approximate number of live entries in the WAL.",
		}),
		BytesOnDisk: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "wal",
			Name: "bytes_on_disk",
			Help: "Approximate WAL on-disk size in bytes (LSM + value log).",
		}),
		OldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "wal",
			Name: "oldest_age_seconds",
			Help: "Age of the oldest live WAL entry in seconds; 0 when empty.",
		}),
		Appended: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "wal",
			Name: "appended_total",
			Help: "WAL entries written.",
		}),
		Drained: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "wal",
			Name: "drained_total",
			Help: "WAL entries successfully read and deleted by the flusher.",
		}),
		Expired: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "wal",
			Name: "expired_total",
			Help: "WAL entries dropped because they exceeded the configured TTL.",
		}),
	}
	reg.MustRegister(m.Entries, m.BytesOnDisk, m.OldestAge, m.Appended, m.Drained, m.Expired)
	return m
}

// SubscriberMetrics is the metric set the MQTT subscriber exports.
type SubscriberMetrics struct {
	Connected     prometheus.Gauge   // 1 if currently connected, 0 otherwise
	Paused        prometheus.Gauge   // 1 when ring is at pause threshold AND WAL spill failed (we are not acking)
	Pauses        prometheus.Counter // number of pause events (transitions into Paused=1)
	Reconnects    prometheus.Counter // reconnection attempts (successful or not)
	Received      prometheus.Counter // messages delivered to our handler
	Acked         prometheus.Counter // messages we manually acked
	HandlerErrors prometheus.Counter // handler-level errors (parse failures, etc.)
}

// NewSubscriberMetrics registers the subscriber metric set on reg.
func NewSubscriberMetrics(reg prometheus.Registerer) *SubscriberMetrics {
	m := &SubscriberMetrics{
		Connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "connected", Help: "1 when connected to the MQTT broker, 0 otherwise.",
		}),
		Paused: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "paused", Help: "1 when the subscriber is dropping unacked messages so the broker redelivers (ring full + no WAL headroom).",
		}),
		Pauses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "pauses_total", Help: "Pause events (transitions from acking to not acking).",
		}),
		Reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "reconnects_total", Help: "MQTT reconnection attempts.",
		}),
		Received: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "received_total", Help: "Messages delivered to the subscriber's handler.",
		}),
		Acked: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "acked_total", Help: "Messages manually acknowledged after persistence.",
		}),
		HandlerErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "subscriber",
			Name: "handler_errors_total", Help: "Handler-level errors (topic parse failures, encode errors).",
		}),
	}
	reg.MustRegister(m.Connected, m.Paused, m.Pauses, m.Reconnects, m.Received, m.Acked, m.HandlerErrors)
	return m
}

// FlusherMetrics is the metric set the adaptive flusher exports.
type FlusherMetrics struct {
	BatchSize    prometheus.Gauge     // currently active batch size (mode-aware)
	Mode         prometheus.Gauge     // 0=normal, 1=elevated, 2=critical
	FlushLatency prometheus.Histogram // seconds per Postgres flush
	Flushed      prometheus.Counter   // batches flushed successfully
	Inserted     prometheus.Counter   // rows actually inserted (post-dedup)
	FlushErrors  prometheus.Counter   // batch flushes that errored
	RetriesTotal prometheus.Counter   // total per-batch retry attempts
	DeadLettered prometheus.Counter   // messages handed off to dead-letter
	Requeued     prometheus.Counter   // messages re-enqueued to the WAL after a transient terminal failure
}

// NewFlusherMetrics registers the flusher metric set on reg.
func NewFlusherMetrics(reg prometheus.Registerer) *FlusherMetrics {
	m := &FlusherMetrics{
		BatchSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "batch_size_current", Help: "Current effective batch size.",
		}),
		Mode: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "mode", Help: "Adaptive mode: 0=normal, 1=elevated, 2=critical.",
		}),
		FlushLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name:    "flush_latency_seconds",
			Help:    "Latency of a single Postgres flush call.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms .. ~10s
		}),
		Flushed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "flushed_total", Help: "Batches flushed without error.",
		}),
		Inserted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "inserted_total", Help: "Rows actually inserted (post ON CONFLICT DO NOTHING).",
		}),
		FlushErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "flush_errors_total", Help: "Batch flushes that errored.",
		}),
		RetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "retries_total", Help: "Total batch retry attempts (across messages).",
		}),
		DeadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "dead_lettered_total", Help: "Messages handed off to the dead-letter sink.",
		}),
		Requeued: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "flusher",
			Name: "requeued_total", Help: "Messages re-enqueued to the WAL after a transient terminal flush failure.",
		}),
	}
	reg.MustRegister(m.BatchSize, m.Mode, m.FlushLatency, m.Flushed, m.Inserted,
		m.FlushErrors, m.RetriesTotal, m.DeadLettered, m.Requeued)
	return m
}

// DeadLetterMetrics is the metric set for the S3-compatible dead-letter sink.
type DeadLetterMetrics struct {
	Written prometheus.Counter
	Errors  prometheus.Counter
	Latency prometheus.Histogram
}

// NewDeadLetterMetrics registers the dead-letter metric set on reg.
func NewDeadLetterMetrics(reg prometheus.Registerer) *DeadLetterMetrics {
	m := &DeadLetterMetrics{
		Written: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "deadletter",
			Name: "written_total", Help: "Messages written to the dead-letter sink.",
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "deadletter",
			Name: "errors_total", Help: "Dead-letter write attempts that failed.",
		}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Subsystem: "deadletter",
			Name:    "write_latency_seconds",
			Help:    "Latency of a single dead-letter write.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}),
	}
	reg.MustRegister(m.Written, m.Errors, m.Latency)
	return m
}

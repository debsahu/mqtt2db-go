// Package wal is the on-disk overflow log for messages that have been
// accepted by the MQTT subscriber but not yet persisted to PostgreSQL.
//
// Role: when the in-memory ring buffer hits the spill threshold, the
// subscriber routes new messages here instead of dropping them. The
// flusher drains this WAL alongside the ring (ring first, WAL as headroom
// allows), and any message still in the WAL when its TTL expires is
// dropped with a metric (no other behavior — Comqtt redelivery is the
// recovery path under `clean_session=false`).
//
// Durability: this is *not* the durability boundary. CLAUDE.md is explicit
// that PostgreSQL owns durability; the WAL is throughput smoothing. We
// configure SyncWrites by default anyway because the cost is small for
// our throughput target and crashes that lose buffered messages still
// recover via Comqtt redelivery.
//
// Storage: Badger v4. Keys are `msg:{nanos:020d}:{seq:020d}` so iteration
// is time-ordered and rolling restarts replay messages in roughly the
// same order they arrived. The 020d zero-padding keeps lexicographic
// ordering aligned with numeric ordering — without it "msg:9..." sorts
// after "msg:10..." and replay order goes wrong.
package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
)

// keyPrefix groups all WAL entries under a single Badger prefix so we can
// scan a contiguous range and so we have headroom for sibling key spaces
// later (e.g. retry counters in Milestone 8).
const keyPrefix = "msg:"

// Config configures a Store. Path is mandatory; TTL <= 0 disables expiry.
type Config struct {
	Path             string
	TTL              time.Duration
	SyncWrites       bool
	ValueLogFileSize int64
}

// Store is the WAL handle. It is safe for concurrent Append calls and a
// single Drain caller; Drain is not designed for parallel consumers
// because each replica owns its own WAL volume.
type Store struct {
	db  *badger.DB
	cfg Config
	m   *metrics.WALMetrics
	// seq breaks ties when two Appends arrive within the same nanosecond.
	// time-ordered key construction stays correct regardless of clock skew
	// across replicas because each replica writes to its own WAL.
	seq atomic.Uint64
}

// Open opens or creates a Badger database at cfg.Path. Callers must call
// Close on shutdown. The Badger logger is silenced; Badger's own logs are
// noisy at info level and would drown out the service's slog output.
func Open(cfg Config, m *metrics.WALMetrics) (*Store, error) {
	if cfg.Path == "" {
		return nil, errors.New("wal: Path required")
	}
	if m == nil {
		return nil, errors.New("wal: metrics required")
	}
	opts := badger.DefaultOptions(cfg.Path).
		WithSyncWrites(cfg.SyncWrites).
		WithLogger(nil) // silence Badger's INFO chatter
	if cfg.ValueLogFileSize > 0 {
		opts = opts.WithValueLogFileSize(cfg.ValueLogFileSize)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger %q: %w", cfg.Path, err)
	}
	return &Store{db: db, cfg: cfg, m: m}, nil
}

// Close flushes and closes the underlying Badger DB.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Append writes msg to the WAL with a key derived from time.Now() plus a
// monotonic suffix. Returns the assigned key so the caller can correlate
// metrics or trace logs.
func (s *Store) Append(msg postgres.Message) ([]byte, error) {
	key := s.makeKey(time.Now())
	value, err := encodeMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, value)
		if s.cfg.TTL > 0 {
			entry = entry.WithTTL(s.cfg.TTL)
		}
		return txn.SetEntry(entry)
	})
	if err != nil {
		return nil, fmt.Errorf("badger set: %w", err)
	}
	s.m.Appended.Inc()
	return key, nil
}

// Drain returns up to limit oldest entries and deletes them in the same
// transaction. Returned slice is in insertion order. Returns (nil, nil)
// when the WAL is empty.
//
// Drain assumes a single caller. Two parallel drains would not corrupt
// data (Badger transactions are MVCC-safe) but would compete for the
// same key range and waste CPU.
func (s *Store) Drain(limit int) ([]postgres.Message, error) {
	if limit <= 0 {
		return nil, nil
	}

	out := make([]postgres.Message, 0, limit)
	keys := make([][]byte, 0, limit)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(keyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
			item := it.Item()
			// Skip expired entries; Badger surfaces them in iteration only
			// briefly during compaction windows but defensive code is cheap.
			if item.IsDeletedOrExpired() {
				continue
			}
			err := item.Value(func(val []byte) error {
				msg, decErr := decodeMessage(val)
				if decErr != nil {
					return decErr
				}
				out = append(out, msg)
				return nil
			})
			if err != nil {
				return err
			}
			keys = append(keys, item.KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	// Delete the read entries in a write transaction. We do this after
	// the read so a flusher crash mid-flush leaves entries replayable on
	// restart. The flusher must therefore be idempotent — and it is,
	// because telemetry.dedup_key is unique.
	err = s.db.Update(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("delete: %w", err)
	}

	s.m.Drained.Add(float64(len(out)))
	return out, nil
}

// Len returns an approximate live-entry count. It walks key-only iteration
// over the WAL prefix; cheap on small WALs and acceptable for the periodic
// metric refresh path.
func (s *Store) Len() (int, error) {
	var n int
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(keyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if !it.Item().IsDeletedOrExpired() {
				n++
			}
		}
		return nil
	})
	return n, err
}

// RefreshMetrics updates entries / bytes_on_disk / oldest_age gauges. Call
// from a periodic ticker — it walks the prefix so it isn't free.
func (s *Store) RefreshMetrics() error {
	n, err := s.Len()
	if err != nil {
		return err
	}
	s.m.Entries.Set(float64(n))

	lsm, vlog := s.db.Size()
	s.m.BytesOnDisk.Set(float64(lsm + vlog))

	oldest, err := s.oldestEntryTime()
	if err != nil {
		return err
	}
	if oldest.IsZero() {
		s.m.OldestAge.Set(0)
	} else {
		s.m.OldestAge.Set(time.Since(oldest).Seconds())
	}
	return nil
}

// oldestEntryTime returns the timestamp encoded in the smallest live key,
// or the zero time if the WAL is empty.
func (s *Store) oldestEntryTime() (time.Time, error) {
	var ts time.Time
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(keyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if it.Item().IsDeletedOrExpired() {
				continue
			}
			nanos, ok := parseKeyTime(it.Item().Key())
			if ok {
				ts = time.Unix(0, nanos)
			}
			return nil
		}
		return nil
	})
	return ts, err
}

// makeKey returns msg:{nanos:020d}:{seq:020d}.
func (s *Store) makeKey(at time.Time) []byte {
	seq := s.seq.Add(1)
	// Hand-formatted big-endian text to keep lexicographic ordering aligned
	// with numeric ordering. fmt.Sprintf would also work; this is just a
	// hot path so we avoid the formatter.
	var b bytes.Buffer
	b.Grow(len(keyPrefix) + 20 + 1 + 20)
	b.WriteString(keyPrefix)
	writePadded20(&b, uint64(at.UnixNano()))
	b.WriteByte(':')
	writePadded20(&b, seq)
	return b.Bytes()
}

func writePadded20(b *bytes.Buffer, n uint64) {
	const width = 20
	var raw [width]byte
	for i := width - 1; i >= 0; i-- {
		raw[i] = byte('0' + n%10)
		n /= 10
	}
	b.Write(raw[:])
}

// parseKeyTime returns the nanos encoded in a key, or (0,false) if the key
// is malformed.
func parseKeyTime(key []byte) (int64, bool) {
	if len(key) < len(keyPrefix)+20+1 {
		return 0, false
	}
	if !bytes.HasPrefix(key, []byte(keyPrefix)) {
		return 0, false
	}
	nanosBytes := key[len(keyPrefix) : len(keyPrefix)+20]
	var nanos int64
	for _, c := range nanosBytes {
		if c < '0' || c > '9' {
			return 0, false
		}
		nanos = nanos*10 + int64(c-'0')
	}
	return nanos, true
}

// encodeMessage serialises a postgres.Message to bytes via gob. Gob is the
// path of least resistance: stdlib, schema-flexible, and faster than JSON
// for binary payloads. We do not pretend to support cross-language WAL
// readers; this is a private on-disk format.
func encodeMessage(m postgres.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(walRecord{
		TenantID:   m.TenantID,
		DeviceUUID: m.DeviceUUID[:],
		Topic:      m.Topic,
		Payload:    m.Payload,
		ReceivedAt: m.ReceivedAt,
		DedupKey:   m.DedupKey,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMessage(b []byte) (postgres.Message, error) {
	var rec walRecord
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&rec); err != nil {
		return postgres.Message{}, err
	}
	if len(rec.DeviceUUID) != 16 {
		return postgres.Message{}, fmt.Errorf("wal: bad uuid length %d", len(rec.DeviceUUID))
	}
	var msg postgres.Message
	msg.TenantID = rec.TenantID
	copy(msg.DeviceUUID[:], rec.DeviceUUID)
	msg.Topic = rec.Topic
	msg.Payload = rec.Payload
	msg.ReceivedAt = rec.ReceivedAt
	msg.DedupKey = rec.DedupKey
	return msg, nil
}

// walRecord is the gob shape on disk. Defined separately so changes to
// postgres.Message don't accidentally break disk compatibility.
type walRecord struct {
	TenantID   string
	DeviceUUID []byte // 16 raw bytes
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
	DedupKey   string
}

// encodeUUID is here to keep encoding/binary referenced; future binary
// formats may use it. Avoids "imported and not used" if we later remove
// gob in favor of custom encoding.
var _ = binary.BigEndian

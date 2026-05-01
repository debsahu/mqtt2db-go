package wal_test

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

	"github.com/debsahu/mqtt2db-go/internal/metrics"
	"github.com/debsahu/mqtt2db-go/internal/postgres"
	"github.com/debsahu/mqtt2db-go/internal/wal"
)

func newStore(t *testing.T, ttl time.Duration) (*wal.Store, *metrics.WALMetrics) {
	t.Helper()
	dir := t.TempDir()
	reg := metrics.NewRegistry()
	m := metrics.NewWALMetrics(reg)
	s, err := wal.Open(wal.Config{
		Path:             dir,
		TTL:              ttl,
		SyncWrites:       false, // tests don't need fsync overhead
		ValueLogFileSize: 1 << 26,
	}, m)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s, m
}

func makeMsg(seq int) postgres.Message {
	return postgres.Message{
		TenantID:   "acme",
		DeviceUUID: uuid.New(),
		Topic:      "t/acme/d/x/evt/state",
		Payload:    []byte(fmt.Sprintf(`{"seq":%d}`, seq)),
		ReceivedAt: time.Now().UTC().Truncate(time.Microsecond),
		DedupKey:   fmt.Sprintf("acme:%d", seq),
	}
}

func TestOpen_RejectsEmptyPath(t *testing.T) {
	reg := metrics.NewRegistry()
	m := metrics.NewWALMetrics(reg)
	_, err := wal.Open(wal.Config{Path: ""}, m)
	require.Error(t, err)

	_, err = wal.Open(wal.Config{Path: t.TempDir()}, nil)
	require.Error(t, err)
}

func TestAppendDrain_PreservesOrder(t *testing.T) {
	s, m := newStore(t, 0)

	for i := 0; i < 50; i++ {
		_, err := s.Append(makeMsg(i))
		require.NoError(t, err)
	}
	assert.Equal(t, float64(50), testutil.ToFloat64(m.Appended))

	out, err := s.Drain(50)
	require.NoError(t, err)
	require.Len(t, out, 50)
	for i, msg := range out {
		assert.Equal(t, fmt.Sprintf("acme:%d", i), msg.DedupKey, "i=%d", i)
		assert.Equal(t, []byte(fmt.Sprintf(`{"seq":%d}`, i)), msg.Payload)
	}
	assert.Equal(t, float64(50), testutil.ToFloat64(m.Drained))

	// Drain again — empty.
	out, err = s.Drain(50)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestDrain_RespectsLimit(t *testing.T) {
	s, _ := newStore(t, 0)
	for i := 0; i < 20; i++ {
		_, err := s.Append(makeMsg(i))
		require.NoError(t, err)
	}

	first, err := s.Drain(7)
	require.NoError(t, err)
	require.Len(t, first, 7)

	second, err := s.Drain(7)
	require.NoError(t, err)
	require.Len(t, second, 7)

	rest, err := s.Drain(100)
	require.NoError(t, err)
	require.Len(t, rest, 6)

	// Order across the three drains is contiguous.
	all := append(append(first, second...), rest...)
	for i, m := range all {
		assert.Equal(t, fmt.Sprintf("acme:%d", i), m.DedupKey)
	}
}

func TestDrain_ZeroLimitReturnsEmpty(t *testing.T) {
	s, _ := newStore(t, 0)
	_, err := s.Append(makeMsg(0))
	require.NoError(t, err)

	out, err := s.Drain(0)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestRestart_PersistsAcrossClose(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write 10 messages, close.
	{
		reg := metrics.NewRegistry()
		m := metrics.NewWALMetrics(reg)
		s, err := wal.Open(wal.Config{Path: dir, SyncWrites: true}, m)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			_, err := s.Append(makeMsg(i))
			require.NoError(t, err)
		}
		require.NoError(t, s.Close())
	}

	// Phase 2: reopen, drain — entries survive.
	{
		reg := metrics.NewRegistry()
		m := metrics.NewWALMetrics(reg)
		s, err := wal.Open(wal.Config{Path: dir, SyncWrites: true}, m)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		out, err := s.Drain(100)
		require.NoError(t, err)
		require.Len(t, out, 10)
		for i, msg := range out {
			assert.Equal(t, fmt.Sprintf("acme:%d", i), msg.DedupKey)
		}
	}
}

func TestTTL_ExpiresEntries(t *testing.T) {
	// Tight TTL: entries expire within 200ms.
	s, m := newStore(t, 100*time.Millisecond)
	for i := 0; i < 5; i++ {
		_, err := s.Append(makeMsg(i))
		require.NoError(t, err)
	}

	// Wait past TTL, then attempt drain — Badger surfaces them as expired
	// and our iterator skips IsDeletedOrExpired.
	time.Sleep(300 * time.Millisecond)

	out, err := s.Drain(100)
	require.NoError(t, err)
	assert.Empty(t, out, "expired entries must not be drained")
	// Drained counter stays at 0; expiry is silent at this layer (callers
	// own the periodic Expired counter via RefreshMetrics).
	assert.Equal(t, float64(0), testutil.ToFloat64(m.Drained))
}

func TestRefreshMetrics_TracksEntriesAndOldestAge(t *testing.T) {
	s, m := newStore(t, 0)

	require.NoError(t, s.RefreshMetrics())
	assert.Equal(t, float64(0), testutil.ToFloat64(m.Entries))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.OldestAge))

	for i := 0; i < 7; i++ {
		_, err := s.Append(makeMsg(i))
		require.NoError(t, err)
	}
	// Sleep a touch so OldestAge is observably > 0.
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, s.RefreshMetrics())
	assert.Equal(t, float64(7), testutil.ToFloat64(m.Entries))
	assert.Greater(t, testutil.ToFloat64(m.OldestAge), float64(0))
	// BytesOnDisk reflects LSM + value log; small writes can sit in the
	// memtable for a while, so we only assert the gauge is non-negative.
	assert.GreaterOrEqual(t, testutil.ToFloat64(m.BytesOnDisk), float64(0))
}

func TestAppend_Concurrent(t *testing.T) {
	s, m := newStore(t, 0)

	const writers = 4
	const perWriter = 250
	var wg sync.WaitGroup
	wg.Add(writers)
	var seq atomic.Int64
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				n := int(seq.Add(1))
				_, err := s.Append(makeMsg(n))
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, float64(writers*perWriter), testutil.ToFloat64(m.Appended))

	out, err := s.Drain(writers * perWriter)
	require.NoError(t, err)
	assert.Len(t, out, writers*perWriter)
}

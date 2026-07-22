package store

import (
	"fmt"
	"os"
	"testing"
)

func TestFlushBloomIndexAndCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "quorumdb-store-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 6000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		if err := s.Put(key, []byte(fmt.Sprintf("value-%d", i)), int64(i)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	if s.SegmentCount() < 2 {
		t.Fatalf("expected multiple segments from flushing %d keys, got %d", n, s.SegmentCount())
	}

	// Every key should be readable, whether it ended up in the memtable or an
	// older flushed segment — this exercises the bloom filter + sparse index
	// lookup path, not just an in-memory hit.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		e, found := s.Get(key)
		if !found {
			t.Fatalf("key %s not found", key)
		}
		want := fmt.Sprintf("value-%d", i)
		if string(e.Value) != want {
			t.Fatalf("key %s: got %q want %q", key, e.Value, want)
		}
	}

	// A key that was never written must be definitively absent — this is
	// the case the bloom filter should reject without a disk scan.
	if _, found := s.Get("never-written-key"); found {
		t.Fatal("expected missing key to be reported absent")
	}

	// Delete half the keys, then compact, and confirm: deleted keys gone,
	// surviving keys still correct, and the segment count collapses to 1.
	for i := 0; i < n; i += 2 {
		key := fmt.Sprintf("key-%05d", i)
		if err := s.Delete(key, int64(n+i)); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if s.SegmentCount() != 1 {
		t.Fatalf("expected 1 segment after compaction, got %d", s.SegmentCount())
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		_, found := s.Get(key)
		if i%2 == 0 {
			if found {
				t.Fatalf("key %s should be deleted after compaction", key)
			}
		} else {
			if !found {
				t.Fatalf("key %s should survive compaction", key)
			}
		}
	}
}

func TestRestartDurabilityWithSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "quorumdb-store-restart-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1500; i++ {
		key := fmt.Sprintf("k-%05d", i)
		if err := s.Put(key, []byte("v"), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	segCountBefore := s.SegmentCount()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen against the same directory: segments must reload from their
	// .bloom/.idx sidecars (not rebuilt by scanning), and all data must
	// still be readable.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if s2.SegmentCount() != segCountBefore {
		t.Fatalf("segment count changed across restart: before=%d after=%d", segCountBefore, s2.SegmentCount())
	}

	for i := 0; i < 1500; i++ {
		key := fmt.Sprintf("k-%05d", i)
		if _, found := s2.Get(key); !found {
			t.Fatalf("key %s missing after restart", key)
		}
	}
}

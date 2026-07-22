// Package store implements a single-node LSM-style storage engine:
// writes go to a write-ahead log (durability) and an in-memory memtable
// (speed); once the memtable crosses a size threshold it is flushed to an
// immutable, key-sorted segment file (with a bloom filter + sparse index)
// on disk, and reads fall through memtable -> segments (newest first).
// Compact() merges all segments into one, dropping tombstones and
// superseded versions.
package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Entry struct {
	Value     []byte
	Timestamp int64 // unix nanos, used for last-write-wins conflict resolution
	Tombstone bool
}

type Store struct {
	mu           sync.RWMutex
	dir          string
	memtable     map[string]Entry
	memtableSize int
	flushAt      int
	wal          *os.File
	walEnc       *bufio.Writer
	segments     []*segmentHandle // oldest first
	segCounter   int
}

const flushThresholdBytes = 64 * 1024 // small on purpose so a demo can trigger flushes quickly

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	s := &Store{
		dir:      dir,
		memtable: make(map[string]Entry),
		flushAt:  flushThresholdBytes,
	}

	if err := s.loadSegmentList(); err != nil {
		return nil, err
	}
	if err := s.replayWAL(); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.wal = f
	s.walEnc = bufio.NewWriter(f)

	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.walEnc != nil {
		_ = s.walEnc.Flush()
	}
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

// Put writes key=value with the given timestamp (used for LWW across replicas).
func (s *Store) Put(key string, value []byte, ts int64) error {
	return s.write(key, value, ts, false)
}

func (s *Store) Delete(key string, ts int64) error {
	return s.write(key, nil, ts, true)
}

func (s *Store) write(key string, value []byte, ts int64, tombstone bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.memtable[key]; ok && existing.Timestamp > ts {
		// A newer write already won (last-write-wins); ignore the stale one.
		return nil
	}

	if err := writeRecord(s.walEnc, key, value, ts, tombstone); err != nil {
		return err
	}
	if err := s.walEnc.Flush(); err != nil {
		return err
	}

	s.memtable[key] = Entry{Value: value, Timestamp: ts, Tombstone: tombstone}
	s.memtableSize += len(key) + len(value) + 16

	if s.memtableSize >= s.flushAt {
		if err := s.flushLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Get returns the value and true if found and not deleted, false if absent
// or tombstoned.
func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if e, ok := s.memtable[key]; ok {
		return e, !e.Tombstone
	}

	for i := len(s.segments) - 1; i >= 0; i-- {
		if e, found := s.segments[i].get(key); found {
			return e, !e.Tombstone
		}
	}
	return Entry{}, false
}

// SegmentCount reports how many on-disk segments currently exist (useful for
// tests/metrics to observe flush and compaction behavior).
func (s *Store) SegmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segments)
}

func (s *Store) flushLocked() error {
	s.segCounter++
	segPath := segmentPath(s.dir, s.segCounter)

	keys := make([]string, 0, len(s.memtable))
	for k := range s.memtable {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	handle, err := writeSegment(segPath, keys, s.memtable)
	if err != nil {
		return err
	}

	s.segments = append(s.segments, handle)
	s.memtable = make(map[string]Entry)
	s.memtableSize = 0

	// Reset the WAL: everything in it is now durable in a segment file.
	// Closing and recreating the file (rather than Truncate+Seek on the
	// existing handle) avoids a Windows-specific "Access is denied" when
	// truncating a file opened O_APPEND.
	walPath := filepath.Join(s.dir, "wal.log")
	if err := s.wal.Close(); err != nil {
		return err
	}
	newWal, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	s.wal = newWal
	s.walEnc = bufio.NewWriter(s.wal)

	return nil
}

func segmentPath(dir string, counter int) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%05d.dat", counter))
}

func (s *Store) loadSegmentList() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".dat" {
			paths = append(paths, filepath.Join(s.dir, e.Name()))
		}
	}
	sort.Strings(paths) // segment-00001.dat < segment-00002.dat lexicographically

	for _, p := range paths {
		handle, err := loadSegmentHandle(p)
		if err != nil {
			return fmt.Errorf("loading segment %s: %w", p, err)
		}
		s.segments = append(s.segments, handle)
	}
	s.segCounter = len(paths)
	return nil
}

func (s *Store) replayWAL() error {
	walPath := filepath.Join(s.dir, "wal.log")
	f, err := os.Open(walPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		key, value, ts, tombstone, err := readRecord(r)
		if err != nil {
			// EOF or a truncated final record (crash mid-write) — either way,
			// replay stops here; a truncated record was never fsync'd as complete.
			break
		}
		s.memtable[key] = Entry{Value: value, Timestamp: ts, Tombstone: tombstone}
	}
	return nil
}

// --- on-disk record format: [keyLen uint32][key][tombstone byte][ts int64][valueLen uint32][value] ---

func writeRecord(w *bufio.Writer, key string, value []byte, ts int64, tombstone bool) error {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(len(key)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.WriteString(key); err != nil {
		return err
	}

	if tombstone {
		if _, err := w.Write([]byte{1}); err != nil {
			return err
		}
	} else {
		if _, err := w.Write([]byte{0}); err != nil {
			return err
		}
	}

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ts))
	if _, err := w.Write(tsBuf[:]); err != nil {
		return err
	}

	binary.BigEndian.PutUint32(buf[:], uint32(len(value)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if len(value) > 0 {
		if _, err := w.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func readRecord(r *bufio.Reader) (key string, value []byte, ts int64, tombstone bool, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return
	}
	keyLen := binary.BigEndian.Uint32(lenBuf[:])

	keyBytes := make([]byte, keyLen)
	if _, err = io.ReadFull(r, keyBytes); err != nil {
		return
	}
	key = string(keyBytes)

	var tomb [1]byte
	if _, err = io.ReadFull(r, tomb[:]); err != nil {
		return
	}
	tombstone = tomb[0] == 1

	var tsBuf [8]byte
	if _, err = io.ReadFull(r, tsBuf[:]); err != nil {
		return
	}
	ts = int64(binary.BigEndian.Uint64(tsBuf[:]))

	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return
	}
	valLen := binary.BigEndian.Uint32(lenBuf[:])
	if valLen > 0 {
		value = make([]byte, valLen)
		if _, err = io.ReadFull(r, value); err != nil {
			return
		}
	}
	return
}

func Now() int64 {
	return time.Now().UnixNano()
}

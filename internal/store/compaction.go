package store

import (
	"bufio"
	"fmt"
	"os"
	"sort"
)

// Compact merges every on-disk segment into a single new segment, keeping
// only the newest entry per key (last-write-wins) and dropping tombstones
// entirely — a real system would keep tombstones for a grace period to
// avoid resurrecting a deleted key via a stale replica's read-repair, but
// that's an explicit simplification here (see README known-boundaries).
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.segments) <= 1 {
		return nil // nothing meaningful to merge
	}

	merged := make(map[string]Entry)
	for _, seg := range s.segments {
		if err := scanSegmentInto(seg.dataPath, merged); err != nil {
			return fmt.Errorf("scanning segment %s: %w", seg.dataPath, err)
		}
	}

	live := make(map[string]Entry)
	keys := make([]string, 0, len(merged))
	for k, e := range merged {
		if e.Tombstone {
			continue
		}
		live[k] = e
		keys = append(keys, k)
	}
	sort.Strings(keys)

	s.segCounter++
	newPath := segmentPath(s.dir, s.segCounter)
	newHandle, err := writeSegment(newPath, keys, live)
	if err != nil {
		return err
	}

	oldSegments := s.segments
	s.segments = []*segmentHandle{newHandle}

	for _, old := range oldSegments {
		_ = os.Remove(old.dataPath)
		_ = os.Remove(old.dataPath + ".bloom")
		_ = os.Remove(old.dataPath + ".idx")
	}

	return nil
}

func scanSegmentInto(path string, into map[string]Entry) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		key, value, ts, tombstone, err := readRecord(r)
		if err != nil {
			break
		}
		existing, ok := into[key]
		if !ok || ts > existing.Timestamp {
			into[key] = Entry{Value: value, Timestamp: ts, Tombstone: tombstone}
		}
	}
	return nil
}

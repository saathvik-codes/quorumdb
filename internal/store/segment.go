package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
)

const sparseIndexInterval = 16 // one index entry per N records

type indexEntry struct {
	key    string
	offset int64
}

// segmentHandle is a segment file plus its in-memory bloom filter and sparse
// index, so a point lookup is: bloom check (usually rejects instantly) ->
// binary search the sparse index -> bounded scan of one small window on
// disk, instead of a linear scan of the whole file.
type segmentHandle struct {
	dataPath string
	bloom    *Bloom
	index    []indexEntry // sorted by key
	numKeys  int
}

// writeSegment writes entries (already sorted by key) to dataPath, building
// the bloom filter and sparse index alongside, and persisting both as
// sidecar files so a restart doesn't need to rebuild them by rescanning.
func writeSegment(dataPath string, sortedKeys []string, entries map[string]Entry) (*segmentHandle, error) {
	f, err := os.Create(dataPath)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)

	bloom := NewBloom(max(len(sortedKeys), 1), 0.01)
	var index []indexEntry
	var offset int64

	for i, k := range sortedKeys {
		e := entries[k]
		if i%sparseIndexInterval == 0 {
			index = append(index, indexEntry{key: k, offset: offset})
		}
		n, err := writeRecordCounting(w, k, e)
		if err != nil {
			f.Close()
			return nil, err
		}
		offset += n
		bloom.Add(k)
	}

	if err := w.Flush(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	handle := &segmentHandle{dataPath: dataPath, bloom: bloom, index: index, numKeys: len(sortedKeys)}
	if err := persistSidecars(dataPath, handle); err != nil {
		return nil, err
	}
	return handle, nil
}

func persistSidecars(dataPath string, h *segmentHandle) error {
	if err := os.WriteFile(dataPath+".bloom", h.bloom.Serialize(), 0o644); err != nil {
		return err
	}

	idxFile, err := os.Create(dataPath + ".idx")
	if err != nil {
		return err
	}
	defer idxFile.Close()
	w := bufio.NewWriter(idxFile)
	for _, e := range h.index {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(e.key)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.WriteString(e.key); err != nil {
			return err
		}
		var offBuf [8]byte
		binary.BigEndian.PutUint64(offBuf[:], uint64(e.offset))
		if _, err := w.Write(offBuf[:]); err != nil {
			return err
		}
	}
	return w.Flush()
}

// loadSegmentHandle loads the bloom filter and sparse index sidecars for an
// existing segment file (used on startup so a restart doesn't rescan data).
func loadSegmentHandle(dataPath string) (*segmentHandle, error) {
	bloomBytes, err := os.ReadFile(dataPath + ".bloom")
	if err != nil {
		return nil, err
	}
	bloom := DeserializeBloom(bloomBytes)

	idxBytes, err := os.ReadFile(dataPath + ".idx")
	if err != nil {
		return nil, err
	}
	var index []indexEntry
	r := bufio.NewReader(bytes.NewReader(idxBytes))
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			break
		}
		keyLen := binary.BigEndian.Uint32(lenBuf[:])
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			break
		}
		var offBuf [8]byte
		if _, err := io.ReadFull(r, offBuf[:]); err != nil {
			break
		}
		index = append(index, indexEntry{key: string(keyBytes), offset: int64(binary.BigEndian.Uint64(offBuf[:]))})
	}

	return &segmentHandle{dataPath: dataPath, bloom: bloom, index: index}, nil
}

// get performs bloom-filtered, index-assisted lookup: reject instantly if
// the bloom filter says "definitely not present", otherwise binary-search
// the sparse index for the nearest window and scan only that window.
func (h *segmentHandle) get(key string) (Entry, bool) {
	if !h.bloom.MightContain(key) {
		return Entry{}, false
	}
	if len(h.index) == 0 {
		return Entry{}, false
	}

	// Find the last index entry whose key <= target key: that's where the
	// window containing key (if present) starts.
	i := sort.Search(len(h.index), func(i int) bool { return h.index[i].key > key })
	if i == 0 {
		return Entry{}, false
	}
	startOffset := h.index[i-1].offset

	f, err := os.Open(h.dataPath)
	if err != nil {
		return Entry{}, false
	}
	defer f.Close()
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return Entry{}, false
	}

	r := bufio.NewReader(f)
	for scanned := 0; scanned < sparseIndexInterval+1; scanned++ {
		k, value, ts, tombstone, err := readRecord(r)
		if err != nil {
			return Entry{}, false
		}
		if k == key {
			return Entry{Value: value, Timestamp: ts, Tombstone: tombstone}, true
		}
		if k > key {
			return Entry{}, false
		}
	}
	return Entry{}, false
}

func writeRecordCounting(w *bufio.Writer, key string, e Entry) (int64, error) {
	size := int64(4 + len(key) + 1 + 8 + 4 + len(e.Value))
	return size, writeRecord(w, key, e.Value, e.Timestamp, e.Tombstone)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

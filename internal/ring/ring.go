// Package ring implements consistent hashing with virtual nodes so that
// adding or removing a node only reshuffles a small fraction of keys,
// instead of remapping the whole keyspace (the classic mod-N problem).
package ring

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// HashRing maps keys to owning nodes via consistent hashing.
type HashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	sortedHashes []uint64
	hashToNode   map[uint64]string
	nodes        map[string]bool
}

func New(virtualNodesPerNode int) *HashRing {
	return &HashRing{
		virtualNodes: virtualNodesPerNode,
		hashToNode:   make(map[uint64]string),
		nodes:        make(map[string]bool),
	}
}

func hashKey(key string) uint64 {
	sum := sha1.Sum([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

// AddNode registers a node and its virtual replicas on the ring.
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}
	r.nodes[nodeID] = true

	for i := 0; i < r.virtualNodes; i++ {
		vKey := fmt.Sprintf("%s#%d", nodeID, i)
		h := hashKey(vKey)
		r.hashToNode[h] = nodeID
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

// RemoveNode evicts a node and all its virtual replicas.
func (r *HashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}
	delete(r.nodes, nodeID)

	filtered := r.sortedHashes[:0]
	for _, h := range r.sortedHashes {
		if r.hashToNode[h] == nodeID {
			delete(r.hashToNode, h)
			continue
		}
		filtered = append(filtered, h)
	}
	r.sortedHashes = filtered
}

// PreferenceList returns the N distinct physical nodes responsible for key,
// walking clockwise around the ring starting from key's hash position.
func (r *HashRing) PreferenceList(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0
	}

	seen := make(map[string]bool)
	var result []string
	for i := 0; i < len(r.sortedHashes) && len(result) < n; i++ {
		pos := (idx + i) % len(r.sortedHashes)
		node := r.hashToNode[r.sortedHashes[pos]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}

func (r *HashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

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

// HashRing maps keys to owning nodes via consistent hashing, and is
// topology-aware: PreferenceList spreads replicas across distinct zones
// (racks/AZs) when enough zones are available, so a single zone failure
// can't take out every replica of a key.
type HashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	sortedHashes []uint64
	hashToNode   map[uint64]string
	nodes        map[string]bool
	nodeZone     map[string]string
}

func New(virtualNodesPerNode int) *HashRing {
	return &HashRing{
		virtualNodes: virtualNodesPerNode,
		hashToNode:   make(map[uint64]string),
		nodes:        make(map[string]bool),
		nodeZone:     make(map[string]string),
	}
}

func hashKey(key string) uint64 {
	sum := sha1.Sum([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

// AddNode registers a node (and its virtual replicas) on the ring, tagged
// with the failure domain (rack/AZ/zone) it lives in. An empty zone is
// treated as its own singleton zone equal to the node ID, so placement
// still works (degrading gracefully to "no topology info") if callers don't
// supply one.
func (r *HashRing) AddNode(nodeID, zone string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}
	r.nodes[nodeID] = true
	if zone == "" {
		zone = nodeID
	}
	r.nodeZone[nodeID] = zone

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
	delete(r.nodeZone, nodeID)

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

// PreferenceList returns the N physical nodes responsible for key, walking
// clockwise around the ring from key's hash position and preferring nodes
// from zones not yet represented in the result — so with enough zones, N
// replicas land in N distinct failure domains instead of clustering in one.
// If there aren't enough distinct zones for N replicas, it falls back to
// filling remaining slots from any unseen node regardless of zone.
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

	seenNode := make(map[string]bool)
	seenZone := make(map[string]bool)
	var result []string
	var fallback []string // unseen nodes whose zone was already used

	for i := 0; i < len(r.sortedHashes) && len(result) < n; i++ {
		pos := (idx + i) % len(r.sortedHashes)
		node := r.hashToNode[r.sortedHashes[pos]]
		if seenNode[node] {
			continue
		}
		zone := r.nodeZone[node]
		if !seenZone[zone] {
			seenNode[node] = true
			seenZone[zone] = true
			result = append(result, node)
		} else if len(fallback) < n {
			seenNode[node] = true
			fallback = append(fallback, node)
		}
	}

	for i := 0; len(result) < n && i < len(fallback); i++ {
		result = append(result, fallback[i])
	}

	return result
}

// ZoneOf returns the failure-domain label for a node, or "" if unknown.
func (r *HashRing) ZoneOf(nodeID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodeZone[nodeID]
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

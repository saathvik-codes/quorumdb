package raft

import (
	"context"
	"errors"
	"sync"
)

// memoryCluster wires a set of raft.Node instances together via direct
// function calls (no network), so the algorithm itself can be tested fast
// and deterministically, independent of the HTTP transport used in
// production. Nodes can be marked "down" to simulate a crash/partition.
type memoryCluster struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	up    map[string]bool
}

func newMemoryCluster() *memoryCluster {
	return &memoryCluster{
		nodes: make(map[string]*Node),
		up:    make(map[string]bool),
	}
}

func (c *memoryCluster) register(id string, n *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[id] = n
	c.up[id] = true
}

func (c *memoryCluster) setUp(id string, up bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.up[id] = up
}

func (c *memoryCluster) isUp(id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.up[id]
}

func (c *memoryCluster) node(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

// memoryTransport is the Transport implementation used by one Node within
// a memoryCluster.
type memoryTransport struct {
	self    string
	cluster *memoryCluster
}

func (t *memoryTransport) SendRequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	if !t.cluster.isUp(t.self) || !t.cluster.isUp(peerID) {
		return RequestVoteReply{}, errors.New("peer unreachable")
	}
	target := t.cluster.node(peerID)
	if target == nil {
		return RequestVoteReply{}, errors.New("unknown peer")
	}
	return target.HandleRequestVote(args), nil
}

func (t *memoryTransport) SendAppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if !t.cluster.isUp(t.self) || !t.cluster.isUp(peerID) {
		return AppendEntriesReply{}, errors.New("peer unreachable")
	}
	target := t.cluster.node(peerID)
	if target == nil {
		return AppendEntriesReply{}, errors.New("unknown peer")
	}
	return target.HandleAppendEntries(args), nil
}

// memoryStorage is an in-memory Storage implementation (no real disk I/O
// needed for the algorithm tests; disk persistence is tested separately).
type memoryStorage struct {
	mu          sync.Mutex
	currentTerm int
	votedFor    string
	log         []LogEntry
	hasState    bool
}

func (s *memoryStorage) SaveState(currentTerm int, votedFor string, log []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = currentTerm
	s.votedFor = votedFor
	s.log = append([]LogEntry{}, log...)
	s.hasState = true
	return nil
}

func (s *memoryStorage) LoadState() (int, string, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasState {
		return 0, "", nil, errors.New("no saved state")
	}
	return s.currentTerm, s.votedFor, append([]LogEntry{}, s.log...), nil
}

func newTestCluster(ids []string) (*memoryCluster, map[string]*Node, map[string]*memoryStorage) {
	cluster := newMemoryCluster()
	nodes := make(map[string]*Node)
	storages := make(map[string]*memoryStorage)

	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		storage := &memoryStorage{}
		transport := &memoryTransport{self: id, cluster: cluster}
		node := NewNode(id, peers, transport, storage)
		nodes[id] = node
		storages[id] = storage
		cluster.register(id, node)
	}

	return cluster, nodes, storages
}

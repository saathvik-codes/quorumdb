package coordinator

import (
	"sync"
	"time"
)

// Hint is a write that couldn't reach its intended replica because that
// node was down at the time, held so it can be redelivered once the node
// comes back — this is Dynamo-style "sloppy quorum + hinted handoff".
//
// Simplification (see README known-boundaries): in a textbook implementation
// hints are stored durably on a *different live node* so they survive a
// coordinator crash. Here they're held in the coordinator process's memory
// for clarity of the mechanism; a coordinator restart drops in-flight hints
// (the data itself is not lost — it simply relies on the surviving W
// replicas plus eventual read-repair instead).
type Hint struct {
	Key       string
	Value     string
	Timestamp int64
	Tombstone bool
}

type HintStore struct {
	mu     sync.Mutex
	byNode map[string][]Hint
}

func NewHintStore() *HintStore {
	return &HintStore{byNode: make(map[string][]Hint)}
}

func (h *HintStore) Add(nodeID string, hint Hint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.byNode[nodeID] = append(h.byNode[nodeID], hint)
}

func (h *HintStore) PendingCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	total := 0
	for _, hints := range h.byNode {
		total += len(hints)
	}
	return total
}

func (h *HintStore) PendingByNode() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.byNode))
	for node, hints := range h.byNode {
		if len(hints) > 0 {
			out[node] = len(hints)
		}
	}
	return out
}

func (h *HintStore) drain(nodeID string) []Hint {
	h.mu.Lock()
	defer h.mu.Unlock()
	hints := h.byNode[nodeID]
	delete(h.byNode, nodeID)
	return hints
}

func (h *HintStore) requeue(nodeID string, hints []Hint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.byNode[nodeID] = append(hints, h.byNode[nodeID]...)
}

// StartHintedHandoff launches a background loop that, on every tick, checks
// every node with pending hints and — if that node is currently alive —
// replays its hints and clears them. A node that fails mid-replay keeps
// only the hints that didn't make it, so nothing is lost.
func (c *Coordinator) StartHintedHandoff(interval time.Duration) (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				c.deliverPendingHints()
			}
		}
	}()
	return func() { close(stopCh) }
}

func (c *Coordinator) deliverPendingHints() {
	for nodeID := range c.Hints.PendingByNode() {
		if !c.Membership.IsAlive(nodeID) {
			continue
		}
		hints := c.Hints.drain(nodeID)
		var failed []Hint
		for _, hint := range hints {
			var err error
			if hint.Tombstone {
				err = c.sendDelete(nodeID, hint.Key, hint.Timestamp)
			} else {
				err = c.sendPut(nodeID, hint.Key, hint.Value, hint.Timestamp)
			}
			if err != nil {
				failed = append(failed, hint)
			}
		}
		if len(failed) > 0 {
			c.Hints.requeue(nodeID, failed)
		}
	}
}

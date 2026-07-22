package coordinator

import "testing"

func TestHintStoreAddDrainRequeue(t *testing.T) {
	h := NewHintStore()

	h.Add("node-3", Hint{Key: "a", Value: "1", Timestamp: 1})
	h.Add("node-3", Hint{Key: "b", Value: "2", Timestamp: 2})
	h.Add("node-1", Hint{Key: "c", Value: "3", Timestamp: 3})

	if got := h.PendingCount(); got != 3 {
		t.Fatalf("expected 3 pending hints, got %d", got)
	}

	byNode := h.PendingByNode()
	if byNode["node-3"] != 2 || byNode["node-1"] != 1 {
		t.Fatalf("unexpected per-node hint counts: %v", byNode)
	}

	drained := h.drain("node-3")
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained hints for node-3, got %d", len(drained))
	}
	if h.PendingCount() != 1 {
		t.Fatalf("expected 1 hint remaining after drain, got %d", h.PendingCount())
	}

	// Simulate one of the two hints failing redelivery: it should be
	// requeued so it isn't silently lost.
	h.requeue("node-3", []Hint{drained[0]})
	if h.PendingCount() != 2 {
		t.Fatalf("expected requeued hint to bring pending count back to 2, got %d", h.PendingCount())
	}
	if byNode := h.PendingByNode(); byNode["node-3"] != 1 {
		t.Fatalf("expected 1 pending hint for node-3 after requeue, got %v", byNode)
	}
}

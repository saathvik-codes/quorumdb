package ring

import "testing"

func TestPreferenceListSpreadsAcrossZones(t *testing.T) {
	r := New(100)
	r.AddNode("node-1", "zone-a")
	r.AddNode("node-2", "zone-b")
	r.AddNode("node-3", "zone-c")
	r.AddNode("node-4", "zone-a") // second node in zone-a

	// With 3 distinct zones and N=3, the preference list for any key should
	// land in 3 distinct zones, not just 3 distinct nodes.
	for _, key := range []string{"alpha", "beta", "gamma", "delta-key-with-more-entropy"} {
		list := r.PreferenceList(key, 3)
		if len(list) != 3 {
			t.Fatalf("key %s: expected 3 nodes, got %d: %v", key, len(list), list)
		}

		zones := make(map[string]bool)
		for _, node := range list {
			zones[r.ZoneOf(node)] = true
		}
		if len(zones) != 3 {
			t.Fatalf("key %s: expected replicas spread across 3 zones, got %d zones from nodes %v", key, len(zones), list)
		}
	}
}

func TestPreferenceListFallsBackWhenTooFewZones(t *testing.T) {
	r := New(100)
	r.AddNode("node-1", "zone-a")
	r.AddNode("node-2", "zone-a")
	r.AddNode("node-3", "zone-a")

	// Only one zone exists, so N=3 must still return 3 distinct nodes (can't
	// avoid same-zone placement when there's nowhere else to put them).
	list := r.PreferenceList("some-key", 3)
	if len(list) != 3 {
		t.Fatalf("expected 3 nodes even with only one zone available, got %d: %v", len(list), list)
	}
	seen := make(map[string]bool)
	for _, n := range list {
		if seen[n] {
			t.Fatalf("duplicate node in preference list: %v", list)
		}
		seen[n] = true
	}
}

func TestPreferenceListStableForSameKey(t *testing.T) {
	r := New(50)
	r.AddNode("a", "z1")
	r.AddNode("b", "z2")
	r.AddNode("c", "z3")

	first := r.PreferenceList("stable-key", 3)
	second := r.PreferenceList("stable-key", 3)
	if len(first) != len(second) {
		t.Fatalf("preference list length changed across calls")
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("preference list not stable for the same key: %v vs %v", first, second)
		}
	}
}

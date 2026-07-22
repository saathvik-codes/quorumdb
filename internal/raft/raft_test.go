package raft

import (
	"fmt"
	"testing"
	"time"
)

func startAll(nodes map[string]*Node) {
	for _, n := range nodes {
		n.Start()
	}
}

func stopAll(nodes map[string]*Node) {
	for _, n := range nodes {
		n.Stop()
	}
}

// waitForLeader polls until exactly one node in the cluster believes it is
// leader (or the timeout elapses), and returns that node's ID.
func waitForLeader(t *testing.T, nodes map[string]*Node, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []string
		for id, n := range nodes {
			if n.Role() == Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		if len(leaders) > 1 {
			t.Fatalf("safety violation: multiple simultaneous leaders: %v", leaders)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return ""
}

func TestLeaderElection(t *testing.T) {
	_, nodes, _ := newTestCluster([]string{"n1", "n2", "n3"})
	startAll(nodes)
	defer stopAll(nodes)

	leaderID := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("elected leader: %s (term %d)", leaderID, nodes[leaderID].CurrentTerm())

	// Every other node should agree who the leader is.
	for id, n := range nodes {
		if id == leaderID {
			continue
		}
		deadline := time.Now().Add(1 * time.Second)
		for n.LeaderID() != leaderID && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n.LeaderID() != leaderID {
			t.Fatalf("node %s does not recognize %s as leader (has %q)", id, leaderID, n.LeaderID())
		}
	}
}

func TestLogReplicationAndApply(t *testing.T) {
	_, nodes, _ := newTestCluster([]string{"n1", "n2", "n3"})
	startAll(nodes)
	defer stopAll(nodes)

	leaderID := waitForLeader(t, nodes, 3*time.Second)
	leader := nodes[leaderID]

	applyChans := make(map[string]<-chan ApplyMsg)
	for id, n := range nodes {
		applyChans[id] = n.ApplyChannel()
	}

	index, isLeader, committed := leader.Submit([]byte("set x=1"))
	if !isLeader {
		t.Fatal("expected Submit on the leader to report isLeader=true")
	}
	if !committed {
		t.Fatal("expected the entry to commit")
	}
	if index != 1 {
		t.Fatalf("expected first real log entry to be at index 1, got %d", index)
	}

	// Every node — leader and followers — must apply the same committed
	// entry with the same content.
	for id := range nodes {
		select {
		case msg := <-applyChans[id]:
			if string(msg.Command) != "set x=1" {
				t.Fatalf("node %s applied wrong command: %q", id, msg.Command)
			}
			if msg.Index != 1 {
				t.Fatalf("node %s applied wrong index: %d", id, msg.Index)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("node %s never applied the committed entry", id)
		}
	}
}

func TestLeaderFailoverAndContinuedReplication(t *testing.T) {
	cluster, nodes, _ := newTestCluster([]string{"n1", "n2", "n3"})
	startAll(nodes)
	defer stopAll(nodes)

	firstLeaderID := waitForLeader(t, nodes, 3*time.Second)
	firstTerm := nodes[firstLeaderID].CurrentTerm()

	// Simulate a crash: take the leader off the network entirely.
	cluster.setUp(firstLeaderID, false)

	// A new leader must be elected among the remaining two nodes, in a
	// strictly higher term (this is what makes it safe: the old leader,
	// if it ever comes back, will see a higher term and step down).
	deadline := time.Now().Add(3 * time.Second)
	var secondLeaderID string
	for time.Now().Before(deadline) {
		for id, n := range nodes {
			if id == firstLeaderID {
				continue
			}
			if n.Role() == Leader && n.CurrentTerm() > firstTerm {
				secondLeaderID = id
			}
		}
		if secondLeaderID != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if secondLeaderID == "" {
		t.Fatal("no new leader elected after simulated leader crash")
	}
	if secondLeaderID == firstLeaderID {
		t.Fatal("the crashed node should not be the new leader")
	}
	t.Logf("failed over from %s (term %d) to %s (term %d)",
		firstLeaderID, firstTerm, secondLeaderID, nodes[secondLeaderID].CurrentTerm())

	// The cluster must still be able to commit new entries with only 2 of 3
	// nodes up (a majority).
	index, isLeader, committed := nodes[secondLeaderID].Submit([]byte("set y=2"))
	if !isLeader || !committed {
		t.Fatalf("expected new leader to commit an entry after failover, got isLeader=%v committed=%v", isLeader, committed)
	}
	if index < 1 {
		t.Fatalf("unexpected index after failover: %d", index)
	}

	// Bring the old leader back — it must recognize the new term and step
	// down rather than continuing to believe it's leader (this is the
	// safety property Raft exists to guarantee).
	cluster.setUp(firstLeaderID, true)
	time.Sleep(300 * time.Millisecond)
	if nodes[firstLeaderID].Role() == Leader {
		t.Fatal("old leader did not step down after rejoining with a stale term")
	}
}

func TestRejoinedFollowerCatchesUpLog(t *testing.T) {
	cluster, nodes, _ := newTestCluster([]string{"n1", "n2", "n3"})
	startAll(nodes)
	defer stopAll(nodes)

	leaderID := waitForLeader(t, nodes, 3*time.Second)
	leader := nodes[leaderID]

	// Drain the initial applies from all nodes so later assertions aren't
	// confused by them.
	for _, n := range nodes {
		drainNonBlocking(n.ApplyChannel())
	}

	// Pick a follower and partition it away from the cluster.
	var isolated string
	for id := range nodes {
		if id != leaderID {
			isolated = id
			break
		}
	}
	cluster.setUp(isolated, false)

	// Commit several entries while the follower is partitioned.
	for i := 0; i < 5; i++ {
		_, isLeader, committed := leader.Submit([]byte(fmt.Sprintf("entry-%d", i)))
		if !isLeader || !committed {
			t.Fatalf("entry %d failed to commit with 2/3 nodes up: isLeader=%v committed=%v", i, isLeader, committed)
		}
	}

	// Reconnect the isolated follower; it must catch up via AppendEntries
	// log backtracking, not just future entries.
	cluster.setUp(isolated, true)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[isolated].commitIndexForTest() >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := nodes[isolated].commitIndexForTest(); got < 5 {
		t.Fatalf("isolated follower did not catch up: commitIndex=%d, want >= 5", got)
	}

	applied := drainNonBlocking(nodes[isolated].ApplyChannel())
	if len(applied) < 5 {
		t.Fatalf("isolated follower applied %d entries after catch-up, want >= 5", len(applied))
	}
	for i, msg := range applied {
		want := fmt.Sprintf("entry-%d", i)
		if string(msg.Command) != want {
			t.Fatalf("applied entry %d out of order: got %q want %q", i, msg.Command, want)
		}
	}
}

func TestNoSplitBrainAcrossManyElections(t *testing.T) {
	// Run several independent clusters through startup election and assert
	// at every observation point that at most one leader exists per term —
	// this is Raft's core safety property, exercised probabilistically
	// since election timing is randomized.
	for trial := 0; trial < 5; trial++ {
		func() {
			_, nodes, _ := newTestCluster([]string{"n1", "n2", "n3", "n4", "n5"})
			startAll(nodes)
			defer stopAll(nodes)

			waitForLeader(t, nodes, 3*time.Second)

			// Sample leadership state repeatedly for a bit; waitForLeader
			// itself already fails the test on any multi-leader observation.
			for i := 0; i < 20; i++ {
				var leaders []string
				for id, n := range nodes {
					if n.Role() == Leader {
						leaders = append(leaders, id)
					}
				}
				if len(leaders) > 1 {
					t.Fatalf("trial %d: safety violation, multiple leaders: %v", trial, leaders)
				}
				time.Sleep(15 * time.Millisecond)
			}
		}()
	}
}

// --- test helpers that need access to unexported state / the cluster ---

func (n *Node) commitIndexForTest() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func drainNonBlocking(ch <-chan ApplyMsg) []ApplyMsg {
	var out []ApplyMsg
	for {
		select {
		case msg := <-ch:
			out = append(out, msg)
		default:
			return out
		}
	}
}

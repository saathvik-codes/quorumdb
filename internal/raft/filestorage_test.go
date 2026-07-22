package raft

import (
	"bytes"
	"os"
	"testing"
)

func TestFileStorageSaveLoadRoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "quorumdb-raft-storage-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	log := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1, Command: []byte("set a=1")},
		{Term: 1, Index: 2, Command: []byte("set b=2")},
		{Term: 2, Index: 3, Command: []byte("set c=3")},
	}

	if err := fs.SaveState(2, "node-2", log); err != nil {
		t.Fatal(err)
	}

	// A fresh FileStorage pointed at the same directory (simulating a
	// process restart) must see exactly what was saved.
	fs2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	term, votedFor, loadedLog, err := fs2.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if term != 2 {
		t.Fatalf("term: got %d want 2", term)
	}
	if votedFor != "node-2" {
		t.Fatalf("votedFor: got %q want %q", votedFor, "node-2")
	}
	if len(loadedLog) != len(log) {
		t.Fatalf("log length: got %d want %d", len(loadedLog), len(log))
	}
	for i := range log {
		if loadedLog[i].Term != log[i].Term || loadedLog[i].Index != log[i].Index || !bytes.Equal(loadedLog[i].Command, log[i].Command) {
			t.Fatalf("log[%d]: got %+v want %+v", i, loadedLog[i], log[i])
		}
	}
}

func TestNodeRestoresStateFromDiskOnStart(t *testing.T) {
	dir, err := os.MkdirTemp("", "quorumdb-raft-node-restart-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	preexisting := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 3, Index: 1, Command: []byte("pre-existing entry")},
	}
	if err := fs.SaveState(3, "node-9", preexisting); err != nil {
		t.Fatal(err)
	}

	// A brand-new Node using this storage must pick up the persisted term,
	// vote, and log on Start() — this is what prevents a restarted node
	// from re-voting in a term it already voted in (the classic safety bug
	// if currentTerm/votedFor aren't durable).
	node := NewNode("node-9", []string{"node-x"}, &memoryTransport{self: "node-9", cluster: newMemoryCluster()}, fs)
	node.Start()
	defer node.Stop()

	if got := node.CurrentTerm(); got != 3 {
		t.Fatalf("restored currentTerm: got %d want 3", got)
	}

	node.mu.Lock()
	votedFor := node.votedFor
	logLen := len(node.log)
	node.mu.Unlock()

	if votedFor != "node-9" {
		t.Fatalf("restored votedFor: got %q want %q", votedFor, "node-9")
	}
	if logLen != 2 {
		t.Fatalf("restored log length: got %d want 2", logLen)
	}
}

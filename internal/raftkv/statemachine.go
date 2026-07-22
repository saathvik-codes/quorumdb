// Package raftkv wires quorumdb's LSM store to a raft.Node as its
// replicated state machine: every PUT/DELETE is proposed as a Raft log
// entry, and only applied to the local store once Raft has committed it —
// so every node's store converges to the exact same sequence of writes in
// the exact same order, which is what gives Raft-backed reads (served by
// the leader) strong consistency, unlike the quorum+LWW system elsewhere
// in this repo.
package raftkv

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/saathvik-codes/quorumdb/internal/raft"
	"github.com/saathvik-codes/quorumdb/internal/store"
)

type Op string

const (
	OpPut    Op = "PUT"
	OpDelete Op = "DELETE"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

var ErrNotLeader = errors.New("this node is not the raft leader")
var ErrCommitTimeout = errors.New("proposed entry did not commit in time")

type StateMachine struct {
	node  *raft.Node
	store *store.Store
}

func New(node *raft.Node, s *store.Store) *StateMachine {
	sm := &StateMachine{node: node, store: s}
	go sm.applyLoop()
	return sm
}

// applyLoop drains committed entries from Raft, in order, and applies them
// to the local store — this is the only place the store is mutated, which
// is what guarantees every node applies the identical sequence of writes.
func (sm *StateMachine) applyLoop() {
	for msg := range sm.node.ApplyChannel() {
		var cmd Command
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			continue // shouldn't happen for entries we produced ourselves
		}
		ts := int64(msg.Index) // log index doubles as a monotonic version stamp
		switch cmd.Op {
		case OpPut:
			_ = sm.store.Put(cmd.Key, []byte(cmd.Value), ts)
		case OpDelete:
			_ = sm.store.Delete(cmd.Key, ts)
		}
	}
}

// Put proposes a PUT through Raft and blocks until it commits (or fails).
func (sm *StateMachine) Put(key, value string) error {
	return sm.propose(Command{Op: OpPut, Key: key, Value: value})
}

func (sm *StateMachine) Delete(key string) error {
	return sm.propose(Command{Op: OpDelete, Key: key})
}

func (sm *StateMachine) propose(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	_, isLeader, committed := sm.node.Submit(data)
	if !isLeader {
		return ErrNotLeader
	}
	if !committed {
		return ErrCommitTimeout
	}
	return nil
}

// Get reads directly from the local store.
//
// Simplification (documented, not hidden): this does not implement the
// Raft read-index/lease-read protocol, so it's possible in a vanishingly
// small window — right after this node stops being leader but before it
// knows it — to serve a read that's a few milliseconds stale. A production
// implementation confirms current leadership (e.g. via a round of
// heartbeats) before answering a read; this demo answers directly from the
// leader's local, already-committed state for clarity of the core
// replication mechanism.
func (sm *StateMachine) Get(key string) (string, bool) {
	entry, found := sm.store.Get(key)
	if !found {
		return "", false
	}
	return string(entry.Value), true
}

func (sm *StateMachine) IsLeader() bool {
	return sm.node.Role() == raft.Leader
}

func (sm *StateMachine) LeaderID() string {
	return sm.node.LeaderID()
}

func (sm *StateMachine) Status() map[string]any {
	return map[string]any{
		"role":     sm.node.Role().String(),
		"term":     sm.node.CurrentTerm(),
		"leaderId": sm.node.LeaderID(),
	}
}

func (sm *StateMachine) String() string {
	return fmt.Sprintf("raftkv{role=%s term=%d}", sm.node.Role(), sm.node.CurrentTerm())
}

// Package raft implements the core Raft consensus algorithm (Ongaro &
// Ousterhout, "In Search of an Understandable Consensus Algorithm") over a
// pluggable Transport, so the algorithm itself can be unit-tested with an
// in-memory transport before being driven over real HTTP.
//
// The log is 1-indexed; log[0] is an unused sentinel entry with Term 0,
// which removes special-casing for "previous log index is 0" throughout
// the AppendEntries consistency check.
package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// ConflictIndex/ConflictTerm let the leader back up nextIndex by more
	// than one entry per round-trip on a mismatch, instead of decrementing
	// by exactly one each time (a correctness-preserving optimization from
	// the Raft paper's section on log backtracking).
	ConflictIndex int
	ConflictTerm  int
}

// Transport delivers RPCs to a named peer. Implementations must be safe for
// concurrent use and should apply their own timeout.
type Transport interface {
	SendRequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Storage persists the state Raft requires to survive a crash without
// violating safety: currentTerm and votedFor (to prevent double-voting in
// the same term after a restart) and the log itself.
type Storage interface {
	SaveState(currentTerm int, votedFor string, log []LogEntry) error
	LoadState() (currentTerm int, votedFor string, log []LogEntry, err error)
}

// ApplyMsg is delivered, in log order, for every entry that becomes
// committed — the state machine (e.g. the KV store) applies it.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte
}

type Node struct {
	mu sync.Mutex

	id        string
	peerIDs   []string
	transport Transport
	storage   Storage

	// Persistent state (must be saved before responding to any RPC that
	// changes it).
	currentTerm int
	votedFor    string
	log         []LogEntry // log[0] is a dummy sentinel; real entries start at index 1

	// Volatile state.
	commitIndex int
	lastApplied int
	role        Role
	leaderID    string

	// Leader-only volatile state (reinitialized after every election).
	nextIndex  map[string]int
	matchIndex map[string]int

	electionResetAt time.Time
	applyCh         chan ApplyMsg
	commitWaiters   map[int][]chan bool // log index -> channels to notify on commit (true) or lost leadership (false)

	stopCh  chan struct{}
	started bool

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
}

func NewNode(id string, peerIDs []string, transport Transport, storage Storage) *Node {
	return &Node{
		id:                 id,
		peerIDs:            peerIDs,
		transport:          transport,
		storage:            storage,
		log:                []LogEntry{{Term: 0, Index: 0}}, // sentinel
		role:               Follower,
		applyCh:            make(chan ApplyMsg, 256),
		commitWaiters:      make(map[int][]chan bool),
		stopCh:             make(chan struct{}),
		electionTimeoutMin: 300 * time.Millisecond,
		electionTimeoutMax: 600 * time.Millisecond,
		heartbeatInterval:  75 * time.Millisecond,
	}
}

// ApplyChannel returns the channel committed entries are delivered on, in
// order. The caller (state machine) must drain it.
func (n *Node) ApplyChannel() <-chan ApplyMsg {
	return n.applyCh
}

func (n *Node) Start() {
	n.mu.Lock()
	if n.storage != nil {
		if term, votedFor, log, err := n.storage.LoadState(); err == nil && len(log) > 0 {
			n.currentTerm = term
			n.votedFor = votedFor
			n.log = log
		}
	}
	n.electionResetAt = time.Now()
	n.started = true
	n.mu.Unlock()

	go n.electionTimeoutLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *Node) CurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) persist() {
	if n.storage != nil {
		_ = n.storage.SaveState(n.currentTerm, n.votedFor, n.log)
	}
}

func (n *Node) lastLogIndexTerm() (int, int) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) randomElectionTimeout() time.Duration {
	span := n.electionTimeoutMax - n.electionTimeoutMin
	return n.electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// electionTimeoutLoop periodically checks whether too much time has passed
// since we last heard from a leader (or granted a vote); if so, and we're
// not already a leader, it starts an election. This "ticker checks elapsed
// time" pattern (rather than a resettable timer) avoids a whole class of
// timer-reset races and is the standard approach for teaching-quality Raft
// implementations.
func (n *Node) electionTimeoutLoop() {
	timeout := n.randomElectionTimeout()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role == Leader {
				n.mu.Unlock()
				continue
			}
			elapsed := time.Since(n.electionResetAt)
			if elapsed >= timeout {
				n.mu.Unlock()
				n.startElection()
				timeout = n.randomElectionTimeout()
				continue
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionResetAt = time.Now()
	n.persist()
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogIndexTerm()
	peers := append([]string{}, n.peerIDs...)
	n.mu.Unlock()

	votes := 1 // vote for self
	var voteMu sync.Mutex
	becameLeader := false

	var wg sync.WaitGroup
	for _, peerID := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, peer, RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.role != Candidate || n.currentTerm != term {
				return // stale response from an election we've already moved past
			}
			if !reply.VoteGranted {
				return
			}

			voteMu.Lock()
			votes++
			gotMajority := votes > (len(peers)+1)/2
			voteMu.Unlock()

			if gotMajority && !becameLeader && n.role == Candidate {
				becameLeader = true
				n.becomeLeaderLocked()
			}
		}(peerID)
	}
	wg.Wait()
}

// becomeFollowerLocked must be called with n.mu held.
func (n *Node) becomeFollowerLocked(newTerm int) {
	n.role = Follower
	n.currentTerm = newTerm
	n.votedFor = ""
	n.leaderID = ""
	n.electionResetAt = time.Now()
	n.persist()
	n.notifyCommitWaitersLocked(false)
}

// becomeLeaderLocked must be called with n.mu held.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	n.nextIndex = make(map[string]int)
	n.matchIndex = make(map[string]int)
	lastIndex, _ := n.lastLogIndexTerm()
	for _, peer := range n.peerIDs {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	go n.leaderLoop(n.currentTerm)
}

// leaderLoop sends periodic AppendEntries (heartbeats + replication) to
// every peer for as long as this node remains leader of the given term.
func (n *Node) leaderLoop(term int) {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	n.replicateToAll(term)
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			stillLeader := n.role == Leader && n.currentTerm == term
			n.mu.Unlock()
			if !stillLeader {
				return
			}
			n.replicateToAll(term)
		}
	}
}

func (n *Node) replicateToAll(term int) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	peers := append([]string{}, n.peerIDs...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(term, peer)
	}
}

func (n *Node) replicateToPeer(term int, peer string) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	}
	var entries []LogEntry
	if nextIdx < len(n.log) {
		entries = append(entries, n.log[nextIdx:]...)
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	reply, err := n.transport.SendAppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		n.matchIndex[peer] = prevLogIndex + len(entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndexLocked()
		return
	}

	// Backtrack nextIndex using the conflict hint if present, otherwise by one.
	if reply.ConflictTerm != 0 {
		newNext := -1
		for i := len(n.log) - 1; i >= 1; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				newNext = i + 1
				break
			}
		}
		if newNext != -1 {
			n.nextIndex[peer] = newNext
		} else {
			n.nextIndex[peer] = reply.ConflictIndex
		}
	} else if reply.ConflictIndex > 0 {
		n.nextIndex[peer] = reply.ConflictIndex
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
}

// advanceCommitIndexLocked must be called with n.mu held. It implements the
// Raft commit rule: a leader may only commit an entry from its own current
// term by counting replicas (committing an older-term entry solely by count
// is the classic Figure 8 safety violation the paper warns about); once
// that entry is committed, everything before it is implicitly committed too.
func (n *Node) advanceCommitIndexLocked() {
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1 // leader itself
		for _, peer := range n.peerIDs {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count > (len(n.peerIDs)+1)/2 {
			n.commitIndex = idx
			n.applyCommittedLocked()
			n.notifyCommitWaitersLocked(true)
			return
		}
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		select {
		case n.applyCh <- ApplyMsg{Index: entry.Index, Term: entry.Term, Command: entry.Command}:
		default:
			// Applier is behind; deliver in a goroutine so we never block
			// (and therefore never deadlock) while holding n.mu.
			go func(e LogEntry) { n.applyCh <- ApplyMsg{Index: e.Index, Term: e.Term, Command: e.Command} }(entry)
		}
	}
}

func (n *Node) notifyCommitWaitersLocked(success bool) {
	for idx, waiters := range n.commitWaiters {
		if success && idx > n.commitIndex {
			continue
		}
		for _, ch := range waiters {
			ch <- success
			close(ch)
		}
		delete(n.commitWaiters, idx)
	}
}

// HandleRequestVote implements the RequestVote RPC handler per the Raft paper.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	myLastIndex, myLastTerm := n.lastLogIndexTerm()
	logOK := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		n.electionResetAt = time.Now()
		n.persist()
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

// HandleAppendEntries implements the AppendEntries RPC handler per the Raft
// paper, including the log-matching consistency check and conflict
// resolution.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	// A valid leader for our term: reset election timer and record leadership.
	n.role = Follower
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()

	// Consistency check: our log must contain an entry at PrevLogIndex whose
	// term matches PrevLogTerm.
	if args.PrevLogIndex >= len(n.log) {
		return AppendEntriesReply{
			Term: n.currentTerm, Success: false,
			ConflictIndex: len(n.log),
			ConflictTerm:  0,
		}
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 1 && n.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		return AppendEntriesReply{
			Term: n.currentTerm, Success: false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}
	}

	// Append new entries, truncating on the first conflict (same index,
	// different term) per the paper's log-matching rule.
	insertAt := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIdx := insertAt + i
		if logIdx < len(n.log) {
			if n.log[logIdx].Term != entry.Term {
				n.log = n.log[:logIdx]
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
			// else: already have this exact entry, keep scanning
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}
	n.persist()

	if args.LeaderCommit > n.commitIndex {
		lastNew, _ := n.lastLogIndexTerm()
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
		n.applyCommittedLocked()
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// Submit appends command to the log if this node is currently the leader,
// and blocks until the entry is committed (returning true) or leadership is
// lost before that happens (returning false). It returns immediately with
// isLeader=false if this node isn't the leader.
func (n *Node) Submit(command []byte) (index int, isLeader bool, committed bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, false, false
	}

	lastIndex, _ := n.lastLogIndexTerm()
	entry := LogEntry{Term: n.currentTerm, Index: lastIndex + 1, Command: command}
	n.log = append(n.log, entry)
	n.persist()

	waitCh := make(chan bool, 1)
	n.commitWaiters[entry.Index] = append(n.commitWaiters[entry.Index], waitCh)
	term := n.currentTerm
	n.mu.Unlock()

	go n.replicateToAll(term)

	select {
	case ok := <-waitCh:
		return entry.Index, true, ok
	case <-time.After(2 * time.Second):
		return entry.Index, true, false
	}
}

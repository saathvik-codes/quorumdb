// Package coordinator implements quorum reads/writes over a set of storage
// nodes selected via consistent hashing, with last-write-wins conflict
// resolution and asynchronous read-repair of stale replicas.
package coordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/saathvik-codes/quorumdb/internal/cluster"
	"github.com/saathvik-codes/quorumdb/internal/metrics"
	"github.com/saathvik-codes/quorumdb/internal/ring"
	"github.com/saathvik-codes/quorumdb/internal/store"
)

var ErrQuorumNotMet = errors.New("could not reach enough replicas to satisfy quorum")

type Coordinator struct {
	Ring       *ring.HashRing
	Membership *cluster.Membership
	Hints      *HintStore
	Metrics    *metrics.Registry
	N          int // replication factor
	W          int // write quorum
	R          int // read quorum
	client     *http.Client

	putsTotal            *metrics.Counter
	getsTotal            *metrics.Counter
	deletesTotal         *metrics.Counter
	writeQuorumFailTotal *metrics.Counter
	readQuorumFailTotal  *metrics.Counter
	readRepairsTotal     *metrics.Counter
}

func New(ring *ring.HashRing, membership *cluster.Membership, n, w, r int) *Coordinator {
	reg := metrics.NewRegistry()
	c := &Coordinator{
		Ring:       ring,
		Membership: membership,
		Hints:      NewHintStore(),
		Metrics:    reg,
		N:          n,
		W:          w,
		R:          r,
		client:     &http.Client{Timeout: 2 * time.Second},

		putsTotal:            reg.NewCounter("qdb_coordinator_puts_total", "Total PUT requests handled"),
		getsTotal:            reg.NewCounter("qdb_coordinator_gets_total", "Total GET requests handled"),
		deletesTotal:         reg.NewCounter("qdb_coordinator_deletes_total", "Total DELETE requests handled"),
		writeQuorumFailTotal: reg.NewCounter("qdb_coordinator_write_quorum_failures_total", "Writes that failed to reach W acks"),
		readQuorumFailTotal:  reg.NewCounter("qdb_coordinator_read_quorum_failures_total", "Reads that failed to reach R responses"),
		readRepairsTotal:     reg.NewCounter("qdb_coordinator_read_repairs_total", "Stale replicas repaired after a quorum read"),
	}
	reg.NewGaugeFunc("qdb_coordinator_pending_hints", "Total hinted-handoff writes waiting for redelivery", func() float64 {
		return float64(c.Hints.PendingCount())
	})
	return c
}

type nodeResult struct {
	nodeID    string
	found     bool
	value     string
	timestamp int64
	err       error
}

// Put replicates key=value to the N replicas for key, succeeding once W
// acknowledgements are collected. All N nodes are contacted in parallel
// regardless of W, so slow/dead replicas beyond W don't block the caller.
func (c *Coordinator) Put(key, value string) error {
	c.putsTotal.Inc()
	ts := store.Now()
	replicas := c.Ring.PreferenceList(key, c.N)
	if len(replicas) == 0 {
		return errors.New("no nodes available in the ring")
	}

	acks := c.fanOutWithHints(replicas, Hint{Key: key, Value: value, Timestamp: ts}, func(nodeID string) error {
		return c.sendPut(nodeID, key, value, ts)
	})

	if acks < c.W {
		c.writeQuorumFailTotal.Inc()
		return fmt.Errorf("%w: got %d/%d acks (need W=%d) from replicas %v", ErrQuorumNotMet, acks, len(replicas), c.W, replicas)
	}
	return nil
}

func (c *Coordinator) Delete(key string) error {
	c.deletesTotal.Inc()
	ts := store.Now()
	replicas := c.Ring.PreferenceList(key, c.N)
	if len(replicas) == 0 {
		return errors.New("no nodes available in the ring")
	}

	acks := c.fanOutWithHints(replicas, Hint{Key: key, Timestamp: ts, Tombstone: true}, func(nodeID string) error {
		return c.sendDelete(nodeID, key, ts)
	})

	if acks < c.W {
		c.writeQuorumFailTotal.Inc()
		return fmt.Errorf("%w: got %d/%d acks (need W=%d)", ErrQuorumNotMet, acks, len(replicas), c.W)
	}
	return nil
}

// Get queries R replicas for key, returns the highest-timestamp value
// (last-write-wins across replicas), and asynchronously read-repairs any
// replica that responded with a stale or missing value.
func (c *Coordinator) Get(key string) (value string, found bool, err error) {
	c.getsTotal.Inc()
	replicas := c.Ring.PreferenceList(key, c.N)
	if len(replicas) == 0 {
		return "", false, errors.New("no nodes available in the ring")
	}

	var wg sync.WaitGroup
	results := make(chan nodeResult, len(replicas))
	contacted := 0

	for _, nodeID := range replicas {
		if !c.Membership.IsAlive(nodeID) {
			continue
		}
		contacted++
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			results <- c.sendGet(id, key)
		}(nodeID)
	}

	go func() { wg.Wait(); close(results) }()

	var responses []nodeResult
	for res := range results {
		if res.err == nil {
			responses = append(responses, res)
		}
	}

	if len(responses) < c.R {
		c.readQuorumFailTotal.Inc()
		return "", false, fmt.Errorf("%w: got %d responses (need R=%d) from %d contacted replicas", ErrQuorumNotMet, len(responses), c.R, contacted)
	}

	winner := latestOf(responses)
	c.readRepair(key, winner, responses)

	return winner.value, winner.found, nil
}

func latestOf(responses []nodeResult) nodeResult {
	best := responses[0]
	for _, r := range responses[1:] {
		if r.timestamp > best.timestamp {
			best = r
		}
	}
	return best
}

func (c *Coordinator) readRepair(key string, winner nodeResult, responses []nodeResult) {
	for _, r := range responses {
		if r.timestamp < winner.timestamp {
			c.readRepairsTotal.Inc()
			go func(nodeID string) {
				_ = c.sendPut(nodeID, key, winner.value, winner.timestamp)
			}(r.nodeID)
		}
	}
}

// fanOutWithHints calls fn concurrently for every currently-alive node in
// the preference list, and records a hint for every node that is currently
// dead (so the write reaches it eventually via hinted handoff instead of
// only via a future read-repair). Returns how many live nodes acked.
func (c *Coordinator) fanOutWithHints(nodeIDs []string, hint Hint, fn func(nodeID string) error) int {
	var wg sync.WaitGroup
	var mu sync.Mutex
	acks := 0

	for _, id := range nodeIDs {
		if !c.Membership.IsAlive(id) {
			c.Hints.Add(id, hint)
			continue
		}
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := fn(nodeID); err == nil {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return acks
}

func (c *Coordinator) sendPut(nodeID, key, value string, ts int64) error {
	base, ok := c.Membership.BaseURL(nodeID)
	if !ok {
		return fmt.Errorf("unknown node %s", nodeID)
	}
	body, _ := json.Marshal(map[string]any{"value": value, "timestamp": ts})
	req, err := http.NewRequest(http.MethodPut, base+"/internal/kv/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node %s returned %d", nodeID, resp.StatusCode)
	}
	return nil
}

func (c *Coordinator) sendDelete(nodeID, key string, ts int64) error {
	base, ok := c.Membership.BaseURL(nodeID)
	if !ok {
		return fmt.Errorf("unknown node %s", nodeID)
	}
	body, _ := json.Marshal(map[string]any{"timestamp": ts})
	req, err := http.NewRequest(http.MethodDelete, base+"/internal/kv/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node %s returned %d", nodeID, resp.StatusCode)
	}
	return nil
}

func (c *Coordinator) sendGet(nodeID, key string) nodeResult {
	base, ok := c.Membership.BaseURL(nodeID)
	if !ok {
		return nodeResult{nodeID: nodeID, err: fmt.Errorf("unknown node")}
	}
	resp, err := c.client.Get(base + "/internal/kv/" + key)
	if err != nil {
		return nodeResult{nodeID: nodeID, err: err}
	}
	defer resp.Body.Close()

	var parsed struct {
		Found     bool   `json:"found"`
		Value     string `json:"value"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nodeResult{nodeID: nodeID, err: err}
	}
	return nodeResult{nodeID: nodeID, found: parsed.Found, value: parsed.Value, timestamp: parsed.Timestamp}
}

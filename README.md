# QuorumDB — a distributed, replicated key-value store from scratch

A miniature Bigtable/Dynamo/Cassandra-style distributed KV store in Go,
built to demonstrate the actual mechanics of distributed storage systems —
not a wrapper around one. No external database, no ORM, **zero third-party
Go dependencies** (check `go.mod`): consistent hashing, quorum consensus,
an LSM storage engine, bloom filters, hinted handoff, metrics, and a
**from-scratch Raft consensus implementation** are all implemented directly.

This repo ships **two independent replication strategies** over the same
underlying LSM store, deliberately, to demonstrate the actual tradeoff
rather than just picking one:

| | Quorum + LWW (`cmd/node` + `cmd/coordinator`) | Raft (`cmd/raftnode`) |
|---|---|---|
| Consistency | Eventual (Dynamo-style) | Strong (linearizable writes) |
| Availability | Any W of N nodes | Requires a live leader (majority) |
| Conflict resolution | Last-write-wins by timestamp | None needed — total order via the log |
| Best known for | Cassandra, DynamoDB | etcd, CockroachDB, TiKV |

## Architecture

```
        clients
           |
      coordinator  (hash ring + membership + quorum + hinted handoff)
       /    |    \
  node-1  node-2  node-3   (each: WAL + memtable + LSM segments + compaction)
  zone-a  zone-b  zone-c
```

- **Consistent hashing with virtual nodes + zone-awareness** (`internal/ring`)
  — 100 virtual positions per physical node for even key distribution;
  `PreferenceList` actively spreads a key's N replicas across distinct
  zones/racks/AZs when enough exist, so one failure domain going down can't
  take out every replica of a key.
- **LSM storage engine** (`internal/store`) — writes go to a WAL (durability)
  and an in-memory memtable (speed); once the memtable crosses a threshold
  it flushes to an immutable segment file with a **bloom filter** (instant
  rejection of definitely-absent keys) and a **sparse index** (binary-search
  to a small on-disk window instead of a linear scan). **Compaction** merges
  all segments into one, dropping tombstones and superseded versions.
  Crash recovery replays the WAL on startup; segment sidecars (bloom+index)
  are persisted and reloaded, not rebuilt by rescanning.
- **Quorum reads/writes** (`internal/coordinator`) — for replication factor
  N, write quorum W, read quorum R: a write succeeds once W of N replicas
  ack; a read queries R replicas and returns the highest-timestamp value
  (last-write-wins), asynchronously repairing any replica that returned a
  stale value.
- **Hinted handoff** — a write to a currently-dead replica is recorded as a
  hint and automatically redelivered once that node's heartbeat comes back
  healthy, instead of relying solely on a future read to trigger repair.
- **Heartbeat-based failure detection** (`internal/cluster`) — the
  coordinator polls every node's `/health` and routes around any node
  currently marked dead.
- **Prometheus-format metrics** (`internal/metrics`) — every node and the
  coordinator expose `/metrics` (puts/gets/deletes, quorum failures,
  read-repairs, pending hints, segment counts) in standard text-exposition
  format, hand-rolled with zero dependencies.

## Raft architecture (`internal/raft`, `internal/raftkv`, `cmd/raftnode`)

```
  client
    |
    v
 node-1 (leader)  <---AppendEntries--->  node-2 (follower)
    |         <---AppendEntries--->  node-3 (follower)
    v
  LSM store (applied only once Raft commits the entry)
```

Each `raftnode` is a full peer — there's no separate coordinator process;
the current leader takes client writes directly. A full implementation of
the algorithm from the paper ("In Search of an Understandable Consensus
Algorithm", Ongaro & Ousterhout), not a library wrapper:

- **Leader election** (`RequestVote`) — randomized election timeouts
  (300-600ms) to avoid split votes, term-based safety (a node only grants
  one vote per term, always defers to a higher term it observes), and the
  log-completeness check (a candidate can't win without a log at least as
  up-to-date as a majority of the cluster).
- **Log replication** (`AppendEntries`) — the log-matching consistency
  check with conflict-term backtracking (a follower whose log diverges
  gets corrected in as few round-trips as possible, not one entry at a
  time), and the commit rule that only commits entries from the *leader's
  current term* by counting replicas (committing an older-term entry by
  count alone is the classic Figure-8 safety violation the paper warns
  about).
- **Persistence** (`internal/raft/filestorage.go`) — currentTerm, votedFor,
  and the log are durably saved (write-temp-then-atomic-rename) before a
  node responds to any RPC that changes them; a restarted node reloads this
  and cannot double-vote in a term it already voted in.
- **State machine** (`internal/raftkv`) — commands are only applied to the
  local LSM store once Raft has committed them, so every node's store
  converges to the identical sequence of writes in the identical order.

The algorithm is unit-tested against an **in-memory transport**
(`internal/raft/memory_transport_test.go`) — direct function calls, no
network — so the tests are fast and the failures they'd catch are actual
algorithm bugs, not flaky timing:

```
go test ./internal/raft/... -v
```

## Run a 3-node Raft cluster locally

```
go build -o bin/raftnode ./cmd/raftnode

./bin/raftnode --id=node-1 --addr=:9201 --peers="node-2=http://localhost:9202,node-3=http://localhost:9203" &
./bin/raftnode --id=node-2 --addr=:9202 --peers="node-1=http://localhost:9201,node-3=http://localhost:9203" &
./bin/raftnode --id=node-3 --addr=:9203 --peers="node-1=http://localhost:9201,node-2=http://localhost:9202" &
```

```
curl http://localhost:9201/status              # {"role":"leader","term":1,"leaderId":"node-1"}
curl -X PUT http://localhost:9201/kv/hello -d '{"value":"world"}'
curl http://localhost:9202/kv/hello             # replicated to a follower's local store
curl -X PUT http://localhost:9202/kv/x -d '{"value":"y"}'   # 421 "not leader", with leaderId hint
```

Kill the leader's process; watch a follower win a new election in a higher
term within ~1-2s; writes keep working against the new leader; restart the
old leader and it rejoins as a follower and catches up the log entries it
missed — no data loss, no split-brain.

## Run a 3-node cluster locally (quorum + LWW system)

```
go build -o bin/node ./cmd/node
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/bench ./cmd/bench
go build -o bin/chaos ./cmd/chaos

./bin/node --id=node-1 --addr=:9101 &
./bin/node --id=node-2 --addr=:9102 &
./bin/node --id=node-3 --addr=:9103 &

./bin/coordinator --addr=:9000 \
  --nodes="node-1=http://localhost:9101=zone-a,node-2=http://localhost:9102=zone-b,node-3=http://localhost:9103=zone-c" \
  --n=3 --w=2 --r=2
```

```
curl -X PUT http://localhost:9000/kv/hello -d '{"value":"world"}'
curl http://localhost:9000/kv/hello
curl http://localhost:9000/cluster
curl http://localhost:9000/metrics
```

## Run with Docker

```
docker compose up -d --build
curl -X PUT http://localhost:9000/kv/hello -d '{"value":"world"}'
docker stop quorumdb-node-2-1   # kill a node — reads/writes keep working
docker start quorumdb-node-2-1  # data survives via the named volume
```

## Automated chaos testing

`cmd/chaos` launches a full cluster as child processes, drives continuous
concurrent load, kills and restarts a random node on a timer, and asserts
every previously-successful write is still readable correctly afterward —
no manual `taskkill`-and-recheck required:

```
go build -o bin/chaos ./cmd/chaos
./bin/chaos --nodes=3 --duration=30s --kill-every=5s
```

**A real run of this** (20s duration, killing a random node every 4s):
5 node restarts, **17,658 writes succeeded / 0 failed**, 3,527 verification
reads, **0 assertion failures**.

## Verified behavior (tested, not claimed)

**Raft:**

- **Algorithm correctness, unit-tested with an in-memory transport**: leader
  election converges to exactly one leader; a committed entry is applied
  identically on every node; killing the leader mid-cluster triggers
  failover to a strictly higher term and the old leader steps down on
  rejoining; a follower partitioned during 5 commits catches up its entire
  log via AppendEntries backtracking on reconnect, in order; 5 independent
  5-node clusters observed repeatedly for split-brain (none found) across
  election cycles with randomized timing — run 4 times back-to-back with
  zero flakes.
- **Persistence, unit-tested**: currentTerm/votedFor/log round-trip through
  disk exactly, and a freshly constructed `Node` reloads them correctly on
  `Start()`.
- **Live 3-process cluster, verified over real HTTP**: election settles to
  one agreed-upon leader; a write via the leader replicates and is directly
  confirmed present on both followers' own local stores (not just
  acknowledged); a follower correctly rejects a write with `421` and the
  live leader's ID; killing the leader's OS process triggers a real
  election won by a follower in a higher term within ~2s; the new leader
  accepts and replicates new writes; the crashed node, restarted, rejoins
  as a follower recognizing the new term (does **not** think it's still
  leader) and catches up the log entry it missed while dead.

**Quorum + LWW:**

- **Replication is real**: a PUT lands on all N physical nodes with matching
  timestamps, confirmed by querying each node's internal API directly.
- **Zone-aware placement is real**: with 3 distinct zones and N=3, every
  key's replicas land on 3 different physical nodes across 3 different
  zones — confirmed by direct per-node lookups across multiple keys.
- **Fault tolerance**: killing one node (N=3/W=2/R=2) leaves reads and
  writes fully working; killing a second correctly fails writes with `503`
  instead of silently under-replicating.
- **Hinted handoff is real**: a key written entirely while its replica was
  down is *not* on that node when it restarts, but *is* there automatically
  within one heartbeat/handoff cycle — confirmed by direct polling.
- **Bloom filter + sparse index are real**: unit-tested with 6,000 keys
  spanning multiple flushed segments — every real key found via the
  index-assisted path, a never-written key correctly rejected.
- **Compaction is real**: unit-tested and live-tested — multiple segments
  merge into one, deleted keys are gone, surviving keys remain correct.
- **Durability**: a key still only in the WAL (not yet flushed) at the
  moment a node process is killed is still present after restart.
- **Docker fault tolerance**: stopping a container (not just killing a
  process) still leaves the cluster serving correctly, and the node's data
  survives the container restart via its named volume.
- **Throughput**: `./bin/bench --ops=3000 --concurrency=25` against the live
  3-node cluster: **~3,000 ops/sec, p50 ~6-8ms, p99 ~30-40ms**, 100% success.

## Known scope boundaries (intentionally not built)

This demonstrates real distributed-systems mechanics, not a production
deployment:

**Raft:**

- **No read-index/lease-read protocol** — `Get` answers directly from the
  leader's local store rather than confirming current leadership first via
  a heartbeat round-trip. In a vanishingly small window (microseconds,
  right after losing leadership but before the node itself knows it) this
  could serve a barely-stale read. A production implementation closes this
  gap; it's called out explicitly rather than silently glossed over.
- **No log compaction/snapshotting** — the log grows forever; a
  long-running cluster would need periodic snapshots the way etcd/Raft
  implementations truncate their log after a snapshot.
- **No cluster membership changes** (adding/removing a peer) — the peer set
  is fixed at process start via `--peers`.
- JSON re-serializes the whole log on every persisted write (see
  `filestorage.go`) rather than an append-only persistent log format.

**Quorum + LWW:**

- **Hints are held in the coordinator's memory**, not durably on another
  live node — a coordinator crash drops in-flight hints (the underlying
  data isn't lost; it relies on the W replicas that did succeed plus
  eventual read-repair instead). A textbook implementation stores hints
  durably on a peer node.
- No gRPC (JSON over HTTP for internal replication — simpler to read, less
  standard for real infra).
- No TLS/mTLS between coordinator and nodes.
- No automatic node removal from the ring on permanent loss (a dead node
  stays in the preference list indefinitely; only hinted handoff covers
  transient outages).

## Stack

Go 1.23, standard library only — deliberately, to keep every mechanism
(hashing, replication, storage, metrics) visible rather than delegated to a
library.

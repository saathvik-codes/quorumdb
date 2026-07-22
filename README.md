# QuorumDB — a distributed, replicated key-value store from scratch

A miniature Bigtable/Dynamo/Cassandra-style distributed KV store in Go,
built to demonstrate the actual mechanics of distributed storage systems —
not a wrapper around one. No external database, no ORM, **zero third-party
Go dependencies** (check `go.mod`): consistent hashing, quorum consensus,
an LSM storage engine, bloom filters, hinted handoff, and metrics are all
implemented directly.

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

## Run a 3-node cluster locally

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

- **No Raft/Paxos consensus** — replication uses quorum + last-write-wins,
  not a replicated log with leader election. This is the single biggest
  gap versus etcd/CockroachDB/TiKV-grade systems (see project notes for the
  planned follow-up).
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

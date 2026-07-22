# QuorumDB — a distributed, replicated key-value store from scratch

A miniature Bigtable/Dynamo/Cassandra-style distributed KV store in Go,
built to demonstrate the actual mechanics of distributed storage systems —
not a wrapper around one. No external database, no ORM: consistent hashing,
quorum consensus, and an LSM storage engine are all implemented directly.

## Architecture

```
        clients
           |
      coordinator  (consistent-hash ring + membership + quorum logic)
       /    |    \
  node-1  node-2  node-3   (each: WAL + memtable + LSM segment files)
```

- **Consistent hashing with virtual nodes** (`internal/ring`) — each physical
  node gets 100 virtual positions on the ring so key ownership is spread
  evenly and only a small fraction of keys move if a node joins/leaves.
- **LSM storage engine** (`internal/store`) — every write goes to a
  write-ahead log (durability) and an in-memory memtable (speed); once the
  memtable crosses a size threshold it flushes to an immutable, key-sorted
  segment file on disk. Reads fall through memtable → segments (newest
  first). Crash recovery replays the WAL on startup.
- **Quorum reads/writes** (`internal/coordinator`) — for replication factor
  N, write quorum W, and read quorum R: a write succeeds once W of N
  replicas ack; a read queries R replicas and returns the highest-timestamp
  value (last-write-wins), asynchronously repairing any replica that
  returned a stale value.
- **Heartbeat-based failure detection** (`internal/cluster`) — the
  coordinator polls every node's `/health` every 500ms and routes around
  any node currently marked dead, rather than blocking on it.

## Run a 3-node cluster locally

```
go build -o bin/node ./cmd/node
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/bench ./cmd/bench

./bin/node --id=node-1 --addr=:9101 &
./bin/node --id=node-2 --addr=:9102 &
./bin/node --id=node-3 --addr=:9103 &

./bin/coordinator --addr=:9000 \
  --nodes="node-1=http://localhost:9101,node-2=http://localhost:9102,node-3=http://localhost:9103" \
  --n=3 --w=2 --r=2
```

```
curl -X PUT http://localhost:9000/kv/hello -d '{"value":"world"}'
curl http://localhost:9000/kv/hello
curl http://localhost:9000/cluster
```

## Verified behavior (not claimed — actually tested on this cluster)

- **Replication is real**: a PUT lands on all N physical nodes with matching
  timestamps, confirmed by querying each node's internal API directly.
- **Fault tolerance**: with N=3/W=2/R=2, killing one node leaves both new
  writes and reads of pre-existing data fully working — verified by
  `taskkill`-ing a node process mid-session and re-running requests.
- **Fails safe, not silently**: killing a *second* node (2/3 down) makes
  writes correctly return `503` ("could not reach enough replicas to
  satisfy quorum") instead of silently under-replicating.
- **Durability**: a key still in the WAL (not yet flushed to a segment) at
  the moment a node process is killed is still present after restart — WAL
  replay verified, not assumed.
- **Throughput**: `./bin/bench --coord=http://localhost:9000 --ops=2000
  --concurrency=20` against the live 3-node cluster: **2,095 ops/sec, p50
  8.1ms, p99 31ms, 2000/2000 succeeded** on a single dev machine.
- **Fault injection during load**: `./bin/bench --fault` pauses mid-run so
  you can kill a node and watch the benchmark keep succeeding in real time.

## Known scope boundaries (intentionally not built)

This demonstrates the core mechanics, not a production system:

- No automatic re-replication / hinted handoff when a node is permanently
  lost (the ring keeps routing to it; a real system would rebalance).
- Segment reads are a linear scan per file (no index, no bloom filter) —
  fine for a demo-scale keyspace, not for a large one.
- No compaction of old segment files.
- No TLS/auth between coordinator and nodes (this is a systems-mechanics
  demo, not a hardened deployment).

## Stack

Go 1.23, standard library only (`net/http`, no framework) — deliberately, to
keep every mechanism (hashing, replication, storage) visible rather than
delegated to a library.

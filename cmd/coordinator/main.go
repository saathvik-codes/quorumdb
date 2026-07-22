// Command coordinator runs the client-facing QuorumDB coordinator: it owns
// the consistent-hash ring and membership table, and serves quorum
// read/write requests by fanning out to storage nodes.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/saathvik-codes/quorumdb/internal/api"
	"github.com/saathvik-codes/quorumdb/internal/cluster"
	"github.com/saathvik-codes/quorumdb/internal/coordinator"
	"github.com/saathvik-codes/quorumdb/internal/ring"
)

func main() {
	addr := flag.String("addr", ":9000", "address for the client-facing API")
	nodesFlag := flag.String("nodes", "", "comma-separated id=baseURL pairs, e.g. node-1=http://localhost:9101,node-2=http://localhost:9102")
	n := flag.Int("n", 3, "replication factor")
	w := flag.Int("w", 2, "write quorum")
	r := flag.Int("r", 2, "read quorum")
	virtualNodes := flag.Int("vnodes", 100, "virtual nodes per physical node on the hash ring")
	heartbeatMs := flag.Int("heartbeat-ms", 1000, "heartbeat poll interval in milliseconds")
	flag.Parse()

	if *nodesFlag == "" {
		log.Fatal("--nodes is required, e.g. node-1=http://localhost:9101,node-2=http://localhost:9102,node-3=http://localhost:9103")
	}

	hashRing := ring.New(*virtualNodes)
	membership := cluster.NewMembership(time.Duration(*heartbeatMs) * time.Millisecond)

	for _, pair := range strings.Split(*nodesFlag, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid --nodes entry: %q (want id=baseURL)", pair)
		}
		id, url := parts[0], parts[1]
		hashRing.AddNode(id)
		membership.Register(id, url)
		log.Printf("registered %s at %s", id, url)
	}

	membership.Start()
	defer membership.Stop()

	coord := coordinator.New(hashRing, membership, *n, *w, *r)
	server := &api.CoordinatorServer{Coord: coord}

	log.Printf("coordinator listening on %s (N=%d W=%d R=%d)", *addr, *n, *w, *r)
	if err := http.ListenAndServe(*addr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}

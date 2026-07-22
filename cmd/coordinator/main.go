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
	nodesFlag := flag.String("nodes", "", "comma-separated id=baseURL=zone triples (zone optional), e.g. node-1=http://localhost:9101=zone-a,node-2=http://localhost:9102=zone-b")
	n := flag.Int("n", 3, "replication factor")
	w := flag.Int("w", 2, "write quorum")
	r := flag.Int("r", 2, "read quorum")
	virtualNodes := flag.Int("vnodes", 100, "virtual nodes per physical node on the hash ring")
	heartbeatMs := flag.Int("heartbeat-ms", 1000, "heartbeat poll interval in milliseconds")
	flag.Parse()

	if *nodesFlag == "" {
		log.Fatal("--nodes is required, e.g. node-1=http://localhost:9101=zone-a,node-2=http://localhost:9102=zone-b,node-3=http://localhost:9103=zone-c")
	}

	hashRing := ring.New(*virtualNodes)
	membership := cluster.NewMembership(time.Duration(*heartbeatMs) * time.Millisecond)

	for _, triple := range strings.Split(*nodesFlag, ",") {
		parts := strings.SplitN(triple, "=", 3)
		if len(parts) < 2 {
			log.Fatalf("invalid --nodes entry: %q (want id=baseURL or id=baseURL=zone)", triple)
		}
		id, url := parts[0], parts[1]
		zone := ""
		if len(parts) == 3 {
			zone = parts[2]
		}
		hashRing.AddNode(id, zone)
		membership.Register(id, url)
		log.Printf("registered %s at %s (zone=%s)", id, url, hashRing.ZoneOf(id))
	}

	membership.Start()
	defer membership.Stop()

	coord := coordinator.New(hashRing, membership, *n, *w, *r)
	stopHints := coord.StartHintedHandoff(time.Duration(*heartbeatMs) * time.Millisecond)
	defer stopHints()

	server := &api.CoordinatorServer{Coord: coord}

	log.Printf("coordinator listening on %s (N=%d W=%d R=%d)", *addr, *n, *w, *r)
	if err := http.ListenAndServe(*addr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}

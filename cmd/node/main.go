// Command node runs a single QuorumDB storage node: it owns a slice of the
// keyspace (per the coordinator's consistent-hash ring) and exposes an
// internal HTTP API for the coordinator to read/write replicas against.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/saathvik-codes/quorumdb/internal/api"
	"github.com/saathvik-codes/quorumdb/internal/store"
)

func main() {
	id := flag.String("id", "", "unique node ID (e.g. node-1)")
	addr := flag.String("addr", ":9101", "address to listen on")
	dataDir := flag.String("data", "", "data directory for WAL + segments (defaults to ./data/<id>)")
	flag.Parse()

	if *id == "" {
		log.Fatal("--id is required")
	}
	dir := *dataDir
	if dir == "" {
		dir = "data/" + *id
	}

	s, err := store.Open(dir)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	node := &api.NodeServer{ID: *id, Store: s}
	log.Printf("node %s listening on %s (data dir: %s)", *id, *addr, dir)
	if err := http.ListenAndServe(*addr, node.Handler()); err != nil {
		log.Fatal(err)
	}
}

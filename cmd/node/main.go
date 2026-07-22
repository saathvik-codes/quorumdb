// Command node runs a single QuorumDB storage node: it owns a slice of the
// keyspace (per the coordinator's consistent-hash ring) and exposes an
// internal HTTP API for the coordinator to read/write replicas against.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/saathvik-codes/quorumdb/internal/api"
	"github.com/saathvik-codes/quorumdb/internal/store"
)

func main() {
	id := flag.String("id", "", "unique node ID (e.g. node-1)")
	addr := flag.String("addr", ":9101", "address to listen on")
	dataDir := flag.String("data", "", "data directory for WAL + segments (defaults to ./data/<id>)")
	compactEvery := flag.Duration("compact-interval", 30*time.Second, "how often to check whether compaction is worth running (0 disables)")
	compactThreshold := flag.Int("compact-segment-threshold", 4, "run compaction when segment count reaches this many")
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

	if *compactEvery > 0 {
		go runAutoCompaction(*id, s, *compactEvery, *compactThreshold)
	}

	node := api.NewNodeServer(*id, s)
	log.Printf("node %s listening on %s (data dir: %s)", *id, *addr, dir)
	if err := http.ListenAndServe(*addr, node.Handler()); err != nil {
		log.Fatal(err)
	}
}

// runAutoCompaction periodically merges segments once there are enough of
// them to be worth the I/O — a real system would trigger on size/overlap
// heuristics; segment count is a simple, honest stand-in for a demo.
func runAutoCompaction(nodeID string, s *store.Store, interval time.Duration, threshold int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if s.SegmentCount() < threshold {
			continue
		}
		before := s.SegmentCount()
		if err := s.Compact(); err != nil {
			log.Printf("[%s] compaction failed: %v", nodeID, err)
			continue
		}
		log.Printf("[%s] compacted %d segments -> %d", nodeID, before, s.SegmentCount())
	}
}

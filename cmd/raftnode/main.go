// Command raftnode runs a single Raft-replicated KV node: a full peer
// participating in leader election and log replication, serving reads and
// writes directly (no separate coordinator process — the leader itself
// takes client writes and replicates them via Raft before acknowledging).
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/saathvik-codes/quorumdb/internal/raft"
	"github.com/saathvik-codes/quorumdb/internal/raftkv"
	"github.com/saathvik-codes/quorumdb/internal/store"
)

func main() {
	id := flag.String("id", "", "unique node ID (e.g. node-1)")
	addr := flag.String("addr", ":9201", "address to listen on for both client requests and Raft RPCs")
	peersFlag := flag.String("peers", "", "comma-separated id=baseURL pairs for the OTHER nodes, e.g. node-2=http://localhost:9202,node-3=http://localhost:9203")
	dataDir := flag.String("data", "", "data directory for the KV store + raft state (defaults to ./data/<id>)")
	flag.Parse()

	if *id == "" {
		log.Fatal("--id is required")
	}
	dir := *dataDir
	if dir == "" {
		dir = "data/" + *id
	}

	peerBaseURLs := make(map[string]string)
	var peerIDs []string
	if *peersFlag != "" {
		for _, pair := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid --peers entry: %q (want id=baseURL)", pair)
			}
			peerBaseURLs[parts[0]] = parts[1]
			peerIDs = append(peerIDs, parts[0])
		}
	}

	kvStore, err := store.Open(dir + "/kv")
	if err != nil {
		log.Fatalf("failed to open KV store: %v", err)
	}
	defer kvStore.Close()

	raftStorage, err := raft.NewFileStorage(dir + "/raft")
	if err != nil {
		log.Fatalf("failed to open raft storage: %v", err)
	}

	transport := raft.NewHTTPTransport(peerBaseURLs)
	node := raft.NewNode(*id, peerIDs, transport, raftStorage)

	sm := raftkv.New(node, kvStore)

	mux := http.NewServeMux()
	mux.Handle("/", raftkv.Handler(sm))
	mux.Handle("/raft/request-vote", raft.Handler(node))
	mux.Handle("/raft/append-entries", raft.Handler(node))

	node.Start()
	log.Printf("raftnode %s listening on %s (peers: %v, data: %s)", *id, *addr, peerIDs, dir)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

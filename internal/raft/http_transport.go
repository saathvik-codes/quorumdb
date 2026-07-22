package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// HTTPTransport implements Transport by POSTing JSON RPC bodies to peers'
// /raft/request-vote and /raft/append-entries endpoints.
type HTTPTransport struct {
	PeerBaseURLs map[string]string // peerID -> e.g. "http://localhost:9201"
	Client       *http.Client
}

func NewHTTPTransport(peerBaseURLs map[string]string) *HTTPTransport {
	return &HTTPTransport{
		PeerBaseURLs: peerBaseURLs,
		Client:       &http.Client{},
	}
}

func (t *HTTPTransport) SendRequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.post(ctx, peerID, "/raft/request-vote", args, &reply)
	return reply, err
}

func (t *HTTPTransport) SendAppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.post(ctx, peerID, "/raft/append-entries", args, &reply)
	return reply, err
}

func (t *HTTPTransport) post(ctx context.Context, peerID, path string, body any, out any) error {
	base, ok := t.PeerBaseURLs[peerID]
	if !ok {
		return fmt.Errorf("unknown peer %s", peerID)
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer %s returned status %d", peerID, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Handler returns the HTTP handlers a node must expose for peers to reach
// it via HTTPTransport — mount this alongside the node's client-facing API.
func Handler(node *Node) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		reply := node.HandleRequestVote(args)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		reply := node.HandleAppendEntries(args)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	})

	return mux
}

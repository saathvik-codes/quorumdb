// Package api implements the HTTP surface for both storage nodes (internal
// replica API) and the coordinator (client-facing quorum API).
package api

import (
	"encoding/json"
	"net/http"

	"github.com/saathvik-codes/quorumdb/internal/store"
)

type NodeServer struct {
	ID    string
	Store *store.Store
}

type putRequest struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

type getResponse struct {
	Found     bool   `json:"found"`
	Value     string `json:"value,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	NodeID    string `json:"nodeId"`
}

func (n *NodeServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","nodeId":"` + n.ID + `"}`))
	})

	mux.HandleFunc("/internal/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/internal/kv/"):]
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			var req putRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request body", http.StatusBadRequest)
				return
			}
			if err := n.Store.Put(key, []byte(req.Value), req.Timestamp); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)

		case http.MethodDelete:
			var req putRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if err := n.Store.Delete(key, req.Timestamp); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			entry, found := n.Store.Get(key)
			resp := getResponse{Found: found, NodeID: n.ID}
			if found {
				resp.Value = string(entry.Value)
				resp.Timestamp = entry.Timestamp
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return mux
}

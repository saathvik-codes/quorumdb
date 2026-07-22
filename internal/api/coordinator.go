package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/saathvik-codes/quorumdb/internal/coordinator"
)

type CoordinatorServer struct {
	Coord *coordinator.Coordinator
}

type clientPutRequest struct {
	Value string `json:"value"`
}

type clientGetResponse struct {
	Found bool   `json:"found"`
	Value string `json:"value,omitempty"`
}

func (c *CoordinatorServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/kv/"):]
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			var req clientPutRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request body", http.StatusBadRequest)
				return
			}
			if err := c.Coord.Put(key, req.Value); err != nil {
				writeQuorumError(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			value, found, err := c.Coord.Get(key)
			if err != nil {
				writeQuorumError(w, err)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(clientGetResponse{Found: found, Value: value})

		case http.MethodDelete:
			if err := c.Coord.Delete(key); err != nil {
				writeQuorumError(w, err)
				return
			}
			w.WriteHeader(http.StatusOK)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"nodes":              c.Coord.Ring.Nodes(),
			"alive":              c.Coord.Membership.Snapshot(),
			"replicationFactorN": c.Coord.N,
			"writeQuorumW":       c.Coord.W,
			"readQuorumR":        c.Coord.R,
		})
	})

	return mux
}

func writeQuorumError(w http.ResponseWriter, err error) {
	if errors.Is(err, coordinator.ErrQuorumNotMet) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

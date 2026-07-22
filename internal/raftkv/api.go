package raftkv

import (
	"encoding/json"
	"errors"
	"net/http"
)

type putRequest struct {
	Value string `json:"value"`
}

type getResponse struct {
	Found bool   `json:"found"`
	Value string `json:"value,omitempty"`
}

// Handler serves the client-facing KV API. A non-leader node responds with
// 421 Misdirected Request and a `leaderId` hint (there's no perfect HTTP
// status for "wrong node, try this one instead"; 421 is the closest
// standard fit and is what several real HTTP-based Raft KV stores use for
// exactly this).
func Handler(sm *StateMachine) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sm.Status())
	})

	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/kv/"):]
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
			if err := sm.Put(key, req.Value); err != nil {
				writeProposeError(w, sm, err)
				return
			}
			w.WriteHeader(http.StatusOK)

		case http.MethodDelete:
			if err := sm.Delete(key); err != nil {
				writeProposeError(w, sm, err)
				return
			}
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			value, found := sm.Get(key)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(getResponse{Found: found, Value: value})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return mux
}

func writeProposeError(w http.ResponseWriter, sm *StateMachine, err error) {
	if errors.Is(err, ErrNotLeader) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(421) // Misdirected Request
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":    "not leader",
			"leaderId": sm.LeaderID(),
		})
		return
	}
	if errors.Is(err, ErrCommitTimeout) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

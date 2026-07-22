// Package api implements the HTTP surface for both storage nodes (internal
// replica API) and the coordinator (client-facing quorum API).
package api

import (
	"encoding/json"
	"net/http"

	"github.com/saathvik-codes/quorumdb/internal/metrics"
	"github.com/saathvik-codes/quorumdb/internal/store"
)

type NodeServer struct {
	ID      string
	Store   *store.Store
	Metrics *metrics.Registry

	putsTotal        *metrics.Counter
	getsTotal        *metrics.Counter
	getHitsTotal     *metrics.Counter
	getMissesTotal   *metrics.Counter
	deletesTotal     *metrics.Counter
	compactionsTotal *metrics.Counter
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

func NewNodeServer(id string, s *store.Store) *NodeServer {
	reg := metrics.NewRegistry()
	n := &NodeServer{
		ID:      id,
		Store:   s,
		Metrics: reg,

		putsTotal:        reg.NewCounter("qdb_node_puts_total", "Total PUT requests handled by this node"),
		getsTotal:        reg.NewCounter("qdb_node_gets_total", "Total GET requests handled by this node"),
		getHitsTotal:     reg.NewCounter("qdb_node_get_hits_total", "GET requests that found a value"),
		getMissesTotal:   reg.NewCounter("qdb_node_get_misses_total", "GET requests that found nothing"),
		deletesTotal:     reg.NewCounter("qdb_node_deletes_total", "Total DELETE requests handled by this node"),
		compactionsTotal: reg.NewCounter("qdb_node_compactions_total", "Total compactions run on this node"),
	}
	reg.NewGaugeFunc("qdb_node_segment_count", "Current number of on-disk LSM segments", func() float64 {
		return float64(s.SegmentCount())
	})
	return n
}

func (n *NodeServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","nodeId":"` + n.ID + `"}`))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(n.Metrics.Render()))
	})

	mux.HandleFunc("/internal/compact", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		before := n.Store.SegmentCount()
		if err := n.Store.Compact(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n.compactionsTotal.Inc()
		after := n.Store.SegmentCount()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"segmentsBefore": before, "segmentsAfter": after})
	})

	mux.HandleFunc("/internal/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"nodeId": n.ID, "segmentCount": n.Store.SegmentCount()})
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
			n.putsTotal.Inc()
			w.WriteHeader(http.StatusOK)

		case http.MethodDelete:
			var req putRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if err := n.Store.Delete(key, req.Timestamp); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			n.deletesTotal.Inc()
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			entry, found := n.Store.Get(key)
			n.getsTotal.Inc()
			if found {
				n.getHitsTotal.Inc()
			} else {
				n.getMissesTotal.Inc()
			}
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

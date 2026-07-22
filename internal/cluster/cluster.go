// Package cluster tracks node liveness via periodic HTTP heartbeats so the
// coordinator can route around dead nodes instead of blocking on them.
package cluster

import (
	"net/http"
	"sync"
	"time"
)

type Membership struct {
	mu           sync.RWMutex
	addrs        map[string]string // nodeID -> base URL
	alive        map[string]bool
	client       *http.Client
	pollInterval time.Duration
	stop         chan struct{}
}

func NewMembership(pollInterval time.Duration) *Membership {
	return &Membership{
		addrs:        make(map[string]string),
		alive:        make(map[string]bool),
		client:       &http.Client{Timeout: 500 * time.Millisecond},
		pollInterval: pollInterval,
		stop:         make(chan struct{}),
	}
}

func (m *Membership) Register(nodeID, baseURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addrs[nodeID] = baseURL
	m.alive[nodeID] = true
}

func (m *Membership) BaseURL(nodeID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	url, ok := m.addrs[nodeID]
	return url, ok
}

func (m *Membership) IsAlive(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.alive[nodeID]
}

func (m *Membership) Snapshot() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]bool, len(m.alive))
	for k, v := range m.alive {
		out[k] = v
	}
	return out
}

// Start launches the background heartbeat loop; call Stop to end it.
func (m *Membership) Start() {
	go func() {
		ticker := time.NewTicker(m.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ticker.C:
				m.pollOnce()
			}
		}
	}()
}

func (m *Membership) Stop() {
	close(m.stop)
}

func (m *Membership) pollOnce() {
	m.mu.RLock()
	targets := make(map[string]string, len(m.addrs))
	for id, url := range m.addrs {
		targets[id] = url
	}
	m.mu.RUnlock()

	for id, url := range targets {
		resp, err := m.client.Get(url + "/health")
		up := err == nil && resp != nil && resp.StatusCode == http.StatusOK
		if resp != nil {
			resp.Body.Close()
		}
		m.mu.Lock()
		m.alive[id] = up
		m.mu.Unlock()
	}
}

// Package metrics is a minimal, dependency-free Prometheus-exposition-format
// metrics registry — hand-rolled rather than pulling in the official client
// library, so the whole system stays buildable with zero external
// dependencies (see go.mod: still no requires).
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Counter struct {
	name string
	help string
	v    int64
}

func (c *Counter) Inc()         { atomic.AddInt64(&c.v, 1) }
func (c *Counter) Add(n int64)  { atomic.AddInt64(&c.v, n) }
func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.v) }

type GaugeFunc struct {
	name string
	help string
	fn   func() float64
}

type Registry struct {
	mu       sync.Mutex
	counters []*Counter
	gauges   []*GaugeFunc
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
	return c
}

// NewGaugeFunc registers a gauge whose value is computed on demand at scrape
// time (e.g. reading Store.SegmentCount() live) rather than tracked manually.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.mu.Lock()
	r.gauges = append(r.gauges, &GaugeFunc{name: name, help: help, fn: fn})
	r.mu.Unlock()
}

// Render produces Prometheus text-exposition-format output.
func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var sb strings.Builder

	counters := append([]*Counter{}, r.counters...)
	sort.Slice(counters, func(i, j int) bool { return counters[i].name < counters[j].name })
	for _, c := range counters {
		fmt.Fprintf(&sb, "# HELP %s %s\n", c.name, c.help)
		fmt.Fprintf(&sb, "# TYPE %s counter\n", c.name)
		fmt.Fprintf(&sb, "%s %d\n", c.name, c.Value())
	}

	gauges := append([]*GaugeFunc{}, r.gauges...)
	sort.Slice(gauges, func(i, j int) bool { return gauges[i].name < gauges[j].name })
	for _, g := range gauges {
		fmt.Fprintf(&sb, "# HELP %s %s\n", g.name, g.help)
		fmt.Fprintf(&sb, "# TYPE %s gauge\n", g.name)
		fmt.Fprintf(&sb, "%s %v\n", g.name, g.fn())
	}

	return sb.String()
}

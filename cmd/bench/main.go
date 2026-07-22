// Command bench load-tests a running QuorumDB coordinator: it fires
// concurrent PUT/GET requests, reports throughput and p50/p99 latency, and
// (with -fault) pauses mid-run so you can kill a node and watch quorum
// reads/writes keep succeeding.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	coordAddr := flag.String("coord", "http://localhost:9000", "coordinator base URL")
	ops := flag.Int("ops", 2000, "total number of operations to run")
	concurrency := flag.Int("concurrency", 20, "number of concurrent workers")
	fault := flag.Bool("fault", false, "pause halfway through so you can kill a node and observe fault tolerance")
	flag.Parse()

	client := &http.Client{Timeout: 3 * time.Second}

	var successes, failures int64
	latencies := make([]time.Duration, *ops)

	var wg sync.WaitGroup
	jobs := make(chan int, *ops)
	for i := 0; i < *ops; i++ {
		jobs <- i
	}
	close(jobs)

	start := time.Now()

	halfway := *ops / 2
	var pausedOnce int32

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if *fault && i == halfway && atomic.CompareAndSwapInt32(&pausedOnce, 0, 1) {
					fmt.Println()
					fmt.Println("=== FAULT INJECTION PAUSE ===")
					fmt.Println("Kill one node now (Ctrl+C its terminal), then press Enter here to resume the benchmark.")
					fmt.Println("Watch: with N=3/W=2/R=2, one dead node should NOT cause failures below.")
					var discard string
					fmt.Scanln(&discard)
					fmt.Println("=== RESUMING ===")
				}

				key := fmt.Sprintf("bench-key-%d", i)
				value := fmt.Sprintf("value-%d-%d", i, time.Now().UnixNano())

				t0 := time.Now()
				ok := doPut(client, *coordAddr, key, value)
				elapsed := time.Since(t0)
				latencies[i] = elapsed

				if ok {
					atomic.AddInt64(&successes, 1)
				} else {
					atomic.AddInt64(&failures, 1)
				}
			}
		}()
	}

	wg.Wait()
	totalElapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[min(len(latencies)*99/100, len(latencies)-1)]

	fmt.Println()
	fmt.Println("=== Benchmark Results ===")
	fmt.Printf("Total ops:     %d\n", *ops)
	fmt.Printf("Concurrency:   %d\n", *concurrency)
	fmt.Printf("Successes:     %d\n", successes)
	fmt.Printf("Failures:      %d\n", failures)
	fmt.Printf("Total time:    %v\n", totalElapsed)
	fmt.Printf("Throughput:    %.1f ops/sec\n", float64(*ops)/totalElapsed.Seconds())
	fmt.Printf("Latency p50:   %v\n", p50)
	fmt.Printf("Latency p99:   %v\n", p99)

	if failures > 0 {
		log.Printf("NOTE: %d operations failed (likely quorum not met) — expected if more than N-W nodes were down", failures)
	}
}

func doPut(client *http.Client, coordAddr, key, value string) bool {
	body, _ := json.Marshal(map[string]string{"value": value})
	req, err := http.NewRequest(http.MethodPut, coordAddr+"/kv/"+key, bytes.NewReader(body))
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

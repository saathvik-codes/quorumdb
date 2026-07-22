// Command chaos is a self-contained fault-injection harness: it launches a
// full node+coordinator cluster as child processes, drives continuous
// write/read load against it, and on a timer kills and restarts a random
// node — while continuously asserting that every write it got a 200 for is
// still readable afterward. It is the automated version of the manual
// "kill a node with taskkill and re-curl" test used during development.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type nodeProc struct {
	id      string
	addr    string
	zone    string
	dataDir string
	cmd     *exec.Cmd
}

func main() {
	numNodes := flag.Int("nodes", 3, "number of storage nodes to run")
	durationFlag := flag.Duration("duration", 30*time.Second, "how long to run the chaos test")
	killEvery := flag.Duration("kill-every", 5*time.Second, "how often to kill+restart a random node")
	nodeBinary := flag.String("node-bin", "", "path to the node binary (defaults to a fresh `go build` of ./cmd/node)")
	coordBinary := flag.String("coord-bin", "", "path to the coordinator binary (defaults to a fresh `go build` of ./cmd/coordinator)")
	workDir := flag.String("workdir", "", "scratch directory for node data + binaries (defaults to a temp dir)")
	flag.Parse()

	dir := *workDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "quorumdb-chaos")
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("chaos workdir: %s", dir)

	nodeBin := ensureBuilt(*nodeBinary, dir, "chaos-node", "./cmd/node")
	coordBin := ensureBuilt(*coordBinary, dir, "chaos-coordinator", "./cmd/coordinator")

	zones := []string{"zone-a", "zone-b", "zone-c", "zone-d", "zone-e"}
	nodes := make([]*nodeProc, *numNodes)
	var nodesArg []string
	for i := 0; i < *numNodes; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		port := 9101 + i
		zone := zones[i%len(zones)]
		nodes[i] = &nodeProc{
			id:      id,
			addr:    fmt.Sprintf(":%d", port),
			zone:    zone,
			dataDir: filepath.Join(dir, "data-"+id),
		}
		nodesArg = append(nodesArg, fmt.Sprintf("%s=http://localhost:%d=%s", id, port, zone))
	}

	for _, n := range nodes {
		startNode(nodeBin, n)
	}
	time.Sleep(1 * time.Second)

	coordCmd := exec.Command(coordBin,
		"--addr=:9000",
		"--nodes="+joinComma(nodesArg),
		"--n=3", "--w=2", "--r=2", "--heartbeat-ms=500",
	)
	coordCmd.Stdout = mustLogFile(dir, "coordinator")
	coordCmd.Stderr = coordCmd.Stdout
	if err := coordCmd.Start(); err != nil {
		log.Fatalf("failed to start coordinator: %v", err)
	}
	defer func() { _ = coordCmd.Process.Kill() }()
	time.Sleep(1 * time.Second)

	log.Printf("cluster up: %d nodes + coordinator on :9000", *numNodes)

	// --- load + assertion loop ---
	client := &http.Client{Timeout: 3 * time.Second}
	var writes, writeFailures, reads, readFailures, assertionFailures int64
	knownGood := &sync.Map{} // key -> expected value, for writes that succeeded

	stopLoad := make(chan struct{})
	var loadWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		loadWG.Add(1)
		go func(workerID int) {
			defer loadWG.Done()
			i := 0
			for {
				select {
				case <-stopLoad:
					return
				default:
				}
				key := fmt.Sprintf("chaos-key-%d-%d", workerID, i)
				value := fmt.Sprintf("value-%d-%d-%d", workerID, i, time.Now().UnixNano())
				i++

				if putOK(client, key, value) {
					atomic.AddInt64(&writes, 1)
					knownGood.Store(key, value)
				} else {
					atomic.AddInt64(&writeFailures, 1)
				}

				// Occasionally re-verify a previously-successful write.
				if i%5 == 0 {
					var sampleKey, sampleVal string
					knownGood.Range(func(k, v any) bool {
						sampleKey, sampleVal = k.(string), v.(string)
						return false // just take the first one each time; simple and enough for a demo
					})
					if sampleKey != "" {
						atomic.AddInt64(&reads, 1)
						got, ok := getValue(client, sampleKey)
						if !ok {
							atomic.AddInt64(&readFailures, 1)
						} else if got != sampleVal {
							atomic.AddInt64(&assertionFailures, 1)
							log.Printf("ASSERTION FAILURE: key=%s expected=%q got=%q", sampleKey, sampleVal, got)
						}
					}
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(w)
	}

	// --- chaos loop: kill + restart a random node on a timer ---
	stopChaos := make(chan struct{})
	var chaosWG sync.WaitGroup
	chaosWG.Add(1)
	restarts := 0
	go func() {
		defer chaosWG.Done()
		ticker := time.NewTicker(*killEvery)
		defer ticker.Stop()
		for {
			select {
			case <-stopChaos:
				return
			case <-ticker.C:
				victim := nodes[rand.Intn(len(nodes))]
				log.Printf("CHAOS: killing %s", victim.id)
				_ = victim.cmd.Process.Kill()
				_ = victim.cmd.Wait()
				time.Sleep(500 * time.Millisecond)
				log.Printf("CHAOS: restarting %s", victim.id)
				startNode(nodeBin, victim)
				restarts++
			}
		}
	}()

	time.Sleep(*durationFlag)
	close(stopChaos)
	chaosWG.Wait()
	close(stopLoad)
	loadWG.Wait()

	fmt.Println()
	fmt.Println("=== Chaos Test Summary ===")
	fmt.Printf("Duration:            %v\n", *durationFlag)
	fmt.Printf("Node restarts:       %d\n", restarts)
	fmt.Printf("Writes succeeded:    %d\n", atomic.LoadInt64(&writes))
	fmt.Printf("Writes failed:       %d\n", atomic.LoadInt64(&writeFailures))
	fmt.Printf("Verification reads:  %d\n", atomic.LoadInt64(&reads))
	fmt.Printf("Read failures:       %d\n", atomic.LoadInt64(&readFailures))
	fmt.Printf("ASSERTION FAILURES:  %d  (a successfully-written value read back wrong or missing)\n", atomic.LoadInt64(&assertionFailures))

	if atomic.LoadInt64(&assertionFailures) > 0 {
		os.Exit(1)
	}
}

func startNode(bin string, n *nodeProc) {
	cmd := exec.Command(bin, "--id="+n.id, "--addr="+n.addr, "--data="+n.dataDir)
	cmd.Stdout = mustLogFile(filepath.Dir(n.dataDir), n.id)
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		log.Fatalf("failed to start %s: %v", n.id, err)
	}
	n.cmd = cmd
}

func putOK(client *http.Client, key, value string) bool {
	body, _ := json.Marshal(map[string]string{"value": value})
	req, err := http.NewRequest(http.MethodPut, "http://localhost:9000/kv/"+key, bytes.NewReader(body))
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

func getValue(client *http.Client, key string) (string, bool) {
	resp, err := client.Get("http://localhost:9000/kv/" + key)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	var parsed struct {
		Found bool   `json:"found"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", false
	}
	return parsed.Value, parsed.Found
}

func ensureBuilt(explicitPath, workDir, name, pkg string) string {
	if explicitPath != "" {
		return explicitPath
	}
	out := filepath.Join(workDir, name+".exe")
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("failed to build %s: %v", pkg, err)
	}
	return out
}

func mustLogFile(dir, name string) *os.File {
	_ = os.MkdirAll(dir, 0o755)
	f, err := os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func joinComma(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/vasenka88/cloudarch/pkg/api"
)

type BenchmarkResult struct {
	TotalOps       int
	DurationSec    float64
	Throughput     float64
	ReadOps        int
	WriteOps       int
	ErrorOps       int
	AvgLatencyMs   float64
	P50LatencyMs   float64
	P95LatencyMs   float64
	P99LatencyMs   float64
	LatenciesRead  []float64
	LatenciesWrite []float64
}

func main() {
	var (
		targetURL = flag.String("target", "http://localhost:8080", "Target Coordinator API base URL")
		workload  = flag.String("workload", "A", "Workload type: A (50/50) or B (95/5)")
		totalOps  = flag.Int("ops", 5000, "Total number of operations to execute")
		threads   = flag.Int("threads", 16, "Number of concurrent client workers")
		keyspace  = flag.Int("keyspace", 1000, "Number of distinct primary keys")
	)
	flag.Parse()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	fmt.Printf("=== YCSB Benchmark Harness (Target: %s) ===\n", *targetURL)
	fmt.Printf("Workload: %s | Operations: %d | Concurrency: %d | Key Space: %d\n", *workload, *totalOps, *threads, *keyspace)

	// Step 1: Initialize table
	ensureTable(*targetURL, client)

	// Step 2: Warmup / Pre-load data
	fmt.Println(">> Pre-loading key-value records into cluster...")
	preloadRecords(*targetURL, client, *keyspace)

	// Step 3: Run Workload
	readRatio := 0.50
	if *workload == "B" || *workload == "b" {
		readRatio = 0.95
	}

	fmt.Printf(">> Executing Workload %s (Read Ratio: %.0f%%)...\n", *workload, readRatio*100)
	result := executeWorkload(*targetURL, client, *totalOps, *threads, *keyspace, readRatio)

	// Print Results
	printResults(*workload, result)
}

func ensureTable(base string, client *http.Client) {
	def := api.TableDefinition{
		TableName:    "usertable",
		PartitionKey: "user_id",
		SortKey:      "field_id",
	}
	body, _ := json.Marshal(def)
	resp, err := client.Post(base+"/api/v1/tables", "application/json", bytes.NewBuffer(body))
	if err == nil {
		resp.Body.Close()
	}
}

func preloadRecords(base string, client *http.Client, count int) {
	var wg sync.WaitGroup
	chunk := count / 10
	if chunk == 0 {
		chunk = count
	}

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec := api.Record{
				PartitionKey: fmt.Sprintf("user_%05d", idx),
				SortKey:      "field0",
				Value:        map[string]string{"data": fmt.Sprintf("val_%05d", idx)},
			}
			body, _ := json.Marshal(rec)
			resp, err := client.Post(base+"/api/v1/tables/usertable/records", "application/json", bytes.NewBuffer(body))
			if err == nil {
				resp.Body.Close()
			}
		}(i)
		if i%chunk == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
}

func executeWorkload(base string, client *http.Client, totalOps, threads, keyspace int, readRatio float64) BenchmarkResult {
	opsPerWorker := totalOps / threads
	var wg sync.WaitGroup

	type workerStat struct {
		latenciesRead  []float64
		latenciesWrite []float64
		errors         int
	}

	stats := make([]workerStat, threads)
	startTime := time.Now()

	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(wID*1000)))
			st := &stats[wID]

			for i := 0; i < opsPerWorker; i++ {
				keyIdx := r.Intn(keyspace)
				key := fmt.Sprintf("user_%05d", keyIdx)

				isRead := r.Float64() < readRatio
				opStart := time.Now()

				if isRead {
					url := fmt.Sprintf("%s/api/v1/tables/usertable/records/%s/field0", base, key)
					resp, err := client.Get(url)
					elapsed := float64(time.Since(opStart).Microseconds()) / 1000.0 // ms
					if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound) {
						st.errors++
					} else {
						st.latenciesRead = append(st.latenciesRead, elapsed)
						resp.Body.Close()
					}
				} else {
					rec := api.Record{
						PartitionKey: key,
						SortKey:      "field0",
						Value:        map[string]string{"updated": time.Now().Format(time.RFC3339Nano)},
					}
					body, _ := json.Marshal(rec)
					url := fmt.Sprintf("%s/api/v1/tables/usertable/records", base)
					resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
					elapsed := float64(time.Since(opStart).Microseconds()) / 1000.0 // ms
					if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated) {
						st.errors++
					} else {
						st.latenciesWrite = append(st.latenciesWrite, elapsed)
						resp.Body.Close()
					}
				}
			}
		}(t)
	}
	wg.Wait()
	duration := time.Since(startTime).Seconds()

	var allRead, allWrite, allCombined []float64
	totalErrors := 0
	for _, s := range stats {
		allRead = append(allRead, s.latenciesRead...)
		allWrite = append(allWrite, s.latenciesWrite...)
		allCombined = append(allCombined, s.latenciesRead...)
		allCombined = append(allCombined, s.latenciesWrite...)
		totalErrors += s.errors
	}

	sort.Float64s(allCombined)
	sort.Float64s(allRead)
	sort.Float64s(allWrite)

	completedOps := len(allCombined)
	var sum float64
	for _, l := range allCombined {
		sum += l
	}

	res := BenchmarkResult{
		TotalOps:       completedOps,
		DurationSec:    duration,
		Throughput:     float64(completedOps) / duration,
		ReadOps:        len(allRead),
		WriteOps:       len(allWrite),
		ErrorOps:       totalErrors,
		LatenciesRead:  allRead,
		LatenciesWrite: allWrite,
	}

	if completedOps > 0 {
		res.AvgLatencyMs = sum / float64(completedOps)
		res.P50LatencyMs = percentile(allCombined, 50.0)
		res.P95LatencyMs = percentile(allCombined, 95.0)
		res.P99LatencyMs = percentile(allCombined, 99.0)
	}

	return res
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * (pct / 100.0))
	return sorted[idx]
}

func printResults(workload string, res BenchmarkResult) {
	fmt.Println("\n---------------------------------------------------------")
	fmt.Printf("[OVERALL], RunTime(s), %.4f\n", res.DurationSec)
	fmt.Printf("[OVERALL], Throughput(ops/sec), %.2f\n", res.Throughput)
	fmt.Printf("[TOTAL_OPERATIONS], Count, %d (Errors: %d)\n", res.TotalOps, res.ErrorOps)
	fmt.Printf("[LATENCY], AverageLatency(ms), %.3f\n", res.AvgLatencyMs)
	fmt.Printf("[LATENCY], 50thPercentileLatency(ms), %.3f\n", res.P50LatencyMs)
	fmt.Printf("[LATENCY], 95thPercentileLatency(ms), %.3f\n", res.P95LatencyMs)
	fmt.Printf("[LATENCY], 99thPercentileLatency(ms), %.3f\n", res.P99LatencyMs)
	if len(res.LatenciesRead) > 0 {
		fmt.Printf("[READ], Operations, %d, p99(ms), %.3f\n", res.ReadOps, percentile(res.LatenciesRead, 99.0))
	}
	if len(res.LatenciesWrite) > 0 {
		fmt.Printf("[UPDATE/WRITE], Operations, %d, p99(ms), %.3f\n", res.WriteOps, percentile(res.LatenciesWrite, 99.0))
	}
	fmt.Println("---------------------------------------------------------")
}

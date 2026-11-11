// Command raftsim runs seeded chaos simulations and reports the pass count.
//
//	go run ./cmd/raftsim -seeds 5000 -nodes 5
//	go run ./cmd/raftsim -start 1234 -seeds 1     # replay one seed
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/DranzerD/RaftKV/sim"
)

func main() {
	seeds := flag.Int("seeds", 1000, "number of seeds to run")
	start := flag.Int64("start", 1, "first seed")
	nodes := flag.Int("nodes", 3, "cluster size")
	ticks := flag.Int("ticks", 2000, "fault-phase ticks per run")
	clients := flag.Int("clients", 4, "concurrent clients")
	flag.Parse()

	began := time.Now()
	results := sim.RunSeeds(sim.ChaosConfig{Nodes: *nodes, Ticks: *ticks, Clients: *clients}, *start, *seeds)

	var failed, committed, ops, reads, dups, requests int
	var crashes, midReady, torn, snaps, taken, maxLog, partitions, sent, dropped int
	var maxTerm uint64
	for _, r := range results {
		committed += r.Committed
		ops += r.Ops
		reads += r.Reads
		dups += r.Duplicates
		requests += r.Stats.Requests
		crashes += r.Stats.Crashes
		midReady += r.Stats.MidReadyCrashes
		torn += r.Stats.TornWrites
		snaps += r.Stats.SnapshotsInstalled
		taken += r.Stats.SnapshotsTaken
		if r.Stats.MaxLogLen > maxLog {
			maxLog = r.Stats.MaxLogLen
		}
		partitions += r.Partitions
		sent += r.Stats.Sent
		dropped += r.Stats.Dropped
		if r.MaxTerm > maxTerm {
			maxTerm = r.MaxTerm
		}
		if r.Err != nil {
			failed++
			fmt.Println("FAIL", r.Err)
		}
	}

	fmt.Printf("nodes=%d clients=%d seeds=%d..%d ticks=%d\n", *nodes, *clients, *start, *start+int64(*seeds)-1, *ticks)
	fmt.Printf("passed %d/%d in %s\n", len(results)-failed, len(results), time.Since(began).Round(time.Millisecond))
	fmt.Printf("clients:  %d operations completed (%d reads) from %d requests sent\n", ops, reads, requests)
	fmt.Printf("log:      %d commands committed, %d of them retries the session table suppressed\n", committed, dups)
	fmt.Printf("faults:   %d crashes (%d inside Ready handling, %d of those during a WAL write), %d partitions\n",
		crashes, midReady, torn, partitions)
	fmt.Printf("network:  %d of %d messages dropped\n", dropped, sent)
	fmt.Printf("snapshots: %d taken, %d installed on lagging followers; longest log held by any node: %d entries\n",
		taken, snaps, maxLog)
	fmt.Printf("highest term reached: %d\n", maxTerm)
	if failed > 0 {
		os.Exit(1)
	}
}

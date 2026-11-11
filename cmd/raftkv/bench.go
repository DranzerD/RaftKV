package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/DranzerD/RaftKV/client"
	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/server"
)

type loadResult struct {
	ops      int
	errors   int
	elapsed  time.Duration
	p50, p99 time.Duration
}

func (r loadResult) rate() float64 { return float64(r.ops) / r.elapsed.Seconds() }

// load runs the given number of closed-loop clients against the cluster
// for the given time. Each client waits for one answer before sending the
// next command, as the session protocol requires.
func load(servers []string, clients int, d time.Duration, readRatio float64, valueSize int) loadResult {
	value := strings.Repeat("v", valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var mu sync.Mutex
	var all []time.Duration
	var errs int
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := client.New(servers)
			defer c.Close()
			var lat []time.Duration
			failed := 0
			readEvery := 0
			if readRatio > 0 {
				readEvery = int(1 / readRatio)
			}
			for n := 0; ctx.Err() == nil; n++ {
				key := fmt.Sprintf("key-%d-%d", i, n%1000)
				op, val := kv.OpPut, value
				if readEvery > 0 && n%readEvery == 0 {
					op, val = kv.OpGet, ""
				}
				t0 := time.Now()
				if _, err := c.Do(ctx, op, key, val, ""); err != nil {
					if ctx.Err() == nil {
						failed++
					}
					continue
				}
				lat = append(lat, time.Since(t0))
			}
			mu.Lock()
			all = append(all, lat...)
			errs += failed
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	res := loadResult{ops: len(all), errors: errs, elapsed: time.Since(start)}
	if len(all) > 0 {
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		res.p50 = all[len(all)/2]
		res.p99 = all[len(all)*99/100]
	}
	return res
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d.Microseconds())/1000) }

func bench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	servers := serverList(fs)
	local := fs.Bool("local", false, "run the full suite on in-process clusters instead")
	clients := fs.Int("clients", 32, "concurrent clients")
	duration := fs.Duration("duration", 5*time.Second, "length of each measurement")
	reads := fs.Float64("reads", 0, "fraction of operations that are reads")
	valueSize := fs.Int("value-size", 64, "bytes per value")
	trials := fs.Int("failover-trials", 5, "leader kills per failover measurement (-local only)")
	fs.Parse(args)

	if !*local {
		r := load(splitServers(*servers), *clients, *duration, *reads, *valueSize)
		fmt.Printf("%d clients, %s, %.0f%% reads, %d-byte values\n", *clients, *duration, *reads*100, *valueSize)
		fmt.Printf("%.0f ops/s, p50 %s ms, p99 %s ms, %d errors\n", r.rate(), ms(r.p50), ms(r.p99), r.errors)
		return
	}
	suite(*clients, *duration, *valueSize, *trials)
}

// suite measures in-process clusters on loopback. All nodes share one
// machine and one disk, so the numbers show the relative cost of cluster
// size, batching and fsync, not what separate machines would do.
func suite(clients int, d time.Duration, valueSize, trials int) {
	dir, err := os.MkdirTemp("", "raftkv-bench-")
	if err != nil {
		die("%v", err)
	}
	defer os.RemoveAll(dir)
	run := 0
	start := func(nodes int, tweak func(*server.Config)) *server.Cluster {
		run++
		c, err := server.StartLocal(nodes, filepath.Join(dir, fmt.Sprintf("run%d", run)), tweak)
		if err != nil {
			die("%v", err)
		}
		if c.WaitLeader(10*time.Second) < 0 {
			die("no leader")
		}
		return c
	}

	fmt.Printf("in-process clusters on loopback, %d closed-loop clients, %s per row, %d-byte values\n\n",
		clients, d, valueSize)
	fmt.Println("WRITES")
	fmt.Printf("%-6s %-9s %-6s %10s %10s %10s\n", "nodes", "batching", "fsync", "ops/s", "p50 ms", "p99 ms")
	for _, nodes := range []int{3, 5} {
		for _, batching := range []bool{true, false} {
			for _, fsync := range []bool{true, false} {
				c := start(nodes, func(cfg *server.Config) {
					cfg.NoSync = !fsync
					if !batching {
						cfg.MaxBatch = 1
					}
				})
				r := load(c.Addrs(), clients, d, 0, valueSize)
				c.StopAll()
				fmt.Printf("%-6d %-9s %-6s %10.0f %10s %10s\n",
					nodes, onOff(batching), onOff(fsync), r.rate(), ms(r.p50), ms(r.p99))
			}
		}
	}

	fmt.Println("\nLINEARIZABLE READS (ReadIndex, no log entry, no fsync)")
	fmt.Printf("%-6s %10s %10s %10s\n", "nodes", "ops/s", "p50 ms", "p99 ms")
	for _, nodes := range []int{3, 5} {
		c := start(nodes, nil)
		load(c.Addrs(), 4, 300*time.Millisecond, 0, valueSize) // something to read
		r := load(c.Addrs(), clients, d, 1, valueSize)
		c.StopAll()
		fmt.Printf("%-6d %10.0f %10s %10s\n", nodes, r.rate(), ms(r.p50), ms(r.p99))
	}

	fmt.Println("\nFAILOVER (leader killed; time until a write sent afterwards succeeds)")
	fmt.Printf("%-6s %10s %10s %10s   election timeout 200-400 ms\n", "nodes", "min ms", "median ms", "max ms")
	for _, nodes := range []int{3, 5} {
		c := start(nodes, nil)
		cl := client.New(c.Addrs())
		ctx := context.Background()
		var times []time.Duration
		for t := 0; t < trials; t++ {
			if err := cl.Put(ctx, "warm", "up"); err != nil {
				die("%v", err)
			}
			l := c.WaitLeader(10 * time.Second)
			c.Stop(l)
			t0 := time.Now()
			if err := cl.Put(ctx, "after", fmt.Sprint(t)); err != nil {
				die("%v", err)
			}
			times = append(times, time.Since(t0))
			if err := c.Restart(l); err != nil {
				die("%v", err)
			}
			time.Sleep(500 * time.Millisecond) // let it rejoin before the next kill
		}
		cl.Close()
		c.StopAll()
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		fmt.Printf("%-6d %10s %10s %10s\n", nodes, ms(times[0]), ms(times[len(times)/2]), ms(times[len(times)-1]))
	}
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

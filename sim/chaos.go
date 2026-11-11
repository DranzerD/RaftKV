package sim

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"math/rand"
	"runtime"
	"sync"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
)

// ChaosConfig describes one randomized fault-injection run.
type ChaosConfig struct {
	Nodes   int   // default 3
	Seed    int64 // the whole run is a function of this
	Ticks   int   // length of the fault phase; default 2000
	Clients int   // default 4
}

// Result summarises a chaos run.
type Result struct {
	Seed       int64
	Err        error // nil if every check passed and the cluster recovered
	Ticks      uint64
	MaxTerm    uint64
	Ops        int // client operations completed
	Reads      int // of which gets, served without a log entry
	Committed  int // client commands in the committed log, retries included
	Duplicates int // of which retries that the session table suppressed
	Partitions int
	Stats      Stats
	Digest     uint64 // hash of the committed log, for determinism checks
}

var marker = []byte("\x00recovery-marker")

// RunChaos runs a cluster and a set of retrying clients through a random
// schedule of crashes, restarts and partitions over a lossy, duplicating,
// reordering network. The safety invariants are checked after every tick.
// It then heals everything, requires every client operation to finish, and
// checks the client-observed history for linearizability.
func RunChaos(cfg ChaosConfig) (res Result) {
	// The core panics rather than overwrite a committed entry. Report that
	// as a failed seed instead of taking the whole sweep down.
	defer func() {
		if p := recover(); p != nil {
			res.Seed = cfg.Seed
			res.Err = fmt.Errorf("seed %d: panic: %v", cfg.Seed, p)
		}
	}()
	return runChaos(cfg)
}

func runChaos(cfg ChaosConfig) Result {
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.Ticks <= 0 {
		cfg.Ticks = 2000
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 4
	}
	s := New(Options{
		Nodes:    cfg.Nodes,
		Seed:     cfg.Seed,
		MinDelay: 1,
		MaxDelay: 5,
		DropProb: 0.05,
		DupProb:  0.05,
		// Three seeds in four use tiny append batches, so followers
		// often hold only part of a leader's log. That is what exposes
		// commit-rule bugs such as the Figure 8 case.
		MaxEntries: []int{0, 1, 2, 4}[uint64(cfg.Seed)%4],
		// On top of the scheduled crashes below, kill nodes inside Ready
		// handling, including in the middle of a WAL write.
		CrashPointProb: 0.002,
		// Three seeds in four also compact their logs, at intervals short
		// enough that crashed and partitioned nodes regularly come back
		// to find the entries they need gone.
		SnapshotEvery: []int{0, 8, 20, 50}[(uint64(cfg.Seed)>>2)%4],
	})
	s.AddClients(cfg.Clients)
	// The fault schedule has its own stream so that changing how the
	// network consumes randomness does not reshuffle the schedule.
	r := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed5eed))
	res := Result{Seed: cfg.Seed}
	ids := s.IDs()

	finish := func(err error) Result {
		res.Ticks = s.Now()
		res.Stats = s.Stats()
		h := fnv.New64a()
		for _, e := range s.Committed() {
			fmt.Fprintf(h, "%d/%d/%d/", e.Index, e.Term, e.Type)
			h.Write(e.Data)
			if e.Type == raft.EntryNormal && !bytes.Equal(e.Data, marker) {
				res.Committed++
			}
		}
		res.Digest = h.Sum64()
		res.Duplicates = s.check.dups
		for _, id := range ids {
			if t := s.nodes[id].hs.Term; t > res.MaxTerm {
				res.MaxTerm = t
			}
		}
		for _, op := range s.History() {
			if op.Done {
				res.Ops++
				if op.Cmd.Op == kv.OpGet {
					res.Reads++
				}
			}
		}
		if err == nil && !CheckLinearizable(s.History()) {
			err = fmt.Errorf("seed %d: linearizability violated: no single-copy order explains the %d client operations",
				cfg.Seed, len(s.History()))
		}
		res.Err = err
		return res
	}

	// A few keys, so that clients collide constantly.
	keys := []string{"a", "b", "c"}
	lastWritten := map[string]string{}
	values := 0
	startOp := func(i int) {
		key := keys[r.Intn(len(keys))]
		values++
		val := fmt.Sprintf("v%d", values)
		switch p := r.Float64(); {
		case p < 0.35:
			s.ClientDo(i, kv.OpGet, key, "", "")
		case p < 0.70:
			s.ClientDo(i, kv.OpPut, key, val, "")
			lastWritten[key] = val
		case p < 0.80:
			s.ClientDo(i, kv.OpDelete, key, "", "")
		default:
			// Usually expect the latest value some client tried to
			// write, so that a fair share of swaps succeed.
			expect := lastWritten[key]
			if r.Intn(4) == 0 {
				expect = "nope"
			}
			s.ClientDo(i, kv.OpCAS, key, val, expect)
			lastWritten[key] = val
		}
	}

	// Phase 1: chaos.
	for t := 0; t < cfg.Ticks; t++ {
		for i := 0; i < cfg.Clients; i++ {
			if s.ClientIdle(i) && r.Float64() < 0.25 {
				startOp(i)
			}
		}
		if r.Float64() < 0.01 {
			s.Crash(ids[r.Intn(len(ids))])
		}
		for _, id := range ids {
			if !s.Alive(id) && r.Float64() < 0.03 {
				s.Restart(id)
			}
		}
		if r.Float64() < 0.008 {
			s.Heal()
			res.Partitions++
			switch r.Intn(3) {
			case 0: // split into two groups
				p := r.Perm(len(ids))
				k := 1 + r.Intn(len(ids)-1)
				var a, b []uint64
				for i, j := range p {
					if i < k {
						a = append(a, ids[j])
					} else {
						b = append(b, ids[j])
					}
				}
				s.Partition(a, b)
			case 1: // one node can hear but not speak
				x := ids[r.Intn(len(ids))]
				for _, y := range ids {
					if y != x {
						s.BlockLink(x, y)
					}
				}
			case 2: // one node can speak but not hear
				x := ids[r.Intn(len(ids))]
				for _, y := range ids {
					if y != x {
						s.BlockLink(y, x)
					}
				}
			}
		} else if s.Partitioned() && r.Float64() < 0.02 {
			s.Heal()
		}
		if err := s.Step(); err != nil {
			return finish(err)
		}
	}

	// Phase 2: heal everything and demand progress.
	s.Heal()
	s.SetCrashPoints(0)
	for _, id := range ids {
		s.Restart(id)
	}
	s.SetNetwork(0, 0, 1, 5)

	const recoveryBudget = 5000
	for t := 0; t < recoveryBudget; t++ {
		if t%50 == 0 {
			if l := s.Leader(); l != raft.None {
				s.Propose(l, marker)
			}
		}
		if err := s.Step(); err != nil {
			return finish(err)
		}
		if s.ClientsIdle() && converged(s) {
			return finish(nil)
		}
	}
	return finish(fmt.Errorf("seed %d: liveness: no convergence within %d ticks of healing all faults",
		cfg.Seed, recoveryBudget))
}

// converged reports whether a marker written after the heal has committed
// and every node has applied the full committed log.
func converged(s *Sim) bool {
	all := s.check.committed
	seen := false
	for i := len(all) - 1; i >= 0 && !seen; i-- {
		seen = bytes.Equal(all[i].entry.Data, marker)
	}
	if !seen {
		return false
	}
	for _, id := range s.ids {
		if s.nodes[id].appliedIndex != uint64(len(all)) {
			return false
		}
	}
	return true
}

// RunSeeds runs count chaos runs with seeds start, start+1, ... across all
// CPUs. Results come back in seed order.
func RunSeeds(base ChaosConfig, start int64, count int) []Result {
	out := make([]Result, count)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				cfg := base
				cfg.Seed = start + int64(i)
				out[i] = RunChaos(cfg)
			}
		}()
	}
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return out
}

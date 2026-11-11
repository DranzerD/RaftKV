package sim

import (
	"bytes"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
)

var (
	chaosSeeds = flag.Int("chaos.seeds", 200, "chaos runs per cluster size")
	chaosStart = flag.Int64("chaos.start", 1, "first chaos seed")
	chaosTicks = flag.Int("chaos.ticks", 2000, "fault-phase ticks per chaos run")
)

func waitLeader(t *testing.T, s *Sim) uint64 {
	t.Helper()
	ok, err := s.RunUntil(func() bool { return s.Leader() != raft.None }, 500)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no leader elected within 500 ticks")
	}
	return s.Leader()
}

func others(s *Sim, id uint64) []uint64 {
	var out []uint64
	for _, x := range s.IDs() {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func commands(entries []raft.Entry) []string {
	var out []string
	for _, e := range entries {
		if e.Type == raft.EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func allApplied(s *Sim) bool {
	want := len(s.Committed())
	for _, id := range s.IDs() {
		if !s.Alive(id) || len(s.Applied(id)) != want {
			return false
		}
	}
	return true
}

// Milestone 1: one leader per term, and everyone agrees on who it is.
func TestElectsSingleLeader(t *testing.T) {
	for _, size := range []int{3, 5} {
		for seed := int64(1); seed <= 20; seed++ {
			s := New(Options{Nodes: size, Seed: seed})
			lead := waitLeader(t, s)
			if err := s.Run(100); err != nil {
				t.Fatal(err)
			}
			if got := s.Leaders(); len(got) != 1 {
				t.Fatalf("size %d seed %d: leaders = %v, want exactly one", size, seed, got)
			}
			lead = s.Leader()
			term := s.Node(lead).Term()
			for _, id := range s.IDs() {
				n := s.Node(id)
				if n.Leader() != lead || n.Term() != term {
					t.Fatalf("size %d seed %d: node %d sees leader %d term %d, want %d term %d",
						size, seed, id, n.Leader(), n.Term(), lead, term)
				}
			}
		}
	}
}

// Milestone 1: the cluster recovers from losing its leader.
func TestRecoversFromLeaderLoss(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		s := New(Options{Nodes: 5, Seed: seed})
		old := waitLeader(t, s)
		oldTerm := s.Node(old).Term()
		s.Crash(old)

		next := waitLeader(t, s)
		if next == old || s.Node(next).Term() <= oldTerm {
			t.Fatalf("seed %d: new leader %d term %d does not supersede %d term %d",
				seed, next, s.Node(next).Term(), old, oldTerm)
		}

		s.Restart(old)
		ok, err := s.RunUntil(func() bool {
			n := s.Node(old)
			return n.Role() == raft.Follower && n.Leader() == s.Leader() && s.Leader() != raft.None
		}, 500)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("seed %d: restarted node %d never rejoined as a follower", seed, old)
		}
	}
}

// Milestone 2: entries apply in the same order on every node.
func TestReplicatesInOrder(t *testing.T) {
	for _, size := range []int{3, 5} {
		s := New(Options{Nodes: size, Seed: 7, MinDelay: 1, MaxDelay: 4})
		lead := waitLeader(t, s)
		var want []string
		for i := 0; i < 100; i++ {
			cmd := fmt.Sprintf("cmd-%d", i)
			if !s.Propose(lead, []byte(cmd)) {
				t.Fatalf("leader %d refused proposal %d", lead, i)
			}
			want = append(want, cmd)
			if err := s.Run(1); err != nil {
				t.Fatal(err)
			}
		}
		ok, err := s.RunUntil(func() bool {
			return len(commands(s.Committed())) == len(want) && allApplied(s)
		}, 500)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("size %d: cluster did not apply all %d commands", size, len(want))
		}
		for _, id := range s.IDs() {
			got := commands(s.Applied(id))
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("size %d: node %d applied %v, want %v", size, id, got, want)
			}
		}
	}
}

// A follower that missed a stretch of the log catches up once it can hear
// the leader again.
func TestLaggingFollowerCatchesUp(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 3})
	lead := waitLeader(t, s)
	lagger := others(s, lead)[0]
	s.Partition([]uint64{lagger}, others(s, lagger))

	for i := 0; i < 150; i++ { // more than one MsgApp can carry
		s.Propose(s.Leader(), []byte(fmt.Sprintf("cmd-%d", i)))
		if err := s.Run(1); err != nil {
			t.Fatal(err)
		}
	}
	// Let the tail commit: the healed node's higher term will force an
	// election, and uncommitted entries may legitimately be lost in it.
	if err := s.Run(50); err != nil {
		t.Fatal(err)
	}
	if got := len(commands(s.Applied(lagger))); got != 0 {
		t.Fatalf("isolated node applied %d commands", got)
	}

	s.Heal()
	ok, err := s.RunUntil(func() bool {
		return allApplied(s) && len(commands(s.Committed())) == 150
	}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("node %d applied %d of %d entries after healing",
			lagger, len(s.Applied(lagger)), len(s.Committed()))
	}
}

// A leader cut off from the majority cannot commit, and what it accepted
// while isolated is discarded when it rejoins.
func TestIsolatedLeaderCannotCommit(t *testing.T) {
	s := New(Options{Nodes: 5, Seed: 11})
	old := waitLeader(t, s)
	rest := others(s, old)
	s.Partition([]uint64{old}, rest)

	if !s.Propose(old, []byte("lost")) {
		t.Fatal("isolated leader should still accept the proposal locally")
	}
	ok, err := s.RunUntil(func() bool {
		l := s.Leader()
		return l != raft.None && l != old
	}, 500)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("majority side never elected a new leader")
	}
	if !s.Propose(s.Leader(), []byte("kept")) {
		t.Fatal("new leader refused proposal")
	}

	s.Heal()
	ok, err = s.RunUntil(func() bool {
		return allApplied(s) && s.Node(old).Role() == raft.Follower &&
			bytes.Contains([]byte(strings.Join(commands(s.Committed()), ",")), []byte("kept"))
	}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cluster did not converge after healing")
	}
	for _, id := range s.IDs() {
		got := strings.Join(commands(s.Applied(id)), ",")
		if got != "kept" {
			t.Fatalf("node %d applied %q, want only %q", id, got, "kept")
		}
	}
}

// The checker must not be vacuous. A disk that drops an acknowledged entry
// breaks Raft's assumptions, and the checker has to notice the fallout.
func TestCheckerCatchesLostWrite(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 5})
	lead := waitLeader(t, s)
	fs := others(s, lead)
	acker, isolated := fs[0], fs[1]

	if err := s.Run(50); err != nil { // let the no-op reach everyone
		t.Fatal(err)
	}
	s.Partition([]uint64{isolated}, []uint64{lead, acker})
	base := len(s.Committed())
	if !s.Propose(lead, []byte("x")) {
		t.Fatal("leader refused proposal")
	}
	ok, err := s.RunUntil(func() bool {
		return len(s.Committed()) > base && len(s.DiskLog(acker)) > len(s.DiskLog(isolated))
	}, 200)
	if err != nil || !ok {
		t.Fatalf("x was not committed by the two connected nodes: ok=%v err=%v", ok, err)
	}

	// The acker "forgets" x, the old leader dies, and the two nodes that
	// lack x now form a majority.
	s.Crash(acker)
	s.TruncateDisk(acker, s.DiskLast(isolated))
	s.Crash(lead)
	s.Heal()
	s.Restart(acker)

	err = s.Run(1000)
	if err == nil {
		t.Fatal("checker missed a committed entry vanishing from the new majority")
	}
	if !strings.Contains(err.Error(), "leader completeness") {
		t.Fatalf("expected a leader completeness violation, got: %v", err)
	}
	t.Logf("caught as expected: %v", err)
}

// The same seed must replay the same run, or failing seeds are useless.
func TestDeterministicReplay(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		a := RunChaos(ChaosConfig{Nodes: 5, Seed: seed, Ticks: 1000})
		b := RunChaos(ChaosConfig{Nodes: 5, Seed: seed, Ticks: 1000})
		if a.Digest != b.Digest || a.Ticks != b.Ticks || a.Stats != b.Stats || a.MaxTerm != b.MaxTerm {
			t.Fatalf("seed %d diverged:\n%+v\n%+v", seed, a, b)
		}
	}
	a := RunChaos(ChaosConfig{Nodes: 5, Seed: 1, Ticks: 1000})
	b := RunChaos(ChaosConfig{Nodes: 5, Seed: 2, Ticks: 1000})
	if a.Digest == b.Digest {
		t.Fatal("different seeds produced identical runs; the seed is not being used")
	}
}

// Milestone 2 under fire: random crashes, partitions, loss, duplication
// and reordering, with every invariant checked after every tick.
//
//	go test ./sim -run TestChaos -chaos.seeds=5000
func TestChaos(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("nodes=%d", size), func(t *testing.T) {
			results := RunSeeds(ChaosConfig{Nodes: size, Ticks: *chaosTicks}, *chaosStart, *chaosSeeds)
			var failed, committed, crashes, torn, partitions int
			for _, r := range results {
				committed += r.Committed
				crashes += r.Stats.Crashes
				torn += r.Stats.TornWrites
				partitions += r.Partitions
				if r.Err != nil {
					failed++
					if failed <= 10 {
						t.Errorf("%v", r.Err)
					}
				}
			}
			t.Logf("%d/%d seeds passed; %d commands committed, %d crashes (%d during a WAL write), %d partitions",
				len(results)-failed, len(results), committed, crashes, torn, partitions)
			if torn == 0 && len(results) >= 50 {
				t.Errorf("no crash ever interrupted a WAL write; the torn-write path is untested")
			}
		})
	}
}

// Milestone 3: a full-cluster power cut loses nothing that was committed.
func TestWholeClusterRestartKeepsCommittedData(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 21})
	lead := waitLeader(t, s)
	var want []string
	for i := 0; i < 50; i++ {
		cmd := fmt.Sprintf("cmd-%d", i)
		s.Propose(lead, []byte(cmd))
		want = append(want, cmd)
		if err := s.Run(1); err != nil {
			t.Fatal(err)
		}
	}
	ok, err := s.RunUntil(func() bool { return len(commands(s.Committed())) == len(want) }, 500)
	if err != nil || !ok {
		t.Fatalf("commands did not commit: ok=%v err=%v", ok, err)
	}

	for _, id := range s.IDs() {
		s.Crash(id)
	}
	for _, id := range s.IDs() {
		s.Restart(id)
		if got := len(s.Applied(id)); got != 0 {
			t.Fatalf("node %d kept %d applied entries across a crash", id, got)
		}
	}
	ok, err = s.RunUntil(func() bool { return s.Leader() != raft.None && allApplied(s) }, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cluster did not recover after a full restart")
	}
	for _, id := range s.IDs() {
		if got := commands(s.Applied(id)); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("node %d recovered %d commands, want the original %d in order", id, len(got), len(want))
		}
	}
}

// Milestone 3: nodes killed inside Ready handling, including mid-write,
// recover exactly what their WAL acknowledged and the cluster loses nothing.
func TestCrashPointsLoseNoCommittedData(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		s := New(Options{Nodes: 5, Seed: seed, MinDelay: 1, MaxDelay: 3, CrashPointProb: 0.02})
		for tick := 0; tick < 1500; tick++ {
			for _, id := range s.IDs() {
				if !s.Alive(id) && tick%7 == 0 {
					s.Restart(id)
				}
			}
			if l := s.Leader(); l != raft.None && tick%3 == 0 {
				s.Propose(l, []byte(fmt.Sprintf("cmd-%d", tick)))
			}
			if err := s.Step(); err != nil {
				t.Fatal(err)
			}
		}
		st := s.Stats()
		if st.TornWrites == 0 || st.MidReadyCrashes == st.TornWrites {
			t.Fatalf("seed %d: crash points not exercised: %+v", seed, st)
		}
		if len(commands(s.Committed())) == 0 {
			t.Fatalf("seed %d: nothing committed", seed)
		}
	}
}

// The durability check must not be vacuous either: a disk that lies about
// fsync has to be caught the first time a node comes back short.
func TestCheckerCatchesLyingFsync(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 9, FsyncLies: true})
	lead := waitLeader(t, s)
	for i := 0; i < 20; i++ {
		s.Propose(lead, []byte(fmt.Sprintf("cmd-%d", i)))
		if err := s.Run(2); err != nil {
			t.Fatal(err)
		}
	}
	victim := others(s, lead)[0]
	for i := 0; i < 20 && s.Err() == nil; i++ {
		s.Crash(victim)
		s.Restart(victim)
		s.Run(10)
	}
	err := s.Err()
	if err == nil {
		t.Fatal("a node lost fsynced state across a crash and the checker did not notice")
	}
	if !strings.Contains(err.Error(), "durability") {
		t.Fatalf("expected a durability violation, got: %v", err)
	}
	t.Logf("caught as expected: %v", err)
}

func clientDo(t *testing.T, s *Sim, i int, op kv.Op, key, value, expect string) kv.Result {
	t.Helper()
	if !s.ClientDo(i, op, key, value, expect) {
		t.Fatalf("client %d is busy", i)
	}
	ok, err := s.RunUntil(func() bool { return s.ClientIdle(i) }, 3000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("client %d: %v %q never completed", i, op, key)
	}
	h := s.History()
	return h[len(h)-1].Result
}

// Milestone 4: over a network that loses and duplicates a third of all
// messages, every write still takes effect exactly once.
func TestRetriedWritesApplyOnce(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 4, MinDelay: 1, MaxDelay: 4, DropProb: 0.3, DupProb: 0.3})
	s.AddClients(1)
	clientDo(t, s, 0, kv.OpPut, "n", "0", "")
	const rounds = 40
	for i := 0; i < rounds; i++ {
		// A compare-and-swap that ran twice would fail the second time
		// and report failure for a swap that did happen.
		res := clientDo(t, s, 0, kv.OpCAS, "n", fmt.Sprint(i+1), fmt.Sprint(i))
		if !res.OK {
			t.Fatalf("increment %d reported failure: %+v", i, res)
		}
	}
	if got := clientDo(t, s, 0, kv.OpGet, "n", "", ""); got.Value != fmt.Sprint(rounds) {
		t.Fatalf("counter = %q, want %d", got.Value, rounds)
	}
	if s.check.dups == 0 {
		t.Fatal("no retry ever reached the log; the test did not exercise deduplication")
	}
	if !CheckLinearizable(s.History()) {
		t.Fatal("history is not linearizable")
	}
	t.Logf("%d operations took %d requests; %d duplicate log entries suppressed",
		len(s.History()), s.Stats().Requests, s.check.dups)
}

// Milestone 5: a leader cut off from the majority must not answer reads,
// because the value it holds may already be stale.
func TestDeposedLeaderServesNoReads(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 6})
	s.AddClients(2)
	clientDo(t, s, 0, kv.OpPut, "k", "old", "")
	old := s.Leader()
	s.Partition([]uint64{old}, others(s, old))

	// The majority moves on and overwrites the key.
	ok, err := s.RunUntil(func() bool { l := s.Leader(); return l != raft.None && l != old }, 500)
	if err != nil || !ok {
		t.Fatalf("no new leader: ok=%v err=%v", ok, err)
	}
	s.ClientPin(0, s.Leader())
	clientDo(t, s, 0, kv.OpPut, "k", "new", "")
	if s.Node(old).Role() != raft.Leader {
		t.Fatal("test setup: the isolated node should still believe it leads")
	}

	// Ask the isolated node, which still holds "old".
	s.ClientPin(1, old)
	s.ClientDo(1, kv.OpGet, "k", "", "")
	if err := s.Run(300); err != nil {
		t.Fatal(err)
	}
	if s.ClientIdle(1) {
		h := s.History()
		t.Fatalf("isolated leader answered a read with %+v", h[len(h)-1].Result)
	}

	s.Heal()
	s.ClientUnpin(1)
	ok, err = s.RunUntil(func() bool { return s.ClientIdle(1) }, 2000)
	if err != nil || !ok {
		t.Fatalf("read never completed after healing: ok=%v err=%v", ok, err)
	}
	h := s.History()
	if got := h[len(h)-1].Result; got.Value != "new" {
		t.Fatalf("read returned %+v, want the new value", got)
	}
	if !CheckLinearizable(h) {
		t.Fatal("history is not linearizable")
	}
}

// The linearizability checker must reject a history with a stale read.
func TestLinearizabilityCheckerRejectsStaleRead(t *testing.T) {
	put := func(seq uint64, v string) kv.Command {
		return kv.Command{Client: 1, Seq: seq, Op: kv.OpPut, Key: "k", Value: v}
	}
	get := kv.Command{Client: 2, Seq: 1, Op: kv.OpGet, Key: "k"}
	history := []Operation{
		{Client: 1, Cmd: put(1, "old"), Result: kv.Result{OK: true}, Call: 1, Return: 2, Done: true},
		{Client: 1, Cmd: put(2, "new"), Result: kv.Result{OK: true}, Call: 3, Return: 4, Done: true},
		{Client: 2, Cmd: get, Result: kv.Result{Found: true, Value: "old"}, Call: 5, Return: 6, Done: true},
	}
	if CheckLinearizable(history) {
		t.Fatal("accepted a read of a value that had already been overwritten")
	}
	history[2].Result.Value = "new"
	if !CheckLinearizable(history) {
		t.Fatal("rejected a correct history")
	}
}

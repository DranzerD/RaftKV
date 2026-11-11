package sim

import (
	"fmt"
	"testing"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
)

// Milestone 6: logs stay bounded, and a follower that falls behind the
// compaction point is brought back with a snapshot.
func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 12, SnapshotEvery: 10})
	s.AddClients(1)
	clientDo(t, s, 0, kv.OpPut, "k", "0", "")
	lagger := others(s, s.Leader())[0]
	s.Partition([]uint64{lagger}, others(s, lagger))

	const writes = 120
	for i := 1; i <= writes; i++ {
		clientDo(t, s, 0, kv.OpPut, fmt.Sprintf("k%d", i%7), fmt.Sprint(i), "")
	}
	behind := s.AppliedIndex(lagger)

	s.Heal()
	ok, err := s.RunUntil(func() bool {
		l := s.Leader()
		return l != raft.None && s.AppliedIndex(lagger) == s.AppliedIndex(l) &&
			s.AppliedIndex(lagger) >= uint64(writes)
	}, 3000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("node %d stuck at index %d", lagger, s.AppliedIndex(lagger))
	}
	st := s.Stats()
	if st.SnapshotsInstalled == 0 {
		t.Fatalf("node %d caught up from index %d without a snapshot; compaction is not happening", lagger, behind)
	}
	if st.MaxLogLen > 40 {
		t.Fatalf("a log grew to %d entries with a snapshot every 10", st.MaxLogLen)
	}
	want := s.Data(s.Leader())
	if got := s.Data(lagger); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after the snapshot node %d holds %v, leader holds %v", lagger, got, want)
	}
	t.Logf("%d snapshots taken, %d installed, longest log %d entries for %d writes",
		st.SnapshotsTaken, st.SnapshotsInstalled, st.MaxLogLen, writes)
}

// A node restarted from a snapshot plus a log tail must come back with the
// same state, sessions included.
func TestRestartFromSnapshot(t *testing.T) {
	s := New(Options{Nodes: 3, Seed: 14, SnapshotEvery: 10})
	s.AddClients(1)
	for i := 0; i < 35; i++ {
		clientDo(t, s, 0, kv.OpPut, fmt.Sprintf("k%d", i%4), fmt.Sprint(i), "")
	}
	want := fmt.Sprint(s.Data(s.Leader()))
	for _, id := range s.IDs() {
		s.Crash(id)
	}
	for _, id := range s.IDs() {
		s.Restart(id)
		if s.AppliedIndex(id) == 0 {
			t.Fatalf("node %d restarted without loading a snapshot", id)
		}
	}
	ok, err := s.RunUntil(func() bool {
		l := s.Leader()
		return l != raft.None && s.AppliedIndex(l) >= uint64(len(s.Committed()))
	}, 2000)
	if err != nil || !ok {
		t.Fatalf("no recovery: ok=%v err=%v", ok, err)
	}
	if got := fmt.Sprint(s.Data(s.Leader())); got != want {
		t.Fatalf("state after restart %v, want %v", got, want)
	}
	clientDo(t, s, 0, kv.OpPut, "after", "1", "")
}

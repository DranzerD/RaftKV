package replica

import (
	"testing"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
)

// soloLeader returns a one-node cluster that has won its election and
// applied everything committed so far.
func soloLeader(t *testing.T) *Replica {
	t.Helper()
	n, err := raft.New(raft.Config{ID: 1, Peers: []uint64{1}, ElectionTick: 5, HeartbeatTick: 1, Seed: 1}, raft.HardState{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := New(n)
	for i := 0; i < 50 && n.Role() != raft.Leader; i++ {
		n.Tick()
		r.Apply(n.Ready().Committed)
	}
	if n.Role() != raft.Leader {
		t.Fatal("single node did not become leader")
	}
	r.Apply(n.Ready().Committed) // the leader's no-op
	return r
}

// A read must wait until the state machine has applied everything up to
// the commit index the leader recorded, and then see those writes. In the
// real server and in the simulator the store is never behind, so only a
// direct test can tell whether the replica checks.
func TestReadWaitsForTheStateMachine(t *testing.T) {
	r := soloLeader(t)
	base := r.Applied()

	if out := r.Request(kv.Command{Client: 9, Seq: 1, Op: kv.OpGet, Key: "k"}); len(out) != 0 {
		t.Fatalf("read answered before it was confirmed: %+v", out)
	}
	// The leader confirms the read at an index the store has not reached.
	if out := r.Advance([]raft.ReadState{{ID: 1, Index: base + 2}}); len(out) != 0 {
		t.Fatalf("read answered while the store was at %d, behind read index %d: %+v", r.Applied(), base+2, out)
	}

	put := kv.Command{Client: 7, Seq: 1, Op: kv.OpPut, Key: "k", Value: "v"}
	out := r.Apply([]raft.Entry{{Index: base + 1, Term: 1, Data: put.Encode()}})
	if len(out) != 0 {
		t.Fatalf("read answered at index %d, one short of its read index: %+v", base+1, out)
	}
	out = r.Apply([]raft.Entry{{Index: base + 2, Term: 1}})
	if len(out) != 1 || out[0].Client != 9 || out[0].Status != OK || !out[0].Read ||
		!out[0].Result.Found || out[0].Result.Value != "v" {
		t.Fatalf("read after catching up = %+v, want OK with the value written at index %d", out, base+1)
	}
}

// A read that the leader has not confirmed must never be answered, however
// far the state machine advances.
func TestUnconfirmedReadIsNotServed(t *testing.T) {
	r := soloLeader(t)
	base := r.Applied()
	r.Request(kv.Command{Client: 9, Seq: 1, Op: kv.OpGet, Key: "k"})
	out := r.Apply([]raft.Entry{{Index: base + 1, Term: 1}, {Index: base + 2, Term: 1}})
	if len(out) != 0 {
		t.Fatalf("unconfirmed read answered: %+v", out)
	}
}

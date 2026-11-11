package raft

import "testing"

func TestCompactAndSendSnapshot(t *testing.T) {
	n := newTestNode(t, 1, 3, HardState{}, nil)
	elect(t, n, 2)
	term := n.Term()
	for i := 0; i < 5; i++ {
		n.Propose([]byte{byte(i)})
	}
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, LogIndex: 6})
	n.Ready() // hands out entries 1..6 as committed

	if n.Compact(7, nil) {
		t.Fatal("compacted past the applied index")
	}
	if !n.Compact(4, []byte("state@4")) {
		t.Fatal("compaction refused")
	}
	if n.Compact(4, nil) {
		t.Fatal("compacted the same index twice")
	}
	if n.FirstIndex() != 4 || n.LastIndex() != 6 {
		t.Fatalf("log covers %d..%d, want 5..6", n.FirstIndex()+1, n.LastIndex())
	}
	rd := n.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 4 || rd.Snapshot.Term != term || rd.RestoreSnapshot || !rd.MustSync {
		t.Fatalf("own snapshot not handed out for persistence: %+v", rd)
	}

	// Follower 3 never acknowledged anything, so what it needs next is
	// behind the compaction point. The next heartbeat must carry the
	// snapshot instead of entries.
	n.Tick()
	n.Tick()
	var snap *Message
	for _, m := range n.Ready().Messages {
		if m.To == 3 && m.Type == MsgSnap {
			m := m
			snap = &m
		}
	}
	if snap == nil || string(snap.Snapshot.Data) != "state@4" {
		t.Fatalf("leader did not send its snapshot to the lagging follower")
	}

	// The follower installs it and reports index 4 as matched.
	f := newTestNode(t, 3, 3, HardState{}, nil)
	f.Step(*snap)
	frd := f.Ready()
	if !frd.RestoreSnapshot || frd.Snapshot.Index != 4 || !frd.MustSync || f.Commit() != 4 || f.FirstIndex() != 4 {
		t.Fatalf("follower did not install the snapshot: %+v", frd)
	}
	if m := frd.Messages[0]; m.Type != MsgAppResp || m.Reject || m.LogIndex != 4 {
		t.Fatalf("follower reply %+v, want a match at 4", m)
	}
	// A duplicate of the same snapshot changes nothing.
	f.Step(*snap)
	if frd = f.Ready(); frd.Snapshot != nil {
		t.Fatal("duplicate snapshot was installed again")
	}

	// An append that starts inside the compacted prefix is accepted from
	// the snapshot onward.
	f.Step(Message{Type: MsgApp, From: 1, To: 3, Term: term, LogIndex: 2, LogTerm: term,
		Entries: []Entry{{Index: 3, Term: term}, {Index: 4, Term: term}, {Index: 5, Term: term}}})
	if f.LastIndex() != 5 {
		t.Fatalf("append across the snapshot boundary left last = %d, want 5", f.LastIndex())
	}

	// Restarting from the snapshot and the remaining tail works.
	if _, err := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 2},
		HardState{Term: term}, rd.Snapshot, []Entry{{Index: 5, Term: term}, {Index: 6, Term: term}}); err != nil {
		t.Fatal(err)
	}
}

// A follower that installs a snapshot keeps the log after it only when its
// own entry at the snapshot's index is the snapshot's last entry (Log
// Matching). A tail built on a different entry belongs to a dead leader and
// must go, or the node could later vouch for entries no one committed.
func TestSnapshotKeepsOrDropsTail(t *testing.T) {
	ownLog := func() []Entry {
		return []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}}
	}
	install := func(snapTerm uint64) *Node {
		f := newTestNode(t, 3, 3, HardState{Term: 1}, ownLog())
		f.Step(Message{Type: MsgSnap, From: 1, To: 3, Term: snapTerm,
			Snapshot: &Snapshot{Index: 3, Term: snapTerm, Data: []byte("state@3")}})
		if rd := f.Ready(); rd.Snapshot == nil || !rd.RestoreSnapshot {
			t.Fatalf("snapshot with term %d was not installed: %+v", snapTerm, rd)
		}
		return f
	}

	// Our entry 3 has term 1 and the snapshot says it is term 2: a conflict.
	if f := install(2); f.LastIndex() != 3 || f.FirstIndex() != 3 || f.termAt(3) != 2 {
		t.Fatalf("conflicting tail survived the snapshot: log %d..%d, term at 3 = %d",
			f.FirstIndex()+1, f.LastIndex(), f.termAt(3))
	}
	// Same entry at 3: entry 4 is still good and must not be thrown away.
	if f := install(1); f.LastIndex() != 4 || f.FirstIndex() != 3 {
		t.Fatalf("matching tail was dropped: log %d..%d, want 4..4", f.FirstIndex()+1, f.LastIndex())
	}
}

// ReadIndex: a leader confirms a read only after a majority has answered a
// heartbeat sent after the read arrived.
func TestReadIndexNeedsFreshQuorum(t *testing.T) {
	n := newTestNode(t, 1, 5, HardState{}, nil)
	elect(t, n, 2, 3)
	term := n.Term()

	if n.ReadIndex(1) {
		t.Fatal("read accepted before the leader committed anything in its own term")
	}
	// The no-op commits. These acknowledgements predate any read.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, LogIndex: 1})
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: term, LogIndex: 1})
	n.Ready()

	if !n.ReadIndex(7) {
		t.Fatal("read refused by an established leader")
	}
	var ctx uint64
	for _, m := range n.Ready().Messages {
		if m.Type == MsgApp {
			ctx = m.Ctx
		}
	}
	if ctx == 0 {
		t.Fatal("no heartbeat carried the read context")
	}

	// Old acknowledgements, duplicated or delayed, prove nothing.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, LogIndex: 1})
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: term, LogIndex: 1, Ctx: ctx - 1})
	// One fresh answer plus the leader is two of five.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, LogIndex: 1, Ctx: ctx})
	if rs := n.Ready().ReadStates; len(rs) != 0 {
		t.Fatalf("read confirmed without a majority: %+v", rs)
	}
	// A rejection still shows the follower follows this leader.
	n.Step(Message{Type: MsgAppResp, From: 4, To: 1, Term: term, Reject: true, LogIndex: 99, Ctx: ctx})
	rs := n.Ready().ReadStates
	if len(rs) != 1 || rs[0].ID != 7 || rs[0].Index != 1 {
		t.Fatalf("read states %+v, want read 7 at index 1", rs)
	}

	// Stepping down abandons reads that were still waiting.
	n.ReadIndex(8)
	n.Step(Message{Type: MsgVote, From: 5, To: 1, Term: term + 1, LogIndex: 9, LogTerm: term})
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, Ctx: ctx + 1})
	if rs := n.Ready().ReadStates; len(rs) != 0 {
		t.Fatalf("deposed leader confirmed a read: %+v", rs)
	}
}

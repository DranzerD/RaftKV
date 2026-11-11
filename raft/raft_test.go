package raft

import "testing"

func newTestNode(t *testing.T, id uint64, size int, hs HardState, log []Entry) *Node {
	t.Helper()
	var peers []uint64
	for i := 1; i <= size; i++ {
		peers = append(peers, uint64(i))
	}
	n, err := New(Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 2, Seed: 1}, hs, nil, log)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// elect drives node n to leadership with votes from the given peers.
func elect(t *testing.T, n *Node, voters ...uint64) {
	t.Helper()
	for i := 0; i < 20 && n.Role() != Candidate; i++ {
		n.Tick()
	}
	if n.Role() != Candidate {
		t.Fatal("node never campaigned")
	}
	n.Ready()
	for _, v := range voters {
		n.Step(Message{Type: MsgVoteResp, From: v, To: n.ID(), Term: n.Term()})
	}
	if n.Role() != Leader {
		t.Fatalf("node is %v after votes from %v", n.Role(), voters)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{ID: 0, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: 4, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: 1, Peers: []uint64{1, 2, 2}, ElectionTick: 10, HeartbeatTick: 1},
		{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 2, HeartbeatTick: 2},
	}
	for i, c := range bad {
		if _, err := New(c, HardState{}, nil, nil); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
	if _, err := New(Config{ID: 1, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1},
		HardState{Term: 1}, nil, []Entry{{Index: 2, Term: 1}}); err == nil {
		t.Error("log with a gap accepted")
	}
}

func TestSingleNodeCommitsAlone(t *testing.T) {
	n := newTestNode(t, 1, 1, HardState{}, nil)
	for i := 0; i < 20 && n.Role() != Leader; i++ {
		n.Tick()
	}
	if _, _, ok := n.Propose([]byte("a")); !ok {
		t.Fatal("single node did not become leader")
	}
	rd := n.Ready()
	if len(rd.Committed) != 2 || string(rd.Committed[1].Data) != "a" {
		t.Fatalf("committed = %+v, want no-op then a", rd.Committed)
	}
	if !rd.MustSync || len(rd.Entries) != 2 {
		t.Fatalf("entries must be handed out for persistence alongside the commit: %+v", rd)
	}
}

// A voter refuses a candidate whose log is behind its own (section 5.4.1).
func TestVoteRequiresUpToDateLog(t *testing.T) {
	log := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	cases := []struct {
		lastIndex, lastTerm uint64
		grant               bool
	}{
		{1, 1, false}, // older term
		{5, 1, false}, // longer but older term
		{1, 2, false}, // same term, shorter
		{2, 2, true},  // identical
		{1, 3, true},  // newer term wins regardless of length
	}
	for _, c := range cases {
		n := newTestNode(t, 1, 3, HardState{Term: 2}, log)
		n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LogIndex: c.lastIndex, LogTerm: c.lastTerm})
		rd := n.Ready()
		if len(rd.Messages) != 1 || rd.Messages[0].Reject == c.grant {
			t.Errorf("candidate last=(%d,%d): messages %+v, want grant=%v",
				c.lastIndex, c.lastTerm, rd.Messages, c.grant)
		}
		if c.grant && (rd.HardState.Vote != 2 || !rd.MustSync) {
			t.Errorf("granted vote must be persisted before the reply: %+v", rd)
		}
	}
}

func TestVotesOncePerTerm(t *testing.T) {
	n := newTestNode(t, 1, 3, HardState{}, nil)
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 1})
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1}) // retransmission
	ms := n.Ready().Messages
	if len(ms) != 3 || ms[0].Reject || !ms[1].Reject || ms[2].Reject {
		t.Fatalf("want grant, reject, grant; got %+v", ms)
	}
}

// Figure 8: a leader must not commit an entry from an earlier term just
// because it is on a majority. It commits it only through an entry of its
// own term.
func TestNoCommitOfOldTermByCounting(t *testing.T) {
	log := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	n := newTestNode(t, 1, 5, HardState{Term: 3}, log)
	elect(t, n, 2, 3)
	term := n.Term()
	if n.LastIndex() != 3 {
		t.Fatalf("leader should have appended a no-op at index 3, last = %d", n.LastIndex())
	}

	// Two followers confirm the term-2 entry: index 2 is on a majority.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, LogIndex: 2})
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: term, LogIndex: 2})
	if n.Commit() != 0 {
		t.Fatalf("commit = %d after a majority stored a term-2 entry in term %d; want 0", n.Commit(), term)
	}

	// Once the current-term no-op is on a majority, everything commits.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: term, LogIndex: 3})
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: term, LogIndex: 3})
	if n.Commit() != 3 {
		t.Fatalf("commit = %d, want 3", n.Commit())
	}
}

// A delayed MsgApp that is a prefix of the log must not truncate it.
func TestStaleAppendDoesNotTruncate(t *testing.T) {
	n := newTestNode(t, 2, 3, HardState{}, nil)
	ents := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: ents, Commit: 3})
	n.Ready()
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: ents[:1]})
	rd := n.Ready()
	if n.LastIndex() != 3 || n.Commit() != 3 {
		t.Fatalf("last = %d commit = %d after stale append, want 3 and 3", n.LastIndex(), n.Commit())
	}
	if len(rd.Entries) != 0 {
		t.Fatalf("stale append rewrote the log: %+v", rd.Entries)
	}
}

func TestConflictingSuffixIsReplaced(t *testing.T) {
	log := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}}
	n := newTestNode(t, 2, 3, HardState{Term: 2}, log)

	// A term-3 leader whose log diverges after index 1.
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, LogIndex: 2, LogTerm: 3})
	rd := n.Ready()
	if m := rd.Messages[0]; !m.Reject || m.Hint != 2 {
		t.Fatalf("want reject with hint 2 (first index of the conflicting term), got %+v", m)
	}

	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 3}}})
	rd = n.Ready()
	if n.LastIndex() != 2 || len(rd.Entries) != 1 || rd.Entries[0].Term != 3 {
		t.Fatalf("log not replaced from index 2: last = %d entries = %+v", n.LastIndex(), rd.Entries)
	}
	if m := rd.Messages[0]; m.Reject || m.LogIndex != 2 {
		t.Fatalf("want success with match 2, got %+v", m)
	}
}

// The election timer resets on a granted vote or a current leader's
// append, and on nothing else.
func TestElectionTimerResetRules(t *testing.T) {
	log := []Entry{{Index: 1, Term: 1}}
	fresh := func() *Node {
		n := newTestNode(t, 1, 3, HardState{Term: 1}, log)
		for i := 0; i < 5; i++ {
			n.Tick()
		}
		return n
	}

	n := fresh()
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 2}) // stale log: rejected
	if n.electionElapsed != 5 {
		t.Errorf("rejected vote reset the timer")
	}

	n = fresh()
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 0}) // deposed leader
	if n.electionElapsed != 5 {
		t.Errorf("append from a stale term reset the timer")
	}

	n = fresh()
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 2, LogIndex: 1, LogTerm: 1})
	if n.electionElapsed != 0 {
		t.Errorf("granted vote did not reset the timer")
	}

	n = fresh()
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 1, LogIndex: 1, LogTerm: 1})
	if n.electionElapsed != 0 {
		t.Errorf("valid append did not reset the timer")
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	n := newTestNode(t, 1, 3, HardState{}, nil)
	elect(t, n, 2)
	n.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: n.Term() + 1, Reject: true})
	if n.Role() != Follower {
		t.Fatalf("leader stayed %v after seeing a higher term", n.Role())
	}
	if _, _, ok := n.Propose([]byte("x")); ok {
		t.Fatal("deposed leader accepted a proposal")
	}
}

// A vote granted without a term change is still new durable state. If it
// is not flagged for sync, a crash lets the node vote twice in one term.
func TestVoteInCurrentTermMustSync(t *testing.T) {
	n := newTestNode(t, 1, 3, HardState{Term: 3}, nil)
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3})
	rd := n.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatalf("vote not granted: %+v", rd.Messages)
	}
	if !rd.MustSync || rd.HardState.Vote != 2 {
		t.Fatalf("granted vote not flagged for persistence: %+v", rd)
	}
}

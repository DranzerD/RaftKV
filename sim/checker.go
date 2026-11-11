package sim

import (
	"bytes"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
)

// The checker tracks the four safety properties from Figure 3 of the Raft
// paper across the whole run, including across crashes:
//
//	Election Safety      at most one leader per term
//	Log Matching         same index and term => identical logs up to there
//	Leader Completeness  a committed entry is in the log of every later leader
//	State Machine Safety no two nodes apply different entries at one index
//
// Leader Append-Only is implied by the core's structure and is not checked.
//
// On top of those it checks what the layers above Raft promise: that each
// client command takes effect exactly once, and that snapshots stand in
// faithfully for the log they replace.

type committedRec struct {
	entry raft.Entry
	// seenTerm is the term of the node that first applied the entry. The
	// leader of that term, and of every later term, must hold the entry.
	seenTerm uint64
}

type checker struct {
	leaders   map[uint64]uint64 // term -> the node that led it
	committed []committedRec    // index i is at committed[i-1]

	// The exactly-once reference: the committed log replayed through a
	// store with no sessions, keeping only the first appearance of each
	// (client, seq). Every node's store and every answer must agree.
	ref     *kv.Store
	seen    map[uint64]uint64
	results map[[2]uint64]kv.Result
	dups    int // log entries that were retries of an earlier entry
}

func (c *checker) init() {
	c.leaders = make(map[uint64]uint64)
	c.ref = kv.NewStore()
	c.seen = make(map[uint64]uint64)
	c.results = make(map[[2]uint64]kv.Result)
}

func (c *checker) model(e raft.Entry) {
	if e.Type != raft.EntryNormal {
		return
	}
	cmd, err := kv.Decode(e.Data)
	if err != nil {
		return
	}
	if cmd.Client != 0 {
		if cmd.Seq <= c.seen[cmd.Client] {
			c.dups++
			return
		}
		c.seen[cmd.Client] = cmd.Seq
	}
	anon := cmd
	anon.Client = 0 // bypass the store's own sessions
	res, _ := c.ref.Apply(anon)
	if cmd.Client != 0 {
		c.results[[2]uint64{cmd.Client, cmd.Seq}] = res
	}
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type && bytes.Equal(a.Data, b.Data)
}

// recordApply is called for every entry a node applies. It enforces State
// Machine Safety.
func (c *checker) recordApply(s *Sim, n *node, e raft.Entry) {
	i := int(e.Index)
	switch {
	case i <= len(c.committed):
		if prev := c.committed[i-1].entry; !sameEntry(prev, e) {
			s.fail("state machine safety",
				"node %d applied term %d at index %d, but term %d was applied there before",
				n.id, e.Term, e.Index, prev.Term)
		}
	case i == len(c.committed)+1:
		c.committed = append(c.committed, committedRec{entry: e, seenTerm: n.raft.Term()})
		c.model(e)
	default:
		s.fail("apply order", "node %d applied index %d before index %d was applied anywhere",
			n.id, e.Index, len(c.committed)+1)
	}
}

// checkSnapshot is called when a node persists a snapshot, its own or the
// leader's. A snapshot may only cover entries that are committed, and must
// name the term the committed entry at its index really has.
func (s *Sim) checkSnapshot(n *node, snap *raft.Snapshot) {
	c := &s.check
	if snap.Index > uint64(len(c.committed)) {
		s.fail("snapshot", "node %d has a snapshot at index %d, but only %d entries are committed",
			n.id, snap.Index, len(c.committed))
		return
	}
	if want := c.committed[snap.Index-1].entry.Term; snap.Term != want {
		s.fail("snapshot", "node %d has a snapshot at index %d term %d, but that entry has term %d",
			n.id, snap.Index, snap.Term, want)
	}
}

func (s *Sim) runChecks() {
	if s.err != nil {
		return
	}
	c := &s.check

	// Election Safety and Leader Completeness.
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.raft == nil || n.raft.Role() != raft.Leader {
			continue
		}
		term := n.raft.Term()
		if prev, ok := c.leaders[term]; ok && prev != id {
			s.fail("election safety", "nodes %d and %d both led term %d", prev, id, term)
			return
		}
		c.leaders[term] = id

		// Entries at or below the leader's snapshot are vouched for by
		// checkSnapshot and by the state comparison below.
		for i := n.base(); i < uint64(len(c.committed)); i++ {
			rec := c.committed[i]
			if rec.seenTerm > term {
				continue // committed after this leader's term; it may lack it
			}
			if n.last() < rec.entry.Index || !sameEntry(n.entry(rec.entry.Index), rec.entry) {
				s.fail("leader completeness",
					"leader %d of term %d is missing committed entry %d (term %d)",
					id, term, rec.entry.Index, rec.entry.Term)
				return
			}
		}
	}

	// Exactly-once, and snapshot fidelity: a node whose state machine has
	// reached the end of the committed log, by applying entries or by
	// loading a snapshot, must hold exactly the reference state.
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.raft == nil || n.appliedIndex != uint64(len(c.committed)) || n.checkedAt == n.appliedIndex {
			continue
		}
		n.checkedAt = n.appliedIndex
		got, want := n.rep.Store().Data(), c.ref.Data()
		same := len(got) == len(want)
		for k, v := range want {
			if gv, ok := got[k]; !ok || gv != v {
				same = false
			}
		}
		if !same {
			s.fail("exactly-once", "node %d state %v differs from the reference %v after %d entries",
				id, got, want, n.appliedIndex)
			return
		}
	}

	// Log Matching, over the persisted logs of all nodes, running or not.
	// Two logs are compared where both still hold entries.
	for a := 0; a < len(s.ids); a++ {
		for b := a + 1; b < len(s.ids); b++ {
			na, nb := s.nodes[s.ids[a]], s.nodes[s.ids[b]]
			if !na.dirty && !nb.dirty {
				continue
			}
			lo := na.base()
			if nb.base() > lo {
				lo = nb.base()
			}
			i := na.last()
			if nb.last() < i {
				i = nb.last()
			}
			for i > lo && na.entry(i).Term != nb.entry(i).Term {
				i--
			}
			for ; i > lo; i-- {
				if ea, eb := na.entry(i), nb.entry(i); !sameEntry(ea, eb) {
					s.fail("log matching",
						"nodes %d and %d share a later entry but differ at index %d (terms %d and %d)",
						na.id, nb.id, i, ea.Term, eb.Term)
					return
				}
			}
		}
	}
	for _, id := range s.ids {
		s.nodes[id].dirty = false
	}
}

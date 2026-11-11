// Package replica joins the Raft core to the key-value state machine and
// answers clients. Like the core it does no I/O: the simulator and the real
// server both drive the same code.
package replica

import (
	"sort"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
)

// Status is the outcome of a client request as seen by one node.
type Status uint8

const (
	// OK: the command committed and Result is its outcome.
	OK Status = iota
	// NotLeader: this node cannot take the request. Leader is a hint.
	NotLeader
	// Retry: the request was accepted but leadership changed before it
	// committed here. It may or may not have taken effect; resend it with
	// the same sequence number and the session table sorts it out.
	Retry
)

func (s Status) String() string {
	switch s {
	case OK:
		return "ok"
	case NotLeader:
		return "not-leader"
	case Retry:
		return "retry"
	}
	return "unknown"
}

// Response answers one client request.
type Response struct {
	Client uint64
	Seq    uint64
	Status Status
	Leader uint64
	Result kv.Result
	Read   bool // answered from the state machine without a log entry
}

type waiter struct {
	term   uint64
	client uint64
	seq    uint64
}

type read struct {
	client uint64
	seq    uint64
	key    string
	term   uint64
	index  uint64 // 0 until the leader has confirmed it still leads
}

// Replica is one node's Raft instance plus its state machine.
type Replica struct {
	raft    *raft.Node
	store   *kv.Store
	applied uint64
	// waiting maps a log index to the client whose command this node
	// proposed there. The client is answered when that index applies.
	waiting map[uint64]waiter
	reads   map[uint64]*read // by read ID
	nextID  uint64
	minID   uint64 // no read with a smaller ID is outstanding
}

// New wraps a Raft node with an empty store.
func New(r *raft.Node) *Replica {
	return &Replica{
		raft:    r,
		store:   kv.NewStore(),
		waiting: make(map[uint64]waiter),
		reads:   make(map[uint64]*read),
	}
}

// Raft returns the underlying Raft node.
func (r *Replica) Raft() *raft.Node { return r.raft }

// Store returns the state machine.
func (r *Replica) Store() *kv.Store { return r.store }

// Applied returns the index of the last applied entry.
func (r *Replica) Applied() uint64 { return r.applied }

// Request submits a client command. Any responses returned can be sent at
// once; otherwise the answer arrives later from Apply or Advance.
func (r *Replica) Request(c kv.Command) []Response {
	if r.raft.Role() != raft.Leader {
		return []Response{{Client: c.Client, Seq: c.Seq, Status: NotLeader, Leader: r.raft.Leader()}}
	}
	if c.Op == kv.OpGet {
		// Reads skip the log. The leader first proves it still leads.
		r.nextID++
		if !r.raft.ReadIndex(r.nextID) {
			return []Response{{Client: c.Client, Seq: c.Seq, Status: Retry, Leader: r.raft.Leader()}}
		}
		r.reads[r.nextID] = &read{client: c.Client, seq: c.Seq, key: c.Key, term: r.raft.Term()}
		return nil
	}
	index, term, ok := r.raft.Propose(c.Encode())
	if !ok {
		return []Response{{Client: c.Client, Seq: c.Seq, Status: NotLeader, Leader: r.raft.Leader()}}
	}
	r.waiting[index] = waiter{term: term, client: c.Client, seq: c.Seq}
	return nil
}

// Apply runs committed entries through the state machine. The caller must
// have persisted the Ready they came from first.
func (r *Replica) Apply(entries []raft.Entry) []Response {
	var out []Response
	for _, e := range entries {
		r.applied = e.Index
		w, waiting := r.waiting[e.Index]
		delete(r.waiting, e.Index)

		var res kv.Result
		status := kv.Stale
		if e.Type == raft.EntryNormal {
			if cmd, err := kv.Decode(e.Data); err == nil {
				res, status = r.store.Apply(cmd)
			}
		}
		if !waiting {
			continue
		}
		switch {
		case w.term != e.Term:
			// Another leader's entry landed where ours was. Ours is gone
			// from this index, though a copy may have committed elsewhere.
			out = append(out, Response{Client: w.client, Seq: w.seq, Status: Retry, Leader: r.raft.Leader()})
		case status != kv.Stale:
			out = append(out, Response{Client: w.client, Seq: w.seq, Status: OK, Result: res})
		}
	}
	return append(out, r.serveReads()...)
}

// Restore replaces the state machine with a snapshot. Clients waiting on
// entries the snapshot covers are told to retry: their command may be in
// it, and the session table will recognise the retry if so.
func (r *Replica) Restore(snap *raft.Snapshot) ([]Response, error) {
	if err := r.store.Restore(snap.Data); err != nil {
		return nil, err
	}
	r.applied = snap.Index
	var out []Response
	for index, w := range r.waiting {
		if index <= snap.Index {
			out = append(out, Response{Client: w.client, Seq: w.seq, Status: Retry, Leader: r.raft.Leader()})
			delete(r.waiting, index)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Client != out[j].Client {
			return out[i].Client < out[j].Client
		}
		return out[i].Seq < out[j].Seq
	})
	return out, nil
}

// Advance takes the read confirmations from a Ready and answers every read
// that is now safe. Call it after Apply.
func (r *Replica) Advance(states []raft.ReadState) []Response {
	for _, st := range states {
		if rd, ok := r.reads[st.ID]; ok {
			rd.index = st.Index
		}
	}
	return r.serveReads()
}

func (r *Replica) serveReads() []Response {
	if len(r.reads) == 0 {
		return nil
	}
	var out []Response
	lost := r.raft.Role() != raft.Leader
	for id := r.minID; id <= r.nextID; id++ {
		rd, ok := r.reads[id]
		if !ok {
			continue
		}
		switch {
		case lost || rd.term != r.raft.Term():
			out = append(out, Response{Client: rd.client, Seq: rd.seq, Status: Retry, Leader: r.raft.Leader()})
		case rd.index != 0 && r.applied >= rd.index:
			out = append(out, Response{Client: rd.client, Seq: rd.seq, Status: OK, Result: r.store.Read(rd.key), Read: true})
		default:
			continue
		}
		delete(r.reads, id)
	}
	for r.minID <= r.nextID && r.reads[r.minID] == nil {
		r.minID++
	}
	return out
}

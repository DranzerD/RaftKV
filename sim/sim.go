// Package sim runs a whole Raft cluster in one goroutine with a fake clock,
// a fake network and a fake disk. All randomness comes from one seed, so a
// failing run can be replayed exactly.
package sim

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
	"github.com/DranzerD/RaftKV/replica"
	"github.com/DranzerD/RaftKV/storage"
)

// Options configures a simulated cluster.
type Options struct {
	Nodes         int   // cluster size; default 3
	Seed          int64 // drives the network, timers and restarts
	ElectionTick  int   // default 20
	HeartbeatTick int   // default 4
	MaxEntries    int   // entries per MsgApp; default is the core's (64)

	// SnapshotEvery makes a node snapshot its state machine and compact
	// its log whenever that many entries have been applied since the last
	// snapshot. Zero disables snapshots.
	SnapshotEvery int

	// Network behaviour. Delays are in ticks and drawn uniformly from
	// [MinDelay, MaxDelay]; a range wider than one tick reorders messages.
	MinDelay uint64  // default 1
	MaxDelay uint64  // default MinDelay
	DropProb float64 // chance a message is lost
	DupProb  float64 // chance a message is delivered twice

	// CrashPointProb is the chance, each time a node has a Ready to
	// handle, that it dies somewhere inside handling it: before the save,
	// at a random step of the save, before sending, or part-way through
	// sending.
	CrashPointProb float64

	// FsyncLies makes every disk report a successful sync without making
	// anything durable. Raft cannot survive this; it exists so tests can
	// prove the durability check notices.
	FsyncLies bool
}

// Stats counts what happened during a run.
type Stats struct {
	Sent      int
	Dropped   int
	Delivered int
	Crashes   int
	Restarts  int
	Requests  int // client requests sent, retries included
	Completed int // client operations that got an answer
	// MidReadyCrashes counts crashes injected inside Ready handling, and
	// TornWrites the subset that stopped a save part-way.
	MidReadyCrashes int
	TornWrites      int
	// SnapshotsTaken counts log compactions. SnapshotsInstalled counts
	// followers brought up to date by a snapshot because the leader had
	// already compacted the entries they needed.
	SnapshotsTaken     int
	SnapshotsInstalled int
	// MaxLogLen is the longest log any node held in memory.
	MaxLogLen int
}

// durable is a node's persistent state.
type durable struct {
	hs   raft.HardState
	snap *raft.Snapshot
	log  []raft.Entry // entries after the snapshot
}

func (d durable) base() uint64 {
	if d.snap == nil {
		return 0
	}
	return d.snap.Index
}

func (d durable) last() uint64 { return d.base() + uint64(len(d.log)) }

// entry returns the entry at index i, which must be in (base, last].
func (d durable) entry(i uint64) raft.Entry { return d.log[i-d.base()-1] }

// apply returns the state after persisting one Ready. It follows the same
// rules as the core and the WAL.
func (d durable) apply(hs raft.HardState, entries []raft.Entry, snap *raft.Snapshot) durable {
	out := durable{hs: hs, snap: d.snap, log: d.log}
	if snap != nil && snap.Index > d.base() {
		if snap.Index <= d.last() && d.entry(snap.Index).Term == snap.Term {
			out.log = append([]raft.Entry(nil), d.log[snap.Index-d.base():]...)
		} else {
			out.log = nil
		}
		out.snap = snap
	}
	if len(entries) > 0 {
		keep := entries[0].Index - out.base() - 1
		out.log = append(append([]raft.Entry(nil), out.log[:keep]...), entries...)
	}
	return out
}

func (d durable) same(o durable) bool {
	if d.hs != o.hs || d.base() != o.base() || len(d.log) != len(o.log) {
		return false
	}
	if d.snap != nil && (d.snap.Term != o.snap.Term || !bytes.Equal(d.snap.Data, o.snap.Data)) {
		return false
	}
	for i := range d.log {
		if !sameEntry(d.log[i], o.log[i]) {
			return false
		}
	}
	return true
}

type node struct {
	id   uint64
	raft *raft.Node // nil while crashed
	rep  *replica.Replica

	// The disk: the real storage code on a fake directory. It survives
	// crashes.
	fs    *memFS
	store *storage.Store // nil while crashed

	// What the store has acknowledged as durable. Recovery must return
	// exactly this, or this plus the save that was in flight at the crash.
	durable
	pending *durable // the state the interrupted save was moving to

	applied      []raft.Entry // entries applied one by one since the last start
	appliedIndex uint64       // last index in the state machine
	checkedAt    uint64       // appliedIndex when the store was last compared
	dirty        bool         // durable log changed since the last invariant check
}

type envelope struct {
	at   uint64
	seq  uint64
	kind int
	m    raft.Message
	node uint64 // kindRequest: destination
	req  kv.Command
	resp replica.Response
}

type msgHeap []envelope

func (h msgHeap) Len() int { return len(h) }
func (h msgHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h msgHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *msgHeap) Push(x any)   { *h = append(*h, x.(envelope)) }
func (h *msgHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// Sim is a simulated cluster.
type Sim struct {
	opt   Options
	rng   *rand.Rand
	now   uint64
	seq   uint64
	ids   []uint64
	nodes map[uint64]*node
	queue msgHeap

	blocked map[[2]uint64]bool // {from, to} -> link is cut

	clients []*simClient
	history []Operation
	evt     uint64 // global event clock for the history

	check checker
	stats Stats
	err   error // first invariant violation
}

// New builds a cluster with every node up and no faults.
func New(opt Options) *Sim {
	if opt.Nodes <= 0 {
		opt.Nodes = 3
	}
	if opt.ElectionTick <= 0 {
		opt.ElectionTick = 20
	}
	if opt.HeartbeatTick <= 0 {
		opt.HeartbeatTick = 4
	}
	if opt.MinDelay == 0 {
		opt.MinDelay = 1
	}
	if opt.MaxDelay < opt.MinDelay {
		opt.MaxDelay = opt.MinDelay
	}
	s := &Sim{
		opt:     opt,
		rng:     rand.New(rand.NewSource(opt.Seed)),
		nodes:   make(map[uint64]*node),
		blocked: make(map[[2]uint64]bool),
	}
	s.check.init()
	for i := 1; i <= opt.Nodes; i++ {
		s.ids = append(s.ids, uint64(i))
	}
	for _, id := range s.ids {
		n := &node{id: id, fs: newMemFS(opt.FsyncLies)}
		s.nodes[id] = n
		s.boot(n)
	}
	return s
}

// rewriteDisk replaces a node's disk with one holding exactly d.
func (s *Sim) rewriteDisk(n *node, d durable) {
	n.fs = newMemFS(false)
	store, _, err := storage.OpenStore(n.fs)
	if err == nil {
		err = store.Save(d.hs, d.log, d.snap)
	}
	if err != nil {
		panic(fmt.Sprintf("sim: cannot rewrite disk of node %d: %v", n.id, err))
	}
	n.fs.lies = s.opt.FsyncLies
	n.store, n.durable, n.pending, n.dirty = store, d, nil, true
}

// boot starts a node from whatever its store recovers, and checks that
// this is what the store had promised to keep.
func (s *Sim) boot(n *node) {
	store, st, err := storage.OpenStore(n.fs)
	got := durable{hs: st.HardState, snap: st.Snapshot, log: st.Entries}
	switch {
	case err != nil:
		s.fail("durability", "node %d cannot recover: %v", n.id, err)
		s.rewriteDisk(n, n.durable)
	case got.same(n.durable):
		n.store = store
	case n.pending != nil && got.same(*n.pending):
		// The interrupted save made it to disk after all. It was never
		// acknowledged, so nothing was sent on its strength, but keeping
		// it is legal.
		n.store, n.durable, n.dirty = store, got, true
	default:
		s.fail("durability",
			"node %d recovered term %d vote %d log %d..%d, but had saved term %d vote %d log %d..%d",
			n.id, got.hs.Term, got.hs.Vote, got.base()+1, got.last(),
			n.hs.Term, n.hs.Vote, n.base()+1, n.last())
		s.rewriteDisk(n, n.durable)
	}
	n.pending = nil

	r, err := raft.New(raft.Config{
		ID:               n.id,
		Peers:            s.ids,
		ElectionTick:     s.opt.ElectionTick,
		HeartbeatTick:    s.opt.HeartbeatTick,
		MaxEntriesPerMsg: s.opt.MaxEntries,
		Seed:             s.rng.Int63(),
	}, n.hs, n.snap, append([]raft.Entry(nil), n.log...))
	if err != nil {
		panic(fmt.Sprintf("sim: cannot start node %d: %v", n.id, err))
	}
	n.raft = r
	n.rep = replica.New(r)
	n.applied, n.appliedIndex, n.checkedAt = nil, 0, 0
	if n.snap != nil {
		if _, err := n.rep.Restore(n.snap); err != nil {
			s.fail("snapshot", "node %d cannot load its own snapshot at %d: %v", n.id, n.snap.Index, err)
		}
		n.appliedIndex = n.snap.Index
	}
}

// --- inspection ---

// Now returns the current tick.
func (s *Sim) Now() uint64 { return s.now }

// IDs returns all node IDs in order.
func (s *Sim) IDs() []uint64 { return append([]uint64(nil), s.ids...) }

// Stats returns the counters so far.
func (s *Sim) Stats() Stats { return s.stats }

// Err returns the first invariant violation, if any.
func (s *Sim) Err() error { return s.err }

// Alive reports whether the node is running.
func (s *Sim) Alive(id uint64) bool { return s.nodes[id].raft != nil }

// Node returns the running Raft node, or nil if it is crashed.
func (s *Sim) Node(id uint64) *raft.Node { return s.nodes[id].raft }

// Applied returns the entries the node has applied one at a time since it
// last started. Entries covered by a snapshot it loaded are not included.
func (s *Sim) Applied(id uint64) []raft.Entry { return s.nodes[id].applied }

// AppliedIndex returns the last log index in the node's state machine.
func (s *Sim) AppliedIndex(id uint64) uint64 { return s.nodes[id].appliedIndex }

// Data returns a copy of the node's key-value pairs, or nil if it is down.
func (s *Sim) Data(id uint64) map[string]string {
	if n := s.nodes[id]; n.rep != nil {
		return n.rep.Store().Data()
	}
	return nil
}

// DiskLog returns the node's persisted log entries, which start after its
// snapshot if it has one.
func (s *Sim) DiskLog(id uint64) []raft.Entry { return s.nodes[id].log }

// DiskLast returns the index of the last entry on the node's disk.
func (s *Sim) DiskLast(id uint64) uint64 { return s.nodes[id].last() }

// Committed returns every entry any node has ever applied, in log order.
func (s *Sim) Committed() []raft.Entry {
	out := make([]raft.Entry, len(s.check.committed))
	for i, c := range s.check.committed {
		out[i] = c.entry
	}
	return out
}

// Leader returns the running leader with the highest term, or raft.None.
func (s *Sim) Leader() uint64 {
	var best, bestTerm uint64
	for _, id := range s.ids {
		r := s.nodes[id].raft
		if r != nil && r.Role() == raft.Leader && r.Term() >= bestTerm {
			best, bestTerm = id, r.Term()
		}
	}
	return best
}

// Leaders returns every running node that currently believes it is leader.
// More than one is legal as long as their terms differ.
func (s *Sim) Leaders() []uint64 {
	var out []uint64
	for _, id := range s.ids {
		if r := s.nodes[id].raft; r != nil && r.Role() == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// --- driving ---

// Step advances the simulation by one tick: it delivers every message that
// is due, lets clients send, ticks every running node, then checks the
// invariants.
func (s *Sim) Step() error {
	if s.err != nil {
		return s.err
	}
	s.now++
	for len(s.queue) > 0 && s.queue[0].at <= s.now {
		e := heap.Pop(&s.queue).(envelope)
		switch e.kind {
		case kindRaft:
			s.deliver(e.m)
		case kindRequest:
			s.deliverRequest(e)
		case kindResponse:
			s.deliverResponse(e)
		}
	}
	s.stepClients()
	for _, id := range s.ids {
		if n := s.nodes[id]; n.raft != nil {
			n.raft.Tick()
			s.drain(n)
		}
	}
	s.runChecks()
	return s.err
}

// Run steps for the given number of ticks or until an invariant breaks.
func (s *Sim) Run(ticks int) error {
	for i := 0; i < ticks; i++ {
		if err := s.Step(); err != nil {
			return err
		}
	}
	return nil
}

// RunUntil steps until pred holds. It returns false if maxTicks pass first.
func (s *Sim) RunUntil(pred func() bool, maxTicks int) (bool, error) {
	for i := 0; i < maxTicks; i++ {
		if pred() {
			return true, nil
		}
		if err := s.Step(); err != nil {
			return false, err
		}
	}
	return pred(), s.err
}

// Propose hands raw bytes to the given node's Raft log, bypassing the
// client layer. It returns false if the node is down or does not believe
// it is leader.
func (s *Sim) Propose(id uint64, data []byte) bool {
	n := s.nodes[id]
	if n.raft == nil {
		return false
	}
	_, _, ok := n.raft.Propose(data)
	s.drain(n)
	return ok
}

func (s *Sim) deliver(m raft.Message) {
	n := s.nodes[m.To]
	if n.raft == nil || s.blocked[[2]uint64{m.From, m.To}] {
		s.stats.Dropped++
		return
	}
	s.stats.Delivered++
	n.raft.Step(m)
	s.drain(n)
}

// Places inside Ready handling where a node can be killed.
const (
	crashNone = iota
	crashBeforeSave
	crashDuringSave // at a random step of the save
	crashBeforeSend
	crashMidSend
	crashPoints
)

// drain processes Ready in the order the core requires: persist, send,
// restore, apply.
func (s *Sim) drain(n *node) {
	for n.raft != nil && n.raft.HasReady() {
		rd := n.raft.Ready()

		point := crashNone
		if s.opt.CrashPointProb > 0 && s.rng.Float64() < s.opt.CrashPointProb {
			point = 1 + s.rng.Intn(crashPoints-1)
			s.stats.MidReadyCrashes++
		}
		if point == crashBeforeSave {
			s.crash(n)
			return
		}

		if rd.MustSync {
			if point == crashDuringSave {
				// A plain save is a write and a sync. A snapshot adds a
				// dozen more steps: its own file, renames, the rewrite.
				steps := 2
				if rd.Snapshot != nil {
					steps = 14
				}
				n.fs.failIn, n.fs.tearSeed = 1+s.rng.Intn(steps), s.rng.Int63()
			}
			next := n.durable.apply(rd.HardState, rd.Entries, rd.Snapshot)
			err := n.store.Save(rd.HardState, rd.Entries, rd.Snapshot)
			n.fs.failIn = 0
			if err != nil {
				if point != crashDuringSave {
					panic(fmt.Sprintf("sim: node %d: unexpected storage error: %v", n.id, err))
				}
				// A node that cannot persist must not act on the Ready.
				s.stats.TornWrites++
				n.pending = &next
				s.crash(n)
				return
			}
			n.durable, n.dirty = next, true
			if rd.Snapshot != nil {
				s.checkSnapshot(n, rd.Snapshot)
			}
		} else if rd.HardState != n.hs || len(rd.Entries) > 0 || rd.Snapshot != nil {
			// The core changed durable state without asking for a sync.
			// Messages built on that state would go out unprotected.
			s.fail("persist-before-send",
				"node %d has unsaved state (term %d vote %d, %d entries) in a Ready without MustSync",
				n.id, rd.HardState.Term, rd.HardState.Vote, len(rd.Entries))
		}
		if l := len(n.log); l > s.stats.MaxLogLen {
			s.stats.MaxLogLen = l
		}

		msgs := rd.Messages
		if point == crashMidSend {
			msgs = msgs[:s.rng.Intn(len(msgs)+1)]
		} else if point != crashNone {
			msgs = nil
		}
		for _, m := range msgs {
			s.send(m)
		}
		if point != crashNone {
			s.crash(n)
			return
		}

		if rd.RestoreSnapshot {
			resps, err := n.rep.Restore(rd.Snapshot)
			if err != nil {
				s.fail("snapshot", "node %d cannot load the snapshot at %d: %v", n.id, rd.Snapshot.Index, err)
			}
			n.appliedIndex = rd.Snapshot.Index
			s.stats.SnapshotsInstalled++
			for _, r := range resps {
				s.respond(n, r)
			}
		}
		for _, e := range rd.Committed {
			if want := n.appliedIndex + 1; e.Index != want {
				s.fail("apply order", "node %d applied index %d, expected %d", n.id, e.Index, want)
			}
			n.applied = append(n.applied, e)
			n.appliedIndex = e.Index
			s.check.recordApply(s, n, e)
		}
		for _, r := range n.rep.Apply(rd.Committed) {
			s.respond(n, r)
		}
		for _, r := range n.rep.Advance(rd.ReadStates) {
			s.respond(n, r)
		}

		if every := uint64(s.opt.SnapshotEvery); every > 0 && n.appliedIndex >= n.raft.FirstIndex()+every {
			if n.raft.Compact(n.appliedIndex, n.rep.Store().Snapshot()) {
				s.stats.SnapshotsTaken++
			}
		}
	}
}

func (s *Sim) send(m raft.Message) {
	s.stats.Sent++
	if s.blocked[[2]uint64{m.From, m.To}] || s.rng.Float64() < s.opt.DropProb {
		s.stats.Dropped++
		return
	}
	s.enqueueEnvelope(envelope{kind: kindRaft, m: m})
	if s.rng.Float64() < s.opt.DupProb {
		s.enqueueEnvelope(envelope{kind: kindRaft, m: m})
	}
}

func (s *Sim) enqueueEnvelope(e envelope) {
	delay := s.opt.MinDelay
	if span := s.opt.MaxDelay - s.opt.MinDelay; span > 0 {
		delay += uint64(s.rng.Int63n(int64(span) + 1))
	}
	s.seq++
	e.at, e.seq = s.now+delay, s.seq
	heap.Push(&s.queue, e)
}

func (s *Sim) fail(invariant, format string, args ...any) {
	if s.err == nil {
		s.err = fmt.Errorf("seed %d tick %d: %s violated: %s",
			s.opt.Seed, s.now, invariant, fmt.Sprintf(format, args...))
	}
}

// --- fault injection ---

// Crash stops a node. Its in-memory state is lost; its disk is kept.
func (s *Sim) Crash(id uint64) {
	if n := s.nodes[id]; n.raft != nil {
		s.crash(n)
	}
}

func (s *Sim) crash(n *node) {
	n.raft = nil
	n.rep = nil
	n.store = nil
	n.applied, n.appliedIndex, n.checkedAt = nil, 0, 0
	n.fs.crash(s.rng)
	s.stats.Crashes++
}

// Restart brings a crashed node back from its disk.
func (s *Sim) Restart(id uint64) {
	n := s.nodes[id]
	if n.raft != nil {
		return
	}
	s.boot(n)
	s.stats.Restarts++
}

// Partition cuts every link between nodes of different groups, in both
// directions. Nodes not listed in any group are left untouched.
func (s *Sim) Partition(groups ...[]uint64) {
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					s.blocked[[2]uint64{x, y}] = true
				}
			}
		}
	}
}

// BlockLink cuts one direction of one link, for asymmetric partitions.
func (s *Sim) BlockLink(from, to uint64) { s.blocked[[2]uint64{from, to}] = true }

// Heal restores every link.
func (s *Sim) Heal() { s.blocked = make(map[[2]uint64]bool) }

// Partitioned reports whether any link is cut.
func (s *Sim) Partitioned() bool { return len(s.blocked) > 0 }

// SetNetwork changes the loss, duplication and delay settings.
func (s *Sim) SetNetwork(drop, dup float64, minDelay, maxDelay uint64) {
	if minDelay == 0 {
		minDelay = 1
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	s.opt.DropProb, s.opt.DupProb = drop, dup
	s.opt.MinDelay, s.opt.MaxDelay = minDelay, maxDelay
}

// SetCrashPoints changes the chance of a crash inside Ready handling.
func (s *Sim) SetCrashPoints(p float64) { s.opt.CrashPointProb = p }

// TruncateDisk cuts a crashed node's persisted log so that its last entry
// has the given index. It models a disk that acknowledged writes it never
// made durable. Raft does not tolerate this; it exists so tests can prove
// the checker notices.
func (s *Sim) TruncateDisk(id uint64, last uint64) {
	n := s.nodes[id]
	if n.raft != nil {
		panic("sim: TruncateDisk on a running node")
	}
	if last >= n.base() && last < n.last() {
		d := n.durable
		d.log = d.log[:last-n.base()]
		s.rewriteDisk(n, d)
		n.store = nil
	}
}

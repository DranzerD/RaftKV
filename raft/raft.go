package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// Config holds the static parameters of a node.
type Config struct {
	// ID is this node's ID. It must be nonzero and appear in Peers.
	ID uint64
	// Peers lists every node in the cluster, including this one.
	Peers []uint64
	// ElectionTick is the base election timeout in ticks. The real timeout
	// is drawn uniformly from [ElectionTick, 2*ElectionTick).
	ElectionTick int
	// HeartbeatTick is the leader's heartbeat interval in ticks.
	HeartbeatTick int
	// MaxEntriesPerMsg caps the entries in one MsgApp. Defaults to 64.
	MaxEntriesPerMsg int
	// Seed drives the randomized election timeout. The same seed and the
	// same sequence of inputs always produce the same outputs.
	Seed int64
}

// Node is a single Raft participant. It is not safe for concurrent use.
type Node struct {
	cfg   Config
	id    uint64
	peers []uint64 // sorted, includes id
	rng   *rand.Rand

	term uint64
	vote uint64
	// log[0] is a sentinel carrying the index and term of the entry just
	// before the first real one: (0, 0) at first, the snapshot's after a
	// compaction. The entry with index i is at log[i-log[0].Index].
	log  []Entry
	snap *Snapshot // latest snapshot, kept to send to lagging followers

	pendingSnap    *Snapshot // not yet handed out through Ready
	pendingRestore bool

	commit  uint64
	applied uint64 // highest index handed out through Ready.Committed

	role   Role
	leader uint64

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	votes map[uint64]bool   // candidate only: peer -> granted
	next  map[uint64]uint64 // leader only
	match map[uint64]uint64 // leader only
	// Replication mode per follower (leader only). While probing, the
	// leader does not know where the follower's log ends: it sends one
	// append at a time and waits for the answer. Once an append is
	// accepted it switches to pipelining, sending new entries as they
	// arrive without waiting, until the follower rejects one.
	probing  map[uint64]bool
	inflight map[uint64]bool // probing only: a probe is awaiting its answer

	// needBroadcast defers the appends for new proposals and reads to the
	// next Ready, so that everything accepted since the last one travels
	// in a single message per follower.
	needBroadcast bool

	// Linearizable reads (leader only). Each read gets the next readSeq;
	// it is confirmed once a majority has echoed a Ctx at least that large.
	readSeq      uint64
	readAcks     map[uint64]uint64
	pendingReads []pendingRead
	readStates   []ReadState

	msgs     []Message
	hsDirty  bool
	unstable uint64 // lowest log index not yet handed out for persistence; 0 = none
}

type pendingRead struct {
	id    uint64
	index uint64
	seq   uint64
}

// New builds a node. For a fresh node pass a zero HardState, no snapshot
// and no entries. After a crash pass what was persisted; entries must start
// right after the snapshot, or at index 1 if there is none.
func New(cfg Config, hs HardState, snap *Snapshot, entries []Entry) (*Node, error) {
	if cfg.ID == None {
		return nil, errors.New("raft: ID must be nonzero")
	}
	if cfg.HeartbeatTick <= 0 || cfg.ElectionTick <= cfg.HeartbeatTick {
		return nil, errors.New("raft: need 0 < HeartbeatTick < ElectionTick")
	}
	if cfg.MaxEntriesPerMsg <= 0 {
		cfg.MaxEntriesPerMsg = 64
	}
	peers := append([]uint64(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	found := false
	for i, p := range peers {
		if p == None {
			return nil, errors.New("raft: peer ID must be nonzero")
		}
		if i > 0 && peers[i-1] == p {
			return nil, fmt.Errorf("raft: duplicate peer %d", p)
		}
		if p == cfg.ID {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("raft: ID %d is not in Peers", cfg.ID)
	}

	log := make([]Entry, 1, len(entries)+1)
	var prevTerm uint64
	if snap != nil {
		if snap.Index == 0 || snap.Term > hs.Term {
			return nil, fmt.Errorf("raft: invalid snapshot at index %d term %d", snap.Index, snap.Term)
		}
		log[0] = Entry{Index: snap.Index, Term: snap.Term}
		prevTerm = snap.Term
	}
	for i, e := range entries {
		if want := log[0].Index + uint64(i+1); e.Index != want {
			return nil, fmt.Errorf("raft: entry at position %d has index %d, want %d", i, e.Index, want)
		}
		if e.Term < prevTerm || e.Term > hs.Term {
			return nil, fmt.Errorf("raft: entry %d has invalid term %d", e.Index, e.Term)
		}
		prevTerm = e.Term
		log = append(log, e)
	}

	n := &Node{
		cfg:   cfg,
		id:    cfg.ID,
		peers: peers,
		rng:   rand.New(rand.NewSource(cfg.Seed)),
		term:  hs.Term,
		vote:  hs.Vote,
		log:   log,
		snap:  snap,
		role:  Follower,
	}
	// Whatever a snapshot covers is committed and already in the state
	// machine the caller restored from it.
	n.commit, n.applied = log[0].Index, log[0].Index
	n.resetElectionTimer()
	return n, nil
}

// ID returns this node's ID.
func (n *Node) ID() uint64 { return n.id }

// Term returns the current term.
func (n *Node) Term() uint64 { return n.term }

// Role returns the current role.
func (n *Node) Role() Role { return n.role }

// Leader returns the node this one believes is leader, or None.
func (n *Node) Leader() uint64 { return n.leader }

// Commit returns the commit index.
func (n *Node) Commit() uint64 { return n.commit }

// LastIndex returns the index of the last log entry.
func (n *Node) LastIndex() uint64 { return n.log[0].Index + uint64(len(n.log)-1) }

// FirstIndex returns the index covered by the latest snapshot, or 0. Only
// entries after it are still in the log.
func (n *Node) FirstIndex() uint64 { return n.log[0].Index }

// pos converts a log index to a position in n.log.
func (n *Node) pos(i uint64) uint64 { return i - n.log[0].Index }

func (n *Node) termAt(i uint64) uint64 { return n.log[n.pos(i)].Term }

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) isPeer(id uint64) bool {
	for _, p := range n.peers {
		if p == id {
			return true
		}
	}
	return false
}

func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTick + n.rng.Intn(n.cfg.ElectionTick)
}

// Tick advances the logical clock by one tick.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			// A probe that got no answer within a heartbeat is lost.
			for _, p := range n.peers {
				n.inflight[p] = false
			}
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

// Propose appends a client command to the log if this node is leader.
// The entry is not committed yet; it shows up in Ready.Committed once a
// majority has stored it, and it may be lost if leadership changes first.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	if n.role != Leader {
		return 0, 0, false
	}
	n.appendEntry(Entry{Type: EntryNormal, Data: data})
	n.maybeCommit()
	n.needBroadcast = true
	return n.LastIndex(), n.term, true
}

// Step feeds one incoming message into the node.
func (n *Node) Step(m Message) {
	if m.To != n.id || m.From == n.id || !n.isPeer(m.From) {
		return
	}

	switch {
	case m.Term > n.term:
		lead := None
		if m.Type == MsgApp || m.Type == MsgSnap {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// Answer stale requests so the sender learns the newer term and
		// steps down. Stale responses are simply dropped.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		case MsgApp, MsgSnap:
			n.send(Message{Type: MsgAppResp, To: m.From, Reject: true, LogIndex: m.LogIndex})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	case MsgSnap:
		n.handleSnapshot(m)
	}
}

// HasReady reports whether Ready would return any work.
func (n *Node) HasReady() bool {
	return n.hsDirty || n.unstable != 0 || len(n.msgs) > 0 || n.commit > n.applied ||
		len(n.readStates) > 0 || n.pendingSnap != nil || n.needBroadcast
}

// Compact discards the log up to and including index, which the caller's
// state machine must already have applied, and keeps data as the snapshot
// that stands in for it. The snapshot comes back in the next Ready to be
// persisted. It returns false if index is not applied yet or is already
// covered by a snapshot.
func (n *Node) Compact(index uint64, data []byte) bool {
	if index > n.applied || index <= n.FirstIndex() {
		return false
	}
	s := &Snapshot{Index: index, Term: n.termAt(index), Data: data}
	n.log = append([]Entry{{Index: s.Index, Term: s.Term}}, n.log[n.pos(index)+1:]...)
	n.snap = s
	n.pendingSnap, n.pendingRestore = s, false
	n.dropUnstableThrough(index)
	return true
}

// dropUnstableThrough forgets that entries up to index still need to be
// persisted, because a snapshot now covers them.
func (n *Node) dropUnstableThrough(index uint64) {
	if n.unstable != 0 && n.unstable <= index {
		n.unstable = index + 1
	}
	if n.unstable > n.LastIndex() {
		n.unstable = 0
	}
}

// ReadIndex asks the leader to confirm that it still leads, so that a read
// can be served without going through the log (Raft dissertation, section
// 6.4). The confirmation arrives as a ReadState with the same id. It
// returns false if this node is not leader, or is a leader that has not
// yet committed an entry in its own term and so cannot know the true
// commit index.
//
// A deposed leader never gets its confirmation: a majority that has moved
// to a newer term will not echo its heartbeats in the old one.
func (n *Node) ReadIndex(id uint64) bool {
	if n.role != Leader || n.termAt(n.commit) != n.term {
		return false
	}
	n.readSeq++
	n.pendingReads = append(n.pendingReads, pendingRead{id: id, index: n.commit, seq: n.readSeq})
	if n.quorum() == 1 {
		n.confirmReads()
		return true
	}
	n.needBroadcast = true // the appends double as the heartbeat round
	return true
}

func (n *Node) confirmReads() {
	acks := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			acks = append(acks, n.readSeq)
		} else {
			acks = append(acks, n.readAcks[p])
		}
	}
	sort.Slice(acks, func(i, j int) bool { return acks[i] > acks[j] })
	confirmed := acks[n.quorum()-1]
	i := 0
	for ; i < len(n.pendingReads) && n.pendingReads[i].seq <= confirmed; i++ {
		n.readStates = append(n.readStates, ReadState{ID: n.pendingReads[i].id, Index: n.pendingReads[i].index})
	}
	n.pendingReads = n.pendingReads[i:]
}

// Ready returns the pending work and marks it as handed out. See the Ready
// type for the order in which the caller must process it. The returned
// slices are copies and stay valid after later calls into the node.
func (n *Node) Ready() Ready {
	if n.needBroadcast {
		n.needBroadcast = false
		if n.role == Leader {
			n.broadcastAppend()
		}
	}
	rd := Ready{HardState: HardState{Term: n.term, Vote: n.vote}}
	if n.unstable != 0 {
		rd.Entries = append([]Entry(nil), n.log[n.pos(n.unstable):]...)
	}
	rd.Snapshot, rd.RestoreSnapshot = n.pendingSnap, n.pendingRestore
	n.pendingSnap, n.pendingRestore = nil, false
	rd.MustSync = n.hsDirty || len(rd.Entries) > 0 || rd.Snapshot != nil
	if n.commit > n.applied {
		rd.Committed = append([]Entry(nil), n.log[n.pos(n.applied)+1:n.pos(n.commit)+1]...)
		n.applied = n.commit
	}
	rd.Messages = n.msgs
	n.msgs = nil
	rd.ReadStates = n.readStates
	n.readStates = nil
	n.hsDirty = false
	n.unstable = 0
	return rd
}

// --- role transitions ---

func (n *Node) becomeFollower(term, leader uint64) {
	wasLeader := n.role == Leader
	if term > n.term {
		n.term = term
		n.vote = None
		n.hsDirty = true
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.next = nil
	n.match = nil
	n.readAcks = nil
	n.pendingReads = nil
	n.probing, n.inflight = nil, nil
	n.needBroadcast = false
	// A leader does not run an election timer, so it needs a fresh one.
	// Followers and candidates keep theirs: seeing a higher term is not a
	// reason to postpone the next election.
	if wasLeader {
		n.resetElectionTimer()
	}
}

func (n *Node) campaign() {
	n.role = Candidate
	n.term++
	n.vote = n.id
	n.hsDirty = true
	n.leader = None
	n.votes = map[uint64]bool{n.id: true}
	n.resetElectionTimer()
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	last := n.LastIndex()
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, LogIndex: last, LogTerm: n.termAt(last)})
		}
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.votes = nil
	n.heartbeatElapsed = 0
	n.next = make(map[uint64]uint64, len(n.peers))
	n.match = make(map[uint64]uint64, len(n.peers))
	n.readAcks = make(map[uint64]uint64, len(n.peers))
	n.pendingReads = nil
	n.probing = make(map[uint64]bool, len(n.peers))
	n.inflight = make(map[uint64]bool, len(n.peers))
	n.needBroadcast = false
	last := n.LastIndex()
	for _, p := range n.peers {
		n.next[p] = last + 1
		n.match[p] = 0
		n.probing[p] = true
	}
	// Entries from earlier terms can only be committed indirectly, by
	// committing an entry of the current term on top of them. The no-op
	// guarantees there is one even if no client writes arrive.
	n.appendEntry(Entry{Type: EntryNoop})
	n.maybeCommit()
	n.broadcastAppend()
}

// --- log helpers ---

func (n *Node) appendEntry(e Entry) {
	e.Index = n.LastIndex() + 1
	e.Term = n.term
	n.log = append(n.log, e)
	n.markUnstable(e.Index)
	n.match[n.id] = e.Index
	n.next[n.id] = e.Index + 1
}

func (n *Node) markUnstable(i uint64) {
	if n.unstable == 0 || i < n.unstable {
		n.unstable = i
	}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to uint64) {
	if n.probing[to] {
		if n.inflight[to] {
			return // one probe at a time; the heartbeat retries it
		}
		n.inflight[to] = true
	}
	for {
		next := n.next[to]
		if next <= n.FirstIndex() {
			// The entries this follower needs are gone. Send the
			// snapshot and probe from just after it; if the snapshot is
			// lost, that probe is rejected and next falls back here.
			n.send(Message{Type: MsgSnap, To: to, Snapshot: n.snap, Ctx: n.readSeq})
			n.next[to] = n.snap.Index + 1
			n.probing[to], n.inflight[to] = true, true
			return
		}
		hi := n.LastIndex() + 1
		if max := next + uint64(n.cfg.MaxEntriesPerMsg); hi > max {
			hi = max
		}
		// Copy: the log's backing array can be overwritten after a
		// truncation, and messages outlive this call.
		ents := append([]Entry(nil), n.log[n.pos(next):n.pos(hi)]...)
		n.send(Message{
			Type:     MsgApp,
			To:       to,
			LogIndex: next - 1,
			LogTerm:  n.termAt(next - 1),
			Entries:  ents,
			Commit:   n.commit,
			Ctx:      n.readSeq,
		})
		if n.probing[to] {
			return // next moves only when the follower answers
		}
		// Pipelining: assume it lands and keep going.
		n.next[to] = hi
		if hi > n.LastIndex() {
			return
		}
	}
}

// maybeCommit advances the commit index to the highest index stored on a
// majority, but only if that entry is from the current term (Raft paper,
// section 5.4.2 and Figure 8). It reports whether the index moved.
func (n *Node) maybeCommit() bool {
	ms := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		ms = append(ms, n.match[p])
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] > ms[j] })
	idx := ms[n.quorum()-1]
	if idx > n.commit && n.termAt(idx) == n.term {
		n.commit = idx
		return true
	}
	return false
}

// --- message handlers (m.Term == n.term in all of them) ---

func (n *Node) handleVote(m Message) {
	last := n.LastIndex()
	lastTerm := n.termAt(last)
	upToDate := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.LogIndex >= last)
	grant := (n.vote == None || n.vote == m.From) && upToDate
	if grant {
		if n.vote != m.From {
			n.vote = m.From
			n.hsDirty = true
		}
		// Only a granted vote resets the timer. A rejected request must
		// not, or a node with a stale log could stall elections forever.
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Reject: !grant})
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate {
		return
	}
	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = !m.Reject
	}
	granted, rejected := 0, 0
	for _, g := range n.votes {
		if g {
			granted++
		} else {
			rejected++
		}
	}
	switch {
	case granted >= n.quorum():
		n.becomeLeader()
	case rejected >= n.quorum():
		n.becomeFollower(n.term, None)
	}
}

func (n *Node) handleAppend(m Message) {
	if n.role == Leader {
		// Two leaders in one term would violate election safety. Ignore
		// rather than corrupt state; the simulator checks the invariant.
		return
	}
	n.role = Follower
	n.votes = nil
	n.leader = m.From
	n.resetElectionTimer()

	if first := n.FirstIndex(); m.LogIndex < first {
		// Everything up to first is committed here, so it matches the
		// leader's log. Pretend the append started there.
		skip := first - m.LogIndex
		if uint64(len(m.Entries)) <= skip {
			n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: n.commit, Ctx: m.Ctx})
			return
		}
		m.Entries = m.Entries[skip:]
		m.LogIndex, m.LogTerm = first, n.termAt(first)
	}

	last := n.LastIndex()
	if m.LogIndex > last || n.termAt(m.LogIndex) != m.LogTerm {
		// Tell the leader where to retry so it can skip a whole term per
		// round trip instead of one entry.
		hint := last + 1
		if m.LogIndex <= last {
			ct := n.termAt(m.LogIndex)
			hint = m.LogIndex
			for hint > n.FirstIndex()+1 && n.termAt(hint-1) == ct {
				hint--
			}
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Reject: true, LogIndex: m.LogIndex, Hint: hint, Ctx: m.Ctx})
		return
	}

	// Truncate only on a real conflict. A delayed or duplicated MsgApp
	// that is a prefix of what we already hold must not shorten the log.
	for i, e := range m.Entries {
		if e.Index <= n.LastIndex() && n.termAt(e.Index) == e.Term {
			continue
		}
		if e.Index <= n.commit {
			panic(fmt.Sprintf("raft: node %d asked to overwrite committed index %d", n.id, e.Index))
		}
		n.log = append(n.log[:n.pos(e.Index)], m.Entries[i:]...)
		n.markUnstable(e.Index)
		break
	}

	lastNew := m.LogIndex + uint64(len(m.Entries))
	c := m.Commit
	if c > lastNew {
		c = lastNew
	}
	if c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: lastNew, Ctx: m.Ctx})
}

// handleSnapshot installs the leader's snapshot when this node is so far
// behind that the entries it needs have been compacted away.
func (n *Node) handleSnapshot(m Message) {
	if n.role == Leader || m.Snapshot == nil {
		return
	}
	n.role = Follower
	n.votes = nil
	n.leader = m.From
	n.resetElectionTimer()

	s := m.Snapshot
	if s.Index <= n.commit {
		// Nothing new. Our committed prefix matches the leader's log, so
		// report it as the match point and let appends resume from there.
		n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: n.commit, Ctx: m.Ctx})
		return
	}
	if s.Index <= n.LastIndex() && n.termAt(s.Index) == s.Term {
		// Our log agrees with the snapshot's last entry, so what follows
		// it is still good (Log Matching).
		n.log = append([]Entry{{Index: s.Index, Term: s.Term}}, n.log[n.pos(s.Index)+1:]...)
	} else {
		n.log = []Entry{{Index: s.Index, Term: s.Term}}
	}
	n.dropUnstableThrough(s.Index)
	n.commit, n.applied = s.Index, s.Index
	n.snap = s
	n.pendingSnap, n.pendingRestore = s, true
	n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: s.Index, Ctx: m.Ctx})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	from := m.From
	// Any response in our term, accept or reject, shows the follower still
	// recognised us as leader when it handled a message at least this new.
	if m.Ctx > n.readAcks[from] {
		n.readAcks[from] = m.Ctx
		n.confirmReads()
	}

	if m.Reject {
		// m.LogIndex is the prev index the follower refused.
		if m.LogIndex <= n.match[from] {
			return // stale: it has since matched beyond that point
		}
		if n.probing[from] && m.LogIndex != n.next[from]-1 {
			return // answers an older message, not the current probe
		}
		nx := m.Hint
		if nx > m.LogIndex {
			nx = m.LogIndex
		}
		if nx < n.match[from]+1 {
			nx = n.match[from] + 1
		}
		// Back up and probe. Rejections of other pipelined appends that
		// are still in flight will not match the new probe and are
		// ignored, so one lost message costs one resend, not many.
		n.next[from] = nx
		n.probing[from], n.inflight[from] = true, false
		n.sendAppend(from)
		return
	}

	// The follower's log agrees with ours through m.LogIndex.
	if n.probing[from] && m.LogIndex+1 >= n.next[from] {
		n.probing[from], n.inflight[from] = false, false
	}
	advanced := m.LogIndex > n.match[from]
	if advanced {
		n.match[from] = m.LogIndex
	}
	if n.next[from] < n.match[from]+1 {
		n.next[from] = n.match[from] + 1
	}
	if advanced && n.maybeCommit() {
		n.broadcastAppend() // spread the new commit index right away
		return
	}
	if n.next[from] <= n.LastIndex() {
		n.sendAppend(from)
	}
}

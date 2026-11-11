// Package raft is a deterministic Raft core with no I/O.
//
// A Node never reads a clock, opens a socket or touches a disk. The caller
// feeds it clock ticks (Tick), incoming messages (Step) and client proposals
// (Propose), then collects the consequences with Ready. That makes the whole
// protocol runnable inside a simulator with a fake clock and a fake network.
package raft

// None is the zero node ID, meaning "no node".
const None uint64 = 0

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries a client command in Data.
	EntryNormal EntryType = iota
	// EntryNoop is appended by a new leader so that entries from earlier
	// terms can be committed indirectly. State machines skip it.
	EntryNoop
)

// Entry is one slot of the replicated log. Data is treated as immutable.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState is the state that must survive a crash, besides the log.
type HardState struct {
	Term uint64
	Vote uint64
}

// MsgType identifies a Raft RPC or its response.
type MsgType uint8

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	}
	return "Unknown"
}

// Message is the single wire type for all RPCs. Term is always the sender's
// current term. The meaning of the remaining fields depends on Type:
//
//	MsgVote      LogIndex/LogTerm = candidate's last log entry
//	MsgVoteResp  Reject
//	MsgApp       LogIndex/LogTerm = entry preceding Entries; Commit = leader commit
//	MsgAppResp   success: LogIndex = follower's match index
//	             reject:  LogIndex = the rejected prev index, Hint = where to retry
//	MsgSnap      Snapshot = the leader's snapshot; answered with MsgAppResp
//
// Ctx rides on MsgApp and is echoed in MsgAppResp. The leader uses it to
// tell which responses were sent after a given read request arrived.
type Message struct {
	Type     MsgType
	From     uint64
	To       uint64
	Term     uint64
	LogIndex uint64
	LogTerm  uint64
	Entries  []Entry
	Commit   uint64
	Reject   bool
	Hint     uint64
	Ctx      uint64
	Snapshot *Snapshot
}

// Snapshot is the state machine's serialised state after applying every
// entry up to and including Index. Term is the term of that entry. Data is
// treated as immutable.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Role is a node's current role in the protocol.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// Ready is everything the core wants the outside world to do. The caller
// must handle it in this order:
//
//  1. Persist Snapshot, HardState and Entries durably (when MustSync is
//     set). A snapshot replaces the stored log up to its index. Entries
//     replace the stored log from Entries[0].Index onward.
//  2. Send Messages.
//  3. If RestoreSnapshot is set, load Snapshot into the state machine.
//  4. Apply Committed to the state machine, in order.
//
// Doing step 2 before step 1 breaks Raft: a node would promise a vote or
// acknowledge an entry that a crash could make it forget.
type Ready struct {
	HardState HardState
	Entries   []Entry
	MustSync  bool
	// Snapshot is set when the log was compacted. RestoreSnapshot says it
	// came from the leader and the state machine must be replaced by it;
	// otherwise it is this node's own and the state machine already holds it.
	Snapshot        *Snapshot
	RestoreSnapshot bool
	Messages        []Message
	Committed       []Entry
	// ReadStates lists reads the leader has confirmed. A read may be
	// served once the state machine has applied through its Index.
	ReadStates []ReadState
}

// ReadState says that the read registered under ID is linearizable as soon
// as the state machine has applied every entry up to Index.
type ReadState struct {
	ID    uint64
	Index uint64
}

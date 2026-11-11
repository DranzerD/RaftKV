// Package kv is the replicated state machine: an in-memory key-value store
// that applies each client command exactly once.
//
// Raft only promises that every node sees the same log. A client that
// times out and retries can put the same command in the log twice, so the
// store keeps a session per client: the highest sequence number applied
// and its result. A retry gets the saved result instead of a second run.
// A client must therefore issue one command at a time, with increasing
// sequence numbers. Sessions never expire.
package kv

import (
	"encoding/binary"
	"errors"
	"sort"
)

// Op is a command type.
type Op uint8

const (
	OpGet Op = iota + 1
	OpPut
	OpDelete
	OpCAS // set Value only if the key exists and holds Expect
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	}
	return "unknown"
}

// Command is one client request. Client 0 means "no session": the command
// is applied every time it appears.
type Command struct {
	Client uint64
	Seq    uint64
	Op     Op
	Key    string
	Value  string
	Expect string // OpCAS only
}

// Result is the outcome of a command.
//
//	get     Found, Value
//	put     OK = true
//	delete  OK = the key existed
//	cas     OK = swapped; otherwise Found and Value describe what is there
type Result struct {
	OK    bool
	Found bool
	Value string
}

// Status says how the store treated a command.
type Status uint8

const (
	Applied   Status = iota // ran now
	Duplicate               // a retry of the client's latest command; saved result returned
	Stale                   // older than the client's latest command; ignored
)

const magic = 0xCB

// Encode serialises a command for the Raft log.
func (c Command) Encode() []byte {
	b := make([]byte, 0, 30+len(c.Key)+len(c.Value)+len(c.Expect))
	b = append(b, magic, byte(c.Op))
	b = binary.LittleEndian.AppendUint64(b, c.Client)
	b = binary.LittleEndian.AppendUint64(b, c.Seq)
	for _, s := range []string{c.Key, c.Value, c.Expect} {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	return b
}

// ErrNotCommand means the bytes are not an encoded Command.
var ErrNotCommand = errors.New("kv: not a command")

// Decode parses the output of Encode.
func Decode(b []byte) (Command, error) {
	var c Command
	if len(b) < 18 || b[0] != magic || b[1] < byte(OpGet) || b[1] > byte(OpCAS) {
		return c, ErrNotCommand
	}
	c.Op = Op(b[1])
	c.Client = binary.LittleEndian.Uint64(b[2:10])
	c.Seq = binary.LittleEndian.Uint64(b[10:18])
	b = b[18:]
	for _, dst := range []*string{&c.Key, &c.Value, &c.Expect} {
		if len(b) < 4 {
			return Command{}, ErrNotCommand
		}
		n := binary.LittleEndian.Uint32(b)
		b = b[4:]
		if uint64(n) > uint64(len(b)) {
			return Command{}, ErrNotCommand
		}
		*dst = string(b[:n])
		b = b[n:]
	}
	if len(b) != 0 {
		return Command{}, ErrNotCommand
	}
	return c, nil
}

type session struct {
	seq uint64
	res Result
}

// Store is the state machine. It is deterministic: the same commands in
// the same order always produce the same state and results.
type Store struct {
	data     map[string]string
	sessions map[uint64]session
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: make(map[string]string), sessions: make(map[uint64]session)}
}

// Apply runs a command that has been committed to the log.
func (s *Store) Apply(c Command) (Result, Status) {
	if c.Client != 0 {
		if sess, ok := s.sessions[c.Client]; ok {
			if c.Seq == sess.seq {
				return sess.res, Duplicate
			}
			if c.Seq < sess.seq {
				return Result{}, Stale
			}
		}
	}
	res := s.run(c)
	if c.Client != 0 {
		s.sessions[c.Client] = session{seq: c.Seq, res: res}
	}
	return res, Applied
}

// Read answers a get without going through the log or touching sessions.
// The caller is responsible for making the read linearizable.
func (s *Store) Read(key string) Result {
	v, ok := s.data[key]
	return Result{Found: ok, Value: v}
}

func (s *Store) run(c Command) Result {
	cur, found := s.data[c.Key]
	switch c.Op {
	case OpGet:
		return Result{Found: found, Value: cur}
	case OpPut:
		s.data[c.Key] = c.Value
		return Result{OK: true}
	case OpDelete:
		delete(s.data, c.Key)
		return Result{OK: found}
	case OpCAS:
		if found && cur == c.Expect {
			s.data[c.Key] = c.Value
			return Result{OK: true}
		}
		return Result{Found: found, Value: cur}
	}
	return Result{}
}

// Len returns the number of keys.
func (s *Store) Len() int { return len(s.data) }

// Data returns a copy of the key-value pairs.
func (s *Store) Data() map[string]string {
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Snapshot serialises the whole store, sessions included. Sessions must
// travel with the data: a node restored from a snapshot without them would
// re-run retries of commands the snapshot already contains.
func (s *Store) Snapshot() []byte {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	clients := make([]uint64, 0, len(s.sessions))
	for c := range s.sessions {
		clients = append(clients, c)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })

	str := func(b []byte, v string) []byte {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v)))
		return append(b, v...)
	}
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(keys)))
	for _, k := range keys {
		b = str(str(b, k), s.data[k])
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(clients)))
	for _, c := range clients {
		sess := s.sessions[c]
		b = binary.LittleEndian.AppendUint64(b, c)
		b = binary.LittleEndian.AppendUint64(b, sess.seq)
		var flags byte
		if sess.res.OK {
			flags |= 1
		}
		if sess.res.Found {
			flags |= 2
		}
		b = str(append(b, flags), sess.res.Value)
	}
	return b
}

// ErrBadSnapshot means the bytes are not the output of Snapshot.
var ErrBadSnapshot = errors.New("kv: malformed snapshot")

// Restore replaces the store's contents with a snapshot.
func (s *Store) Restore(b []byte) error {
	u32 := func() (uint32, bool) {
		if len(b) < 4 {
			return 0, false
		}
		v := binary.LittleEndian.Uint32(b)
		b = b[4:]
		return v, true
	}
	u64 := func() (uint64, bool) {
		if len(b) < 8 {
			return 0, false
		}
		v := binary.LittleEndian.Uint64(b)
		b = b[8:]
		return v, true
	}
	str := func() (string, bool) {
		n, ok := u32()
		if !ok || uint64(n) > uint64(len(b)) {
			return "", false
		}
		v := string(b[:n])
		b = b[n:]
		return v, true
	}

	data := make(map[string]string)
	sessions := make(map[uint64]session)
	n, ok := u32()
	if !ok {
		return ErrBadSnapshot
	}
	for i := uint32(0); i < n; i++ {
		k, ok1 := str()
		v, ok2 := str()
		if !ok1 || !ok2 {
			return ErrBadSnapshot
		}
		data[k] = v
	}
	n, ok = u32()
	if !ok {
		return ErrBadSnapshot
	}
	for i := uint32(0); i < n; i++ {
		c, ok1 := u64()
		seq, ok2 := u64()
		if !ok1 || !ok2 || len(b) < 1 {
			return ErrBadSnapshot
		}
		flags := b[0]
		b = b[1:]
		v, ok3 := str()
		if !ok3 {
			return ErrBadSnapshot
		}
		sessions[c] = session{seq: seq, res: Result{OK: flags&1 != 0, Found: flags&2 != 0, Value: v}}
	}
	if len(b) != 0 {
		return ErrBadSnapshot
	}
	s.data, s.sessions = data, sessions
	return nil
}

// Package server runs one RaftKV node for real: a goroutine that drives the
// deterministic replica with a wall-clock ticker, a disk-backed store and
// an HTTP transport.
//
// Everything that decides anything lives in the raft, replica and kv
// packages, which the simulator exercises. This package only moves bytes
// and time, in the order the core requires: persist, send, apply.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
	"github.com/DranzerD/RaftKV/replica"
	"github.com/DranzerD/RaftKV/storage"
)

// Config describes one node.
type Config struct {
	ID uint64
	// Peers maps every node ID, this one included, to its host:port.
	Peers map[uint64]string
	// DataDir holds the WAL and snapshots.
	DataDir string
	// Listener, if set, is used instead of listening on Peers[ID].
	Listener net.Listener

	TickInterval  time.Duration // default 10ms
	ElectionTick  int           // default 20 ticks
	HeartbeatTick int           // default 4 ticks
	// SnapshotEvery compacts the log after that many applied entries.
	// Default 10000; negative disables snapshots.
	SnapshotEvery int
	// MaxBatch caps how many client requests are folded into one disk
	// sync. Default 256; 1 turns batching off.
	MaxBatch int
	// NoSync skips fsync. Acknowledged writes can then be lost in a power
	// cut. For benchmarks only.
	NoSync bool
	Logf   func(format string, args ...any) // default log.Printf
}

// Status is a point-in-time view of a node.
type Status struct {
	ID         uint64 `json:"id"`
	Term       uint64 `json:"term"`
	Role       string `json:"role"`
	Leader     uint64 `json:"leader"`
	Commit     uint64 `json:"commit"`
	Applied    uint64 `json:"applied"`
	FirstIndex uint64 `json:"first_index"`
	LastIndex  uint64 `json:"last_index"`
	Keys       int    `json:"keys"`
	WALBytes   int64  `json:"wal_bytes"`
}

type request struct {
	cmd  kv.Command
	resp chan replica.Response
}

// Server is a running node.
type Server struct {
	cfg   Config
	rep   *replica.Replica
	store *storage.Store
	tr    *transport
	http  *http.Server
	ln    net.Listener

	recvc chan raft.Message
	reqc  chan request
	stopc chan struct{}
	done  chan struct{}
	once  sync.Once

	mu      sync.Mutex
	waiters map[[2]uint64]chan replica.Response

	status  atomic.Pointer[Status]
	metrics metrics
}

// Start recovers the node's state from disk and begins serving.
func Start(cfg Config) (*Server, error) {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 10 * time.Millisecond
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 20
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 4
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 10000
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: node %d is not in the peer list", cfg.ID)
	}

	fs, err := storage.NewDirFS(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	fs.NoSync = cfg.NoSync
	store, st, err := storage.OpenStore(fs)
	if err != nil {
		return nil, fmt.Errorf("server: recover %s: %w", cfg.DataDir, err)
	}
	ids := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	node, err := raft.New(raft.Config{
		ID:            cfg.ID,
		Peers:         ids,
		ElectionTick:  cfg.ElectionTick,
		HeartbeatTick: cfg.HeartbeatTick,
		Seed:          time.Now().UnixNano() ^ int64(cfg.ID)<<32,
	}, st.HardState, st.Snapshot, st.Entries)
	if err != nil {
		store.Close()
		return nil, err
	}
	rep := replica.New(node)
	if st.Snapshot != nil {
		if _, err := rep.Restore(st.Snapshot); err != nil {
			store.Close()
			return nil, fmt.Errorf("server: load snapshot: %w", err)
		}
	}

	ln := cfg.Listener
	if ln == nil {
		if ln, err = net.Listen("tcp", cfg.Peers[cfg.ID]); err != nil {
			store.Close()
			return nil, err
		}
	}
	s := &Server{
		cfg:     cfg,
		rep:     rep,
		store:   store,
		ln:      ln,
		recvc:   make(chan raft.Message, 4096),
		reqc:    make(chan request, 4096),
		stopc:   make(chan struct{}),
		done:    make(chan struct{}),
		waiters: make(map[[2]uint64]chan replica.Response),
	}
	s.tr = newTransport(cfg.ID, cfg.Peers, &s.metrics)
	s.publishStatus()
	s.http = &http.Server{
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}
	go s.http.Serve(ln)
	go s.run()
	cfg.Logf("node %d: serving on %s, recovered term %d, log %d..%d",
		cfg.ID, ln.Addr(), st.HardState.Term, node.FirstIndex()+1, node.LastIndex())
	return s, nil
}

// Addr returns the address the node is listening on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Status returns the node's latest published state.
func (s *Server) Status() Status { return *s.status.Load() }

// Stop shuts the node down. It is safe to call more than once.
func (s *Server) Stop() {
	s.once.Do(func() {
		close(s.stopc)
		<-s.done
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s.http.Shutdown(ctx)
		cancel()
		s.http.Close()
		s.tr.stop()
		s.store.Close()
	})
}

// Do submits one command and waits for this node's answer.
func (s *Server) Do(ctx context.Context, cmd kv.Command) (replica.Response, error) {
	req := request{cmd: cmd, resp: make(chan replica.Response, 1)}
	select {
	case s.reqc <- req:
	case <-ctx.Done():
		return replica.Response{}, ctx.Err()
	case <-s.done: // stopped, or halted by a storage failure
		return replica.Response{}, errStopped
	}
	select {
	case r := <-req.resp:
		return r, nil
	case <-ctx.Done():
		s.forget(cmd, req.resp)
		return replica.Response{}, ctx.Err()
	case <-s.done: // stopped, or halted by a storage failure
		return replica.Response{}, errStopped
	}
}

var errStopped = errors.New("server: stopped")

func (s *Server) forget(cmd kv.Command, ch chan replica.Response) {
	key := [2]uint64{cmd.Client, cmd.Seq}
	s.mu.Lock()
	if s.waiters[key] == ch {
		delete(s.waiters, key)
	}
	s.mu.Unlock()
}

// run is the node's only goroutine that touches the replica.
func (s *Server) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopc:
			return
		case <-ticker.C:
			s.rep.Raft().Tick()
		case m := <-s.recvc:
			s.rep.Raft().Step(m)
			// Take whatever else has arrived, so that one disk sync
			// covers all of it.
			for more := true; more; {
				select {
				case m := <-s.recvc:
					s.rep.Raft().Step(m)
				default:
					more = false
				}
			}
		case req := <-s.reqc:
			s.accept(req)
			for n := 1; n < s.cfg.MaxBatch; n++ {
				select {
				case req := <-s.reqc:
					s.accept(req)
					continue
				default:
				}
				break
			}
		}
		if err := s.processReady(); err != nil {
			// A node that cannot persist must stop: carrying on would
			// mean acknowledging state it may not have.
			s.cfg.Logf("node %d: storage failure, halting: %v", s.cfg.ID, err)
			return
		}
		s.publishStatus()
	}
}

func (s *Server) accept(req request) {
	key := [2]uint64{req.cmd.Client, req.cmd.Seq}
	s.mu.Lock()
	if old, ok := s.waiters[key]; ok {
		// A retry of a request still in flight takes over the wait.
		old <- replica.Response{Client: key[0], Seq: key[1], Status: replica.Retry}
	}
	s.waiters[key] = req.resp
	s.mu.Unlock()
	s.reply(s.rep.Request(req.cmd))
}

func (s *Server) reply(resps []replica.Response) {
	if len(resps) == 0 {
		return
	}
	s.mu.Lock()
	for _, r := range resps {
		key := [2]uint64{r.Client, r.Seq}
		if ch, ok := s.waiters[key]; ok {
			ch <- r // buffered; never blocks
			delete(s.waiters, key)
		}
	}
	s.mu.Unlock()
}

// processReady handles Ready in the order the core requires.
func (s *Server) processReady() error {
	node := s.rep.Raft()
	for node.HasReady() {
		rd := node.Ready()
		if rd.MustSync {
			start := time.Now()
			if err := s.store.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
				return err
			}
			s.metrics.saves.Add(1)
			s.metrics.saveNanos.Add(time.Since(start).Nanoseconds())
			s.metrics.entriesSaved.Add(int64(len(rd.Entries)))
		}
		for _, m := range rd.Messages {
			s.tr.send(m)
		}
		if rd.RestoreSnapshot {
			resps, err := s.rep.Restore(rd.Snapshot)
			if err != nil {
				return fmt.Errorf("load snapshot at %d: %w", rd.Snapshot.Index, err)
			}
			s.metrics.snapshotsInstalled.Add(1)
			s.reply(resps)
		}
		s.reply(s.rep.Apply(rd.Committed))
		s.reply(s.rep.Advance(rd.ReadStates))

		if every := s.cfg.SnapshotEvery; every > 0 && s.rep.Applied() >= node.FirstIndex()+uint64(every) {
			if node.Compact(s.rep.Applied(), s.rep.Store().Snapshot()) {
				s.metrics.snapshotsTaken.Add(1)
			}
		}
	}
	return nil
}

func (s *Server) publishStatus() {
	node := s.rep.Raft()
	s.status.Store(&Status{
		ID:         s.cfg.ID,
		Term:       node.Term(),
		Role:       node.Role().String(),
		Leader:     node.Leader(),
		Commit:     node.Commit(),
		Applied:    s.rep.Applied(),
		FirstIndex: node.FirstIndex(),
		LastIndex:  node.LastIndex(),
		Keys:       s.rep.Store().Len(),
		WALBytes:   s.store.WALSize(),
	})
}

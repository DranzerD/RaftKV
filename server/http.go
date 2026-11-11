package server

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
	"github.com/DranzerD/RaftKV/replica"
)

// Request is the JSON body of POST /kv.
type Request struct {
	Client uint64 `json:"client"`
	Seq    uint64 `json:"seq"`
	Op     string `json:"op"` // get, put, delete, cas
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Expect string `json:"expect,omitempty"`
}

// Response is the JSON answer to POST /kv.
type Response struct {
	Status string `json:"status"` // ok, not-leader, retry
	Leader uint64 `json:"leader,omitempty"`
	OK     bool   `json:"ok"`
	Found  bool   `json:"found"`
	Value  string `json:"value,omitempty"`
}

type metrics struct {
	requestsOK, requestsNotLeader, requestsRetry, requestsTimeout atomic.Int64
	saves, saveNanos, entriesSaved                                atomic.Int64
	messagesSent, messagesReceived, sendErrors                    atomic.Int64
	snapshotsTaken, snapshotsInstalled                            atomic.Int64
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", s.handleRaft)
	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

// Request body limits. A Raft batch can carry a whole snapshot, so it gets
// far more room than a client command.
const (
	maxRaftBody = 1 << 30
	maxKVBody   = 1 << 20
)

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var batch []raft.Message
	if err := gob.NewDecoder(http.MaxBytesReader(w, r.Body, maxRaftBody)).Decode(&batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.metrics.messagesReceived.Add(int64(len(batch)))
	for _, m := range batch {
		select {
		case s.recvc <- m:
		case <-s.done: // stopped, or halted by a storage failure
			return
		case <-r.Context().Done():
			return
		}
	}
}

var ops = map[string]kv.Op{"get": kv.OpGet, "put": kv.OpPut, "delete": kv.OpDelete, "cas": kv.OpCAS}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxKVBody)).Decode(&req); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	op, ok := ops[req.Op]
	if !ok {
		http.Error(w, "op must be get, put, delete or cas", http.StatusBadRequest)
		return
	}
	if req.Client == 0 {
		http.Error(w, "client must be nonzero: it identifies the session that makes retries safe", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	resp, err := s.Do(ctx, kv.Command{
		Client: req.Client, Seq: req.Seq, Op: op, Key: req.Key, Value: req.Value, Expect: req.Expect,
	})
	if err != nil {
		// No answer in time. The command may still commit; the client
		// must retry with the same sequence number.
		s.metrics.requestsTimeout.Add(1)
		resp = replica.Response{Status: replica.Retry, Leader: s.Status().Leader}
	}
	switch resp.Status {
	case replica.OK:
		s.metrics.requestsOK.Add(1)
	case replica.NotLeader:
		s.metrics.requestsNotLeader.Add(1)
	case replica.Retry:
		s.metrics.requestsRetry.Add(1)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Status: resp.Status.String(),
		Leader: resp.Leader,
		OK:     resp.Result.OK,
		Found:  resp.Result.Found,
		Value:  resp.Result.Value,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.Status())
}

// handleMetrics writes the Prometheus text exposition format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	st := s.Status()
	m := &s.metrics
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	gauge := func(name, help string, v any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, v)
	}
	counter := func(name, help string, v any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %v\n", name, help, name, name, v)
	}
	leader := 0
	if st.Role == raft.Leader.String() {
		leader = 1
	}
	gauge("raftkv_term", "Current Raft term.", st.Term)
	gauge("raftkv_is_leader", "1 if this node believes it is leader.", leader)
	gauge("raftkv_commit_index", "Highest log index known to be committed.", st.Commit)
	gauge("raftkv_applied_index", "Highest log index applied to the state machine.", st.Applied)
	gauge("raftkv_log_entries", "Entries held in the log after the last snapshot.", st.LastIndex-st.FirstIndex)
	gauge("raftkv_wal_bytes", "Size of the write-ahead log file.", st.WALBytes)
	gauge("raftkv_keys", "Keys in the store.", st.Keys)

	fmt.Fprint(w, "# HELP raftkv_requests_total Client requests by outcome.\n# TYPE raftkv_requests_total counter\n")
	for _, kvp := range []struct {
		label string
		v     int64
	}{
		{"ok", m.requestsOK.Load()}, {"not_leader", m.requestsNotLeader.Load()},
		{"retry", m.requestsRetry.Load()}, {"timeout", m.requestsTimeout.Load()},
	} {
		fmt.Fprintf(w, "raftkv_requests_total{outcome=%q} %d\n", kvp.label, kvp.v)
	}
	counter("raftkv_wal_saves_total", "Durable writes (one fsync each unless disabled).", m.saves.Load())
	counter("raftkv_wal_save_seconds_total", "Time spent in durable writes.",
		float64(m.saveNanos.Load())/1e9)
	counter("raftkv_wal_entries_total", "Log entries written.", m.entriesSaved.Load())
	counter("raftkv_messages_sent_total", "Raft messages delivered to peers.", m.messagesSent.Load())
	counter("raftkv_messages_received_total", "Raft messages received from peers.", m.messagesReceived.Load())
	counter("raftkv_send_errors_total", "Failed message batches.", m.sendErrors.Load())
	counter("raftkv_snapshots_taken_total", "Log compactions.", m.snapshotsTaken.Load())
	counter("raftkv_snapshots_installed_total", "Snapshots received from a leader.", m.snapshotsInstalled.Load())
}

package sim

import (
	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/raft"
	"github.com/DranzerD/RaftKV/replica"
)

// Envelope kinds on the simulated network.
const (
	kindRaft = iota
	kindRequest
	kindResponse
)

// Operation is one client call as the client saw it. Call and Return are
// positions on a global event clock, so no two events share a timestamp.
type Operation struct {
	Client uint64
	Cmd    kv.Command
	Result kv.Result
	Call   uint64
	Return uint64
	Done   bool // false if the client never got an answer
}

// simClient behaves like a real client library: one command at a time,
// resent with the same sequence number until some node answers OK.
type simClient struct {
	id       uint64
	seq      uint64
	busy     bool
	cmd      kv.Command
	op       int // index into Sim.history
	target   int // index into Sim.ids
	pinned   bool
	nextSend uint64
}

// AddClients creates n clients with IDs 1..n.
func (s *Sim) AddClients(n int) {
	for i := 0; i < n; i++ {
		s.clients = append(s.clients, &simClient{id: uint64(len(s.clients) + 1)})
	}
}

// ClientIdle reports whether client i has no command outstanding.
func (s *Sim) ClientIdle(i int) bool { return !s.clients[i].busy }

// ClientsIdle reports whether every client is idle.
func (s *Sim) ClientsIdle() bool {
	for _, c := range s.clients {
		if c.busy {
			return false
		}
	}
	return true
}

// ClientDo starts a command on client i. It returns false if the client is
// still waiting for its previous command.
func (s *Sim) ClientDo(i int, op kv.Op, key, value, expect string) bool {
	c := s.clients[i]
	if c.busy {
		return false
	}
	c.seq++
	c.busy = true
	c.cmd = kv.Command{Client: c.id, Seq: c.seq, Op: op, Key: key, Value: value, Expect: expect}
	c.op = len(s.history)
	c.nextSend = s.now
	s.evt++
	s.history = append(s.history, Operation{Client: c.id, Cmd: c.cmd, Call: s.evt})
	return true
}

// History returns every client operation so far.
func (s *Sim) History() []Operation { return s.history }

// stepClients (re)sends every outstanding command whose timer has run out.
func (s *Sim) stepClients() {
	for _, c := range s.clients {
		if !c.busy || s.now < c.nextSend {
			continue
		}
		s.stats.Requests++
		s.post(envelope{kind: kindRequest, node: s.ids[c.target], req: c.cmd})
		// If no answer comes, try the next node: this one may be dead,
		// partitioned or a leader that can no longer commit.
		if !c.pinned {
			c.target = (c.target + 1) % len(s.ids)
		}
		c.nextSend = s.now + 20 + uint64(s.rng.Intn(20))
	}
}

// post puts a client message on the network, subject to the same loss,
// duplication and delay as Raft traffic.
func (s *Sim) post(e envelope) {
	if s.rng.Float64() < s.opt.DropProb {
		return
	}
	s.enqueueEnvelope(e)
	if s.rng.Float64() < s.opt.DupProb {
		s.enqueueEnvelope(e)
	}
}

func (s *Sim) deliverRequest(e envelope) {
	n := s.nodes[e.node]
	if n.raft == nil {
		return
	}
	resps := n.rep.Request(e.req)
	s.drain(n)
	if n.raft == nil {
		return // died handling it; nothing was sent
	}
	for _, r := range resps {
		s.respond(n, r)
	}
}

// respond checks a node's answer against the reference model and sends it.
func (s *Sim) respond(n *node, r replica.Response) {
	if r.Status == replica.OK && !r.Read {
		want, ok := s.check.results[[2]uint64{r.Client, r.Seq}]
		switch {
		case !ok:
			s.fail("exactly-once", "node %d answered OK to client %d seq %d, which is not committed",
				n.id, r.Client, r.Seq)
		case want != r.Result:
			s.fail("exactly-once", "node %d answered client %d seq %d with %+v, but its one true result is %+v",
				n.id, r.Client, r.Seq, r.Result, want)
		}
	}
	if r.Client == 0 || int(r.Client) > len(s.clients) {
		return
	}
	s.post(envelope{kind: kindResponse, resp: r})
}

func (s *Sim) deliverResponse(e envelope) {
	r := e.resp
	c := s.clients[r.Client-1]
	if !c.busy || r.Seq != c.seq {
		return // an answer to something the client has finished with
	}
	switch r.Status {
	case replica.OK:
		s.evt++
		op := &s.history[c.op]
		op.Result, op.Return, op.Done = r.Result, s.evt, true
		c.busy = false
		s.stats.Completed++
	case replica.NotLeader:
		if r.Leader != raft.None && !c.pinned {
			c.target = int(r.Leader - 1)
		}
		c.nextSend = s.now + 1 + uint64(s.rng.Intn(3))
	case replica.Retry:
		c.nextSend = s.now + 1 + uint64(s.rng.Intn(3))
	}
}

// ClientPin makes client i talk to one node only, ignoring redirects. Tests
// use it to aim requests at a node that should refuse them.
func (s *Sim) ClientPin(i int, node uint64) {
	c := s.clients[i]
	c.pinned = true
	c.target = int(node - 1)
}

// ClientUnpin lets client i follow redirects again.
func (s *Sim) ClientUnpin(i int) { s.clients[i].pinned = false }

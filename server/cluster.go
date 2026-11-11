package server

import (
	"fmt"
	"net"
	"path/filepath"
	"time"
)

// Cluster is a set of nodes running in this process on loopback ports. It
// exists for tests and benchmarks.
type Cluster struct {
	Nodes []*Server // nil while a node is stopped
	cfgs  []Config
}

// StartLocal starts n nodes that keep their data under dir. tweak, if not
// nil, can adjust each node's Config before it starts.
func StartLocal(n int, dir string, tweak func(*Config)) (*Cluster, error) {
	peers := make(map[uint64]string, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		listeners[i] = ln
		peers[uint64(i+1)] = ln.Addr().String()
	}
	c := &Cluster{Nodes: make([]*Server, n), cfgs: make([]Config, n)}
	for i := 0; i < n; i++ {
		cfg := Config{
			ID:       uint64(i + 1),
			Peers:    peers,
			DataDir:  filepath.Join(dir, fmt.Sprintf("node%d", i+1)),
			Listener: listeners[i],
			Logf:     func(string, ...any) {},
		}
		if tweak != nil {
			tweak(&cfg)
		}
		srv, err := Start(cfg)
		if err != nil {
			c.StopAll()
			return nil, err
		}
		cfg.Listener = nil // a restart must open the port again
		c.cfgs[i] = cfg
		c.Nodes[i] = srv
	}
	return c, nil
}

// Addrs returns every node's address in node ID order.
func (c *Cluster) Addrs() []string {
	out := make([]string, len(c.cfgs))
	for i, cfg := range c.cfgs {
		out[i] = cfg.Peers[cfg.ID]
	}
	return out
}

// Stop halts node i (zero-based), as a crash would.
func (c *Cluster) Stop(i int) {
	if c.Nodes[i] != nil {
		c.Nodes[i].Stop()
		c.Nodes[i] = nil
	}
}

// Restart brings node i back from its data directory.
func (c *Cluster) Restart(i int) error {
	if c.Nodes[i] != nil {
		return nil
	}
	var err error
	// The old listener may take a moment to release the port.
	for attempt := 0; attempt < 50; attempt++ {
		var srv *Server
		if srv, err = Start(c.cfgs[i]); err == nil {
			c.Nodes[i] = srv
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return err
}

// StopAll halts every node.
func (c *Cluster) StopAll() {
	for i := range c.Nodes {
		c.Stop(i)
	}
}

// Leader returns the index of the running node that leads with the highest
// term, or -1.
func (c *Cluster) Leader() int {
	best, bestTerm := -1, uint64(0)
	for i, n := range c.Nodes {
		if n == nil {
			continue
		}
		if st := n.Status(); st.Role == "Leader" && st.Term >= bestTerm {
			best, bestTerm = i, st.Term
		}
	}
	return best
}

// WaitLeader blocks until some node leads, and returns its index, or -1
// after the timeout.
func (c *Cluster) WaitLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l >= 0 {
			return l
		}
		time.Sleep(5 * time.Millisecond)
	}
	return -1
}

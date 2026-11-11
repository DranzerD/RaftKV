package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/server"
)

func startCluster(t *testing.T) *server.Cluster {
	t.Helper()
	c, err := server.StartLocal(3, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.StopAll)
	if c.WaitLeader(5*time.Second) < 0 {
		t.Fatal("no leader within 5s")
	}
	return c
}

// A client that starts at a follower is redirected to the leader and stays
// there for later commands.
func TestFollowsLeaderHint(t *testing.T) {
	c := startCluster(t)
	leader := c.Leader()
	cl := New(c.Addrs())
	defer cl.Close()
	cl.cur = (leader + 1) % 3

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if cl.cur != leader && c.Leader() == leader {
		t.Fatalf("client settled on node %d, leader is node %d", cl.cur+1, leader+1)
	}
	if v, ok, err := cl.Get(ctx, "k"); err != nil || !ok || v != "v" {
		t.Fatalf("get: %q found=%v err=%v", v, ok, err)
	}
}

// With no node reachable the client gives up when its context ends and
// says why it could not get through.
func TestGivesUpWhenClusterIsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens here now

	cl := New([]string{addr})
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = cl.Do(ctx, kv.OpPut, "k", "v", "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want a deadline error", err)
	}
}

func TestNoServers(t *testing.T) {
	cl := New(nil)
	if _, err := cl.Do(context.Background(), kv.OpGet, "k", "", ""); !errors.Is(err, errNoServers) {
		t.Fatalf("got %v, want errNoServers", err)
	}
}

// Every client gets its own nonzero session ID.
func TestSessionIDs(t *testing.T) {
	seen := map[uint64]bool{}
	for i := 0; i < 100; i++ {
		id := New([]string{"x"}).id
		if id == 0 || seen[id] {
			t.Fatalf("bad or repeated session id %d", id)
		}
		seen[id] = true
	}
}

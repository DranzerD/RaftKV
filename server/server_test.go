package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DranzerD/RaftKV/client"
	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/server"
	"github.com/DranzerD/RaftKV/sim"
)

func startCluster(t *testing.T, n int, tweak func(*server.Config)) *server.Cluster {
	t.Helper()
	c, err := server.StartLocal(n, t.TempDir(), tweak)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.StopAll)
	if c.WaitLeader(5*time.Second) < 0 {
		t.Fatal("no leader within 5s")
	}
	return c
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestBasicOperations(t *testing.T) {
	c := startCluster(t, 3, nil)
	cl := client.New(c.Addrs())
	defer cl.Close()
	ctx := ctxFor(t, 10*time.Second)

	if _, found, err := cl.Get(ctx, "k"); err != nil || found {
		t.Fatalf("get of a missing key: found=%v err=%v", found, err)
	}
	if err := cl.Put(ctx, "k", "1"); err != nil {
		t.Fatal(err)
	}
	if ok, err := cl.CAS(ctx, "k", "0", "2"); err != nil || ok {
		t.Fatalf("cas with the wrong expectation: ok=%v err=%v", ok, err)
	}
	if ok, err := cl.CAS(ctx, "k", "1", "2"); err != nil || !ok {
		t.Fatalf("cas: ok=%v err=%v", ok, err)
	}
	if v, found, err := cl.Get(ctx, "k"); err != nil || !found || v != "2" {
		t.Fatalf("get: %q found=%v err=%v", v, found, err)
	}
	if existed, err := cl.Delete(ctx, "k"); err != nil || !existed {
		t.Fatalf("delete: existed=%v err=%v", existed, err)
	}

	// Every node exposes status and metrics.
	for _, addr := range c.Addrs() {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		for _, want := range []string{"raftkv_term ", "raftkv_commit_index ", `raftkv_requests_total{outcome="ok"}`} {
			if !strings.Contains(string(body), want) {
				t.Fatalf("%s/metrics lacks %q", addr, want)
			}
		}
	}
}

// The whole cluster is stopped and started again: nothing acknowledged may
// be lost, with and without a snapshot in the way.
func TestRestartKeepsData(t *testing.T) {
	for _, every := range []int{-1, 20} {
		c := startCluster(t, 3, func(cfg *server.Config) { cfg.SnapshotEvery = every })
		cl := client.New(c.Addrs())
		ctx := ctxFor(t, 30*time.Second)
		for i := 0; i < 100; i++ {
			if err := cl.Put(ctx, fmt.Sprintf("k%d", i%10), fmt.Sprint(i)); err != nil {
				t.Fatal(err)
			}
		}
		c.StopAll()
		for i := range c.Nodes {
			if err := c.Restart(i); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			v, found, err := cl.Get(ctx, fmt.Sprintf("k%d", i))
			if err != nil || !found || v != fmt.Sprint(90+i) {
				t.Fatalf("snapshots every %d: k%d = %q found=%v err=%v, want %d", every, i, v, found, err, 90+i)
			}
		}
		if every > 0 {
			if st := c.Nodes[c.WaitLeader(5*time.Second)].Status(); st.FirstIndex == 0 {
				t.Fatalf("no snapshot was taken: %+v", st)
			}
		}
		cl.Close()
		c.StopAll()
	}
}

// Concurrent clients keep working while leaders are killed and restarted,
// and what they observed must be linearizable.
func TestLinearizableThroughLeaderFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("takes a few seconds")
	}
	c := startCluster(t, 3, func(cfg *server.Config) { cfg.SnapshotEvery = 50 })
	const clients, opsEach = 4, 120

	var clock atomic.Uint64
	var mu sync.Mutex
	var history []sim.Operation
	var wg sync.WaitGroup
	ctx := ctxFor(t, 60*time.Second)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := client.New(c.Addrs())
			defer cl.Close()
			for n := 0; n < opsEach; n++ {
				key := fmt.Sprintf("k%d", n%2)
				cmd := kv.Command{Key: key}
				switch n % 4 {
				case 0, 1:
					cmd.Op, cmd.Value = kv.OpPut, fmt.Sprintf("c%d-%d", i, n)
				case 2:
					cmd.Op = kv.OpGet
				case 3:
					cmd.Op, cmd.Value, cmd.Expect = kv.OpCAS, fmt.Sprintf("c%d-%d", i, n), fmt.Sprintf("c%d-%d", i, n-2)
				}
				op := sim.Operation{Client: uint64(i + 1), Cmd: cmd, Call: clock.Add(1)}
				res, err := cl.Do(ctx, cmd.Op, cmd.Key, cmd.Value, cmd.Expect)
				if err == nil {
					op.Result, op.Return, op.Done = res, clock.Add(1), true
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
				if err != nil {
					t.Errorf("client %d op %d: %v", i, n, err)
					return
				}
			}
		}(i)
	}

	// Kill the leader three times while the clients run.
	for round := 0; round < 3; round++ {
		time.Sleep(300 * time.Millisecond)
		l := c.WaitLeader(5 * time.Second)
		if l < 0 {
			t.Fatal("no leader to kill")
		}
		c.Stop(l)
		time.Sleep(600 * time.Millisecond)
		if err := c.Restart(l); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if len(history) != clients*opsEach {
		t.Fatalf("%d operations recorded, want %d", len(history), clients*opsEach)
	}
	if !sim.CheckLinearizable(history) {
		t.Fatal("history is not linearizable")
	}
	t.Logf("%d operations across 3 leader kills are linearizable", len(history))
}

// Malformed or oversized requests are refused before they reach the node.
func TestRejectsBadRequests(t *testing.T) {
	c := startCluster(t, 1, nil)
	base := "http://" + c.Addrs()[0]
	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"raft over GET", http.MethodGet, "/raft", "", http.StatusMethodNotAllowed},
		{"raft garbage", http.MethodPost, "/raft", "not gob", http.StatusBadRequest},
		{"kv over GET", http.MethodGet, "/kv", "", http.StatusMethodNotAllowed},
		{"kv bad JSON", http.MethodPost, "/kv", "{", http.StatusBadRequest},
		{"kv unknown op", http.MethodPost, "/kv", `{"client":1,"seq":1,"op":"drop","key":"k"}`, http.StatusBadRequest},
		{"kv no session", http.MethodPost, "/kv", `{"client":0,"seq":1,"op":"put","key":"k"}`, http.StatusBadRequest},
		{"kv too large", http.MethodPost, "/kv",
			`{"client":1,"seq":1,"op":"put","key":"k","value":"` + strings.Repeat("x", 2<<20) + `"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, base+tc.path, strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s: HTTP %d, want %d", tc.name, resp.StatusCode, tc.want)
		}
	}
}

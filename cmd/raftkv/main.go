// Command raftkv runs a RaftKV node and talks to a cluster.
//
//	raftkv serve  -id 1 -peers 1=host1:7001,2=host2:7001,3=host3:7001 -data ./data
//	raftkv put    -servers host1:7001,host2:7001,host3:7001 color blue
//	raftkv get    -servers ... color
//	raftkv cas    -servers ... color blue green
//	raftkv delete -servers ... color
//	raftkv status -servers ...
//	raftkv bench  -local            # benchmark suite on in-process clusters
//	raftkv bench  -servers ...      # load an existing cluster
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/DranzerD/RaftKV/client"
	"github.com/DranzerD/RaftKV/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "serve":
		serve(args)
	case "get", "put", "delete", "cas":
		kvCommand(cmd, args)
	case "status":
		status(args)
	case "bench":
		bench(args)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raftkv serve|get|put|delete|cas|status|bench [flags] [args]")
	fmt.Fprintln(os.Stderr, "run a subcommand with -h for its flags")
	os.Exit(2)
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "raftkv: "+format+"\n", args...)
	os.Exit(1)
}

func parsePeers(s string) map[uint64]string {
	peers := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.ParseUint(id, 10, 64)
		if !ok || err != nil || n == 0 || addr == "" {
			die("bad -peers entry %q, want id=host:port", part)
		}
		peers[n] = addr
	}
	return peers
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Uint64("id", 0, "this node's ID")
	peers := fs.String("peers", "", "every node as id=host:port, comma separated")
	listen := fs.String("listen", "", "address to bind (default: this node's entry in -peers)")
	data := fs.String("data", "data", "directory for the WAL and snapshots")
	snapshotEvery := fs.Int("snapshot-every", 10000, "compact the log after this many applied entries (negative: never)")
	maxBatch := fs.Int("max-batch", 256, "client requests folded into one disk sync (1: no batching)")
	noSync := fs.Bool("unsafe-no-sync", false, "skip fsync; acknowledged writes can be lost in a power cut")
	fs.Parse(args)
	if *id == 0 || *peers == "" {
		die("serve needs -id and -peers")
	}
	cfg := server.Config{
		ID: *id, Peers: parsePeers(*peers), DataDir: *data,
		SnapshotEvery: *snapshotEvery, MaxBatch: *maxBatch, NoSync: *noSync,
	}
	if *listen != "" {
		// Bind one address while advertising another, as in a container.
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			die("%v", err)
		}
		cfg.Listener = ln
	}
	srv, err := server.Start(cfg)
	if err != nil {
		die("%v", err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	srv.Stop()
}

// splitServers parses a -servers value, dropping empty entries.
func splitServers(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	if len(out) == 0 {
		die("-servers must list at least one host:port")
	}
	return out
}

func serverList(fs *flag.FlagSet) *string {
	return fs.String("servers", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003",
		"every node's host:port, in node ID order")
}

func kvCommand(cmd string, args []string) {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	servers := serverList(fs)
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this long")
	fs.Parse(args)
	rest := fs.Args()
	want := map[string]int{"get": 1, "put": 2, "delete": 1, "cas": 3}[cmd]
	if len(rest) != want {
		die("usage: raftkv %s [flags] %s", cmd,
			map[string]string{"get": "KEY", "put": "KEY VALUE", "delete": "KEY", "cas": "KEY EXPECT VALUE"}[cmd])
	}
	c := client.New(splitServers(*servers))
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch cmd {
	case "get":
		v, found, err := c.Get(ctx, rest[0])
		if err != nil {
			die("%v", err)
		}
		if !found {
			fmt.Fprintln(os.Stderr, "(not found)")
			os.Exit(1)
		}
		fmt.Println(v)
	case "put":
		if err := c.Put(ctx, rest[0], rest[1]); err != nil {
			die("%v", err)
		}
		fmt.Println("OK")
	case "delete":
		existed, err := c.Delete(ctx, rest[0])
		if err != nil {
			die("%v", err)
		}
		if existed {
			fmt.Println("deleted")
		} else {
			fmt.Println("(not found)")
		}
	case "cas":
		swapped, err := c.CAS(ctx, rest[0], rest[1], rest[2])
		if err != nil {
			die("%v", err)
		}
		if !swapped {
			fmt.Fprintln(os.Stderr, "not swapped: the key is missing or holds a different value")
			os.Exit(1)
		}
		fmt.Println("swapped")
	}
}

func status(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	servers := serverList(fs)
	fs.Parse(args)
	hc := &http.Client{Timeout: 2 * time.Second}
	fmt.Printf("%-22s %-3s %-9s %6s %6s %8s %8s %8s %6s\n",
		"ADDRESS", "ID", "ROLE", "TERM", "LEADER", "COMMIT", "APPLIED", "LOG", "KEYS")
	for _, addr := range splitServers(*servers) {
		resp, err := hc.Get("http://" + addr + "/status")
		if err != nil {
			fmt.Printf("%-22s unreachable\n", addr)
			continue
		}
		var st server.Status
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("%-22s bad response: %v\n", addr, err)
			continue
		}
		fmt.Printf("%-22s %-3d %-9s %6d %6d %8d %8d %8d %6d\n",
			addr, st.ID, st.Role, st.Term, st.Leader, st.Commit, st.Applied, st.LastIndex-st.FirstIndex, st.Keys)
	}
}

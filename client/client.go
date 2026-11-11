// Package client talks to a RaftKV cluster over HTTP. It follows leader
// redirects and retries with the same sequence number until the cluster
// answers, which the server-side session table makes safe.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/DranzerD/RaftKV/kv"
	"github.com/DranzerD/RaftKV/server"
)

// Client is one session. It is not safe for concurrent use: the session
// protocol allows one command in flight per client, so run one Client per
// goroutine.
type Client struct {
	servers []string // in node ID order: servers[i] is node i+1
	id      uint64
	seq     uint64
	cur     int
	http    *http.Client
}

// New creates a session. servers lists every node's host:port in node ID
// order, so that a "leader is node N" hint can be followed.
func New(servers []string) *Client {
	var b [8]byte
	rand.Read(b[:])
	return &Client{
		servers: servers,
		id:      binary.LittleEndian.Uint64(b[:])>>1 | 1, // nonzero
		http: &http.Client{
			Timeout:   4 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second},
		},
	}
}

// Get returns the value of key and whether it exists.
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	r, err := c.Do(ctx, kv.OpGet, key, "", "")
	return r.Value, r.Found, err
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.Do(ctx, kv.OpPut, key, value, "")
	return err
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	r, err := c.Do(ctx, kv.OpDelete, key, "", "")
	return r.OK, err
}

// CAS sets key to value if it currently holds expect, and reports whether
// it did.
func (c *Client) CAS(ctx context.Context, key, expect, value string) (bool, error) {
	r, err := c.Do(ctx, kv.OpCAS, key, value, expect)
	return r.OK, err
}

// Do runs one command to completion, retrying until ctx ends.
func (c *Client) Do(ctx context.Context, op kv.Op, key, value, expect string) (kv.Result, error) {
	if len(c.servers) == 0 {
		return kv.Result{}, errNoServers
	}
	c.seq++
	body, _ := json.Marshal(server.Request{
		Client: c.id, Seq: c.seq, Op: op.String(), Key: key, Value: value, Expect: expect,
	})
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return kv.Result{}, fmt.Errorf("%w (last error: %v)", err, lastErr)
			}
			return kv.Result{}, err
		}
		resp, err := c.post(ctx, c.servers[c.cur], body)
		switch {
		case err != nil:
			lastErr = err
			c.cur = (c.cur + 1) % len(c.servers)
		case resp.Status == "ok":
			return kv.Result{OK: resp.OK, Found: resp.Found, Value: resp.Value}, nil
		case resp.Status == "not-leader" && resp.Leader >= 1 && int(resp.Leader) <= len(c.servers):
			lastErr = errors.New("not leader")
			c.cur = int(resp.Leader) - 1
		default:
			lastErr = errors.New(resp.Status)
			if resp.Status == "not-leader" {
				c.cur = (c.cur + 1) % len(c.servers)
			}
		}
		// Back off briefly once every node has been tried; an election
		// may be under way.
		if attempt >= len(c.servers) {
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
			}
		}
	}
}

var errNoServers = errors.New("client: no servers configured")

func (c *Client) post(ctx context.Context, addr string, body []byte) (server.Response, error) {
	var out server.Response
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/kv", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return out, fmt.Errorf("%s: HTTP %d: %s", addr, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// Close releases idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

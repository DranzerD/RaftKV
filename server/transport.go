package server

import (
	"bytes"
	"encoding/gob"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/DranzerD/RaftKV/raft"
)

// transport sends Raft messages to peers over HTTP, as gob-encoded batches
// posted to /raft. Delivery is best effort: Raft already copes with lost
// and repeated messages, so a full queue or a failed request simply drops.
type transport struct {
	self  uint64
	peers map[uint64]*peer
	wg    sync.WaitGroup
	stopc chan struct{}
}

type peer struct {
	url    string
	ch     chan raft.Message
	client *http.Client
}

func newTransport(self uint64, addrs map[uint64]string, m *metrics) *transport {
	t := &transport{self: self, peers: make(map[uint64]*peer), stopc: make(chan struct{})}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		p := &peer{
			url: "http://" + addr + "/raft",
			ch:  make(chan raft.Message, 4096),
			client: &http.Client{
				Timeout:   2 * time.Second,
				Transport: &http.Transport{MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second},
			},
		}
		t.peers[id] = p
		t.wg.Add(1)
		go t.pump(p, m)
	}
	return t
}

func (t *transport) send(m raft.Message) {
	p, ok := t.peers[m.To]
	if !ok {
		return
	}
	select {
	case p.ch <- m:
	default: // the peer is slow or down; Raft will resend
	}
}

func (t *transport) pump(p *peer, m *metrics) {
	defer t.wg.Done()
	for {
		var batch []raft.Message
		select {
		case <-t.stopc:
			return
		case msg := <-p.ch:
			batch = append(batch, msg)
		}
		for more := true; more && len(batch) < 256; {
			select {
			case msg := <-p.ch:
				batch = append(batch, msg)
			default:
				more = false
			}
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
			continue
		}
		resp, err := p.client.Post(p.url, "application/octet-stream", &buf)
		if err != nil {
			m.sendErrors.Add(1)
			continue
		}
		// Drain the body so the connection can be reused.
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			m.sendErrors.Add(1)
			continue
		}
		m.messagesSent.Add(int64(len(batch)))
	}
}

func (t *transport) stop() {
	close(t.stopc)
	t.wg.Wait()
	for _, p := range t.peers {
		p.client.CloseIdleConnections()
	}
}

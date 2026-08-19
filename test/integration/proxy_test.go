//go:build integration

package integration

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// proxy is a TCP relay that can be told to fail, standing in for a broker or a
// database that goes away and comes back.
//
// Breaking a proxy rather than stopping a container keeps the resilience tests
// deterministic and independent of the container runtime: an outage begins the
// instant Break returns, existing connections are severed the way a crash severs
// them, and new ones are refused until Heal.
type proxy struct {
	target   string
	listener net.Listener

	mu     sync.Mutex
	broken bool
	conns  map[net.Conn]struct{}

	closed chan struct{}
	wg     sync.WaitGroup
}

// newProxy starts relaying to target and stops when the test ends.
func newProxy(tb testing.TB, target string) *proxy {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}

	p := &proxy{
		target:   target,
		listener: ln,
		conns:    map[net.Conn]struct{}{},
		closed:   make(chan struct{}),
	}

	p.wg.Add(1)
	go p.serve()

	tb.Cleanup(p.Close)

	return p
}

// Addr is the host:port to point a client at.
func (p *proxy) Addr() string { return p.listener.Addr().String() }

func (p *proxy) serve() {
	defer p.wg.Done()

	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		broken := p.broken
		p.mu.Unlock()

		if broken {
			// Refused the way a dead port refuses: the client sees the
			// connection close immediately.
			_ = client.Close()

			continue
		}

		p.wg.Add(1)

		go func() {
			defer p.wg.Done()

			p.relay(client)
		}()
	}
}

func (p *proxy) relay(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()

		return
	}

	p.track(client)
	p.track(upstream)

	defer func() {
		p.forget(client)
		p.forget(upstream)
		_ = client.Close()
		_ = upstream.Close()
	}()

	done := make(chan struct{}, 2)

	copyOne := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}

	go copyOne(upstream, client)
	go copyOne(client, upstream)

	select {
	case <-done:
	case <-p.closed:
	}
}

func (p *proxy) track(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.conns[c] = struct{}{}
}

func (p *proxy) forget(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.conns, c)
}

// Break severs every open connection and refuses new ones, the way a broker or
// a database that has just died behaves.
func (p *proxy) Break() {
	p.mu.Lock()
	p.broken = true

	for c := range p.conns {
		_ = c.Close()
		delete(p.conns, c)
	}
	p.mu.Unlock()
}

// Heal lets connections through again.
func (p *proxy) Heal() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.broken = false
}

// Broken reports the current state, for assertions.
func (p *proxy) Broken() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.broken
}

func (p *proxy) Close() {
	select {
	case <-p.closed:
		return
	default:
	}

	close(p.closed)
	_ = p.listener.Close()

	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()

	p.wg.Wait()
}

// dialThrough reports whether a plain TCP connection currently succeeds, so a
// test can assert the proxy itself behaves before assuming anything about the
// dispatcher.
func dialThrough(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	// A broken proxy accepts and closes at once; a read confirms which happened.
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

	buf := make([]byte, 1)
	if _, err := c.Read(buf); err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil // held open: healthy
		}

		return err // closed on us: broken
	}

	return nil
}

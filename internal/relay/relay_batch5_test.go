package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-gost/core/chain"
	"github.com/go-gost/core/handler"
	"github.com/go-gost/core/metadata"
	coreservice "github.com/go-gost/core/service"
	xhandler "github.com/go-gost/x/handler/forward/local"
	"github.com/go-gost/x/logger"
	xmetadata "github.com/go-gost/x/metadata"
	xstats "github.com/go-gost/x/observer/stats"
	statswrapper "github.com/go-gost/x/observer/stats/wrapper"
	xservice "github.com/go-gost/x/service"

	"github.com/cedar2025/xboard-node/internal/panel"
)

type batch5Panel struct {
	doc   *panel.RelayDocument
	acks  []batch5Ack
	posts []panel.RelayTrafficBatch
}

type batch5Ack struct {
	version int
	applied bool
	err     string
}

func (p *batch5Panel) FetchRelayConfig() (*panel.RelayConfigResponse, error) {
	hash := fmt.Sprintf("hash-%d", p.doc.Version)
	return &panel.RelayConfigResponse{Version: p.doc.Version, Config: p.doc, Hash: &hash}, nil
}

func (p *batch5Panel) PostRelayAck(version int, _ string, applied bool, errText string) error {
	p.acks = append(p.acks, batch5Ack{version, applied, errText})
	return nil
}

func (p *batch5Panel) PostRelayTraffic(batches []panel.RelayTrafficBatch) (*panel.RelayTrafficResult, error) {
	p.posts = append(p.posts, batches...)
	return &panel.RelayTrafficResult{Accepted: len(batches)}, nil
}

func batch5Manager(dir string, client *batch5Panel) *Manager {
	m := NewManager(nil, 99, dir)
	m.client = client
	m.loadState()
	return m
}

func TestBatch5StateCreatesMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing", "instance")
	m := batch5Manager(dir, nil)
	m.state.Banked["test"] = counters{In: 42}
	if err := m.saveStateLocked(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(m.statePath())
	if err != nil {
		t.Fatal(err)
	}
	var state diskState
	if err := json.Unmarshal(raw, &state); err != nil || state.Banked["test"].In != 42 {
		t.Fatalf("saved state %s, decode error %v", raw, err)
	}
}

func TestBatch5TrafficWaitsForDurableQueue(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(parent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(parent, "instance")
	client := &batch5Panel{}
	m := batch5Manager(dir, client)
	m.state.Banked["test"] = counters{In: 42}
	m.flushTraffic()
	if len(client.posts) != 0 {
		t.Fatal("posted a traffic batch before persistence")
	}
	if len(m.state.Pending) != 1 || m.state.NextSeq["test"] != 1 || m.state.Reported["test"].In != 42 {
		t.Fatalf("queue transaction lost after write error: %+v", m.state)
	}
	want := m.state.Pending[0]
	if err := os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	m.flushTraffic()
	if len(client.posts) != 1 {
		t.Fatalf("posts after retry = %d, want 1", len(client.posts))
	}
	if got := client.posts[0]; got != want || len(m.state.Pending) != 0 {
		t.Fatalf("settled retry = %+v, want %+v; pending = %v", got, want, m.state.Pending)
	}
}

func TestBatch5DrainClosesEachGenerationBeforeAck(t *testing.T) {
	client := &batch5Panel{doc: &panel.RelayDocument{Version: 2, MachineStatus: "DRAINING"}}
	m := batch5Manager(t.TempDir(), client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	short := batch5ForwardingFlow(t, m, "short", 1)
	long := batch5ForwardingFlow(t, m, "long", 3)
	batch5Echo(t, short)
	batch5Echo(t, long)
	done := make(chan struct{})
	go func() { defer close(done); m.syncOnce(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	if err := batch5ReadUntilClosed(short, 2400*time.Millisecond); err != nil {
		t.Fatalf("short generation at its deadline: %v", err)
	}
	select {
	case <-done:
		t.Fatal("ACK before long generation drained")
	default:
	}
	batch5Echo(t, long)
	if err := batch5ReadUntilClosed(long, 4*time.Second); err != nil {
		t.Fatalf("long generation at its deadline: %v", err)
	}
	<-done
	if len(client.acks) != 1 || client.acks[0] != (batch5Ack{version: 2, applied: true}) {
		t.Fatalf("drain ACK = %v", client.acks)
	}
	if short, long := m.state.Banked["short"], m.state.Banked["long"]; short.In != 1 || short.Out != 1 || long.In != 2 || long.Out != 2 {
		t.Fatalf("final accounting at ACK: short=%+v long=%+v", short, long)
	}
}

// Run Gost's real service, forwarding handler and pipe on in-memory
// connections so the drain contract can be tested without a socket.
func batch5ForwardingFlow(t *testing.T, m *Manager, code string, timeout int) net.Conn {
	t.Helper()
	rt := &ruleRuntime{spec: panel.RelayRule{RuleCode: code, DrainTimeoutSec: timeout}}
	stats := &xstats.Stats{}
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close() })
	ln := &batch5Listener{ingress: make(chan net.Conn, 1), closed: make(chan struct{})}
	ln.ingress <- statswrapper.WrapConn(server, stats)
	h := xhandler.NewHandler(handler.RouterOption(batch5EchoRouter{}), handler.LoggerOption(logger.Nop()))
	if err := h.Init(xmetadata.NewMetadata(nil)); err != nil {
		t.Fatal(err)
	}
	tracked := &trackedHandler{Handler: h, flows: &rt.flows}
	svc := xservice.NewService(code, ln, tracked, xservice.StatsOption(stats), xservice.LoggerOption(logger.Nop()))
	ref := &svcRef{name: code, svc: svc, finished: make(chan struct{})}
	rt.services = []*svcRef{ref}
	m.running[code] = rt
	go func() {
		defer close(ref.finished)
		m.serveRule(rt, ref)
	}()
	t.Cleanup(func() {
		svc.Close()
		rt.flows.terminate()
		<-ref.finished
	})
	return client
}

type batch5Listener struct {
	ingress chan net.Conn
	closed  chan struct{}
}

func (*batch5Listener) Init(metadata.Metadata) error { return nil }
func (*batch5Listener) Addr() net.Addr               { return &net.TCPAddr{} }
func (l *batch5Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.ingress:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *batch5Listener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

type batch5EchoRouter struct{ chain.Router }

func (batch5EchoRouter) Dial(context.Context, string, string, ...chain.DialOption) (net.Conn, error) {
	upstream, echo := net.Pipe()
	go func() {
		defer echo.Close()
		_, _ = io.Copy(echo, echo)
	}()
	return upstream, nil
}

func batch5Echo(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := io.ReadFull(conn, b[:]); err != nil || b[0] != 'x' {
		t.Fatalf("echo = %q, %v", b, err)
	}
}

func batch5ReadUntilClosed(conn net.Conn, timeout time.Duration) error {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var b [1]byte
	_, err := conn.Read(b[:])
	if errors.Is(err, io.EOF) {
		return nil
	}
	return fmt.Errorf("expected EOF, got %v", err)
}

type batch5FailedService struct{ coreservice.Service }

func (batch5FailedService) Serve() error { return errors.New("test service exit") }

func TestBatch5SuspectAckBelongsToCommittedRuntime(t *testing.T) {
	client := &batch5Panel{}
	m := batch5Manager(t.TempDir(), client)
	spec := panel.RelayRule{RuleCode: "test"}
	ref := &svcRef{name: "staged", svc: batch5FailedService{}}
	staged := &ruleRuntime{spec: spec, services: []*svcRef{ref}}
	active := &ruleRuntime{spec: spec, version: 2}
	m.appliedVersion, m.appliedHash = 2, "hash-2"
	m.running[spec.RuleCode] = active
	m.serveRule(staged, ref)
	if err := staged.serveError(); err == nil {
		t.Fatal("staged failure did not reach the apply error path")
	}
	if len(client.acks) != 0 {
		t.Fatalf("staged failure ACKed a healthy version: %v", client.acks)
	}
	stale := &ruleRuntime{spec: spec, version: 1}
	m.running[spec.RuleCode] = stale
	m.serveRule(stale, &svcRef{svc: batch5FailedService{}})
	if len(client.acks) != 0 {
		t.Fatalf("stale generation ACKed current version: %v", client.acks)
	}
	m.running[spec.RuleCode] = active
	m.serveRule(active, &svcRef{svc: batch5FailedService{}})
	m.serveRule(active, &svcRef{svc: batch5FailedService{}})
	if len(client.acks) != 1 || client.acks[0].version != 2 || client.acks[0].applied {
		t.Fatalf("committed failure ACKs = %v", client.acks)
	}
}

func TestBatch5RelayRejectsResolvedLocalLoops(t *testing.T) {
	for _, tc := range []struct {
		name, host, transport string
		self, wantLoop        bool
	}{
		{"self", "127.0.0.1", "tcp", true, true},
		{"other wildcard listener", "192.0.2.19", "tcp", false, true},
		{"external", "192.0.2.40", "tcp", false, false},
		{"resolved hostname", "localhost", "tcp", false, true},
		{"different transport", "127.0.0.1", "udp", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := panel.RelayRule{RuleCode: "listener", Transport: "tcp", ListenIP: "0.0.0.0", ListenPort: 10001, TargetHost: "192.0.2.50", TargetPort: 443}
			source := panel.RelayRule{RuleCode: "source", Transport: tc.transport, ListenIP: "127.0.0.1", ListenPort: 10002, TargetHost: tc.host, TargetPort: listener.ListenPort}
			desired := map[string]panel.RelayRule{"listener": listener, "source": source}
			if tc.self {
				source.TargetPort = source.ListenPort
				desired = map[string]panel.RelayRule{"source": source}
			}
			ctx := context.Background()
			interfaceAddrs := func() ([]net.Addr, error) {
				return []net.Addr{
					&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
					&net.IPNet{IP: net.ParseIP("192.0.2.19"), Mask: net.CIDRMask(24, 32)},
				}, nil
			}
			targets, err := resolveRelayTargets(ctx, desired, interfaceAddrs)
			if !tc.wantLoop {
				if err != nil || targets["source"] != tc.host {
					t.Fatalf("expected allowed pinned target, got %v, %v", targets, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "loops to local") {
				t.Fatalf("expected loop rejection, got %v, %v", targets, err)
			}
			rules := []panel.RelayRule{source}
			if !tc.self {
				rules = append(rules, listener)
			}
			client := &batch5Panel{doc: &panel.RelayDocument{Version: 1, RelayEnabled: true, Rules: rules}}
			m := batch5Manager(t.TempDir(), client)
			m.interfaceAddrs = interfaceAddrs
			m.syncOnce(ctx)
			if len(client.acks) != 1 || client.acks[0].applied || !strings.Contains(client.acks[0].err, "loops to local") || len(m.running) != 0 {
				t.Fatalf("expected loop rejection before startup, got %v", client.acks)
			}
		})
	}
}

package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/panel"
)

// ─── mock panel ──────────────────────────────────────────────────────

type mockPanel struct {
	mu       sync.Mutex
	version  int
	hash     string
	doc      *panel.RelayDocument
	acks     []map[string]any
	booked   map[string]bool // batch_id -> booked
	totIn    int64
	totOut   int64
	totConns int64
}

func (p *mockPanel) setDoc(version int, doc *panel.RelayDocument) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.version = version
	p.hash = fmt.Sprintf("hash-%d", version)
	doc.Version = version
	p.doc = doc
}

func (p *mockPanel) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/server/machine/relay/config", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		resp := map[string]any{"protocol": "relay.config.v1", "machine_id": 99, "version": p.version}
		if p.doc != nil {
			resp["hash"] = p.hash
			resp["config"] = p.doc
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/v2/server/machine/relay/ack", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		p.mu.Lock()
		p.acks = append(p.acks, body)
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"ok": true}})
	})
	mux.HandleFunc("/api/v2/server/machine/relay/traffic", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Batches []panel.RelayTrafficBatch `json:"batches"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		p.mu.Lock()
		defer p.mu.Unlock()
		accepted, duplicates := 0, 0
		for _, b := range body.Batches {
			id := fmt.Sprintf("99:%s:%s:%d", b.RuleCode, b.BootID, b.Seq)
			if p.booked[id] {
				duplicates++
				continue
			}
			p.booked[id] = true
			accepted++
			p.totIn += b.BytesIn
			p.totOut += b.BytesOut
			p.totConns += b.Connections
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{
			"accepted": accepted, "duplicates": duplicates, "rejected": 0,
		}})
	})
	return mux
}

func (p *mockPanel) lastAck() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.acks) == 0 {
		return nil
	}
	return p.acks[len(p.acks)-1]
}

func (p *mockPanel) totals() (int64, int64, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totIn, p.totOut, p.totConns
}

// ─── echo servers ────────────────────────────────────────────────────

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startEcho runs a TCP echo and a UDP echo on the same port; the TCP
// echo prefixes replies with prefix so tests can tell servers apart.
func startEcho(t *testing.T, port int, prefix string) {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						out := append([]byte(prefix), buf[:n]...)
						_, _ = c.Write(out)
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()
	pc, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			out := append([]byte(prefix), buf[:n]...)
			_, _ = pc.WriteTo(out, addr)
		}
	}()
	t.Cleanup(func() { ln.Close(); pc.Close() })
}

func tcpRoundTrip(t *testing.T, addr, msg string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(buf[:n])
}

// udpWorks reports whether a raw UDP round trip to addr succeeds at
// all in this environment (sandboxed VMs may forbid UDP entirely).
func udpWorks(addr string) bool {
	c, err := net.DialTimeout("udp", addr, time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte("x")); err != nil {
		return false
	}
	buf := make([]byte, 64)
	_, err = c.Read(buf)
	return err == nil
}

func udpRoundTrip(t *testing.T, addr, msg string) string {
	t.Helper()
	c, err := net.DialTimeout("udp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("udp dial %s: %v", addr, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("udp write: %v", err)
	}
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("udp read: %v", err)
	}
	return string(buf[:n])
}

// Regression test for the HK2 pilot finding (2026-11-25): building a
// second rule must not close or disturb the first rule's services.
// (loader.Load's registry group replacement closes every registered
// service; the manager now parses services per rule instead.)
func TestAddRuleDoesNotDisturbExisting(t *testing.T) {
	echoPort := freePort(t)
	startEcho(t, echoPort, "E:")
	portA := freePort(t)
	portB := freePort(t)

	mp := &mockPanel{booked: make(map[string]bool)}
	srv := httptest.NewServer(mp.handler())
	defer srv.Close()

	client := panel.NewClient(config.PanelConfig{URL: srv.URL, Token: "tok", MachineID: 99})
	m := NewManager(client, 99, t.TempDir())
	m.loadState()
	ctx := context.Background()

	mkRule := func(code string, port int) panel.RelayRule {
		return panel.RelayRule{
			RuleCode: code, Name: code, Transport: "tcp",
			ListenIP: "127.0.0.1", ListenPort: port,
			TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: echoPort,
			MaxConnections: 500, UDPMaxSessions: 512,
			UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
		}
	}
	ruleA := mkRule("RL000101", portA)
	ruleB := mkRule("RL000102", portB)

	mp.setDoc(1, &panel.RelayDocument{
		Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
		MachineStatus: "ENABLED", Rules: []panel.RelayRule{ruleA},
	})
	m.syncOnce(ctx)
	addrA := fmt.Sprintf("127.0.0.1:%d", portA)
	if got := tcpRoundTrip(t, addrA, "first"); got != "E:first" {
		t.Fatalf("rule A echo = %q, want E:first", got)
	}

	// v2 adds rule B; rule A's spec is unchanged.
	mp.setDoc(2, &panel.RelayDocument{
		Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
		MachineStatus: "ENABLED", Rules: []panel.RelayRule{ruleA, ruleB},
	})
	m.syncOnce(ctx)
	ack := mp.lastAck()
	if ack == nil || ack["applied"] != true || int(ack["version"].(float64)) != 2 {
		t.Fatalf("v2 ack = %v, want applied v2", ack)
	}
	if got := tcpRoundTrip(t, addrA, "second"); got != "E:second" {
		t.Fatalf("rule A disturbed by adding rule B: echo = %q, want E:second", got)
	}
	addrB := fmt.Sprintf("127.0.0.1:%d", portB)
	if got := tcpRoundTrip(t, addrB, "bee"); got != "E:bee" {
		t.Fatalf("rule B echo = %q, want E:bee", got)
	}
	// No failure ack may follow: give async serve goroutines a moment,
	// then a same-version sync must stay clean (no suspect ack).
	time.Sleep(400 * time.Millisecond)
	m.syncOnce(ctx)
	ack = mp.lastAck()
	if ack != nil && ack["applied"] == false {
		t.Fatalf("unexpected failure ack after adding rule B: %v", ack)
	}
}

// GPT review R04 fault injection (disjoint swap): v2 moves the rule to
// a listen port the test itself occupies, so the new runtime cannot
// bind. Make-before-break must leave the OLD rule serving on its old
// port, the version must be acked as failed, and a later good version
// must still apply cleanly.
func TestApplyFailureKeepsOldRuleServing(t *testing.T) {
	echoPort := freePort(t)
	startEcho(t, echoPort, "E:")
	portA := freePort(t)
	portB := freePort(t)
	portC := freePort(t)

	// Occupy portB for the whole test: v2's bind target.
	blocker, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", portB))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	mp := &mockPanel{booked: make(map[string]bool)}
	srv := httptest.NewServer(mp.handler())
	defer srv.Close()
	client := panel.NewClient(config.PanelConfig{URL: srv.URL, Token: "tok", MachineID: 99})
	m := NewManager(client, 99, t.TempDir())
	m.loadState()
	ctx := context.Background()

	mkRule := func(port int) panel.RelayRule {
		return panel.RelayRule{
			RuleCode: "RL000201", Name: "atomic", Transport: "tcp",
			ListenIP: "127.0.0.1", ListenPort: port,
			TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: echoPort,
			MaxConnections: 500, UDPMaxSessions: 512,
			UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
		}
	}
	doc := func(r panel.RelayRule) *panel.RelayDocument {
		return &panel.RelayDocument{
			Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
			MachineStatus: "ENABLED", Rules: []panel.RelayRule{r},
		}
	}

	mp.setDoc(1, doc(mkRule(portA)))
	m.syncOnce(ctx)
	addrA := fmt.Sprintf("127.0.0.1:%d", portA)
	if got := tcpRoundTrip(t, addrA, "one"); got != "E:one" {
		t.Fatalf("v1 echo = %q, want E:one", got)
	}

	// v2: move to the occupied port — the apply must fail...
	mp.setDoc(2, doc(mkRule(portB)))
	m.syncOnce(ctx)
	ack := mp.lastAck()
	if ack == nil || ack["applied"] != false || int(ack["version"].(float64)) != 2 {
		t.Fatalf("v2 ack = %v, want failed v2", ack)
	}
	// ...and the old rule must still be serving on portA.
	if got := tcpRoundTrip(t, addrA, "two"); got != "E:two" {
		t.Fatalf("old rule after failed v2 = %q, want E:two (old rule must survive a failed apply)", got)
	}

	// v3: move to a free port — applies; the old port then refuses.
	mp.setDoc(3, doc(mkRule(portC)))
	m.syncOnce(ctx)
	ack = mp.lastAck()
	if ack == nil || ack["applied"] != true || int(ack["version"].(float64)) != 3 {
		t.Fatalf("v3 ack = %v, want applied v3", ack)
	}
	addrC := fmt.Sprintf("127.0.0.1:%d", portC)
	if got := tcpRoundTrip(t, addrC, "three"); got != "E:three" {
		t.Fatalf("v3 echo = %q, want E:three", got)
	}
	if c, err := net.DialTimeout("tcp", addrA, time.Second); err == nil {
		c.Close()
		t.Fatal("old listen port still accepts after successful move")
	}
}

// GPT review R04 fault injection (overlapping swap): v2 keeps the same
// listen port but widens the transport tcp -> both while the test holds
// the UDP socket on that port, so the new runtime cannot fully start.
// The old runtime must be restored automatically (old target serving
// again) and the version acked as failed; v3 (tcp again, new target)
// then applies.
func TestApplySameListenFailureRestoresOldRule(t *testing.T) {
	echoPortA := freePort(t)
	startEcho(t, echoPortA, "A:")
	echoPortB := freePort(t)
	startEcho(t, echoPortB, "B:")
	portR := freePort(t)

	// Occupy the UDP side of portR: v2's "both" runtime cannot bind it.
	udpBlock, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", portR))
	if err != nil {
		t.Fatalf("udp blocker: %v", err)
	}
	defer udpBlock.Close()

	mp := &mockPanel{booked: make(map[string]bool)}
	srv := httptest.NewServer(mp.handler())
	defer srv.Close()
	client := panel.NewClient(config.PanelConfig{URL: srv.URL, Token: "tok", MachineID: 99})
	m := NewManager(client, 99, t.TempDir())
	m.loadState()
	ctx := context.Background()

	mkRule := func(transport string, target int) panel.RelayRule {
		return panel.RelayRule{
			RuleCode: "RL000202", Name: "restore", Transport: transport,
			ListenIP: "127.0.0.1", ListenPort: portR,
			TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: target,
			MaxConnections: 500, UDPMaxSessions: 512,
			UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
		}
	}
	doc := func(r panel.RelayRule) *panel.RelayDocument {
		return &panel.RelayDocument{
			Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
			MachineStatus: "ENABLED", Rules: []panel.RelayRule{r},
		}
	}
	addr := fmt.Sprintf("127.0.0.1:%d", portR)

	mp.setDoc(1, doc(mkRule("tcp", echoPortA)))
	m.syncOnce(ctx)
	if got := tcpRoundTrip(t, addr, "one"); got != "A:one" {
		t.Fatalf("v1 echo = %q, want A:one", got)
	}

	// v2: same listen, transport widens to both (UDP blocked), target B.
	mp.setDoc(2, doc(mkRule("both", echoPortB)))
	m.syncOnce(ctx)
	ack := mp.lastAck()
	if ack == nil || ack["applied"] != false || int(ack["version"].(float64)) != 2 {
		t.Fatalf("v2 ack = %v, want failed v2", ack)
	}
	// The old runtime must have been restored: old target, old transport.
	if got := tcpRoundTrip(t, addr, "two"); got != "A:two" {
		t.Fatalf("after failed v2 = %q, want A:two (old runtime restored)", got)
	}

	// v3: tcp again, target B — applies over the restored runtime.
	mp.setDoc(3, doc(mkRule("tcp", echoPortB)))
	m.syncOnce(ctx)
	ack = mp.lastAck()
	if ack == nil || ack["applied"] != true || int(ack["version"].(float64)) != 3 {
		t.Fatalf("v3 ack = %v, want applied v3", ack)
	}
	if got := tcpRoundTrip(t, addr, "three"); got != "B:three" {
		t.Fatalf("v3 echo = %q, want B:three", got)
	}
}

// ─── the end-to-end test ─────────────────────────────────────────────

func TestManagerEndToEnd(t *testing.T) {
	echoPort1 := freePort(t)
	startEcho(t, echoPort1, "A:")
	echoPort2 := freePort(t)
	startEcho(t, echoPort2, "B:")
	relayPort := freePort(t)

	mp := &mockPanel{booked: make(map[string]bool)}
	srv := httptest.NewServer(mp.handler())
	defer srv.Close()

	client := panel.NewClient(config.PanelConfig{URL: srv.URL, Token: "tok", MachineID: 99})
	m := NewManager(client, 99, t.TempDir())
	m.loadState()
	ctx := context.Background()

	rule := panel.RelayRule{
		RuleCode: "RL000001", Name: "pilot", Transport: "both",
		ListenIP: "127.0.0.1", ListenPort: relayPort,
		TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: echoPort1,
		MaxConnections: 500, UDPMaxSessions: 512,
		UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
	}
	mp.setDoc(1, &panel.RelayDocument{
		Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
		MachineStatus: "ENABLED", Rules: []panel.RelayRule{rule},
	})
	m.syncOnce(ctx)
	ack := mp.lastAck()
	if ack == nil || ack["applied"] != true || int(ack["version"].(float64)) != 1 {
		t.Fatalf("v1 ack = %v, want applied v1", ack)
	}

	relayAddr := fmt.Sprintf("127.0.0.1:%d", relayPort)
	if got := tcpRoundTrip(t, relayAddr, "hello-relay"); got != "A:hello-relay" {
		t.Fatalf("tcp echo = %q, want A:hello-relay", got)
	}
	// UDP is blocked outright in some sandboxes (even a raw loopback
	// sendto returns EPERM — seen on the Muse VM). Probe the echo
	// server directly; where raw UDP cannot work, the relay's UDP path
	// is exercised on a real machine instead (SB-RELAY pilot).
	wantIn, wantOut := int64(11), int64(13)
	if udpWorks(fmt.Sprintf("127.0.0.1:%d", echoPort1)) {
		if got := udpRoundTrip(t, relayAddr, "udp-ping"); got != "A:udp-ping" {
			t.Fatalf("udp echo = %q, want A:udp-ping", got)
		}
		wantIn, wantOut = 19, 23
	} else {
		t.Log("raw UDP unavailable in this environment; skipping relay UDP assertions")
	}

	// Traffic: flush and check the mock panel booked the bytes.
	m.flushTraffic()
	in, out, conns := mp.totals()
	// in: 11 (tcp) + 8 (udp); out adds the 2-byte echo prefixes.
	if in != wantIn {
		t.Fatalf("booked bytes_in = %d, want %d", in, wantIn)
	}
	if out != wantOut {
		t.Fatalf("booked bytes_out = %d, want %d", out, wantOut)
	}
	if conns < 1 {
		t.Fatalf("booked connections = %d, want >= 1", conns)
	}
	// Second flush: no new deltas, totals unchanged.
	m.flushTraffic()
	in2, out2, _ := mp.totals()
	if in2 != in || out2 != out {
		t.Fatalf("second flush changed totals: %d/%d -> %d/%d", in, out, in2, out2)
	}

	// Hot update: retarget to echo B at a new version.
	rule2 := rule
	rule2.TargetPort = echoPort2
	mp.setDoc(2, &panel.RelayDocument{
		Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
		MachineStatus: "ENABLED", Rules: []panel.RelayRule{rule2},
	})
	m.syncOnce(ctx)
	ack = mp.lastAck()
	if ack == nil || ack["applied"] != true || int(ack["version"].(float64)) != 2 {
		t.Fatalf("v2 ack = %v, want applied v2", ack)
	}
	if got := tcpRoundTrip(t, relayAddr, "again"); got != "B:again" {
		t.Fatalf("after hot update tcp echo = %q, want B:again", got)
	}

	// Drain: hold one connection open, publish the DRAINING document.
	hold, err := net.DialTimeout("tcp", relayAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial hold conn: %v", err)
	}
	_ = hold.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := hold.Write([]byte("stay")); err != nil {
		t.Fatalf("hold write: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := io.ReadFull(hold, buf[:6]); err != nil || string(buf[:6]) != "B:stay" {
		t.Fatalf("hold echo = %q err=%v", buf[:6], err)
	}
	mp.setDoc(3, &panel.RelayDocument{
		Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: false,
		MachineStatus: "DRAINING", Rules: []panel.RelayRule{},
	})
	applyDone := make(chan struct{})
	go func() { m.syncOnce(ctx); close(applyDone) }()
	// New connections must be refused while the held one drains.
	deadline := time.Now().Add(3 * time.Second)
	refused := false
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", relayAddr, 500*time.Millisecond)
		if err != nil {
			refused = true
			break
		}
		c.Close()
		time.Sleep(100 * time.Millisecond)
	}
	if !refused {
		t.Fatal("new connection not refused during drain")
	}
	// The held connection still serves during the drain window.
	if _, err := hold.Write([]byte("more")); err != nil {
		t.Fatalf("hold write during drain: %v", err)
	}
	if _, err := io.ReadFull(hold, buf[:6]); err != nil || string(buf[:6]) != "B:more" {
		t.Fatalf("hold echo during drain = %q err=%v", buf[:6], err)
	}
	hold.Close()
	select {
	case <-applyDone:
	case <-time.After(8 * time.Second):
		t.Fatal("drain apply did not complete after held conn closed")
	}
	ack = mp.lastAck()
	if ack == nil || ack["applied"] != true || int(ack["version"].(float64)) != 3 {
		t.Fatalf("v3 ack = %v, want applied v3", ack)
	}
	// Final traffic flush must not error and books the drain-period bytes.
	m.flushTraffic()
	in3, _, _ := mp.totals()
	if in3 <= in {
		t.Fatalf("drain-period traffic not booked: in %d -> %d", in, in3)
	}
}

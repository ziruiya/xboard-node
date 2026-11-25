package relay

// Batch-4 regression tests: NODE-001 / NODE-002 (SB-ER-014) /
// NODE-003 / NODE-005 / NODE-007.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-gost/x/registry"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func batch(code string, seq int64, in int64) panel.RelayTrafficBatch {
	return panel.RelayTrafficBatch{
		RuleCode: code, BootID: "boot-1", Seq: seq,
		BytesIn: in, BytesOut: in * 2, Connections: 1,
		PeriodStart: 1000, PeriodEnd: 2000,
	}
}

func pendingSeqs(m *Manager) []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var seqs []int64
	for _, b := range m.state.Pending {
		seqs = append(seqs, b.Seq)
	}
	return seqs
}

// NODE-001: after a restart the Reported watermark must be rebased
// onto Banked (the previous process's live counters are gone), and a
// queue already poisoned with negative batches must be unblocked.
func TestLoadStateRebasesReportedAndDropsPoisonedBatches(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(nil, 99, dir)

	poison := batch("RL000001", 1, -50) // negative: unbookable forever
	valid := batch("RL000001", 2, 70)
	s := diskState{
		Banked:   map[string]counters{"RL000001": {In: 100, Out: 200, Conns: 3}},
		Reported: map[string]counters{"RL000001": {In: 900, Out: 900, Conns: 9}, "RL000002": {In: 5, Out: 5}},
		NextSeq:  map[string]int64{"RL000001": 3},
		Pending:  []panel.RelayTrafficBatch{poison, valid},
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m.statePath(), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	m2 := NewManager(nil, 99, dir)
	m2.loadState()

	if got := m2.state.Reported["RL000001"]; got != (counters{In: 100, Out: 200, Conns: 3}) {
		t.Fatalf("Reported[RL000001] = %+v, want rebased onto Banked {100 200 3}", got)
	}
	if got := m2.state.Reported["RL000002"]; got != (counters{}) {
		t.Fatalf("Reported[RL000002] = %+v, want zeroed (no banked totals survive)", got)
	}
	if len(m2.state.Pending) != 1 || m2.state.Pending[0].Seq != 2 {
		t.Fatalf("Pending = %+v, want only the valid batch seq 2 kept for verbatim resend", m2.state.Pending)
	}
}

// NODE-001 (in-flush guard): if a delta ever computes negative
// mid-process, no batch is emitted and the watermark is rebased.
func TestFlushNeverEmitsNegativeBatch(t *testing.T) {
	m := NewManager(nil, 99, t.TempDir())
	m.state = &diskState{
		Banked:   map[string]counters{"RL000001": {In: 100}},
		Reported: map[string]counters{"RL000001": {In: 150}},
		NextSeq:  map[string]int64{},
	}
	m.flushTraffic() // nil client: must not be reached with any batch

	if len(m.state.Pending) != 0 {
		t.Fatalf("Pending = %+v, want no (negative) batch created", m.state.Pending)
	}
	if got := m.state.Reported["RL000001"].In; got != 100 {
		t.Fatalf("Reported.In = %d, want rebased to live total 100", got)
	}
}

// NODE-002 / SB-ER-014: settlement is per batch. A rejected batch is
// dropped, accepted ones are booked, and batches in chunks that were
// never sent stay queued with their original identity.
func TestSettleChunkDropsOnlyAccountedBatches(t *testing.T) {
	m := NewManager(nil, 99, t.TempDir())
	a1 := batch("RL000001", 1, 10)
	a2 := batch("RL000001", 2, 20) // panel rejects this one (rule deleted)
	b1 := batch("RL000002", 1, 30) // later chunk, never sent yet
	m.state = &diskState{
		Banked:   map[string]counters{},
		Reported: map[string]counters{},
		NextSeq:  map[string]int64{},
		Pending:  []panel.RelayTrafficBatch{a1, a2, b1},
	}

	res := &panel.RelayTrafficResult{
		Accepted: 1, Rejected: 1,
		RejectedBatches: []panel.RelayRejectedBatch{{RuleCode: "RL000001", BootID: "boot-1", Seq: 2, Reason: "unknown_rule"}},
	}
	if !m.settleChunk([]panel.RelayTrafficBatch{a1, a2}, res) {
		t.Fatal("settleChunk returned false for a reconcilable verdict")
	}
	seqs := pendingSeqs(m)
	if len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("pending seqs = %v, want [1] (only the unsent RL000002 batch)", seqs)
	}
	if m.state.Pending[0].RuleCode != "RL000002" {
		t.Fatalf("surviving batch rule = %s, want RL000002", m.state.Pending[0].RuleCode)
	}
}

// NODE-002 guard rails: verdicts that cannot be reconciled with the
// chunk keep the whole chunk queued.
func TestSettleChunkKeepsQueueOnUnreconcilableVerdict(t *testing.T) {
	newManager := func() *Manager {
		m := NewManager(nil, 99, t.TempDir())
		m.state = &diskState{
			Banked:   map[string]counters{},
			Reported: map[string]counters{},
			NextSeq:  map[string]int64{},
			Pending:  []panel.RelayTrafficBatch{batch("RL000001", 1, 10), batch("RL000001", 2, 20)},
		}
		return m
	}
	chunk := []panel.RelayTrafficBatch{batch("RL000001", 1, 10), batch("RL000001", 2, 20)}

	m := newManager()
	if m.settleChunk(chunk, &panel.RelayTrafficResult{Accepted: 5}) {
		t.Fatal("count mismatch must not settle")
	}
	if got := pendingSeqs(m); len(got) != 2 {
		t.Fatalf("pending = %v, want both batches kept", got)
	}

	m = newManager()
	if m.settleChunk(chunk, &panel.RelayTrafficResult{Accepted: 1, Rejected: 1}) {
		t.Fatal("rejections without per-batch names (pre-ER-014 panel) must not settle")
	}
	if got := pendingSeqs(m); len(got) != 2 {
		t.Fatalf("pending = %v, want both batches kept", got)
	}
}

// NODE-003: overlapping flushes (timer vs shutdown) are serialized
// and settle by identity — no batch is lost or double-booked.
func TestConcurrentFlushesLoseNothing(t *testing.T) {
	mp := &mockPanel{booked: make(map[string]bool)}
	srv := httptest.NewServer(mp.handler())
	defer srv.Close()

	client := panel.NewClient(config.PanelConfig{URL: srv.URL, Token: "tok", MachineID: 99})
	m := NewManager(client, 99, t.TempDir())

	var pending []panel.RelayTrafficBatch
	for i := int64(1); i <= 20; i++ {
		pending = append(pending, batch("RL000001", i, i))
	}
	m.state = &diskState{
		Banked:   map[string]counters{},
		Reported: map[string]counters{},
		NextSeq:  map[string]int64{},
		Pending:  pending,
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.flushTraffic()
		}()
	}
	wg.Wait()

	if got := pendingSeqs(m); len(got) != 0 {
		t.Fatalf("pending after concurrent flushes = %v, want empty", got)
	}
	in, _, _ := mp.totals()
	want := int64(0)
	for i := int64(1); i <= 20; i++ {
		want += i
	}
	if in != want {
		t.Fatalf("booked bytes_in = %d, want %d (each batch exactly once)", in, want)
	}
}

// NODE-005: the UDP service's connection limiter must carry
// UDPMaxSessions, not MaxConnections.
func TestBuildRuleUDPUsesUDPMaxSessions(t *testing.T) {
	echoPort := freePort(t)
	startEcho(t, echoPort, "U:")
	m := NewManager(nil, 99, t.TempDir())
	m.loadState()

	spec := panel.RelayRule{
		RuleCode: "RLB405U", Name: "udpquota", Transport: "udp",
		ListenIP: "127.0.0.1", ListenPort: freePort(t),
		TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: echoPort,
		MaxConnections: 100, UDPMaxSessions: 3,
		UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
	}
	m.mu.Lock()
	rt, err := m.buildRuntimeLocked(spec)
	if err != nil {
		m.mu.Unlock()
		t.Fatalf("buildRuntimeLocked: %v", err)
	}
	var climName string
	for _, c := range rt.components {
		if c.kind == "clim" {
			climName = c.name
		}
	}
	var gotLimit int
	if climName != "" {
		if lim := registry.ConnLimiterRegistry().Get(climName); lim != nil {
			gotLimit = lim.Limiter("203.0.113.7").Limit()
		}
	}
	m.closeRuntimeLocked(rt)
	m.mu.Unlock()

	if climName == "" {
		t.Fatal("no connection limiter component registered for UDP service")
	}
	if gotLimit != 3 {
		t.Fatalf("UDP limiter limit = %d, want UDPMaxSessions 3 (not MaxConnections 100)", gotLimit)
	}
}

// NODE-007: a staged (built but not yet committed) runtime binds its
// listener but must not forward traffic until it is served.
func TestStagedRuntimeDoesNotForwardUntilServed(t *testing.T) {
	echoPort := freePort(t)
	startEcho(t, echoPort, "S:")
	relayPort := freePort(t)
	m := NewManager(nil, 99, t.TempDir())
	m.loadState()

	spec := panel.RelayRule{
		RuleCode: "RLB407S", Name: "staged", Transport: "tcp",
		ListenIP: "127.0.0.1", ListenPort: relayPort,
		TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: echoPort,
		MaxConnections: 10, UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
	}

	m.mu.Lock()
	rt, err := m.buildRuntimeLocked(spec)
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("buildRuntimeLocked: %v", err)
	}
	defer func() {
		m.mu.Lock()
		m.closeRuntimeLocked(rt)
		m.mu.Unlock()
	}()

	// The socket is bound (kernel backlog completes the handshake)
	// but nothing may be forwarded before the commit point serves it.
	addr := fmt.Sprintf("127.0.0.1:%d", relayPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial staged listener: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(400 * time.Millisecond))
	if _, err := conn.Write([]byte("early")); err != nil {
		t.Fatalf("write to staged listener: %v", err)
	}
	buf := make([]byte, 32)
	n, _ := conn.Read(buf)
	_ = conn.Close()
	if n > 0 {
		t.Fatalf("staged runtime forwarded %q before commit — staging must not serve", buf[:n])
	}

	m.mu.Lock()
	err = m.serveRuntimeLocked(rt)
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("serveRuntimeLocked: %v", err)
	}
	if got := tcpRoundTrip(t, addr, "after-commit"); got != "S:after-commit" {
		t.Fatalf("after serve, echo = %q, want S:after-commit", got)
	}
}

// NODE-007 (apply level): when a later rule fails staging, the fresh
// rules staged before it must not be left forwarding.
func TestFailedApplyLeavesStagedRuleSilent(t *testing.T) {
	echoPort := freePort(t)
	startEcho(t, echoPort, "F:")
	goodPort := freePort(t)
	badPort := freePort(t)

	// Occupy the second rule's port so its staging fails.
	blocker, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", badPort))
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

	mk := func(code string, port int) panel.RelayRule {
		return panel.RelayRule{
			RuleCode: code, Name: code, Transport: "tcp",
			ListenIP: "127.0.0.1", ListenPort: port,
			TargetType: "MANUAL", TargetHost: "127.0.0.1", TargetPort: echoPort,
			MaxConnections: 10, UDPIdleTimeoutSec: 60, DrainTimeoutSec: 5, BufferBytes: 32768,
		}
	}
	doc := &panel.RelayDocument{
		Protocol: "relay.config.v1", MachineID: 99, RelayEnabled: true,
		MachineStatus: "ENABLED",
		Rules:         []panel.RelayRule{mk("RLB407A", goodPort), mk("RLB407B", badPort)},
	}
	if err := m.apply(context.Background(), doc); err == nil {
		t.Fatal("apply should fail when a rule's port is occupied")
	}

	// The first rule was staged before the failure: its listener must
	// have been torn down, not left serving an uncommitted ruleset.
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", goodPort), time.Second)
	if err == nil {
		_ = conn.SetDeadline(time.Now().Add(300 * time.Millisecond))
		if _, werr := conn.Write([]byte("x")); werr == nil {
			buf := make([]byte, 16)
			if n, _ := conn.Read(buf); n > 0 {
				t.Fatalf("uncommitted rule forwarded %q after failed apply", buf[:n])
			}
		}
		_ = conn.Close()
	}
	m.mu.Lock()
	_, stillRunning := m.running["RLB407A"]
	m.mu.Unlock()
	if stillRunning {
		t.Fatal("failed apply left RLB407A in the running set")
	}
}

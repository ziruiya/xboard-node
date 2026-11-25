// Package relay implements the SB-RELAY-01 agent side: gost v3 is
// embedded as a library (no separate process, no hand-rolled forwarder)
// and driven by the panel's relay.config.v1 documents.
//
// Sync model (design v0.2): the panel publishes whole documents at
// monotonically increasing versions (Outbox). This manager polls the
// document, applies the newest version as a full ruleset, and persists
// the result through the ack endpoint. Rollback is the panel republishing
// old content at a higher version — the agent never rolls back on its
// own. The panel emits no relay wakeup events, so polling (20s) is the
// wakeup; the panel reconciles every 5 minutes on its side.
//
// Traffic: per-rule gost counters are folded into durable totals on this
// side and reported as immutable batches (boot_id + seq per rule). The
// pending queue and the seq counters are persisted BEFORE sending, so a
// crash or an offline window resends the exact same batch ids and the
// panel ledger books them exactly once.
package relay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	corestats "github.com/go-gost/core/observer/stats"
	coreservice "github.com/go-gost/core/service"
	xconfig "github.com/go-gost/x/config"
	"github.com/go-gost/x/config/loader"
	admission_parser "github.com/go-gost/x/config/parsing/admission"
	limiter_parser "github.com/go-gost/x/config/parsing/limiter"
	service_parser "github.com/go-gost/x/config/parsing/service"
	"github.com/go-gost/x/registry"
	xservice "github.com/go-gost/x/service"

	_ "github.com/go-gost/x/connector/http"
	_ "github.com/go-gost/x/dialer/tcp"
	_ "github.com/go-gost/x/dialer/udp"
	_ "github.com/go-gost/x/handler/forward/local"
	_ "github.com/go-gost/x/listener/tcp"
	_ "github.com/go-gost/x/listener/udp"

	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/panel"
)

const (
	pollInterval    = 20 * time.Second
	trafficInterval = 30 * time.Second
	stateFileName   = "relay-state.json"

	fdWarnRatio = 0.70
	fdStopRatio = 0.85
)

// counters is one rule's folded traffic totals.
type counters struct {
	In    int64 `json:"in"`
	Out   int64 `json:"out"`
	Conns int64 `json:"conns"`
}

func (c counters) add(o counters) counters {
	return counters{In: c.In + o.In, Out: c.Out + o.Out, Conns: c.Conns + o.Conns}
}

func (c counters) sub(o counters) counters {
	return counters{In: c.In - o.In, Out: c.Out - o.Out, Conns: c.Conns - o.Conns}
}

func (c counters) zero() bool { return c.In == 0 && c.Out == 0 && c.Conns == 0 }

// diskState is persisted across restarts so traffic batches keep their
// identity (boot_id is per process; seq and folded totals are durable).
type diskState struct {
	NextSeq  map[string]int64            `json:"next_seq"`
	Banked   map[string]counters         `json:"banked"`
	Reported map[string]counters         `json:"reported"`
	Pending  []panel.RelayTrafficBatch   `json:"pending"`
}

func newDiskState() *diskState {
	return &diskState{
		NextSeq:  make(map[string]int64),
		Banked:   make(map[string]counters),
		Reported: make(map[string]counters),
	}
}

// svcRef is one running gost service (one transport of one rule).
type svcRef struct {
	name string
	svc  coreservice.Service
	err  atomic.Value // string, set when Serve exits unexpectedly
	done atomic.Bool
}

// componentRef is a named gost component (limiter / admission) that a
// rule registered in the global registries at build time. They are
// registered individually — never through loader.Load, whose group
// replacement would close every other rule's components and services.
type componentRef struct {
	kind string // tlim | clim | adm
	name string
}

// ruleRuntime is one applied rule: one gost service per transport.
type ruleRuntime struct {
	spec       panel.RelayRule
	services   []*svcRef
	components []componentRef
	closed     atomic.Bool
	restarts   []time.Time // recent restart timestamps (crash backoff)
}

func (rt *ruleRuntime) healthy() bool {
	if len(rt.services) == 0 {
		return false
	}
	for _, ref := range rt.services {
		if ref.done.Load() {
			return false
		}
	}
	return true
}

func (rt *ruleRuntime) totals() counters {
	var out counters
	for _, ref := range rt.services {
		out = out.add(svcCounters(ref.svc))
	}
	return out
}

func (rt *ruleRuntime) currentConns() int64 {
	var n int64
	for _, ref := range rt.services {
		n += svcCurrentConns(ref.svc)
	}
	return n
}

func svcStats(svc coreservice.Service) corestats.Stats {
	st, ok := svc.(interface{ Status() *xservice.Status })
	if !ok || st.Status() == nil {
		return nil
	}
	return st.Status().Stats()
}

func svcCounters(svc coreservice.Service) counters {
	s := svcStats(svc)
	if s == nil {
		return counters{}
	}
	return counters{
		In:    int64(s.Get(corestats.KindInputBytes)),
		Out:   int64(s.Get(corestats.KindOutputBytes)),
		Conns: int64(s.Get(corestats.KindTotalConns)),
	}
}

func svcCurrentConns(svc coreservice.Service) int64 {
	s := svcStats(svc)
	if s == nil {
		return 0
	}
	return int64(s.Get(corestats.KindCurrentConns))
}

// Manager owns the relay data plane for one machine-mode agent.
type Manager struct {
	client    *panel.Client
	machineID int
	stateDir  string
	ownNodes  func() map[int]bool
	bootID    string

	mu             sync.Mutex
	gostInitOnce   sync.Once
	state          *diskState
	running        map[string]*ruleRuntime
	zombies        map[string][]*ruleRuntime // closed services still draining; folded once idle
	appliedVersion int
	appliedHash    string
	ackedVersion   int
	ackedHash      string
	suspectNoted   bool
	lastFlush      time.Time
}

// NewManager creates the relay manager. stateDir is where the durable
// traffic state lives (the agent's kernel config dir, /etc/xboard-node
// in production). ownNodes reports the node ids this machine runs, used
// for the same-machine loopback target rule.
func NewManager(client *panel.Client, machineID int, stateDir string, ownNodes func() map[int]bool) *Manager {
	if stateDir == "" {
		stateDir = os.TempDir()
	}
	var rb [8]byte
	_, _ = rand.Read(rb[:])
	return &Manager{
		client:    client,
		machineID: machineID,
		stateDir:  stateDir,
		ownNodes:  ownNodes,
		bootID:    "b" + hex.EncodeToString(rb[:]),
		running:   make(map[string]*ruleRuntime),
		zombies:   make(map[string][]*ruleRuntime),
		lastFlush: time.Now(),
	}
}

// Run blocks until ctx is cancelled: config sync loop + traffic loop.
func (m *Manager) Run(ctx context.Context) {
	m.loadState()
	m.ensureGostInit()
	nlog.Core().Info("relay manager started", "machine_id", m.machineID, "boot_id", m.bootID, "state_dir", m.stateDir)

	go m.trafficLoop(ctx)
	m.syncLoop(ctx)

	// Shutdown: stop listeners, then one best-effort traffic flush.
	m.mu.Lock()
	for _, rt := range m.running {
		m.closeRuntimeLocked(rt)
	}
	m.running = make(map[string]*ruleRuntime)
	m.mu.Unlock()
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { m.flushTraffic(); close(done) }()
	select {
	case <-done:
	case <-flushCtx.Done():
	}
	nlog.Core().Info("relay manager stopped")
}

func (m *Manager) syncLoop(ctx context.Context) {
	m.syncOnce(ctx)
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.syncOnce(ctx)
		}
	}
}

func (m *Manager) trafficLoop(ctx context.Context) {
	t := time.NewTicker(trafficInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.flushTraffic()
		}
	}
}

// ─── Config sync ─────────────────────────────────────────────────────

func (m *Manager) syncOnce(ctx context.Context) {
	resp, err := m.client.FetchRelayConfig()
	if err != nil {
		nlog.Core().Warn("relay config fetch failed", "error", err)
		return
	}
	if resp.Config == nil || resp.Version == 0 || resp.Hash == nil {
		return // nothing published for this machine yet
	}
	doc := resp.Config
	hash := *resp.Hash

	m.mu.Lock()
	if doc.Version < m.appliedVersion {
		m.mu.Unlock()
		return // stale document; versions are monotonic
	}
	if doc.Version == m.appliedVersion && hash == m.appliedHash {
		// Already applied locally; make sure the ack landed, and that
		// every desired rule is still actually alive — a service that
		// died after its ack must be restarted within one poll cycle.
		healthy := true
		if doc.RelayEnabled && doc.MachineStatus != "DRAINING" {
			for _, r := range doc.Rules {
				rt, ok := m.running[r.RuleCode]
				if !ok || !rt.healthy() {
					healthy = false
					break
				}
			}
		}
		if healthy {
			if m.ackedVersion != doc.Version || m.ackedHash != hash {
				m.ackLocked(doc.Version, hash, true, "")
			}
			m.mu.Unlock()
			return
		}
		nlog.Core().Warn("relay rule unhealthy at applied version, re-applying", "version", doc.Version)
	}
	m.mu.Unlock()

	if err := m.apply(ctx, doc); err != nil {
		nlog.Core().Error("relay apply failed", "version", doc.Version, "error", err)
		m.mu.Lock()
		m.ackLocked(doc.Version, hash, false, err.Error())
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	m.appliedVersion = doc.Version
	m.appliedHash = hash
	m.suspectNoted = false
	m.ackLocked(doc.Version, hash, true, "")
	m.mu.Unlock()
	nlog.Core().Info("relay config applied", "version", doc.Version, "rules", len(doc.Rules), "machine_status", doc.MachineStatus)
}

// ackLocked records and sends an ack. Caller holds m.mu.
func (m *Manager) ackLocked(version int, hash string, applied bool, errText string) {
	if err := m.client.PostRelayAck(version, hash, applied, errText); err != nil {
		nlog.Core().Warn("relay ack failed", "version", version, "applied", applied, "error", err)
		return
	}
	if applied {
		m.ackedVersion = version
		m.ackedHash = hash
	}
}

// apply brings the running ruleset to the document's state. The lock is
// released during the drain wait so traffic accounting keeps flowing.
func (m *Manager) apply(ctx context.Context, doc *panel.RelayDocument) error {
	draining := !doc.RelayEnabled || doc.MachineStatus == "DRAINING"
	desired := make(map[string]panel.RelayRule)
	if !draining {
		for _, r := range doc.Rules {
			desired[r.RuleCode] = normalizeRule(r)
		}
	}

	m.mu.Lock()
	var stopped []*ruleRuntime
	for code, rt := range m.running {
		want, ok := desired[code]
		if ok && rt.healthy() && reflect.DeepEqual(rt.spec, want) {
			continue
		}
		if ok && !rt.healthy() && crashBackoffExceeded(rt) {
			// Crashed repeatedly: leave it down (already reported as
			// suspect via the failed ack at crash time) instead of
			// flapping the listener every poll.
			nlog.Core().Error("relay rule restart backoff", "rule", code)
			continue
		}
		m.closeRuntimeLocked(rt)
		stopped = append(stopped, rt)
		delete(m.running, code)
	}
	var toStart []panel.RelayRule
	for code, want := range desired {
		if _, ok := m.running[code]; !ok {
			toStart = append(toStart, want)
		}
	}
	sort.Slice(toStart, func(i, j int) bool { return toStart[i].RuleCode < toStart[j].RuleCode })
	var startErr error
	if len(toStart) > 0 {
		startErr = m.startRulesLocked(toStart)
	}
	m.mu.Unlock()
	if startErr != nil {
		return startErr
	}

	if draining && len(stopped) > 0 {
		m.waitDrain(ctx, stopped)
	}
	return nil
}

// waitDrain waits for stopped rules' connections to empty, bounded by
// the longest drain window among them (design: UDP gets no lossless
// promise — idle sessions end by TTL, anything left at the deadline is
// force-cleaned by abandonment and the bytes stay accounted through the
// zombie fold).
func (m *Manager) waitDrain(ctx context.Context, stopped []*ruleRuntime) {
	maxDrain := 0
	for _, rt := range stopped {
		if rt.spec.DrainTimeoutSec > maxDrain {
			maxDrain = rt.spec.DrainTimeoutSec
		}
	}
	if maxDrain <= 0 {
		maxDrain = 300
	}
	deadline := time.Now().Add(time.Duration(maxDrain) * time.Second)
	for {
		remaining := int64(0)
		for _, rt := range stopped {
			remaining += rt.currentConns()
		}
		if remaining == 0 {
			nlog.Core().Info("relay drain complete", "rules", len(stopped))
			return
		}
		if time.Now().After(deadline) {
			nlog.Core().Warn("relay drain deadline reached, forcing cleanup", "rules", len(stopped), "open_conns", remaining)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// closeRuntimeLocked closes a rule's services (listeners stop accepting
// immediately; established flows finish on their own and keep counting
// into the zombie entry until idle, when the traffic fold banks them)
// and unregisters its gost components. Caller holds m.mu.
func (m *Manager) closeRuntimeLocked(rt *ruleRuntime) {
	if rt.closed.Load() {
		return
	}
	rt.closed.Store(true)
	for _, ref := range rt.services {
		if err := ref.svc.Close(); err != nil {
			nlog.Core().Warn("relay service close failed", "service", ref.name, "error", err)
		}
	}
	unregisterComponents(rt)
	m.zombies[rt.spec.RuleCode] = append(m.zombies[rt.spec.RuleCode], rt)
}

func unregisterComponents(rt *ruleRuntime) {
	for _, c := range rt.components {
		switch c.kind {
		case "tlim":
			registry.TrafficLimiterRegistry().Unregister(c.name)
		case "clim":
			registry.ConnLimiterRegistry().Unregister(c.name)
		case "adm":
			registry.AdmissionRegistry().Unregister(c.name)
		}
	}
	rt.components = nil
}

// ensureGostInit performs the one-time gost runtime initialization
// (default logger + TLS config). This is the ONLY loader.Load call in
// the process: its group-replacement semantics close everything
// previously registered, so per-rule work uses the parser directly.
func (m *Manager) ensureGostInit() {
	m.gostInitOnce.Do(func() {
		if err := loader.Load(&xconfig.Config{Log: &xconfig.LogConfig{Level: "error"}}); err != nil {
			nlog.Core().Warn("relay gost init failed", "error", err)
		}
	})
}

// startRulesLocked builds and starts gost services for the given rules,
// one rule at a time through the config parser. Services are held
// directly by the manager and never registered in the global service
// registry: loader.Load's group replacement closes every previously
// registered service, which would kill unrelated rules on any change
// (found in the HK2 pilot, 2026-11-25). Caller holds m.mu.
func (m *Manager) startRulesLocked(specs []panel.RelayRule) error {
	m.ensureGostInit()
	if ratio, err := fdUsage(); err == nil {
		if ratio >= fdStopRatio {
			return fmt.Errorf("fd usage %.0f%% >= %.0f%% stop line, refusing new relay services", ratio*100, fdStopRatio*100)
		}
		if ratio >= fdWarnRatio {
			nlog.Core().Warn("relay fd usage high", "usage", fmt.Sprintf("%.0f%%", ratio*100))
		}
	}

	var started []*ruleRuntime
	for _, spec := range specs {
		rt, err := m.buildRule(spec)
		if err != nil {
			return err
		}
		m.running[spec.RuleCode] = rt
		started = append(started, rt)
		for _, ref := range rt.services {
			go m.serveRule(rt, ref)
		}
	}
	// Surface immediate serve failures (bind races etc.) to the ack.
	time.Sleep(250 * time.Millisecond)
	for _, rt := range started {
		for _, ref := range rt.services {
			if v := ref.err.Load(); v != nil {
				return fmt.Errorf("rule %s service %s: %s", rt.spec.RuleCode, ref.name, v.(string))
			}
		}
	}
	return nil
}

// buildRule parses one rule's gost services and registers its named
// components. On error everything it created is torn down again.
func (m *Manager) buildRule(spec panel.RelayRule) (rt *ruleRuntime, err error) {
	rt = &ruleRuntime{spec: spec}
	defer func() {
		if err != nil {
			for _, ref := range rt.services {
				_ = ref.svc.Close()
			}
			unregisterComponents(rt)
		}
	}()

	host, port := m.effectiveTarget(spec)
	targetAddr := net.JoinHostPort(host, strconv.Itoa(port))

	var transports []string
	switch strings.ToLower(spec.Transport) {
	case "tcp":
		transports = []string{"tcp"}
	case "udp":
		transports = []string{"udp"}
	default:
		transports = []string{"tcp", "udp"}
	}

	for _, tr := range transports {
		name := "relay-" + strings.ToLower(spec.RuleCode) + "-" + tr
		svcCfg := &xconfig.ServiceConfig{
			Name:     name,
			Addr:     net.JoinHostPort(spec.ListenIP, strconv.Itoa(spec.ListenPort)),
			Handler:  &xconfig.HandlerConfig{Type: tr},
			Listener: &xconfig.ListenerConfig{Type: tr},
			Forwarder: &xconfig.ForwarderConfig{
				Nodes: []*xconfig.ForwardNodeConfig{{Name: "target-0", Addr: targetAddr}},
			},
			Metadata: map[string]any{"enableStats": true},
		}
		if tr == "udp" && spec.UDPIdleTimeoutSec > 0 {
			svcCfg.Listener.Metadata = map[string]any{
				"ttl": (time.Duration(spec.UDPIdleTimeoutSec) * time.Second).String(),
			}
		}
		if spec.MaxConnections > 0 {
			climName := name + "-clim"
			lim := limiter_parser.ParseConnLimiter(&xconfig.LimiterConfig{
				Name:   climName,
				Limits: []string{"$ " + strconv.Itoa(spec.MaxConnections)},
			})
			if err = registry.ConnLimiterRegistry().Register(climName, lim); err != nil {
				return nil, fmt.Errorf("register conn limiter %s: %w", climName, err)
			}
			rt.components = append(rt.components, componentRef{kind: "clim", name: climName})
			svcCfg.CLimiter = climName
		}
		if spec.RateInBps != nil || spec.RateOutBps != nil {
			tlimName := name + "-tlim"
			var in, out int64
			if spec.RateInBps != nil {
				in = *spec.RateInBps
			}
			if spec.RateOutBps != nil {
				out = *spec.RateOutBps
			}
			limit := "$ " + strconv.FormatInt(in, 10) + " " + strconv.FormatInt(out, 10)
			if spec.BurstBytes != nil && *spec.BurstBytes > 0 {
				limit += " " + strconv.FormatInt(*spec.BurstBytes, 10)
			}
			lim := limiter_parser.ParseTrafficLimiter(&xconfig.LimiterConfig{
				Name:   tlimName,
				Limits: []string{limit},
			})
			if err = registry.TrafficLimiterRegistry().Register(tlimName, lim); err != nil {
				return nil, fmt.Errorf("register traffic limiter %s: %w", tlimName, err)
			}
			rt.components = append(rt.components, componentRef{kind: "tlim", name: tlimName})
			svcCfg.Limiter = tlimName
		}
		if len(spec.AllowlistCIDRs) > 0 {
			admName := name + "-adm"
			adm := admission_parser.ParseAdmission(&xconfig.AdmissionConfig{
				Name:      admName,
				Whitelist: true,
				Matchers:  spec.AllowlistCIDRs,
			})
			if err = registry.AdmissionRegistry().Register(admName, adm); err != nil {
				return nil, fmt.Errorf("register admission %s: %w", admName, err)
			}
			rt.components = append(rt.components, componentRef{kind: "adm", name: admName})
			svcCfg.Admissions = []string{admName}
		}
		svc, perr := service_parser.ParseService(svcCfg)
		if perr != nil {
			return nil, fmt.Errorf("parse service %s: %w", name, perr)
		}
		rt.services = append(rt.services, &svcRef{name: name, svc: svc})
	}
	return rt, nil
}

// serveRule runs one gost service with panic isolation: a single rule's
// crash never takes the agent or the other rules down with it.
func (m *Manager) serveRule(rt *ruleRuntime, ref *svcRef) {
	defer func() {
		ref.done.Store(true)
		if r := recover(); r != nil {
			ref.err.Store(fmt.Sprintf("panic: %v", r))
			nlog.Core().Error("relay service panicked", "service", ref.name, "panic", r)
			m.noteSuspect(rt, fmt.Sprintf("panic: %v", r))
		}
	}()
	if err := ref.svc.Serve(); err != nil && !rt.closed.Load() {
		ref.err.Store(err.Error())
		nlog.Core().Error("relay service exited", "service", ref.name, "error", err)
		m.noteSuspect(rt, err.Error())
	}
}

// noteSuspect flags the currently applied version as failed on the
// panel (its rules go UNKNOWN and the machine error is set), once per
// applied version.
func (m *Manager) noteSuspect(rt *ruleRuntime, errText string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rt.restarts = append(rt.restarts, time.Now())
	if m.appliedVersion <= 0 || m.suspectNoted {
		return
	}
	m.suspectNoted = true
	m.ackLocked(m.appliedVersion, m.appliedHash, false,
		fmt.Sprintf("rule %s runtime failure: %s", rt.spec.RuleCode, errText))
}

func crashBackoffExceeded(rt *ruleRuntime) bool {
	cutoff := time.Now().Add(-10 * time.Minute)
	n := 0
	for _, t := range rt.restarts {
		if t.After(cutoff) {
			n++
		}
	}
	return n >= 5
}

// ─── Rule → gost config ──────────────────────────────────────────────

// effectiveTarget resolves where the forwarder actually dials. A NODE
// target hosted on this very machine is dialed on loopback (the bytes
// still terminate at the node's own listener, so TLS SNI / Reality
// semantics — which live inside the forwarded stream — are untouched);
// anything else uses the panel-published host, re-resolved by the
// dialer on every connection.
func (m *Manager) effectiveTarget(spec panel.RelayRule) (string, int) {
	host, port := spec.TargetHost, spec.TargetPort
	if spec.TargetType == "NODE" && spec.TargetNodeID != nil && m.ownNodes != nil {
		if m.ownNodes()[*spec.TargetNodeID] {
			host = "127.0.0.1"
		}
	}
	return host, port
}

func normalizeRule(r panel.RelayRule) panel.RelayRule {
	if r.AllowlistCIDRs == nil {
		r.AllowlistCIDRs = []string{}
	} else {
		sorted := append([]string(nil), r.AllowlistCIDRs...)
		sort.Strings(sorted)
		r.AllowlistCIDRs = sorted
	}
	r.Transport = strings.ToLower(r.Transport)
	return r
}

// ─── Traffic accounting ──────────────────────────────────────────────

func (m *Manager) ruleTotalsLocked(code string) counters {
	total := m.state.Banked[code]
	if rt, ok := m.running[code]; ok {
		total = total.add(rt.totals())
	}
	for _, z := range m.zombies[code] {
		total = total.add(z.totals())
	}
	return total
}

// flushTraffic folds idle zombies, turns counter deltas into durable
// batches, and sends the pending queue. Batches are persisted before
// sending; a failed send leaves them queued with their original ids.
func (m *Manager) flushTraffic() {
	m.mu.Lock()
	// Fold zombies whose connections have fully drained.
	for code, list := range m.zombies {
		keep := list[:0]
		for _, z := range list {
			if z.currentConns() == 0 {
				m.state.Banked[code] = m.state.Banked[code].add(z.totals())
			} else {
				keep = append(keep, z)
			}
		}
		m.zombies[code] = keep
	}

	now := time.Now()
	codes := make(map[string]bool)
	for code := range m.state.Banked {
		codes[code] = true
	}
	for code := range m.running {
		codes[code] = true
	}
	for code := range m.zombies {
		codes[code] = true
	}
	for code := range codes {
		total := m.ruleTotalsLocked(code)
		delta := total.sub(m.state.Reported[code])
		if delta.zero() {
			continue
		}
		seq := m.state.NextSeq[code]
		batch := panel.RelayTrafficBatch{
			RuleCode:    code,
			BootID:      m.bootID,
			Seq:         seq,
			PeriodStart: m.lastFlush.Unix(),
			PeriodEnd:   now.Unix(),
			BytesIn:     delta.In,
			BytesOut:    delta.Out,
			Connections: delta.Conns,
		}
		if batch.PeriodEnd <= batch.PeriodStart {
			batch.PeriodEnd = batch.PeriodStart + 1
		}
		m.state.Pending = append(m.state.Pending, batch)
		m.state.NextSeq[code] = seq + 1
		m.state.Reported[code] = total
	}
	m.lastFlush = now
	pending := append([]panel.RelayTrafficBatch(nil), m.state.Pending...)
	m.saveStateLocked()
	m.mu.Unlock()

	if len(pending) == 0 {
		return
	}
	for start := 0; start < len(pending); start += 500 {
		end := start + 500
		if end > len(pending) {
			end = len(pending)
		}
		res, err := m.client.PostRelayTraffic(pending[start:end])
		if err != nil {
			nlog.Core().Warn("relay traffic report failed, batches stay queued", "count", len(pending), "error", err)
			return
		}
		if res.Rejected > 0 {
			// A batch the panel will never take (e.g. its rule was
			// deleted) must not poison the queue forever. The bytes
			// stay counted in Reported, so this is a stats loss on a
			// deleted rule only — logged loudly.
			nlog.Core().Error("relay traffic batches rejected by panel, dropping queue",
				"accepted", res.Accepted, "duplicates", res.Duplicates, "rejected", res.Rejected)
			m.mu.Lock()
			m.state.Pending = nil
			m.saveStateLocked()
			m.mu.Unlock()
			return
		}
	}
	m.mu.Lock()
	m.state.Pending = nil
	m.saveStateLocked()
	m.mu.Unlock()
}

// ─── Durable state ───────────────────────────────────────────────────

func (m *Manager) statePath() string {
	return filepath.Join(m.stateDir, stateFileName)
}

func (m *Manager) loadState() {
	m.state = newDiskState()
	raw, err := os.ReadFile(m.statePath())
	if err != nil {
		return
	}
	var s diskState
	if err := json.Unmarshal(raw, &s); err != nil {
		nlog.Core().Warn("relay state unreadable, starting fresh", "error", err)
		return
	}
	if s.NextSeq == nil {
		s.NextSeq = make(map[string]int64)
	}
	if s.Banked == nil {
		s.Banked = make(map[string]counters)
	}
	if s.Reported == nil {
		s.Reported = make(map[string]counters)
	}
	m.state = &s
}

// saveStateLocked persists the traffic state atomically. Caller holds m.mu.
func (m *Manager) saveStateLocked() {
	raw, err := json.Marshal(m.state)
	if err != nil {
		return
	}
	tmp := m.statePath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		nlog.Core().Warn("relay state write failed", "error", err)
		return
	}
	if err := os.Rename(tmp, m.statePath()); err != nil {
		nlog.Core().Warn("relay state rename failed", "error", err)
	}
}

func fdUsage() (float64, error) {
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return 0, err
	}
	if rlim.Cur == 0 {
		return 0, fmt.Errorf("zero fd limit")
	}
	return float64(len(fds)) / float64(rlim.Cur), nil
}

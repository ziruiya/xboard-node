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
	bootID    string

	mu             sync.Mutex
	flushMu        sync.Mutex // NODE-003: serializes flushTraffic end to end
	gostInitOnce   sync.Once
	genCounter     atomic.Int64 // R04: per-start generation, suffixes gost component names
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
// in production).
func NewManager(client *panel.Client, machineID int, stateDir string) *Manager {
	if stateDir == "" {
		stateDir = os.TempDir()
	}
	var rb [8]byte
	_, _ = rand.Read(rb[:])
	return &Manager{
		client:    client,
		machineID: machineID,
		stateDir:  stateDir,
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
//
// GPT review R04 (apply atomicity): the previous implementation closed
// every changed rule BEFORE starting any replacement, so a single start
// failure (port conflict, FD stop line) took the old, working rules
// down with it — and the FD check itself sat inside the start phase,
// after the closes. The phased version below never lets a failed apply
// reduce service:
//   0. the FD admission check runs before anything is closed;
//   1. fresh rules start first — a failure aborts with all old rules
//      still serving;
//   2. changed rules swap make-before-break when the new spec's listen
//      sockets are disjoint from the old one's; when they overlap
//      (the common retarget case), the old runtime is closed first and
//      AUTOMATICALLY RESTORED from its spec if the new start fails;
//   3. removed rules close last, so a mid-apply failure never removes
//      service the document still wants gone only on success.
//
// GPT round 2, B02 (whole-document atomicity): R04's phases were atomic
// per RULE, not per DOCUMENT — a multi-rule apply that failed halfway
// left the rules it had already switched running their NEW specs: a
// mixed ruleset that is neither the old document nor the new one. The
// apply below treats the document as one atomic unit:
//   A. every fresh rule and every disjoint swap is STAGED — built
//      (sockets bound, bind conflicts proven out) but NOT serving and
//      NOT registered, without closing anything it will replace; any
//      failure tears down all staged runtimes and the confirmed
//      ruleset is untouched (NODE-007: staged runtimes forward
//      nothing; serving starts only at the commit point below);
//   B. overlapping swaps (which cannot be staged — the new runtime
//      cannot bind until the old one closes) run one by one, each
//      individually restored on failure as in R04; if one fails, the
//      swaps already completed in this phase are REVERSED back to
//      their old specs and the staged set is torn down;
//   C. only when A and B have fully succeeded does the commit register
//      the staged runtimes, close the disjoint-swap predecessors and
//      close the removals.
// A failed apply therefore leaves the machine serving exactly the last
// confirmed document, rule for rule.
func (m *Manager) apply(ctx context.Context, doc *panel.RelayDocument) error {
	draining := !doc.RelayEnabled || doc.MachineStatus == "DRAINING"
	desired := make(map[string]panel.RelayRule)
	if !draining {
		for _, r := range doc.Rules {
			desired[r.RuleCode] = normalizeRule(r)
		}
	}

	m.mu.Lock()

	// Phase 0/1: classify without mutating anything.
	type swap struct {
		old  *ruleRuntime
		spec panel.RelayRule
	}
	var swaps []swap
	var fresh []panel.RelayRule
	var removed []*ruleRuntime
	for code, rt := range m.running {
		want, ok := desired[code]
		switch {
		case !ok:
			removed = append(removed, rt)
		case rt.healthy() && reflect.DeepEqual(rt.spec, want):
			// Already serving exactly this spec: keep.
		case !rt.healthy() && crashBackoffExceeded(rt):
			// Crashed repeatedly: leave it down (already reported as
			// suspect via the failed ack at crash time) instead of
			// flapping the listener every poll.
			nlog.Core().Error("relay rule restart backoff", "rule", code)
		default:
			swaps = append(swaps, swap{old: rt, spec: want})
		}
	}
	for code, want := range desired {
		if _, ok := m.running[code]; !ok {
			fresh = append(fresh, want)
		}
	}
	sort.Slice(fresh, func(i, j int) bool { return fresh[i].RuleCode < fresh[j].RuleCode })
	sort.Slice(swaps, func(i, j int) bool { return swaps[i].spec.RuleCode < swaps[j].spec.RuleCode })

	// The FD admission check gates ALL start work and runs before the
	// first close (R04: it previously ran after the closes).
	if len(fresh) > 0 || len(swaps) > 0 {
		if err := checkFDAdmission(); err != nil {
			m.mu.Unlock()
			return err
		}
	}

	// Stage A (B02): stage every fresh rule and every disjoint swap —
	// build, start and serve-verify the new runtimes WITHOUT
	// registering them and without closing anything they will replace.
	// The confirmed ruleset keeps serving untouched throughout; any
	// failure tears down everything staged and aborts.
	type stagedSwap struct {
		old *ruleRuntime
		rt  *ruleRuntime
	}
	var stagedFresh []*ruleRuntime
	var stagedDisjoint []stagedSwap
	teardownStaged := func() {
		for _, rt := range stagedFresh {
			m.closeRuntimeLocked(rt)
		}
		for _, s := range stagedDisjoint {
			m.closeRuntimeLocked(s.rt)
		}
	}
	for _, spec := range fresh {
		rt, err := m.buildRuntimeLocked(spec)
		if err != nil {
			teardownStaged()
			m.mu.Unlock()
			return err
		}
		stagedFresh = append(stagedFresh, rt)
	}
	for _, sw := range swaps {
		if socketsOverlap(sw.old.spec, sw.spec) {
			continue // stage B: cannot be staged, the sockets collide
		}
		// Make-before-break: the new runtime binds disjoint sockets, so
		// it can prove its sockets while the old one keeps serving. It
		// does not forward yet (NODE-007) — serving starts at commit.
		rt, err := m.buildRuntimeLocked(sw.spec)
		if err != nil {
			teardownStaged()
			m.mu.Unlock()
			return err
		}
		stagedDisjoint = append(stagedDisjoint, stagedSwap{old: sw.old, rt: rt})
	}

	// Stage B (B02): overlapping swaps, one by one. Each swap closes
	// its old runtime first (the new one cannot bind otherwise) and
	// restores it if the new start fails (R04). If any swap fails, the
	// swaps already completed in this stage are REVERSED to their old
	// specs and the staged set is torn down: the machine returns to
	// exactly the last confirmed ruleset instead of a mixture.
	var doneOverlap []swap
	reverseOverlap := func() {
		for i := len(doneOverlap) - 1; i >= 0; i-- {
			sw := doneOverlap[i]
			code := sw.spec.RuleCode
			if cur, ok := m.running[code]; ok {
				m.closeRuntimeLocked(cur)
				delete(m.running, code)
			}
			if rerr := m.restoreLocked(sw.old); rerr != nil {
				nlog.Core().Error("relay apply rollback: reversing a completed swap failed",
					"rule", code, "error", rerr)
			}
		}
	}
	for _, sw := range swaps {
		if !socketsOverlap(sw.old.spec, sw.spec) {
			continue // staged in A
		}
		code := sw.spec.RuleCode
		m.closeRuntimeLocked(sw.old)
		delete(m.running, code)
		if err := m.startOneLocked(sw.spec); err != nil {
			if rerr := m.restoreLocked(sw.old); rerr != nil {
				nlog.Core().Error("relay rule restore after failed swap failed",
					"rule", code, "start_error", err, "restore_error", rerr)
			} else {
				nlog.Core().Warn("relay rule swap failed, old runtime restored", "rule", code, "error", err)
			}
			reverseOverlap()
			teardownStaged()
			m.mu.Unlock()
			return err
		}
		doneOverlap = append(doneOverlap, sw)
	}

	// Commit (B02 + NODE-007): the staged runtimes start serving only
	// now — until this point they held their sockets bound but
	// forwarded nothing, so no uncommitted ruleset was ever exposed to
	// traffic. If a staged runtime fails its serve check here, all
	// staged runtimes are torn down and the stage-B swaps are
	// reversed: the machine keeps exactly the last confirmed ruleset.
	serveStaged := func() error {
		for _, rt := range stagedFresh {
			if err := m.serveRuntimeLocked(rt); err != nil {
				return err
			}
		}
		for _, s := range stagedDisjoint {
			if err := m.serveRuntimeLocked(s.rt); err != nil {
				return err
			}
		}
		return nil
	}
	if err := serveStaged(); err != nil {
		teardownStaged()
		reverseOverlap()
		m.mu.Unlock()
		return err
	}

	// Register the staged runtimes, close the disjoint-swap
	// predecessors they replace, and close the removals last.
	for _, rt := range stagedFresh {
		m.running[rt.spec.RuleCode] = rt
	}
	for _, s := range stagedDisjoint {
		m.running[s.rt.spec.RuleCode] = s.rt
		m.closeRuntimeLocked(s.old)
	}
	for _, rt := range removed {
		m.closeRuntimeLocked(rt)
		delete(m.running, rt.spec.RuleCode)
	}
	m.mu.Unlock()

	if draining && len(removed) > 0 {
		m.waitDrain(ctx, removed)
	}
	return nil
}

// specTransports returns the transport set a rule spec listens on.
func specTransports(spec panel.RelayRule) map[string]bool {
	switch strings.ToLower(spec.Transport) {
	case "tcp":
		return map[string]bool{"tcp": true}
	case "udp":
		return map[string]bool{"udp": true}
	default:
		return map[string]bool{"tcp": true, "udp": true}
	}
}

func isWildcardIP(ip string) bool {
	return ip == "" || ip == "0.0.0.0" || ip == "::"
}

// socketsOverlap reports whether two specs for the same rule would bind
// any common (transport, ip, port) socket — the condition under which
// make-before-break is impossible.
func socketsOverlap(a, b panel.RelayRule) bool {
	if a.ListenPort != b.ListenPort {
		return false
	}
	if a.ListenIP != b.ListenIP && !isWildcardIP(a.ListenIP) && !isWildcardIP(b.ListenIP) {
		return false
	}
	ta, tb := specTransports(a), specTransports(b)
	for tr := range ta {
		if tb[tr] {
			return true
		}
	}
	return false
}

// checkFDAdmission is the pre-flight resource gate for starting new
// services (R04: hoisted out of the start path so it runs before any
// existing service is closed).
func checkFDAdmission() error {
	if ratio, err := fdUsage(); err == nil {
		if ratio >= fdStopRatio {
			return fmt.Errorf("fd usage %.0f%% >= %.0f%% stop line, refusing new relay services", ratio*100, fdStopRatio*100)
		}
		if ratio >= fdWarnRatio {
			nlog.Core().Warn("relay fd usage high", "usage", fmt.Sprintf("%.0f%%", ratio*100))
		}
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

// buildRuntimeLocked builds one rule's gost services through the
// config parser WITHOUT serving them and WITHOUT registering the
// runtime in m.running — the caller decides when (and whether) the
// runtime joins the serving set. Services are held directly by the
// manager and never registered in the global service registry:
// loader.Load's group replacement closes every previously registered
// service, which would kill unrelated rules on any change (found in
// the HK2 pilot, 2026-11-25).
//
// gost binds its sockets at parse time, so a built runtime already
// proves its listen sockets are free — but it forwards nothing until
// serveRuntimeLocked runs its accept loops. NODE-007: apply() stages
// new runtimes in this built-but-silent state and only serves them at
// the commit point, so an apply that later fails never exposed an
// uncommitted ruleset to traffic.
//
// R04: on ANY failure the half-built runtime is torn down completely
// (services closed, components unregistered) before the error returns.
// Caller holds m.mu.
func (m *Manager) buildRuntimeLocked(spec panel.RelayRule) (*ruleRuntime, error) {
	m.ensureGostInit()
	gen := m.genCounter.Add(1)
	return m.buildRule(spec, gen)
}

// serveRuntimeLocked starts a built runtime's services and verifies
// they actually serve, surfacing immediate failures (bind races etc.)
// to the caller. On failure the runtime is torn down completely.
// Caller holds m.mu.
func (m *Manager) serveRuntimeLocked(rt *ruleRuntime) error {
	for _, ref := range rt.services {
		go m.serveRule(rt, ref)
	}
	// Surface immediate serve failures (bind races etc.) to the ack.
	time.Sleep(250 * time.Millisecond)
	for _, ref := range rt.services {
		if v := ref.err.Load(); v != nil {
			m.closeRuntimeLocked(rt)
			return fmt.Errorf("rule %s service %s: %s", rt.spec.RuleCode, ref.name, v.(string))
		}
	}
	return nil
}

// startRuntimeLocked builds and immediately serves one rule's runtime
// (stage-B overlapping swaps and restores, where the predecessor is
// already closed and delay would only extend the outage). Caller
// holds m.mu.
func (m *Manager) startRuntimeLocked(spec panel.RelayRule) (*ruleRuntime, error) {
	rt, err := m.buildRuntimeLocked(spec)
	if err != nil {
		return nil, err
	}
	if err := m.serveRuntimeLocked(rt); err != nil {
		return nil, err
	}
	return rt, nil
}

// startOneLocked starts one rule and registers its verified runtime.
// Caller holds m.mu.
func (m *Manager) startOneLocked(spec panel.RelayRule) error {
	rt, err := m.startRuntimeLocked(spec)
	if err != nil {
		return err
	}
	m.running[spec.RuleCode] = rt
	return nil
}

// restoreLocked restarts a previously closed runtime from its spec
// after a failed overlapping swap (R04). The restored runtime inherits
// the old one's crash-restart history so backoff accounting survives
// the swap attempt. Caller holds m.mu.
func (m *Manager) restoreLocked(old *ruleRuntime) error {
	rt, err := m.startRuntimeLocked(old.spec)
	if err != nil {
		return err
	}
	rt.restarts = old.restarts
	m.running[old.spec.RuleCode] = rt
	return nil
}

// buildRule parses one rule's gost services and registers its named
// components. On error everything it created is torn down again.
//
// The generation suffix (R04) keeps a replacement runtime's gost
// component names distinct from the still-running old runtime's, which
// is what makes make-before-break swaps possible at all: the global
// registries reject duplicate names, so an unsuffixed rebuild could
// never start while its predecessor lived.
func (m *Manager) buildRule(spec panel.RelayRule, gen int64) (rt *ruleRuntime, err error) {
	// built is deliberately a local, not the named return: error paths
	// return nil for rt, and the teardown below must still reach the
	// partially built runtime (dereferencing the named return there
	// panics and masks the real error).
	built := &ruleRuntime{spec: spec}
	defer func() {
		if err != nil {
			for _, ref := range built.services {
				_ = ref.svc.Close()
			}
			unregisterComponents(built)
		}
	}()

	// The panel publishes the rule's target as the final dial endpoint
	// (for NODE targets: the hosting machine's public address + the
	// node's effective landing port). The agent dials exactly what was
	// published — there is deliberately no same-machine loopback
	// shortcut: machine roles are exclusive (a machine is a landing
	// machine or a relay machine, never both), so a relay machine never
	// hosts its rules' target nodes, and a shortcut that silently
	// rewrites the dial address is how the HK2 pilot masked the
	// cross-machine addressing defect.
	targetAddr := net.JoinHostPort(spec.TargetHost, strconv.Itoa(spec.TargetPort))

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
		name := fmt.Sprintf("relay-%s-%s-g%d", strings.ToLower(spec.RuleCode), tr, gen)
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
		// NODE-005: the panel publishes an independent UDP session
		// cap (udp_max_sessions). gost enforces a service's conn
		// limiter on its listener Accept path — for UDP that is
		// exactly session creation/teardown — so the UDP service
		// gets its own limiter with the UDP cap instead of silently
		// inheriting (or ignoring) the TCP connection cap.
		connLimit := spec.MaxConnections
		if tr == "udp" && spec.UDPMaxSessions > 0 {
			connLimit = spec.UDPMaxSessions
		}
		if connLimit > 0 {
			climName := name + "-clim"
			lim := limiter_parser.ParseConnLimiter(&xconfig.LimiterConfig{
				Name:   climName,
				Limits: []string{"$ " + strconv.Itoa(connLimit)},
			})
			if err = registry.ConnLimiterRegistry().Register(climName, lim); err != nil {
				return nil, fmt.Errorf("register conn limiter %s: %w", climName, err)
			}
			built.components = append(built.components, componentRef{kind: "clim", name: climName})
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
			built.components = append(built.components, componentRef{kind: "tlim", name: tlimName})
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
			built.components = append(built.components, componentRef{kind: "adm", name: admName})
			svcCfg.Admissions = []string{admName}
		}
		svc, perr := service_parser.ParseService(svcCfg)
		if perr != nil {
			return nil, fmt.Errorf("parse service %s: %w", name, perr)
		}
		built.services = append(built.services, &svcRef{name: name, svc: svc})
	}
	return built, nil
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
//
// NODE-003: the whole flush is serialized on flushMu. The ticker flush
// and the shutdown flush used to interleave — the network send happens
// outside m.mu, and the older flush's unconditional queue clear could
// delete batches a younger flush had just enqueued (and not yet sent
// successfully). Settlement now removes exactly the batches the panel
// accounted for, by identity, never the whole queue by position.
func (m *Manager) flushTraffic() {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()

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
		if delta.In < 0 || delta.Out < 0 || delta.Conns < 0 {
			// NODE-001 (defense in depth): a negative batch is
			// unbookable — the panel rejects it and it would poison
			// the queue for every later batch. The startup rebase in
			// loadState makes this unreachable in normal operation;
			// if a counter ever does move backwards mid-process,
			// rebase the watermark instead of emitting poison.
			nlog.Core().Error("relay traffic delta went negative, rebasing watermark",
				"rule", code, "delta_in", delta.In, "delta_out", delta.Out, "delta_conns", delta.Conns)
			m.state.Reported[code] = total
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
		if !m.settleChunk(pending[start:end], res) {
			return
		}
	}
}

// batchKey identifies one immutable traffic batch.
type batchKey struct {
	rule string
	boot string
	seq  int64
}

func keyOf(b panel.RelayTrafficBatch) batchKey {
	return batchKey{rule: b.RuleCode, boot: b.BootID, seq: b.Seq}
}

// settleChunk removes exactly the batches of one sent chunk that the
// panel has accounted for (SB-ER-014 / NODE-002): accepted and
// duplicate batches are booked, batches named in rejected_batches are
// permanently refused (e.g. their rule was deleted) and dropped loudly
// — their bytes stay counted in Reported, so this is a stats loss on
// dead rules only. Every other batch stays queued with its original
// id, including batches in later chunks that were never sent.
//
// The old behaviour — any rejection cleared the ENTIRE pending queue —
// permanently lost valid data whenever one stale batch sat anywhere
// in a multi-chunk backlog.
//
// Returns false when the panel's verdict cannot be reconciled with
// the chunk (counts disagree, or rejections without per-batch names
// from a pre-ER-014 panel): the chunk then stays fully queued and the
// flush stops, rather than guessing which batches died.
func (m *Manager) settleChunk(chunk []panel.RelayTrafficBatch, res *panel.RelayTrafficResult) bool {
	if res.Accepted+res.Duplicates+res.Rejected != len(chunk) {
		nlog.Core().Error("relay traffic verdict does not add up, keeping chunk queued",
			"chunk", len(chunk), "accepted", res.Accepted, "duplicates", res.Duplicates, "rejected", res.Rejected)
		return false
	}
	if res.Rejected > 0 && len(res.RejectedBatches) != res.Rejected {
		nlog.Core().Error("relay traffic rejections without per-batch verdicts, keeping chunk queued",
			"chunk", len(chunk), "rejected", res.Rejected, "named", len(res.RejectedBatches))
		return false
	}

	settled := make(map[batchKey]bool, len(chunk))
	for _, b := range chunk {
		settled[keyOf(b)] = true
	}
	for _, rb := range res.RejectedBatches {
		nlog.Core().Error("relay traffic batch permanently rejected by panel, dropping it",
			"rule", rb.RuleCode, "boot_id", rb.BootID, "seq", rb.Seq, "reason", rb.Reason)
	}

	m.mu.Lock()
	kept := m.state.Pending[:0]
	for _, b := range m.state.Pending {
		if settled[keyOf(b)] {
			continue
		}
		kept = append(kept, b)
	}
	// Only this chunk's identities are in the settled set, so batches
	// from later chunks (not yet sent) survive untouched.
	m.state.Pending = kept
	m.saveStateLocked()
	m.mu.Unlock()
	return true
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

	// NODE-001: Reported is a watermark over counter totals that
	// included the PREVIOUS process's live services. Those counters
	// died with that process; this process's services start from
	// zero, so total − Reported could go negative — and the panel
	// rejects negative batches, which used to poison the queue for
	// every later batch. Rebase the watermark onto the durable
	// Banked totals (the only totals that survive a restart).
	// Pending batches carry their own bytes and are resent verbatim,
	// so nothing already batched is counted twice, and new deltas
	// measure only this process's counters.
	for code, banked := range s.Banked {
		s.Reported[code] = banked
	}
	for code := range s.Reported {
		if _, ok := s.Banked[code]; !ok {
			s.Reported[code] = counters{}
		}
	}
	// Recovery path for queues already poisoned on disk: a negative
	// batch can never be booked, so drop it (loudly) instead of
	// letting it block the queue forever.
	if len(s.Pending) > 0 {
		kept := s.Pending[:0]
		dropped := 0
		for _, b := range s.Pending {
			if b.BytesIn < 0 || b.BytesOut < 0 || b.Connections < 0 {
				dropped++
				continue
			}
			kept = append(kept, b)
		}
		if dropped > 0 {
			nlog.Core().Error("relay state: dropped unbookable negative traffic batches from a previous run",
				"count", dropped)
			s.Pending = kept
		}
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

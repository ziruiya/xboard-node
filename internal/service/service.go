package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/cert/dnsproviders"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/singbox"
	"github.com/cedar2025/xboard-node/internal/kernel/xray"
	"github.com/cedar2025/xboard-node/internal/limiter"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

type Service struct {
	cfg          *config.Config
	source       controlplane.Source
	sink         controlplane.Sink
	kernel       kernel.Kernel
	tracker      *tracker.Tracker
	limiter      *limiter.Limiter
	speedTracker *limiter.SpeedTracker
	cert         certificateManager

	lastConfig *model.NodeSpec
	lastUsers  []model.UserSpec

	// nodeLog is the logger with node context for this service instance.
	nodeLog *nlog.NodeLog

	// appliedState tracks the configuration and users that are currently
	// successfully running in the kernel.
	appliedState struct {
		Config *model.NodeSpec
		Users  []model.UserSpec
	}

	pushInterval int // seconds
	pullInterval int // seconds

	lastUserHash     string // hash of user list for change detection
	configNeedsApply bool
	lastConfigHash   string     // hash of full config for change detection
	pullBackoff      apiBackoff // backoff for panel pull failures
	pushBackoff      apiBackoff // backoff for panel push failures

	// pushActive prevents overlapping push/pull goroutines.
	pushActive atomic.Bool
	pullActive atomic.Bool
	// pullResults delivers async pullViaAPI results back to the main goroutine.
	pullResults chan pullResult

	wsClient        controlplane.PushClient        // Push client (nil if push is not enabled)
	wsEvents        chan controlplane.Event        // receives data events from push transport
	wsStatusCh      chan controlplane.StatusChange // receives push connectivity notifications
	wsCancel        context.CancelFunc             // cancels the WS client goroutine
	wsDisconnectAt  time.Time                      // when WS last disconnected (zero if connected)
	wsResyncPending bool
	// Local WS observations are ordered independently for each resource.
	resyncRevision       uint64
	configRevision       uint64
	userRevision         uint64
	polledConfigRevision uint64
	polledUserRevision   uint64
	machineMailbox       *controlplane.NodeMailbox
	machineMailboxCh     <-chan struct{}

	// metricsMu: lastUsers, lastConfig, wsClient, wsDisconnectAt (buildMetrics vs main loop).
	metricsMu sync.RWMutex
}

type certificateManager interface {
	Start(context.Context) error
	Stop()
	Reconfigure(context.Context, config.CertConfig) (bool, error)
	HasCert() bool
	TLSCert() kernel.TLSCert
	PendingRenewal() uint64
	ConfirmRenewal(uint64)
}

// pullResult carries the outcome of an async pullViaAPI back to the main goroutine.
type pullResult struct {
	commit                                       func(bool, bool)
	configRevision, userRevision, resyncRevision uint64
	failed                                       bool
	config                                       *model.NodeSpec
	users                                        []model.UserSpec
	configHash                                   string
	userHash                                     string
}

// apiBackoff implements simple exponential backoff for API failures.
type apiBackoff struct {
	mu            sync.Mutex
	skipRemaining int
}

func (b *apiBackoff) shouldSkip() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.skipRemaining > 0 {
		b.skipRemaining--
		return true
	}
	return false
}

func (b *apiBackoff) onSuccess() {
	b.mu.Lock()
	b.skipRemaining = 0
	b.mu.Unlock()
}

func (b *apiBackoff) onFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.skipRemaining <= 0 {
		b.skipRemaining = 1
	} else if b.skipRemaining < 8 {
		b.skipRemaining *= 2
	}
}

func New(cfg *config.Config) *Service {
	var cp controlplane.ControlPlane
	if cfg.IsStandalone() {
		cp = controlplane.NewLocalControlPlane(cfg)
	} else {
		cp = controlplane.NewPanelControlPlane(cfg.Panel, cfg.WS, cfg.Kernel)
	}
	return newService(cfg, cp)
}

// NewWithControlPlane creates a Service with an externally-provided
// ControlPlane. Used by the machine orchestrator to inject a
// MachinePanelControlPlane with WS mux routing.
func NewWithControlPlane(cfg *config.Config, cp controlplane.ControlPlane) *Service {
	return newService(cfg, cp)
}

func newService(cfg *config.Config, cp controlplane.ControlPlane) *Service {
	certMgr := cert.NewManager(cfg.Cert)

	var k kernel.Kernel
	switch cfg.Kernel.Type {
	case "singbox":
		k = singbox.New(cfg.Kernel)
	case "xray":
		k = xray.New(cfg.Kernel)
	default:
		nlog.Core().Warn("unsupported kernel type, defaulting to sing-box", "type", cfg.Kernel.Type)
		k = singbox.New(cfg.Kernel)
	}

	l := limiter.New()
	st := limiter.NewSpeedTracker(l)

	s := &Service{
		cfg:          cfg,
		source:       cp,
		sink:         cp,
		kernel:       k,
		tracker:      tracker.New(),
		limiter:      l,
		speedTracker: st,
		cert:         certMgr,
		wsEvents:     make(chan controlplane.Event, 16),
		wsStatusCh:   make(chan controlplane.StatusChange, 4),
		pullResults:  make(chan pullResult, 1),
	}

	// SB-TA-010: persist the unacknowledged traffic batch so a
	// restart resends it with its original ID instead of losing the
	// last window. Machine-mode config dirs are already per-node;
	// standalone multi-node layouts can share one dir, so the file
	// name carries the node ID when known.
	if dir := cfg.Kernel.ConfigDir; dir != "" {
		name := "traffic-batch.json"
		if cfg.Panel.NodeID > 0 {
			name = fmt.Sprintf("traffic-batch-%d.json", cfg.Panel.NodeID)
		}
		s.tracker.EnablePersistence(filepath.Join(dir, name))
	}
	return s
}

func (s *Service) Run(ctx context.Context) error {
	// Start cert manager (handles auto-TLS or manual cert verification)
	if err := s.cert.Start(ctx); err != nil {
		return fmt.Errorf("cert manager: %w", err)
	}
	defer s.cert.Stop()

	// Handshake: get WS config + initial data in one call
	if err := s.initialSetup(ctx); err != nil {
		return fmt.Errorf("initial setup: %w", err)
	}
	defer s.kernel.Stop()

	// Set up tickers
	trackTicker := time.NewTicker(time.Duration(s.cfg.Node.TrackInterval) * time.Second)
	pushInterval := time.Duration(math.Max(float64(s.pushInterval), 5)) * time.Second
	pullInterval := time.Duration(s.pullInterval) * time.Second
	reportTicker := time.NewTicker(pushInterval)
	pullTicker := time.NewTicker(pullInterval)
	deviceReportTicker := time.NewTicker(time.Duration(s.cfg.Node.DeviceReportInterval) * time.Second)

	// WS discovery: when in REST-only mode, periodically re-handshake to check
	// if WS has been enabled. When WS is disconnected for too long, re-check
	// if it's still available.
	wsDiscoveryTicker := time.NewTicker(time.Duration(s.cfg.WS.DiscoveryInterval) * time.Second)

	defer trackTicker.Stop()
	defer reportTicker.Stop()
	defer pullTicker.Stop()
	defer deviceReportTicker.Stop()
	defer wsDiscoveryTicker.Stop()

	s.startWSClient(ctx)
	return s.runEvents(ctx, trackTicker, reportTicker, pullTicker, deviceReportTicker, wsDiscoveryTicker)
}

func (s *Service) runEvents(ctx context.Context, trackTicker, reportTicker, pullTicker, deviceReportTicker, wsDiscoveryTicker *time.Ticker) error {
	renewalTicker := time.NewTicker(time.Second)
	defer renewalTicker.Stop()
	defer func() {
		s.wsResyncPending = false
		s.pullActive.Store(false)
	}()
	for {
		select {
		case <-ctx.Done():
			s.pushReportSync()
			return nil

		case <-renewalTicker.C:
			s.applyCertificateRenewal()
			s.checkWSDrops(ctx)
			if s.wsResyncPending {
				s.pullViaAPIAsync(ctx)
			}

		case <-trackTicker.C:
			s.trackAndEnforce(ctx)

		case <-reportTicker.C:
			s.pushReportAsync()

		case <-deviceReportTicker.C:
			s.reportDevices()

		case <-pullTicker.C:
			// When WebSocket is connected, skip REST polling entirely.
			// Config/user updates arrive via WS push.
			if s.wsClient != nil && s.wsClient.IsConnected() {
				continue
			}
			nlog.Core().Debug("polling from API (ws not connected)")
			s.pullViaAPIAsync(ctx)

		case result := <-s.pullResults:
			s.checkWSDrops(ctx)
			s.drainMachineMailbox(ctx)
			for queued := len(s.wsEvents); queued > 0; queued-- {
				s.handleWSEvent(ctx, <-s.wsEvents)
			}
			s.applyPullResult(ctx, result)

		case <-wsDiscoveryTicker.C:
			s.wsDiscovery(ctx)

		case status := <-s.wsStatusCh:
			s.handleWSStatus(ctx, status)

		case <-s.machineMailboxCh:
			s.drainMachineMailbox(ctx)

		case event := <-s.wsEvents:
			s.handleWSEvent(ctx, event)
		}
	}
}

func (s *Service) initialSetup(ctx context.Context) error {
	// Register speed limit lookup with kernel unconditionally (before push/poll branch).
	s.kernel.SetSpeedLimitFunc(s.speedTracker.GetLimiter)
	s.kernel.SetDeviceLimitFunc(s.limiter.GetDeviceLimitByUUID)

	bootstrap, err := s.source.Initial(ctx, s.wsMetrics, s.wsEvents, s.wsStatusCh)
	if err != nil {
		return err
	}

	if s.cfg.Node.PushInterval == 0 && bootstrap.PushInterval > 0 {
		s.pushInterval = bootstrap.PushInterval
	} else {
		s.pushInterval = s.cfg.Node.PushInterval
	}
	if s.pushInterval == 0 {
		s.pushInterval = 60
	}

	if s.cfg.Node.PullInterval == 0 && bootstrap.PullInterval > 0 {
		s.pullInterval = bootstrap.PullInterval
	} else {
		s.pullInterval = s.cfg.Node.PullInterval
	}
	if s.pullInterval == 0 {
		s.pullInterval = 60
	}

	if bootstrap.Push != nil {
		s.wsClient = bootstrap.Push
	}
	s.machineMailbox = bootstrap.Mailbox
	if s.machineMailbox != nil {
		s.machineMailboxCh = s.machineMailbox.NotifyCh()
	}
	if bootstrap.Config == nil {
		if bootstrap.Push != nil {
			// In machine mode a shared WS client may be available before the first
			// per-node snapshot arrives. In that case we wait for subsequent WS/REST
			// updates instead of failing startup.
			return nil
		}
		return fmt.Errorf("initial config is nil")
	}

	s.updateUserState(bootstrap.Users)
	if !s.applyConfig(ctx, bootstrap.Config, computeConfigHash(bootstrap.Config), nil) {
		return fmt.Errorf("apply initial configuration")
	}
	nlog.Core().Info("initial snapshot ready", "protocol", bootstrap.Config.Protocol,
		"port", bootstrap.Config.ServerPort, "users", len(bootstrap.Users))
	s.markMailboxReadyAndDrain(ctx)
	return nil
}

// applyRemoteOverrides updates service-level settings (log level, cert config)
// from the panel's NodeConfig. Returns true if cert paths changed (kernel restart needed).
func (s *Service) applyRemoteOverrides(ctx context.Context, nc *model.NodeSpec) bool {
	if nc == nil {
		return false
	}

	// Dynamic Log Level (Kernel)
	if nc.KernelLogLevel != "" && nc.KernelLogLevel != s.cfg.Kernel.LogLevel {
		nlog.Core().Info("cert: kernel log level override", "old", s.cfg.Kernel.LogLevel, "new", nc.KernelLogLevel)
		s.cfg.Kernel.LogLevel = nc.KernelLogLevel
	}

	// Certificate configuration from panel (panel-first: takes precedence over local config)
	if nc.CertConfig != nil {
		return s.applyNodeCert(ctx, nc.CertConfig)
	}

	// Legacy fields (deprecated: prefer cert_config)
	if nc.AutoTLS != s.cfg.Cert.AutoTLS {
		nlog.Core().Info("cert: auto_tls policy changed (deprecated field)", "new", nc.AutoTLS)
		s.cfg.Cert.AutoTLS = nc.AutoTLS
	}
	if nc.Domain != "" && nc.Domain != s.cfg.Cert.Domain {
		s.cfg.Cert.Domain = nc.Domain
	}

	return false
}

// applyPanelCert converts a panel CertConfig into the local config format and
// reconfigures the cert manager. Reports whether cert paths changed.
func (s *Service) applyNodeCert(ctx context.Context, newCfg *config.CertConfig) bool {
	if newCfg == nil {
		return false
	}
	cfgCopy := *newCfg
	cfgCopy.CertDir = s.cfg.Cert.CertDir

	changed, err := s.cert.Reconfigure(ctx, cfgCopy)
	if err != nil {
		nlog.Core().Error("failed to apply runtime cert config", "mode", cfgCopy.CertMode, "error", err)
		return false
	}
	s.cfg.Cert = cfgCopy
	if changed {
		msg := fmt.Sprintf("cert: material updated, has_cert=%v", s.cert.HasCert())
		if s.nodeLog != nil {
			s.nodeLog.Info(msg)
		} else {
			nlog.Core().Info(msg)
		}
	}
	return changed
}

// startWSClient starts the push client goroutine if a client is configured.
func (s *Service) startWSClient(ctx context.Context) {
	if s.wsClient == nil {
		return
	}
	wsCtx, wsCancel := context.WithCancel(ctx)
	s.wsCancel = wsCancel
	go s.wsClient.Run(wsCtx)
}

func (s *Service) markMailboxReadyAndDrain(ctx context.Context) {
	if s.machineMailbox == nil {
		return
	}
	// Seed mailbox with bootstrap state so delta events can be applied
	// incrementally instead of always triggering REST reconciliation.
	s.metricsMu.RLock()
	users := s.lastUsers
	config := s.lastConfig
	s.metricsMu.RUnlock()
	s.machineMailbox.SeedBaseline(users, config)
	s.machineMailbox.MarkReady()
	s.drainMachineMailbox(ctx)
}

func (s *Service) drainMachineMailbox(ctx context.Context) {
	if s.machineMailbox == nil {
		return
	}
	state := s.machineMailbox.DrainIfReady()
	if state.HasConfig {
		s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncConfig, Config: state.Config})
	}
	if state.HasUsers {
		s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncUsers, Users: state.Users})
	}
	if state.HasDevices {
		s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncDevices, DeviceUsers: state.DeviceUsers})
	}
	if state.NeedsReconcile {
		s.userRevision++
		s.requestWSResync(ctx, "machine_mailbox_reconcile")
	}
}

func (s *Service) checkWSDrops(ctx context.Context) {
	if push, ok := s.wsClient.(interface{ TakeDroppedEvents() bool }); ok && push.TakeDroppedEvents() {
		s.configRevision++
		s.userRevision++
		s.requestWSResync(ctx, "drop_detected")
	}
}

func (s *Service) requestWSResync(ctx context.Context, reason string) {
	if ctx.Err() != nil || !s.source.SupportsPolling() {
		return
	}
	s.resyncRevision++
	if !s.wsResyncPending {
		nlog.Core().Warn("ws state may be stale, scheduling REST reconciliation", "reason", reason)
		s.wsResyncPending = true
	}
	s.pullViaAPIAsync(ctx)
}

func (s *Service) wsMetrics() map[string]interface{} {
	status := monitor.Collect()
	m := s.buildMetrics(status)
	m["kernel_status"] = s.kernel.IsRunning()
	return m
}

// handleWSStatus reacts to WS connectivity changes.

// - On disconnect: record timestamp, immediately REST poll.
// - On reconnect: clear disconnect timestamp, REST poll to catch missed events.
func (s *Service) handleWSStatus(ctx context.Context, status controlplane.StatusChange) {
	if status.NeedsResync {
		s.requestWSResync(ctx, "drop_detected")
	}
	if status.Connected {
		s.metricsMu.Lock()
		s.wsDisconnectAt = time.Time{}
		s.metricsMu.Unlock()
		// Use nodeLog if available, otherwise core
		if s.nodeLog != nil {
			s.nodeLog.Info("ws connected")
		} else {
			nlog.Core().Info("ws connected")
		}
		// After reconnect, proactively pull once to ensure we haven't missed
		// any updates during the disconnection window.
		s.requestWSResync(ctx, "ws_connected")
	} else {
		s.metricsMu.Lock()
		if s.wsDisconnectAt.IsZero() {
			s.wsDisconnectAt = time.Now()
		}
		s.metricsMu.Unlock()
		if s.nodeLog != nil {
			s.nodeLog.Info("ws disconnected")
		} else {
			nlog.Core().Info("ws disconnected")
		}
		// Clear global device state on disconnect
		s.kernel.ClearGlobalDevices()
		s.pullViaAPIAsync(ctx)
	}
}

// wsDiscovery periodically checks WS availability:
//
//  1. REST-only mode (wsClient == nil): Re-handshake to check if panel now has
//     WS enabled. If so, create and start a WS client. This handles the case
//     where WS was not enabled at startup but enabled later.
//
//  2. WS disconnected for >10 min: Re-handshake to check if WS config changed.
//     If WS is now disabled, stop the WS client and switch to REST-only.
//     If WS config changed (different URL/channel), restart with new config.
func (s *Service) wsDiscovery(ctx context.Context) {
	if !s.source.SupportsDiscovery() {
		return
	}

	needsCheck := false
	if s.wsClient == nil {
		needsCheck = true
		nlog.Core().Debug("push discovery: no push client, checking if control plane enabled push")
	} else if !s.wsDisconnectAt.IsZero() && time.Since(s.wsDisconnectAt) > 10*time.Minute {
		needsCheck = true
		nlog.Core().Debug("push discovery: push disconnected for >10min, re-checking")
	}
	if !needsCheck {
		return
	}

	pushClient, err := s.source.Discover(ctx, s.wsMetrics, s.wsEvents, s.wsStatusCh)
	if err != nil {
		nlog.Core().Debug("push discovery failed", "error", err)
		return
	}
	if s.source.SupportsPolling() {
		s.pullViaAPIAsync(ctx)
	}

	if pushClient != nil {
		if s.wsClient == nil {
			nlog.Core().Info("push discovery: control plane enabled push, creating client")
			s.metricsMu.Lock()
			s.wsClient = pushClient
			s.wsDisconnectAt = time.Time{}
			s.metricsMu.Unlock()
			s.startWSClient(ctx)
		}
	} else if s.wsClient != nil {
		nlog.Core().Info("push discovery: control plane disabled push, switching to polling")
		if s.wsCancel != nil {
			s.wsCancel()
		}
		s.metricsMu.Lock()
		s.wsClient = nil
		s.wsDisconnectAt = time.Time{}
		s.metricsMu.Unlock()
		s.wsCancel = nil
	}
}

// handleWSEvent processes data events received via WebSocket
func (s *Service) handleWSEvent(ctx context.Context, event controlplane.Event) {
	switch event.Type {
	case controlplane.EventSyncConfig:
		if event.Config == nil {
			return
		}
		s.configRevision++
		if !s.applyConfig(ctx, event.Config, computeConfigHash(event.Config), nil) {
			s.requestWSResync(ctx, "config_apply_failed")
		}

	case controlplane.EventSyncUsers:
		if event.Users == nil {
			s.requestWSResync(ctx, "missing_user_snapshot")
			return
		}
		s.userRevision++
		hash := computeUserHash(event.Users)
		if hash != s.lastUserHash {
			s.applyUserUpdate(ctx, event.Users, hash)
			if hash != s.lastUserHash {
				s.requestWSResync(ctx, "users_apply_failed")
			}
		}

	case controlplane.EventSyncUserDelta:
		// The legacy wire event has no base revision. Reconcile with a full set.
		s.userRevision++
		s.requestWSResync(ctx, "unversioned_user_delta")

	case controlplane.EventSyncDevices:
		// Sync global device state
		if event.DeviceUsers != nil {
			s.kernel.UpdateGlobalDevices(event.DeviceUsers)
		}

	default:
		nlog.Core().Debug(fmt.Sprintf("unknown ws event: %v", event.Type))
	}
}

// pullViaAPIAsync fetches config/users from the panel API in a background
// goroutine and sends the result to pullResults for the main goroutine to apply.
func (s *Service) pullViaAPIAsync(ctx context.Context) {
	if ctx.Err() != nil || !s.source.SupportsPolling() {
		return
	}
	if !s.pullActive.CompareAndSwap(false, true) {
		nlog.Core().Debug("pull already in progress, skipping")
		return
	}
	if s.pullBackoff.shouldSkip() {
		nlog.Core().Debug("skipping pull due to backoff")
		s.pullActive.Store(false)
		return
	}

	result := pullResult{
		configRevision: s.configRevision, userRevision: s.userRevision,
		resyncRevision: s.resyncRevision,
	}
	pollCtx := ctx
	if s.wsResyncPending || s.configRevision != s.polledConfigRevision || s.userRevision != s.polledUserRevision {
		pollCtx = controlplane.FullPoll(ctx)
	}
	go func() {
		snapshot, err := s.source.Poll(pollCtx)
		if err != nil {
			nlog.Core().Error("poll control plane failed", "error", err)
			s.pullBackoff.onFailure()
			result.failed = true
		} else {
			s.pullBackoff.onSuccess()
			result.config, result.users, result.commit = snapshot.Config, snapshot.Users, snapshot.Commit
			if result.config != nil {
				result.configHash = computeConfigHash(result.config)
			}
			if result.users != nil {
				result.userHash = computeUserHash(result.users)
			}
		}
		select {
		case s.pullResults <- result:
		case <-ctx.Done():
			s.pullActive.Store(false)
		}
	}()
}

// applyPullResult rechecks both local revision domains at the commit point.
func (s *Service) applyPullResult(ctx context.Context, result pullResult) {
	defer s.pullActive.Store(false)
	if result.failed || ctx.Err() != nil {
		return
	}

	configCurrent := result.configRevision == s.configRevision
	usersCurrent := result.userRevision == s.userRevision
	configApplied, usersApplied := configCurrent, usersCurrent
	usersChanged := usersCurrent && result.users != nil && result.userHash != s.lastUserHash
	configChanged := configCurrent && result.config != nil && (result.configHash != s.lastConfigHash || s.configNeedsApply)
	if configChanged && usersChanged && len(result.users) != 0 {
		// Restart or reload the candidate config with its matching authorization set.
		configApplied = s.applyConfig(ctx, result.config, result.configHash, result.users)
		usersApplied = configApplied
		if !configApplied && s.kernel.IsRunning() {
			// Config validation can fail while the previous listener is still serving.
			s.applyUserUpdate(ctx, result.users, result.userHash)
			usersApplied = result.userHash == s.lastUserHash
		}
	} else {
		if usersChanged {
			s.applyUserUpdate(ctx, result.users, result.userHash)
			usersApplied = result.userHash == s.lastUserHash
		}
		if configChanged {
			configApplied = s.applyConfig(ctx, result.config, result.configHash, nil)
		}
	}
	if result.commit != nil {
		result.commit(configApplied, usersApplied)
	}
	if configApplied {
		s.polledConfigRevision = result.configRevision
	}
	if usersApplied {
		s.polledUserRevision = result.userRevision
	}

	if s.machineMailbox != nil {
		var users []model.UserSpec
		var cfg *model.NodeSpec
		if usersApplied && result.users != nil {
			users = s.lastUsers
		}
		if configApplied && result.config != nil {
			cfg = s.lastConfig
		}
		s.machineMailbox.SyncBaseline(users, cfg)
	}
	if configApplied && usersApplied && result.resyncRevision == s.resyncRevision {
		s.wsResyncPending = false
	} else if !configApplied || !usersApplied {
		s.wsResyncPending = true
	}
}

// ─── User state helpers ─────────────────────────────────────────────────────

func (s *Service) updateUserState(users []model.UserSpec) {
	if users == nil {
		users = []model.UserSpec{}
	}
	_, _ = s.prepareUserState(users)
}

func (s *Service) prepareUserState(users []model.UserSpec) (prevUsers []model.UserSpec, prevHash string) {
	if users == nil {
		users = []model.UserSpec{}
	}

	s.metricsMu.RLock()
	prevUsers = slices.Clone(s.lastUsers)
	s.metricsMu.RUnlock()
	prevHash = s.lastUserHash

	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()

	s.metricsMu.Lock()
	s.lastUsers = append([]model.UserSpec{}, users...)
	s.metricsMu.Unlock()
	s.lastUserHash = computeUserHash(users)
	return prevUsers, prevHash
}

func (s *Service) restoreUserState(users []model.UserSpec, hash string) {
	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()
	s.metricsMu.Lock()
	s.lastUsers = slices.Clone(users)
	s.metricsMu.Unlock()
	s.lastUserHash = hash
}

func (s *Service) applyCertificateRenewal() {
	if s.cert.PendingRenewal() != 0 && s.lastConfig != nil && len(s.lastUsers) != 0 {
		s.startKernel(s.lastConfig, s.lastUsers)
	}
}

// startKernel starts (or restarts) the kernel with the given config/users and
// records the successfully applied state. Returns false on error.
func (s *Service) startKernel(nc *model.NodeSpec, users []model.UserSpec) bool {
	renewal := s.cert.PendingRenewal()
	tls := s.cert.TLSCert()
	if err := s.kernel.Start(nc, users, tls); err != nil {
		nlog.Core().Error("failed to start kernel", "error", err)
		return false
	}

	if renewal != 0 && tls.HasCert() {
		s.cert.ConfirmRenewal(renewal)
	}
	s.appliedState.Config = nc
	s.appliedState.Users = users

	// Initialize node logger on first successful start
	if s.nodeLog == nil {
		s.nodeLog = nlog.ForNode(nc.Protocol, nc.ServerPort)
	}
	s.speedTracker.SetLogCallback(func(msg string) {
		fullMsg := fmt.Sprintf("speedtracker: %s active_limiters=%d", msg, s.speedTracker.LimitedUserCount())
		s.nodeLog.Info(fullMsg)
	})
	s.nodeLog.Info(fmt.Sprintf("started, %d users", len(users)))
	return true
}

// startWithCandidateUsers starts a stopped kernel directly with a
// candidate user set (NODE-012). The decision is made against the
// CANDIDATE set, never against the previously applied one: the old
// ensureRunning() helper judged by the stale set, so a node that had
// started with zero users discarded every later user-only update and
// could never pick up its first user. On start failure the previous
// state is restored and false is returned.
func (s *Service) startWithCandidateUsers(users []model.UserSpec) bool {
	if s.kernel.IsRunning() {
		return true
	}
	if s.lastConfig == nil || len(users) == 0 {
		return false
	}
	prevUsers, prevHash := s.prepareUserState(users)
	if !s.startKernel(s.lastConfig, users) {
		s.restoreUserState(prevUsers, prevHash)
		return false
	}
	return true
}

// ─── User update entry points ───────────────────────────────────────────────

// applyUserUpdate replaces the full user set and hot-swaps the kernel.
// Called from WS sync.users and REST polling.
func (s *Service) applyUserUpdate(ctx context.Context, users []model.UserSpec, newHash string) {
	if !s.kernel.IsRunning() {
		// NODE-012: judge by the candidate set. With a stopped kernel
		// and a non-empty candidate set + config, start the kernel
		// with the new set directly; with nothing startable, still
		// record the authoritative set so state and hashes stay
		// truthful for the next update.
		if s.startWithCandidateUsers(users) {
			if newHash != "" {
				s.lastUserHash = newHash
			}
			return
		}
		if s.lastConfig == nil || len(users) == 0 {
			s.updateUserState(users)
		}
		return
	}

	prevUsers, prevHash := s.prepareUserState(users)
	added, removed, err := s.kernel.UpdateUsers(users)
	if err != nil {
		nlog.Core().Warn(fmt.Sprintf("UpdateUsers failed, restarting kernel: %v", err))
		if !s.startKernel(s.lastConfig, users) {
			s.restoreUserState(prevUsers, prevHash)
		}
		return
	}
	if newHash != "" {
		s.lastUserHash = newHash
	}
	if s.nodeLog != nil && (added > 0 || removed > 0) {
		s.nodeLog.Info(fmt.Sprintf("users updated: +%d -%d", added, removed))
	}
}

// applyUserDelta applies an incremental user change (add or remove) directly
// via the kernel's atomic user API. Kernel updates run before updateUserState.
func (s *Service) applyUserDelta(ctx context.Context, action string, deltaUsers []model.UserSpec) {
	switch action {
	case "add":
		// Defensive check for empty or nil deltaUsers
		if deltaUsers == nil || len(deltaUsers) == 0 {
			return
		}
		merged := mergeUsers(s.lastUsers, deltaUsers)

		if !s.kernel.IsRunning() {
			// NODE-012: same candidate-set rule as applyUserUpdate —
			// a first user arriving as a delta on a node that started
			// empty must start the kernel with the merged set, not be
			// discarded for the stale set being empty.
			s.startWithCandidateUsers(merged)
			return
		}

		// NODE-013 (residual): the rotation pre-delete revokes the old
		// credential ahead of the incremental add. Its result must be
		// checked — silently ignoring a failed revoke would let this
		// path claim success while the old credential stays valid. On
		// failure, skip the incremental path entirely and converge
		// through the full rebuild below.
		preDeleteFailed := false
		for _, delta := range deltaUsers {
			for _, old := range s.lastUsers {
				if old.ID == delta.ID && old.UUID != delta.UUID {
					if _, err := s.kernel.RemoveUsers([]model.UserSpec{old}); err != nil {
						nlog.Core().Warn(fmt.Sprintf("rotation pre-delete failed for user %d: %v, falling back to full rebuild", old.ID, err))
						preDeleteFailed = true
					}
					break
				}
			}
			if preDeleteFailed {
				break
			}
		}

		prevUsers, prevHash := s.prepareUserState(merged)
		added := 0
		incrementalOK := false
		if !preDeleteFailed {
			var err error
			added, err = s.kernel.AddUsers(deltaUsers)
			if err != nil {
				nlog.Core().Warn(fmt.Sprintf("AddUsers failed: %v, falling back to full rebuild", err))
			} else {
				incrementalOK = true
			}
		}
		if !incrementalOK {
			// Trusted full rebuild: full-set sync first, then a full
			// kernel restart. Both results are checked — if the
			// restart also fails, restore the pre-change state rather
			// than claiming a convergence that did not happen.
			if _, _, err := s.kernel.UpdateUsers(merged); err != nil {
				nlog.Core().Warn(fmt.Sprintf("UpdateUsers fallback failed: %v, restarting kernel", err))
				if !s.startKernel(s.lastConfig, merged) {
					s.restoreUserState(prevUsers, prevHash)
					return
				}
			}
		}
		if s.nodeLog != nil && added > 0 {
			s.nodeLog.Info(fmt.Sprintf("users added: +%d", added))
		}

	case "remove":
		// Defensive check for empty or nil deltaUsers
		if deltaUsers == nil || len(deltaUsers) == 0 {
			return
		}
		filtered := subtractUsers(s.lastUsers, deltaUsers)

		if !s.kernel.IsRunning() {
			return
		}

		prevUsers, prevHash := s.prepareUserState(filtered)
		removed, err := s.kernel.RemoveUsers(deltaUsers)
		if err != nil {
			nlog.Core().Warn(fmt.Sprintf("RemoveUsers failed: %v, falling back to UpdateUsers", err))
			if _, _, err := s.kernel.UpdateUsers(filtered); err != nil {
				nlog.Core().Error(fmt.Sprintf("UpdateUsers fallback failed: %v", err))
				s.restoreUserState(prevUsers, prevHash)
				return
			}
		}
		if s.nodeLog != nil && removed > 0 {
			s.nodeLog.Info(fmt.Sprintf("users removed: -%d", removed))
		}

	default:
		nlog.Core().Warn(fmt.Sprintf("unknown user delta action: %s", action))
	}
}

// mergeUsers overlays deltaUsers onto base (keyed by ID). New users are
// appended, existing users have their properties overwritten.
func mergeUsers(base, delta []model.UserSpec) []model.UserSpec {
	// Handle nil slices
	if base == nil {
		base = []model.UserSpec{}
	}
	if delta == nil {
		return base
	}

	m := make(map[int]model.UserSpec, len(base))
	for _, u := range base {
		m[u.ID] = u
	}
	for _, u := range delta {
		m[u.ID] = u
	}
	out := make([]model.UserSpec, 0, len(m))
	for _, u := range m {
		out = append(out, u)
	}
	return out
}

// subtractUsers returns base with all users in delta removed.
func subtractUsers(base, delta []model.UserSpec) []model.UserSpec {
	if base == nil {
		return nil
	}
	if delta == nil || len(delta) == 0 {
		return base
	}
	removeSet := make(map[int]struct{}, len(delta))
	for _, u := range delta {
		removeSet[u.ID] = struct{}{}
	}
	out := make([]model.UserSpec, 0, len(base))
	for _, u := range base {
		if _, ok := removeSet[u.ID]; !ok {
			out = append(out, u)
		}
	}
	return out
}

// applyConfig publishes the config and hash only after kernel confirmation.
func (s *Service) applyConfig(ctx context.Context, candidate *model.NodeSpec, hash string, users []model.UserSpec) bool {
	if hash == s.lastConfigHash && !s.configNeedsApply {
		return true
	}
	if err := validateNodeRuntime(s.cfg, s.kernel.Protocols(), candidate, s.cert.TLSCert()); err != nil {
		nlog.Core().Warn("runtime config validation failed", "error", err)
		return false
	}
	var previousUsers []model.UserSpec
	var previousUserHash string
	if users != nil {
		previousUsers, previousUserHash = s.prepareUserState(users)
	}
	s.applyRemoteOverrides(ctx, candidate)
	if len(s.lastUsers) == 0 {
		s.kernel.Stop()
		s.appliedState.Users = nil
	} else {
		reloaded := false
		if s.kernel.IsRunning() {
			if err := s.kernel.Reload(candidate, s.lastUsers, s.cert.TLSCert()); err != nil {
				nlog.Core().Warn("reload failed, restarting", "error", err)
				// A partial Reload may already have removed the previous inbound.
				s.kernel.Stop()
			} else {
				reloaded = true
			}
		}
		if !reloaded && !s.startKernel(candidate, s.lastUsers) {
			s.configNeedsApply = true
			if users != nil {
				s.restoreUserState(previousUsers, previousUserHash)
			}
			return false
		}
		s.appliedState.Users = s.lastUsers
	}
	s.appliedState.Config = candidate
	s.metricsMu.Lock()
	s.lastConfig = candidate
	s.metricsMu.Unlock()
	s.lastConfigHash = hash
	s.configNeedsApply = false
	return true
}

func (s *Service) trackAndEnforce(ctx context.Context) {
	if !s.kernel.IsRunning() {
		return
	}

	traffic, aliveIPs, connCount, err := s.kernel.GetUserTraffic(ctx)
	if err != nil {
		nlog.Core().Debug("get user traffic failed", "error", err)
		return
	}

	s.tracker.Process(traffic, aliveIPs, connCount)

	// Only log stats if there's actual traffic or connections
	if connCount > 0 || len(traffic) > 0 {
		if s.nodeLog != nil {
			s.nodeLog.Debug(fmt.Sprintf("tracker: %d conns, %d users online", connCount, len(traffic)))
		} else {
			nlog.TrackerStats(connCount, len(traffic))
		}
	}
}

// pushReportAsync sends the report in a background goroutine so the select
// loop is never blocked by slow HTTP. Only one push runs at a time.
func (s *Service) pushReportAsync() {
	if !s.sink.SupportsReporting() {
		return
	}
	if !s.pushActive.CompareAndSwap(false, true) {
		nlog.Core().Debug("push already in progress, skipping")
		return
	}
	if s.pushBackoff.shouldSkip() {
		nlog.Core().Debug("skipping report due to backoff")
		s.pushActive.Store(false)
		return
	}

	// SB-TA-001/002: an unacknowledged batch is re-sent verbatim
	// (same ID, same content); new samples keep accumulating into the
	// next batch and are never merged into it. Only when no batch is
	// outstanding do we flush a fresh one.
	batch := s.tracker.Unacked()
	if batch == nil {
		batch = s.tracker.FlushBatch()
		if batch != nil {
			s.tracker.SetUnacked(batch)
		}
	}
	var traffic map[int][2]int64
	var batchMeta *controlplane.TrafficBatchMeta
	if batch != nil {
		traffic = batch.Data
		batchMeta = &controlplane.TrafficBatchMeta{BatchID: batch.ID, CollectedFrom: batch.CollectedFrom, CollectedTo: batch.CollectedTo}
	} else {
		traffic = make(map[int][2]int64)
	}
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()
	metrics := s.buildMetrics(status)
	metrics["kernel_status"] = s.kernel.IsRunning()

	go func() {
		defer s.pushActive.Store(false)
		if err := s.sink.Report(controlplane.ReportPayload{Traffic: traffic, TrafficBatch: batchMeta, Alive: aliveIPs, Online: online, CPU: status.CPU, Mem: [2]uint64{status.MemTotal, status.MemUsed}, Swap: [2]uint64{status.SwapTotal, status.SwapUsed}, Disk: [2]uint64{status.DiskTotal, status.DiskUsed}, Metrics: metrics}); err != nil {
			nlog.Core().Warn("failed to push report", "error", err)
			// The batch stays unacknowledged and will be re-sent
			// verbatim on the next push; do NOT merge it back into
			// the pending accumulator (that was the TA-001 defect).
			if len(aliveIPs) > 0 {
				s.tracker.RestoreAliveIPs(aliveIPs)
			}
			s.pushBackoff.onFailure()
			return
		}
		if batch != nil {
			s.tracker.AckBatch(batch.ID)
		}
		s.pushBackoff.onSuccess()
		nlog.ReportPushed(len(traffic), len(online))
	}()
}

// pushReportSync is used only during shutdown to ensure final data is sent.
func (s *Service) pushReportSync() {
	if !s.sink.SupportsReporting() {
		return
	}
	// Same batch discipline as the async path: an outstanding batch is
	// re-sent verbatim; otherwise flush whatever is still pending.
	batch := s.tracker.Unacked()
	if batch == nil {
		batch = s.tracker.FlushBatch()
		if batch != nil {
			s.tracker.SetUnacked(batch)
		}
	}
	var traffic map[int][2]int64
	var batchMeta *controlplane.TrafficBatchMeta
	if batch != nil {
		traffic = batch.Data
		batchMeta = &controlplane.TrafficBatchMeta{BatchID: batch.ID, CollectedFrom: batch.CollectedFrom, CollectedTo: batch.CollectedTo}
	} else {
		traffic = make(map[int][2]int64)
	}
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()
	metrics := s.buildMetrics(status)
	metrics["kernel_status"] = s.kernel.IsRunning()

	if err := s.sink.Report(controlplane.ReportPayload{Traffic: traffic, TrafficBatch: batchMeta, Alive: aliveIPs, Online: online, CPU: status.CPU, Mem: [2]uint64{status.MemTotal, status.MemUsed}, Swap: [2]uint64{status.SwapTotal, status.SwapUsed}, Disk: [2]uint64{status.DiskTotal, status.DiskUsed}, Metrics: metrics}); err != nil {
		nlog.Core().Warn("failed to push final report", "error", err)
		return
	}
	if batch != nil {
		s.tracker.AckBatch(batch.ID)
	}
}

// buildMetrics aggregates node-level metrics to be reported to the panel.
// This includes active connections, per-core CPU, GC stats, API call stats,
// WebSocket status, and limiter hit counts.
func (s *Service) buildMetrics(status monitor.Status) map[string]interface{} {
	s.metricsMu.RLock()
	lastUsers := s.lastUsers
	wsClient := s.wsClient
	s.metricsMu.RUnlock()

	m := make(map[string]interface{})
	online := s.tracker.CurrentOnline()

	m["uptime"] = status.Uptime
	m["goroutines"] = status.Goroutines

	// Active connections (last measured during tracker.Process()).
	m["active_connections"] = s.tracker.ActiveConnections()
	m["total_connections"] = s.tracker.TotalConnections()
	m["active_users"] = len(online)
	m["total_users"] = len(lastUsers)

	// Speed
	m["inbound_speed"] = s.tracker.InboundSpeed()
	m["outbound_speed"] = s.tracker.OutboundSpeed()

	// Per-core CPU usage (if available).
	if len(status.CPUPerCore) > 0 {
		m["cpu_per_core"] = status.CPUPerCore
	}

	m["load"] = map[string]interface{}{
		"load1":  status.Load1,
		"load5":  status.Load5,
		"load15": status.Load15,
	}

	// Speed Limiter metrics
	m["speed_limiter"] = map[string]interface{}{
		"has_limits":    s.speedTracker.HasLimits(),
		"limited_users": s.speedTracker.LimitedUserCount(),
	}

	// GC metrics.
	m["gc"] = map[string]interface{}{
		"num_gc":        status.NumGC,
		"last_pause_ms": status.LastPauseMS,
	}

	// API metrics.
	api := s.source.Metrics()
	m["api"] = map[string]interface{}{
		"success": api.Success,
		"failure": api.Failure,
	}

	// WebSocket status.
	wsEnabled := wsClient != nil
	wsConnected := wsEnabled && wsClient.IsConnected()
	m["ws"] = map[string]interface{}{
		"enabled":   wsEnabled,
		"connected": wsConnected,
	}

	// Limiter metrics.
	lm := s.limiter.SnapshotMetrics()
	m["limits"] = map[string]interface{}{
		"device_limit_events": lm.DeviceLimitEvents,
		"speed_limited_users": s.speedTracker.LimitedUserCount(),
	}

	return m
}

// computeConfigHash returns a deterministic hash of the node config.
// It uses JSON marshaling to ensure all fields are captured, ensuring that
// any configuration change correctly triggers a kernel reload.
func computeConfigHash(cfg *model.NodeSpec) string {
	if cfg == nil {
		return ""
	}
	h := sha256.New()
	// We marshal the entire config to be safe. Node config updates are low-frequency,
	// so the robustness of capturing all fields outweighs the micro-performance of manual hashing.
	data, _ := json.Marshal(cfg)
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// computeUserHash returns a deterministic hash of the user list for change detection.
// Uses direct byte encoding instead of binary.Write to avoid reflection overhead.
func computeUserHash(users []model.UserSpec) string {
	sorted := make([]model.UserSpec, len(users))
	copy(sorted, users)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	h := sha256.New()
	var buf [8]byte
	for _, u := range sorted {
		binary.LittleEndian.PutUint64(buf[:], uint64(u.ID))
		h.Write(buf[:])
		io.WriteString(h, u.UUID)
		binary.LittleEndian.PutUint64(buf[:], uint64(u.SpeedLimit))
		h.Write(buf[:])
		binary.LittleEndian.PutUint64(buf[:], uint64(u.DeviceLimit))
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ─── Device management ──────────────────────────────────────────────────

// sendDeviceBatch reports local device snapshot to panel via WS.
func (s *Service) sendDeviceBatch() {
	if s.wsClient == nil || !s.wsClient.IsConnected() {
		return
	}

	devices := s.tracker.FlushAliveIPs()
	// FlushAliveIPs returns nil if no changes since last flush
	if devices == nil {
		nlog.Core().Debug("device snapshot unchanged, skipping")
		return
	}
	s.sink.ReportDevices(s.wsClient, devices)
	nlog.Core().Debug("device snapshot sent", "users", len(devices))
}

// reportDevices periodically reports device snapshot to panel.
func (s *Service) reportDevices() {
	s.sendDeviceBatch()
}

// ─── Runtime validation ─────────────────────────────────────────────────

func validateNodeRuntime(cfg *config.Config, kcfgSupported []string, spec *model.NodeSpec, tls kernel.TLSCert) error {
	if spec == nil {
		return fmt.Errorf("node spec is nil")
	}
	if !containsString(kcfgSupported, spec.Protocol) {
		return fmt.Errorf("protocol %q is not supported by kernel %q", spec.Protocol, cfg.Kernel.Type)
	}
	if err := validateTLSRequirements(spec, tls, cfgKernelType(cfg)); err != nil {
		return err
	}
	if err := validateRuntimeCertConfig(spec); err != nil {
		return err
	}
	return nil
}

func validateTLSRequirements(spec *model.NodeSpec, tls kernel.TLSCert, kernelType string) error {
	needsCert := false
	switch spec.Protocol {
	case "hysteria", "hysteria2", "tuic", "anytls":
		needsCert = true
	case "trojan":
		if spec.TLS != 2 {
			needsCert = true
		}
	}
	if needsCert && !hasUsableTLSConfig(spec, tls) {
		return fmt.Errorf("protocol %q requires TLS certificate files", spec.Protocol)
	}
	if spec.TLS == 2 {
		if err := validateRealityRequirements(spec, kernelType); err != nil {
			return err
		}
	}
	return nil
}

func hasUsableTLSConfig(spec *model.NodeSpec, tls kernel.TLSCert) bool {
	if tls.HasCert() {
		return true
	}
	if spec == nil || spec.CertConfig == nil {
		return false
	}
	mode := strings.ToLower(strings.TrimSpace(spec.CertConfig.CertMode))
	switch mode {
	case "self":
		return true
	case "content":
		return strings.TrimSpace(spec.CertConfig.CertContent) != "" && strings.TrimSpace(spec.CertConfig.KeyContent) != ""
	case "file":
		return strings.TrimSpace(spec.CertConfig.CertFile) != "" && strings.TrimSpace(spec.CertConfig.KeyFile) != ""
	case "http":
		return strings.TrimSpace(spec.CertConfig.Domain) != ""
	case "dns":
		return strings.TrimSpace(spec.CertConfig.Domain) != "" && strings.TrimSpace(spec.CertConfig.DNSProvider) != ""
	default:
		return false
	}
}

func validateRuntimeCertConfig(spec *model.NodeSpec) error {
	if spec == nil || spec.CertConfig == nil {
		return nil
	}
	mode := strings.ToLower(strings.TrimSpace(spec.CertConfig.CertMode))
	if mode != "dns" {
		return nil
	}
	provider := strings.TrimSpace(spec.CertConfig.DNSProvider)
	if provider == "" {
		return fmt.Errorf("dns cert mode requires cert_config.dns_provider")
	}
	if _, ok := dnsproviders.Get(provider); !ok {
		return fmt.Errorf("unsupported cert_config.dns_provider %q (supported: %s)", provider, strings.Join(dnsproviders.CanonicalNames(), ", "))
	}
	return nil
}

func validateRealityRequirements(spec *model.NodeSpec, _ string) error {
	if spec.TLSSettings == nil {
		return fmt.Errorf("reality tls requires tls_settings")
	}
	privateKey := strings.TrimSpace(stringValue(spec.TLSSettings["private_key"]))
	serverName := strings.TrimSpace(stringValue(spec.TLSSettings["server_name"]))
	dest := strings.TrimSpace(stringValue(spec.TLSSettings["dest"]))
	if privateKey == "" {
		return fmt.Errorf("reality tls requires tls_settings.private_key")
	}
	if serverName == "" && dest == "" {
		return fmt.Errorf("reality tls requires tls_settings.server_name or tls_settings.dest")
	}
	return nil
}

func cfgKernelType(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(cfg.Kernel.Type))
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func stringValue(v any) string {
	switch value := v.(type) {
	case string:
		return value
	default:
		return ""
	}
}

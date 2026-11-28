// Package machine implements the machine-mode orchestrator that dynamically
// discovers nodes from the panel's machine API and manages their lifecycles.
package machine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/panel"
	"github.com/cedar2025/xboard-node/internal/relay"
	"github.com/cedar2025/xboard-node/internal/service"
)

// nodeHandle tracks a running node service.
type nodeHandle struct {
	cancel  context.CancelFunc
	done    chan struct{}
	mailbox *controlplane.NodeMailbox
}

const (
	initialNodeRetry = time.Second
	maxNodeRetry     = time.Minute
)

type nodeRetry struct {
	delay time.Duration
	after time.Time
}

// Orchestrator manages all nodes bound to a panel machine. It:
//   - discovers nodes via GET /machine/nodes
//   - starts / stops Service instances as nodes are added / removed
//   - maintains a shared WS connection that demuxes events by node_id
//   - reports machine-level load via POST /machine/status
type Orchestrator struct {
	cfg    *config.Config
	client *panel.Client // machine-level client (no node_id)

	mu      sync.Mutex
	nodes   map[int]*nodeHandle // node_id → handle
	retries map[int]nodeRetry

	// Per-node mailbox keyed by node_id. Shared WS events are aggregated here
	// and each node service drains the latest state when ready.
	eventsMu  sync.RWMutex
	mailboxes map[int]*controlplane.NodeMailbox
	statuses  map[int]chan<- controlplane.StatusChange

	// Shared WS client (nil when WS is disabled).
	ws       *panel.WSClient
	wsCancel context.CancelFunc
	wsDone   chan struct{}

	discoveryRequested chan struct{}

	// relayDone is closed when the relay manager's Run returns (after
	// its shutdown flush). NODE-009: Run waits for it so the process
	// cannot exit — or a hot reload start a second manager on the
	// same relay state path — before the final flush lands.
	relayDone chan struct{}

	pullInterval time.Duration
	pushInterval time.Duration
}

// New creates a machine orchestrator from the given config.
func New(cfg *config.Config) *Orchestrator {
	panelCfg := config.PanelConfig{
		URL:       cfg.Panel.URL,
		Token:     cfg.Machine.Token,
		MachineID: cfg.Machine.MachineID,
	}
	return &Orchestrator{
		cfg:                cfg,
		client:             panel.NewClient(panelCfg),
		nodes:              make(map[int]*nodeHandle),
		retries:            make(map[int]nodeRetry),
		mailboxes:          make(map[int]*controlplane.NodeMailbox),
		statuses:           make(map[int]chan<- controlplane.StatusChange),
		discoveryRequested: make(chan struct{}, 1),
	}
}

// Run acquires the generation before starting any machine activity.
func (o *Orchestrator) Run(ctx context.Context) error {
	manager, release, err := o.AcquireGeneration()
	if err != nil {
		return fmt.Errorf("relay cannot acquire traffic state: %w", err)
	}
	defer release()
	return o.RunWithGeneration(ctx, manager)
}

func (o *Orchestrator) AcquireGeneration() (*relay.Manager, func(), error) {
	manager := relay.NewManager(o.client, o.cfg.Machine.MachineID, o.cfg.Kernel.ConfigDir)
	release, err := manager.AcquireGeneration()
	return manager, release, err
}

// RunWithGeneration runs under a generation lease already held by the caller.
func (o *Orchestrator) RunWithGeneration(ctx context.Context, relayMgr *relay.Manager) error {
	nodesResp, err := o.client.GetMachineNodes()
	if err != nil {
		return fmt.Errorf("initial node discovery: %w", err)
	}

	o.applyIntervals(nodesResp.BaseConfig)
	nlog.Core().Info(fmt.Sprintf("machine %d: discovered %d nodes",
		o.cfg.Machine.MachineID, len(nodesResp.Nodes)))

	// Configure the shared client before creating node services.
	o.prepareWS()

	// SB-RELAY-01: the embedded gost relay data plane runs alongside the
	// nodes. It stays completely idle until the panel publishes a relay
	// config for this machine and enables it there.
	o.relayDone = make(chan struct{})
	go func() {
		defer close(o.relayDone)
		relayMgr.RunWithGeneration(ctx)
	}()

	// Start initial nodes.
	for _, n := range nodesResp.Nodes {
		o.startNode(ctx, n)
	}

	// Initial mailboxes must exist before the panel sends the WS baseline.
	o.startWS(ctx)

	discoveryTicker := time.NewTicker(o.pullInterval)
	statusTicker := time.NewTicker(o.pushInterval)
	defer discoveryTicker.Stop()
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			o.stopAll()
			// NODE-009: the relay manager performs its final traffic
			// flush after its loops see the cancelled context (it
			// bounds that flush itself). Wait for it so shutdown and
			// hot reload never race a second manager on the same
			// state file, and the last batches are not abandoned.
			if o.relayDone != nil {
				<-o.relayDone
			}
			return nil

		case <-discoveryTicker.C:
			o.rediscover(ctx)
		case <-o.discoveryRequested:
			o.rediscover(ctx)

		case <-statusTicker.C:
			o.reportMachineStatus()
		}
	}
}

// ─── Node lifecycle ──────────────────────────────────────────────────────

func (o *Orchestrator) startNode(ctx context.Context, mn panel.MachineNode) {
	o.mu.Lock()
	if o.nodes[mn.ID] != nil || ctx.Err() != nil || time.Now().Before(o.retries[mn.ID].after) {
		o.mu.Unlock()
		return
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	mb := controlplane.NewNodeMailbox()
	h := &nodeHandle{cancel: cancel, done: make(chan struct{}), mailbox: mb}
	o.nodes[mn.ID] = h
	o.eventsMu.Lock()
	o.mailboxes[mn.ID] = mb
	o.eventsMu.Unlock()
	o.mu.Unlock()

	ws := o.ws
	nlog.Core().Info(fmt.Sprintf("machine: starting node %d (%s/%s)",
		mn.ID, mn.Type, mn.Name))
	go func() {
		defer cancel()
		err := o.runNode(nodeCtx, mn, mb, ws, func() {
			o.mu.Lock()
			defer o.mu.Unlock()
			if o.nodes[mn.ID] == h {
				delete(o.retries, mn.ID)
			}
		})
		o.finishNode(nodeCtx, mn.ID, h, ws, err)
	}()
}

func (o *Orchestrator) runNode(ctx context.Context, mn panel.MachineNode, mb *controlplane.NodeMailbox, ws *panel.WSClient, ready func()) error {
	nodeCfg := o.cfg.ExpandMachineNode(mn.ID, mn.Type)

	perNodeClient := o.client.ForNode(mn.ID)

	// Pre-fetch node config to detect transport-based kernel requirements.
	// If the transport (e.g. xhttp) is incompatible with the configured kernel
	// (e.g. singbox), auto-switch to the required kernel for this node.
	if cfgSnapshot, err := perNodeClient.GetConfig(); err == nil && cfgSnapshot != nil {
		resolved, err := model.ResolveKernelForTransport(cfgSnapshot.Network, nodeCfg.Kernel.Type)
		if err != nil {
			return err
		}
		if resolved != nodeCfg.Kernel.Type {
			nlog.Core().Info(fmt.Sprintf("machine: auto-switching kernel for node %d (%s→%s, transport=%s)",
				mn.ID, nodeCfg.Kernel.Type, resolved, cfgSnapshot.Network))
			nodeCfg.Kernel.Type = resolved
		}
	}
	// Reset cached ETag so the subsequent GetConfig in Initial() gets a full response.
	perNodeClient.ResetConfigETag()

	var push controlplane.PushClient
	if ws != nil {
		push = &machineNodePush{
			nodeID: mn.ID,
			ws:     ws,
		}
	}

	// The registerFn is called by MachinePanelControlPlane.Initial() to expose
	// the node mailbox + status channel to the Service.
	nodeID := mn.ID
	registerFn := func(st chan<- controlplane.StatusChange) *controlplane.NodeMailbox {
		o.registerNode(nodeID, mb, st)
		return mb
	}

	cp := controlplane.NewMachinePanelControlPlane(perNodeClient, nodeCfg.Kernel, push, registerFn)
	svc := service.NewWithControlPlane(nodeCfg, cp)

	return svc.RunWithReady(ctx, ready)
}

func (o *Orchestrator) finishNode(ctx context.Context, nodeID int, h *nodeHandle, ws *panel.WSClient, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer close(h.done)
	if o.nodes[nodeID] != h {
		return
	}
	o.unregisterNode(nodeID, h.mailbox)
	if ws != nil {
		ws.ForgetDeviceReportForNode(nodeID)
	}
	delete(o.nodes, nodeID)
	if ctx.Err() != nil {
		return
	}
	delay := max(initialNodeRetry, min(o.retries[nodeID].delay*2, maxNodeRetry))
	o.retries[nodeID] = nodeRetry{delay: delay, after: time.Now().Add(delay)}
	nlog.Core().Error("machine node exited, retry deferred until discovery after backoff",
		"node_id", nodeID, "retry_delay", delay, "error", err)
}

func (o *Orchestrator) stopNode(nodeID int) {
	o.mu.Lock()
	h, ok := o.nodes[nodeID]
	delete(o.retries, nodeID)
	if !ok {
		o.mu.Unlock()
		return
	}
	h.cancel()
	o.mu.Unlock()

	nlog.Core().Info(fmt.Sprintf("machine: stopping node %d", nodeID))
	<-h.done
}

func (o *Orchestrator) stopAll() {
	o.mu.Lock()
	handles := make(map[int]*nodeHandle, len(o.nodes))
	for id, h := range o.nodes {
		handles[id] = h
	}
	o.mu.Unlock()

	for id, h := range handles {
		nlog.Core().Info(fmt.Sprintf("machine: stopping node %d", id))
		h.cancel()
	}
	for _, h := range handles {
		<-h.done
	}

	if o.wsCancel != nil {
		o.wsCancel()
		<-o.wsDone
	}
}

// ─── Node discovery ──────────────────────────────────────────────────────

func (o *Orchestrator) rediscover(ctx context.Context) {
	nodesResp, err := o.client.GetMachineNodes()
	if err != nil {
		nlog.Core().Warn("machine node discovery failed", "error", err)
		return
	}

	if ctx.Err() != nil {
		return
	}
	wanted := make(map[int]panel.MachineNode, len(nodesResp.Nodes))
	for _, n := range nodesResp.Nodes {
		wanted[n.ID] = n
	}

	o.mu.Lock()
	for id := range o.retries {
		if _, ok := wanted[id]; !ok {
			delete(o.retries, id)
		}
	}
	var toRemove []int
	for id := range o.nodes {
		if _, ok := wanted[id]; !ok {
			toRemove = append(toRemove, id)
		}
	}
	o.mu.Unlock()

	for _, id := range toRemove {
		o.stopNode(id)
	}

	for _, n := range nodesResp.Nodes {
		o.startNode(ctx, n) // no-op if already running
	}
}

// ─── Machine status reporting ────────────────────────────────────────────

func (o *Orchestrator) reportMachineStatus() {
	s := monitor.Collect()
	if err := o.client.ReportMachineStatus(
		s.CPU,
		[2]uint64{s.MemTotal, s.MemUsed},
		[2]uint64{s.SwapTotal, s.SwapUsed},
		[2]uint64{s.DiskTotal, s.DiskUsed},
		s.NetInSpeed, s.NetOutSpeed,
	); err != nil {
		nlog.Core().Warn("machine status report failed", "error", err)
	}
}

// ─── WS mux ─────────────────────────────────────────────────────────────

func (o *Orchestrator) prepareWS() {
	hs, err := o.client.Handshake()
	if err != nil {
		nlog.Core().Warn("machine ws handshake failed, REST only", "error", err)
		return
	}
	if !hs.WebSocket.Enabled || hs.WebSocket.WSURL == "" {
		nlog.Core().Info("machine: ws disabled by panel, REST only")
		return
	}

	wsCfg := panel.WSClientConfig{
		StatusInterval:   time.Duration(o.cfg.WS.StatusInterval) * time.Second,
		HandshakeTimeout: time.Duration(o.cfg.WS.HandshakeTimeout) * time.Second,
		BackoffInitial:   time.Duration(o.cfg.WS.BackoffInitial) * time.Second,
		BackoffMax:       time.Duration(o.cfg.WS.BackoffMax) * time.Second,
		MachineID:        o.cfg.Machine.MachineID,
	}

	o.ws = panel.NewWSClient(
		hs.WebSocket.WSURL,
		o.cfg.Machine.Token,
		0, // no single node_id
		wsCfg,
		o.onWSEvent,
		o.onWSStatus,
		nil, // per-node status is sent via machineNodePush
	)
}

func (o *Orchestrator) startWS(ctx context.Context) {
	if o.ws == nil {
		return
	}
	wsCtx, wsCancel := context.WithCancel(ctx)
	o.wsCancel = wsCancel
	o.wsDone = make(chan struct{})
	go func() {
		defer close(o.wsDone)
		o.ws.Run(wsCtx)
	}()

	nlog.Core().Info("machine: ws mux started")
}

// onWSEvent routes a WS event to the correct node's channel.
// sync.nodes is a machine-level event that triggers immediate rediscovery.
func (o *Orchestrator) onWSEvent(event panel.WSEvent) {
	// sync.nodes is a machine-level event, not per-node
	if event.Type == panel.WSEventSyncNodes {
		nlog.Core().Info("machine received sync.nodes, triggering immediate rediscovery")
		select {
		case o.discoveryRequested <- struct{}{}:
		default:
		}
		return
	}

	nodeID := event.NodeID
	if nodeID == 0 {
		nlog.Core().Debug("machine ws event missing node_id, dropping", "type", event.Type)
		return
	}

	translated, err := controlplane.TranslateWSEvent(event, o.cfg.Kernel)
	if err != nil {
		nlog.Core().Warn("machine ws event translation failed",
			"type", event.Type, "node_id", nodeID, "error", err)
		return
	}

	o.eventsMu.RLock()
	mailbox, ok := o.mailboxes[nodeID]
	o.eventsMu.RUnlock()
	if !ok {
		nlog.Core().Debug("machine ws event for unknown node", "node_id", nodeID, "type", event.Type)
		return
	}
	mailbox.Apply(translated)
}

// onWSStatus broadcasts WS connectivity changes to all registered nodes.
func (o *Orchestrator) onWSStatus(status panel.WSStatusChange) {
	change := controlplane.StatusChange{Connected: status.Connected}
	o.eventsMu.RLock()
	defer o.eventsMu.RUnlock()
	for _, ch := range o.statuses {
		select {
		case ch <- change:
		default:
		}
	}
}

func (o *Orchestrator) registerNode(nodeID int, mb *controlplane.NodeMailbox, st chan<- controlplane.StatusChange) {
	o.eventsMu.Lock()
	if o.mailboxes[nodeID] == mb {
		o.statuses[nodeID] = st
	}
	o.eventsMu.Unlock()
}

func (o *Orchestrator) unregisterNode(nodeID int, mb *controlplane.NodeMailbox) {
	o.eventsMu.Lock()
	if o.mailboxes[nodeID] == mb {
		delete(o.mailboxes, nodeID)
		delete(o.statuses, nodeID)
	}
	o.eventsMu.Unlock()
}

func (o *Orchestrator) applyIntervals(bc panel.MachineBaseConfig) {
	o.pullInterval = time.Duration(bc.PullInterval) * time.Second
	if o.pullInterval < 30*time.Second {
		o.pullInterval = 60 * time.Second
	}
	o.pushInterval = time.Duration(bc.PushInterval) * time.Second
	if o.pushInterval < 10*time.Second {
		o.pushInterval = 60 * time.Second
	}
}

// ─── Virtual PushClient ─────────────────────────────────────────────────

// machineNodePush implements controlplane.PushClient for a single node
// backed by the shared machine WS connection. Events are routed by the
// WS mux directly to the Service's channels; this adapter only provides
// connectivity status and send capabilities.
type machineNodePush struct {
	nodeID int
	ws     *panel.WSClient
}

func (p *machineNodePush) Run(ctx context.Context) {
	// The shared WS mux pushes events into our channels; we just wait.
	<-ctx.Done()
}

func (p *machineNodePush) IsConnected() bool {
	return p.ws != nil && p.ws.IsConnected()
}

func (p *machineNodePush) SendDeviceReport(devices map[int][]string) {
	if p.ws == nil {
		return
	}
	p.ws.SendDeviceReportForNode(p.nodeID, devices)
}

func (p *machineNodePush) Session() uint64 { return p.ws.Session() }

func (p *machineNodePush) SendDeviceReportForSession(devices map[int][]string, session uint64) {
	p.ws.SendDeviceReportForSession(p.nodeID, devices, session)
}

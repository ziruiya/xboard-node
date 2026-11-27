package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/gorilla/websocket"
)

// WSEvent types
const (
	WSEventSyncConfig    = "sync.config"
	WSEventSyncUsers     = "sync.users"
	WSEventSyncUserDelta = "sync.user.delta"
	WSEventSyncDevices   = "sync.devices"   // panel → node: global device state
	WSEventSyncNodes     = "sync.nodes"     // panel → machine: node list changed
	WSEventReportDevices = "report.devices" // node → panel: report device snapshot
)

// WSEvent is a parsed data event delivered to the service layer.
type WSEvent struct {
	Type        string
	Config      *NodeConfig
	Users       []User
	DeltaAction string // "add" or "remove" (only for sync.user.delta)
	DeltaUsers  []User // users affected by the delta

	// Device sync fields
	DeviceUsers map[int][]string // userID -> IPs (for sync.devices)
	NodeID      int

	// Machine node discovery fields (for sync.nodes)
	Nodes []MachineNode
}

// WSStatusChange notifies the service when WS connectivity changes.
type WSStatusChange struct {
	Connected bool
}

// wsMessage is the JSON envelope for all WS messages.
type wsMessage struct {
	Event     string          `json:"event"`
	Data      json.RawMessage `json:"data,omitempty"`
	Timestamp int64           `json:"timestamp,omitempty"`
}

// Payload structures for data events.
type syncConfigPayload struct {
	Config    NodeConfig `json:"config"`
	Timestamp int64      `json:"timestamp"`
	NodeID    int        `json:"node_id"`
}

type syncUsersPayload struct {
	Users     []User `json:"users"`
	Timestamp int64  `json:"timestamp"`
	NodeID    int    `json:"node_id"`
}

type syncUserDeltaPayload struct {
	Action    string `json:"action"`
	Users     []User `json:"users"`
	Timestamp int64  `json:"timestamp"`
	NodeID    int    `json:"node_id"`
}

// syncDevicesPayload carries global device state from panel.
type syncDevicesPayload struct {
	Users     map[int][]string `json:"users"`
	Timestamp int64            `json:"timestamp"`
	NodeID    int              `json:"node_id"`
}

// syncNodesPayload carries the updated node list for a machine.
type syncNodesPayload struct {
	Nodes []MachineNode `json:"nodes"`
}

// WSClientConfig holds WebSocket client tuning options.
type WSClientConfig struct {
	StatusInterval   time.Duration
	HandshakeTimeout time.Duration
	BackoffInitial   time.Duration
	BackoffMax       time.Duration
	MachineID        int
}

// WSClient connects to the panel's Workerman WS server using native WebSocket.
// Authentication is done via query parameters (token + node_id) during the
// WebSocket handshake — no separate auth step needed.
type WSClient struct {
	wsURL    string // base WS URL, e.g. ws://panel.example.com:8076
	token    string
	nodeID   int
	onEvent  func(WSEvent)
	onStatus func(WSStatusChange)
	onPing   func() map[string]interface{}

	cfg WSClientConfig

	sessionMu sync.Mutex
	session   chan wsMessage // non-nil only for a live session
}

// NewWSClient creates a new WebSocket client.
// wsURL is the base WebSocket URL (e.g. "ws://panel.example.com:8076").
// token and nodeID are used for authentication via query parameters.
func NewWSClient(wsURL string, token string, nodeID int, cfg WSClientConfig, onEvent func(WSEvent), onStatus func(WSStatusChange), onPing func() map[string]interface{}) *WSClient {
	// Apply defaults
	if cfg.StatusInterval == 0 {
		cfg.StatusInterval = 10 * time.Second
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 15 * time.Second
	}
	if cfg.BackoffInitial == 0 {
		cfg.BackoffInitial = time.Second
	}
	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = 60 * time.Second
	}
	return &WSClient{
		wsURL:    wsURL,
		token:    token,
		nodeID:   nodeID,
		cfg:      cfg,
		onEvent:  onEvent,
		onStatus: onStatus,
		onPing:   onPing,
	}
}

func (w *WSClient) IsConnected() bool {
	w.sessionMu.Lock()
	defer w.sessionMu.Unlock()
	return w.session != nil
}

func (w *WSClient) enqueue(msg wsMessage) {
	w.sessionMu.Lock()
	defer w.sessionMu.Unlock()
	if w.session == nil {
		return
	}
	select {
	case w.session <- msg:
	default:
		nlog.Core().Warn("ws write channel full, skipping message")
	}
}

func (w *WSClient) notifyStatus(connected bool) {
	if w.onStatus != nil {
		w.onStatus(WSStatusChange{Connected: connected})
	}
}

// Run connects and reconnects until ctx is cancelled.
func (w *WSClient) Run(ctx context.Context) {
	backoff := w.cfg.BackoffInitial
	for {
		start := time.Now()
		err := w.connect(ctx)

		if err != nil && ctx.Err() == nil {
			nlog.Core().Warn("ws disconnected", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		// Reset backoff if the connection was up for a meaningful duration.
		if time.Since(start) > 2*time.Minute {
			backoff = w.cfg.BackoffInitial
		}

		// Apply exponential backoff with jitter to prevent thundering herd.
		jitter := time.Duration(rand.Int63n(int64(backoff / 5)))
		wait := backoff + jitter

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < w.cfg.BackoffMax {
			backoff = min(backoff*2, w.cfg.BackoffMax)
		}
	}
}

func (w *WSClient) connect(ctx context.Context) error {
	u, err := url.Parse(w.wsURL)
	if err != nil {
		w.notifyStatus(false)
		return fmt.Errorf("parse ws url: %w", withoutRequestURL(err))
	}
	q := u.Query()
	q.Set("token", w.token)
	if w.cfg.MachineID > 0 {
		q.Set("machine_id", strconv.Itoa(w.cfg.MachineID))
	} else {
		q.Set("node_id", strconv.Itoa(w.nodeID))
	}
	u.RawQuery = q.Encode()

	nlog.Core().Debug("ws connecting")

	dialer := websocket.Dialer{
		HandshakeTimeout: w.cfg.HandshakeTimeout,
	}
	conn, resp, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		w.notifyStatus(false)
		if resp != nil {
			defer resp.Body.Close()
			return fmt.Errorf("dial websocket (HTTP %d): %w", resp.StatusCode, withoutRequestURL(err))
		}
		return fmt.Errorf("dial: %w", withoutRequestURL(err))
	}
	return w.serveConnection(ctx, conn, max(90*time.Second, 3*w.cfg.StatusInterval))
}

func (w *WSClient) serveConnection(ctx context.Context, conn *websocket.Conn, readTimeout time.Duration) error {
	defer w.notifyStatus(false)
	defer conn.Close()
	stopCancel := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopCancel()
	if err := conn.SetReadDeadline(time.Now().Add(w.cfg.HandshakeTimeout)); err != nil {
		return fmt.Errorf("set auth read deadline: %w", err)
	}

	conn.SetReadLimit(10 << 20) // 10MB max message size

	// Read first message — expect auth.success or error
	var firstMsg wsMessage
	if err := conn.ReadJSON(&firstMsg); err != nil {
		return fmt.Errorf("read auth response: %w", safeWSReadError(err))
	}
	if firstMsg.Event == "error" {
		return fmt.Errorf("panel rejected websocket authentication")
	}
	refreshRead := func() error { return conn.SetReadDeadline(time.Now().Add(readTimeout)) }
	if err := refreshRead(); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error { return refreshRead() })

	reportTicker := time.NewTicker(w.cfg.StatusInterval)
	defer reportTicker.Stop()

	writeCh := make(chan wsMessage, 16)
	w.sessionMu.Lock()
	w.session = writeCh
	w.sessionMu.Unlock()
	w.notifyStatus(true)
	w.handleMessage(firstMsg)
	if firstMsg.Event == "ping" {
		w.enqueue(wsMessage{Event: "pong"})
	}

	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var msg wsMessage
			if err := conn.ReadJSON(&msg); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if err := refreshRead(); err != nil {
				errCh <- err
				return
			}
			w.handleMessage(msg)
			if msg.Event == "ping" {
				select {
				case writeCh <- wsMessage{Event: "pong"}:
				default:
					nlog.Core().Warn("ws write channel full, skipping pong")
				}
			}
		}
	}()

	defer func() {
		w.sessionMu.Lock()
		w.session = nil
		w.sessionMu.Unlock()
		conn.Close()
		<-done
	}()

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-errCh:
			return fmt.Errorf("read: %w", safeWSReadError(err))

		case <-reportTicker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
			// Send periodic node.status via WebSocket
			if w.onPing != nil {
				msg := wsMessage{Event: "node.status"}
				if stats := w.onPing(); stats != nil {
					data, _ := json.Marshal(stats)
					msg.Data = data
					msg.Timestamp = time.Now().Unix()
				}
				select {
				case writeCh <- msg:
				default:
					nlog.Core().Warn("ws write channel full, skipping status push (network slow?)")
				}
			}

		case msg := <-writeCh:
			// Perform the actual network write asynchronously in this loop.
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteJSON(msg); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
	}
}

func safeWSReadError(err error) error {
	if e, ok := err.(*websocket.CloseError); ok {
		return &websocket.CloseError{Code: e.Code}
	}
	return err
}

func (w *WSClient) handleMessage(msg wsMessage) {
	switch msg.Event {
	case "ping":
		// Server ping — handled by the pong timer reset above
		nlog.Core().Debug("ws received ping")

	case "auth.success":
		nlog.Core().Debug("ws auth confirmed")

	case WSEventSyncConfig:
		w.handleDataEvent(msg)

	case WSEventSyncUsers:
		w.handleDataEvent(msg)

	case WSEventSyncUserDelta:
		w.handleDataEvent(msg)

	case WSEventSyncDevices:
		w.handleDataEvent(msg)

	case WSEventSyncNodes:
		w.handleDataEvent(msg)

	default:
		nlog.Core().Debug("ws unknown event")
	}
}

func (w *WSClient) handleDataEvent(msg wsMessage) {
	var event WSEvent
	event.Type = msg.Event

	// Helper to unmarshal and decode with weak Typing
	decodeData := func(data []byte, target interface{}) error {
		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		if err := decodeWeakRaw(raw, target); err != nil {
			return fmt.Errorf("invalid payload field types (%T)", err)
		}
		return nil
	}

	switch msg.Event {
	case WSEventSyncConfig:
		nlog.Core().Debug("ws sync config event received")
		var p syncConfigPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode config payload", "error", err)
			return
		}
		if p.Config.Protocol == "" {
			nlog.Core().Warn("ws: config payload missing protocol")
			return
		}
		event.Config = &p.Config
		if p.NodeID > 0 {
			event.NodeID = p.NodeID
		} else if p.Config.NodeID > 0 {
			event.NodeID = p.Config.NodeID
		}

	case WSEventSyncUsers:
		nlog.Core().Debug("ws sync users event received")
		// NODE-010: an explicit "users": [] is a valid full snapshot —
		// it means nobody is authorized any more and the local set
		// must be cleared. Only a MISSING (or null) users field is a
		// malformed payload. The old len==0 check dropped the empty
		// snapshot, so while WS stayed connected (suppressing REST
		// reconciliation) previously loaded credentials kept working
		// indefinitely.
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(msg.Data, &probe); err != nil {
			nlog.Core().Warn("ws: cannot decode users payload", "error", err)
			return
		}
		rawUsers, present := probe["users"]
		if !present || string(rawUsers) == "null" {
			nlog.Core().Warn("ws: users payload missing users field")
			return
		}
		var p syncUsersPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode users payload", "error", err)
			return
		}
		if p.Users == nil {
			p.Users = []User{}
		}
		event.Users = p.Users
		event.NodeID = p.NodeID

	case WSEventSyncUserDelta:
		nlog.Core().Debug("ws sync user delta event received")
		var p syncUserDeltaPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode user delta payload", "error", err)
			return
		}
		if p.Action == "" {
			nlog.Core().Warn("ws: user delta payload missing action")
			return
		}
		if len(p.Users) == 0 {
			nlog.Core().Warn("ws: user delta payload has no users")
			return
		}
		event.DeltaAction = p.Action
		event.DeltaUsers = p.Users
		event.NodeID = p.NodeID

	case WSEventSyncDevices:
		nlog.Core().Debug("ws sync devices event received")
		var p syncDevicesPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode devices payload", "error", err)
			return
		}
		event.DeviceUsers = p.Users
		event.NodeID = p.NodeID

	case WSEventSyncNodes:
		nlog.Core().Info("ws sync nodes event received (machine node list changed)")
		var p syncNodesPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode nodes payload", "error", err)
			return
		}
		event.Nodes = p.Nodes
	}

	w.onEvent(event)
}

// SendDeviceReport sends local device snapshot to panel via WS.
func (w *WSClient) SendDeviceReport(devices map[int][]string) {
	w.SendDeviceReportForNode(0, devices)
}

// SendDeviceReportForNode sends a device report tagged with a specific node_id.
// nodeID == 0 omits the field (legacy single-node mode).
func (w *WSClient) SendDeviceReportForNode(nodeID int, devices map[int][]string) {
	if !w.IsConnected() {
		return
	}

	var payload interface{}
	if nodeID > 0 {
		payload = map[string]interface{}{"node_id": nodeID, "devices": devices}
	} else {
		payload = devices
	}

	d, err := json.Marshal(payload)
	if err != nil {
		return
	}
	msg := wsMessage{
		Event:     WSEventReportDevices,
		Data:      d,
		Timestamp: time.Now().Unix(),
	}

	w.enqueue(msg)
}

// SendNodeStatus sends a node.status event with the given node_id (machine mode).
func (w *WSClient) SendNodeStatus(nodeID int, stats map[string]interface{}) {
	if !w.IsConnected() || stats == nil {
		return
	}
	if nodeID > 0 {
		stats["node_id"] = nodeID
	}
	data, _ := json.Marshal(stats)
	msg := wsMessage{
		Event:     "node.status",
		Data:      data,
		Timestamp: time.Now().Unix(),
	}
	w.enqueue(msg)
}

// SendRaw sends a raw message to the panel via WebSocket.
func (w *WSClient) SendRaw(event string, data json.RawMessage) {
	if !w.IsConnected() {
		return
	}

	msg := wsMessage{
		Event:     event,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}

	w.enqueue(msg)
}

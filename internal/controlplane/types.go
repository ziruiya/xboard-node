package controlplane

import (
	"context"

	"github.com/cedar2025/xboard-node/internal/model"
)

type EventType string

const (
	EventSyncConfig    EventType = "sync.config"
	EventSyncUsers     EventType = "sync.users"
	EventSyncUserDelta EventType = "sync.user.delta"
	EventSyncDevices   EventType = "sync.devices"
)

type Event struct {
	Type        EventType
	Config      *model.NodeSpec
	Users       []model.UserSpec
	DeltaAction string
	DeltaUsers  []model.UserSpec
	DeviceUsers map[int][]string
}

type StatusChange struct {
	Connected   bool
	NeedsResync bool
}

type APIMetrics struct {
	Success uint64
	Failure uint64
}

type Bootstrap struct {
	PushInterval int
	PullInterval int
	Push         PushClient
	Config       *model.NodeSpec
	Users        []model.UserSpec
	Mailbox      *NodeMailbox
}

type fullPollKey struct{}

// FullPoll requests an unconditional snapshot for reconciliation.
func FullPoll(ctx context.Context) context.Context {
	return context.WithValue(ctx, fullPollKey{}, true)
}

type Snapshot struct {
	// Commit advances validators for the resources accepted by the service loop.
	Commit func(configApplied, usersApplied bool)
	Config *model.NodeSpec
	Users  []model.UserSpec
}

// TrafficBatchMeta is the report envelope of the persistent batch-id
// protocol (SB-TA-001/002/004). Nil for reports without traffic.
type TrafficBatchMeta struct {
	BatchID       string
	CollectedFrom int64
	CollectedTo   int64
}

type ReportPayload struct {
	Traffic map[int][2]int64
	// TrafficBatch describes Traffic: the batch id and the collection
	// window. Old panels ignore the envelope and keep using the
	// content-hash fallback on Traffic itself.
	TrafficBatch *TrafficBatchMeta
	Alive        map[int][]string
	Online       map[int]int
	CPU          float64
	Mem          [2]uint64
	Swap         [2]uint64
	Disk         [2]uint64
	Metrics      map[string]interface{}
}

type PushClient interface {
	Run(ctx context.Context)
	IsConnected() bool
	SendDeviceReport(devices map[int][]string)
}

type Source interface {
	Initial(ctx context.Context, metricsFn func() map[string]interface{}, events chan<- Event, statuses chan<- StatusChange) (Bootstrap, error)
	Poll(ctx context.Context) (Snapshot, error)
	Discover(ctx context.Context, metricsFn func() map[string]interface{}, events chan<- Event, statuses chan<- StatusChange) (PushClient, error)
	Metrics() APIMetrics
	SupportsPolling() bool
	SupportsDiscovery() bool
}

type Sink interface {
	Report(payload ReportPayload) error
	ReportDevices(push PushClient, devices map[int][]string)
	SupportsReporting() bool
	SupportsDeviceReports() bool
}

type ControlPlane interface {
	Source
	Sink
}

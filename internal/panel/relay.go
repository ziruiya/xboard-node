package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// SB-RELAY-01 agent channel (relay.config.v1).
//
// The panel side lives in suzuboard's RelayAgentController under
// POST /api/v2/server/machine/relay/{config,ack,traffic}, authenticated
// with the same machine_id + token pair as the other machine endpoints.
// Field names mirror App\Services\Relay\RelayConfigService /
// RelayTrafficService verbatim — do not rename.

// RelayRule is one rule entry inside a relay.config.v1 document.
type RelayRule struct {
	RuleCode          string   `json:"rule_code"`
	Name              string   `json:"name"`
	Transport         string   `json:"transport"`
	ListenIP          string   `json:"listen_ip"`
	ListenPort        int      `json:"listen_port"`
	TargetType        string   `json:"target_type"` // NODE | MANUAL
	TargetNodeID      *int     `json:"target_node_id"`
	TargetHost        string   `json:"target_host"`
	TargetPort        int      `json:"target_port"`
	TargetResolveMode string   `json:"target_resolve_mode"`
	AllowlistCIDRs    []string `json:"allowlist_cidrs"`
	MaxConnections    int      `json:"max_connections"`
	UDPMaxSessions    int      `json:"udp_max_sessions"`
	UDPIdleTimeoutSec int      `json:"udp_idle_timeout_sec"`
	DrainTimeoutSec   int      `json:"drain_timeout_sec"`
	BufferBytes       int      `json:"buffer_bytes"`
	RateInBps         *int64   `json:"rate_in_bps"`
	RateOutBps        *int64   `json:"rate_out_bps"`
	BurstBytes        *int64   `json:"burst_bytes"`
}

// RelayDocument is the full config document the panel publishes per
// version (Outbox pattern: monotonically increasing version, whole
// document every time, rollback = old content at a new higher version).
type RelayDocument struct {
	Protocol      string      `json:"protocol"`
	MachineID     int         `json:"machine_id"`
	RelayEnabled  bool        `json:"relay_enabled"`
	MachineStatus string      `json:"machine_status"` // ENABLED | DRAINING | DISABLED
	GeneratedAt   string      `json:"generated_at"`
	Version       int         `json:"version"`
	Rules         []RelayRule `json:"rules"`
}

// RelayConfigResponse is the relay/config endpoint payload. Config is
// null (and Version 0) when the panel has never published for this
// machine.
type RelayConfigResponse struct {
	Protocol  string         `json:"protocol"`
	MachineID int            `json:"machine_id"`
	Version   int            `json:"version"`
	Hash      *string        `json:"hash"`
	Config    *RelayDocument `json:"config"`
}

// RelayTrafficBatch is one immutable traffic batch. The panel derives
// batch_id = "{machine_id}:{rule_id}:{boot_id}:{seq}" and books each id
// exactly once, so a resent batch must keep its boot_id and seq.
type RelayTrafficBatch struct {
	RuleCode    string `json:"rule_code"`
	BootID      string `json:"boot_id"`
	Seq         int64  `json:"seq"`
	PeriodStart int64  `json:"period_start"`
	PeriodEnd   int64  `json:"period_end"`
	BytesIn     int64  `json:"bytes_in"`
	BytesOut    int64  `json:"bytes_out"`
	Connections int64  `json:"connections"`
}

// RelayRejectedBatch names one batch the panel permanently refused
// (SB-ER-014, panel half): the agent drops exactly these batches from
// its pending queue and keeps everything else for resend.
type RelayRejectedBatch struct {
	RuleCode string `json:"rule_code"`
	BootID   string `json:"boot_id"`
	Seq      int64  `json:"seq"`
	Reason   string `json:"reason"` // unknown_rule | invalid_batch
}

// RelayTrafficResult mirrors RelayTrafficService::ingest's return.
type RelayTrafficResult struct {
	Accepted        int                  `json:"accepted"`
	Duplicates      int                  `json:"duplicates"`
	Rejected        int                  `json:"rejected"`
	RejectedBatches []RelayRejectedBatch `json:"rejected_batches"`
}

// FetchRelayConfig pulls the machine's current published relay config.
func (c *Client) FetchRelayConfigContext(ctx context.Context) (*RelayConfigResponse, error) {
	resp, err := c.doRequestContext(ctx, "POST", "/api/v2/server/machine/relay/config", nil, "")
	if err != nil {
		return nil, fmt.Errorf("relay config: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay config status %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	var out RelayConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode relay config: %w", err)
	}
	return &out, nil
}

// PostRelayAck persists the apply result for one config version. The
// panel rejects unknown versions (422) and hash mismatches (applied is
// recorded as false with the machine's relay_last_error set).
func (c *Client) PostRelayAckContext(ctx context.Context, version int, hash string, applied bool, errText string) error {
	payload := map[string]interface{}{
		"version": version,
		"applied": applied,
	}
	if hash != "" {
		payload["hash"] = hash
	}
	if errText != "" {
		payload["error"] = errText
	}
	return c.postJSONContext(ctx, "/api/v2/server/machine/relay/ack", payload)
}

// PostRelayTraffic submits traffic batches (max 500 per call, enforced
// panel-side) and returns the ingest counters.
func (c *Client) PostRelayTrafficContext(ctx context.Context, batches []RelayTrafficBatch) (*RelayTrafficResult, error) {
	if len(batches) == 0 {
		return &RelayTrafficResult{}, nil
	}
	payload := map[string]interface{}{"batches": batches}
	c.injectAuth(payload)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal relay traffic: %w", err)
	}
	resp, err := c.doRequestContext(ctx, "POST", "/api/v2/server/machine/relay/traffic", body, "")
	if err != nil {
		return nil, fmt.Errorf("relay traffic: %w", err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay traffic status %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	var envelope struct {
		Status string             `json:"status"`
		Data   RelayTrafficResult `json:"data"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read relay traffic result: %w", err)
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		// Tolerate a bare result object without the envelope.
		var bare RelayTrafficResult
		if err2 := json.Unmarshal(raw, &bare); err2 != nil {
			return nil, fmt.Errorf("decode relay traffic result: %w", err)
		}
		return &bare, nil
	}
	return &envelope.Data, nil
}

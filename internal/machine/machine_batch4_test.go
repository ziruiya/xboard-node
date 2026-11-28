package machine

// NODE-009 regression test: Orchestrator.Run must not return on
// shutdown until the embedded relay manager has finished (its final
// traffic flush included) — otherwise the process can exit mid-flush
// or a hot reload can start a second manager on the same state file.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestRunWaitsForRelayManagerOnShutdown(t *testing.T) {
	mux := http.NewServeMux()
	relayStarted := make(chan struct{}, 1)
	mux.HandleFunc("/api/v2/server/machine/nodes", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"nodes":       []any{},
			"base_config": map[string]any{"push_interval": 60, "pull_interval": 60},
		})
	})
	mux.HandleFunc("/api/v2/server/machine/relay/config", func(w http.ResponseWriter, r *http.Request) {
		select {
		case relayStarted <- struct{}{}:
		default:
		}
		http.NotFound(w, r)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := &config.Config{}
	cfg.Panel.URL = srv.URL
	cfg.Machine = &config.MachineConfig{MachineID: 99, Token: "tok"}
	cfg.Kernel.ConfigDir = t.TempDir()

	o := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	select {
	case <-relayStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("relay manager was not started")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// The instant Run returns, the relay manager must already be
	// fully stopped — Run waited for it rather than abandoning it.
	select {
	case <-o.relayDone:
	default:
		t.Fatal("Run returned before the relay manager finished its shutdown flush")
	}
}

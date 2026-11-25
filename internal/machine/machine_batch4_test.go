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
	mux.HandleFunc("/api/v2/server/machine/nodes", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"nodes":       []any{},
			"base_config": map[string]any{"push_interval": 60, "pull_interval": 60},
		})
	})
	// Everything else (handshake, relay config, ...) 404s: the
	// orchestrator degrades to REST-only and the relay manager idles.
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := &config.Config{}
	cfg.Panel.URL = srv.URL
	cfg.Machine = &config.MachineConfig{MachineID: 99, Token: "tok"}
	cfg.Kernel.ConfigDir = t.TempDir()

	o := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	// Let discovery + relay startup complete.
	time.Sleep(300 * time.Millisecond)
	if o.relayDone == nil {
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

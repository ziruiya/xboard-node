package relay

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/panel"
)

type batch1aACKObserver struct {
	*batch5Panel
	onApplied func()
}

func (p batch1aACKObserver) PostRelayAck(version int, hash string, applied bool, errText string) error {
	if applied {
		p.onApplied()
	}
	return p.batch5Panel.PostRelayAck(version, hash, applied, errText)
}

func TestBatch1AOrdinaryRelayRemovalCutsFlowAndPersistsBeforeACK(t *testing.T) {
	client := &batch5Panel{doc: &panel.RelayDocument{Version: 2, RelayEnabled: true}}
	m := batch5Manager(t.TempDir(), client)
	var persistedAtACK []byte
	var readErr error
	m.client = batch1aACKObserver{batch5Panel: client, onApplied: func() {
		persistedAtACK, readErr = os.ReadFile(m.statePath())
	}}
	conn := batch5ForwardingFlow(t, m, "removed", 1)
	batch5Echo(t, conn)

	done := make(chan struct{})
	go func() { defer close(done); m.syncOnce(context.Background()) }()
	select {
	case <-done:
		t.Fatal("rule removal completed before the connection drained")
	case <-time.After(100 * time.Millisecond):
	}
	if err := batch5ReadUntilClosed(conn, 2500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("rule removal did not commit after the deadline")
	}
	if len(client.acks) != 1 || !client.acks[0].applied {
		t.Fatalf("expected a successful ACK after cutoff: %+v", client.acks)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	var disk struct{ Pending []panel.RelayTrafficBatch }
	if err := json.Unmarshal(persistedAtACK, &disk); err != nil {
		t.Fatal(err)
	}
	if len(disk.Pending) != 1 || disk.Pending[0].RuleCode != "removed" || disk.Pending[0].BytesIn != 1 || disk.Pending[0].BytesOut != 1 {
		t.Fatalf("final accounting was missing at ACK: %+v", disk.Pending)
	}
}

func TestBatch1ARelayPersistenceFailureDefersSuccessfulACK(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	if err := os.WriteFile(stateDir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	client := &batch5Panel{doc: &panel.RelayDocument{Version: 2, RelayEnabled: true}}
	m := batch5Manager(stateDir, client)
	m.state.Banked["removed"] = counters{In: 12}
	m.syncOnce(context.Background())
	if len(client.acks) != 1 || client.acks[0].applied || m.appliedVersion == 2 {
		t.Fatalf("persistence failure advanced applied state: %v, %+v", m.appliedVersion, client.acks)
	}
	if err := os.Remove(stateDir); err != nil {
		t.Fatal(err)
	}
	m.syncOnce(context.Background())
	if len(client.acks) != 2 || !client.acks[1].applied {
		t.Fatalf("persistence retry did not permit ACK: %+v", client.acks)
	}
	raw, err := os.ReadFile(m.statePath())
	if err != nil {
		t.Fatal(err)
	}
	var disk struct{ Pending []panel.RelayTrafficBatch }
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	if len(disk.Pending) != 1 || disk.Pending[0].BytesIn != 12 {
		t.Fatalf("lost or duplicated final batch on persistence retry: %+v", disk.Pending)
	}
}

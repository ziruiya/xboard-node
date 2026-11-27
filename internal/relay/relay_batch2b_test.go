package relay

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/panel"
)

type batch2bRelayPanel struct {
	batch5Panel
	started   chan []panel.RelayTrafficBatch
	cancelled chan error
}

func (*batch2bRelayPanel) FetchRelayConfigContext(context.Context) (*panel.RelayConfigResponse, error) {
	return &panel.RelayConfigResponse{}, nil
}

func (p *batch2bRelayPanel) PostRelayTrafficContext(ctx context.Context, batches []panel.RelayTrafficBatch) (*panel.RelayTrafficResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	p.started <- batches
	<-ctx.Done()
	p.cancelled <- ctx.Err()
	// A response may arrive concurrently with cancellation. A cancelled
	// generation must keep this batch for the next manager to retry.
	return &panel.RelayTrafficResult{Accepted: len(batches)}, nil
}

func TestBatch2BRelayHandoffCancelsOldWriterAndPreservesNextGeneration(t *testing.T) {
	dir := t.TempDir()
	seed := NewManager(nil, 1, dir)
	seed.loadState()
	seed.state.Banked["rule"] = counters{In: 10}
	seed.collectTrafficLocked()
	if err := seed.saveStateLocked(); err != nil {
		t.Fatal(err)
	}
	initial := seed.state.Pending[0]

	panelClient := &batch2bRelayPanel{started: make(chan []panel.RelayTrafficBatch, 1), cancelled: make(chan error, 1)}
	old := NewManager(nil, 1, dir)
	old.client = panelClient
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); old.Run(ctx) }()
	select {
	case sent := <-panelClient.started:
		if !reflect.DeepEqual(sent, []panel.RelayTrafficBatch{initial}) {
			t.Fatalf("recovered relay batch changed: %v", sent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("periodic reporter did not send recovered batch")
	}

	next := NewManager(nil, 1, dir)
	if release, err := next.acquireStateGeneration(); err == nil {
		release()
		t.Fatal("a new generation acquired state while the old writer was in flight")
	}
	old.mu.Lock()
	old.state.Banked["rule"] = counters{In: 17}
	old.mu.Unlock()
	cancel()
	select {
	case err := <-panelClient.cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old relay request did not observe shutdown cancellation")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("old generation did not finish shutdown")
	}
	release, err := next.acquireStateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	next.loadState()
	if len(next.state.Pending) != 2 || next.state.Pending[0] != initial ||
		next.state.Pending[1].BytesIn != 7 || next.state.Pending[1].BootID != old.bootID {
		t.Fatalf("shutdown state = %+v, want unchanged original plus final old-generation traffic", next.state.Pending)
	}
	next.state.Banked["rule"] = counters{In: 29}
	next.collectTrafficLocked()
	if err := next.saveStateLocked(); err != nil {
		t.Fatal(err)
	}
	if len(next.state.Pending) != 3 {
		t.Fatalf("new generation did not add a batch: %+v", next.state.Pending)
	}
	if want := next.state.Pending[2]; want.BytesIn != 12 || want.BootID != next.bootID || want.Seq <= initial.Seq {
		t.Fatalf("wrong new-generation batch: %+v", want)
	}
	if old.settleChunk([]panel.RelayTrafficBatch{initial}, &panel.RelayTrafficResult{Accepted: 1}) {
		t.Fatal("retired generation accepted a late traffic receipt")
	}
	old.mu.Lock()
	err = old.saveStateLocked()
	old.mu.Unlock()
	if !errors.Is(err, errStateGenerationReleased) {
		t.Fatalf("late write returned %v, want revoked generation", err)
	}
	read := NewManager(nil, 1, dir)
	read.loadState()
	if !reflect.DeepEqual(read.state.Pending, next.state.Pending) {
		t.Fatalf("late write changed the new generation's pending batches: %+v", read.state.Pending)
	}
}

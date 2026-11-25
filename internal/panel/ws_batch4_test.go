package panel

// NODE-010 regression test: an explicit empty users array over WS is
// a valid full snapshot (nobody is authorized any more) and must be
// delivered, not dropped as a malformed payload.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWSClient_EmptyUsersSnapshotIsDelivered(t *testing.T) {
	usersData, _ := json.Marshal(map[string]any{
		"users":     []User{},
		"node_id":   7,
		"timestamp": 2000,
	})
	events := []wsMessage{{Event: WSEventSyncUsers, Data: usersData}}
	server := fakeWSServer(t, events)
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	var mu sync.Mutex
	var received []WSEvent
	ws := NewWSClient("ws://"+host, "test-token", 1, WSClientConfig{}, func(event WSEvent) {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
	}, nil, func() map[string]interface{} { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go ws.Run(ctx)

	deadline := time.After(1500 * time.Millisecond)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out: empty users snapshot was dropped instead of delivered")
		case <-time.After(50 * time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()
	ev := received[0]
	if ev.Type != WSEventSyncUsers {
		t.Fatalf("event type = %q, want %q", ev.Type, WSEventSyncUsers)
	}
	if ev.Users == nil {
		t.Fatal("event Users is nil; an explicit empty snapshot must stay distinguishable from 'no snapshot'")
	}
	if len(ev.Users) != 0 {
		t.Fatalf("event Users = %v, want empty", ev.Users)
	}
	if ev.NodeID != 7 {
		t.Fatalf("event NodeID = %d, want 7", ev.NodeID)
	}
}

func TestWSClient_MissingUsersFieldIsDropped(t *testing.T) {
	usersData, _ := json.Marshal(map[string]any{
		"node_id":   7,
		"timestamp": 2000,
	})
	events := []wsMessage{{Event: WSEventSyncUsers, Data: usersData}}
	server := fakeWSServer(t, events)
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	var mu sync.Mutex
	var received []WSEvent
	ws := NewWSClient("ws://"+host, "test-token", 1, WSClientConfig{}, func(event WSEvent) {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
	}, nil, func() map[string]interface{} { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	go ws.Run(ctx)

	// Give the client ample time to (not) deliver the malformed event.
	time.Sleep(900 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 0 {
		t.Fatalf("payload without a users field produced %d events, want 0", len(received))
	}
}

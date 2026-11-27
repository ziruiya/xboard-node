package machine

import (
	"sync"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func TestBatch1AWSDiscoveryCoalescesOnMainLoop(t *testing.T) {
	o := New(&config.Config{Machine: &config.MachineConfig{}})
	var callers sync.WaitGroup
	for range 16 {
		callers.Go(func() { o.onWSEvent(panel.WSEvent{Type: panel.WSEventSyncNodes}) })
	}
	callers.Wait()
	select {
	case <-o.discoveryRequested:
	default:
		t.Fatal("WS discovery did not reach the main loop")
	}
	select {
	case <-o.discoveryRequested:
		t.Fatal("duplicate discovery requests were not coalesced")
	default:
	}
	o.onWSEvent(panel.WSEvent{Type: panel.WSEventSyncNodes})
	select {
	case <-o.discoveryRequested:
	default:
		t.Fatal("a later WS discovery was lost")
	}
}

func TestBatch1AOldNodeCannotUnregisterReplacement(t *testing.T) {
	o := New(&config.Config{Machine: &config.MachineConfig{}})
	const id = 11
	old, replacement := controlplane.NewNodeMailbox(), controlplane.NewNodeMailbox()
	notifications := make(chan controlplane.StatusChange, 1)
	o.mailboxes[id] = replacement
	var callers sync.WaitGroup
	callers.Go(func() { o.registerNode(id, replacement, notifications) })
	callers.Go(func() {
		o.registerNode(id, old, make(chan controlplane.StatusChange))
		o.unregisterNode(id, old)
	})
	callers.Wait()
	o.onWSStatus(panel.WSStatusChange{Connected: true})
	select {
	case change := <-notifications:
		if !change.Connected || o.mailboxes[id] != replacement {
			t.Fatal("replacement registration was modified")
		}
	default:
		t.Fatal("replacement lost its status registration")
	}
	o.unregisterNode(id, replacement)
	if o.mailboxes[id] != nil || o.statuses[id] != nil {
		t.Fatal("current generation was not unregistered")
	}
}

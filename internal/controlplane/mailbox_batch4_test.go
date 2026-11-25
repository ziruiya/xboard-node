package controlplane

// NODE-011 regression tests: a REST snapshot must move the mailbox
// baseline, or later WS deltas resurrect users REST already removed.

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func mailboxUserIDs(users []model.UserSpec) []int {
	ids := make([]int, 0, len(users))
	for _, u := range users {
		ids = append(ids, u.ID)
	}
	return ids
}

func TestSyncBaselinePreventsResurrection(t *testing.T) {
	mb := NewNodeMailbox()
	mb.SeedBaseline([]model.UserSpec{{ID: 1, UUID: "a"}, {ID: 2, UUID: "b"}}, nil)

	// REST reconciliation removes user 2 and the service feeds the
	// authoritative snapshot back into the mailbox baseline.
	mb.SyncBaseline([]model.UserSpec{{ID: 1, UUID: "a"}}, nil)

	// A later WS delta adds user 3; the drained full set must be
	// {1, 3} — user 2 must NOT come back from the stale baseline.
	mb.Apply(Event{Type: EventSyncUserDelta, DeltaAction: "add", DeltaUsers: []model.UserSpec{{ID: 3, UUID: "c"}}})
	mb.MarkReady()
	state := mb.DrainIfReady()
	if !state.HasUsers {
		t.Fatal("drain returned no users")
	}
	ids := mailboxUserIDs(state.Users)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("drained user IDs = %v, want [1 3] (user 2 stays removed)", ids)
	}
}

func TestSyncBaselineDefersWhileDeltaPending(t *testing.T) {
	mb := NewNodeMailbox()
	mb.SeedBaseline([]model.UserSpec{{ID: 1, UUID: "a"}}, nil)

	// A WS delta lands first and has not drained yet; a REST snapshot
	// racing it must not clobber the newer delta. Instead the drain
	// asks for a reconcile so the next snapshot re-orders the sources.
	mb.Apply(Event{Type: EventSyncUserDelta, DeltaAction: "add", DeltaUsers: []model.UserSpec{{ID: 2, UUID: "b"}}})
	mb.SyncBaseline([]model.UserSpec{{ID: 9, UUID: "z"}}, nil)

	mb.MarkReady()
	state := mb.DrainIfReady()
	if !state.NeedsReconcile {
		t.Fatal("drain must request a reconcile when a baseline sync was deferred")
	}
	ids := mailboxUserIDs(state.Users)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("drained user IDs = %v, want [1 2] (pending delta preserved)", ids)
	}
}

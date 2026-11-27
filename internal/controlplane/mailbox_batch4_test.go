package controlplane

// NODE-011 regression tests: a REST snapshot must move the mailbox
// baseline, or later WS deltas resurrect users REST already removed.

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestSyncBaselinePreservesExplicitEmptySnapshot(t *testing.T) {
	mb := NewNodeMailbox()
	mb.SeedBaseline([]model.UserSpec{{ID: 1, UUID: "a"}}, nil)
	mb.SyncBaseline(nil, nil)
	if len(mb.users) != 1 {
		t.Fatalf("missing snapshot cleared baseline: %v", mb.users)
	}
	mb.SyncBaseline([]model.UserSpec{}, nil)
	if !mb.hasFullUserState || mb.users == nil || len(mb.users) != 0 {
		t.Fatalf("empty snapshot did not replace baseline: %v", mb.users)
	}
	mb.MarkReady()
	mb.Apply(Event{Type: EventSyncUserDelta, DeltaAction: "add", DeltaUsers: []model.UserSpec{{ID: 2, UUID: "b"}}})
	state := mb.DrainIfReady()
	if state.HasUsers || !state.NeedsReconcile || len(mb.users) != 0 {
		t.Fatalf("unversioned delta altered cleared baseline: %+v", state)
	}
}

func TestSyncBaselineDefersWhileFullSnapshotPending(t *testing.T) {
	mb := NewNodeMailbox()
	mb.MarkReady()
	mb.Apply(Event{Type: EventSyncUsers, Users: []model.UserSpec{{ID: 2, UUID: "b"}}})
	mb.SyncBaseline([]model.UserSpec{}, nil)
	state := mb.DrainIfReady()
	if !state.NeedsReconcile || !state.HasUsers || len(state.Users) != 1 || state.Users[0].ID != 2 {
		t.Fatalf("pending full set lost: %+v", state)
	}
}

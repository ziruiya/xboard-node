package service

// Batch-4 regression tests: NODE-012 (first user on an empty-started
// node) and the service half of NODE-011 (REST snapshots feed the
// mailbox baseline).

import (
	"context"
	"errors"
	"testing"

	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
)

// NODE-012: a node that started with zero users must start its kernel
// when the first user arrives via a full user update.
func TestApplyUserUpdateStartsKernelWithFirstUser(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless", ServerPort: 443}
	s.updateUserState(nil) // started empty

	users := []model.UserSpec{{ID: 1, UUID: "uuid-first"}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users))

	if k.startCalls != 1 {
		t.Fatalf("startCalls = %d, want 1 (first user must start the kernel)", k.startCalls)
	}
	if !k.running {
		t.Fatal("kernel not running after first user update")
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].UUID != "uuid-first" {
		t.Fatalf("lastUsers = %+v, want the first user applied", s.lastUsers)
	}
}

// NODE-012: same for a delta add — the merged candidate set decides,
// not the stale (empty) applied set.
func TestApplyUserDeltaAddStartsKernelWithFirstUser(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless", ServerPort: 443}
	s.updateUserState(nil)

	s.applyUserDelta(context.Background(), "add", []model.UserSpec{{ID: 5, UUID: "uuid-delta"}})

	if k.startCalls != 1 {
		t.Fatalf("startCalls = %d, want 1 (first delta user must start the kernel)", k.startCalls)
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].ID != 5 {
		t.Fatalf("lastUsers = %+v, want merged set with user 5", s.lastUsers)
	}
}

// NODE-012 boundary: an empty update on a stopped kernel records
// state but must not start anything.
func TestApplyUserUpdateEmptyStaysStopped(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless", ServerPort: 443}

	s.applyUserUpdate(context.Background(), []model.UserSpec{}, "")

	if k.startCalls != 0 {
		t.Fatalf("startCalls = %d, want 0 for an empty user set", k.startCalls)
	}
	if len(s.lastUsers) != 0 {
		t.Fatalf("lastUsers = %+v, want empty", s.lastUsers)
	}
}

// NODE-012 failure path: a failed start restores the previous state
// instead of claiming users that are not running.
func TestApplyUserUpdateStartFailureRestoresState(t *testing.T) {
	k := &fakeKernel{running: false, startErr: errors.New("boom")}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless", ServerPort: 443}
	s.updateUserState(nil)

	users := []model.UserSpec{{ID: 1, UUID: "uuid-first"}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users))

	if k.startCalls != 1 {
		t.Fatalf("startCalls = %d, want 1 attempt", k.startCalls)
	}
	if len(s.lastUsers) != 0 {
		t.Fatalf("lastUsers = %+v, want restored to empty after failed start", s.lastUsers)
	}
}

// NODE-011 (service wiring): a REST pull that removes a user must
// move the mailbox baseline, so a later WS delta cannot resurrect
// the removed user.
func TestApplyPullResultSyncsMailboxBaseline(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)

	mb := controlplane.NewNodeMailbox()
	mb.SeedBaseline([]model.UserSpec{{ID: 1, UUID: "a"}, {ID: 2, UUID: "b"}}, nil)
	s.machineMailbox = mb

	// REST snapshot: user 2 is gone at the panel.
	restUsers := []model.UserSpec{{ID: 1, UUID: "a"}}
	s.applyPullResult(context.Background(), pullResult{
		users:    restUsers,
		userHash: computeUserHash(restUsers),
	})

	// WS delta adds user 3 afterwards.
	mb.Apply(controlplane.Event{
		Type:        controlplane.EventSyncUserDelta,
		DeltaAction: "add",
		DeltaUsers:  []model.UserSpec{{ID: 3, UUID: "c"}},
	})
	mb.MarkReady()
	state := mb.DrainIfReady()
	if !state.HasUsers {
		t.Fatal("drain returned no users")
	}
	if len(state.Users) != 2 || state.Users[0].ID != 1 || state.Users[1].ID != 3 {
		t.Fatalf("drained users = %+v, want IDs [1 3] — user 2 must not be resurrected", state.Users)
	}
}

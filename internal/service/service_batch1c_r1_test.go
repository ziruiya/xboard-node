package service

import (
	"cmp"
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
)

func assertWSUsers(t *testing.T, s *Service, wantRevision uint64, wantUsers []model.UserSpec) {
	t.Helper()
	if s.appliedUserRevision == nil || *s.appliedUserRevision != wantRevision {
		t.Fatalf("expected applied users revision %d and users %v, got users %v", wantRevision, wantUsers, s.lastUsers)
	}
	gotUsers := slices.Clone(s.lastUsers)
	slices.SortFunc(gotUsers, func(a, b model.UserSpec) int { return cmp.Compare(a.ID, b.ID) })
	if !reflect.DeepEqual(gotUsers, wantUsers) {
		t.Fatalf("users = %v, want %v", s.lastUsers, wantUsers)
	}
}

func TestUserDeltaGapRejectedAndRecoveredBySnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := &fakeKernel{running: true}
	s := newTestService(k)
	polled := make(chan struct{})
	s.source = batch5Source{polled: polled}
	s.pullResults = make(chan pullResult, 1)

	initial := []model.UserSpec{{ID: 1, UUID: "initial"}}
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUsers, StateRevision: revision(10), Users: initial,
	})
	assertWSUsers(t, s, 10, initial)
	before := k.updateCalls
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUserDelta, BaseRevision: revision(10), StateRevision: revision(12),
		DeltaAction: "add", DeltaUsers: []model.UserSpec{{ID: 2, UUID: "gap"}},
	})
	assertWSUsers(t, s, 10, initial)
	if k.updateCalls != before || !s.wsResyncPending {
		t.Fatalf("gap changed kernel update count to %d or missed resync (pending=%v)", k.updateCalls, s.wsResyncPending)
	}
	select {
	case <-polled:
	case <-time.After(time.Second):
		t.Fatal("full reconciliation was not started")
	}

	recovered := []model.UserSpec{{ID: 3, UUID: "snapshot"}}
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUsers, StateRevision: revision(12), Users: recovered,
	})
	assertWSUsers(t, s, 12, recovered)
	if k.updateCalls != before+1 {
		t.Fatalf("kernel updates = %d, want %d after recovery", k.updateCalls, before+1)
	}
}

func TestUserDeltaConsecutiveAppliesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.source = batch5Source{polled: make(chan struct{})}
	s.pullResults = make(chan pullResult, 1)

	initial := model.UserSpec{ID: 1, UUID: "initial"}
	added := model.UserSpec{ID: 2, UUID: "added"}
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUsers, StateRevision: revision(10), Users: []model.UserSpec{initial},
	})
	before := k.updateCalls
	delta := controlplane.Event{
		Type: controlplane.EventSyncUserDelta, BaseRevision: revision(10), StateRevision: revision(11),
		DeltaAction: "add", DeltaUsers: []model.UserSpec{added},
	}
	s.handleWSEvent(ctx, delta)
	assertWSUsers(t, s, 11, []model.UserSpec{initial, added})
	if k.updateCalls != before+1 || s.wsResyncPending {
		t.Fatalf("first delta got %d kernel updates and resync pending=%v", k.updateCalls-before, s.wsResyncPending)
	}

	s.handleWSEvent(ctx, delta)
	assertWSUsers(t, s, 11, []model.UserSpec{initial, added})
	if k.updateCalls != before+1 || !s.wsResyncPending {
		t.Fatalf("replay got %d total kernel updates and resync pending=%v", k.updateCalls-before, s.wsResyncPending)
	}
}

func TestWSConfigAndUserDeltaRevisionsAreIndependent(t *testing.T) {
	ctx := context.Background()
	s := newTestService(&fakeKernel{running: true})
	s.source = batch5Source{}
	s.cfg = &config.Config{}
	initial := model.UserSpec{ID: 1, UUID: "initial"}
	added := model.UserSpec{ID: 2, UUID: "added"}
	cfg := &model.NodeSpec{Protocol: "vless", ServerPort: 443}
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUsers, StateRevision: revision(10), Users: []model.UserSpec{initial},
	})
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncConfig, StateRevision: revision(12), Config: cfg,
	})
	assertWSUsers(t, s, 10, []model.UserSpec{initial})
	if s.appliedConfigRevision == nil || *s.appliedConfigRevision != 12 || s.lastConfig != cfg {
		t.Fatal("config snapshot did not apply at revision 12")
	}

	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUserDelta, BaseRevision: revision(10), StateRevision: revision(11),
		DeltaAction: "add", DeltaUsers: []model.UserSpec{added},
	})
	assertWSUsers(t, s, 11, []model.UserSpec{initial, added})
	if s.appliedConfigRevision == nil || *s.appliedConfigRevision != 12 || s.lastConfig != cfg {
		t.Fatal("accepted user delta disturbed config")
	}

	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUserDelta, BaseRevision: revision(12), StateRevision: revision(13),
		DeltaAction: "remove", DeltaUsers: []model.UserSpec{initial},
	})
	assertWSUsers(t, s, 11, []model.UserSpec{initial, added})
	if s.appliedConfigRevision == nil || *s.appliedConfigRevision != 12 || s.lastConfig != cfg {
		t.Fatal("rejected user delta disturbed config")
	}
}

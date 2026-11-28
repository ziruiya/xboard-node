package service

import (
	"context"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
)

func revision(n uint64) *uint64 { return &n }

func TestUserDeltaChecksAppliedDomainAtCommit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		base  *uint64
		state uint64
		want  bool
	}{
		{"matching", revision(10), 11, true},
		{"behind", revision(9), 10, false},
		{"ahead and equal to config revision", revision(11), 12, false},
		{"missing", nil, 12, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			k := &fakeKernel{running: true}
			s := newTestService(k)
			polled := make(chan struct{})
			s.source = batch5Source{polled: polled}
			s.pullResults = make(chan pullResult, 1)
			s.appliedConfigRevision, s.appliedUserRevision = revision(11), revision(9)
			s.machineMailbox = controlplane.NewNodeMailbox()
			s.machineMailbox.MarkReady()
			s.machineMailbox.Apply(controlplane.Event{
				Type: controlplane.EventSyncUserDelta, BaseRevision: tc.base, StateRevision: revision(tc.state),
				DeltaAction: "add", DeltaUsers: []model.UserSpec{{ID: 2, UUID: "new"}},
			})
			// The applied revision changes while the delta is queued.
			users := []model.UserSpec{{ID: 1, UUID: "old"}}
			s.applyPullResult(ctx, pullResult{users: users, userHash: computeUserHash(users), panelUserRevision: revision(10)})
			before := k.updateCalls
			s.drainMachineMailbox(ctx)
			if *s.appliedConfigRevision != 11 {
				t.Fatalf("user delta changed config revision to %d", *s.appliedConfigRevision)
			}
			if tc.want {
				if k.updateCalls != before+1 || len(s.lastUsers) != 2 || *s.appliedUserRevision != tc.state || s.wsResyncPending {
					t.Fatalf("matching delta did not commit, users=%v revision=%d", s.lastUsers, *s.appliedUserRevision)
				}
				return
			}
			if k.updateCalls != before || len(s.lastUsers) != 1 || *s.appliedUserRevision != 10 || !s.wsResyncPending {
				t.Fatalf("invalid delta changed state or missed reconcile, users=%v revision=%d", s.lastUsers, *s.appliedUserRevision)
			}
			select {
			case <-polled:
			case <-time.After(time.Second):
				t.Fatal("full reconciliation was not started")
			}
		})
	}
}

func TestUserDeltaWaitsForTwoDomainReconnectBaseline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.source = batch5Source{}
	s.wsSession, s.wsConfigBaseline, s.wsUserBaseline = 1, 1, 1
	s.appliedConfigRevision, s.appliedUserRevision = revision(30), revision(10)
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "old"}})
	delta := controlplane.Event{
		Type: controlplane.EventSyncUserDelta, Session: 2, BaseRevision: revision(10), StateRevision: revision(11),
		DeltaAction: "add", DeltaUsers: []model.UserSpec{{ID: 2, UUID: "new"}},
	}
	s.handleWSEvent(ctx, delta)
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncUsers, Session: 2, Users: s.lastUsers, StateRevision: revision(10),
	})
	s.handleWSEvent(ctx, delta)
	if k.updateCalls != 0 {
		t.Fatal("delta committed before both reconnect snapshots")
	}
	cfg := &model.NodeSpec{Protocol: "vless", ServerPort: 443}
	s.lastConfig, s.lastConfigHash = cfg, computeConfigHash(cfg)
	s.handleWSEvent(ctx, controlplane.Event{
		Type: controlplane.EventSyncConfig, Session: 2, Config: cfg, StateRevision: revision(31),
	})
	s.handleWSEvent(ctx, delta)
	if k.updateCalls != 1 || *s.appliedUserRevision != 11 || *s.appliedConfigRevision != 31 {
		t.Fatal("delta did not commit after both reconnect snapshots")
	}
}

func TestPullCommitComparesPanelRevisionsIndependently(t *testing.T) {
	s := newTestService(&fakeKernel{running: true})
	s.appliedConfigRevision, s.appliedUserRevision = revision(30), revision(10)
	cfg := &model.NodeSpec{Protocol: "vless", ServerPort: 443}
	s.lastConfig, s.lastConfigHash = cfg, computeConfigHash(cfg)
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "current"}})
	ctx := context.Background()
	stale := []model.UserSpec{{ID: 2, UUID: "stale"}}
	s.applyPullResult(ctx, pullResult{
		config: cfg, configHash: s.lastConfigHash, panelConfigRevision: revision(31),
		users: stale, userHash: computeUserHash(stale), panelUserRevision: revision(9),
		commit: func(configApplied, usersApplied bool) {
			if !configApplied || usersApplied {
				t.Fatalf("commit accepted config=%v users=%v", configApplied, usersApplied)
			}
		},
	})
	if *s.appliedConfigRevision != 31 || *s.appliedUserRevision != 10 || s.lastUsers[0].UUID != "current" {
		t.Fatal("REST changed the stale user domain or missed the current config revision")
	}
	legacy := []model.UserSpec{{ID: 3, UUID: "legacy"}}
	s.applyPullResult(ctx, pullResult{users: legacy, userHash: computeUserHash(legacy)})
	if s.wsResyncPending || s.appliedUserRevision != nil || s.lastUsers[0].UUID != "legacy" {
		t.Fatal("unversioned REST snapshot failed the legacy commit path")
	}
}

type countOnlyKernel struct{ *fakeKernel }

func (countOnlyKernel) GetUserTraffic(context.Context) (map[int][2]int64, map[int]map[string]bool, int, error) {
	return nil, nil, 1, nil
}

func TestAliveReportWithholdsEmptySetWhenKernelHasConnections(t *testing.T) {
	var restReports int
	reports := &batch2bReports{report: func(p controlplane.ReportPayload) error {
		restReports++
		if p.Alive != nil {
			t.Fatalf("REST would replace device state with %v", p.Alive)
		}
		return nil
	}}
	s := batch2bService(reports)
	s.kernel = countOnlyKernel{s.kernel.(*fakeKernel)}
	ctx := context.Background()
	s.sendDeviceBatch(ctx)
	if !s.sendReport(ctx, nil) || restReports != 1 || len(reports.devices) != 0 {
		t.Fatalf("REST reports = %d, WS device reports = %v", restReports, reports.devices)
	}
}

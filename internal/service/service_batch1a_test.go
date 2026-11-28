package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

type batch1aPollSource struct {
	controlplane.Source
	poll func(context.Context) (controlplane.Snapshot, error)
}

func (b batch1aPollSource) SupportsPolling() bool { return b.poll != nil }
func (b batch1aPollSource) Poll(ctx context.Context) (controlplane.Snapshot, error) {
	return b.poll(ctx)
}

func receiveBatch1APoll(t *testing.T, s *Service) pullResult {
	t.Helper()
	select {
	case result := <-s.pullResults:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("REST pull never delivered a result")
		return pullResult{}
	}
}

func TestBatch1ARejectsRESTUsersAfterWSCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldUsers := []model.UserSpec{{ID: 1, UUID: "revoked"}}
	s := newTestService(&fakeKernel{running: true})
	s.cfg = &config.Config{}
	oldConfig := &model.NodeSpec{Protocol: "vless", ServerPort: 1234}
	s.lastConfig, s.lastConfigHash = oldConfig, computeConfigHash(oldConfig)
	s.updateUserState(oldUsers)
	s.pullResults = make(chan pullResult, 1)
	started, release := make(chan struct{}), make(chan struct{})
	var configETagCommitted, userETagCommitted bool
	s.source = batch1aPollSource{poll: func(ctx context.Context) (controlplane.Snapshot, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return controlplane.Snapshot{}, ctx.Err()
		}
		return controlplane.Snapshot{
			Config: &model.NodeSpec{Protocol: "vless", ServerPort: 2345},
			Users:  oldUsers,
			Commit: func(configApplied, usersApplied bool) {
				configETagCommitted, userETagCommitted = configApplied, usersApplied
			},
		}, nil
	}}
	s.pullViaAPIAsync(ctx)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("REST pull did not start")
	}
	s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncUsers, Users: []model.UserSpec{}})
	close(release)
	s.applyPullResult(ctx, receiveBatch1APoll(t, s))
	if len(s.lastUsers) != 0 || userETagCommitted || !configETagCommitted || s.lastConfig.ServerPort != 2345 || !s.wsResyncPending {
		t.Fatalf("invalid commit: config=%d users=%v configETag=%v userETag=%v pending=%v",
			s.lastConfig.ServerPort, s.lastUsers, configETagCommitted, userETagCommitted, s.wsResyncPending)
	}
}

func TestBatch1AResyncRetriesAfterBackoffAndFailure(t *testing.T) {
	ctx := context.Background()
	s := newTestService(&fakeKernel{})
	s.pullResults = make(chan pullResult, 1)
	attempts := 0 // poll results synchronize access
	s.source = batch1aPollSource{poll: func(context.Context) (controlplane.Snapshot, error) {
		attempts++
		if attempts == 1 {
			return controlplane.Snapshot{}, errors.New("temporarily unavailable")
		}
		return controlplane.Snapshot{Users: []model.UserSpec{}}, nil
	}}
	s.pullBackoff.skipRemaining = 1
	s.requestWSResync(ctx, "test")
	if s.pullActive.Load() || !s.wsResyncPending {
		t.Fatal("failed to retain pending resync across backoff")
	}
	s.pullViaAPIAsync(ctx)
	s.applyPullResult(ctx, receiveBatch1APoll(t, s))
	if s.pullActive.Load() || !s.wsResyncPending {
		t.Fatal("failed to release pull or retain resync after error")
	}
	s.pullViaAPIAsync(ctx) // backoff after failed request
	s.pullViaAPIAsync(ctx)
	s.applyPullResult(ctx, receiveBatch1APoll(t, s))
	if s.wsResyncPending || attempts != 2 {
		t.Fatalf("pending=%v attempts=%d, want false and 2", s.wsResyncPending, attempts)
	}
}

type batch1aReloadFailure struct{ *fakeKernel }

func (batch1aReloadFailure) Reload(*model.NodeSpec, []model.UserSpec, kernel.TLSCert) error {
	return errors.New("reload failed")
}

func TestBatch1AConfigHashOnlyCommitsAfterSuccess(t *testing.T) {
	ctx := context.Background()
	k := &fakeKernel{running: true, startErr: errors.New("start failed")}
	s := newTestService(k)
	s.cfg = &config.Config{}
	s.kernel = batch1aReloadFailure{k}
	s.source = batch1aPollSource{}
	previous := &model.NodeSpec{Protocol: "vless", ServerPort: 1234}
	candidate := &model.NodeSpec{Protocol: "vless", ServerPort: 5678}
	s.lastConfig, s.lastConfigHash = previous, computeConfigHash(previous)
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "a"}})
	event := controlplane.Event{Type: controlplane.EventSyncConfig, Config: candidate}
	s.handleWSEvent(ctx, event)
	if s.lastConfig != previous || s.lastConfigHash != computeConfigHash(previous) || k.startCalls != 1 {
		t.Fatal("failed kernel application committed the config or skipped restart")
	}
	k.startErr = nil
	s.handleWSEvent(ctx, event)
	if s.lastConfig != candidate || s.lastConfigHash != computeConfigHash(candidate) || k.startCalls != 2 {
		t.Fatal("identical config did not retry and commit")
	}
}

func TestBatch1ARESTCommitsNeitherResourceWhenCombinedApplyFails(t *testing.T) {
	ctx := context.Background()
	k := &fakeKernel{running: true, startErr: errors.New("start failed")}
	s := newTestService(k)
	s.cfg = &config.Config{}
	s.kernel = batch1aReloadFailure{k}
	previous := &model.NodeSpec{Protocol: "vless", ServerPort: 1234}
	s.lastConfig, s.lastConfigHash = previous, computeConfigHash(previous)
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "a"}})
	configApplied, usersApplied := true, true
	candidate := &model.NodeSpec{Protocol: "vless", ServerPort: 5678}
	users := []model.UserSpec{{ID: 2, UUID: "b"}}
	result := pullResult{config: candidate, configHash: computeConfigHash(candidate),
		users: users, userHash: computeUserHash(users),
		commit: func(config, users bool) { configApplied, usersApplied = config, users },
	}
	s.applyPullResult(ctx, result)
	if configApplied || usersApplied || s.lastConfig != previous || len(s.lastUsers) != 1 || s.lastUsers[0].ID != 1 {
		t.Fatalf("failed apply committed state: configApplied=%v usersApplied=%v users=%v",
			configApplied, usersApplied, s.lastUsers)
	}
	k.startErr = nil
	s.applyPullResult(ctx, result)
	if !configApplied || !usersApplied || s.lastConfig != candidate || len(s.lastUsers) != 1 || s.lastUsers[0].ID != 2 {
		t.Fatalf("retry did not commit state: configApplied=%v usersApplied=%v users=%v",
			configApplied, usersApplied, s.lastUsers)
	}
}

type preservedKernel struct{ *fakeKernel }

func (preservedKernel) Reload(*model.NodeSpec, []model.UserSpec, kernel.TLSCert) error {
	return kernel.ErrReloadKeepsRunning
}

func TestRejectedKernelReplacementKeepsRunningConfig(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.cfg = &config.Config{}
	s.kernel = preservedKernel{k}
	previous := &model.NodeSpec{Protocol: "vless", ServerPort: 1234}
	candidate := &model.NodeSpec{Protocol: "vless", ServerPort: 5678}
	s.lastConfig, s.lastConfigHash = previous, computeConfigHash(previous)
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "a"}})
	if s.applyConfig(context.Background(), candidate, computeConfigHash(candidate), []model.UserSpec{{ID: 2, UUID: "b"}}) {
		t.Fatal("rejected replacement was accepted")
	}
	if !k.running || k.startCalls != 0 || s.lastConfig != previous || len(s.lastUsers) != 1 || s.lastUsers[0].ID != 1 {
		t.Fatal("rejected replacement changed the running kernel, config, or users")
	}
}

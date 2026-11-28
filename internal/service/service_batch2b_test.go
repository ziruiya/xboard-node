package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

type batch2bReports struct {
	controlplane.Source
	controlplane.Sink
	report  func(controlplane.ReportPayload) error
	devices []map[int][]string
}

func (*batch2bReports) SupportsReporting() bool                           { return true }
func (*batch2bReports) Metrics() controlplane.APIMetrics                  { return controlplane.APIMetrics{} }
func (r *batch2bReports) Report(payload controlplane.ReportPayload) error { return r.report(payload) }
func (r *batch2bReports) ReportDevices(_ controlplane.PushClient, devices map[int][]string) {
	r.devices = append(r.devices, devices)
}

type batch2bPush struct{ controlplane.PushClient }

func (batch2bPush) IsConnected() bool { return true }

type liveDeviceKernel struct {
	*fakeKernel
	alive map[int]map[string]bool
}

func (k *liveDeviceKernel) GetUserTraffic(context.Context) (map[int][2]int64, map[int]map[string]bool, int, error) {
	return nil, k.alive, len(k.alive), nil
}

func batch2bService(reports *batch2bReports) *Service {
	s := newTestService(&fakeKernel{running: true})
	s.tracker = tracker.New()
	s.sink, s.source = reports, reports
	s.wsClient = batch2bPush{}
	return s
}

func waitBatch2B(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for report")
	}
}

func TestBatch2BAsyncDeviceSnapshotIsIndependent(t *testing.T) {
	first := map[int][]string{1: {"192.0.2.1"}}
	second := map[int][]string{2: {"192.0.2.2"}}
	reports := &batch2bReports{}
	s := batch2bService(reports)
	started, release := make(chan struct{}), make(chan struct{})
	failures := make(chan []byte, 1)
	reports.report = func(p controlplane.ReportPayload) error {
		close(started)
		<-release
		for i := 0; i < 200; i++ {
			encoded, err := json.Marshal(p.Alive)
			if err != nil || string(encoded) != `{"1":["192.0.2.1"]}` {
				failures <- encoded
				break
			}
		}
		return nil
	}
	k := &liveDeviceKernel{fakeKernel: s.kernel.(*fakeKernel), alive: map[int]map[string]bool{1: {"192.0.2.1": true}}}
	s.kernel = k
	s.pushReportAsync(context.Background())
	waitBatch2B(t, started)
	close(release)
	for i := 0; i < 200; i++ {
		// Alternate snapshots so every legacy flush would rewrite the buffer.
		k.alive = map[int]map[string]bool{2: {"192.0.2.2": true}}
		s.sendDeviceBatch(context.Background())
		k.alive = map[int]map[string]bool{1: {"192.0.2.1": true}}
		s.sendDeviceBatch(context.Background())
	}
	waitBatch2B(t, s.pushDone)
	select {
	case got := <-failures:
		t.Fatalf("background REST payload changed: %s", got)
	default:
	}
	if !reflect.DeepEqual(reports.devices[0], second) || !reflect.DeepEqual(reports.devices[1], first) {
		t.Fatalf("WS snapshots changed: %v", reports.devices[:2])
	}
}

func TestBatch2BFailedRESTDeviceSnapshotRetriesIndependentlyOfWS(t *testing.T) {
	var attempts []map[int][]string
	reports := &batch2bReports{report: func(p controlplane.ReportPayload) error {
		attempts = append(attempts, p.Alive)
		if len(attempts) == 1 {
			return errors.New("offline")
		}
		return nil
	}}
	s := batch2bService(reports)
	s.kernel = &liveDeviceKernel{fakeKernel: s.kernel.(*fakeKernel), alive: map[int]map[string]bool{1: {"192.0.2.1": true}}}
	ctx := context.Background()
	s.trackAndEnforce(ctx)
	if s.sendReport(ctx, nil) {
		t.Fatal("failed report was accepted")
	}
	s.sendDeviceBatch(ctx)
	if !s.sendReport(ctx, nil) || !s.sendReport(ctx, nil) {
		t.Fatal("REST retry did not succeed")
	}
	want := map[int][]string{1: {"192.0.2.1"}}
	if len(reports.devices) != 1 || !reflect.DeepEqual(reports.devices[0], want) ||
		!reflect.DeepEqual(attempts, []map[int][]string{want, want, want}) {
		t.Fatalf("WS = %v, REST attempts = %v", reports.devices, attempts)
	}
}

type batch2bFinalKernel struct {
	*fakeKernel
	bytes int64
}

func (k *batch2bFinalKernel) GetUserTraffic(context.Context) (map[int][2]int64, map[int]map[string]bool, int, error) {
	return map[int][2]int64{1: {k.bytes, 0}}, nil, 0, nil
}

func (k *batch2bFinalKernel) Stop() {
	k.bytes += 7
	k.fakeKernel.Stop()
}

func TestBatch2BShutdownPersistsAndSendsTrafficInOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failOn int
	}{
		{"acknowledged", 0},
		{"offline", 1},
		{"head_acknowledged_tail_offline", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "traffic.json")
			reports := &batch2bReports{}
			s := batch2bService(reports)
			k := &batch2bFinalKernel{fakeKernel: &fakeKernel{running: true}, bytes: 10}
			s.kernel = k
			s.tracker.EnablePersistence(path)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			var inFlightID string
			var sends []string
			var savedA, savedB *tracker.Batch
			reports.report = func(p controlplane.ReportPayload) error {
				if p.Context == ctx {
					inFlightID = p.TrafficBatch.BatchID
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}
				if k.IsRunning() {
					t.Fatal("final sending started before kernel stop")
				}
				sends = append(sends, p.TrafficBatch.BatchID)
				if len(sends) == 1 {
					read := tracker.New()
					read.EnablePersistence(path)
					savedA = read.Unacked()
					if savedA == nil || savedA.ID != p.TrafficBatch.BatchID || savedA.Data[1] != [2]int64{10, 0} {
						t.Fatalf("A was not durable when first sent during shutdown: %+v", savedA)
					}
					queue := readQueue(t, path)
					if len(queue) != 2 {
						t.Fatal("B was not durable before the shutdown send of A")
					}
					savedB = queue[1]
				}
				if len(sends) == 2 && p.Traffic[1] != [2]int64{32, 0} {
					t.Fatalf("B payload = %v, want 20 newly sampled + 5 since last tick + 7 during stop", p.Traffic)
				}
				if len(sends) == tc.failOn {
					return errors.New("offline")
				}
				return nil
			}

			s.trackAndEnforce(ctx)
			s.pushReportAsync(ctx)
			waitBatch2B(t, started)
			k.bytes = 30
			s.trackAndEnforce(ctx)
			k.bytes = 35
			cancel()
			s.pushReportSync()
			if savedA == nil || savedA.ID != inFlightID || savedB == nil ||
				savedB.ID == "" || savedB.ID == inFlightID || savedB.Data[1] != [2]int64{32, 0} {
				t.Fatalf("wrong saved windows: inflight=%q A=%+v B=%+v", inFlightID, savedA, savedB)
			}
			wantSends := []string{inFlightID, savedB.ID}
			var wantRecovered []*tracker.Batch
			switch tc.failOn {
			case 1:
				wantSends = wantSends[:1]
				wantRecovered = []*tracker.Batch{savedA, savedB}
			case 2:
				wantRecovered = []*tracker.Batch{savedB}
			}
			if !reflect.DeepEqual(sends, wantSends) {
				t.Fatalf("sent IDs %v, want %v", sends, wantSends)
			}
			if got := readQueue(t, path); !reflect.DeepEqual(got, wantRecovered) {
				t.Fatalf("restart recovered %+v, want %+v", got, wantRecovered)
			}
		})
	}
}

// Read a copy so acknowledging the head can inspect the following batch
// without changing the live sender's file.
func readQueue(t *testing.T, path string) []*tracker.Batch {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "queue-copy.json")
	if err := os.WriteFile(copyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	restored := tracker.New()
	restored.EnablePersistence(copyPath)
	var result []*tracker.Batch
	for batch := restored.Unacked(); batch != nil; batch = restored.Unacked() {
		result = append(result, batch)
		if !restored.AckBatch(batch.ID) {
			t.Fatal("could not acknowledge copied batch")
		}
	}
	return result
}

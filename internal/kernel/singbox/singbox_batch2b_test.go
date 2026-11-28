package singbox

import (
	"context"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

func TestBatch2BSingBoxTrafficAcrossReplacementAndDrain(t *testing.T) {
	s, spec := batch5RunningSingBox(t)
	users := []model.UserSpec{{ID: 1, UUID: "11111111-1111-1111-1111-111111111111"}}
	// Use the normal swap and retirement path with no listening inbounds.
	start := func() {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.startLocked(&spec, users, kernel.TLSCert{}, M{}); err != nil {
			t.Fatal(err)
		}
	}
	start()
	old := s.connTracker.users[1]
	old.upload.Add(10)
	old.addConn("192.0.2.1")
	tally := tracker.New()
	sample := func(want int64) {
		t.Helper()
		data, ips, count, err := s.GetUserTraffic(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		tally.Process(data, ips, count)
		if got := tally.FlushTraffic()[1][0]; got != want {
			t.Fatalf("newly sampled bytes = %d, want %d (cumulative %v)", got, want, data)
		}
	}
	sample(10)
	start()
	current := s.connTracker.users[1]
	current.upload.Add(25) // Higher than the old generation's lastSeen.
	old.upload.Add(7)      // Bytes after replacement while the old instance drains.
	sample(32)
	old.upload.Add(3) // Unseen bytes just before the old instance becomes idle.
	old.removeConn("192.0.2.1")
	joined := make(chan struct{})
	go func() {
		s.retiring.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(6 * time.Second):
		t.Fatal("old sing-box instance did not retire")
	}
	sample(3)
	old.upload.Add(2) // A late copy callback can account bytes after retirement.
	sample(2)
	current.upload.Add(8)
	s.Stop()
	sample(8)
	sample(0)
}

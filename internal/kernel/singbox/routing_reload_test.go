package singbox

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

func TestRoutingReloadForwardsAndSettlesTraffic(t *testing.T) {
	first := routingTestEcho(t)
	second := routingTestEcho(t)
	reserved, err := net.Listen("tcp6", "[::]:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reserved.Addr().(*net.TCPAddr).Port
	reserved.Close()

	spec := model.NodeSpec{Protocol: "socks", ServerPort: port, CustomRoutes: routingTestAllow(first)}
	users := []model.UserSpec{{ID: 1, UUID: "loopback-user"}}
	dir := t.TempDir()
	cache := filepath.Join(dir, "cache.db")
	custom := filepath.Join(dir, "custom.json")
	if err := os.WriteFile(custom, fmt.Appendf(nil, `{"experimental":{"cache_file":{"enabled":true,"path":%q}}}`, cache), 0600); err != nil {
		t.Fatal(err)
	}
	s := New(config.KernelConfig{LogLevel: "warn", CustomConfig: custom})
	s.platform = batch5Platform{}
	if err := s.Start(&spec, users, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	connect := func(target *net.TCPListener) (net.Conn, error) {
		return connectAuthTestProxy("socks", fmt.Sprintf("127.0.0.1:%d", port), target.Addr().String(), users[0].UUID, users[0].UUID)
	}
	exchange := func(c net.Conn, want string) {
		t.Helper()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.WriteString(c, want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(c, got); err != nil || string(got) != want {
			t.Fatalf("echo got %q, want %q: %v", got, want, err)
		}
	}
	tally := tracker.New()
	sample := func(wantDelta, wantTotal int64) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for {
			traffic, ips, count, err := s.GetUserTraffic(context.Background())
			want := [2]int64{wantTotal, wantTotal}
			if err != nil {
				t.Fatal(err)
			}
			if traffic[1] == want {
				tally.Process(traffic, ips, count)
				delta := tally.FlushTraffic()[1]
				if delta != [2]int64{wantDelta, wantDelta} {
					t.Fatalf("cumulative %v, delta %v, want delta %d each way", traffic[1], delta, wantDelta)
				}
				t.Logf("cumulative upload/download %v, newly sampled %v", traffic[1], delta)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("traffic got %v, want %v", traffic[1], want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	old, err := connect(first)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	exchange(old, "before")
	sample(6, 6)

	replacement := spec
	replacement.CustomRoutes = routingTestAllow(second)
	reloaded := make(chan error, 1)
	go func() { reloaded <- s.Reload(&replacement, users, kernel.TLSCert{}) }()
	exchange(old, "during-swap")
	if err := <-reloaded; err != nil {
		t.Fatal(err)
	}
	exchange(old, "during-drain")
	if rejected, err := connect(first); err == nil {
		rejected.Close()
		t.Fatal("new connection received retired routing rules")
	}
	current, err := connect(second)
	if err != nil {
		t.Fatalf("new rule did not permit forwarding: %v", err)
	}
	defer current.Close()
	exchange(current, "new")
	t.Log("old TCP connection drained and new connections used the replacement rules")
	sample(26, 32)

	old.Close()
	s.retiring.Wait()
	sample(0, 32)
	exchange(current, "after")
	sample(5, 37)
}

func routingTestAllow(listener *net.TCPListener) []map[string]any {
	return []map[string]any{{
		"ip_cidr":  []string{"127.0.0.1/32"},
		"port":     []int{listener.Addr().(*net.TCPAddr).Port},
		"outbound": "direct",
	}}
}

func routingTestEcho(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener
}

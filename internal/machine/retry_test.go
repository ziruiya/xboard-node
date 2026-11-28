package machine

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestDiscoveryRetriesFailedNodeAndResetsBackoff(t *testing.T) {
	reserved, err := net.Listen("tcp6", "[::]:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reserved.Addr().(*net.TCPAddr).Port
	reserved.Close()

	var attempts atomic.Int32
	var healthy, unbound atomic.Bool
	var network atomic.Value
	network.Store("xhttp")
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/server/machine/nodes", func(w http.ResponseWriter, r *http.Request) {
		if unbound.Load() {
			fmt.Fprint(w, `{"nodes":[]}`)
			return
		}
		fmt.Fprint(w, `{"nodes":[{"id":7,"type":"socks","name":"local"}]}`)
	})
	mux.HandleFunc("/api/v2/server/config", func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if !healthy.Load() {
			http.Error(w, "injected config failure", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"protocol":"socks","server_port":%d,"network":%q}`, port, network.Load())
	})
	mux.HandleFunc("/api/v2/server/user", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"users":[{"id":1,"uuid":"loopback-user"}]}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, fmt.Appendf(nil, "panel:\n  url: %s\nmachine:\n  machine_id: 1\n  token: test\n", server.URL), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	o := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); o.stopAll() }()

	waitFor := func(predicate func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !predicate() {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for node state")
			}
			time.Sleep(time.Millisecond)
		}
	}
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
		discovered := time.Now()
		o.rediscover(ctx)
		waitFor(func() bool {
			o.mu.Lock()
			defer o.mu.Unlock()
			retry := o.retries[7]
			return o.nodes[7] == nil && !retry.after.Before(discovered.Add(want))
		})
		o.mu.Lock()
		retry := o.retries[7]
		o.mu.Unlock()
		if retry.delay != want || time.Until(retry.after) > want {
			t.Fatalf("retry delay got %v (%v left), want %v", retry.delay, time.Until(retry.after), want)
		}
		count := attempts.Load()
		o.rediscover(ctx)
		if attempts.Load() != count {
			t.Fatal("discovery attempted a node inside its backoff window")
		}
		o.mu.Lock()
		if o.nodes[7] != nil {
			t.Fatal("discovery started a node during backoff")
		}
		o.mu.Unlock()
		t.Logf("attempts=%d backoff=%v, immediate discovery suppressed", count, retry.delay)
		if want == time.Second {
			time.Sleep(time.Until(retry.after))
		} else {
			// Advance only the stored deadline to exercise the cap without pacing
			// the test across minutes of backoff.
			o.mu.Lock()
			retry.after = time.Now()
			o.retries[7] = retry
			o.mu.Unlock()
		}
	}

	healthy.Store(true)
	o.rediscover(ctx)
	waitFor(func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.nodes[7] == nil && o.retries[7].after.After(time.Now())
	})
	t.Log("xhttp was refused before starting the node")

	o.mu.Lock()
	retry := o.retries[7]
	retry.after = time.Now()
	o.retries[7] = retry
	o.mu.Unlock()
	network.Store("tcp")
	o.rediscover(ctx)
	waitFor(func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.nodes[7] != nil && o.retries[7].delay == 0
	})
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	t.Log("sing-box listener recovered and successful startup reset backoff")

	stale := &nodeHandle{done: make(chan struct{})}
	o.finishNode(ctx, 7, stale, nil, fmt.Errorf("late exit"))
	o.mu.Lock()
	current := o.nodes[7]
	o.mu.Unlock()
	if current == nil || current == stale {
		t.Fatal("late exit changed the current node registration")
	}

	unbound.Store(true)
	o.rediscover(ctx)
	o.mu.Lock()
	remaining := len(o.nodes)
	o.mu.Unlock()
	if remaining != 0 {
		t.Fatal("unbound node did not stop")
	}
}

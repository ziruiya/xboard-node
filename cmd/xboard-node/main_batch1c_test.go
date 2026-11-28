package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestGenerationLeaseProcess(t *testing.T) {
	if path := os.Getenv("XBOARD_LEASE_TEST_CONFIG"); path != "" {
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		os.Args = []string{os.Args[0], "-c", path}
		main()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	echo, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		conn, err := echo.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}
	}()
	portProbe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := portProbe.Addr().String()
	port := portProbe.Addr().(*net.TCPAddr).Port
	portProbe.Close()

	type frame struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	type observation struct {
		session int64
		data    string
	}
	connections := make(chan *websocket.Conn, 8)
	reports := make(chan observation, 16)
	var requests, sessions, closed atomic.Int64
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		id := sessions.Add(1)
		defer closed.Add(1)
		defer conn.Close()
		if conn.WriteJSON(frame{Event: "auth.success"}) != nil {
			return
		}
		connections <- conn
		for {
			var msg frame
			if conn.ReadJSON(&msg) != nil {
				return
			}
			if msg.Event == "report.devices" {
				reports <- observation{id, string(msg.Data)}
			}
		}
	})
	mux.HandleFunc("/api/v2/server/machine/nodes", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"nodes":[{"id":1,"type":"http","name":"lease-test"}]}`)
	})
	mux.HandleFunc("/api/v2/server/handshake", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"websocket":{"enabled":true,"ws_url":"ws://%s/ws"},"settings":{"push_interval":60}}`, r.Host)
	})
	mux.HandleFunc("/api/v2/server/config", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"protocol":"http","listen_ip":"127.0.0.1","server_port":%d,"state_revision":7,"custom_routes":[{"ip_cidr":["127.0.0.1/32"],"outbound":"direct"}]}`, port)
	})
	mux.HandleFunc("/api/v2/server/user", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"state_revision":8,"users":[{"id":17,"uuid":"test-user"}]}`)
	})
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		mux.ServeHTTP(w, r)
	}))
	defer server.Close()
	path := filepath.Join(dir, "agent.yaml")
	cfg := fmt.Sprintf("panel:\n  url: %q\nmachine:\n  machine_id: 1\n  token: local-test\nkernel:\n  type: singbox\n  config_dir: %q\nnode:\n  device_report_interval: 1\nws:\n  backoff_initial: 1\n", server.URL, dir)
	if err := os.WriteFile(path, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}

	command := func(ctx context.Context) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGenerationLeaseProcess$")
		cmd.Env = append(os.Environ(), "XBOARD_LEASE_TEST_CONFIG="+path)
		return cmd
	}
	start := func() func() {
		t.Helper()
		cmd := command(ctx)
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			_ = cmd.Process.Signal(syscall.SIGTERM)
			if err := cmd.Wait(); err != nil {
				t.Errorf("agent shut down with %v\n%s", err, output.String())
			}
		}
		t.Cleanup(stop)
		return stop
	}
	waitConnection := func() *websocket.Conn {
		t.Helper()
		select {
		case c := <-connections:
			return c
		case <-ctx.Done():
			t.Fatal("agent did not connect to the loopback panel")
			return nil
		}
	}
	stop := start()
	firstWS := waitConnection()

	var proxy net.Conn
	for ctx.Err() == nil {
		proxy, err = net.DialTimeout("tcp4", proxyAddr, 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("proxy never became ready: %v", err)
	}
	defer proxy.Close()
	_ = proxy.SetDeadline(time.Now().Add(20 * time.Second))
	auth := base64.StdEncoding.EncodeToString([]byte("test-user:test-user"))
	fmt.Fprintf(proxy, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Basic %s\r\n\r\n", echo.Addr(), echo.Addr(), auth)
	reader := bufio.NewReader(proxy)
	response, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("proxy CONNECT response: %v, %v", response, err)
	}
	assertProxy := func(message string) {
		t.Helper()
		if _, err := io.WriteString(proxy, message); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(message))
		if _, err := io.ReadFull(reader, got); err != nil || string(got) != message {
			t.Fatalf("existing proxy connection: received %q, error %v", got, err)
		}
	}
	const want = `{"alive_complete":true,"devices":{"17":["127.0.0.1"]},"node_id":1}`
	waitDevice := func(session int64, requireFirst bool) string {
		t.Helper()
		for {
			select {
			case report := <-reports:
				if report.session != session {
					continue
				}
				if report.data == want {
					return report.data
				}
				if requireFirst {
					t.Fatalf("first snapshot after reconnect = %s, want %s", report.data, want)
				}
			case <-ctx.Done():
				t.Fatal("panel never received the live device")
			}
		}
	}
	assertProxy("before")
	before := waitDevice(1, false)
	requestCount := requests.Load()
	loserCtx, stopLoser := context.WithTimeout(ctx, 3*time.Second)
	defer stopLoser()
	output, err := command(loserCtx).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != exitGenerationLease ||
		strings.Count(string(output), "relay cannot acquire traffic state") != 1 {
		t.Fatalf("duplicate startup = %v, output %s", err, output)
	}
	if requests.Load() != requestCount || sessions.Load() != 1 || closed.Load() != 0 {
		t.Fatal("duplicate startup contacted the panel or disturbed the first WS connection")
	}
	assertProxy("after")
	firstWS.Close()
	waitConnection()
	if after := waitDevice(2, true); after != before {
		t.Fatalf("device table changed from %s to %s", before, after)
	}
	assertProxy("reconnected")
	stop()
	stopNext := start()
	waitConnection()
	if sessions.Load() != 3 {
		t.Fatalf("takeover opened %d total sessions, want 3", sessions.Load())
	}
	stopNext()
}

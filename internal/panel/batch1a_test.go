package panel

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/gorilla/websocket"
)

type batch1aRoundTrip func(*http.Request) (*http.Response, error)

func (f batch1aRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBatch1AETagsWaitForSnapshotCommit(t *testing.T) {
	client := NewClient(config.PanelConfig{URL: "http://panel.invalid"})
	var wantConfig, wantUsers string
	failUsers := true
	client.httpClient.Transport = batch1aRoundTrip(func(r *http.Request) (*http.Response, error) {
		want, tag, body, status := wantConfig, "c1", `{"protocol":"socks"}`, http.StatusOK
		if strings.HasSuffix(r.URL.Path, "/user") {
			want, tag, body = wantUsers, "u1", `{"users":[]}`
			if failUsers {
				status = http.StatusServiceUnavailable
			}
		}
		if got := r.Header.Get("If-None-Match"); got != want {
			t.Errorf("%s validator = %q, want %q", r.URL.Path, got, want)
		}
		if want == tag {
			status = http.StatusNotModified
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Etag": {tag}}}, nil
	})
	var configTag, userTag string
	if _, err := client.GetConfig(&configTag); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetUsers(&userTag); err == nil {
		t.Fatal("expected a partial snapshot failure")
	}
	failUsers = false
	users, err := client.GetUsers(&userTag)
	if err != nil || users == nil || len(users) != 0 {
		t.Fatalf("empty users snapshot = %v, error = %v", users, err)
	}
	if _, err := client.GetConfig(&configTag); err != nil {
		t.Fatal(err)
	}
	client.CommitETags(nil, &userTag)
	wantUsers = "u1"
	if _, err := client.GetConfig(&configTag); err != nil {
		t.Fatal(err)
	}
	client.CommitETags(&configTag, nil)
	wantConfig = "c1"
	if cfg, err := client.GetConfig(); cfg != nil || err != nil {
		t.Fatalf("config after commit = %v, error = %v", cfg, err)
	}
	if users, err := client.GetUsers(); users != nil || err != nil {
		t.Fatalf("users after commit = %v, error = %v", users, err)
	}
}

func TestBatch1APanelCredentialsAreExcludedFromLogs(t *testing.T) {
	var logs bytes.Buffer
	nlog.Init(&logs, slog.LevelDebug, false)
	t.Cleanup(func() { nlog.Init(nil, slog.LevelInfo, true) })
	const token, privateKey, dnsCredential = "test+secret-token", "private-key-fixture", "dns-fixture"
	client := NewClient(config.PanelConfig{URL: "https://panel.invalid", Token: token})
	transportErr := errors.New("offline fixture")
	client.httpClient.Transport = batch1aRoundTrip(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	})
	_, err := client.GetConfig()
	if !errors.Is(err, transportErr) {
		t.Fatalf("transport cause lost: %v", err)
	}
	nlog.Core().Warn("panel request failed", "error", err)
	client.httpClient.Transport = batch1aRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(token))}, nil
	})
	_, err = client.GetUsers()
	nlog.Core().Error("panel request failed", "error", err)
	agent := NewWSClient("", token, 1, WSClientConfig{}, nil, nil, nil)
	agent.handleMessage(wsMessage{Event: WSEventSyncConfig, Data: json.RawMessage(
		`{"config":{"server_port":"` + dnsCredential + `","cert_config":{"key_content":"` + privateKey + `"}}}`)})
	nlog.Core().Warn("ws read failed", "error", safeWSReadError(&websocket.CloseError{Code: 1008, Text: token}))
	for _, secret := range []string{token, url.QueryEscape(token), privateKey, dnsCredential} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("panel credential appeared in log output")
		}
	}
	for _, reason := range []string{"offline fixture", "403", "invalid payload field types", "1008"} {
		if !strings.Contains(logs.String(), reason) {
			t.Errorf("missing safe diagnostic %q in %s", reason, logs.String())
		}
	}
}

// Perform the actual Gorilla upgrade and frame I/O on a paired byte stream.
type batch1aWSResponse struct {
	*httptest.ResponseRecorder
	conn net.Conn
	buf  *bufio.ReadWriter
}

func (w batch1aWSResponse) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, w.buf, nil
}

func batch1aWebsocketPair(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { local.Close(); remote.Close() })
	ready := make(chan *websocket.Conn, 1)
	failed := make(chan error, 1)
	go func() {
		buf := bufio.NewReadWriter(bufio.NewReader(remote), bufio.NewWriter(remote))
		req, err := http.ReadRequest(buf.Reader)
		if err == nil {
			upgrader := websocket.Upgrader{}
			var peer *websocket.Conn
			peer, err = upgrader.Upgrade(batch1aWSResponse{httptest.NewRecorder(), remote, buf}, req, nil)
			if err == nil {
				ready <- peer
				return
			}
		}
		failed <- err
		remote.Close()
	}()
	dialer := websocket.Dialer{
		HandshakeTimeout: time.Second,
		NetDialContext:   func(context.Context, string, string) (net.Conn, error) { return local, nil },
	}
	client, _, err := dialer.Dial("ws://panel.invalid", nil)
	if err != nil {
		t.Fatalf("upgrade client: %v", err)
	}
	select {
	case peer := <-ready:
		return client, peer
	case err := <-failed:
		t.Fatalf("upgrade server: %v", err)
		return nil, nil
	}
}

func TestBatch1AWSReadDeadlinesAndCancellation(t *testing.T) {
	for _, state := range []string{"auth_timeout", "idle_timeout", "cancel_auth", "cancel_online"} {
		t.Run(state, func(t *testing.T) {
			client, peer := batch1aWebsocketPair(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			status := make(chan bool, 2)
			online := strings.HasSuffix(state, "online") || strings.HasPrefix(state, "idle")
			deadline := 150 * time.Millisecond
			if strings.HasPrefix(state, "cancel") {
				deadline = 20 * time.Second
			}
			agent := NewWSClient("", "", 1, WSClientConfig{HandshakeTimeout: deadline}, nil, func(s WSStatusChange) { status <- s.Connected }, nil)
			done := make(chan error, 1)
			go func() { done <- agent.serveConnection(ctx, client, deadline) }()
			if online {
				_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
				if err := peer.WriteJSON(wsMessage{Event: "auth.success"}); err != nil {
					t.Fatal(err)
				}
				select {
				case connected := <-status:
					if !connected || !agent.IsConnected() {
						t.Fatal("session was not published")
					}
				case <-time.After(time.Second):
					t.Fatal("missing online status")
				}
			}
			if strings.HasPrefix(state, "cancel") {
				cancel()
			}
			select {
			case err := <-done:
				if strings.HasSuffix(state, "timeout") {
					var timeout net.Error
					if !errors.As(err, &timeout) || !timeout.Timeout() {
						t.Fatalf("expected read timeout, got %v", err)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("read blocked shutdown")
			}
			if agent.IsConnected() || <-status {
				t.Fatal("session still online")
			}
		})
	}
}

func TestBatch1AWSSessionPublishesQueueAndDisconnectsWhileSending(t *testing.T) {
	client, peer := batch1aWebsocketPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agent := NewWSClient("", "", 1, WSClientConfig{}, nil, nil, nil)
	agent.onStatus = func(s WSStatusChange) {
		if s.Connected {
			agent.SendRaw("first.report", nil)
		}
	}
	done := make(chan error, 1)
	go func() { done <- agent.serveConnection(ctx, client, time.Second) }()
	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	_ = peer.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := peer.WriteJSON(wsMessage{Event: "auth.success"}); err != nil {
		t.Fatal(err)
	}
	var first wsMessage
	if err := peer.ReadJSON(&first); err != nil || first.Event != "first.report" {
		t.Fatalf("client queue was unavailable on publication: %v, %v", first, err)
	}
	var senders sync.WaitGroup
	for range 4 {
		senders.Go(func() {
			for range 40 {
				agent.SendDeviceReport(map[int][]string{1: {"127.0.0.1"}})
				agent.SendRaw("report", nil)
			}
		})
	}
	cancel()
	senders.Wait()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not close")
	}
	if agent.IsConnected() {
		t.Fatal("disconnected queue is still published")
	}
}

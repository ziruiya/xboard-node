package panel

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBatch2BDeviceSnapshotsSurviveQueueSaturationAndReconnect(t *testing.T) {
	w := NewWSClient("", "", 0, WSClientConfig{}, nil, nil, nil)
	w.session = make(chan wsMessage, 1)
	w.session <- wsMessage{Event: "pong"} // The generic queue is full.
	w.SendDeviceReportForNode(5, map[int][]string{1: {"192.0.2.1"}})
	w.SendDeviceReportForNode(5, map[int][]string{2: {"192.0.2.2"}})
	want := `{"devices":{"2":["192.0.2.2"]},"node_id":5}`
	sent := make(map[int]json.RawMessage)
	broken := errors.New("socket closed")
	if err := w.writeDeviceSnapshots(sent, func(msg wsMessage) error {
		if string(msg.Data) != want {
			t.Fatalf("failed write payload = %s, want %s", msg.Data, want)
		}
		return broken
	}); !errors.Is(err, broken) || len(sent) != 0 {
		t.Fatalf("failed write advanced cursor: sent=%v error=%v", sent, err)
	}
	var frames []string
	record := func(msg wsMessage) error {
		if msg.Event != WSEventReportDevices {
			t.Fatalf("unexpected event %q", msg.Event)
		}
		frames = append(frames, string(msg.Data))
		return nil
	}
	for _, cursor := range []map[int]json.RawMessage{sent, sent, make(map[int]json.RawMessage)} {
		if err := w.writeDeviceSnapshots(cursor, record); err != nil {
			t.Fatal(err)
		}
	}
	if len(frames) != 2 || frames[0] != want || frames[1] != want {
		t.Fatalf("expected one successful retry and one full replay, got %v", frames)
	}
	w.SendDeviceReportForNode(5, map[int][]string{})
	if err := w.writeDeviceSnapshots(sent, record); err != nil {
		t.Fatal(err)
	}
	if len(frames) != 3 || frames[2] != `{"devices":{},"node_id":5}` {
		t.Fatalf("empty full snapshot missing: %v", frames)
	}
}

func TestBatch2BWSReconnectReplaysFullDeviceSnapshot(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if errors.Is(err, os.ErrPermission) {
		t.Skipf("loopback socket unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	frames := make(chan wsMessage, 2)
	upgrader := websocket.Upgrader{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close() // Force a reconnect after receiving the first report.
		if conn.WriteJSON(wsMessage{Event: "auth.success"}) != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var frame wsMessage
		if conn.ReadJSON(&frame) == nil {
			select {
			case frames <- frame:
			default:
			}
		}
	})
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.Start()
	defer server.Close()
	client := NewWSClient("ws"+strings.TrimPrefix(server.URL, "http"), "test-token", 0,
		WSClientConfig{MachineID: 1, BackoffInitial: time.Millisecond, BackoffMax: time.Millisecond}, nil, nil, nil)
	client.SendDeviceReportForNode(5, map[int][]string{2: {"192.0.2.2"}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); client.Run(ctx) }()
	defer func() { cancel(); <-done }()
	for i := 0; i < 2; i++ {
		select {
		case frame := <-frames:
			if frame.Event != WSEventReportDevices || string(frame.Data) != `{"devices":{"2":["192.0.2.2"]},"node_id":5}` {
				t.Fatalf("session %d: wrong frame %+v", i, frame)
			}
		case <-ctx.Done():
			t.Fatalf("session %d: did not receive a full device snapshot", i)
		}
	}
}

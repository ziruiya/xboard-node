package panel

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestAliveCompleteRESTPayloads(t *testing.T) {
	bodies := make(chan map[string]json.RawMessage, 1)
	server, client := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		bodies <- body
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()
	for _, alive := range []map[int][]string{{}, {7: {"192.0.2.7"}}} {
		if err := client.PushAlive(alive); err != nil {
			t.Fatal(err)
		}
		body := <-bodies
		delete(body, "node_id")
		delete(body, "token")
		if string(body["alive_complete"]) != "true" || len(body) != 1+len(alive) {
			t.Fatalf("legacy alive endpoint payload = %s", body)
		}
		if len(alive) > 0 && string(body["7"]) != `["192.0.2.7"]` {
			t.Fatalf("legacy endpoint lost IPs: %s", body)
		}
		if err := client.Report(nil, alive, nil, 0, [2]uint64{}, [2]uint64{}, [2]uint64{}, nil, nil); err != nil {
			t.Fatal(err)
		}
		body = <-bodies
		want, _ := json.Marshal(alive)
		if string(body["alive_complete"]) != "true" || string(body["alive"]) != string(want) {
			t.Fatalf("consolidated report alive=%s marker=%s", body["alive"], body["alive_complete"])
		}
	}
}

func TestSingleNodeWSAliveComplete(t *testing.T) {
	client := NewWSClient("", "", 1, WSClientConfig{}, nil, nil, nil)
	sent := make(map[int]json.RawMessage)
	for _, tc := range []struct {
		alive map[int][]string
		want  string
	}{
		{map[int][]string{7: {"192.0.2.7"}}, `{"7":["192.0.2.7"],"alive_complete":true}`},
		{map[int][]string{}, `{"alive_complete":true}`},
	} {
		client.SendDeviceReport(tc.alive)
		calls := 0
		err := client.writeDeviceSnapshots(sent, func(msg wsMessage) error {
			calls++
			if msg.Event != WSEventReportDevices || string(msg.Data) != tc.want {
				t.Fatalf("device report = %s", msg.Data)
			}
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("device report writes = %d, error = %v", calls, err)
		}
	}
}

func TestRESTAndWSDecodeExactRevisions(t *testing.T) {
	const current = uint64(9007199254740993)
	server, client := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == clientConfigTestPath {
			fmt.Fprintf(w, `{"protocol":"vless","state_revision":%d}`, current)
		} else {
			fmt.Fprintf(w, `{"users":[],"state_revision":%d}`, current+1)
		}
	})
	defer server.Close()
	cfg, err := client.GetConfig()
	if err != nil || cfg.StateRevision == nil || *cfg.StateRevision != current {
		t.Fatalf("config revision lost: %+v, %v", cfg, err)
	}
	var userRev *uint64
	users, err := client.GetUsersWithRevision(&userRev)
	if err != nil || users == nil || userRev == nil || *userRev != current+1 {
		t.Fatalf("user revision/explicit empty lost: %v, %v", userRev, err)
	}
	calls := 0
	ws := NewWSClient("", "", 1, WSClientConfig{}, func(e WSEvent) {
		calls++
		if e.BaseRevision == nil || *e.BaseRevision != current || e.StateRevision == nil || *e.StateRevision != current+1 {
			t.Fatalf("wrong WS revisions: %+v", e)
		}
	}, nil, nil)
	ws.handleMessage(wsMessage{Event: WSEventSyncUserDelta, Data: json.RawMessage(fmt.Sprintf(
		`{"action":"add","users":[{"id":7}],"base_revision":%d,"state_revision":%d}`, current, current+1))})
	if calls != 1 {
		t.Fatal("delta did not decode")
	}
}

const clientConfigTestPath = "/api/v1/server/UniProxy/config"

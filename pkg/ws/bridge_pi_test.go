package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hypernewbie/phi/pkg/pty"
)

func TestBroadcastBridgeDeliversControlFrame(t *testing.T) {
	hub := NewHub(0)
	client := &Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-1", client)
	<-client.Send // ATTACH_HEAD
	hub.BroadcastBridge("pane-1", []byte(`{"type":"pi-bridge-settled"}`))
	select {
	case frame := <-client.Send:
		if len(frame) == 0 || frame[0] != 0x02 {
			t.Fatalf("frame type=%v, want 0x02", frame)
		}
		var control struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(frame[1:], &control); err != nil || control.Type != "pi-bridge-settled" {
			t.Fatalf("payload=%s err=%v", frame[1:], err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bridge frame not delivered")
	}
}

func TestBroadcastBridgeUnknownPaneIsNoop(t *testing.T) {
	hub := NewHub(0)
	hub.BroadcastBridge("no-such-pane", []byte(`{}`))
}

func TestBroadcastBridgeFullClientClosesForReconnect(t *testing.T) {
	hub := NewHub(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := Upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		client := &Client{Ws: conn, Send: make(chan []byte, 4)}
		hub.Register("pane-1", client)
		// Fill the queue; no writer drains it.
		for i := 0; i < 4; i++ {
			client.Send <- []byte{0x01}
		}
		hub.BroadcastBridge("pane-1", []byte(`{"type":"pi-bridge-settled"}`))
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("expected the congested connection to close")
	}
}

func TestBroadcastBridgeLeavesQueuedFramesUndisturbed(t *testing.T) {
	hub := NewHub(0)
	client := &Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-1", client)
	<-client.Send // ATTACH_HEAD
	queued := []byte{0x01, 'o', 'u', 't'}
	client.Send <- queued
	hub.BroadcastBridge("pane-1", []byte(`{}`))
	first := <-client.Send
	if len(first) == 0 || first[0] != 0x01 {
		t.Fatalf("queued output disturbed: %v", first)
	}
}

func TestBridgeAttachSnapshotFollowsTitleAndNeverBlocks(t *testing.T) {
	hub := NewHub(0)
	hub.SetBridgeAttach(func(snapshot pty.PTYInstanceSnapshot) []byte {
		return []byte(`{"type":"pi-bridge-state","pane_id":"` + snapshot.ID + `"}`)
	})
	client := &Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-1", client)
	snapshot := pty.PTYInstanceSnapshot{ID: "pane-1", Coder: "pi", SessionID: "native-id", Title: "Native title", TitleRevision: 4}
	if !hub.enqueuePiTitleSnapshot(snapshot, client) {
		t.Fatal("title snapshot not queued")
	}
	if !hub.enqueueBridgeSnapshot(snapshot, client) {
		t.Fatal("bridge snapshot not queued")
	}
	frames := [][]byte{<-client.Send, <-client.Send, <-client.Send}
	if frames[0][0] != 0x08 || frames[1][0] != 0x02 || frames[2][0] != 0x02 {
		t.Fatalf("attach order wrong: %v %v %v", frames[0][0], frames[1][0], frames[2][0])
	}
	var control struct {
		Type   string `json:"type"`
		PaneID string `json:"pane_id"`
	}
	if err := json.Unmarshal(frames[2][1:], &control); err != nil || control.Type != "pi-bridge-state" {
		t.Fatalf("bridge payload=%s err=%v", frames[2][1:], err)
	}
}

func TestBridgeAttachSnapshotNilSendsNothing(t *testing.T) {
	hub := NewHub(0)
	hub.SetBridgeAttach(func(pty.PTYInstanceSnapshot) []byte { return nil })
	client := &Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-1", client)
	if hub.enqueueBridgeSnapshot(pty.PTYInstanceSnapshot{ID: "pane-1"}, client) {
		t.Fatal("nil attach payload must send nothing")
	}
	if len(client.Send) != 1 {
		t.Fatalf("unexpected frames queued: %d", len(client.Send))
	}
}

func TestOnPaneExitHookDispatches(t *testing.T) {
	hub := NewHub(0)
	var got string
	var code int
	hub.SetOnPaneExit(func(paneID string, c int) {
		got, code = paneID, c
	})
	hub.notifyPaneExit("pane-9", 3)
	if got != "pane-9" || code != 3 {
		t.Fatalf("hook got (%q,%d)", got, code)
	}
	(&Hub{}).notifyPaneExit("pane-9", 0) // nil hook is a no-op
}

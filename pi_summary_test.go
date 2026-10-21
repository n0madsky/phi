package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
	"github.com/hypernewbie/phi/pkg/pty"
	"github.com/hypernewbie/phi/pkg/ws"
)

// Summary endpoint: POST /api/terminals/{id}/pi/summary. The fake peer
// speaks the bridge protocol over the test bridge socket.

func withSummaryPane(t *testing.T, coder string) (paneID string, b *Bridge) {
	t.Helper()
	withPaneManager(t)
	paneID = "pane-summary"
	ptyManager.AddInstanceForTesting(&pty.PTYInstance{
		ID:        paneID,
		Coder:     coder,
		SessionID: "sess-1",
	})
	b = startPiBridge(testBridgeHub())
	if b == nil {
		t.Fatal("bridge did not start")
	}
	t.Cleanup(b.Stop)
	orig := piBridge
	piBridge = b
	t.Cleanup(func() { piBridge = orig })
	return paneID, b
}

func provisionSummaryPeer(t *testing.T, b *Bridge, paneID string, caps []string, state pibridgepb.LogicalState) (env map[string]string, stream pibridgepb.Integration_ConnectClient) {
	t.Helper()
	var managed bool
	env, _, managed = b.Provision(paneID)
	if !managed {
		t.Fatal("provision not managed")
	}
	b.Complete(paneID)
	stream = dialBridge(t, b, paneID, env[piBridgeEnvProcess], env[piBridgeEnvToken])
	hello := &pibridgepb.PiToPhi{
		PaneId: paneID, ProcessId: env[piBridgeEnvProcess], RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_Hello{Hello: &pibridgepb.Hello{
			ProtocolVersion: 1, SessionId: "sess", RuntimeId: "rt",
			Capabilities: caps,
			CurrentState: &pibridgepb.StateSnapshot{
				State: pibridgepb.LogicalState(state), SessionId: "sess", LeafId: "leaf",
				Model:         &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"},
				StateRevision: 4,
			},
		}},
	}
	if err := stream.Send(hello); err != nil {
		t.Fatalf("hello: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("accepted: %v", err)
	}
	return env, stream
}

func summaryRequest(paneID, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/terminals/"+paneID+"/pi/summary", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "example.com"
	return req
}

func recvPhiToPi(t *testing.T, stream pibridgepb.Integration_ConnectClient) *pibridgepb.PhiToPi {
	t.Helper()
	type received struct {
		msg *pibridgepb.PhiToPi
		err error
	}
	ch := make(chan received, 1)
	go func() {
		msg, err := stream.Recv()
		ch <- received{msg: msg, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("recv: %v", res.err)
		}
		return res.msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server message")
		return nil
	}
}

func TestPiSummarySuccess(t *testing.T) {
	paneID, b := withSummaryPane(t, "pi")
	_, stream := provisionSummaryPeer(t, b, paneID,
		[]string{"pi.lifecycle.v1", "pi.summary.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_IDLE)

	w := httptest.NewRecorder()
	req := summaryRequest(paneID, `{"confirm":true}`)
	req.Header.Set("Origin", "http://example.com")
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleFallback(w, req)
	}()

	msg := recvPhiToPi(t, stream)
	sum := msg.GetSummarize()
	if sum == nil {
		t.Fatalf("expected summarize, got %+v", msg.GetPayload())
	}
	if sum.GetSessionId() != "sess" || sum.GetLeafId() != "leaf" || sum.GetStateRevision() != 4 {
		t.Fatalf("snapshot not from live lease: %+v", sum)
	}
	if sum.GetModel().GetProvider() != "p" || sum.GetModel().GetModel() != "m" {
		t.Fatalf("model not from live lease: %+v", sum.GetModel())
	}
	input, output, cost := uint64(10), uint64(5), 0.001
	result := &pibridgepb.PiToPhi{
		PaneId: paneID, ProcessId: msg.GetProcessId(), RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_SummaryResult{SummaryResult: &pibridgepb.SummaryResult{
			RequestId: sum.GetRequestId(), SessionId: "sess", LeafId: "leaf",
			Text:          "Summary text.",
			SelectedModel: sum.GetModel(),
			ReportedModel: &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"},
			Usage:         &pibridgepb.Usage{InputTokens: &input, OutputTokens: &output, CostUsd: &cost, CostFromPriceTables: true},
			StateRevision: 4,
		}},
	}
	if err := stream.Send(result); err != nil {
		t.Fatalf("result: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("cache header %q", w.Header().Get("Cache-Control"))
	}
	var body struct {
		RequestID string `json:"request_id"`
		Text      string `json:"text"`
		SessionID string `json:"session_id"`
		LeafID    string `json:"leaf_id"`
		Reported  struct {
			Provider string `json:"provider"`
			Model    string `json:"model"`
		} `json:"reported_model"`
		Usage struct {
			InputTokens  uint64  `json:"input_tokens"`
			OutputTokens uint64  `json:"output_tokens"`
			CostUSD      float64 `json:"cost_usd"`
			CostPriced   bool    `json:"cost_from_price_tables"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Text != "Summary text." || body.SessionID != "sess" || body.LeafID != "leaf" {
		t.Fatalf("body %+v", body)
	}
	if body.Reported.Provider != "p" || body.Reported.Model != "m" {
		t.Fatalf("reported %+v", body.Reported)
	}
	if body.Usage.InputTokens != 10 || body.Usage.OutputTokens != 5 || body.Usage.CostUSD != 0.001 || !body.Usage.CostPriced {
		t.Fatalf("usage %+v", body.Usage)
	}
	if body.RequestID == "" || body.RequestID == "0" {
		t.Fatalf("request id %q", body.RequestID)
	}
}

func TestPiSummaryValidation(t *testing.T) {
	paneID, b := withSummaryPane(t, "pi")
	_, stream := provisionSummaryPeer(t, b, paneID,
		[]string{"pi.lifecycle.v1", "pi.summary.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_IDLE)
	defer stream.CloseSend()

	cases := []struct {
		name  string
		path  string
		body  string
		setup func(*http.Request)
		want  int
	}{
		{"unknown pane", "/api/terminals/nope/pi/summary", `{"confirm":true}`, nil, http.StatusNotFound},
		{"bad json", "/api/terminals/" + paneID + "/pi/summary", `{bad`, nil, http.StatusBadRequest},
		{"missing confirm", "/api/terminals/" + paneID + "/pi/summary", `{}`, nil, http.StatusBadRequest},
		{"confirm false", "/api/terminals/" + paneID + "/pi/summary", `{"confirm":false}`, nil, http.StatusBadRequest},
		{"unknown field", "/api/terminals/" + paneID + "/pi/summary", `{"confirm":true,"extra":1}`, nil, http.StatusBadRequest},
		{"trailing garbage", "/api/terminals/" + paneID + "/pi/summary", `{"confirm":true} {}`, nil, http.StatusBadRequest},
		{"origin mismatch", "/api/terminals/" + paneID + "/pi/summary", `{"confirm":true}`,
			func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") }, http.StatusForbidden},
		{"wrong content type", "/api/terminals/" + paneID + "/pi/summary", `{"confirm":true}`,
			func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(c.body))
			req.Header.Set("Content-Type", "application/json")
			req.Host = "example.com"
			if c.setup != nil {
				c.setup(req)
			}
			w := httptest.NewRecorder()
			handleFallback(w, req)
			if w.Code != c.want {
				t.Fatalf("status %d, want %d: %s", w.Code, c.want, w.Body.String())
			}
		})
	}
}

func TestPiSummaryNonPiCoder(t *testing.T) {
	paneID, _ := withSummaryPane(t, "bash")
	w := httptest.NewRecorder()
	handleFallback(w, summaryRequest(paneID, `{"confirm":true}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestPiSummaryUnmanaged(t *testing.T) {
	paneID, _ := withSummaryPane(t, "pi")
	w := httptest.NewRecorder()
	handleFallback(w, summaryRequest(paneID, `{"confirm":true}`))
	if w.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409: %s", w.Code, w.Body.String())
	}
}

func TestPiSummaryBusyAndCapability(t *testing.T) {
	paneID, b := withSummaryPane(t, "pi")
	_, stream := provisionSummaryPeer(t, b, paneID,
		[]string{"pi.lifecycle.v1", "pi.summary.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_RUNNING)
	defer stream.CloseSend()
	w := httptest.NewRecorder()
	handleFallback(w, summaryRequest(paneID, `{"confirm":true}`))
	if w.Code != http.StatusConflict {
		t.Fatalf("busy status %d, want 409: %s", w.Code, w.Body.String())
	}

	pane2 := "pane-nocaps"
	ptyManager.AddInstanceForTesting(&pty.PTYInstance{ID: pane2, Coder: "pi", SessionID: "s2"})
	_, stream2 := provisionSummaryPeer(t, b, pane2,
		[]string{"pi.lifecycle.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_IDLE)
	defer stream2.CloseSend()
	w2 := httptest.NewRecorder()
	handleFallback(w2, summaryRequest(pane2, `{"confirm":true}`))
	if w2.Code != http.StatusConflict {
		t.Fatalf("caps status %d, want 409: %s", w2.Code, w2.Body.String())
	}
}

func TestPiSummaryExtensionError(t *testing.T) {
	paneID, b := withSummaryPane(t, "pi")
	_, stream := provisionSummaryPeer(t, b, paneID,
		[]string{"pi.lifecycle.v1", "pi.summary.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_IDLE)

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleFallback(w, summaryRequest(paneID, `{"confirm":true}`))
	}()
	msg := recvPhiToPi(t, stream)
	sum := msg.GetSummarize()
	if sum == nil {
		t.Fatalf("expected summarize, got %+v", msg.GetPayload())
	}
	errMsg := &pibridgepb.PiToPhi{
		PaneId: paneID, ProcessId: msg.GetProcessId(), RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_CommandError{CommandError: &pibridgepb.CommandError{
			RequestId: sum.GetRequestId(), Code: pibridgepb.CommandErrorCode_COMMAND_ERROR_MODEL_FAILURE,
			SafeMessage: "model request failed",
		}},
	}
	if err := stream.Send(errMsg); err != nil {
		t.Fatalf("error: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return")
	}
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502: %s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] != "model_failure" || body["message"] != "model request failed" {
		t.Fatalf("body %+v", body)
	}
}

func TestPiSummarySecondConcurrentRejected(t *testing.T) {
	paneID, b := withSummaryPane(t, "pi")
	_, stream := provisionSummaryPeer(t, b, paneID,
		[]string{"pi.lifecycle.v1", "pi.summary.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_IDLE)

	first := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		handleFallback(first, summaryRequest(paneID, `{"confirm":true}`))
	}()
	msg := recvPhiToPi(t, stream)
	if msg.GetSummarize() == nil {
		t.Fatalf("expected summarize, got %+v", msg.GetPayload())
	}
	second := httptest.NewRecorder()
	handleFallback(second, summaryRequest(paneID, `{"confirm":true}`))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status %d, want 429: %s", second.Code, second.Body.String())
	}
	var denied map[string]string
	if err := json.Unmarshal(second.Body.Bytes(), &denied); err != nil {
		t.Fatalf("429 body not JSON: %v", err)
	}
	if denied["error"] != "resource_exhausted" || denied["message"] == "" {
		t.Fatalf("429 body %+v", denied)
	}
	// Release the first with a cancel ack: peer acks a user cancel.
	cancel := &pibridgepb.PiToPhi{
		PaneId: paneID, ProcessId: msg.GetProcessId(), RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_CancelAcknowledged{CancelAcknowledged: &pibridgepb.CancelAcknowledged{
			RequestId: msg.GetSummarize().GetRequestId(), Disposition: pibridgepb.SummaryTermination_SUMMARY_TERMINATION_ENDED,
		}},
	}
	if err := stream.Send(cancel); err != nil {
		t.Fatalf("ack: %v", err)
	}
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not return")
	}
	if first.Code != http.StatusGatewayTimeout {
		t.Fatalf("first status %d, want 504: %s", first.Code, first.Body.String())
	}
}

func TestPiSummaryClientCancel(t *testing.T) {
	paneID, b := withSummaryPane(t, "pi")
	_, stream := provisionSummaryPeer(t, b, paneID,
		[]string{"pi.lifecycle.v1", "pi.summary.v1"}, pibridgepb.LogicalState_LOGICAL_STATE_IDLE)

	ctx, cancel := context.WithCancel(context.Background())
	req := summaryRequest(paneID, `{"confirm":true}`).WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleFallback(w, req)
	}()
	msg := recvPhiToPi(t, stream)
	if msg.GetSummarize() == nil {
		t.Fatalf("expected summarize, got %+v", msg.GetPayload())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after cancel")
	}
	cancelMsg := recvPhiToPi(t, stream)
	c := cancelMsg.GetCancel()
	if c == nil || c.GetRequestId() != msg.GetSummarize().GetRequestId() ||
		c.GetReason() != pibridgepb.CancelReason_CANCEL_REASON_USER {
		t.Fatalf("expected user cancel, got %+v", cancelMsg.GetPayload())
	}
}

func TestPiSummaryStatePush(t *testing.T) {
	hub := testBridgeHub()
	b := startPiBridge(hub)
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()
	orig := piBridge
	piBridge = b
	defer func() { piBridge = orig }()

	env, _, managed := b.Provision("pane-push")
	if !managed {
		t.Fatal("provision not managed")
	}
	b.Complete("pane-push")
	client := &ws.Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-push", client)
	<-client.Send // ATTACH_HEAD

	stream := dialBridge(t, b, "pane-push", env[piBridgeEnvProcess], env[piBridgeEnvToken])
	hello := &pibridgepb.PiToPhi{
		PaneId: "pane-push", ProcessId: env[piBridgeEnvProcess], RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_Hello{Hello: &pibridgepb.Hello{
			ProtocolVersion: 1, SessionId: "sess", RuntimeId: "rt",
			Capabilities: []string{"pi.lifecycle.v1", "pi.summary.v1"},
			CurrentState: &pibridgepb.StateSnapshot{
				State: pibridgepb.LogicalState_LOGICAL_STATE_IDLE, SessionId: "sess", LeafId: "leaf",
				Model: &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"},
			},
		}},
	}
	if err := stream.Send(hello); err != nil {
		t.Fatalf("hello: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("accepted: %v", err)
	}
	select {
	case frame := <-client.Send:
		var control map[string]string
		if err := json.Unmarshal(frame[1:], &control); err != nil {
			t.Fatalf("control decode: %v", err)
		}
		if control["type"] != "pi-bridge-state" || control["running"] != "false" ||
			control["model"] != "p/m" || control["capabilities"] != "pi.lifecycle.v1,pi.summary.v1" {
			t.Fatalf("control %+v", control)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no state push after hello")
	}

	// A later snapshot pushes a fresh view.
	snap := &pibridgepb.PiToPhi{
		PaneId: "pane-push", ProcessId: env[piBridgeEnvProcess], RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_StateSnapshot{StateSnapshot: &pibridgepb.StateSnapshot{
			State: pibridgepb.LogicalState_LOGICAL_STATE_RUNNING, SessionId: "sess", LeafId: "leaf",
			Model: &pibridgepb.ModelSnapshot{Provider: "q", Model: "n"},
		}},
	}
	if err := stream.Send(snap); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	select {
	case frame := <-client.Send:
		var control map[string]string
		if err := json.Unmarshal(frame[1:], &control); err != nil {
			t.Fatalf("control decode: %v", err)
		}
		if control["running"] != "true" || control["model"] != "q/n" {
			t.Fatalf("stale control %+v", control)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no state push after snapshot")
	}

	// HTTP and attach views carry the same identities.
	got := b.PaneSnapshot("pane-push")
	if got.Model != "q/n" || len(got.Capabilities) != 2 {
		t.Fatalf("snapshot %+v", got)
	}
	if got.Running == nil || *got.Running != true {
		t.Fatalf("snapshot running %+v", got.Running)
	}
}

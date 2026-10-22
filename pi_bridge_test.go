package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
	"github.com/hypernewbie/phi/pkg/pty"
	"github.com/hypernewbie/phi/pkg/ws"
)

func testBridgeHub() *ws.Hub {
	return ws.NewHub(0)
}

func TestBridgeStartStopCleansOwnedPaths(t *testing.T) {
	b := startPiBridge(testBridgeHub())
	if b == nil {
		t.Fatal("bridge did not start")
	}
	info, err := os.Stat(b.artifact)
	if err != nil {
		t.Fatalf("artifact not materialized: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode %o, want 0600", info.Mode().Perm())
	}
	socket := strings.TrimPrefix(b.endpoint.Target, "unix:")
	artifact := b.artifact
	b.Stop()
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket survives stop: %v", err)
	}
	if _, err := os.Stat(artifact); !os.IsNotExist(err) {
		t.Fatalf("artifact survives stop: %v", err)
	}
	b.Stop() // idempotent
}

func TestBridgeProvisionCompleteRevoke(t *testing.T) {
	b := startPiBridge(testBridgeHub())
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()

	env, args, managed := b.Provision("pane-1", true)
	if !managed {
		t.Fatal("provision not managed")
	}
	for _, key := range []string{piBridgeEnvTarget, piBridgeEnvPane, piBridgeEnvProcess, piBridgeEnvToken} {
		if env[key] == "" {
			t.Fatalf("env %s empty", key)
		}
	}
	if len(args) != 2 || args[0] != "--extension" || args[1] != b.artifact {
		t.Fatalf("extension args %v", args)
	}
	if len(env[piBridgeEnvToken]) != 64 {
		t.Fatalf("token len %d, want 64 hex", len(env[piBridgeEnvToken]))
	}

	// Second provision of the same pane is unmanaged and preserves
	// the live record: no new lease, no clobbered state, and the
	// first lease keeps its revoke path.
	_, _, managed = b.Provision("pane-1", true)
	if managed {
		t.Fatal("double provision managed")
	}
	if got := b.PaneSnapshot("pane-1"); got.State != piBridgeManaged {
		t.Fatalf("double provision state %+v, want managed", got)
	}
	if _, ok := b.server.PaneState(env[piBridgeEnvProcess]); !ok {
		t.Fatal("double provision orphaned the live lease")
	}

	b.Complete("pane-1")
	b.RevokePane("pane-1")
	if got := b.PaneSnapshot("pane-1"); got.State != piBridgePlain {
		t.Fatalf("revoked snapshot %+v, want plain", got)
	}
	b.RevokePane("pane-1") // idempotent
	b.Complete("unknown")  // ignored
	if got := b.PaneSnapshot("unknown"); got.State != piBridgePlain {
		t.Fatalf("unknown snapshot %+v, want plain", got)
	}
}

// TestBridgeMissingArtifactProvisionsUnavailable covers a bridge whose
// runtime failed to materialize (corrupt embed, unwritable dir): every
// pane launches unmanaged with a safe reason, never with extension args.
func TestBridgeMissingArtifactProvisionsUnavailable(t *testing.T) {
	b := startPiBridge(testBridgeHub())
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()
	b.artifact = "" // simulate materialize failure after startup
	b.reason = "runtime unavailable"
	env, args, managed := b.Provision("pane-1", true)
	if managed || env != nil || args != nil {
		t.Fatalf("missing artifact provisioned: managed=%v env=%v args=%v", managed, env, args)
	}
	if got := b.PaneSnapshot("pane-1"); got.State != piBridgeUnavailable || got.Reason == "" {
		t.Fatalf("missing artifact snapshot %+v, want unavailable with reason", got)
	}
	frame := b.encodeAttach(pty.PTYInstanceSnapshot{ID: "pane-1", Coder: "pi"})
	if frame == nil || !strings.Contains(string(frame), piBridgeUnavailable) {
		t.Fatalf("missing artifact attach %q, want unavailable state", frame)
	}
}

func TestBridgeNilSafe(t *testing.T) {
	var b *Bridge
	if _, _, managed := b.Provision("pane-1", true); managed {
		t.Fatal("nil bridge provisioned")
	}
	b.Complete("pane-1")
	b.RevokePane("pane-1")
	b.Stop()
	if got := b.PaneSnapshot("pane-1"); got.State != piBridgePlain {
		t.Fatalf("nil snapshot %+v, want plain", got)
	}
	if b.encodeAttach(pty.PTYInstanceSnapshot{ID: "pane-1", Coder: "pi"}) != nil {
		t.Fatal("nil bridge encoded attach")
	}
}

// dialBridge opens a raw client stream against a test bridge.
func dialBridge(t *testing.T, b *Bridge, pane, process, token string) pibridgepb.Integration_ConnectClient {
	t.Helper()
	socket := strings.TrimPrefix(b.endpoint.Target, "unix:")
	conn, err := grpc.NewClient("passthrough:///unused",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return net.Dial("unix", socket)
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	md := metadata.Pairs("phi-pi-pane", pane, "phi-pi-process", process, "phi-pi-token", token)
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := pibridgepb.NewIntegrationClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return stream
}

func TestBridgeSettlementNotifiesHub(t *testing.T) {
	hub := testBridgeHub()
	b := startPiBridge(hub)
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()

	env, _, managed := b.Provision("pane-1", true)
	if !managed {
		t.Fatal("provision not managed")
	}
	b.Complete("pane-1")

	client := &ws.Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-1", client)
	<-client.Send // ATTACH_HEAD

	stream := dialBridge(t, b, "pane-1", env[piBridgeEnvProcess], env[piBridgeEnvToken])
	hello := &pibridgepb.PiToPhi{
		PaneId: "pane-1", ProcessId: env[piBridgeEnvProcess], RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_Hello{Hello: &pibridgepb.Hello{
			ProtocolVersion: 1, SessionId: "sess", RuntimeId: "rt",
			Capabilities: []string{"pi.lifecycle.v1"},
			CurrentState: &pibridgepb.StateSnapshot{
				State: pibridgepb.LogicalState_LOGICAL_STATE_IDLE, SessionId: "sess",
			},
		}},
	}
	if err := stream.Send(hello); err != nil {
		t.Fatalf("hello: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("accepted: %v", err)
	}
	// Hello pushes a fresh bridge view before any settlement.
	select {
	case frame := <-client.Send:
		var state struct {
			Type         string `json:"type"`
			State        string `json:"state"`
			Running      string `json:"running"`
			Capabilities string `json:"capabilities"`
		}
		if err := json.Unmarshal(frame[1:], &state); err != nil {
			t.Fatalf("state decode: %v", err)
		}
		if state.Type != "pi-bridge-state" || state.State != "managed" ||
			state.Running != "false" || state.Capabilities != "pi.lifecycle.v1" {
			t.Fatalf("state %+v", state)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no state frame")
	}
	settled := &pibridgepb.PiToPhi{
		PaneId: "pane-1", ProcessId: env[piBridgeEnvProcess], RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_AgentSettled{AgentSettled: &pibridgepb.AgentSettled{
			EventSequence: 2, RunId: 7, Outcome: pibridgepb.RunOutcome_RUN_OUTCOME_COMPLETED,
		}},
	}
	if err := stream.Send(settled); err != nil {
		t.Fatalf("settled: %v", err)
	}

	select {
	case frame := <-client.Send:
		if len(frame) == 0 || frame[0] != 0x02 {
			t.Fatalf("frame type %v, want 0x02", frame)
		}
		var control struct {
			Type    string `json:"type"`
			PaneID  string `json:"pane_id"`
			Run     string `json:"run"`
			Outcome string `json:"outcome"`
		}
		if err := json.Unmarshal(frame[1:], &control); err != nil {
			t.Fatalf("control decode: %v", err)
		}
		if control.Type != "pi-bridge-settled" || control.PaneID != "pane-1" || control.Run != "7" || control.Outcome != "completed" {
			t.Fatalf("control %+v", control)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no settlement frame")
	}

	// Duplicate edge never notifies twice.
	if err := stream.Send(settled); err != nil {
		t.Fatalf("duplicate settled: %v", err)
	}
	select {
	case frame := <-client.Send:
		t.Fatalf("duplicate settlement notified: %s", frame[1:])
	case <-time.After(200 * time.Millisecond):
	}

	if got := b.PaneSnapshot("pane-1"); got.LastRun != "7" {
		t.Fatalf("snapshot %+v, want last_run 7", got)
	}
}

func TestBridgeStaleSettlementDropped(t *testing.T) {
	hub := testBridgeHub()
	b := startPiBridge(hub)
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()

	env, _, _ := b.Provision("pane-1", true)
	client := &ws.Client{Send: make(chan []byte, 8)}
	hub.AttachHot("pane-1", client)
	<-client.Send
	b.RevokePane("pane-1")

	b.onSettled("pane-1", env[piBridgeEnvProcess], 3, pibridgepb.RunOutcome_RUN_OUTCOME_COMPLETED)
	select {
	case frame := <-client.Send:
		t.Fatalf("stale settlement notified: %s", frame[1:])
	case <-time.After(200 * time.Millisecond):
	}
}

func TestBridgeAttachEncoding(t *testing.T) {
	hub := testBridgeHub()
	b := startPiBridge(hub)
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()

	if got := b.encodeAttach(pty.PTYInstanceSnapshot{ID: "pane-1", Coder: "bash"}); got != nil {
		t.Fatalf("non-pi attach encoded: %s", got)
	}
	if got := b.encodeAttach(pty.PTYInstanceSnapshot{ID: "unknown", Coder: "pi"}); got != nil {
		t.Fatalf("unknown pane attach encoded: %s", got)
	}
	env, _, _ := b.Provision("pane-1", true)
	payload := b.encodeAttach(pty.PTYInstanceSnapshot{ID: "pane-1", Coder: "pi"})
	var view map[string]string
	if err := json.Unmarshal(payload, &view); err != nil {
		t.Fatalf("attach decode: %v", err)
	}
	if view["type"] != "pi-bridge-state" || view["state"] != piBridgeManaged {
		t.Fatalf("attach %+v", view)
	}
	b.onSettled("pane-1", env[piBridgeEnvProcess], 11, pibridgepb.RunOutcome_RUN_OUTCOME_ABORTED)
	payload = b.encodeAttach(pty.PTYInstanceSnapshot{ID: "pane-1", Coder: "pi"})
	view = nil
	if err := json.Unmarshal(payload, &view); err != nil {
		t.Fatalf("attach decode: %v", err)
	}
	if view["last_run"] != "11" {
		t.Fatalf("attach catch-up %+v", view)
	}
}

func TestBridgeSnapshotCopiesRunningFlag(t *testing.T) {
	hub := testBridgeHub()
	b := startPiBridge(hub)
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()

	env, _, _ := b.Provision("pane-1", true)
	b.onState("pane-1", env[piBridgeEnvProcess], true, "p/m", []string{"pi.lifecycle.v1"})
	b.mu.Lock()
	p := b.panes["pane-1"]
	view := p.snapshot()
	// Simulate a future in-place update: a published view must not move.
	*p.lastRunning = false
	b.mu.Unlock()
	if view.running == nil || *view.running != true {
		t.Fatal("published snapshot shares the live running flag")
	}
}

func TestBridgeProvisionNoInject(t *testing.T) {
	b := startPiBridge(testBridgeHub())
	if b == nil {
		t.Fatal("bridge did not start")
	}
	defer b.Stop()

	// Manual install: the lease and credentials exist, but no
	// --extension args are returned for the spawn.
	env, args, managed := b.Provision("pane-1", false)
	if !managed {
		t.Fatal("provision not managed")
	}
	if env[piBridgeEnvToken] == "" || env[piBridgeEnvProcess] == "" {
		t.Fatal("manual provision lacks credentials")
	}
	if len(args) != 0 {
		t.Fatalf("no-inject provision returned args %v", args)
	}
}

func TestCompareStamp(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"0.1.0", "0.1.0", 0},
		{"0.2.0", "0.1.9", 1},
		{"0.1.0", "0.1.1", -1},
		{"1.0.0", "0.9.9", 1},
		{"0.1.0\n", "0.1.0", 0},
		{"0.1.0 hash-a", "0.1.0 hash-b", 0},
		{"0.1.0 hash-a", "0.1.1 hash-b", -1},
		{"garbage", "0.1.0", -1},
		{"0.1.0", "garbage", 1},
		{"", "", 0},
	}
	for _, c := range cases {
		if got := compareStamp(c.a, c.b); got != c.want {
			t.Fatalf("compareStamp(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestMaterializeStablePiBridge(t *testing.T) {
	home := withTempHome(t)
	stub := func() ([]byte, error) { return []byte("bundle-bytes"), nil }
	stableFile := func() string {
		return filepath.Join(home, piBridgeStableDir, piBridgeArtifactName)
	}
	stableVersion := func() string {
		raw, err := os.ReadFile(filepath.Join(home, piBridgeStableDir, "VERSION"))
		if err != nil {
			t.Fatalf("read stable VERSION: %v", err)
		}
		return strings.TrimSpace(string(raw))
	}

	// Fresh install writes bundle and version.
	if err := materializeStablePiBridgeAt(stub, "0.1.0"); err != nil {
		t.Fatalf("fresh install: %v", err)
	}
	if raw, _ := os.ReadFile(stableFile()); string(raw) != "bundle-bytes" {
		t.Fatal("stable bundle missing or wrong")
	}
	if stableVersion() != "0.1.0" {
		t.Fatal("stable version missing or wrong")
	}

	// Older installed version is replaced.
	newer := func() ([]byte, error) { return []byte("bundle-v2"), nil }
	if err := materializeStablePiBridgeAt(newer, "0.2.0"); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if raw, _ := os.ReadFile(stableFile()); string(raw) != "bundle-v2" {
		t.Fatal("upgrade did not replace bundle")
	}
	if stableVersion() != "0.2.0" {
		t.Fatal("upgrade did not replace version")
	}

	// Newer installed version is left alone, byte for byte.
	before, _ := os.ReadFile(stableFile())
	if err := materializeStablePiBridgeAt(stub, "0.1.0"); err != nil {
		t.Fatalf("downgrade guard: %v", err)
	}
	after, _ := os.ReadFile(stableFile())
	if string(before) != string(after) || stableVersion() != "0.2.0" {
		t.Fatal("newer install was clobbered")
	}

	// Corrupt installed version loses to a well-formed bundled one.
	if err := os.WriteFile(filepath.Join(home, piBridgeStableDir, "VERSION"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := materializeStablePiBridgeAt(newer, "0.2.0"); err != nil {
		t.Fatalf("corrupt recovery: %v", err)
	}
	if stableVersion() != "0.2.0" {
		t.Fatal("corrupt version not replaced")
	}

	// Unparsable bundled version fails loud without touching the install.
	if err := materializeStablePiBridgeAt(stub, "garbage"); err == nil {
		t.Fatal("unparsable bundled version accepted")
	}
	if stableVersion() != "0.2.0" {
		t.Fatal("failed upgrade clobbered the install")
	}

	// Same version, new content: the stamp decides, not the version
	// number. A rebuilt bundle must not sit stale behind an equal
	// version.
	if err := materializeStablePiBridgeAt(stub, "0.2.0 hash-b"); err != nil {
		t.Fatalf("same-version refresh: %v", err)
	}
	if raw, _ := os.ReadFile(stableFile()); string(raw) != "bundle-bytes" {
		t.Fatal("same-version rebuild did not replace the bundle")
	}
	if stableVersion() != "0.2.0 hash-b" {
		t.Fatalf("same-version rebuild did not update the stamp: %q", stableVersion())
	}

	// Identical stamp: nothing is written again.
	if err := materializeStablePiBridgeAt(newer, "0.2.0 hash-b"); err != nil {
		t.Fatalf("identical stamp: %v", err)
	}
	if raw, _ := os.ReadFile(stableFile()); string(raw) != "bundle-bytes" {
		t.Fatal("identical stamp replaced the bundle")
	}
	if stableVersion() != "0.2.0 hash-b" {
		t.Fatal("identical stamp changed the version file")
	}

	// A newer installed version still wins over an older bundled one,
	// even when the stamps differ.
	if err := materializeStablePiBridgeAt(stub, "0.1.0 hash-c"); err != nil {
		t.Fatalf("newer install guard: %v", err)
	}
	if raw, _ := os.ReadFile(stableFile()); string(raw) != "bundle-bytes" {
		t.Fatal("newer install was clobbered")
	}
	if stableVersion() != "0.2.0 hash-b" {
		t.Fatal("newer install lost its version")
	}
}

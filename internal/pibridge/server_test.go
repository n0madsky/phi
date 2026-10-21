package pibridge

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
)

// testServer starts a bridge Server on a native Unix socket in /tmp.
// It returns the server, socket path, and cleanup.
func testServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "phi-pi-test-")
	if err != nil {
		t.Fatalf("tmpdir: %v", err)
	}
	sock := filepath.Join(dir, "s.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("listen: %v", err)
	}
	s := NewServer()
	go func() { _ = s.GRPCServer().Serve(lis) }()
	// Wait for serve.
	time.Sleep(20 * time.Millisecond)
	return s, sock, func() {
		s.Stop()
		lis.Close()
		os.RemoveAll(dir)
	}
}

func dialUnix(t *testing.T, sock string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///unused",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return net.Dial("unix", sock)
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func outgoingCtx(pane, proc, token string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		metadataPaneKey, pane, metadataProcessKey, proc, metadataTokenKey, token,
	))
}

func openStream(t *testing.T, sock, pane, proc, token string) (pibridgepb.Integration_ConnectClient, context.CancelFunc) {
	t.Helper()
	conn := dialUnix(t, sock)
	client := pibridgepb.NewIntegrationClient(conn)
	ctx, cancel := context.WithTimeout(outgoingCtx(pane, proc, token), 10*time.Second)
	stream, err := client.Connect(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Connect: %v", err)
	}
	return stream, cancel
}

func testHello(pane, proc string, gen uint64, caps ...string) *pibridgepb.PiToPhi {
	return &pibridgepb.PiToPhi{
		PaneId: pane, ProcessId: proc, RuntimeGeneration: gen,
		Payload: &pibridgepb.PiToPhi_Hello{
			Hello: &pibridgepb.Hello{
				ProtocolVersion: ProtocolVersion, PackageVersion: "0.1.0", HostVersion: "1.3.0",
				SessionId: "sess", RuntimeId: "rt", Capabilities: caps,
				CurrentState: &pibridgepb.StateSnapshot{
					State:     pibridgepb.LogicalState_LOGICAL_STATE_IDLE,
					SessionId: "sess", LeafId: "leaf",
					Model: &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"},
				},
			},
		},
	}
}

func recvAccepted(t *testing.T, stream pibridgepb.Integration_ConnectClient) *pibridgepb.Accepted {
	t.Helper()
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv Accepted: %v", err)
	}
	acc := msg.GetAccepted()
	if acc == nil {
		t.Fatalf("first server message not Accepted: %v", msg.GetPayload())
	}
	return acc
}

func TestConnectWrongToken(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-auth")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-auth", proc, strings.Repeat("0", 64))
	defer cancel()
	_ = token
	if err := stream.Send(testHello("pane-auth", proc, 1, CapabilityLifecycle)); err != nil {
		return // send failed: also auth rejection evidence
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code=%v, want Unauthenticated", status.Code(err))
	}
}

func TestConnectPaneMismatch(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-a")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-b", proc, token)
	defer cancel()
	_ = stream.Send(testHello("pane-b", proc, 1, CapabilityLifecycle))
	_, err = stream.Recv()
	if got := status.Code(err); got != codes.PermissionDenied && got != codes.Unauthenticated {
		t.Fatalf("code=%v, want PermissionDenied/Unauthenticated", got)
	}
}

func TestConnectVersionRejected(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-ver")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-ver", proc, token)
	defer cancel()
	hello := testHello("pane-ver", proc, 1, CapabilityLifecycle)
	hello.GetHello().ProtocolVersion = 999
	if err := stream.Send(hello); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Server may send ProtocolError before the status; drain until error.
	for {
		_, err := stream.Recv()
		if err != nil {
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("code=%v, want FailedPrecondition", status.Code(err))
			}
			return
		}
	}
}

func TestHelloLifecycleHandshake(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-hi")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-hi", proc, token)
	defer cancel()
	if err := stream.Send(testHello("pane-hi", proc, 1, CapabilityLifecycle, CapabilitySummary)); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	acc := recvAccepted(t, stream)
	if acc.GetProtocolVersion() != ProtocolVersion {
		t.Fatalf("accepted version %d", acc.GetProtocolVersion())
	}
	if len(acc.GetCapabilities()) != 2 {
		t.Fatalf("accepted caps %v", acc.GetCapabilities())
	}
	// Lifecycle events are retained without closing the stream.
	if err := stream.Send(&pibridgepb.PiToPhi{
		PaneId: "pane-hi", ProcessId: proc, RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_AgentStarted{AgentStarted: &pibridgepb.AgentStarted{EventSequence: 1, RunId: 1}},
	}); err != nil {
		t.Fatalf("send started: %v", err)
	}
	if err := stream.Send(&pibridgepb.PiToPhi{
		PaneId: "pane-hi", ProcessId: proc, RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_AgentSettled{AgentSettled: &pibridgepb.AgentSettled{EventSequence: 2, RunId: 1, Outcome: pibridgepb.RunOutcome_RUN_OUTCOME_COMPLETED}},
	}); err != nil {
		t.Fatalf("send settled: %v", err)
	}
	// Unknown capability is ignored, not an error.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
}

func TestStaleGenerationRejected(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-gen")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	first, cancelFirst := openStream(t, sock, "pane-gen", proc, token)
	defer cancelFirst()
	if err := first.Send(testHello("pane-gen", proc, 2, CapabilityLifecycle)); err != nil {
		t.Fatalf("send gen2: %v", err)
	}
	recvAccepted(t, first)

	second, cancelSecond := openStream(t, sock, "pane-gen", proc, token)
	defer cancelSecond()
	if err := second.Send(testHello("pane-gen", proc, 1, CapabilityLifecycle)); err != nil {
		t.Fatalf("send gen1: %v", err)
	}
	for {
		_, err := second.Recv()
		if err != nil {
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("stale code=%v", status.Code(err))
			}
			return
		}
	}
}

func TestConflictingRuntimeRejected(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-dup")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	first, cancelFirst := openStream(t, sock, "pane-dup", proc, token)
	defer cancelFirst()
	if err := first.Send(testHello("pane-dup", proc, 1, CapabilityLifecycle)); err != nil {
		t.Fatalf("send: %v", err)
	}
	recvAccepted(t, first)
	second, cancelSecond := openStream(t, sock, "pane-dup", proc, token)
	defer cancelSecond()
	conflict := testHello("pane-dup", proc, 1, CapabilityLifecycle)
	conflict.GetHello().RuntimeId = "different-rt"
	if err := second.Send(conflict); err != nil {
		t.Fatalf("send conflict: %v", err)
	}
	for {
		_, err := second.Recv()
		if err != nil {
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("conflict code=%v", status.Code(err))
			}
			return
		}
	}
}

func TestReconnectReplacesStreamAtomically(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-re")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	first, cancelFirst := openStream(t, sock, "pane-re", proc, token)
	defer cancelFirst()
	if err := first.Send(testHello("pane-re", proc, 1, CapabilityLifecycle)); err != nil {
		t.Fatalf("send: %v", err)
	}
	recvAccepted(t, first)
	second, cancelSecond := openStream(t, sock, "pane-re", proc, token)
	defer cancelSecond()
	if err := second.Send(testHello("pane-re", proc, 1, CapabilityLifecycle)); err != nil {
		t.Fatalf("reconnect send: %v", err)
	}
	recvAccepted(t, second)
	// Old stream must be closed by replacement.
	_, err = first.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatalf("old stream code=%v, want Canceled", status.Code(err))
	}
	active, _, _ := s.Stats()
	if active != 1 {
		t.Fatalf("active streams %d, want 1", active)
	}
}

func TestSummarizeCorrelation(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-sum")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-sum", proc, token)
	defer cancel()
	if err := stream.Send(testHello("pane-sum", proc, 1, CapabilityLifecycle, CapabilitySummary)); err != nil {
		t.Fatalf("hello: %v", err)
	}
	recvAccepted(t, stream)
	id, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second)
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	if id == 0 {
		t.Fatal("zero request id allocated")
	}
	// Client receives Summarize and replies with unicode + large ids.
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv summarize: %v", err)
	}
	sum := msg.GetSummarize()
	if sum == nil || sum.GetRequestId() != id {
		t.Fatalf("expected summarize %d, got %+v", id, msg.GetPayload())
	}
	bigRun := uint64(9007199254740993) // 2^53+1, beyond JS safe range
	text := "Synthetic summary ↔ünicode—done ✓"
	reply := &pibridgepb.PiToPhi{
		PaneId: "pane-sum", ProcessId: proc, RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_SummaryResult{
			SummaryResult: &pibridgepb.SummaryResult{
				RequestId: id, SessionId: "sess", LeafId: "leaf", Text: text,
				SelectedModel: &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"},
				ReportedModel: &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"},
				Usage:         &pibridgepb.Usage{InputTokens: func() *uint64 { v := bigRun; return &v }()},
			},
		},
	}
	if err := stream.Send(reply); err != nil {
		t.Fatalf("send result: %v", err)
	}
	out, err := s.Await(proc, id, 5*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if out.Kind != OutcomeSummary || out.Text != text {
		t.Fatalf("outcome %+v", out)
	}
	if out.Usage == nil || out.Usage.GetInputTokens() != bigRun {
		t.Fatalf("uint64 not preserved: %+v", out.Usage)
	}
	// Duplicate result after completion is stale, not a new outcome.
	if err := stream.Send(reply); err != nil {
		t.Fatalf("send duplicate: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	l, _ := s.reg.lookupByProcess(proc)
	l.mu.Lock()
	stale := l.staleDrops
	l.mu.Unlock()
	if stale < 1 {
		t.Fatal("duplicate result not counted as stale")
	}
}

func TestCancelIdempotent(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-cancel")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-cancel", proc, token)
	defer cancel()
	if err := stream.Send(testHello("pane-cancel", proc, 1, CapabilityLifecycle, CapabilitySummary)); err != nil {
		t.Fatalf("hello: %v", err)
	}
	recvAccepted(t, stream)
	// Cancel unknown id is a no-op success.
	if err := s.Cancel(proc, 999, pibridgepb.CancelReason_CANCEL_REASON_USER); err != nil {
		t.Fatalf("cancel unknown: %v", err)
	}
	id, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second)
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	_, _ = stream.Recv() // Summarize
	if err := s.Cancel(proc, id, pibridgepb.CancelReason_CANCEL_REASON_USER); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv cancel: %v", err)
	}
	if msg.GetCancel() == nil || msg.GetCancel().GetRequestId() != id {
		t.Fatalf("expected cancel %d", id)
	}
	// Cancel does not free the slot; ack does.
	if _, _, _ = s.Stats(); true {
		l, _ := s.reg.lookupByProcess(proc)
		l.mu.Lock()
		_, stillPending := l.pending[id]
		l.mu.Unlock()
		if !stillPending {
			t.Fatal("cancel freed slot before ack")
		}
	}
	ack := &pibridgepb.PiToPhi{
		PaneId: "pane-cancel", ProcessId: proc, RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_CancelAcknowledged{CancelAcknowledged: &pibridgepb.CancelAcknowledged{RequestId: id, Disposition: pibridgepb.SummaryTermination_SUMMARY_TERMINATION_ENDED}},
	}
	if err := stream.Send(ack); err != nil {
		t.Fatalf("send ack: %v", err)
	}
	out, err := s.Await(proc, id, 5*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if out.Kind != OutcomeCancelAck {
		t.Fatalf("outcome %+v, want CancelAck", out)
	}
	// Second cancel after completion is still idempotent success.
	if err := s.Cancel(proc, id, pibridgepb.CancelReason_CANCEL_REASON_USER); err != nil {
		t.Fatalf("cancel after ack: %v", err)
	}
}

func TestSummarizeConcurrencyBounds(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	open := func(pane string) (string, pibridgepb.Integration_ConnectClient, context.CancelFunc) {
		proc, token, err := s.RegisterPending(pane)
		if err != nil {
			t.Fatalf("register %s: %v", pane, err)
		}
		s.Complete(proc)
		stream, cancel := openStream(t, sock, pane, proc, token)
		if err := stream.Send(testHello(pane, proc, 1, CapabilityLifecycle, CapabilitySummary)); err != nil {
			cancel()
			t.Fatalf("hello %s: %v", pane, err)
		}
		recvAccepted(t, stream)
		return proc, stream, cancel
	}
	procA, streamA, cancelA := open("pane-conc-a")
	defer cancelA()
	defer streamA.CloseSend()
	procB, streamB, cancelB := open("pane-conc-b")
	defer cancelB()
	defer streamB.CloseSend()
	procC, streamC, cancelC := open("pane-conc-c")
	defer cancelC()
	defer streamC.CloseSend()

	if _, err := s.Summarize(procA, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second on same pane must fail before provider work.
	if _, err := s.Summarize(procA, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("per-pane second code=%v, want ResourceExhausted", status.Code(err))
	}
	if _, err := s.Summarize(procB, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second); err != nil {
		t.Fatalf("second pane: %v", err)
	}
	// Third across server must fail (limit 2).
	if _, err := s.Summarize(procC, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("server third code=%v, want ResourceExhausted", status.Code(err))
	}
}

func TestSummarizeBusyAndStale(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-busy")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-busy", proc, token)
	defer cancel()
	hello := testHello("pane-busy", proc, 1, CapabilityLifecycle, CapabilitySummary)
	hello.GetHello().GetCurrentState().State = pibridgepb.LogicalState_LOGICAL_STATE_RUNNING
	if err := stream.Send(hello); err != nil {
		t.Fatalf("hello: %v", err)
	}
	recvAccepted(t, stream)
	if _, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("busy code=%v, want FailedPrecondition", status.Code(err))
	}
	// Publish idle snapshot, then stale session must fail.
	snap := &pibridgepb.PiToPhi{
		PaneId: "pane-busy", ProcessId: proc, RuntimeGeneration: 1,
		Payload: &pibridgepb.PiToPhi_StateSnapshot{
			StateSnapshot: &pibridgepb.StateSnapshot{State: pibridgepb.LogicalState_LOGICAL_STATE_IDLE, SessionId: "sess", LeafId: "leaf", StateRevision: 3},
		},
	}
	if err := stream.Send(snap); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := s.Summarize(proc, "other", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 3, 5*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale session code=%v", status.Code(err))
	}
	if _, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 99, 5*time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale revision code=%v", status.Code(err))
	}
}

func TestPendingLeaseNoCommands(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-pending")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	stream, cancel := openStream(t, sock, "pane-pending", proc, token)
	defer cancel()
	if err := stream.Send(testHello("pane-pending", proc, 1, CapabilityLifecycle, CapabilitySummary)); err != nil {
		t.Fatalf("hello: %v", err)
	}
	recvAccepted(t, stream)
	if _, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{}, 0, time.Second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("pending summarize code=%v, want FailedPrecondition", status.Code(err))
	}
	s.Complete(proc)
	if _, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second); err != nil {
		t.Fatalf("live summarize after complete: %v", err)
	}
}

func TestRevokeClosesStreamAndFailsPending(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	proc, token, err := s.RegisterPending("pane-revoke")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Complete(proc)
	stream, cancel := openStream(t, sock, "pane-revoke", proc, token)
	defer cancel()
	if err := stream.Send(testHello("pane-revoke", proc, 1, CapabilityLifecycle, CapabilitySummary)); err != nil {
		t.Fatalf("hello: %v", err)
	}
	recvAccepted(t, stream)
	id, err := s.Summarize(proc, "sess", "leaf", &pibridgepb.ModelSnapshot{Provider: "p", Model: "m"}, 0, 5*time.Second)
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	s.Revoke(proc)
	out, err := s.Await(proc, id, 2*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if out.Kind != OutcomeRevoked {
		t.Fatalf("outcome %+v, want Revoked", out)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatalf("stream code=%v, want Canceled", status.Code(err))
	}
}

func TestConcurrentClientsIndependent(t *testing.T) {
	s, sock, cleanup := testServer(t)
	defer cleanup()
	const n = 8
	type client struct {
		proc   string
		stream pibridgepb.Integration_ConnectClient
		cancel context.CancelFunc
	}
	clients := make([]client, 0, n)
	for i := 0; i < n; i++ {
		pane := "pane-iso-" + string(rune('a'+i))
		proc, token, err := s.RegisterPending(pane)
		if err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
		s.Complete(proc)
		stream, cancel := openStream(t, sock, pane, proc, token)
		hello := testHello(pane, proc, 1, CapabilityLifecycle)
		hello.GetHello().SessionId = "sess-" + pane
		hello.GetHello().RuntimeId = "rt-" + pane
		hello.GetHello().GetCurrentState().SessionId = "sess-" + pane
		if err := stream.Send(hello); err != nil {
			cancel()
			t.Fatalf("hello %d: %v", i, err)
		}
		recvAccepted(t, stream)
		clients = append(clients, client{proc: proc, stream: stream, cancel: cancel})
	}
	for i, c := range clients {
		seq := uint64(i + 1)
		if err := c.stream.Send(&pibridgepb.PiToPhi{
			PaneId: "pane-iso-" + string(rune('a'+i)), ProcessId: c.proc, RuntimeGeneration: 1,
			Payload: &pibridgepb.PiToPhi_AgentStarted{AgentStarted: &pibridgepb.AgentStarted{EventSequence: seq, RunId: seq}},
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	for i, c := range clients {
		l, ok := s.reg.lookupByProcess(c.proc)
		if !ok {
			t.Fatalf("lease %d missing", i)
		}
		_, _, _, last := func() ([]lifecycleRecord, bool, uint64, uint64) { return l.jrn.snapshot() }()
		if last != uint64(i+1) {
			t.Fatalf("client %d last seq %d", i, last)
		}
		c.cancel()
		_ = c.stream.CloseSend()
	}
}

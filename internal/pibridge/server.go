package pibridge

import (
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
)

// Server owns one local IPC gRPC service for provisioned Pi processes.
// It holds process leases, authenticates streams, enforces bounds, retains
// bounded lifecycle state, and correlates typed summary commands.
type Server struct {
	pibridgepb.UnimplementedIntegrationServer

	reg *registry

	mu              sync.Mutex
	activeSummaries int
	grpcServer      *grpc.Server
	onSettled       func(paneID, processID string, runID uint64, outcome pibridgepb.RunOutcome)
	onState         func(paneID, processID string, running bool, model string, caps []string)
}

// NewServer creates a bridge server with bounded gRPC limits and no TCP
// listener. Callers serve it on a private IPC listener from NewEndpoint.
func NewServer() *Server {
	s := &Server{reg: newRegistry()}
	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxHeaderListSize(maxMetadataBytes),
	)
	pibridgepb.RegisterIntegrationServer(s.grpcServer, s)
	return s
}

// GRPCServer exposes the configured gRPC server for Serve.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// SetOnSettled registers the callback fired once for each freshly appended
// settlement edge (duplicates and out-of-order edges never fire). Set before
// serving; the callback runs on a stream goroutine, must return quickly,
// and must not call back into the Server.
func (s *Server) SetOnSettled(fn func(paneID, processID string, runID uint64, outcome pibridgepb.RunOutcome)) {
	s.onSettled = fn
}

// SetOnState registers the callback fired after each accepted snapshot
// (hello or state) updates the stored lease state. Same contract as
// SetOnSettled; the callback receives copies, never shared slices.
func (s *Server) SetOnState(fn func(paneID, processID string, running bool, model string, caps []string)) {
	s.onState = fn
}

// fireState reports one accepted snapshot to the state callback. A nil
// snapshot (hello without state) still reports capabilities. A nil caps
// slice (state snapshot) keeps the last negotiated set server-side; the
// callback receives the stored set. It runs lock-free on a stream
// goroutine and never calls back into the Server.
func (s *Server) fireState(paneID, processID string, snap *pibridgepb.StateSnapshot, caps []string) {
	if s.onState == nil {
		return
	}
	if caps == nil {
		l, ok := s.reg.lookupByProcess(processID)
		if !ok {
			return
		}
		l.mu.Lock()
		stored := make([]string, len(l.caps))
		copy(stored, l.caps)
		l.mu.Unlock()
		caps = stored
	}
	running := snap != nil && snap.GetState() == pibridgepb.LogicalState_LOGICAL_STATE_RUNNING
	model := ""
	if m := snap.GetModel(); m != nil {
		model = m.GetProvider() + "/" + m.GetModel()
	}
	s.onState(paneID, processID, running, model, caps)
}

// Stop drains gracefully for shutdownDrainTimeout, then force-stops.
func (s *Server) Stop() {
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownDrainTimeout):
		s.grpcServer.Stop()
	}
}

// RegisterPending creates a pending lease for a pane before spawn. It
// returns the opaque process nonce and hex-encoded 32-byte token for the
// child environment only. Never log or persist the token.
func (s *Server) RegisterPending(paneID string) (processID, tokenHex string, err error) {
	if paneID == "" {
		return "", "", fmt.Errorf("pibridge: pane id required")
	}
	l, err := s.reg.registerPending(paneID)
	if err != nil {
		return "", "", err
	}
	return l.processID, hex.EncodeToString(l.token[:]), nil
}

// Complete marks a pending lease live after successful spawn.
func (s *Server) Complete(processID string) {
	s.reg.complete(processID)
}

// Revoke removes a lease, fails its pending commands, and closes its peer.
func (s *Server) Revoke(processID string) {
	cleared := s.reg.revokeWithCount(processID)
	if cleared > 0 {
		s.mu.Lock()
		s.activeSummaries -= cleared
		if s.activeSummaries < 0 {
			s.activeSummaries = 0
		}
		s.mu.Unlock()
	}
}

// Stats reports active streams and summary admissions for tests.
func (s *Server) Stats() (activeStreams, activeSummaries, leases int) {
	s.mu.Lock()
	activeSummaries = s.activeSummaries
	s.mu.Unlock()
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	leases = len(s.reg.byProcess)
	for _, l := range s.reg.byProcess {
		l.mu.Lock()
		if l.peer != nil {
			activeStreams++
		}
		l.mu.Unlock()
	}
	return activeStreams, activeSummaries, leases
}

// StateView is the nonsecret lease inspection surface for the host and
// tests. It carries no tokens, process nonces beyond the lookup key, or
// payloads. Session, leaf, model, capability, and runtime identities are
// the same values published to browsers and logs.
type StateView struct {
	HasPeer      bool
	HasState     bool
	State        pibridgepb.LogicalState
	Revision     uint64
	JournalFirst uint64
	JournalLast  uint64
	JournalGap   bool
	StaleDrops   int
	Pending      int
	Generation   uint64
	RuntimeID    string
	Caps         []string
	SessionID    string
	LeafID       string
	Model        string
}

// PaneState returns the nonsecret lease view for one process. It reports
// false when the process has no lease.
func (s *Server) paneStateLocked(l *lease) StateView {
	view := StateView{
		HasPeer:    l.peer != nil,
		StaleDrops: l.staleDrops,
		Pending:    len(l.pending),
		Generation: l.maxGen,
		RuntimeID:  l.runtimeID,
		SessionID:  l.sessionID,
	}
	view.Caps = append(view.Caps, l.caps...)
	if l.lastState != nil {
		view.HasState = true
		view.State = l.lastState.State
		view.Revision = l.lastState.StateRevision
		if l.lastState.SessionId != "" {
			view.SessionID = l.lastState.SessionId
		}
		view.LeafID = l.lastState.LeafId
		if m := l.lastState.Model; m != nil {
			view.Model = m.Provider + "/" + m.Model
		}
	}
	return view
}

func (s *Server) PaneState(processID string) (StateView, bool) {
	l, ok := s.reg.lookupByProcess(processID)
	if !ok {
		return StateView{}, false
	}
	l.mu.Lock()
	view := s.paneStateLocked(l)
	l.mu.Unlock()
	_, gap, first, last := l.jrn.snapshot()
	view.JournalFirst = first
	view.JournalLast = last
	view.JournalGap = gap
	return view, true
}

// InspectForTest is PaneState for the interop harness.
func (s *Server) InspectForTest(processID string) (StateView, bool) {
	return s.PaneState(processID)
}

// FloodForTest bypasses command admission and enqueues count dummy Cancel
// messages to exercise the slow-peer bound. It returns how many enqueued
// before overflow. Test-only; production code uses Cancel.
func (s *Server) FloodForTest(processID string, count int) int {
	l, ok := s.reg.lookupByProcess(processID)
	if !ok {
		return 0
	}
	l.mu.Lock()
	p := l.peer
	pane := l.paneID
	proc := l.processID
	var gen uint64
	if p != nil {
		gen = p.generation
	}
	l.mu.Unlock()
	if p == nil {
		return 0
	}
	sent := 0
	for i := 0; i < count; i++ {
		msg := &pibridgepb.PhiToPi{
			PaneId:            pane,
			ProcessId:         proc,
			RuntimeGeneration: gen,
			Payload: &pibridgepb.PhiToPi_Cancel{
				Cancel: &pibridgepb.Cancel{RequestId: uint64(1000000 + i), Reason: pibridgepb.CancelReason_CANCEL_REASON_USER},
			},
		}
		if !p.send(msg) {
			break
		}
		sent++
	}
	return sent
}

// OutcomeKind identifies the terminal result of an admitted summary.
type OutcomeKind int

const (
	OutcomeSummary OutcomeKind = iota + 1
	OutcomeError
	OutcomeCancelAck
	OutcomeRevoked
	OutcomeTimeout
)

// Outcome is the public terminal result of one summary command.
type Outcome struct {
	Kind      OutcomeKind
	RequestID uint64
	Text      string
	Code      pibridgepb.CommandErrorCode
	SafeMsg   string
	Terminate pibridgepb.SummaryTermination
	SessionID string
	LeafID    string
	Selected  *pibridgepb.ModelSnapshot
	Reported  *pibridgepb.ModelSnapshot
	Usage     *pibridgepb.Usage
}

// Summarize admits one synthetic summary command and sends it to the live
// peer. It checks liveness, capability, idle state, snapshot identity, and
// concurrency before provider work. It returns the allocated request ID;
// use Await for the terminal outcome.
func (s *Server) Summarize(processID, sessionID, leafID string, model *pibridgepb.ModelSnapshot, stateRevision uint64, deadline time.Duration) (uint64, error) {
	if deadline <= 0 {
		deadline = summaryDeadline
	}
	l, ok := s.reg.lookupByProcess(processID)
	if !ok {
		return 0, status.Error(codes.Unavailable, "unknown process")
	}
	l.mu.Lock()
	if l.state != leaseLive {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "lease not live")
	}
	p := l.peer
	last := l.lastState
	if p == nil {
		l.mu.Unlock()
		return 0, status.Error(codes.Unavailable, "no active stream")
	}
	if !p.hasCapability(CapabilitySummary) {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "summary capability not negotiated")
	}
	if last == nil {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "no lifecycle baseline")
	}
	if last.State == pibridgepb.LogicalState_LOGICAL_STATE_RUNNING {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "agent busy")
	}
	if last.SessionId != "" && sessionID != last.SessionId {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "stale session")
	}
	if last.LeafId != "" && leafID != "" && leafID != last.LeafId {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "stale leaf")
	}
	if stateRevision != last.StateRevision {
		l.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "stale revision")
	}
	if len(l.pending) >= maxSummaryPerPane {
		l.mu.Unlock()
		return 0, status.Error(codes.ResourceExhausted, "pane summary busy")
	}
	peerGen := p.generation
	peerPane := l.paneID
	peerProc := l.processID
	l.mu.Unlock()

	s.mu.Lock()
	if s.activeSummaries >= maxSummaryServer {
		s.mu.Unlock()
		return 0, status.Error(codes.ResourceExhausted, "server summary busy")
	}
	s.activeSummaries++
	s.mu.Unlock()

	l.mu.Lock()
	// Re-check under lease lock after global admission: revoke or replace
	// may have raced the first check.
	if l.state != leaseLive || l.peer == nil || l.peer.generation != peerGen {
		l.mu.Unlock()
		s.mu.Lock()
		s.activeSummaries--
		s.mu.Unlock()
		return 0, status.Error(codes.Unavailable, "stream replaced")
	}
	if len(l.pending) >= maxSummaryPerPane {
		l.mu.Unlock()
		s.mu.Lock()
		s.activeSummaries--
		s.mu.Unlock()
		return 0, status.Error(codes.ResourceExhausted, "pane summary busy")
	}
	if l.nextReqID == 0 {
		// Wrapped: stop and re-establish identity before reuse.
		l.mu.Unlock()
		s.mu.Lock()
		s.activeSummaries--
		s.mu.Unlock()
		return 0, status.Error(codes.FailedPrecondition, "request id wrap")
	}
	id := l.nextReqID
	l.nextReqID++
	ch := make(chan *commandOutcome, 1)
	l.pending[id] = &pendingReq{ch: ch, generation: peerGen}
	p = l.peer
	l.mu.Unlock()

	deadlineMs := uint64(deadline.Milliseconds())
	if deadlineMs == 0 {
		deadlineMs = 1
	}
	msg := &pibridgepb.PhiToPi{
		PaneId:            peerPane,
		ProcessId:         peerProc,
		RuntimeGeneration: peerGen,
		Payload: &pibridgepb.PhiToPi_Summarize{
			Summarize: &pibridgepb.Summarize{
				RequestId:     id,
				SessionId:     sessionID,
				LeafId:        leafID,
				Model:         model,
				DeadlineMs:    deadlineMs,
				StateRevision: stateRevision,
			},
		},
	}
	if !p.send(msg) {
		l.mu.Lock()
		delete(l.pending, id)
		l.mu.Unlock()
		s.mu.Lock()
		s.activeSummaries--
		s.mu.Unlock()
		if err := p.closeErr(); err != nil {
			return 0, err
		}
		return 0, status.Error(codes.Unavailable, "peer closed")
	}
	// Deadline enforcement sends Cancel but retains the admission slot
	// until terminal evidence or process exit.
	go func() {
		timer := time.NewTimer(deadline)
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = s.Cancel(processID, id, pibridgepb.CancelReason_CANCEL_REASON_DEADLINE)
		case <-p.done:
		}
	}()
	return id, nil
}

// Cancel requests cancellation of one admitted command. It is idempotent:
// unknown or completed IDs succeed without starting work or affecting
// another request. It never frees the admission slot; terminal evidence does.
func (s *Server) Cancel(processID string, requestID uint64, reason pibridgepb.CancelReason) error {
	if requestID == 0 {
		return status.Error(codes.InvalidArgument, "request id required")
	}
	l, ok := s.reg.lookupByProcess(processID)
	if !ok {
		return nil
	}
	l.mu.Lock()
	pend, exists := l.pending[requestID]
	p := l.peer
	if !exists || p == nil {
		l.mu.Unlock()
		return nil
	}
	if pend.generation != p.generation {
		// Old-runtime request: new runtime cannot cancel it. Retain the
		// slot until snapshot termination or process exit.
		l.mu.Unlock()
		return nil
	}
	if reason == pibridgepb.CancelReason_CANCEL_REASON_UNSPECIFIED {
		l.mu.Unlock()
		return status.Error(codes.InvalidArgument, "cancel reason required")
	}
	msg := &pibridgepb.PhiToPi{
		PaneId:            l.paneID,
		ProcessId:         l.processID,
		RuntimeGeneration: p.generation,
		Payload: &pibridgepb.PhiToPi_Cancel{
			Cancel: &pibridgepb.Cancel{RequestId: requestID, Reason: reason},
		},
	}
	l.mu.Unlock()
	_ = p.send(msg)
	return nil
}

// Await waits for the terminal outcome of one admitted request. A timeout
// reports OutcomeTimeout without freeing the admission slot; revocation
// reports OutcomeRevoked and frees it. An ID that already completed
// reports its retained outcome; only evicted or never-admitted IDs read
// as revoked.
func (s *Server) Await(processID string, requestID uint64, timeout time.Duration) (*Outcome, error) {
	l, ok := s.reg.lookupByProcess(processID)
	if !ok {
		return &Outcome{Kind: OutcomeRevoked, RequestID: requestID}, nil
	}
	l.mu.Lock()
	pend, exists := l.pending[requestID]
	recent := l.recent[requestID]
	l.mu.Unlock()
	if !exists {
		if recent != nil {
			return toPublicOutcome(recent), nil
		}
		// Evicted, stale, or never admitted. Callers treat unknown IDs
		// as revoked to avoid blocking on a freed slot.
		return &Outcome{Kind: OutcomeRevoked, RequestID: requestID}, nil
	}
	if timeout <= 0 {
		timeout = summaryDeadline
	}
	select {
	case out := <-pend.ch:
		return toPublicOutcome(out), nil
	case <-time.After(timeout):
		return &Outcome{Kind: OutcomeTimeout, RequestID: requestID}, nil
	}
}

func toPublicOutcome(out *commandOutcome) *Outcome {
	switch out.kind {
	case outcomeSummary:
		return &Outcome{Kind: OutcomeSummary, RequestID: out.request, Text: out.text, SessionID: out.session, LeafID: out.leaf, Selected: out.selected, Reported: out.reported, Usage: out.usage}
	case outcomeError:
		return &Outcome{Kind: OutcomeError, RequestID: out.request, Code: out.code, SafeMsg: out.safeMsg}
	case outcomeCancelAck:
		return &Outcome{Kind: OutcomeCancelAck, RequestID: out.request, Terminate: out.terminate}
	default:
		return &Outcome{Kind: OutcomeRevoked, RequestID: out.request}
	}
}

// deliverOutcome removes one pending request and releases its server slot.
// It returns false when the ID is unknown or already completed (stale).
// Completed outcomes are retained bounded so a late Await still reports
// them; only evicted or never-admitted IDs read as revoked.
func (s *Server) deliverOutcome(l *lease, id uint64, out *commandOutcome) bool {
	l.mu.Lock()
	pend, ok := l.pending[id]
	if !ok {
		l.staleDrops++
		l.mu.Unlock()
		return false
	}
	delete(l.pending, id)
	l.recent[id] = out
	l.recentOrder = append(l.recentOrder, id)
	for len(l.recentOrder) > maxRecentOutcomes {
		evict := l.recentOrder[0]
		l.recentOrder = l.recentOrder[1:]
		delete(l.recent, evict)
	}
	l.mu.Unlock()
	s.mu.Lock()
	s.activeSummaries--
	if s.activeSummaries < 0 {
		s.activeSummaries = 0
	}
	s.mu.Unlock()
	select {
	case pend.ch <- out:
	default:
	}
	return true
}

// noteStaleResult counts one stale or duplicate result without closing the
// healthy stream.
func noteStaleResult(l *lease) {
	l.mu.Lock()
	l.staleDrops++
	l.mu.Unlock()
}

// Connect implements the generated Integration service: one long-lived
// bidirectional stream per provisioned runtime.
func (s *Server) Connect(stream pibridgepb.Integration_ConnectServer) error {
	admission := time.Now()
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing credentials")
	}
	if err := checkMetadataSize(md); err != nil {
		return status.Error(codes.ResourceExhausted, "metadata too large")
	}
	panes := md.Get(metadataPaneKey)
	tokens := md.Get(metadataTokenKey)
	procs := md.Get(metadataProcessKey)
	if len(panes) != 1 || len(tokens) != 1 || len(procs) != 1 {
		return status.Error(codes.Unauthenticated, "credentials required")
	}
	pane, tokenHex, proc := panes[0], tokens[0], procs[0]
	if pane == "" || proc == "" || tokenHex == "" {
		return status.Error(codes.Unauthenticated, "credentials required")
	}
	l, ok := s.reg.lookupByProcess(proc)
	if !ok {
		return status.Error(codes.Unauthenticated, "unknown process")
	}
	if l.paneID != pane {
		return status.Error(codes.PermissionDenied, "pane mismatch")
	}
	if !l.authenticateToken(tokenHex) {
		return status.Error(codes.Unauthenticated, "bad token")
	}

	remaining := helloDeadline - time.Since(admission)
	if remaining <= 0 {
		return status.Error(codes.DeadlineExceeded, "hello deadline")
	}
	first, err := recvWithTimeout(stream, remaining)
	if err != nil {
		if status.Code(err) == codes.DeadlineExceeded {
			return status.Error(codes.DeadlineExceeded, "hello deadline")
		}
		return err
	}
	if first.GetPaneId() != pane || first.GetProcessId() != proc {
		return status.Error(codes.PermissionDenied, "identity mismatch")
	}
	if first.GetRuntimeGeneration() == 0 {
		return status.Error(codes.InvalidArgument, "generation required")
	}
	if proto.Size(first) > maxHelloBytes {
		return status.Error(codes.ResourceExhausted, "hello too large")
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first message must be hello")
	}
	if hello.GetProtocolVersion() != ProtocolVersion {
		_ = trySendProtocolError(stream, pane, proc, first.GetRuntimeGeneration(), pibridgepb.ProtocolErrorCode_PROTOCOL_ERROR_UNSUPPORTED_VERSION, "unsupported version")
		return status.Error(codes.FailedPrecondition, "unsupported version")
	}
	if hello.GetRuntimeId() == "" || hello.GetSessionId() == "" {
		return status.Error(codes.InvalidArgument, "runtime and session required")
	}
	caps := negotiateCapabilities(hello.GetCapabilities())

	l.mu.Lock()
	gen := first.GetRuntimeGeneration()
	if gen < l.maxGen {
		l.mu.Unlock()
		_ = trySendProtocolError(stream, pane, proc, gen, pibridgepb.ProtocolErrorCode_PROTOCOL_ERROR_STALE_GENERATION, "stale generation")
		return status.Error(codes.FailedPrecondition, "stale generation")
	}
	if gen == l.maxGen && l.maxGen != 0 {
		if l.runtimeID != hello.GetRuntimeId() || l.sessionID != hello.GetSessionId() {
			l.mu.Unlock()
			_ = trySendProtocolError(stream, pane, proc, gen, pibridgepb.ProtocolErrorCode_PROTOCOL_ERROR_DUPLICATE_RUNTIME, "conflicting runtime")
			return status.Error(codes.FailedPrecondition, "conflicting runtime")
		}
		if hello.GetCurrentState() != nil {
			if err := validateSnapshot(hello.GetCurrentState(), hello.GetSessionId()); err != nil {
				l.mu.Unlock()
				return status.Error(codes.InvalidArgument, err.Error())
			}
			l.lastState = hello.GetCurrentState()
		}
	} else {
		l.maxGen = gen
		l.runtimeID = hello.GetRuntimeId()
		l.sessionID = hello.GetSessionId()
		l.jrn.reset()
		if hello.GetCurrentState() != nil {
			if err := validateSnapshot(hello.GetCurrentState(), hello.GetSessionId()); err != nil {
				l.mu.Unlock()
				return status.Error(codes.InvalidArgument, err.Error())
			}
			l.lastState = hello.GetCurrentState()
			for _, r := range hello.GetCurrentState().GetJournal() {
				l.jrn.append(r.GetEventSequence(), r.GetRunId(), r.GetSettled(), r.GetOutcome())
			}
			if hello.GetCurrentState().GetJournalGap() {
				// Gap from the new runtime is retained in lastState;
				// the server journal starts fresh and fills via events.
			}
		} else {
			l.lastState = nil
		}
	}
	if l.state == leaseRevoked {
		l.mu.Unlock()
		return status.Error(codes.Canceled, "lease revoked")
	}
	oldPeer := l.peer
	p := newPeer(gen, hello.GetSessionId(), caps, stream)
	l.peer = p
	l.caps = l.caps[:0]
	for c := range caps {
		l.caps = append(l.caps, c)
	}
	slices.Sort(l.caps)
	l.mu.Unlock()

	if oldPeer != nil {
		oldPeer.closeWith(codes.Canceled, "replaced by reconnect")
	}
	go p.runWriter()

	acceptedCaps := make([]string, 0, len(caps))
	for c := range caps {
		acceptedCaps = append(acceptedCaps, c)
	}
	slices.Sort(acceptedCaps)
	accepted := &pibridgepb.PhiToPi{
		PaneId:            pane,
		ProcessId:         proc,
		RuntimeGeneration: gen,
		Payload: &pibridgepb.PhiToPi_Accepted{
			Accepted: &pibridgepb.Accepted{
				ProtocolVersion:   ProtocolVersion,
				Capabilities:      acceptedCaps,
				RuntimeGeneration: gen,
				RuntimeId:         hello.GetRuntimeId(),
			},
		},
	}
	if !p.send(accepted) {
		s.detachPeer(l, p)
		if err := p.closeErr(); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "peer closed")
	}
	s.fireState(pane, proc, hello.GetCurrentState(), acceptedCaps)

	for {
		msg, err := p.recv()
		if err != nil {
			s.detachPeer(l, p)
			if err == io.EOF {
				return nil
			}
			return err
		}
		if msg.GetPaneId() != pane || msg.GetProcessId() != proc || msg.GetRuntimeGeneration() != gen {
			s.detachPeer(l, p)
			_ = p.send(&pibridgepb.PhiToPi{
				PaneId: pane, ProcessId: proc, RuntimeGeneration: gen,
				Payload: &pibridgepb.PhiToPi_ProtocolError{
					ProtocolError: &pibridgepb.ProtocolError{Code: pibridgepb.ProtocolErrorCode_PROTOCOL_ERROR_BAD_IDENTITY, SafeMessage: "identity mismatch"},
				},
			})
			p.closeWith(codes.PermissionDenied, "identity mismatch")
			return status.Error(codes.PermissionDenied, "identity mismatch")
		}
		if msg.GetPayload() == nil {
			s.detachPeer(l, p)
			return status.Error(codes.InvalidArgument, "missing payload")
		}
		if err := s.handleMessage(l, p, msg); err != nil {
			s.detachPeer(l, p)
			return err
		}
	}
}

// detachPeer removes p from its lease when it is still the active peer.
// Journal, last state, and pending commands are preserved for reconnect.
func (s *Server) detachPeer(l *lease, p *peer) {
	l.mu.Lock()
	if l.peer == p {
		l.peer = nil
	}
	l.mu.Unlock()
	p.closeWith(codes.Canceled, "stream closed")
}

// handleMessage validates and applies one Pi → Phi message. Protocol
// violations close the stream with a gRPC status; stale command results
// are counted and ignored without closing healthy streams.
func (s *Server) handleMessage(l *lease, p *peer, msg *pibridgepb.PiToPhi) error {
	switch {
	case msg.GetHello() != nil:
		return status.Error(codes.InvalidArgument, "duplicate hello")
	case msg.GetAgentStarted() != nil:
		ev := msg.GetAgentStarted()
		if ev.GetEventSequence() == 0 || ev.GetRunId() == 0 {
			return status.Error(codes.InvalidArgument, "lifecycle ids required")
		}
		l.jrn.append(ev.GetEventSequence(), ev.GetRunId(), false, pibridgepb.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
		return nil
	case msg.GetAgentSettled() != nil:
		ev := msg.GetAgentSettled()
		if ev.GetEventSequence() == 0 || ev.GetRunId() == 0 {
			return status.Error(codes.InvalidArgument, "lifecycle ids required")
		}
		switch ev.GetOutcome() {
		case pibridgepb.RunOutcome_RUN_OUTCOME_UNSPECIFIED,
			pibridgepb.RunOutcome_RUN_OUTCOME_COMPLETED,
			pibridgepb.RunOutcome_RUN_OUTCOME_ERROR,
			pibridgepb.RunOutcome_RUN_OUTCOME_ABORTED:
		default:
			return status.Error(codes.InvalidArgument, "unknown outcome")
		}
		if l.jrn.append(ev.GetEventSequence(), ev.GetRunId(), true, ev.GetOutcome()) {
			// Fresh edge only: duplicates and out-of-order arrivals never
			// notify. No locks are held here; the callback must not call
			// back into the Server.
			if s.onSettled != nil {
				s.onSettled(l.paneID, l.processID, ev.GetRunId(), ev.GetOutcome())
			}
		}
		return nil
	case msg.GetStateSnapshot() != nil:
		snap := msg.GetStateSnapshot()
		if err := validateSnapshot(snap, p.sessionID); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		l.mu.Lock()
		l.lastState = snap
		// Snapshot termination evidence can release old or unresolved
		// slots without publishing a result.
		var toRelease []uint64
		for _, o := range snap.GetOutstandingSummaries() {
			if o.GetRequestId() == 0 {
				continue
			}
			if o.GetTermination() != pibridgepb.SummaryTermination_SUMMARY_TERMINATION_ENDED &&
				o.GetTermination() != pibridgepb.SummaryTermination_SUMMARY_TERMINATION_UNKNOWN {
				continue
			}
			if _, ok := l.pending[o.GetRequestId()]; ok {
				toRelease = append(toRelease, o.GetRequestId())
			}
		}
		released := make([]*pendingReq, 0, len(toRelease))
		releaseIDs := make([]uint64, 0, len(toRelease))
		for _, id := range toRelease {
			if pend, ok := l.pending[id]; ok {
				delete(l.pending, id)
				released = append(released, pend)
				releaseIDs = append(releaseIDs, id)
			}
		}
		l.mu.Unlock()
		if len(released) > 0 {
			s.mu.Lock()
			s.activeSummaries -= len(released)
			if s.activeSummaries < 0 {
				s.activeSummaries = 0
			}
			s.mu.Unlock()
			for i, pend := range released {
				select {
				case pend.ch <- &commandOutcome{kind: outcomeRevoked, request: releaseIDs[i]}:
				default:
				}
			}
		}
		s.fireState(l.paneID, l.processID, snap, nil)
		return nil
	case msg.GetSummaryResult() != nil:
		res := msg.GetSummaryResult()
		if res.GetRequestId() == 0 {
			return status.Error(codes.InvalidArgument, "request id required")
		}
		if err := checkSummaryText(res.GetText()); err != nil {
			return status.Error(codes.InvalidArgument, "summary text too large")
		}
		l.mu.Lock()
		pend, ok := l.pending[res.GetRequestId()]
		genOK := ok && pend.generation == p.generation
		l.mu.Unlock()
		if !ok || !genOK {
			noteStaleResult(l)
			return nil
		}
		s.deliverOutcome(l, res.GetRequestId(), &commandOutcome{
			kind: outcomeSummary, request: res.GetRequestId(), text: res.GetText(),
			session: res.GetSessionId(), leaf: res.GetLeafId(),
			selected: res.GetSelectedModel(), reported: res.GetReportedModel(), usage: res.GetUsage(),
		})
		return nil
	case msg.GetCommandError() != nil:
		res := msg.GetCommandError()
		if res.GetRequestId() == 0 {
			return status.Error(codes.InvalidArgument, "request id required")
		}
		if res.GetCode() == pibridgepb.CommandErrorCode_COMMAND_ERROR_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error code required")
		}
		if err := checkSafeMessage(res.GetSafeMessage()); err != nil {
			return status.Error(codes.InvalidArgument, "safe message too large")
		}
		l.mu.Lock()
		pend, ok := l.pending[res.GetRequestId()]
		genOK := ok && pend.generation == p.generation
		l.mu.Unlock()
		if !ok || !genOK {
			noteStaleResult(l)
			return nil
		}
		s.deliverOutcome(l, res.GetRequestId(), &commandOutcome{
			kind: outcomeError, request: res.GetRequestId(),
			code: res.GetCode(), safeMsg: res.GetSafeMessage(),
		})
		return nil
	case msg.GetCancelAcknowledged() != nil:
		res := msg.GetCancelAcknowledged()
		if res.GetRequestId() == 0 {
			return status.Error(codes.InvalidArgument, "request id required")
		}
		if res.GetDisposition() != pibridgepb.SummaryTermination_SUMMARY_TERMINATION_ENDED &&
			res.GetDisposition() != pibridgepb.SummaryTermination_SUMMARY_TERMINATION_UNKNOWN {
			return status.Error(codes.InvalidArgument, "terminal disposition required")
		}
		l.mu.Lock()
		pend, ok := l.pending[res.GetRequestId()]
		genOK := ok && pend.generation == p.generation
		l.mu.Unlock()
		if !ok || !genOK {
			noteStaleResult(l)
			return nil
		}
		s.deliverOutcome(l, res.GetRequestId(), &commandOutcome{
			kind: outcomeCancelAck, request: res.GetRequestId(), terminate: res.GetDisposition(),
		})
		return nil
	default:
		return status.Error(codes.InvalidArgument, "unsupported payload")
	}
}

func validateSnapshot(snap *pibridgepb.StateSnapshot, sessionID string) error {
	switch snap.GetState() {
	case pibridgepb.LogicalState_LOGICAL_STATE_UNSPECIFIED,
		pibridgepb.LogicalState_LOGICAL_STATE_IDLE,
		pibridgepb.LogicalState_LOGICAL_STATE_RUNNING:
	default:
		return fmt.Errorf("unknown logical state")
	}
	if snap.GetSessionId() != "" && sessionID != "" && snap.GetSessionId() != sessionID {
		return fmt.Errorf("snapshot session mismatch")
	}
	for _, r := range snap.GetJournal() {
		if r.GetEventSequence() == 0 || r.GetRunId() == 0 {
			return fmt.Errorf("journal ids required")
		}
	}
	for _, o := range snap.GetOutstandingSummaries() {
		if o.GetRequestId() == 0 {
			return fmt.Errorf("outstanding request id required")
		}
	}
	if len(snap.GetJournal()) > maxJournalRecords {
		return fmt.Errorf("journal too large")
	}
	return nil
}

func trySendProtocolError(stream pibridgepb.Integration_ConnectServer, pane, proc string, gen uint64, code pibridgepb.ProtocolErrorCode, msg string) error {
	return stream.Send(&pibridgepb.PhiToPi{
		PaneId:            pane,
		ProcessId:         proc,
		RuntimeGeneration: gen,
		Payload: &pibridgepb.PhiToPi_ProtocolError{
			ProtocolError: &pibridgepb.ProtocolError{Code: code, SafeMessage: msg},
		},
	})
}

// recvWithTimeout reads one message with a deadline from admission.
func recvWithTimeout(stream pibridgepb.Integration_ConnectServer, timeout time.Duration) (*pibridgepb.PiToPhi, error) {
	type res struct {
		msg *pibridgepb.PiToPhi
		err error
	}
	ch := make(chan res, 1)
	go func() {
		m, e := stream.Recv()
		ch <- res{msg: m, err: e}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.msg, r.err
	case <-timer.C:
		return nil, status.Error(codes.DeadlineExceeded, "hello deadline")
	case <-stream.Context().Done():
		return nil, status.Error(codes.Canceled, "stream done")
	}
}

// recv reads one message unless the peer closes first.
func (p *peer) recv() (*pibridgepb.PiToPhi, error) {
	type res struct {
		msg *pibridgepb.PiToPhi
		err error
	}
	ch := make(chan res, 1)
	go func() {
		m, e := p.stream.Recv()
		ch <- res{msg: m, err: e}
	}()
	select {
	case <-p.done:
		if err := p.closeErr(); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Canceled, "peer closed")
	case r := <-ch:
		return r.msg, r.err
	case <-p.stream.Context().Done():
		return nil, status.Error(codes.Canceled, "stream done")
	}
}

package main

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/hypernewbie/phi/internal/pibridge"
	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
	"github.com/hypernewbie/phi/pkg/pty"
	"github.com/hypernewbie/phi/pkg/ws"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Child environment keys for Pi bridge provisioning. The extension reads
// them at session start and deletes them so agent-spawned tools cannot
// inherit credentials. Keep in sync with bonus/pi-phi and PROTOCOL.md.
const (
	piBridgeEnvTarget  = "PHI_PI_BRIDGE_ENDPOINT"
	piBridgeEnvPane    = "PHI_PI_BRIDGE_PANE_ID"
	piBridgeEnvProcess = "PHI_PI_BRIDGE_PROCESS_ID"
	piBridgeEnvToken   = "PHI_PI_BRIDGE_TOKEN"
)

// piBridgeArtifactName is the materialized runtime filename inside the
// bridge endpoint directory. piBridgeEntryName is the materialized
// entry shim; --extension points at the shim, which requires the
// sibling bundle by this exact name.
const piBridgeArtifactName = "pi-phi-extension.cjs"
const piBridgeEntryName = "pi-phi-entry.ts"

// Bridge states published to the browser. "managed" means provisioned
// with credentials and extension args; "unavailable" means provisioning
// was attempted but failed (reason is nonsecret); "plain" means no
// provisioning was attempted for this pane.
const (
	piBridgeManaged     = "managed"
	piBridgeUnavailable = "unavailable"
	piBridgePlain       = "plain"
)

// Bridge owns the Pi↔Phi local-IPC server for this Phi process. It
// provisions per-pane leases before spawn, injects credentials and the
// materialized runtime path, revokes on every exit path, and publishes
// nonsecret bridge state plus settlement notifications to browsers.
type Bridge struct {
	server   *pibridge.Server
	endpoint *pibridge.Endpoint
	listener net.Listener
	hub      *ws.Hub
	artifact string
	reason   string

	mu    sync.Mutex
	panes map[string]*bridgePane
}

type bridgePane struct {
	processID   string
	managed     bool
	reason      string
	lastRun     uint64
	hasRun      bool
	lastOutcome string
	lastRunning *bool
	lastModel   string
	lastCaps    []string
}

// startPiBridge creates the endpoint, materializes the runtime, and
// serves the bridge. A listener failure returns nil so Pi startup
// proceeds without integration. A runtime failure keeps the server but
// marks every provisioning unavailable.
func startPiBridge(hub *ws.Hub) *Bridge {
	endpoint, err := pibridge.NewEndpoint()
	if err != nil {
		log.Printf("[pi-bridge] endpoint unavailable, integration disabled: %v", err)
		return nil
	}
	listener, err := endpoint.Listen()
	if err != nil {
		endpoint.Cleanup()
		log.Printf("[pi-bridge] listen failed, integration disabled: %v", err)
		return nil
	}
	b := &Bridge{
		server:   pibridge.NewServer(),
		endpoint: endpoint,
		listener: listener,
		hub:      hub,
		panes:    map[string]*bridgePane{},
	}
	if err := b.materialize(); err != nil {
		b.reason = "runtime unavailable"
		log.Printf("[pi-bridge] %v, launches proceed unmanaged: %v", b.reason, err)
	}
	b.server.SetOnSettled(b.onSettled)
	b.server.SetOnState(b.onState)
	hub.SetBridgeAttach(b.encodeAttach)
	hub.SetOnPaneExit(func(paneID string, _ int) { b.RevokePane(paneID) })
	go func() {
		_ = b.server.GRPCServer().Serve(listener)
	}()
	log.Printf("[pi-bridge] serving local IPC")
	return b
}

// materialize writes the embedded runtime and entry shim to the owned
// endpoint directory for Pi --extension loading. The files inherit the
// directory's private ownership; content is public bundle code. The
// artifact path points at the shim, which requires its sibling bundle.
func (b *Bridge) materialize() error {
	data, err := piPhiRuntime()
	if err != nil {
		return err
	}
	if _, err := b.endpoint.WriteOwnedFile(piBridgeArtifactName, data); err != nil {
		return err
	}
	entry, err := piPhiEntry()
	if err != nil {
		return err
	}
	path, err := b.endpoint.WriteOwnedFile(piBridgeEntryName, entry)
	if err != nil {
		return err
	}
	b.artifact = path
	return nil
}

// Stop revokes every pane, stops the server, and releases owned paths.
func (b *Bridge) Stop() {
	if b == nil {
		return
	}
	b.mu.Lock()
	panes := make([]*bridgePane, 0, len(b.panes))
	for _, p := range b.panes {
		panes = append(panes, p)
	}
	b.panes = map[string]*bridgePane{}
	b.mu.Unlock()
	for _, p := range panes {
		b.server.Revoke(p.processID)
	}
	b.server.Stop()
	_ = b.listener.Close()
	b.endpoint.Cleanup()
}

// Provision registers a pending lease for a pane before spawn. It
// returns child env overrides and extension args. When managed is
// false the caller launches normally without bridge injection, and
// the pane is recorded unavailable with a nonsecret reason.
func (b *Bridge) Provision(paneID string) (env map[string]string, args []string, managed bool) {
	if b == nil {
		return nil, nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	fail := func(reason string) (map[string]string, []string, bool) {
		b.panes[paneID] = &bridgePane{reason: reason}
		return nil, nil, false
	}
	if b.artifact == "" {
		return fail(b.reason)
	}
	if _, dup := b.panes[paneID]; dup {
		// Never clobber the live record: the first lease stays
		// authoritative and keeps its revoke path. The duplicate
		// launches unmanaged.
		return nil, nil, false
	}
	processID, token, err := b.server.RegisterPending(paneID)
	if err != nil {
		return fail("lease unavailable")
	}
	b.panes[paneID] = &bridgePane{processID: processID, managed: true}
	return map[string]string{
		piBridgeEnvTarget:  b.endpoint.Target,
		piBridgeEnvPane:    paneID,
		piBridgeEnvProcess: processID,
		piBridgeEnvToken:   token,
	}, []string{"--extension", b.artifact}, true
}

// Complete marks a provisioned lease live after successful spawn.
// Unknown panes are ignored.
func (b *Bridge) Complete(paneID string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	p, ok := b.panes[paneID]
	b.mu.Unlock()
	if !ok || !p.managed {
		return
	}
	b.server.Complete(p.processID)
}

// RevokePane revokes a pane's lease and drops its record. Unknown panes
// and nil bridges are ignored. Idempotent.
func (b *Bridge) RevokePane(paneID string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	p, ok := b.panes[paneID]
	if ok {
		delete(b.panes, paneID)
	}
	b.mu.Unlock()
	if ok && p.managed {
		b.server.Revoke(p.processID)
	}
}

// BridgePaneState is the nonsecret per-pane bridge view for HTTP and
// attach snapshots. RunID is decimal for JSON safety.
type BridgePaneState struct {
	State        string   `json:"state"`
	Reason       string   `json:"reason,omitempty"`
	Running      *bool    `json:"running,omitempty"`
	LastRun      string   `json:"last_run,omitempty"`
	LastOutcome  string   `json:"last_outcome,omitempty"`
	Model        string   `json:"model,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

// PaneSnapshot reports the bridge view for one pane. Unknown panes and
// nil bridges report plain.
func (b *Bridge) PaneSnapshot(paneID string) BridgePaneState {
	if b == nil {
		return BridgePaneState{State: piBridgePlain}
	}
	b.mu.Lock()
	p, ok := b.panes[paneID]
	b.mu.Unlock()
	if !ok {
		return BridgePaneState{State: piBridgePlain}
	}
	if !p.managed {
		return BridgePaneState{State: piBridgeUnavailable, Reason: p.reason}
	}
	st := BridgePaneState{State: piBridgeManaged}
	if p.hasRun {
		st.LastRun = strconv.FormatUint(p.lastRun, 10)
		st.LastOutcome = p.lastOutcome
	}
	if view, ok := b.server.PaneState(p.processID); ok && view.HasState {
		running := view.State == pibridgepb.LogicalState_LOGICAL_STATE_RUNNING
		st.Running = &running
		st.Model = view.Model
		st.Capabilities = append(st.Capabilities, view.Caps...)
	}
	return st
}

// onState pushes a fresh bridge view after each accepted snapshot so
// browsers track running state, model, and capabilities without
// polling. Stale callbacks for revoked panes are dropped. It never
// calls back into the Server; all live values arrive as arguments.
func (b *Bridge) onState(paneID, processID string, running bool, model string, caps []string) {
	b.mu.Lock()
	p, ok := b.panes[paneID]
	var view bridgeStateView
	if ok && p.managed && p.processID == processID {
		r := running
		p.lastRunning = &r
		p.lastModel = model
		p.lastCaps = append(p.lastCaps[:0], caps...)
		view = p.snapshot()
	} else {
		ok = false
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	if payload := encodeBridgeState(paneID, view); payload != nil {
		b.hub.BroadcastBridge(paneID, payload)
	}
}

// bridgeStateView is a lock-free copy of one pane's retained view.
type bridgeStateView struct {
	hasRun      bool
	lastRun     uint64
	lastOutcome string
	running     *bool
	model       string
	caps        []string
}

// snapshot copies the retained view. The caller must hold b.mu.
func (p *bridgePane) snapshot() bridgeStateView {
	return bridgeStateView{
		hasRun:      p.hasRun,
		lastRun:     p.lastRun,
		lastOutcome: p.lastOutcome,
		running:     p.lastRunning,
		model:       p.lastModel,
		caps:        append([]string(nil), p.lastCaps...),
	}
}

// encodeBridgeState renders the managed bridge view from a copy.
func encodeBridgeState(paneID string, view bridgeStateView) []byte {
	frame := map[string]string{"type": "pi-bridge-state", "pane_id": paneID, "state": piBridgeManaged}
	if view.hasRun {
		frame["last_run"] = strconv.FormatUint(view.lastRun, 10)
		frame["last_outcome"] = view.lastOutcome
	}
	if view.running != nil {
		frame["running"] = strconv.FormatBool(*view.running)
	}
	if view.model != "" {
		frame["model"] = view.model
	}
	if len(view.caps) > 0 {
		frame["capabilities"] = strings.Join(view.caps, ",")
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return nil
	}
	return payload
}

// onSettled retains the latest settlement per pane and notifies
// browsers. Stale callbacks for revoked panes are dropped.
func (b *Bridge) onSettled(paneID, processID string, runID uint64, outcome pibridgepb.RunOutcome) {
	b.mu.Lock()
	p, ok := b.panes[paneID]
	if ok && p.managed && p.processID == processID {
		p.lastRun, p.hasRun = runID, true
		p.lastOutcome = outcomeName(outcome)
	} else {
		ok = false
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	payload, err := json.Marshal(map[string]string{
		"type":    "pi-bridge-settled",
		"pane_id": paneID,
		"run":     strconv.FormatUint(runID, 10),
		"outcome": outcomeName(outcome),
	})
	if err != nil {
		return
	}
	b.hub.BroadcastBridge(paneID, payload)
}

func outcomeName(outcome pibridgepb.RunOutcome) string {
	switch outcome {
	case pibridgepb.RunOutcome_RUN_OUTCOME_COMPLETED:
		return "completed"
	case pibridgepb.RunOutcome_RUN_OUTCOME_ABORTED:
		return "aborted"
	case pibridgepb.RunOutcome_RUN_OUTCOME_ERROR:
		return "error"
	default:
		return "unknown"
	}
}

// encodeAttach renders the bridge attach snapshot for pi panes with a
// bridge record. Other panes get no frame. The retained last run lets
// a reconnecting browser catch up on a settlement it never saw.
func (b *Bridge) encodeAttach(snapshot pty.PTYInstanceSnapshot) []byte {
	if b == nil || snapshot.Coder != "pi" {
		return nil
	}
	b.mu.Lock()
	p, ok := b.panes[snapshot.ID]
	var view bridgeStateView
	var managed bool
	var reason string
	if ok {
		managed = p.managed
		reason = p.reason
		if managed {
			view = p.snapshot()
		}
	}
	b.mu.Unlock()
	if !ok {
		return nil
	}
	frame := map[string]string{"type": "pi-bridge-state", "pane_id": snapshot.ID}
	if !managed {
		frame["state"] = piBridgeUnavailable
		if reason != "" {
			frame["reason"] = reason
		}
		payload, err := json.Marshal(frame)
		if err != nil {
			return nil
		}
		return payload
	}
	return encodeBridgeState(snapshot.ID, view)
}

// piBridgeForCoder reports whether a coder ID gets bridge provisioning.
// Only interactive Pi PTY tabs are managed; shells and other backends
// never receive credentials or extension args.
func piBridgeForCoder(coderID string) bool {
	return coderID == "pi"
}

// Max paid-summary request body. The body carries only an explicit
// confirmation flag; snapshot identities come from live lease state.
const piSummaryMaxBody = 8 << 10

// handlePiSummary runs one explicit, user-confirmed summary for a
// managed Pi pane: POST /api/terminals/{id}/pi/summary with
// {"confirm":true}. Snapshot identities come from the live lease,
// never the browser, so a stale tab cannot summarize a session the
// user did not see. Client disconnect cancels the bridge command.
func handlePiSummary(w http.ResponseWriter, r *http.Request, id string) {
	w.Header().Set("Cache-Control", "no-store")
	if !sameOrigin(r) {
		http.Error(w, "origin mismatch", http.StatusForbidden)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct == "" || !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		http.Error(w, "JSON body required", http.StatusBadRequest)
		return
	}
	var req struct {
		Confirm bool `json:"confirm"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, piSummaryMaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if !req.Confirm {
		http.Error(w, "explicit confirmation required", http.StatusBadRequest)
		return
	}
	inst, ok := ptyManager.Get(id)
	if !ok || inst == nil {
		http.Error(w, "terminal instance not found", http.StatusNotFound)
		return
	}
	if inst.Coder != "pi" {
		http.Error(w, "summary is only available for Pi tabs", http.StatusBadRequest)
		return
	}
	if piBridge == nil {
		http.Error(w, "bridge unavailable", http.StatusServiceUnavailable)
		return
	}
	processID, managed, reason := piBridge.liveProcess(id)
	if !managed {
		if reason == "" {
			reason = "pane is not bridge-managed"
		}
		http.Error(w, reason, http.StatusConflict)
		return
	}
	view, ok := piBridge.server.PaneState(processID)
	if !ok || !view.HasState {
		http.Error(w, "no bridge state yet", http.StatusConflict)
		return
	}
	if !slices.Contains(view.Caps, pibridge.CapabilitySummary) {
		http.Error(w, "summary capability not negotiated", http.StatusConflict)
		return
	}
	var model *pibridgepb.ModelSnapshot
	if view.Model != "" {
		if provider, name, found := strings.Cut(view.Model, "/"); found {
			model = &pibridgepb.ModelSnapshot{Provider: provider, Model: name}
		}
	}
	requestID, err := piBridge.server.Summarize(processID, view.SessionID, view.LeafID, model, view.Revision, 0)
	if err != nil {
		writeBridgeStatus(w, err)
		return
	}
	type awaited struct {
		outcome *pibridge.Outcome
		err     error
	}
	done := make(chan awaited, 1)
	go func() {
		outcome, err := piBridge.server.Await(processID, requestID, 0)
		done <- awaited{outcome: outcome, err: err}
	}()
	select {
	case <-r.Context().Done():
		_ = piBridge.server.Cancel(processID, requestID, pibridgepb.CancelReason_CANCEL_REASON_USER)
		return
	case res := <-done:
		if res.err != nil {
			writeBridgeStatus(w, res.err)
			return
		}
		writeSummaryOutcome(w, res.outcome)
	}
}

// liveProcess returns the process ID for a managed pane.
func (b *Bridge) liveProcess(paneID string) (processID string, managed bool, reason string) {
	if b == nil {
		return "", false, "bridge unavailable"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.panes[paneID]
	if !ok || !p.managed {
		if ok && p.reason != "" {
			return "", false, p.reason
		}
		return "", false, ""
	}
	return p.processID, true, ""
}

// sameOrigin reports whether a state-changing request comes from the
// served origin. Browsers attach Origin (or Referer) to POSTs; its
// absence marks a non-browser client, which auth already gates.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = r.Header.Get("Referer")
	}
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return u.Host == r.Host
}

// writeBridgeStatus maps bridge admission errors to JSON statuses,
// the same shape as outcome errors so browsers keep the message.
func writeBridgeStatus(w http.ResponseWriter, err error) {
	code := status.Code(err)
	msg := status.Convert(err).Message()
	switch code {
	case codes.FailedPrecondition:
		writeJSONError(w, http.StatusConflict, "failed_precondition", msg)
	case codes.ResourceExhausted:
		writeJSONError(w, http.StatusTooManyRequests, "resource_exhausted", msg)
	case codes.Unavailable:
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", msg)
	default:
		writeJSONError(w, http.StatusInternalServerError, "internal", "summary failed")
	}
}

// writeSummaryOutcome renders one terminal bridge outcome as JSON.
// Error messages are the extension's bounded safe strings.
func writeSummaryOutcome(w http.ResponseWriter, outcome *pibridge.Outcome) {
	if outcome == nil {
		http.Error(w, "summary failed", http.StatusInternalServerError)
		return
	}
	switch outcome.Kind {
	case pibridge.OutcomeSummary:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(summaryView{
			RequestID: strconv.FormatUint(outcome.RequestID, 10),
			Text:      outcome.Text,
			SessionID: outcome.SessionID,
			LeafID:    outcome.LeafID,
			Selected:  newModelView(outcome.Selected),
			Reported:  newModelView(outcome.Reported),
			Usage:     newUsageView(outcome.Usage),
		})
	case pibridge.OutcomeError:
		writeCommandError(w, outcome.Code, outcome.SafeMsg)
	case pibridge.OutcomeCancelAck, pibridge.OutcomeTimeout:
		writeJSONError(w, http.StatusGatewayTimeout, "deadline", "summary deadline expired")
	default:
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "bridge process is gone")
	}
}

type summaryView struct {
	RequestID string     `json:"request_id"`
	Text      string     `json:"text"`
	SessionID string     `json:"session_id,omitempty"`
	LeafID    string     `json:"leaf_id,omitempty"`
	Selected  *modelView `json:"selected_model,omitempty"`
	Reported  *modelView `json:"reported_model,omitempty"`
	Usage     *usageView `json:"usage,omitempty"`
}

type modelView struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

func newModelView(m *pibridgepb.ModelSnapshot) *modelView {
	if m == nil {
		return nil
	}
	return &modelView{Provider: m.GetProvider(), Model: m.GetModel()}
}

type usageView struct {
	InputTokens  *uint64  `json:"input_tokens,omitempty"`
	OutputTokens *uint64  `json:"output_tokens,omitempty"`
	CostUSD      *float64 `json:"cost_usd,omitempty"`
	CostPriced   bool     `json:"cost_from_price_tables"`
}

func newUsageView(u *pibridgepb.Usage) *usageView {
	if u == nil {
		return nil
	}
	view := &usageView{CostPriced: u.GetCostFromPriceTables()}
	if u.InputTokens != nil {
		view.InputTokens = u.InputTokens
	}
	if u.OutputTokens != nil {
		view.OutputTokens = u.OutputTokens
	}
	if u.CostUsd != nil {
		view.CostUSD = u.CostUsd
	}
	if view.InputTokens == nil && view.OutputTokens == nil && view.CostUSD == nil {
		return nil
	}
	return view
}

// writeCommandError maps typed bridge failures to HTTP statuses.
func writeCommandError(w http.ResponseWriter, code pibridgepb.CommandErrorCode, safeMsg string) {
	if safeMsg == "" {
		safeMsg = "summary failed"
	}
	switch code {
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_BUSY,
		pibridgepb.CommandErrorCode_COMMAND_ERROR_STALE_SNAPSHOT,
		pibridgepb.CommandErrorCode_COMMAND_ERROR_UNSUPPORTED_CAPABILITY,
		pibridgepb.CommandErrorCode_COMMAND_ERROR_CANCELED,
		pibridgepb.CommandErrorCode_COMMAND_ERROR_DUPLICATE_REQUEST:
		writeJSONError(w, http.StatusConflict, commandErrorName(code), safeMsg)
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_MODEL_FAILURE:
		writeJSONError(w, http.StatusBadGateway, commandErrorName(code), safeMsg)
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_DEADLINE_EXPIRED:
		writeJSONError(w, http.StatusGatewayTimeout, commandErrorName(code), safeMsg)
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_LIMIT:
		writeJSONError(w, http.StatusUnprocessableEntity, commandErrorName(code), safeMsg)
	default:
		writeJSONError(w, http.StatusInternalServerError, commandErrorName(code), safeMsg)
	}
}

func commandErrorName(code pibridgepb.CommandErrorCode) string {
	switch code {
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_BUSY:
		return "busy"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_STALE_SNAPSHOT:
		return "stale"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_UNSUPPORTED_CAPABILITY:
		return "unsupported"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_MODEL_FAILURE:
		return "model_failure"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_CANCELED:
		return "canceled"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_DEADLINE_EXPIRED:
		return "deadline"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_LIMIT:
		return "limit"
	case pibridgepb.CommandErrorCode_COMMAND_ERROR_DUPLICATE_REQUEST:
		return "duplicate"
	default:
		return "internal"
	}
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": message})
}

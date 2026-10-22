package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hypernewbie/phi/internal/pibridge"
	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
	"github.com/hypernewbie/phi/pkg/pty"
	"github.com/hypernewbie/phi/pkg/ws"
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
// bridge endpoint directory. --extension points at it directly; Pi loads
// the self-contained CJS bundle without a loader shim.
const piBridgeArtifactName = "pi-phi-extension.cjs"

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

	mu     sync.Mutex
	panes  map[string]*bridgePane
	closed bool
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
	// The stable copy serves manual installs; its failure never
	// affects injection, so it only logs.
	if err := materializeStablePiBridge(); err != nil {
		log.Printf("[pi-bridge] stable copy unavailable: %v", err)
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

// materialize writes the embedded runtime to the owned endpoint
// directory for Pi --extension loading. The file inherits the
// directory's private ownership; content is public bundle code.
func (b *Bridge) materialize() error {
	data, err := piPhiRuntime()
	if err != nil {
		return err
	}
	path, err := b.endpoint.WriteOwnedFile(piBridgeArtifactName, data)
	if err != nil {
		return err
	}
	b.artifact = path
	return nil
}

// piBridgeStableDir is the phi-owned directory holding the stable
// bridge bundle copy for manual Pi installs. Unlike the endpoint
// copy, this path survives restarts, so a settings file-path entry
// keeps working across upgrades.
const piBridgeStableDir = ".phi/pi-bridge"

// compareDotVersion compares dot-separated numeric versions
// ("0.1.0"). Non-numeric suffixes are ignored. Unparsable input
// sorts below everything, so a corrupt installed version always
// loses to a well-formed bundled one.
func compareDotVersion(a, b string) int {
	pa, oka := parseDotVersion(a)
	pb, okb := parseDotVersion(b)
	if !oka || !okb {
		switch {
		case oka:
			return 1
		case okb:
			return -1
		default:
			return 0
		}
	}
	for i := 0; i < len(pa) && i < len(pb); i++ {
		if pa[i] != pb[i] {
			if pa[i] < pb[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(pa) < len(pb):
		return -1
	case len(pa) > len(pb):
		return 1
	default:
		return 0
	}
}

func parseDotVersion(s string) ([]int, bool) {
	var out []int
	for _, part := range strings.Split(strings.TrimSpace(s), ".") {
		num := ""
		for _, r := range part {
			if r < '0' || r > '9' {
				break
			}
			num += string(r)
		}
		if num == "" {
			return nil, false
		}
		n, err := strconv.Atoi(num)
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// materializeStablePiBridge keeps the stable bundle copy current:
// it rewrites the directory only when the embedded stamp is newer
// than the installed one, or when the stamps differ at the same
// version (new content, same version number). An up-to-date copy
// costs one small file read per phi start.
func materializeStablePiBridge() error {
	if testing.Testing() {
		// Tests cover this through materializeStablePiBridgeAt with
		// a temp home. The real home must stay untouched: the repo
		// isolates tests from ~/.phi the same way (see config.go).
		return nil
	}
	return materializeStablePiBridgeAt(piPhiRuntime, strings.TrimSpace(piPhiVersionAsset))
}

func materializeStablePiBridgeAt(runtime func() ([]byte, error), version string) error {
	if _, ok := parseDotVersion(version); !ok {
		return fmt.Errorf("pi-phi: bundled version %q unparsable", version)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("pi-phi: home: %w", err)
	}
	dir := filepath.Join(home, piBridgeStableDir)
	if raw, err := os.ReadFile(filepath.Join(dir, "VERSION")); err == nil {
		installed := strings.TrimSpace(string(raw))
		// The stamp carries the version and the content hash, so a
		// matching stamp means a matching bundle. A newer installed
		// version still wins and is left alone.
		if compareDotVersion(version, installed) < 0 {
			return nil
		}
		if installed == version {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("pi-phi: read stable version: %w", err)
	}
	data, err := runtime()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("pi-phi: stable dir: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(dir, piBridgeArtifactName), data, 0o600); err != nil {
		return err
	}
	// VERSION lands last: a crash between the two files leaves the
	// old version gating a rewrite on the next start, never a new
	// version atop old bytes.
	return writeFileAtomic(filepath.Join(dir, "VERSION"), []byte(version+"\n"), 0o600)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("pi-phi: temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("pi-phi: write temp: %w", err)
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("pi-phi: chmod temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("pi-phi: close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("pi-phi: replace: %w", err)
	}
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
	b.closed = true
	b.mu.Unlock()
	for _, p := range panes {
		b.server.Revoke(p.processID)
	}
	b.server.Stop()
	_ = b.listener.Close()
	b.endpoint.Cleanup()
}

// Provision registers a pending lease for a pane before spawn. It
// returns child env overrides and extension args. When inject is
// false (manual install), the lease and credentials are still
// provisioned but no --extension args are returned. When managed is
// false the caller launches normally without bridge injection, and
// the pane is recorded unavailable with a nonsecret reason.
func (b *Bridge) Provision(paneID string, inject bool) (env map[string]string, args []string, managed bool) {
	if b == nil {
		return nil, nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		// Shutdown raced provisioning: launch unmanaged without
		// recording, so no lease leaks past the cleared map.
		return nil, nil, false
	}
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
		// launches unmanaged. Pane IDs are unique per spawn, so a
		// duplicate indicates a caller bug; log it loudly.
		log.Printf("[pi-bridge] duplicate provision for pane %q, launching unmanaged", paneID)
		return nil, nil, false
	}
	processID, token, err := b.server.RegisterPending(paneID)
	if err != nil {
		return fail("lease unavailable")
	}
	b.panes[paneID] = &bridgePane{processID: processID, managed: true}
	env = map[string]string{
		piBridgeEnvTarget:  b.endpoint.Target,
		piBridgeEnvPane:    paneID,
		piBridgeEnvProcess: processID,
		piBridgeEnvToken:   token,
	}
	if inject {
		args = []string{"--extension", b.artifact}
	}
	return env, args, true
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

// snapshot copies the retained view. The caller must hold b.mu. The
// running flag is copied by value so later updates never mutate a
// published view.
func (p *bridgePane) snapshot() bridgeStateView {
	var running *bool
	if p.lastRunning != nil {
		r := *p.lastRunning
		running = &r
	}
	return bridgeStateView{
		hasRun:      p.hasRun,
		lastRun:     p.lastRun,
		lastOutcome: p.lastOutcome,
		running:     running,
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
	if b == nil || !piBridgeForCoder(snapshot.Coder) {
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

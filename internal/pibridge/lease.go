package pibridge

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
	"google.golang.org/grpc/codes"
)

// tokenBytes is the per-process credential size.
const tokenBytes = 32

// randomHex returns n cryptographically random bytes, hex-encoded.
func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("pibridge: rand: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// lease is the registered process identity for one provisioned Pi process.
// A lease exists before spawn (pending), becomes live after spawn, and is
// revoked on failure, death, replacement, or shutdown.
//
// mu guards every mutable field. Registry.mu guards only the lookup maps.
// Acquire registry.mu before lease.mu, never the reverse.
type lease struct {
	paneID    string
	processID string
	token     [tokenBytes]byte

	mu        sync.Mutex
	state     leaseState
	maxGen    uint64   // highest accepted runtime generation
	runtimeID string   // identity bound at maxGen
	sessionID string   // identity bound at maxGen
	caps      []string // accepted capabilities at maxGen, retained

	nextReqID uint64 // Phi-issued command IDs, monotonic, never reset
	jrn       journal
	lastState *pibridgepb.StateSnapshot
	peer      *peer // current active stream, replaced atomically
	pending   map[uint64]*pendingReq
	// recent retains bounded terminal outcomes so an Await that loses
	// the race with a fast peer still reports the outcome, not a
	// phantom revocation. recentOrder evicts oldest-first.
	recent      map[uint64]*commandOutcome
	recentOrder []uint64
	staleDrops  int
}

// pendingReq is one admitted command awaiting terminal evidence.
// Generation binds the request to the runtime that must execute it;
// results from another generation are stale and cannot free the slot.
type pendingReq struct {
	ch         chan *commandOutcome
	generation uint64
}

type leaseState int

const (
	leasePending leaseState = iota
	leaseLive
	leaseRevoked
)

// commandOutcome is the terminal result of one Phi-issued command.
// Exactly one outcome is delivered per admitted request; duplicates and
// stale results are counted and ignored without closing the stream.
type commandOutcome struct {
	kind      outcomeKind
	request   uint64
	text      string
	code      pibridgepb.CommandErrorCode
	safeMsg   string
	terminate pibridgepb.SummaryTermination
	session   string
	leaf      string
	selected  *pibridgepb.ModelSnapshot
	reported  *pibridgepb.ModelSnapshot
	usage     *pibridgepb.Usage
}

type outcomeKind int

const (
	outcomeSummary outcomeKind = iota
	outcomeError
	outcomeCancelAck
	outcomeRevoked
)

// registry owns all leases, keyed by process identity and pane.
type registry struct {
	mu        sync.Mutex
	byProcess map[string]*lease
	byPane    map[string]*lease
}

func newRegistry() *registry {
	return &registry{
		byProcess: map[string]*lease{},
		byPane:    map[string]*lease{},
	}
}

// registerPending creates a lease with a random process nonce and token
// before spawn. Pane identity must not already hold a live or pending lease.
func (r *registry) registerPending(paneID string) (*lease, error) {
	process, err := randomHex(16)
	if err != nil {
		return nil, err
	}
	var token [tokenBytes]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, fmt.Errorf("pibridge: token: %w", err)
	}
	r.mu.Lock()
	if old, ok := r.byPane[paneID]; ok {
		old.mu.Lock()
		revoked := old.state == leaseRevoked
		old.mu.Unlock()
		if !revoked {
			r.mu.Unlock()
			return nil, fmt.Errorf("pibridge: pane %q already has an active lease", paneID)
		}
	}
	l := &lease{
		paneID:    paneID,
		processID: process,
		token:     token,
		state:     leasePending,
		nextReqID: 1,
		pending:   map[uint64]*pendingReq{},
		recent:    map[uint64]*commandOutcome{},
	}
	r.byProcess[process] = l
	r.byPane[paneID] = l
	r.mu.Unlock()
	return l, nil
}

// complete marks a pending lease live after a successful spawn.
func (r *registry) complete(processID string) {
	r.mu.Lock()
	l, ok := r.byProcess[processID]
	r.mu.Unlock()
	if !ok {
		return
	}
	l.mu.Lock()
	if l.state == leasePending {
		l.state = leaseLive
	}
	l.mu.Unlock()
}

// revoke removes a lease, fails its pending commands, and closes its
// active peer. Idempotent.
func (r *registry) revoke(processID string) {
	_ = r.revokeWithCount(processID)
}

// revokeWithCount is revoke that reports how many admission slots it freed.
func (r *registry) revokeWithCount(processID string) int {
	r.mu.Lock()
	l, ok := r.byProcess[processID]
	if !ok {
		r.mu.Unlock()
		return 0
	}
	delete(r.byProcess, processID)
	if cur, paneOK := r.byPane[l.paneID]; paneOK && cur == l {
		delete(r.byPane, l.paneID)
	}
	r.mu.Unlock()

	l.mu.Lock()
	l.state = leaseRevoked
	peer := l.peer
	l.peer = nil
	pending := l.pending
	l.pending = map[uint64]*pendingReq{}
	l.recent = map[uint64]*commandOutcome{}
	l.recentOrder = nil
	l.mu.Unlock()

	for id, pend := range pending {
		select {
		case pend.ch <- &commandOutcome{kind: outcomeRevoked, request: id}:
		default:
		}
	}
	if peer != nil {
		peer.closeWith(codes.Canceled, "lease revoked")
	}
	return len(pending)
}

// lookupByProcess returns the lease for a process identity.
func (r *registry) lookupByProcess(processID string) (*lease, bool) {
	r.mu.Lock()
	l, ok := r.byProcess[processID]
	r.mu.Unlock()
	if !ok {
		return nil, false
	}
	l.mu.Lock()
	revoked := l.state == leaseRevoked
	l.mu.Unlock()
	return l, !revoked
}

// lookupByPane returns the lease for a pane identity.
func (r *registry) lookupByPane(paneID string) (*lease, bool) {
	r.mu.Lock()
	l, ok := r.byPane[paneID]
	r.mu.Unlock()
	if !ok {
		return nil, false
	}
	l.mu.Lock()
	revoked := l.state == leaseRevoked
	l.mu.Unlock()
	return l, !revoked
}

// authenticateToken compares a hex token against the lease credential in
// constant time. It returns false for malformed input.
func (l *lease) authenticateToken(hexToken string) bool {
	raw, err := hex.DecodeString(strings.ToLower(hexToken))
	if err != nil || len(raw) != tokenBytes {
		return false
	}
	var provided [tokenBytes]byte
	copy(provided[:], raw)
	return subtle.ConstantTimeCompare(provided[:], l.token[:]) == 1
}

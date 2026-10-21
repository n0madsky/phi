// Command testserver is the M1 Go ↔ TypeScript interop harness. It owns one
// local IPC endpoint and bridge Server, then executes JSON control ops from
// stdin and reports JSON results on stdout. Request IDs cross the control
// surface as decimal strings to preserve uint64 range. Never log tokens.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc/status"

	"github.com/hypernewbie/phi/internal/pibridge"
	pibridgepb "github.com/hypernewbie/phi/internal/pibridge/pb"
)

type request struct {
	Op         string `json:"op"`
	Pane       string `json:"pane"`
	Process    string `json:"process"`
	Session    string `json:"session"`
	Leaf       string `json:"leaf"`
	Provider   string `json:"provider"`
	Model      string `json:"model"`
	Revision   string `json:"revision"`
	RequestID  string `json:"requestId"`
	Reason     int32  `json:"reason"`
	DeadlineMs int64  `json:"deadlineMs"`
	TimeoutMs  int64  `json:"timeoutMs"`
	Count      int    `json:"count"`
}

type response struct {
	OK           bool   `json:"ok"`
	Error        string `json:"error,omitempty"`
	Code         int    `json:"code,omitempty"`
	Process      string `json:"process,omitempty"`
	Token        string `json:"token,omitempty"`
	RequestID    string `json:"requestId,omitempty"`
	Kind         string `json:"kind,omitempty"`
	Text         string `json:"text,omitempty"`
	SafeMsg      string `json:"safeMsg,omitempty"`
	Terminate    int32  `json:"terminate,omitempty"`
	Sent         int    `json:"sent,omitempty"`
	Active       int    `json:"active,omitempty"`
	Summaries    int    `json:"summaries,omitempty"`
	Leases       int    `json:"leases,omitempty"`
	HasPeer      bool   `json:"hasPeer,omitempty"`
	HasState     bool   `json:"hasState,omitempty"`
	State        int32  `json:"state,omitempty"`
	Revision     string `json:"revision,omitempty"`
	JournalFirst string `json:"journalFirst,omitempty"`
	JournalLast  string `json:"journalLast,omitempty"`
	JournalGap   bool   `json:"journalGap,omitempty"`
	StaleDrops   int    `json:"staleDrops"`
	Pending      int    `json:"pending"`
}

func fail(format string, args ...any) response {
	return response{OK: false, Error: fmt.Sprintf(format, args...)}
}

func main() {
	if err := run(); err != nil {
		enc := json.NewEncoder(os.Stdout)
		_ = enc.Encode(fail("%v", err))
		os.Exit(1)
	}
}

func run() error {
	ep, err := pibridge.NewEndpoint()
	if err != nil {
		return err
	}
	lis, err := ep.Listen()
	if err != nil {
		ep.Cleanup()
		return err
	}
	srv := pibridge.NewServer()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.GRPCServer().Serve(lis) }()

	enc := json.NewEncoder(os.Stdout)
	// Ready line first; integration asserts the target shape.
	if err := enc.Encode(map[string]any{"ready": true, "target": ep.Target}); err != nil {
		return err
	}

	scan := bufio.NewScanner(os.Stdin)
	scan.Buffer(make([]byte, 64*1024), 64*1024)
	for scan.Scan() {
		line := scan.Bytes()
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			_ = enc.Encode(fail("bad control json: %v", err))
			continue
		}
		switch req.Op {
		case "register":
			proc, token, err := srv.RegisterPending(req.Pane)
			if err != nil {
				_ = enc.Encode(fail("%v", err))
				continue
			}
			_ = enc.Encode(response{OK: true, Process: proc, Token: token})
		case "complete":
			srv.Complete(req.Process)
			_ = enc.Encode(response{OK: true})
		case "revoke":
			srv.Revoke(req.Process)
			_ = enc.Encode(response{OK: true})
		case "summarize":
			rev := uint64(0)
			if req.Revision != "" {
				rev, err = strconv.ParseUint(req.Revision, 10, 64)
				if err != nil {
					_ = enc.Encode(fail("bad revision: %v", err))
					continue
				}
			}
			deadline := time.Duration(req.DeadlineMs) * time.Millisecond
			id, err := srv.Summarize(req.Process, req.Session, req.Leaf,
				&pibridgepb.ModelSnapshot{Provider: req.Provider, Model: req.Model}, rev, deadline)
			if err != nil {
				_ = enc.Encode(response{OK: false, Error: err.Error(), Code: int(status.Code(err))})
				continue
			}
			_ = enc.Encode(response{OK: true, RequestID: strconv.FormatUint(id, 10)})
		case "cancel":
			id, err := strconv.ParseUint(req.RequestID, 10, 64)
			if err != nil {
				_ = enc.Encode(fail("bad request id: %v", err))
				continue
			}
			reason := pibridgepb.CancelReason(req.Reason)
			if err := srv.Cancel(req.Process, id, reason); err != nil {
				_ = enc.Encode(response{OK: false, Error: err.Error(), Code: int(status.Code(err))})
				continue
			}
			_ = enc.Encode(response{OK: true})
		case "await":
			id, err := strconv.ParseUint(req.RequestID, 10, 64)
			if err != nil {
				_ = enc.Encode(fail("bad request id: %v", err))
				continue
			}
			timeout := time.Duration(req.TimeoutMs) * time.Millisecond
			out, err := srv.Await(req.Process, id, timeout)
			if err != nil {
				_ = enc.Encode(fail("%v", err))
				continue
			}
			resp := response{OK: true, RequestID: strconv.FormatUint(out.RequestID, 10)}
			switch out.Kind {
			case pibridge.OutcomeSummary:
				resp.Kind = "summary"
				resp.Text = out.Text
			case pibridge.OutcomeError:
				resp.Kind = "error"
				resp.Code = int(out.Code)
				resp.SafeMsg = out.SafeMsg
			case pibridge.OutcomeCancelAck:
				resp.Kind = "cancelAck"
				resp.Terminate = int32(out.Terminate)
			case pibridge.OutcomeRevoked:
				resp.Kind = "revoked"
			default:
				resp.Kind = "timeout"
			}
			_ = enc.Encode(resp)
		case "flood":
			sent := srv.FloodForTest(req.Process, req.Count)
			_ = enc.Encode(response{OK: true, Sent: sent})
		case "stats":
			active, summaries, leases := srv.Stats()
			_ = enc.Encode(response{OK: true, Active: active, Summaries: summaries, Leases: leases})
		case "inspect":
			view, found := srv.InspectForTest(req.Process)
			if !found {
				_ = enc.Encode(fail("unknown process"))
				continue
			}
			_ = enc.Encode(response{
				OK: true, HasPeer: view.HasPeer, HasState: view.HasState,
				State: int32(view.State), Revision: strconv.FormatUint(view.Revision, 10),
				JournalFirst: strconv.FormatUint(view.JournalFirst, 10),
				JournalLast:  strconv.FormatUint(view.JournalLast, 10),
				JournalGap:   view.JournalGap, StaleDrops: view.StaleDrops, Pending: view.Pending,
			})
		case "shutdown":
			_ = enc.Encode(response{OK: true})
			srv.Stop()
			_ = lis.Close()
			ep.Cleanup()
			return nil
		default:
			_ = enc.Encode(fail("unknown op %q", req.Op))
		}
	}
	if err := scan.Err(); err != nil {
		srv.Stop()
		_ = lis.Close()
		ep.Cleanup()
		return err
	}
	srv.Stop()
	_ = lis.Close()
	ep.Cleanup()
	return nil
}

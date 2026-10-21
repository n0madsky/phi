package pibridge

import (
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestParseIPCTargetUnix(t *testing.T) {
	kind, path, err := ParseIPCTarget("unix:/tmp/phi-pi-abc/bridge.sock")
	if err != nil {
		t.Fatalf("valid unix target rejected: %v", err)
	}
	if kind != targetUnix || path != "/tmp/phi-pi-abc/bridge.sock" {
		t.Fatalf("unexpected parse: kind=%v path=%q", kind, path)
	}
}

func TestParseIPCTargetSpacesSurvive(t *testing.T) {
	kind, path, err := ParseIPCTarget("unix:/tmp/phi pi/bridge.sock")
	if err != nil {
		t.Fatalf("spaces rejected: %v", err)
	}
	if kind != targetUnix || path != "/tmp/phi pi/bridge.sock" {
		t.Fatalf("spaces mangled: %q", path)
	}
}

func TestParseIPCTargetWindowsPipe(t *testing.T) {
	pipe := `unix:\\.\pipe\phi-pi-` + strings.Repeat("ab", 16)
	kind, path, err := ParseIPCTarget(pipe)
	if err != nil {
		t.Fatalf("valid pipe rejected: %v", err)
	}
	if kind != targetWindowsPipe {
		t.Fatalf("expected pipe kind, got %v", kind)
	}
	if path != strings.TrimPrefix(pipe, "unix:") {
		t.Fatalf("pipe path mangled: %q", path)
	}
}

func TestParseIPCTargetRejects(t *testing.T) {
	cases := map[string]string{
		"tcp":            "127.0.0.1:50051",
		"dns":            "dns:///localhost:50051",
		"unix-authority": "unix:///tmp/bridge.sock",
		"relative":       "unix:bridge.sock",
		"empty":          "unix:",
		"no-scheme":      "/tmp/bridge.sock",
		"nul":            "unix:/tmp/bad\x00.sock",
		"percent":        "unix:/tmp/%2e.sock",
		"credentials":    "unix://user@/tmp/bridge.sock",
		"scheme-auth":    "unix://authority/tmp/bridge.sock",
		"remote-pipe":    `unix:\\.\pipe\other`,
		"remote-host":    `unix:\\host\pipe\phi-pi-` + strings.Repeat("a", 32),
		"short-pipe":     `unix:\\.\pipe\phi-pi-abc`,
		"nonhex-pipe":    `unix:\\.\pipe\phi-pi-` + strings.Repeat("zz", 16),
		"passthrough":    "passthrough:///tmp/x",
	}
	for name, target := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseIPCTarget(target); err == nil {
				t.Fatalf("target %q accepted, want rejection", target)
			}
		})
	}
}

func TestParseIPCTargetDoubleSlashAuthority(t *testing.T) {
	// unix:// + path carries an authority and must be rejected even when
	// the remainder looks absolute.
	if _, _, err := ParseIPCTarget("unix://tmp/bridge.sock"); err == nil {
		t.Fatal("authority target accepted")
	}
}

func TestMetadataSizeBound(t *testing.T) {
	md := metadata.Pairs(metadataPaneKey, "p", metadataProcessKey, "q", metadataTokenKey, strings.Repeat("a", 64))
	if err := checkMetadataSize(md); err != nil {
		t.Fatalf("small metadata rejected: %v", err)
	}
	big := metadata.Pairs("x-pad", strings.Repeat("y", maxMetadataBytes+1))
	if err := checkMetadataSize(big); err == nil {
		t.Fatal("oversized metadata accepted")
	}
}

func TestSafeMessageBound(t *testing.T) {
	if err := checkSafeMessage("ok"); err != nil {
		t.Fatalf("short message rejected: %v", err)
	}
	if err := checkSafeMessage(strings.Repeat("e", maxSafeMessageBytes+1)); err == nil {
		t.Fatal("oversized safe message accepted")
	}
	if err := checkSafeMessage(string([]byte{0xff, 0xfe})); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestNegotiateCapabilities(t *testing.T) {
	got := negotiateCapabilities([]string{CapabilityLifecycle, CapabilitySummary, "unknown.future.v9"})
	if !got[CapabilityLifecycle] || !got[CapabilitySummary] {
		t.Fatalf("known caps dropped: %v", got)
	}
	if got["unknown.future.v9"] {
		t.Fatalf("unknown cap accepted: %v", got)
	}
	if len(negotiateCapabilities(nil)) != 0 {
		t.Fatal("empty advise must yield empty accepted set")
	}
}

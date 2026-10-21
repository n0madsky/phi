package pibridge

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/metadata"
)

// targetKind identifies the validated local IPC target shape.
type targetKind int

const (
	targetUnix targetKind = iota + 1
	targetWindowsPipe
)

// ParseIPCTarget validates a client dial target. It accepts only the
// server-generated local shapes:
//
//   - "unix:" followed by an absolute filesystem socket path (macOS/Linux)
//   - "unix:" followed by a literal local pipe name under
//     \\.\pipe\phi-pi- (Windows)
//
// It rejects TCP addresses, hostnames, URL authorities, URL credentials,
// relative Unix paths, NUL bytes, percent encoding, and unsupported syntax.
// Spaces survive unchanged. The returned path is the literal address after
// the "unix:" prefix.
func ParseIPCTarget(target string) (kind targetKind, path string, err error) {
	const prefix = "unix:"
	rest, ok := strings.CutPrefix(target, prefix)
	if !ok {
		return 0, "", fmt.Errorf("pibridge: target must use unix: scheme")
	}
	if rest == "" {
		return 0, "", fmt.Errorf("pibridge: target has no address")
	}
	if strings.ContainsRune(rest, 0) {
		return 0, "", fmt.Errorf("pibridge: target contains NUL")
	}
	if strings.Contains(rest, "%") {
		return 0, "", fmt.Errorf("pibridge: target must not use percent encoding")
	}
	if strings.Contains(rest, "@") {
		return 0, "", fmt.Errorf("pibridge: target must not carry credentials")
	}
	if strings.Contains(rest, "://") {
		return 0, "", fmt.Errorf("pibridge: target must not carry an authority")
	}
	// Windows local pipe namespace. Checked before the Unix absolute-path
	// rule because the literal name is not a filesystem path.
	if name, ok := strings.CutPrefix(rest, `\\.\pipe\phi-pi-`); ok {
		if len(name) < 32 {
			return 0, "", fmt.Errorf("pibridge: pipe name too short")
		}
		for _, r := range name {
			isHex := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
			if !isHex {
				return 0, "", fmt.Errorf("pibridge: pipe name must be hex")
			}
		}
		// At least 128 random bits: 32 hex chars. Longer names stay valid.
		return targetWindowsPipe, rest, nil
	}
	if strings.HasPrefix(rest, `\\`) {
		return 0, "", fmt.Errorf("pibridge: remote pipe hosts are rejected")
	}
	if !filepath.IsAbs(rest) {
		return 0, "", fmt.Errorf("pibridge: unix socket path must be absolute")
	}
	if strings.HasPrefix(rest, "//") {
		return 0, "", fmt.Errorf("pibridge: target must not carry an authority")
	}
	return targetUnix, rest, nil
}

// metadataSize sums key and value bytes in gRPC metadata.
func metadataSize(md metadata.MD) int {
	n := 0
	for k, vs := range md {
		n += len(k)
		for _, v := range vs {
			n += len(v)
		}
	}
	return n
}

// checkMetadataSize rejects oversized gRPC metadata.
func checkMetadataSize(md metadata.MD) error {
	if metadataSize(md) > maxMetadataBytes {
		return fmt.Errorf("pibridge: metadata exceeds %d bytes", maxMetadataBytes)
	}
	return nil
}

// checkSafeMessage bounds safe_message text without credentials or
// transcripts. Callers map overlong text to a fixed protocol error.
func checkSafeMessage(s string) error {
	if len(s) > maxSafeMessageBytes {
		return fmt.Errorf("pibridge: safe message exceeds %d bytes", maxSafeMessageBytes)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("pibridge: safe message is not valid UTF-8")
	}
	return nil
}

// negotiateCapabilities intersects client-advertised names with the
// server-supported set. Unknown names are ignored, never an error.
func negotiateCapabilities(advised []string) map[string]bool {
	supported := map[string]bool{CapabilityLifecycle: true, CapabilitySummary: true}
	out := map[string]bool{}
	for _, c := range advised {
		if supported[c] {
			out[c] = true
		}
	}
	return out
}

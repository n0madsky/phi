package pibridge

import (
	"errors"
	"os"
)

// ErrEndpointUnavailable reports that no permitted endpoint root could host
// the local listener, or the chosen location was not exclusively creatable.
// Integration features stay unavailable; never fall back to TCP.
var ErrEndpointUnavailable = errors.New("pibridge: local IPC endpoint unavailable")

// maxSocketPathBytes bounds the complete absolute Unix socket path.
const maxSocketPathBytes = 100

// socketBasename is the socket file created inside the owned directory.
const socketBasename = "bridge.sock"

// Endpoint is a server-owned local IPC endpoint. Target is the gRPC dial
// target passed to the extension. Cleanup removes only files this endpoint
// created, is idempotent, and is a no-op for pipe endpoints.
type Endpoint struct {
	// Target is the client dial target: "unix:" followed by an absolute
	// filesystem socket path, or by the literal local pipe name.
	Target string

	unixDir    string // owned private directory (unix only)
	unixSocket string // owned socket file inside unixDir (unix only)
}

// Cleanup removes the owned socket and the owned private directory when it
// is empty. It never touches any other path and is safe to call twice.
func (e *Endpoint) Cleanup() {
	if e.unixSocket != "" {
		_ = os.Remove(e.unixSocket)
	}
	if e.unixDir != "" {
		_ = os.Remove(e.unixDir) // fails unless empty: intended
	}
}

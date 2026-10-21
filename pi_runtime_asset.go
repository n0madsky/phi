package main

import (
	"bytes"
	_ "embed"
	"fmt"
)

// The canonical Pi extension runtime travels inside the Phi binary so
// binary-only installations load the same artifacts the source checkout
// builds. Both build modes embed these files; stale or missing assets
// fail here at startup, never silently. Rebuild with pnpm build:pi-phi.
//
//go:embed bonus/pi-phi/dist/pi-phi-extension.cjs
var piPhiRuntimeAsset []byte

// The entry shim ships as TypeScript source: the host loader
// transforms it, so its Pi imports resolve to the host package.
//
//go:embed bonus/pi-phi/dist/pi-phi-entry.ts
var piPhiEntryAsset []byte

// piPhiRuntimeMarker is a stable bundle string that must survive
// embedding. It trips on truncation or a wrong-file embed, not malice.
var piPhiRuntimeMarker = []byte("PHI_PI_BRIDGE_ENDPOINT")

// piPhiEntryMarker is a stable shim string with the same purpose.
var piPhiEntryMarker = []byte("withHelpers")

// piPhiRuntime returns the canonical extension bytes after a sanity
// check. An absent or corrupt asset disables provisioning; it never
// blocks Pi startup.
func piPhiRuntime() ([]byte, error) {
	if len(piPhiRuntimeAsset) == 0 {
		return nil, fmt.Errorf("pi-phi: extension runtime missing; run pnpm build:pi-phi")
	}
	if !bytes.Contains(piPhiRuntimeAsset, piPhiRuntimeMarker) {
		return nil, fmt.Errorf("pi-phi: extension runtime corrupt")
	}
	return piPhiRuntimeAsset, nil
}

// piPhiEntry returns the canonical entry shim after the same check.
func piPhiEntry() ([]byte, error) {
	if len(piPhiEntryAsset) == 0 {
		return nil, fmt.Errorf("pi-phi: extension entry missing; run pnpm build:pi-phi")
	}
	if !bytes.Contains(piPhiEntryAsset, piPhiEntryMarker) {
		return nil, fmt.Errorf("pi-phi: extension entry corrupt")
	}
	return piPhiEntryAsset, nil
}

# @phi/pi-phi

Phi's integration endpoint inside Pi. The extension reports explicit
Pi completion to Phi's existing bell. It never changes the Pi
conversation: no prompts, no compaction, no context edits.

## Layout

- `proto/bridge.proto` — the canonical wire schema. Generated bindings
  (`src/gen/`, Go `internal/pibridge/pb`) must be regenerated from it,
  never hand-edited.
- `src/extension-entry.cts` — the file Pi loads: the factory as the
  module itself, callable through the host loader.
- `src/extension.ts` — the factory: credential handling, process-global
  ownership, and event subscriptions.
- `src/runtime.ts` — lifecycle mapping, journal, and reconnect.
- `src/channel.ts` — bounded-queue gRPC transport.
- `src/test-client.ts` — the synthetic peer for interop tests.
- `test/` — interop suites driven against the Go testserver.

Only type imports reference the Pi package from the bundle, so the
bundle carries no Pi runtime code. See `PROTOCOL.md` for the wire
protocol, capabilities, and bounds.

## Commands

From the Phi checkout:

- `pnpm build:pi-phi` — build the bundle.
- `pnpm check:pi-phi-generated` — fail on binding drift.
- `pnpm --filter @phi/pi-phi typecheck` — strict typecheck.
- `pnpm --filter @phi/pi-phi test` — unit suite.
- `pnpm test:pi-phi:integration` — interop over native IPC.
- `pnpm test:pi-phi:hosts` — both pinned Pi versions load the bundle.

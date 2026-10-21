# @phi/pi-phi

Phi's integration endpoint inside Pi. The extension reports explicit
Pi completion to Phi's existing bell and answers explicit
separate-session summary requests. It never changes the Pi
conversation: no prompts, no compaction, no context edits.

## Layout

- `proto/bridge.proto` — the canonical wire schema. Generated bindings
  (`src/gen/`, Go `internal/pibridge/pb`) must be regenerated from it,
  never hand-edited.
- `src/entry-shim.ts` — the file Pi loads. The host loader transforms
  it, so its Pi imports resolve to the host's own package. It injects
  the public summary helpers into the bundle and stays inert without
  Phi-provisioned credentials.
- `src/extension.ts` — the factory: credential handling, process-global
  ownership, and event subscriptions.
- `src/runtime.ts` — lifecycle mapping, journal, reconnect, and command
  dispatch.
- `src/summary.ts` — the guarded summary adapter (idle snapshot,
  host-routed model call, typed results).
- `src/channel.ts`, `src/commands.ts` — transport and command tracking.
- `src/test-client.ts` — the synthetic peer for interop tests.
- `test/` — interop suites driven against the Go testserver.

Only type imports reference the Pi package from the bundle, so the
bundle carries no Pi runtime code. See `PROTOCOL.md` for the wire
protocol, capabilities, and bounds.

## Commands

From the Phi checkout:

- `pnpm build:pi-phi` — build the bundle and stage the entry shim.
- `pnpm check:pi-phi-generated` — fail on binding drift.
- `pnpm --filter @phi/pi-phi typecheck` — strict typecheck.
- `pnpm --filter @phi/pi-phi test` — unit suite.
- `pnpm test:pi-phi:integration` — interop over native IPC.
- `pnpm test:pi-phi:hosts` — both pinned Pi versions load the entry.

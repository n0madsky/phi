// Pi extension factory. Loading is inert without Phi-provisioned
// credentials: no handlers, no connection, no Pi behavior change. The
// first session_start claimant in the process owns the runtime; duplicate
// loads stay inert. Credentials are stashed in the process owner slot and
// removed from process.env so agent-spawned tools cannot inherit them.

import type {
    ExtensionAPI,
    ExtensionContext,
} from '@earendil-works/pi-coding-agent';
import type { ChannelCreds } from './channel.js';
import { CommandTracker } from './commands.js';
import { type PiContextView, Runtime } from './runtime.js';
import type { SummaryHelpers } from './summary.js';

export const ENV_TARGET = 'PHI_PI_BRIDGE_ENDPOINT';
export const ENV_PANE = 'PHI_PI_BRIDGE_PANE_ID';
export const ENV_PROCESS = 'PHI_PI_BRIDGE_PROCESS_ID';
export const ENV_TOKEN = 'PHI_PI_BRIDGE_TOKEN';

const OWNER_KEY = Symbol.for('phi.pi-phi.owner');

interface OwnerSlot {
    active: boolean;
    generation: bigint;
    creds?: ChannelCreds | undefined;
    tracker?: CommandTracker | undefined;
}

function ownerSlot(): OwnerSlot {
    const existing = (globalThis as Record<symbol, unknown>)[OWNER_KEY] as
        | OwnerSlot
        | undefined;
    if (existing) return existing;
    const fresh: OwnerSlot = { active: false, generation: 0n };
    (globalThis as Record<symbol, unknown>)[OWNER_KEY] = fresh;
    return fresh;
}

/** Visible for tests to reset process-global ownership. */
export function resetOwnerSlotForTest(): void {
    (globalThis as Record<symbol, unknown>)[OWNER_KEY] = {
        active: false,
        generation: 0n,
    } as OwnerSlot;
}

/** Read provisioned credentials from the child environment. */
export function readCredsFromEnv(
    env: NodeJS.ProcessEnv = process.env,
): ChannelCreds | null {
    const target = env[ENV_TARGET];
    const pane = env[ENV_PANE];
    const process = env[ENV_PROCESS];
    const token = env[ENV_TOKEN];
    if (!target || !pane || !process || !token) return null;
    return { target, pane, process, token };
}

/** Remove provisioned credentials so later tools cannot inherit them. */
export function clearCredsFromEnv(env: NodeJS.ProcessEnv = process.env): void {
    delete env[ENV_TARGET];
    delete env[ENV_PANE];
    delete env[ENV_PROCESS];
    delete env[ENV_TOKEN];
}

function contextView(ctx: ExtensionContext): PiContextView {
    return {
        sessionManager: {
            getSessionId: () => ctx.sessionManager.getSessionId(),
            getLeafId: () => ctx.sessionManager.getLeafId(),
            getBranch: () => ctx.sessionManager.getBranch(),
        },
        get model() {
            return ctx.model ?? undefined;
        },
        modelRegistry: ctx.modelRegistry,
        thinkingLevel: ctx.thinkingLevel ?? undefined,
        isIdle: () => ctx.isIdle(),
        hasPendingMessages: () => ctx.hasPendingMessages(),
        getContextUsage: () => ctx.getContextUsage(),
    };
}

/** Visible for tests to pin the cross-version abort reading. */
export function settledAbortedForTest(event: unknown): boolean {
    if (typeof event !== 'object' || event === null) return false;
    return (event as { aborted?: unknown }).aborted === true;
}

/** Narrow runtime surface the factory needs; Runtime satisfies it. */
export interface FactoryRuntime {
    start(ctx: PiContextView): void;
    onSessionStart(ctx: PiContextView): void;
    onAgentStart(ctx: PiContextView): void;
    onAgentSettled(ctx: PiContextView, aborted: boolean): void;
    onModelSelect(ctx: PiContextView): void;
    onSessionTree(ctx: PiContextView): void;
    onCompactStart(ctx: PiContextView): void;
    onCompactEnd(ctx: PiContextView): void;
    shutdown(): void;
}

export interface FactoryDeps {
    makeRuntime?: (creds: ChannelCreds, generation: bigint) => FactoryRuntime;
}

/**
 * Extension factory. Registers lifecycle handlers only when credentials
 * are provisioned (environment) or stashed (reload in a provisioned
 * process). Long-lived resources start from session_start, never here.
 */
export function createExtension(
    pi: ExtensionAPI,
    deps: FactoryDeps = {},
): void {
    createExtensionWithHelpers(pi, null, deps);
}

/**
 * Factory with injected host summary helpers. The entry shim resolves
 * the helpers through the host loader; null keeps lifecycle-only mode.
 */
export function createExtensionWithHelpers(
    pi: ExtensionAPI,
    helpers: SummaryHelpers | null,
    deps: FactoryDeps = {},
): void {
    const envCreds = readCredsFromEnv();
    if (!envCreds && !ownerSlot().creds) return;

    let runtime: FactoryRuntime | null = null;
    let claimed = false;

    pi.on('session_start', (_event, ctx) => {
        const slot = ownerSlot();
        if (slot.active) {
            // A replacement session on a claimed runtime.
            runtime?.onSessionStart(contextView(ctx));
            return;
        }
        const creds = readCredsFromEnv() ?? slot.creds;
        if (!creds) return;
        slot.creds = creds;
        clearCredsFromEnv();
        slot.active = true;
        slot.generation += 1n;
        claimed = true;
        if (slot.tracker === undefined) slot.tracker = new CommandTracker();
        const tracker = slot.tracker;
        const make =
            deps.makeRuntime ??
            ((c, g) =>
                new Runtime({ creds: c, generation: g, helpers, tracker }));
        runtime = make(creds, slot.generation);
        runtime.start(contextView(ctx));
    });

    pi.on('agent_start', (_event, ctx) => {
        runtime?.onAgentStart(contextView(ctx));
    });

    pi.on('agent_settled', (event, ctx) => {
        runtime?.onAgentSettled(contextView(ctx), settledAbortedForTest(event));
    });

    pi.on('model_select', (_event, ctx) => {
        runtime?.onModelSelect(contextView(ctx));
    });

    pi.on('session_tree', (_event, ctx) => {
        runtime?.onSessionTree(contextView(ctx));
    });

    pi.on('session_before_compact', (_event, ctx) => {
        runtime?.onCompactStart(contextView(ctx));
    });

    const compactEnd = (_event: unknown, ctx: ExtensionContext): void => {
        runtime?.onCompactEnd(contextView(ctx));
    };
    pi.on('session_compact', compactEnd);
    pi.on('session_compact_failed', compactEnd);

    pi.on('session_shutdown', () => {
        runtime?.shutdown();
        runtime = null;
        if (claimed) {
            ownerSlot().active = false;
            claimed = false;
        }
    });
}

export default function (pi: ExtensionAPI): void {
    createExtension(pi);
}

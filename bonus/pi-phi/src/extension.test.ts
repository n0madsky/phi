import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type {
    ExtensionAPI,
    ExtensionContext,
} from '@earendil-works/pi-coding-agent';
import {
    ENV_PANE,
    ENV_PROCESS,
    ENV_TARGET,
    ENV_TOKEN,
    clearCredsFromEnv,
    createExtension,
    readCredsFromEnv,
    resetOwnerSlotForTest,
    type FactoryRuntime,
} from './extension.js';

const SAVED = { ...process.env };

function setEnv(): void {
    process.env[ENV_TARGET] = 'unix:/tmp/x.sock';
    process.env[ENV_PANE] = 'pane';
    process.env[ENV_PROCESS] = 'proc';
    process.env[ENV_TOKEN] = 'tok';
}

function mockPi(): {
    pi: ExtensionAPI;
    handlers: Map<string, (event: unknown, ctx: never) => unknown>;
} {
    const handlers = new Map<string, (event: unknown, ctx: never) => unknown>();
    const pi = {
        on: (
            event: string,
            handler: (event: unknown, ctx: never) => unknown,
        ) => {
            handlers.set(event, handler);
            return () => {
                handlers.delete(event);
            };
        },
    } as unknown as ExtensionAPI;
    return { pi, handlers };
}

function mockCtx(): ExtensionContext {
    return {
        sessionManager: { getSessionId: () => 'sess', getLeafId: () => 'leaf' },
        model: { provider: 'p', id: 'm' },
    } as unknown as ExtensionContext;
}

function recordingRuntime(): { calls: string[]; runtime: FactoryRuntime } {
    const calls: string[] = [];
    return {
        calls,
        runtime: {
            start: () => calls.push('start'),
            onSessionStart: () => calls.push('session_start'),
            onAgentStart: () => calls.push('agent_start'),
            onAgentSettled: (_ctx, aborted) => calls.push(`settled:${aborted}`),
            onModelSelect: () => calls.push('model_select'),
            onSessionTree: () => calls.push('session_tree'),
            onCompactStart: () => calls.push('compact_start'),
            onCompactEnd: () => calls.push('compact_end'),
            shutdown: () => calls.push('shutdown'),
        },
    };
}

beforeEach(() => {
    resetOwnerSlotForTest();
    for (const key of [ENV_TARGET, ENV_PANE, ENV_PROCESS, ENV_TOKEN])
        delete process.env[key];
});

afterEach(() => {
    for (const key of Object.keys(process.env)) {
        if (!(key in SAVED)) delete process.env[key];
    }
    Object.assign(process.env, SAVED);
    resetOwnerSlotForTest();
});

describe('credential environment', () => {
    it('reads complete credentials and rejects partial ones', () => {
        expect(readCredsFromEnv()).toBeNull();
        setEnv();
        expect(readCredsFromEnv()).toEqual({
            target: 'unix:/tmp/x.sock',
            pane: 'pane',
            process: 'proc',
            token: 'tok',
        });
        delete process.env[ENV_TOKEN];
        expect(readCredsFromEnv()).toBeNull();
    });

    it('clears all four keys', () => {
        setEnv();
        clearCredsFromEnv();
        expect(readCredsFromEnv()).toBeNull();
    });
});

describe('factory inertness', () => {
    it('registers nothing without credentials', () => {
        const { pi, handlers } = mockPi();
        createExtension(pi);
        expect(handlers.size).toBe(0);
    });

    it('registers lifecycle handlers with credentials', () => {
        setEnv();
        const { pi, handlers } = mockPi();
        createExtension(pi);
        expect([...handlers.keys()].sort()).toEqual([
            'agent_settled',
            'agent_start',
            'model_select',
            'session_before_compact',
            'session_compact',
            'session_compact_failed',
            'session_shutdown',
            'session_start',
            'session_tree',
        ]);
    });
});

describe('owner guard', () => {
    it('first claimant owns; duplicate stays inert', () => {
        setEnv();
        const first = mockPi();
        const second = mockPi();
        const rec1 = recordingRuntime();
        const rec2 = recordingRuntime();
        createExtension(first.pi, { makeRuntime: () => rec1.runtime });
        createExtension(second.pi, { makeRuntime: () => rec2.runtime });
        const ctx = mockCtx();
        first.handlers.get('session_start')!({}, ctx as never);
        second.handlers.get('session_start')!({}, ctx as never);
        expect(rec1.calls).toEqual(['start']);
        expect(rec2.calls).toEqual([]);
        // Only the owner forwards events.
        first.handlers.get('agent_start')!({}, ctx as never);
        second.handlers.get('agent_start')!({}, ctx as never);
        expect(rec1.calls).toEqual(['start', 'agent_start']);
        expect(rec2.calls).toEqual([]);
        // Only the owner releases on shutdown.
        second.handlers.get('session_shutdown')!({}, ctx as never);
        first.handlers.get('agent_settled')!(
            { type: 'agent_settled' },
            ctx as never,
        );
        expect(rec1.calls).toContain('settled:false');
    });

    it('stashes credentials, clears env, and reloads from the stash', () => {
        setEnv();
        const first = mockPi();
        const rec1 = recordingRuntime();
        createExtension(first.pi, { makeRuntime: () => rec1.runtime });
        const ctx = mockCtx();
        first.handlers.get('session_start')!({}, ctx as never);
        expect(process.env[ENV_TOKEN]).toBeUndefined();
        expect(readCredsFromEnv()).toBeNull();
        first.handlers.get('session_shutdown')!({}, ctx as never);
        expect(rec1.calls).toEqual(['start', 'shutdown']);
        // Reload: no env remains, but the stash provisions a fresh runtime.
        const reloaded = mockPi();
        const rec2 = recordingRuntime();
        createExtension(reloaded.pi, { makeRuntime: () => rec2.runtime });
        expect(reloaded.handlers.size).toBe(9);
        reloaded.handlers.get('session_start')!({}, ctx as never);
        expect(rec2.calls).toEqual(['start']);
    });

    it('forwards the settled abort bit', () => {
        setEnv();
        const { pi, handlers } = mockPi();
        const rec = recordingRuntime();
        createExtension(pi, { makeRuntime: () => rec.runtime });
        const ctx = mockCtx();
        handlers.get('session_start')!({}, ctx as never);
        handlers.get('agent_settled')!(
            { type: 'agent_settled', aborted: true },
            ctx as never,
        );
        handlers.get('agent_settled')!({ type: 'agent_settled' }, ctx as never);
        expect(rec.calls).toEqual(['start', 'settled:true', 'settled:false']);
    });
});

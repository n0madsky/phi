// Provider-fixture summary interop: a REAL Runtime with the REAL host
// helpers talks to the Go testserver over native IPC. Only the Pi host
// surface (session, registry) and the model stream are faked.
import { type ChildProcess, spawn } from 'node:child_process';
import {
    buildSessionContext,
    generateSummaryWithUsage,
    SessionManager,
} from '@earendil-works/pi-coding-agent';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Runtime, type PiContextView } from '../src/runtime.js';
import type { SummaryHelpers } from '../src/summary.js';

const HELPERS: SummaryHelpers = {
    buildSessionContext,
    generateSummaryWithUsage,
};

interface ControlResponse {
    ok: boolean;
    error?: string;
    process?: string;
    token?: string;
    requestId?: string;
    kind?: string;
    text?: string;
    code?: number;
    safeMsg?: string;
    terminate?: number;
    hasPeer?: boolean;
    hasState?: boolean;
    revision?: string;
}

let server: ChildProcess;
let target = '';
const lineWaiters: Array<(line: string) => void> = [];
const lineQueue: string[] = [];

function onLine(line: string): void {
    const waiter = lineWaiters.shift();
    if (waiter) waiter(line);
    else lineQueue.push(line);
}

function readLine(timeoutMs = 10_000): Promise<string> {
    const queued = lineQueue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
        const timer = setTimeout(
            () => reject(new Error('control read timeout')),
            timeoutMs,
        );
        lineWaiters.push((line: string) => {
            clearTimeout(timer);
            resolve(line);
        });
    });
}

async function control(op: Record<string, unknown>): Promise<ControlResponse> {
    if (!server.stdin) throw new Error('testserver stdin unavailable');
    server.stdin.write(`${JSON.stringify(op)}\n`);
    return JSON.parse(await readLine()) as ControlResponse;
}

/** Minimal AssistantMessageEventStream shape the helper consumes. */
function scriptedStream(chunks: string[], text: string) {
    let i = 0;
    return {
        [Symbol.asyncIterator]() {
            return {
                next: async () =>
                    i < chunks.length
                        ? {
                              value: {
                                  type: 'text_delta',
                                  contentIndex: 0,
                                  delta: chunks[i++],
                                  partial: {},
                              },
                              done: false,
                          }
                        : { value: undefined, done: true },
            };
        },
        result: async () => ({
            role: 'assistant',
            content: [{ type: 'text', text }],
            api: 'fixture',
            provider: 'fixture-provider',
            model: 'fixture-model',
            usage: {
                input: 10,
                output: 5,
                cacheRead: 0,
                cacheWrite: 0,
                totalTokens: 15,
                cost: {
                    input: 0,
                    output: 0,
                    cacheRead: 0,
                    cacheWrite: 0,
                    total: 0.001,
                },
            },
            stopReason: 'stop',
            timestamp: Date.now(),
        }),
    };
}

interface Fixture {
    view: PiContextView;
    streamCalls: () => number;
    aborted: () => boolean;
    session: SessionManager;
    leafBefore: string | null;
}

function makeFixture(opts: {
    idle: boolean;
    stream: () => unknown;
    usage?: { tokens: number | null; contextWindow: number; percent: number };
}): Fixture {
    const session = SessionManager.inMemory('/tmp');
    // A compacted branch with an abandoned fork: the helper reads the
    // active leaf path (research probe), and the run must leave every
    // entry and the leaf untouched.
    const first = session.appendMessage({
        role: 'user',
        content: 'Synthetic goal: repair the bell.',
        timestamp: 1,
    });
    session.appendMessage({
        role: 'user',
        content: 'ABANDONED_BRANCH_MARKER',
        timestamp: 2,
    });
    session.branch(first);
    const kept = session.appendMessage({
        role: 'user',
        content: 'ACTIVE_BRANCH_MARKER',
        timestamp: 3,
    });
    session.appendCompaction('OLDER_HISTORY_SUMMARY_MARKER', kept, 10000);
    session.appendMessage({
        role: 'user',
        content: 'LATEST_MESSAGE_MARKER',
        timestamp: 4,
    });
    let calls = 0;
    let lastSignal: AbortSignal | null = null;
    const view: PiContextView = {
        sessionManager: {
            getSessionId: () => 'fixture-session',
            getLeafId: () => session.getLeafId(),
            getBranch: () => session.getBranch(),
        },
        model: {
            id: 'fixture-model',
            provider: 'fixture-provider',
            api: 'fixture',
            maxTokens: 200000,
            reasoning: false,
        } as PiContextView['model'],
        modelRegistry: {
            streamSimple: ((...args: unknown[]) => {
                calls += 1;
                const options = args[2] as { signal?: AbortSignal } | undefined;
                if (options?.signal) lastSignal = options.signal;
                return opts.stream();
            }) as PiContextView['modelRegistry'] extends infer R
                ? R extends { streamSimple: infer F }
                    ? F
                    : never
                : never,
        } as PiContextView['modelRegistry'],
        isIdle: () => opts.idle,
        hasPendingMessages: () => false,
        getContextUsage: () =>
            opts.usage ?? { tokens: 100, contextWindow: 200000, percent: 1 },
        thinkingLevel: undefined,
    };
    return {
        view,
        streamCalls: () => calls,
        aborted: () => lastSignal?.aborted ?? false,
        session,
        leafBefore: session.getLeafId(),
    };
}

async function awaitHello(process: string): Promise<string> {
    for (let i = 0; i < 100; i += 1) {
        const res = await control({ op: 'inspect', process });
        if (res.ok && res.hasState) return res.revision ?? '0';
        await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error('hello never accepted');
}

async function startRuntime(
    pane: string,
    view: PiContextView,
): Promise<{ runtime: Runtime; process: string; revision: string }> {
    const reg = await control({ op: 'register', pane });
    expect(reg.ok, `register: ${reg.error}`).toBe(true);
    await control({ op: 'complete', process: reg.process });
    const runtime = new Runtime({
        creds: { target, pane, process: reg.process!, token: reg.token! },
        generation: 1n,
        helpers: HELPERS,
    });
    runtime.start(view);
    try {
        const revision = await awaitHello(reg.process!);
        return { runtime, process: reg.process!, revision };
    } catch (err) {
        runtime.shutdown();
        throw err;
    }
}

beforeAll(async () => {
    const bin = process.env.PI_PHI_TESTSERVER;
    expect(
        bin,
        'PI_PHI_TESTSERVER must point at the testserver binary',
    ).toBeTruthy();
    server = spawn(bin!, { stdio: ['pipe', 'pipe', 'inherit'] });
    if (!server.stdin || !server.stdout)
        throw new Error('testserver stdio unavailable');
    let buffer = '';
    server.stdout.on('data', (chunk: Buffer) => {
        buffer += chunk.toString('utf8');
        for (;;) {
            const idx = buffer.indexOf('\n');
            if (idx < 0) break;
            const line = buffer.slice(0, idx);
            buffer = buffer.slice(idx + 1);
            if (line.length > 0) onLine(line);
        }
    });
    const ready = JSON.parse(await readLine(15_000)) as {
        ready: boolean;
        target: string;
    };
    expect(ready.ready).toBe(true);
    target = ready.target;
}, 30_000);

afterAll(async () => {
    if (server && server.exitCode === null) {
        try {
            await control({ op: 'shutdown' });
        } catch {
            server.kill();
        }
        await new Promise((resolve) => server.once('exit', resolve));
    }
});

describe('provider fixtures', () => {
    it('summarizes successfully and leaves the session unchanged', async () => {
        const text = 'Synthetic session summary.';
        const fixture = makeFixture({
            idle: true,
            stream: () =>
                scriptedStream(['Synthetic ', 'session summary.'], text),
        });
        const before = JSON.stringify(fixture.session.getEntries());
        const { runtime, process, revision } = await startRuntime(
            'pane-success',
            fixture.view,
        );
        try {
            const admitted = await control({
                op: 'summarize',
                process,
                session: 'fixture-session',
                leaf: fixture.leafBefore,
                provider: 'fixture-provider',
                model: 'fixture-model',
                revision,
                deadlineMs: 30_000,
            });
            expect(admitted.ok, `admit: ${admitted.error}`).toBe(true);
            const out = await control({
                op: 'await',
                process,
                requestId: admitted.requestId,
                timeoutMs: 30_000,
            });
            expect(out.ok).toBe(true);
            expect(out.kind).toBe('summary');
            expect(out.text).toBe(text);
            expect(fixture.streamCalls()).toBe(1);
            expect(JSON.stringify(fixture.session.getEntries())).toBe(before);
            expect(fixture.session.getLeafId()).toBe(fixture.leafBefore);
        } finally {
            runtime.shutdown();
        }
    });

    it('rejects a busy pane without provider work', async () => {
        const fixture = makeFixture({
            idle: false,
            stream: () => scriptedStream(['x'], 'x'),
        });
        // The busy pane never reaches provider work: the server sees
        // the runtime's lifecycle snapshot, and the extension rejects
        // the non-idle host before calling the model.
        const reg = await control({ op: 'register', pane: 'pane-busy' });
        expect(reg.ok).toBe(true);
        await control({ op: 'complete', process: reg.process });
        const runtime = new Runtime({
            creds: {
                target,
                pane: 'pane-busy',
                process: reg.process!,
                token: reg.token!,
            },
            generation: 1n,
            helpers: HELPERS,
        });
        runtime.start(fixture.view);
        try {
            const revision = await awaitHello(reg.process!);
            const admitted = await control({
                op: 'summarize',
                process: reg.process,
                session: 'fixture-session',
                leaf: fixture.leafBefore,
                provider: 'fixture-provider',
                model: 'fixture-model',
                revision,
                deadlineMs: 10_000,
            });
            // Server-side admission rejects the busy snapshot...
            // or the extension rejects it; either way no provider work.
            if (admitted.ok) {
                const out = await control({
                    op: 'await',
                    process: reg.process,
                    requestId: admitted.requestId,
                    timeoutMs: 10_000,
                });
                expect(out.kind).toBe('error');
            } else {
                expect(admitted.error).toMatch(/busy/i);
            }
            expect(fixture.streamCalls()).toBe(0);
        } finally {
            runtime.shutdown();
        }
    });

    it('rejects a stale revision before provider work', async () => {
        const fixture = makeFixture({
            idle: true,
            stream: () => scriptedStream(['x'], 'x'),
        });
        const before = JSON.stringify(fixture.session.getEntries());
        const { runtime, process, revision } = await startRuntime(
            'pane-stale',
            fixture.view,
        );
        try {
            const stale = String(BigInt(revision) + 99n);
            const admitted = await control({
                op: 'summarize',
                process,
                session: 'fixture-session',
                leaf: fixture.leafBefore,
                provider: 'fixture-provider',
                model: 'fixture-model',
                revision: stale,
                deadlineMs: 10_000,
            });
            expect(admitted.ok).toBe(false);
            expect(admitted.error).toMatch(/stale/i);
            expect(fixture.streamCalls()).toBe(0);
            expect(JSON.stringify(fixture.session.getEntries())).toBe(before);
            expect(fixture.session.getLeafId()).toBe(fixture.leafBefore);
        } finally {
            runtime.shutdown();
        }
    });

    it('reports canceled when an abort-ignoring provider answers late', async () => {
        let release!: (value: unknown) => void;
        const gate = new Promise((resolve) => {
            release = resolve;
        });
        const fixture = makeFixture({
            idle: true,
            stream: () => ({
                [Symbol.asyncIterator]() {
                    return {
                        next: async () => ({ value: undefined, done: true }),
                    };
                },
                // Ignores the abort signal: resolves only when released,
                // with a would-be success. The adapter must not emit it.
                result: () =>
                    gate.then(() => ({
                        role: 'assistant',
                        content: [{ type: 'text', text: 'must never surface' }],
                        api: 'fixture',
                        provider: 'fixture-provider',
                        model: 'fixture-model',
                        usage: {
                            input: 1,
                            output: 1,
                            cacheRead: 0,
                            cacheWrite: 0,
                            totalTokens: 2,
                            cost: {
                                input: 0,
                                output: 0,
                                cacheRead: 0,
                                cacheWrite: 0,
                                total: 0,
                            },
                        },
                        stopReason: 'stop',
                        timestamp: Date.now(),
                    })),
            }),
        });
        const { runtime, process, revision } = await startRuntime(
            'pane-abort',
            fixture.view,
        );
        try {
            const admitted = await control({
                op: 'summarize',
                process,
                session: 'fixture-session',
                leaf: fixture.leafBefore,
                provider: 'fixture-provider',
                model: 'fixture-model',
                revision,
                deadlineMs: 30_000,
            });
            expect(admitted.ok, `admit: ${admitted.error}`).toBe(true);
            // Wait until the provider call starts, then cancel.
            for (let i = 0; i < 100 && fixture.streamCalls() === 0; i += 1) {
                await new Promise((r) => setTimeout(r, 50));
            }
            expect(fixture.streamCalls()).toBe(1);
            const cancelled = await control({
                op: 'cancel',
                process,
                requestId: admitted.requestId,
                reason: 1,
            });
            expect(cancelled.ok, `cancel: ${cancelled.error}`).toBe(true);
            // Wait until the runtime processes the Cancel frame (it
            // aborts synchronously on receipt) before the provider
            // answers; otherwise the success legitimately wins.
            for (let i = 0; i < 100 && !fixture.aborted(); i += 1) {
                await new Promise((r) => setTimeout(r, 50));
            }
            expect(fixture.aborted()).toBe(true);
            // The provider answers late with a success; the canceled
            // invocation must still end as canceled, never as a summary.
            release(undefined);
            const out = await control({
                op: 'await',
                process,
                requestId: admitted.requestId,
                timeoutMs: 30_000,
            });
            expect(out.ok).toBe(true);
            expect(out.kind).toBe('cancelAck');
            expect(out.terminate).toBe(2); // ENDED
        } finally {
            runtime.shutdown();
        }
    });
});

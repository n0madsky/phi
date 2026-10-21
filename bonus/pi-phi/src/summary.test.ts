// Summary adapter tests. Helpers and the host registry are fakes;
// no provider work happens here. The e2e suite runs the real
// generateSummaryWithUsage against a scripted registry stream.

import { describe, expect, it, vi } from 'vitest';
import {
    CommandErrorCode,
    ModelSnapshot,
    Summarize,
    SummaryTermination,
} from './gen/bridge.js';
import { CommandTracker } from './commands.js';
import type { PiContextView } from './runtime.js';
import {
    SummaryAdapter,
    type SummaryHelpers,
    type SummaryDeps,
    hasSummarySurface,
    SUMMARY_INSTRUCTIONS,
} from './summary.js';
import { MAX_SUMMARY_TEXT_BYTES } from './protocol.js';

interface StreamScript {
    deltas?: string[];
    stopReason?: string;
    model?: string;
    hangUntilAbort?: boolean;
}

interface FakeRegistry {
    calls: unknown[][];
    script: StreamScript;
    streamSimple: (...args: unknown[]) => FakeStream;
}

interface FakeStream {
    events: unknown[];
    [Symbol.asyncIterator](): AsyncIterator<unknown>;
    result(): Promise<unknown>;
}

function fakeStream(script: StreamScript, signal?: AbortSignal): FakeStream {
    const events: unknown[] = [{ type: 'start', partial: {} }];
    for (const delta of script.deltas ?? []) {
        events.push({
            type: 'text_delta',
            contentIndex: 0,
            delta,
            partial: {},
        });
    }
    events.push({
        type: 'text_end',
        contentIndex: 0,
        content: (script.deltas ?? []).join(''),
        partial: {},
    });
    let resultPromise: Promise<unknown>;
    if (script.hangUntilAbort && signal) {
        resultPromise = new Promise((resolve) => {
            if (signal.aborted) {
                resolve({ stopReason: 'aborted', model: script.model ?? 'm' });
                return;
            }
            signal.addEventListener(
                'abort',
                () =>
                    resolve({
                        stopReason: 'aborted',
                        model: script.model ?? 'm',
                    }),
                { once: true },
            );
        });
    } else {
        resultPromise = Promise.resolve({
            stopReason: script.stopReason ?? 'stop',
            model: script.model ?? 'm',
        });
    }
    return {
        events,
        async *[Symbol.asyncIterator]() {
            for (const event of events) yield event;
        },
        result: () => resultPromise,
    };
}

function fakeRegistry(script: StreamScript = {}): FakeRegistry {
    const calls: unknown[][] = [];
    const registry = {
        calls,
        script,
        streamSimple: (...args: unknown[]) => {
            calls.push(args);
            const options = (args[2] ?? {}) as { signal?: AbortSignal };
            return fakeStream(registry.script, options.signal);
        },
    };
    return registry;
}

interface FakeViewOptions {
    sessionId?: string;
    leafId?: string | null;
    model?:
        | {
              provider: string;
              id: string;
              maxTokens: number;
              reasoning: boolean;
          }
        | undefined;
    idle?: boolean;
    pendingMessages?: boolean;
    usage?: { tokens: number | null; contextWindow: number } | undefined;
    branch?: unknown[];
    registry?: FakeRegistry;
    surface?: boolean;
}

function fakeView(options: FakeViewOptions = {}): PiContextView {
    const full = options.surface !== false;
    return {
        sessionManager: {
            getSessionId: () => options.sessionId ?? 'sess',
            getLeafId: () => options.leafId ?? 'leaf',
            ...(full
                ? {
                      getBranch: () =>
                          (options.branch ?? [{ id: 'e1' }]) as never,
                  }
                : {}),
        },
        model:
            options.model === undefined
                ? (options.model as undefined)
                : (options.model as never),
        ...(full
            ? {
                  modelRegistry: (options.registry ?? fakeRegistry()) as never,
                  isIdle: () => options.idle ?? true,
                  hasPendingMessages: () => options.pendingMessages ?? false,
                  getContextUsage: () =>
                      (options.usage === undefined
                          ? { tokens: 100, contextWindow: 100000, percent: 0.1 }
                          : { percent: null, ...options.usage }) as never,
              }
            : {}),
    };
}

interface FakeHelpersOptions {
    messages?: unknown[];
    text?: string;
    usage?: unknown;
    error?: unknown;
    hangUntilAbort?: boolean;
    buildError?: unknown;
}

function fakeHelpers(
    options: FakeHelpersOptions = {},
): SummaryHelpers & { calls: { streamFn: unknown } } {
    const calls: { streamFn: unknown } = { streamFn: null };
    return {
        calls,
        buildSessionContext: (() => {
            if (options.buildError !== undefined) throw options.buildError;
            return {
                messages: options.messages ?? [
                    {
                        role: 'user',
                        content: [{ type: 'text', text: 'hello' }],
                    },
                ],
                thinkingLevel: undefined,
                model: undefined,
            };
        }) as never,
        generateSummaryWithUsage: (async (
            _messages: unknown,
            _model: unknown,
            _reserve: number,
            _apiKey: unknown,
            _headers: unknown,
            signal: AbortSignal,
            _instructions: string,
            _previous: unknown,
            _thinking: unknown,
            streamFn: (...args: never[]) => { result(): Promise<unknown> },
        ) => {
            calls.streamFn = streamFn;
            if (options.error !== undefined) throw options.error;
            // Drive the stream like the real helper: call streamFn, await result.
            const stream = streamFn(
                {} as never,
                {} as never,
                { signal } as never,
            );
            if (options.hangUntilAbort) {
                await new Promise<void>((resolve) => {
                    if (signal.aborted) {
                        resolve();
                        return;
                    }
                    signal.addEventListener('abort', () => resolve(), {
                        once: true,
                    });
                });
                return {
                    text: 'late text that must not send',
                    usage: options.usage ?? {},
                };
            }
            await stream.result();
            return {
                text: options.text ?? 'Summary text.',
                usage: options.usage ?? {
                    input: 10,
                    output: 5,
                    totalTokens: 15,
                    cost: {
                        input: 0,
                        output: 0,
                        cacheRead: 0,
                        cacheWrite: 0,
                        total: 0.001,
                    },
                },
            };
        }) as never,
    };
}

/** Wrap helpers so generation waits on a gate (for cancel races). */
function gatedHelpers(
    helpers: SummaryHelpers & { calls: { streamFn: unknown } },
    gate: Promise<void>,
): SummaryHelpers & { calls: { streamFn: unknown } } {
    const inner = helpers.generateSummaryWithUsage;
    return {
        ...helpers,
        generateSummaryWithUsage: (async (
            ...args: Parameters<SummaryHelpers['generateSummaryWithUsage']>
        ) => {
            await gate;
            return inner(...args);
        }) as SummaryHelpers['generateSummaryWithUsage'],
    };
}

interface Harness {
    adapter: SummaryAdapter;
    sent: { $case: string; value: unknown }[];
    errors: [string, unknown][];
    deps: SummaryDeps;
}

function harness(view: PiContextView | null, compacting = false): Harness {
    const sent: { $case: string; value: unknown }[] = [];
    const errors: [string, unknown][] = [];
    const deps: SummaryDeps = {
        getView: () => view,
        isCompacting: () => compacting,
        getRevision: () => 7n,
        send: (payload) =>
            sent.push(payload as { $case: string; value: unknown }),
        logError: (message, error) => errors.push([message, error]),
    };
    return {
        adapter: new SummaryAdapter(deps, new CommandTracker()),
        sent,
        errors,
        deps,
    };
}

function command(over: Partial<Summarize> = {}): Summarize {
    return Summarize.create({
        requestId: 1n,
        sessionId: 'sess',
        leafId: 'leaf',
        model: ModelSnapshot.create({ provider: 'p', model: 'm' }),
        deadlineMs: 5000n,
        stateRevision: 3n,
        ...over,
    });
}

function idleView(over: FakeViewOptions = {}): PiContextView {
    return fakeView({
        model: { provider: 'p', id: 'm', maxTokens: 8000, reasoning: false },
        ...over,
    });
}

describe('hasSummarySurface', () => {
    it('accepts a full view', () => {
        expect(hasSummarySurface(idleView())).toBe(true);
    });

    it('rejects a lifecycle-only view', () => {
        expect(hasSummarySurface(fakeView({ surface: false }))).toBe(false);
    });
});

describe('SummaryAdapter gates', () => {
    it('rejects duplicate request IDs', async () => {
        const h = harness(idleView());
        await h.adapter.handleSummarize(command(), fakeHelpers());
        await h.adapter.handleSummarize(command(), fakeHelpers());
        expect(h.sent).toHaveLength(2);
        const second = h.sent[1]!.value as { code: CommandErrorCode };
        expect(h.sent[1]!.$case).toBe('commandError');
        expect(second.code).toBe(
            CommandErrorCode.COMMAND_ERROR_DUPLICATE_REQUEST,
        );
    });

    it('reports unsupported capability without helpers', async () => {
        const h = harness(idleView());
        await h.adapter.handleSummarize(command(), null);
        expect(h.sent[0]!.$case).toBe('commandError');
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_UNSUPPORTED_CAPABILITY,
        );
    });

    it('reports internal error without a context view', async () => {
        const h = harness(null);
        await h.adapter.handleSummarize(command(), fakeHelpers());
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_INTERNAL,
        );
    });

    it('reports internal error without a summary surface', async () => {
        const h = harness(fakeView({ surface: false }));
        await h.adapter.handleSummarize(command(), fakeHelpers());
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_INTERNAL,
        );
    });

    it('refuses a busy agent', async () => {
        const h = harness(idleView({ idle: false }));
        await h.adapter.handleSummarize(command(), fakeHelpers());
        const value = h.sent[0]!.value as {
            code: CommandErrorCode;
            safeMessage: string;
        };
        expect(h.sent[0]!.$case).toBe('commandError');
        expect(value.code).toBe(CommandErrorCode.COMMAND_ERROR_BUSY);
        expect(value.safeMessage).toBe('agent is busy');
    });

    it('refuses pending messages and compaction', async () => {
        const queued = harness(idleView({ pendingMessages: true }));
        await queued.adapter.handleSummarize(command(), fakeHelpers());
        expect((queued.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_BUSY,
        );
        const compacting = harness(idleView(), true);
        await compacting.adapter.handleSummarize(command(), fakeHelpers());
        expect(
            (compacting.sent[0]!.value as { code: CommandErrorCode }).code,
        ).toBe(CommandErrorCode.COMMAND_ERROR_BUSY);
    });

    it('refuses a missing model', async () => {
        const h = harness(idleView({ model: undefined }));
        await h.adapter.handleSummarize(command(), fakeHelpers());
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_INTERNAL,
        );
    });

    it('rejects stale snapshots', async () => {
        for (const over of [
            { sessionId: 'other' },
            { leafId: 'other' },
            { model: ModelSnapshot.create({ provider: 'p', model: 'other' }) },
        ]) {
            const h = harness(idleView());
            await h.adapter.handleSummarize(command(over), fakeHelpers());
            expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
                CommandErrorCode.COMMAND_ERROR_STALE_SNAPSHOT,
            );
        }
    });

    it('rejects an empty session', async () => {
        const h = harness(idleView());
        await h.adapter.handleSummarize(
            command(),
            fakeHelpers({ messages: [] }),
        );
        const value = h.sent[0]!.value as {
            code: CommandErrorCode;
            safeMessage: string;
        };
        expect(value.code).toBe(CommandErrorCode.COMMAND_ERROR_INTERNAL);
        expect(value.safeMessage).toBe('session is empty');
    });

    it('refuses an over-budget session', async () => {
        const h = harness(
            idleView({ usage: { tokens: 99000, contextWindow: 100000 } }),
        );
        await h.adapter.handleSummarize(command(), fakeHelpers());
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_LIMIT,
        );
    });

    it('estimates the budget when usage is unknown', async () => {
        const small = harness(
            idleView({ usage: { tokens: null, contextWindow: 100000 } }),
        );
        await small.adapter.handleSummarize(command(), fakeHelpers());
        expect(small.sent[0]!.$case).toBe('summaryResult');
        const big = harness(
            idleView({ usage: { tokens: null, contextWindow: 1000 } }),
        );
        await big.adapter.handleSummarize(
            command(),
            fakeHelpers({
                messages: [
                    {
                        role: 'user',
                        content: [{ type: 'text', text: 'x'.repeat(20000) }],
                    },
                ],
            }),
        );
        expect((big.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_LIMIT,
        );
    });

    it('rejects a second concurrent summary', async () => {
        const h = harness(idleView());
        let release!: () => void;
        const gate = new Promise<void>((resolve) => (release = resolve));
        const helpers = gatedHelpers(fakeHelpers(), gate);
        const first = h.adapter.handleSummarize(
            command({ requestId: 1n }),
            helpers,
        );
        await new Promise((resolve) => setTimeout(resolve, 10));
        await h.adapter.handleSummarize(command({ requestId: 2n }), helpers);
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_BUSY,
        );
        release();
        await first;
        expect(h.sent[1]!.$case).toBe('summaryResult');
    });
});

describe('SummaryAdapter success', () => {
    it('returns a typed result and routes through the host registry', async () => {
        const registry = fakeRegistry({
            deltas: ['Summary text.'],
            model: 'reported-m',
        });
        const h = harness(idleView({ registry }));
        const helpers = fakeHelpers({ text: 'Summary text.' });
        await h.adapter.handleSummarize(command(), helpers);
        expect(h.sent).toHaveLength(1);
        const frame = h.sent[0]!;
        expect(frame.$case).toBe('summaryResult');
        const result = frame.value as {
            requestId: bigint;
            sessionId: string;
            leafId: string;
            text: string;
            selectedModel?: { provider: string; model: string };
            reportedModel?: { provider: string; model: string };
            usage?: {
                inputTokens?: bigint;
                outputTokens?: bigint;
                costUsd?: number;
                costFromPriceTables: boolean;
            };
            stateRevision: bigint;
        };
        expect(result.requestId).toBe(1n);
        expect(result.sessionId).toBe('sess');
        expect(result.leafId).toBe('leaf');
        expect(result.text).toBe('Summary text.');
        expect(result.selectedModel).toMatchObject({
            provider: 'p',
            model: 'm',
        });
        expect(result.reportedModel).toMatchObject({
            provider: 'p',
            model: 'reported-m',
        });
        expect(result.usage?.inputTokens).toBe(10n);
        expect(result.usage?.outputTokens).toBe(5n);
        expect(result.usage?.costUsd).toBeCloseTo(0.001);
        expect(result.usage?.costFromPriceTables).toBe(true);
        expect(result.stateRevision).toBe(7n);
        // The host stream gets no retries and a bounded timeout.
        expect(registry.calls).toHaveLength(1);
        const options = registry.calls[0]![2] as {
            maxRetries: number;
            timeoutMs: number;
            signal: AbortSignal;
        };
        expect(options.maxRetries).toBe(0);
        expect(options.timeoutMs).toBeGreaterThan(0);
        expect(options.timeoutMs).toBeLessThanOrEqual(5000);
        expect(options.signal).toBeInstanceOf(AbortSignal);
    });

    it('passes fixed instructions and disables helper retries', async () => {
        const h = harness(idleView());
        const seen: unknown[][] = [];
        const helpers = fakeHelpers();
        const inner = helpers.generateSummaryWithUsage;
        const spied: SummaryHelpers = {
            ...helpers,
            generateSummaryWithUsage: (async (
                ...args: Parameters<SummaryHelpers['generateSummaryWithUsage']>
            ) => {
                seen.push(args);
                return inner(...args);
            }) as SummaryHelpers['generateSummaryWithUsage'],
        };
        await h.adapter.handleSummarize(command(), spied);
        expect(seen).toHaveLength(1);
        const args = seen[0]!;
        expect(args[2]).toBe(4096);
        expect(args[6]).toBe(SUMMARY_INSTRUCTIONS);
        expect(args[11]).toEqual({
            enabled: false,
            maxRetries: 0,
            baseDelayMs: 0,
        });
    });

    it('falls back to the selected model when the stream reports none', async () => {
        const registry = fakeRegistry({ deltas: ['x'] });
        // Strip the model from the tapped result.
        const streamSimple = registry.streamSimple;
        registry.streamSimple = (...args: unknown[]) => {
            const stream = streamSimple(...args);
            return { ...stream, result: async () => ({ stopReason: 'stop' }) };
        };
        const h = harness(idleView({ registry }));
        await h.adapter.handleSummarize(command(), fakeHelpers({ text: 'x' }));
        const result = h.sent[0]!.value as {
            reportedModel?: { model: string };
        };
        expect(result.reportedModel?.model).toBe('m');
    });

    it('omits non-finite usage fields', async () => {
        const h = harness(idleView());
        await h.adapter.handleSummarize(
            command(),
            fakeHelpers({
                usage: {
                    input: Number.NaN,
                    output: -1,
                    totalTokens: 0,
                    cost: { total: Number.POSITIVE_INFINITY },
                },
            }),
        );
        const result = h.sent[0]!.value as { usage?: unknown };
        expect(h.sent[0]!.$case).toBe('summaryResult');
        expect(result.usage).toBeUndefined();
    });
});

describe('SummaryAdapter failures', () => {
    it('maps helper errors to model failure without leaking provider text', async () => {
        const h = harness(idleView());
        const raw = new Error('provider exploded: sk-secret');
        await h.adapter.handleSummarize(command(), fakeHelpers({ error: raw }));
        const value = h.sent[0]!.value as {
            code: CommandErrorCode;
            safeMessage: string;
        };
        expect(value.code).toBe(CommandErrorCode.COMMAND_ERROR_MODEL_FAILURE);
        expect(value.safeMessage).toBe('model request failed');
        expect(
            JSON.stringify(
                h.sent,
                (_key, value) =>
                    (typeof value === 'bigint'
                        ? String(value)
                        : value) as unknown,
            ),
        ).not.toContain('sk-secret');
        expect(h.errors).toHaveLength(1);
    });

    it('rejects empty summaries', async () => {
        const h = harness(idleView());
        await h.adapter.handleSummarize(command(), fakeHelpers({ text: '' }));
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_MODEL_FAILURE,
        );
    });

    it('rejects overlong summaries during the stream', async () => {
        const registry = fakeRegistry({
            deltas: ['x'.repeat(MAX_SUMMARY_TEXT_BYTES + 1)],
        });
        const h = harness(idleView({ registry }));
        await h.adapter.handleSummarize(
            command(),
            fakeHelpers({ text: 'small' }),
        );
        const value = h.sent[0]!.value as { code: CommandErrorCode };
        expect(value.code).toBe(CommandErrorCode.COMMAND_ERROR_LIMIT);
        const options = registry.calls[0]![2] as { signal: AbortSignal };
        expect(options.signal.aborted).toBe(true);
    });

    it('rejects overlong final text', async () => {
        const h = harness(idleView());
        await h.adapter.handleSummarize(
            command(),
            fakeHelpers({ text: 'y'.repeat(MAX_SUMMARY_TEXT_BYTES + 1) }),
        );
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_LIMIT,
        );
    });

    it('maps context failures to internal errors', async () => {
        const failing = harness(
            fakeView({
                model: {
                    provider: 'p',
                    id: 'm',
                    maxTokens: 1,
                    reasoning: false,
                },
            }),
        );
        await failing.adapter.handleSummarize(
            command(),
            fakeHelpers({ buildError: new Error('boom') }),
        );
        expect(
            (failing.sent[0]!.value as { code: CommandErrorCode }).code,
        ).toBe(CommandErrorCode.COMMAND_ERROR_INTERNAL);
        expect(failing.errors).toHaveLength(1);
    });
});

describe('SummaryAdapter cancellation', () => {
    it('acks unknown cancels immediately', () => {
        const h = harness(idleView());
        h.adapter.handleCancel(9n);
        expect(h.sent).toHaveLength(1);
        expect(h.sent[0]!.$case).toBe('cancelAcknowledged');
        expect(
            (h.sent[0]!.value as { disposition: SummaryTermination })
                .disposition,
        ).toBe(SummaryTermination.SUMMARY_TERMINATION_UNKNOWN);
    });

    it('acks after the local invocation ends', async () => {
        const h = harness(idleView());
        let release!: () => void;
        const gate = new Promise<void>((resolve) => (release = resolve));
        const helpers = gatedHelpers(fakeHelpers(), gate);
        const run = h.adapter.handleSummarize(command(), helpers);
        await new Promise((resolve) => setTimeout(resolve, 10));
        h.adapter.handleCancel(1n);
        // No ack while the invocation is still running.
        expect(h.sent).toHaveLength(0);
        release();
        await run;
        expect(h.sent).toHaveLength(1);
        expect(h.sent[0]!.$case).toBe('cancelAcknowledged');
        expect(
            (h.sent[0]!.value as { disposition: SummaryTermination })
                .disposition,
        ).toBe(SummaryTermination.SUMMARY_TERMINATION_ENDED);
    });

    it('reports host cancellation as a cancel error', async () => {
        const h = harness(idleView());
        let release!: () => void;
        const gate = new Promise<void>((resolve) => (release = resolve));
        const helpers = gatedHelpers(fakeHelpers(), gate);
        const run = h.adapter.handleSummarize(command(), helpers);
        await new Promise((resolve) => setTimeout(resolve, 10));
        h.adapter.cancelActive();
        release();
        await run;
        expect(h.sent[0]!.$case).toBe('commandError');
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_CANCELED,
        );
    });

    it('never yields a summary after an abort', async () => {
        const registry = fakeRegistry({ stopReason: 'aborted' });
        const h = harness(idleView({ registry }));
        await h.adapter.handleSummarize(
            command(),
            fakeHelpers({ text: 'must not send' }),
        );
        expect(h.sent[0]!.$case).toBe('commandError');
        expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
            CommandErrorCode.COMMAND_ERROR_CANCELED,
        );
    });

    it('expires the command deadline', async () => {
        vi.useFakeTimers();
        try {
            const h = harness(idleView());
            const run = h.adapter.handleSummarize(
                command({ deadlineMs: 50n }),
                fakeHelpers({ hangUntilAbort: true }),
            );
            await vi.advanceTimersByTimeAsync(60);
            await run;
            expect(h.sent).toHaveLength(1);
            expect((h.sent[0]!.value as { code: CommandErrorCode }).code).toBe(
                CommandErrorCode.COMMAND_ERROR_DEADLINE_EXPIRED,
            );
        } finally {
            vi.useRealTimers();
        }
    });
});

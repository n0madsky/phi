import { describe, expect, it } from 'vitest';
import type { ServiceError } from '@grpc/grpc-js';
import {
    type AgentSettled,
    CommandErrorCode,
    LogicalState,
    type PhiToPi,
    type PiToPhi,
    RunOutcome,
    Summarize,
} from './gen/bridge.js';
import { CAPABILITY_LIFECYCLE, CAPABILITY_SUMMARY } from './protocol.js';
import type { ChannelCreds, ChannelHooks } from './channel.js';
import { type PiContextView, Runtime, type RuntimeChannel } from './runtime.js';
import type { SummaryHelpers } from './summary.js';

const CREDS: ChannelCreds = {
    target: 'unix:/tmp/test.sock',
    pane: 'pane',
    process: 'proc',
    token: 'tok',
};

function ctx(over: Partial<PiContextView> = {}): PiContextView {
    return {
        sessionManager: { getSessionId: () => 'sess', getLeafId: () => 'leaf' },
        model: {
            provider: 'p',
            model: 'm',
            providerId: 'p',
            id: 'm',
        } as unknown as PiContextView['model'],
        ...over,
    };
}

/** Full summary surface over the lifecycle-only ctx shape. */
function fullCtx(over: Partial<PiContextView> = {}): PiContextView {
    return ctx({
        sessionManager: {
            getSessionId: () => 'sess',
            getLeafId: () => 'leaf',
            getBranch: () => [{ id: 'e1' }] as never,
        },
        modelRegistry: { streamSimple: () => fakeStream() } as never,
        isIdle: () => true,
        hasPendingMessages: () => false,
        getContextUsage: () => ({
            tokens: 10,
            contextWindow: 100000,
            percent: 0.01,
        }),
        ...over,
    });
}

function fakeStream(): AsyncIterable<unknown> & { result(): Promise<unknown> } {
    return {
        async *[Symbol.asyncIterator]() {
            yield {
                type: 'text_delta',
                contentIndex: 0,
                delta: 'ok',
                partial: {},
            };
        },
        result: async () => ({ stopReason: 'stop', model: 'm' }),
    };
}

function fakeHelpers(): SummaryHelpers {
    return {
        buildSessionContext: (() => ({
            messages: [
                { role: 'user', content: [{ type: 'text', text: 'hi' }] },
            ],
            thinkingLevel: undefined,
            model: undefined,
        })) as never,
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
            const stream = streamFn(
                {} as never,
                {} as never,
                { signal } as never,
            );
            await stream.result();
            return {
                text: 'Summary.',
                usage: {
                    input: 1,
                    output: 1,
                    totalTokens: 2,
                    cost: { total: 0 },
                },
            };
        }) as never,
    };
}

interface MockChannel extends RuntimeChannel {
    hellos: PiToPhi[];
    sends: PiToPhi[];
    closed: number;
    hooks: ChannelHooks | null;
    sendResult: boolean;
    connectError: Error | null;
}

function mockChannel(): MockChannel {
    const m: MockChannel = {
        hellos: [],
        sends: [],
        closed: 0,
        hooks: null,
        sendResult: true,
        connectError: null,
        connect: async (hello: PiToPhi) => {
            m.hellos.push(hello);
            if (m.connectError) throw m.connectError;
            return {
                protocolVersion: 1,
                capabilities: [CAPABILITY_LIFECYCLE],
                runtimeGeneration: 1n,
                runtimeId: 'rt',
            };
        },
        send: (msg: PiToPhi) => {
            m.sends.push(msg);
            return m.sendResult;
        },
        close: () => {
            m.closed += 1;
        },
    };
    return m;
}

function wired(
    ch: MockChannel,
): (_c: ChannelCreds, hooks: ChannelHooks) => RuntimeChannel {
    return (_c, hooks) => {
        ch.hooks = hooks;
        return ch;
    };
}

function status(code: number): ServiceError {
    return { code, details: 'test', metadata: {} } as ServiceError;
}

function payloadOf(msg: PiToPhi): NonNullable<PiToPhi['payload']> {
    const payload = msg.payload;
    if (!payload) throw new Error('missing payload');
    return payload;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('Runtime hello', () => {
    it('advertises lifecycle only with provisioned identity', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 3n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        expect(ch.hellos).toHaveLength(1);
        const hello = ch.hellos[0]!;
        expect(hello.paneId).toBe('pane');
        expect(hello.processId).toBe('proc');
        expect(hello.runtimeGeneration).toBe(3n);
        const payload = payloadOf(hello);
        expect(payload.$case).toBe('hello');
        if (payload.$case !== 'hello') return;
        expect(payload.value.capabilities).toEqual([CAPABILITY_LIFECYCLE]);
        expect(payload.value.sessionId).toBe('sess');
        expect(payload.value.currentState?.state).toBe(
            LogicalState.LOGICAL_STATE_IDLE,
        );
        expect(payload.value.currentState?.leafId).toBe('leaf');
        expect(payload.value.currentState?.model?.provider).toBe('p');
        rt.shutdown();
    });
});

describe('Runtime lifecycle mapping', () => {
    it('opens one run on first start and continues on extra starts', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        rt.onAgentStart(ctx());
        rt.onAgentStart(ctx());
        const started = ch.sends.filter(
            (m) => payloadOf(m).$case === 'agentStarted',
        );
        expect(started).toHaveLength(2);
        const first = payloadOf(started[0]!);
        const second = payloadOf(started[1]!);
        if (first.$case !== 'agentStarted' || second.$case !== 'agentStarted')
            return;
        expect(first.value.runId).toBe(1n);
        expect(second.value.runId).toBe(1n);
        expect(second.value.eventSequence).toBe(first.value.eventSequence + 1n);
        const snaps = ch.sends.filter(
            (m) => payloadOf(m).$case === 'stateSnapshot',
        );
        expect(snaps).toHaveLength(2);
        rt.shutdown();
    });

    it('settles the observed run and ignores idle settlements', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        rt.onAgentSettled(ctx(), false);
        expect(ch.sends).toHaveLength(0);
        rt.onAgentStart(ctx());
        rt.onAgentSettled(ctx(), false);
        const settled = ch.sends.filter(
            (m) => payloadOf(m).$case === 'agentSettled',
        );
        expect(settled).toHaveLength(1);
        const payload = payloadOf(settled[0]!);
        if (payload.$case !== 'agentSettled') return;
        expect(payload.value.runId).toBe(1n);
        expect(payload.value.outcome).toBe(RunOutcome.RUN_OUTCOME_COMPLETED);
        // Duplicate settlement after completion is ignored.
        rt.onAgentSettled(ctx(), false);
        expect(
            ch.sends.filter((m) => payloadOf(m).$case === 'agentSettled'),
        ).toHaveLength(1);
        rt.shutdown();
    });

    it('maps abort to ABORTED and never reuses an old outcome', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        rt.onAgentStart(ctx());
        rt.onAgentSettled(ctx(), true);
        rt.onAgentStart(ctx());
        rt.onAgentSettled(ctx(), false);
        const settled = ch.sends.filter(
            (m) => payloadOf(m).$case === 'agentSettled',
        );
        expect(settled).toHaveLength(2);
        const outcomes = settled.map((m) => {
            const p = payloadOf(m);
            return p.$case === 'agentSettled'
                ? (p.value as AgentSettled).outcome
                : undefined;
        });
        expect(outcomes).toEqual([
            RunOutcome.RUN_OUTCOME_ABORTED,
            RunOutcome.RUN_OUTCOME_COMPLETED,
        ]);
        rt.shutdown();
    });

    it('bounds the journal and reports the gap', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        for (let i = 0; i < 40; i++) {
            rt.onAgentStart(ctx());
            rt.onAgentSettled(ctx(), false);
        }
        const snaps = ch.sends.filter(
            (m) => payloadOf(m).$case === 'stateSnapshot',
        );
        const last = payloadOf(snaps[snaps.length - 1]!);
        if (last.$case !== 'stateSnapshot') return;
        expect(last.value.journal.length).toBe(64);
        expect(last.value.journalGap).toBe(true);
        expect(last.value.journal[0]!.eventSequence).toBe(17n);
        rt.shutdown();
    });
});

describe('Runtime server commands', () => {
    it('answers summarize with unsupported capability and ignores cancel', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        ch.hooks!.onMessage({
            payload: {
                $case: 'summarize',
                value: Summarize.create({ requestId: 9n }),
            },
        } as PhiToPi);
        ch.hooks!.onMessage({
            payload: { $case: 'cancel', value: { requestId: 9n } },
        } as PhiToPi);
        const errors = ch.sends.filter(
            (m) => payloadOf(m).$case === 'commandError',
        );
        expect(errors).toHaveLength(1);
        const payload = payloadOf(errors[0]!);
        if (payload.$case !== 'commandError') return;
        expect(payload.value.requestId).toBe(9n);
        expect(payload.value.code).toBe(
            CommandErrorCode.COMMAND_ERROR_UNSUPPORTED_CAPABILITY,
        );
        rt.shutdown();
    });

    it('advertises the summary capability with helpers and a full surface', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
            helpers: fakeHelpers(),
        });
        rt.start(fullCtx());
        await sleep(10);
        const payload = payloadOf(ch.hellos[0]!);
        if (payload.$case !== 'hello') return;
        expect(payload.value.capabilities).toEqual([
            CAPABILITY_LIFECYCLE,
            CAPABILITY_SUMMARY,
        ]);
        rt.shutdown();
    });

    it('runs one summary to a result through the dispatch', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
            helpers: fakeHelpers(),
        });
        rt.start(fullCtx());
        await sleep(10);
        ch.hooks!.onMessage({
            payload: {
                $case: 'summarize',
                value: Summarize.create({
                    requestId: 1n,
                    sessionId: 'sess',
                    leafId: 'leaf',
                    model: { provider: 'p', model: 'm' },
                    deadlineMs: 5000n,
                }),
            },
        } as PhiToPi);
        await sleep(20);
        const results = ch.sends.filter(
            (m) => payloadOf(m).$case === 'summaryResult',
        );
        expect(results).toHaveLength(1);
        const payload = payloadOf(results[0]!);
        if (payload.$case !== 'summaryResult') return;
        expect(payload.value.text).toBe('Summary.');
        rt.shutdown();
    });

    it('cancels the in-flight summary on a new agent run', async () => {
        const ch = mockChannel();
        const helpers = fakeHelpers();
        let release!: () => void;
        const gate = new Promise<void>((resolve) => (release = resolve));
        const inner = helpers.generateSummaryWithUsage;
        const gated: SummaryHelpers = {
            ...helpers,
            generateSummaryWithUsage: (async (
                ...args: Parameters<SummaryHelpers['generateSummaryWithUsage']>
            ) => {
                await gate;
                return inner(...args);
            }) as SummaryHelpers['generateSummaryWithUsage'],
        };
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
            helpers: gated,
        });
        rt.start(fullCtx());
        await sleep(10);
        ch.hooks!.onMessage({
            payload: {
                $case: 'summarize',
                value: Summarize.create({
                    requestId: 1n,
                    sessionId: 'sess',
                    leafId: 'leaf',
                    model: { provider: 'p', model: 'm' },
                    deadlineMs: 5000n,
                }),
            },
        } as PhiToPi);
        await sleep(10);
        rt.onAgentStart(fullCtx());
        release();
        await sleep(20);
        const errors = ch.sends.filter(
            (m) => payloadOf(m).$case === 'commandError',
        );
        expect(errors).toHaveLength(1);
        const payload = payloadOf(errors[0]!);
        if (payload.$case !== 'commandError') return;
        expect(payload.value.code).toBe(
            CommandErrorCode.COMMAND_ERROR_CANCELED,
        );
        rt.shutdown();
    });

    it('publishes snapshots on identity events', async () => {
        const ch = mockChannel();
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            makeChannel: wired(ch),
        });
        rt.start(ctx());
        await sleep(10);
        const before = ch.sends.length;
        rt.onModelSelect(ctx());
        rt.onSessionTree(ctx());
        rt.onCompactStart(ctx());
        rt.onCompactEnd(ctx());
        rt.onSessionStart(ctx());
        const snapshots = ch.sends
            .slice(before)
            .filter((m) => payloadOf(m).$case === 'stateSnapshot');
        // Model, tree, compaction end, and replacement session refresh
        // identities; compaction start only marks, never snapshots.
        expect(snapshots).toHaveLength(4);
        rt.shutdown();
    });
});

describe('Runtime reconnect', () => {
    it('retries retryable ends with the same generation and journal', async () => {
        const channels: MockChannel[] = [];
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            backoffBaseMs: 5,
            backoffMaxMs: 10,
            makeChannel: (_c, hooks) => {
                const ch = mockChannel();
                ch.hooks = hooks;
                channels.push(ch);
                return ch;
            },
        });
        rt.start(ctx());
        await sleep(10);
        rt.onAgentStart(ctx());
        channels[0]!.hooks!.onStatus(status(14));
        await sleep(30);
        expect(channels.length).toBe(2);
        const hello2 = payloadOf(channels[1]!.hellos[0]!);
        if (hello2.$case !== 'hello') return;
        expect(channels[1]!.hellos[0]!.runtimeGeneration).toBe(1n);
        expect(hello2.value.currentState?.journal.length).toBe(1);
        expect(hello2.value.currentState?.state).toBe(
            LogicalState.LOGICAL_STATE_IDLE,
        );
        rt.shutdown();
    });

    it('stops on terminal statuses', async () => {
        for (const code of [16, 7, 9, 1]) {
            const channels: MockChannel[] = [];
            const rt = new Runtime({
                creds: CREDS,
                generation: 1n,
                backoffBaseMs: 5,
                backoffMaxMs: 10,
                makeChannel: (_c, hooks) => {
                    const ch = mockChannel();
                    ch.hooks = hooks;
                    channels.push(ch);
                    return ch;
                },
            });
            rt.start(ctx());
            await sleep(10);
            channels[0]!.hooks!.onStatus(status(code));
            await sleep(25);
            expect(channels.length, `code ${code}`).toBe(1);
            rt.shutdown();
        }
    });

    it('reconnects after queue overflow instead of gapping the stream', async () => {
        const channels: MockChannel[] = [];
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            backoffBaseMs: 5,
            backoffMaxMs: 10,
            makeChannel: (_c, hooks) => {
                const ch = mockChannel();
                ch.hooks = hooks;
                channels.push(ch);
                return ch;
            },
        });
        rt.start(ctx());
        await sleep(10);
        channels[0]!.sendResult = false;
        rt.onAgentStart(ctx());
        await sleep(30);
        expect(channels.length).toBe(2);
        rt.shutdown();
    });

    it('shutdown stops retries and ignores events', async () => {
        const channels: MockChannel[] = [];
        const rt = new Runtime({
            creds: CREDS,
            generation: 1n,
            backoffBaseMs: 5,
            backoffMaxMs: 10,
            makeChannel: (_c, hooks) => {
                const ch = mockChannel();
                ch.hooks = hooks;
                channels.push(ch);
                return ch;
            },
        });
        rt.start(ctx());
        await sleep(10);
        rt.shutdown();
        expect(channels[0]!.closed).toBe(1);
        channels[0]!.hooks!.onStatus(status(14));
        rt.onAgentStart(ctx());
        await sleep(25);
        expect(channels.length).toBe(1);
        expect(channels[0]!.sends).toHaveLength(0);
    });

    it('maps aborted settlements from the 1.3.0 event shape', async () => {
        const { settledAbortedForTest } = await import('./extension.js');
        expect(
            settledAbortedForTest({ type: 'agent_settled', aborted: true }),
        ).toBe(true);
        expect(settledAbortedForTest({ type: 'agent_settled' })).toBe(false);
        expect(
            settledAbortedForTest({ type: 'agent_settled', aborted: 1 }),
        ).toBe(false);
        expect(settledAbortedForTest(null)).toBe(false);
    });
});

// Explicit session-summary adapter. Runs one Phi-issued Summarize
// command against the live Pi session: idle recheck, snapshot
// identity compare, context budget, then one bounded call to the
// host's public generateSummaryWithUsage through the current host
// registry's streamSimple. Cancellation and failures never yield a
// summary. Safe messages are fixed strings; raw provider errors are
// logged, never sent over the bridge.

import {
    CancelAcknowledged,
    CommandError,
    CommandErrorCode,
    ModelSnapshot,
    SummaryResult,
    SummaryTermination,
    Usage,
    type Summarize,
} from './gen/bridge.js';
import { CommandTracker } from './commands.js';
import { MAX_SUMMARY_TEXT_BYTES, SUMMARY_DEADLINE_MS } from './protocol.js';
import type { PiContextView } from './runtime.js';

type HostModule = typeof import('@earendil-works/pi-coding-agent');

/** Host summary helpers, injected by the entry shim. Null disables summaries. */
export interface SummaryHelpers {
    readonly buildSessionContext: HostModule['buildSessionContext'];
    readonly generateSummaryWithUsage: HostModule['generateSummaryWithUsage'];
}

/** Fixed summary instructions. Never user-controlled. */
export const SUMMARY_INSTRUCTIONS =
    'Include goals, decisions, progress, and next steps.';

/** Output token reserve passed to the summary helper. */
const RESERVE_TOKENS = 4096;

/**
 * Settle grace after the command deadline. A hung provider that ignores
 * abort must not hold the server admission slot forever: once the
 * grace expires, the adapter sends terminal evidence and abandons the
 * invocation. A late settlement is dropped, never sent twice.
 */
const SETTLE_GRACE_MS = 30_000;

/** Helper-level retries stay disabled; the bridge never replays paid work. */
const NO_RETRY = { enabled: false, maxRetries: 0, baseDelayMs: 0 };

export interface SummaryDeps {
    /** Latest live context view, or null before the first event. */
    getView(): PiContextView | null;
    /** True while a compaction run is in flight. */
    isCompacting(): boolean;
    /** Current snapshot revision for result attribution. */
    getRevision(): bigint;
    /** Send one result, error, or acknowledgment frame. */
    send(payload: SummaryPayload): void;
    /** Log a raw failure for host-side diagnosis. Never sent. */
    logError(message: string, error: unknown): void;
}

type SummaryPayload =
    | { $case: 'summaryResult'; value: SummaryResult }
    | { $case: 'commandError'; value: CommandError }
    | { $case: 'cancelAcknowledged'; value: CancelAcknowledged };

interface Observed {
    bytes: number;
    limitHit: boolean;
    stopReason: string | null;
    model: string | null;
}

type RunOutcome =
    | {
          kind: 'result';
          text: string;
          usage: unknown;
          sessionId: string;
          leafId: string;
          reported: string | null;
      }
    | { kind: 'error'; code: CommandErrorCode; message: string }
    | { kind: 'ack'; disposition: SummaryTermination };

/** True when the view exposes every surface the adapter needs. */
export function hasSummarySurface(view: PiContextView): boolean {
    return (
        typeof view.sessionManager.getBranch === 'function' &&
        typeof view.modelRegistry?.streamSimple === 'function' &&
        typeof view.isIdle === 'function' &&
        typeof view.hasPendingMessages === 'function' &&
        typeof view.getContextUsage === 'function'
    );
}

/** Guard one live-context read; Pi asserts active on every access. */
function read<T>(fn: () => T): T | undefined {
    try {
        return fn();
    } catch {
        return undefined;
    }
}

/** Narrow a value the host typed but the bridge only observes. */
function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function optionalUint64(value: unknown): bigint | undefined {
    if (
        typeof value !== 'number' ||
        !Number.isInteger(value) ||
        value < 0 ||
        value > Number.MAX_SAFE_INTEGER
    ) {
        return undefined;
    }
    return BigInt(value);
}

/** One adapter per runtime; the tracker may be shared across replacement. */
export class SummaryAdapter {
    private active: bigint | null = null;

    constructor(
        private readonly deps: SummaryDeps,
        private readonly tracker: CommandTracker = new CommandTracker(),
    ) {}

    /** Abort the in-flight summary, if any, for a host-side reason. */
    cancelActive(): void {
        if (this.active !== null) this.tracker.noteCancel(this.active, 'host');
    }

    get hasActive(): boolean {
        return this.active !== null;
    }

    /** Handle one bridge Cancel. Unknown IDs ack immediately as unknown. */
    handleCancel(requestId: bigint): void {
        if (!this.tracker.noteCancel(requestId, 'phi')) {
            this.deps.send({
                $case: 'cancelAcknowledged',
                value: CancelAcknowledged.create({
                    requestId,
                    disposition: SummaryTermination.SUMMARY_TERMINATION_UNKNOWN,
                }),
            });
        }
        // Pending requests ack ENDED from the run finally below, only
        // after the local invocation has actually ended.
    }

    /** Run one Summarize command to a terminal frame. Never throws. */
    async handleSummarize(
        cmd: Summarize,
        helpers: SummaryHelpers | null,
    ): Promise<void> {
        const id = cmd.requestId;
        if (this.tracker.noteSummarize(id) === 'duplicate') {
            this.fail(
                id,
                CommandErrorCode.COMMAND_ERROR_DUPLICATE_REQUEST,
                'duplicate request',
            );
            return;
        }
        if (helpers === null) {
            this.fail(
                id,
                CommandErrorCode.COMMAND_ERROR_UNSUPPORTED_CAPABILITY,
                'summary not available',
            );
            return;
        }
        const view = this.deps.getView();
        if (view === null || !hasSummarySurface(view)) {
            this.fail(
                id,
                CommandErrorCode.COMMAND_ERROR_INTERNAL,
                'no session context',
            );
            return;
        }
        if (this.active !== null) {
            this.fail(
                id,
                CommandErrorCode.COMMAND_ERROR_BUSY,
                'summary already running',
            );
            return;
        }
        const controller = new AbortController();
        if (!this.tracker.setController(id, controller)) {
            this.finishCancel(id);
            return;
        }
        this.active = id;
        const deadlineMs = clampDeadline(cmd.deadlineMs);
        let timedOut = false;
        const timer = setTimeout(() => {
            timedOut = true;
            controller.abort();
        }, deadlineMs);
        // Timers must not keep the host process alive on their own.
        timer.unref?.();
        let done = false;
        const finish = (outcome: RunOutcome): void => {
            if (done) return;
            done = true;
            clearTimeout(timer);
            clearTimeout(grace);
            if (this.active === id) this.active = null;
            this.sendOutcome(id, outcome, cmd);
            this.tracker.complete(id);
        };
        const grace = setTimeout(() => {
            controller.abort();
            if (this.tracker.phiCancelled(id)) {
                finish({
                    kind: 'ack',
                    disposition: SummaryTermination.SUMMARY_TERMINATION_ENDED,
                });
            } else if (this.tracker.isCancelled(id)) {
                finish({
                    kind: 'error',
                    code: CommandErrorCode.COMMAND_ERROR_CANCELED,
                    message: 'summary cancelled',
                });
            } else {
                finish({
                    kind: 'error',
                    code: CommandErrorCode.COMMAND_ERROR_DEADLINE_EXPIRED,
                    message: 'summary deadline expired',
                });
            }
        }, deadlineMs + SETTLE_GRACE_MS);
        grace.unref?.();
        const started = Date.now();
        const outcome = await this.run(
            id,
            cmd,
            view,
            helpers,
            controller,
            started,
            deadlineMs,
            () => timedOut,
        );
        finish(outcome);
    }

    private async run(
        id: bigint,
        cmd: Summarize,
        view: PiContextView,
        helpers: SummaryHelpers,
        controller: AbortController,
        started: number,
        deadlineMs: number,
        isTimedOut: () => boolean,
    ): Promise<RunOutcome> {
        if (!this.checkIdle(view) || this.tracker.isCancelled(id)) {
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_BUSY,
                'agent is busy',
            );
        }
        const model = read(() => view.model) ?? undefined;
        if (model === undefined) {
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_INTERNAL,
                'no model selected',
            );
        }
        if (!this.checkSnapshot(cmd, view)) {
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_STALE_SNAPSHOT,
                'session changed',
            );
        }
        const sessionId = read(() => view.sessionManager.getSessionId()) ?? '';
        const leafId = read(() => view.sessionManager.getLeafId()) ?? null;
        let branch: ReturnType<
            NonNullable<PiContextView['sessionManager']['getBranch']>
        >;
        try {
            branch = view.sessionManager.getBranch!();
        } catch {
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_INTERNAL,
                'cannot read session',
            );
        }
        let messages: { length: number } & Parameters<
            SummaryHelpers['generateSummaryWithUsage']
        >[0];
        try {
            messages = helpers.buildSessionContext(
                branch,
                leafId ?? undefined,
            ).messages;
        } catch (error) {
            this.deps.logError('buildSessionContext failed', error);
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_INTERNAL,
                'cannot read session',
            );
        }
        if (messages.length === 0) {
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_INTERNAL,
                'session is empty',
            );
        }
        const budget = this.checkBudget(view, messages);
        if (budget !== null) {
            return this.gateOutcome(
                id,
                CommandErrorCode.COMMAND_ERROR_LIMIT,
                budget,
            );
        }
        const observed: Observed = {
            bytes: 0,
            limitHit: false,
            stopReason: null,
            model: null,
        };
        let observedDone: Promise<void> | null = null;
        const registry = view.modelRegistry!;
        const streamFn: NonNullable<
            Parameters<SummaryHelpers['generateSummaryWithUsage']>[9]
        > = (streamModel, context, options) => {
            const remaining = Math.max(1, deadlineMs - (Date.now() - started));
            const stream = registry.streamSimple(streamModel, context, {
                ...options,
                maxRetries: 0,
                timeoutMs: remaining,
                signal: controller.signal,
            });
            // Classification awaits observation below, so a limit
            // exceed always reports the limit, never a late cancel.
            observedDone = this.observe(stream, observed, controller).catch(
                () => {
                    // Observation never fails the summary.
                },
            );
            return stream;
        };
        let text: string;
        let usage: unknown;
        try {
            const result = await helpers.generateSummaryWithUsage(
                messages,
                model,
                RESERVE_TOKENS,
                undefined,
                undefined,
                controller.signal,
                SUMMARY_INSTRUCTIONS,
                undefined,
                read(() => view.thinkingLevel) ?? undefined,
                streamFn,
                undefined,
                NO_RETRY,
                undefined,
                undefined,
            );
            text = result.text;
            usage = result.usage;
        } catch (error) {
            if (observedDone !== null) await observedDone;
            return this.settledOutcome(
                id,
                isTimedOut,
                observed,
                () => {
                    this.deps.logError(
                        'generateSummaryWithUsage failed',
                        error,
                    );
                    return {
                        kind: 'error',
                        code: CommandErrorCode.COMMAND_ERROR_MODEL_FAILURE,
                        message: 'model request failed',
                    } as const;
                },
                controller.signal.aborted,
            );
        }
        if (observedDone !== null) await observedDone;
        return this.settledOutcome(
            id,
            isTimedOut,
            observed,
            () => {
                if (
                    observed.limitHit ||
                    Buffer.byteLength(text, 'utf8') > MAX_SUMMARY_TEXT_BYTES
                ) {
                    return {
                        kind: 'error',
                        code: CommandErrorCode.COMMAND_ERROR_LIMIT,
                        message: 'summary too large',
                    } as const;
                }
                if (text === '') {
                    return {
                        kind: 'error',
                        code: CommandErrorCode.COMMAND_ERROR_MODEL_FAILURE,
                        message: 'empty summary',
                    } as const;
                }
                return {
                    kind: 'result',
                    text,
                    usage,
                    sessionId,
                    leafId: leafId ?? '',
                    reported: observed.model,
                } as const;
            },
            controller.signal.aborted,
        );
    }

    /**
     * Classify a settled invocation. A bridge Cancel owns an ack;
     * otherwise the deadline, then the output limit, then any
     * cancellation wins over the natural outcome, so cancellation
     * never yields a summary and a self-abort for the limit reports
     * the limit, not a cancellation.
     */
    private settledOutcome(
        id: bigint,
        isTimedOut: () => boolean,
        observed: Observed,
        natural: () => RunOutcome,
        aborted: boolean,
    ): RunOutcome {
        if (this.tracker.phiCancelled(id)) {
            return {
                kind: 'ack',
                disposition: SummaryTermination.SUMMARY_TERMINATION_ENDED,
            };
        }
        if (isTimedOut()) {
            return {
                kind: 'error',
                code: CommandErrorCode.COMMAND_ERROR_DEADLINE_EXPIRED,
                message: 'summary deadline expired',
            };
        }
        if (observed.limitHit) {
            return {
                kind: 'error',
                code: CommandErrorCode.COMMAND_ERROR_LIMIT,
                message: 'summary too large',
            };
        }
        if (
            this.tracker.isCancelled(id) ||
            aborted ||
            observed.stopReason === 'aborted'
        ) {
            return {
                kind: 'error',
                code: CommandErrorCode.COMMAND_ERROR_CANCELED,
                message: 'summary cancelled',
            };
        }
        return natural();
    }

    /** Send one terminal outcome frame. Called exactly once per run. */
    private sendOutcome(id: bigint, outcome: RunOutcome, cmd: Summarize): void {
        if (outcome.kind === 'ack') {
            this.deps.send({
                $case: 'cancelAcknowledged',
                value: CancelAcknowledged.create({
                    requestId: id,
                    disposition: outcome.disposition,
                }),
            });
            return;
        }
        if (outcome.kind === 'error') {
            this.deps.send({
                $case: 'commandError',
                value: CommandError.create({
                    requestId: id,
                    code: outcome.code,
                    safeMessage: outcome.message,
                }),
            });
            return;
        }
        const view = this.deps.getView();
        const model = view?.model;
        this.deps.send({
            $case: 'summaryResult',
            value: SummaryResult.create({
                requestId: id,
                sessionId: outcome.sessionId,
                leafId: outcome.leafId,
                text: outcome.text,
                selectedModel: cmd.model,
                reportedModel:
                    model === undefined || model === null
                        ? undefined
                        : ModelSnapshot.create({
                              provider: model.provider,
                              model: outcome.reported ?? model.id,
                          }),
                usage: mapUsage(outcome.usage),
                stateRevision: this.deps.getRevision(),
            }),
        });
    }

    private checkIdle(view: PiContextView): boolean {
        if (this.deps.isCompacting()) return false;
        if (read(() => view.isIdle!()) !== true) return false;
        if (read(() => view.hasPendingMessages!()) !== false) return false;
        return true;
    }

    private checkSnapshot(cmd: Summarize, view: PiContextView): boolean {
        const sessionId = read(() => view.sessionManager.getSessionId());
        if (sessionId === undefined || cmd.sessionId !== sessionId)
            return false;
        const leafId = read(() => view.sessionManager.getLeafId()) ?? '';
        if (cmd.leafId !== (leafId ?? '')) return false;
        const model = read(() => view.model) ?? undefined;
        if (cmd.model === undefined) return model === undefined;
        if (model === undefined) return false;
        return (
            cmd.model.provider === model.provider &&
            cmd.model.model === model.id
        );
    }

    /**
     * Refuse calls that cannot fit the window. Returns a safe message
     * when over budget, null when the call may proceed. Unknown usage
     * falls back to a rough length estimate so a fresh compaction does
     * not refuse a summarizable session.
     */
    private checkBudget(
        view: PiContextView,
        messages: readonly unknown[],
    ): string | null {
        const usage = read(() => view.getContextUsage!());
        const window = usage?.contextWindow;
        if (
            typeof window !== 'number' ||
            !Number.isFinite(window) ||
            window <= 0
        ) {
            return null;
        }
        let tokens = usage?.tokens;
        if (
            typeof tokens !== 'number' ||
            !Number.isFinite(tokens) ||
            tokens < 0
        ) {
            tokens = estimateTokens(messages);
        }
        if (tokens + RESERVE_TOKENS > window) {
            return 'session exceeds the context budget';
        }
        return null;
    }

    private async observe(
        stream: AsyncIterable<unknown> & { result(): Promise<unknown> },
        observed: Observed,
        controller: AbortController,
    ): Promise<void> {
        const tapped = stream.result().then(
            (message) => {
                if (!isRecord(message)) return;
                if (typeof message['stopReason'] === 'string') {
                    observed.stopReason = message['stopReason'] as string;
                }
                if (typeof message['model'] === 'string') {
                    observed.model = message['model'] as string;
                }
            },
            () => {
                // The helper owns stream errors; observation only watches.
            },
        );
        for await (const event of stream) {
            if (!isRecord(event)) continue;
            if (
                event['type'] !== 'text_delta' ||
                typeof event['delta'] !== 'string'
            )
                continue;
            observed.bytes += Buffer.byteLength(
                event['delta'] as string,
                'utf8',
            );
            if (observed.bytes > MAX_SUMMARY_TEXT_BYTES) {
                observed.limitHit = true;
                controller.abort();
                break;
            }
        }
        await tapped;
    }

    /** A pre-work cancel owes an ack when Phi asked, else a cancel error. */
    private finishCancel(id: bigint): void {
        if (this.tracker.phiCancelled(id)) {
            this.ack(id, SummaryTermination.SUMMARY_TERMINATION_ENDED);
        } else {
            this.fail(
                id,
                CommandErrorCode.COMMAND_ERROR_CANCELED,
                'summary cancelled',
            );
        }
        this.tracker.complete(id);
    }

    /** Cancel wins over any gate failure once the run owns the request. */
    private gateOutcome(
        id: bigint,
        code: CommandErrorCode,
        message: string,
    ): RunOutcome {
        if (this.tracker.phiCancelled(id)) {
            return {
                kind: 'ack',
                disposition: SummaryTermination.SUMMARY_TERMINATION_ENDED,
            };
        }
        if (this.tracker.isCancelled(id)) {
            return {
                kind: 'error',
                code: CommandErrorCode.COMMAND_ERROR_CANCELED,
                message: 'summary cancelled',
            };
        }
        return { kind: 'error', code, message };
    }

    private fail(
        id: bigint,
        code: CommandErrorCode,
        safeMessage: string,
    ): void {
        this.deps.send({
            $case: 'commandError',
            value: CommandError.create({ requestId: id, code, safeMessage }),
        });
        if (this.active === id) this.active = null;
        this.tracker.complete(id);
    }

    private ack(id: bigint, disposition: SummaryTermination): void {
        this.deps.send({
            $case: 'cancelAcknowledged',
            value: CancelAcknowledged.create({ requestId: id, disposition }),
        });
        if (this.active === id) this.active = null;
        this.tracker.complete(id);
    }
}

function clampDeadline(deadlineMs: bigint): number {
    if (deadlineMs <= 0n) return SUMMARY_DEADLINE_MS;
    if (deadlineMs > BigInt(SUMMARY_DEADLINE_MS)) return SUMMARY_DEADLINE_MS;
    const ms = Number(deadlineMs);
    return Number.isSafeInteger(ms) && ms > 0 ? ms : SUMMARY_DEADLINE_MS;
}

/** Rough token estimate from text content when usage is unknown. */
function estimateTokens(messages: readonly unknown[]): number {
    let chars = 0;
    for (const message of messages) {
        if (!isRecord(message) || !Array.isArray(message['content'])) continue;
        for (const block of message['content'] as unknown[]) {
            if (!isRecord(block)) continue;
            if (block['type'] === 'text' && typeof block['text'] === 'string') {
                chars += (block['text'] as string).length;
            }
        }
    }
    return Math.ceil(chars / 4);
}

/** Map host usage to the wire form. Non-finite fields are omitted. */
function mapUsage(usage: unknown): Usage | undefined {
    if (!isRecord(usage)) return undefined;
    const input = optionalUint64(usage['input']);
    const output = optionalUint64(usage['output']);
    const cost = isRecord(usage['cost']) ? usage['cost'] : undefined;
    const total = cost?.['total'];
    const costUsd =
        typeof total === 'number' && Number.isFinite(total) ? total : undefined;
    if (input === undefined && output === undefined && costUsd === undefined) {
        return undefined;
    }
    return Usage.create({
        inputTokens: input,
        outputTokens: output,
        costUsd,
        costFromPriceTables: costUsd !== undefined,
    });
}

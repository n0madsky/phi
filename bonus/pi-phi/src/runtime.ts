// Extension runtime: one claimed session_start owns one Runtime. It maps
// Pi lifecycle events to bridge edges, retains the bounded journal,
// publishes authoritative snapshots on transitions, and reconnects the
// stream with the same generation after transport failures. It never
// starts provider work and never touches the Pi session.

import { randomUUID } from 'node:crypto';
import type { ServiceError } from '@grpc/grpc-js';
import type {
    ContextUsage,
    ExtensionContext,
    SessionEntry,
} from '@earendil-works/pi-coding-agent';
import {
    AgentSettled,
    AgentStarted,
    Hello,
    LogicalState,
    ModelSnapshot,
    type PiToPhi,
    RunOutcome,
    StateSnapshot,
} from './gen/bridge.js';
import { BridgeChannel, type ChannelCreds } from './channel.js';
import { CommandTracker } from './commands.js';
import { PROTOCOL_VERSION } from './protocol.js';
import { CAPABILITY_LIFECYCLE } from './protocol.js';
import { CAPABILITY_SUMMARY } from './protocol.js';
import { MAX_JOURNAL_RECORDS } from './protocol.js';
import {
    SummaryAdapter,
    type SummaryHelpers,
    hasSummarySurface,
} from './summary.js';

/** Structural Pi session view; satisfied by both pinned host versions. */
export interface PiSessionView {
    getSessionId(): string;
    getLeafId(): string | null;
    /** Present on hosts that expose the branch for summaries. */
    getBranch?(): SessionEntry[];
}

/** The live host model object, for provider routing. Never copied. */
export type PiModelView = NonNullable<ExtensionContext['model']>;

/** Structural Pi event-context view; satisfied by both pinned versions. */
export interface PiContextView {
    readonly sessionManager: PiSessionView;
    readonly model?: PiModelView | undefined;
    /** Present on hosts that expose the model registry for summaries. */
    readonly modelRegistry?: ExtensionContext['modelRegistry'] | undefined;
    readonly thinkingLevel?:
        | NonNullable<ExtensionContext['thinkingLevel']>
        | undefined;
    /** Present on hosts that expose idle state for summaries. */
    isIdle?(): boolean;
    /** Present on hosts that expose queue state for summaries. */
    hasPendingMessages?(): boolean;
    /** Present on hosts that expose usage for summaries. */
    getContextUsage?(): ContextUsage | undefined;
}

export interface RuntimeOptions {
    readonly creds: ChannelCreds;
    readonly generation: bigint;
    /** Base reconnect delay; doubles per attempt up to maxBackoffMs. */
    readonly backoffBaseMs?: number;
    readonly backoffMaxMs?: number;
    readonly makeChannel?: (
        creds: ChannelCreds,
        hooks: ChannelHooks,
    ) => RuntimeChannel;
    /** Host summary helpers, injected by the entry shim. Null disables summaries. */
    readonly helpers?: SummaryHelpers | null;
    /** Shared command tracker; the mark survives runtime replacement. */
    readonly tracker?: CommandTracker;
}

export interface ChannelHooks {
    onMessage(msg: import('./gen/bridge.js').PhiToPi): void;
    onStatus(status: ServiceError | null): void;
}

/** Narrow channel surface the runtime needs; BridgeChannel satisfies it. */
export interface RuntimeChannel {
    connect(hello: PiToPhi): Promise<import('./gen/bridge.js').Accepted>;
    send(msg: PiToPhi): boolean;
    close(): void;
}

interface JournalRow {
    readonly sequence: bigint;
    readonly runId: bigint;
    readonly settled: boolean;
    readonly outcome: RunOutcome;
}

const TERMINAL_STATUS_CODES = new Set([
    16, // UNAUTHENTICATED: lease revoked or never valid; retry is futile.
    7, // PERMISSION_DENIED: identity mismatch; retry cannot fix it.
    9, // FAILED_PRECONDITION: version or generation rejected; retry cannot fix it.
    1, // CANCELLED: server-side revoke or replace; retry cannot fix it.
]);

/** One claimed extension runtime for one Pi session. */
export class Runtime {
    readonly runtimeId: string = randomUUID();
    private channel: RuntimeChannel | null = null;
    private running = false;
    private seq = 0n;
    private runId = 0n;
    private revision = 0n;
    private journal: JournalRow[] = [];
    private journalGap = false;
    private sessionId = '';
    private leafId = '';
    private model: PiModelView | undefined;
    private lastView: PiContextView | null = null;
    private compacting = false;
    private summaryAvailable = false;
    private readonly tracker: CommandTracker;
    private readonly summaries: SummaryAdapter;
    private shutdownRequested = false;
    private backoffMs: number;
    private retryTimer: ReturnType<typeof setTimeout> | null = null;
    private starting = false;

    constructor(private readonly opts: RuntimeOptions) {
        this.backoffMs = opts.backoffBaseMs ?? 250;
        this.tracker = opts.tracker ?? new CommandTracker();
        this.summaries = new SummaryAdapter(
            {
                getView: () => this.lastView,
                isCompacting: () => this.compacting,
                getRevision: () => this.revision,
                send: (payload) =>
                    this.send({
                        paneId: this.opts.creds.pane,
                        processId: this.opts.creds.process,
                        runtimeGeneration: this.opts.generation,
                        payload,
                    }),
                logError: (message, error) =>
                    console.error(`[pi-phi] ${message}`, error),
            },
            this.tracker,
        );
    }

    /** Begin connecting; never throws, never blocks Pi startup. */
    start(ctx: PiContextView): void {
        if (this.channel || this.starting || this.shutdownRequested) return;
        this.starting = true;
        this.noteView(ctx);
        void this.attempt();
    }

    /** A replacement session invalidates in-flight summaries. */
    onSessionStart(ctx: PiContextView): void {
        if (this.shutdownRequested) return;
        this.summaries.cancelActive();
        this.noteView(ctx);
        this.sendSnapshot(
            this.running
                ? LogicalState.LOGICAL_STATE_RUNNING
                : LogicalState.LOGICAL_STATE_IDLE,
        );
    }

    /** Map one agent_start edge. Extra starts continue the current run. */
    onAgentStart(ctx: PiContextView): void {
        if (this.shutdownRequested) return;
        this.summaries.cancelActive();
        this.noteView(ctx);
        if (!this.running) {
            this.running = true;
            this.runId += 1n;
        }
        const sequence = this.nextSeq();
        this.appendJournal({
            sequence,
            runId: this.runId,
            settled: false,
            outcome: RunOutcome.RUN_OUTCOME_UNSPECIFIED,
        });
        this.send({
            paneId: this.opts.creds.pane,
            processId: this.opts.creds.process,
            runtimeGeneration: this.opts.generation,
            payload: {
                $case: 'agentStarted',
                value: AgentStarted.create({
                    eventSequence: sequence,
                    runId: this.runId,
                }),
            },
        });
        this.sendSnapshot(LogicalState.LOGICAL_STATE_RUNNING);
    }

    /**
     * Map one agent_settled edge. A settlement with no observed run is
     * ignored: unobserved work must not ring the bell. The outcome comes
     * from this event only; an old outcome is never reused.
     */
    onAgentSettled(ctx: PiContextView, aborted: boolean): void {
        if (this.shutdownRequested || !this.running) return;
        this.noteView(ctx);
        const sequence = this.nextSeq();
        const outcome = aborted
            ? RunOutcome.RUN_OUTCOME_ABORTED
            : RunOutcome.RUN_OUTCOME_COMPLETED;
        this.appendJournal({
            sequence,
            runId: this.runId,
            settled: true,
            outcome,
        });
        this.running = false;
        this.send({
            paneId: this.opts.creds.pane,
            processId: this.opts.creds.process,
            runtimeGeneration: this.opts.generation,
            payload: {
                $case: 'agentSettled',
                value: AgentSettled.create({
                    eventSequence: sequence,
                    runId: this.runId,
                    outcome,
                }),
            },
        });
        this.sendSnapshot(LogicalState.LOGICAL_STATE_IDLE);
    }

    /** A model change invalidates in-flight summaries and the snapshot. */
    onModelSelect(ctx: PiContextView): void {
        if (this.shutdownRequested) return;
        this.summaries.cancelActive();
        this.noteView(ctx);
        this.sendSnapshot(
            this.running
                ? LogicalState.LOGICAL_STATE_RUNNING
                : LogicalState.LOGICAL_STATE_IDLE,
        );
    }

    /** Tree navigation invalidates in-flight summaries and the snapshot. */
    onSessionTree(ctx: PiContextView): void {
        if (this.shutdownRequested) return;
        this.summaries.cancelActive();
        this.noteView(ctx);
        this.sendSnapshot(
            this.running
                ? LogicalState.LOGICAL_STATE_RUNNING
                : LogicalState.LOGICAL_STATE_IDLE,
        );
    }

    /** Compaction starts: no summary may run against a moving session. */
    onCompactStart(ctx: PiContextView): void {
        if (this.shutdownRequested) return;
        this.compacting = true;
        this.summaries.cancelActive();
        this.noteView(ctx);
    }

    /** Compaction ends: publish the post-compaction snapshot. */
    onCompactEnd(ctx: PiContextView): void {
        if (this.shutdownRequested) return;
        this.compacting = false;
        this.noteView(ctx);
        this.sendSnapshot(
            this.running
                ? LogicalState.LOGICAL_STATE_RUNNING
                : LogicalState.LOGICAL_STATE_IDLE,
        );
    }

    /** Idempotent shutdown: stop retrying and release the stream. */
    shutdown(): void {
        if (this.shutdownRequested) return;
        this.shutdownRequested = true;
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = null;
        }
        this.tracker.resetRuntime();
        this.channel?.close();
        this.channel = null;
        this.starting = false;
    }

    get isRunning(): boolean {
        return this.running;
    }

    get lastSequence(): bigint {
        return this.seq;
    }

    /**
     * Cache the latest live view and refresh event-time identities.
     * The summary capability is fixed at the first view: it needs
     * injected helpers plus a host surface that exposes summaries.
     */
    private noteView(ctx: PiContextView): void {
        this.lastView = ctx;
        if (this.opts.helpers != null && hasSummarySurface(ctx)) {
            this.summaryAvailable = true;
        }
        this.refreshIdentities(ctx);
    }

    private refreshIdentities(ctx: PiContextView): void {
        try {
            this.sessionId = ctx.sessionManager.getSessionId();
        } catch {
            // Keep the last known identity; never fail an event mapping.
        }
        try {
            this.leafId = ctx.sessionManager.getLeafId() ?? '';
        } catch {
            // Keep the last known leaf; never fail an event mapping.
        }
        this.model = ctx.model;
    }

    private nextSeq(): bigint {
        this.seq += 1n;
        return this.seq;
    }

    private appendJournal(row: JournalRow): void {
        this.journal.push(row);
        if (this.journal.length > MAX_JOURNAL_RECORDS) {
            this.journal.splice(0, this.journal.length - MAX_JOURNAL_RECORDS);
            this.journalGap = true;
        }
    }

    private snapshot(state: LogicalState): StateSnapshot {
        this.revision += 1n;
        return StateSnapshot.create({
            state,
            sessionId: this.sessionId,
            leafId: this.leafId,
            model: this.model
                ? ModelSnapshot.create({
                      provider: this.model.provider,
                      model: this.model.id,
                  })
                : undefined,
            journal: this.journal.map((row) => ({
                eventSequence: row.sequence,
                runId: row.runId,
                settled: row.settled,
                outcome: row.outcome,
            })),
            journalGap: this.journalGap,
            stateRevision: this.revision,
            outstandingSummaries: [],
        });
    }

    private hello(): PiToPhi {
        return {
            paneId: this.opts.creds.pane,
            processId: this.opts.creds.process,
            runtimeGeneration: this.opts.generation,
            payload: {
                $case: 'hello',
                value: Hello.create({
                    protocolVersion: PROTOCOL_VERSION,
                    packageVersion: '0.1.0',
                    hostVersion: '',
                    sessionId: this.sessionId,
                    runtimeId: this.runtimeId,
                    capabilities: this.summaryAvailable
                        ? [CAPABILITY_LIFECYCLE, CAPABILITY_SUMMARY]
                        : [CAPABILITY_LIFECYCLE],
                    currentState: this.snapshot(
                        LogicalState.LOGICAL_STATE_IDLE,
                    ),
                    journalFirstSequence:
                        this.journal.length > 0
                            ? this.journal[0]!.sequence
                            : 0n,
                    journalLastSequence: this.seq,
                }),
            },
        };
    }

    private send(msg: PiToPhi): void {
        if (!this.channel) return;
        if (!this.channel.send(msg)) {
            // Queue overflow is fatal for this attempt; reconnect for a
            // fresh baseline rather than emit a gapped event stream.
            this.channel.close();
            this.channel = null;
            this.scheduleRetry();
        }
    }

    private sendSnapshot(state: LogicalState): void {
        this.send({
            paneId: this.opts.creds.pane,
            processId: this.opts.creds.process,
            runtimeGeneration: this.opts.generation,
            payload: { $case: 'stateSnapshot', value: this.snapshot(state) },
        });
    }

    private async attempt(): Promise<void> {
        if (this.shutdownRequested) {
            this.starting = false;
            return;
        }
        const make =
            this.opts.makeChannel ??
            ((creds, hooks) => new BridgeChannel(creds, hooks));
        const channel = make(this.opts.creds, {
            onMessage: (msg) => this.onServerMessage(msg),
            onStatus: (status) => this.onStreamEnd(status),
        });
        this.channel = channel;
        try {
            await channel.connect(this.hello());
        } catch {
            this.channel = null;
            if (!this.shutdownRequested) this.scheduleRetry();
            return;
        } finally {
            this.starting = false;
        }
        // Accepted resets the backoff for the next transport failure.
        this.backoffMs = this.opts.backoffBaseMs ?? 250;
    }

    private onServerMessage(msg: import('./gen/bridge.js').PhiToPi): void {
        const payload = msg.payload;
        if (payload?.$case === 'summarize') {
            // Never throws: the adapter ends every command with a frame.
            void this.summaries.handleSummarize(
                payload.value,
                this.opts.helpers ?? null,
            );
            return;
        }
        if (payload?.$case === 'cancel') {
            this.summaries.handleCancel(payload.value.requestId);
            return;
        }
        // Protocol errors are informational. Terminal failures arrive as
        // the stream status, not messages.
    }

    private onStreamEnd(status: ServiceError | null): void {
        this.channel?.close();
        this.channel = null;
        // A disconnect fails outstanding requests; the reconnect starts
        // with no survivors, so no paid work is replayed.
        this.tracker.resetRuntime();
        if (this.shutdownRequested) return;
        if (status && TERMINAL_STATUS_CODES.has(status.code)) return;
        this.scheduleRetry();
    }

    private scheduleRetry(): void {
        if (this.shutdownRequested || this.retryTimer || this.channel) return;
        const max = this.opts.backoffMaxMs ?? 10_000;
        const delay = Math.min(this.backoffMs, max);
        this.backoffMs = Math.min(this.backoffMs * 2, max);
        this.retryTimer = setTimeout(() => {
            this.retryTimer = null;
            if (!this.shutdownRequested) void this.attempt();
        }, delay);
    }
}

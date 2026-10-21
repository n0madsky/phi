// M1 Go ↔ TypeScript interop over native Unix IPC. Spawns the Go
// testserver, drives 64 independent synthetic runtimes, and asserts
// auth, correlation, cancellation, concurrency, slow-peer, and endpoint
// behavior with explicit gRPC codes. No mocks, no TCP, no fake timers.

import { spawn } from 'node:child_process';
import { statSync } from 'node:fs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { status as GrpcStatus } from '@grpc/grpc-js';
import {
    CommandErrorCode,
    LogicalState,
    RunOutcome,
    StateSnapshot,
    SummaryTermination,
} from '../src/gen/bridge.js';
import { BridgeTestClient } from '../src/test-client.js';

interface ControlResponse {
    ok: boolean;
    error?: string;
    code?: number;
    process?: string;
    token?: string;
    requestId?: string;
    kind?: string;
    text?: string;
    safeMsg?: string;
    terminate?: number;
    sent?: number;
    active?: number;
    summaries?: number;
    leases?: number;
    hasPeer?: boolean;
    hasState?: boolean;
    state?: number;
    revision?: string;
    journalFirst?: string;
    journalLast?: string;
    journalGap?: boolean;
    staleDrops?: number;
    pending?: number;
}

let server: ReturnType<typeof spawn>;
let target = '';
let socketPath = '';
let socketDir = '';
let pendingLines: string[] = [];
let lineWaiters: Array<(line: string) => void> = [];

function onLine(line: string): void {
    const waiter = lineWaiters.shift();
    if (waiter) waiter(line);
    else pendingLines.push(line);
}

function readLine(timeoutMs = 10_000): Promise<string> {
    const queued = pendingLines.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('control read timeout')), timeoutMs);
        lineWaiters.push((line: string) => {
            clearTimeout(timer);
            resolve(line);
        });
    });
}

async function control(op: Record<string, unknown>): Promise<ControlResponse> {
    if (!server.stdin) throw new Error('testserver stdin unavailable');
    server.stdin.write(JSON.stringify(op) + '\n');
    const line = await readLine();
    return JSON.parse(line) as ControlResponse;
}

async function registerPane(pane: string): Promise<{ process: string; token: string }> {
    const res = await control({ op: 'register', pane });
    expect(res.ok, `register ${pane}: ${res.error}`).toBe(true);
    return { process: res.process!, token: res.token! };
}

async function makeClient(
    pane: string,
    opts: { token?: string; capabilities?: string[]; sessionId?: string; runtimeId?: string } = {},
): Promise<{ client: BridgeTestClient; process: string }> {
    const { process, token } = await registerPane(pane);
    await control({ op: 'complete', process });
    const client = new BridgeTestClient({
        target,
        pane,
        process,
        token: opts.token ?? token,
        capabilities: opts.capabilities,
        sessionId: opts.sessionId ?? `sess-${pane}`,
        runtimeId: opts.runtimeId ?? `rt-${pane}`,
        state: StateSnapshot.create({
            state: LogicalState.LOGICAL_STATE_IDLE,
            sessionId: opts.sessionId ?? `sess-${pane}`,
            leafId: `leaf-${pane}`,
            stateRevision: 0n,
        }),
    });
    return { client, process };
}

beforeAll(async () => {
    const bin = process.env.PI_PHI_TESTSERVER;
    expect(bin, 'PI_PHI_TESTSERVER must point at the testserver binary').toBeTruthy();
    server = spawn(bin!, { stdio: ['pipe', 'pipe', 'inherit'] });
    if (!server.stdin || !server.stdout) throw new Error('testserver stdio unavailable');
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
    const readyLine = await readLine(15_000);
    const ready = JSON.parse(readyLine) as { ready: boolean; target: string };
    expect(ready.ready).toBe(true);
    target = ready.target;
    expect(target.startsWith('unix:/')).toBe(true);
    expect(target).not.toContain('://');
    socketPath = target.slice('unix:'.length);
    socketDir = socketPath.slice(0, socketPath.lastIndexOf('/'));
}, 30_000);

afterAll(async () => {
    if (server && server.exitCode === null) {
        try {
            await control({ op: 'shutdown' });
        } catch {
            server.kill();
        }
        await new Promise(resolve => server.once('exit', resolve));
    }
    // Socket and directory must be gone after shutdown.
    if (socketPath) {
        expect(() => statSync(socketPath)).toThrow();
        expect(() => statSync(socketDir)).toThrow();
    }
});

describe('endpoint', () => {
    it('uses a private socket with strict modes', () => {
        const sock = statSync(socketPath);
        expect(sock.mode & 0o777).toBe(0o600);
        const dir = statSync(socketDir);
        expect(dir.mode & 0o777).toBe(0o700);
        expect(socketPath.length).toBeLessThanOrEqual(100);
    });
    it('ignores proxy env for local IPC', async () => {
        process.env.https_proxy = 'http://127.0.0.1:9';
        process.env.http_proxy = 'http://127.0.0.1:9';
        try {
            const { client } = await makeClient('pane-proxy');
            const accepted = await client.connect();
            expect(accepted.protocolVersion).toBe(1);
            await client.close();
        } finally {
            delete process.env.https_proxy;
            delete process.env.http_proxy;
        }
    }, 20_000);
});

describe('auth and protocol', () => {
    it('rejects a wrong token', async () => {
        const { process } = await registerPane('pane-bad-token');
        await control({ op: 'complete', process });
        const client = new BridgeTestClient({
            target,
            pane: 'pane-bad-token',
            process,
            token: '0'.repeat(64),
        });
        await expect(client.connect()).rejects.toMatchObject({ code: GrpcStatus.UNAUTHENTICATED });
        await client.close();
    }, 20_000);
    it('rejects a pane mismatch', async () => {
        const { process, token } = await registerPane('pane-real');
        await control({ op: 'complete', process });
        const client = new BridgeTestClient({
            target,
            pane: 'pane-other',
            process,
            token,
        });
        await expect(client.connect()).rejects.toMatchObject({ code: GrpcStatus.PERMISSION_DENIED });
        await client.close();
    }, 20_000);
    it('rejects an unsupported version', async () => {
        const { client } = await makeClient('pane-badver');
        client.withHelloOverride({ protocolVersion: 999 });
        await expect(client.connect()).rejects.toMatchObject({ code: GrpcStatus.FAILED_PRECONDITION });
        await client.close();
    }, 20_000);
});

describe('lifecycle isolation', () => {
    it('runs 64 independent channels with 2048 events', async () => {
        const N = 64;
        const PER = 32;
        const clients: BridgeTestClient[] = [];
        const processes: string[] = [];
        for (let i = 0; i < N; i++) {
            const pane = `pane-iso-${i}`;
            const { client, process } = await makeClient(pane);
            clients.push(client);
            processes.push(process);
        }
        await Promise.all(clients.map(c => c.connect()));
        for (let i = 0; i < N; i++) {
            for (let seq = 1; seq <= PER; seq++) {
                const run = BigInt(seq);
                if (seq % 2 === 1) clients[i]!.sendStarted(BigInt(seq), run);
                else clients[i]!.sendSettled(BigInt(seq), run - 1n, RunOutcome.RUN_OUTCOME_COMPLETED);
                if (seq % 8 === 0) await clients[i]!.drain();
            }
        }
        await Promise.all(clients.map(c => c.drain()));
        // Allow the server to drain all streams.
        await new Promise(r => setTimeout(r, 1500));
        for (let i = 0; i < N; i++) {
            const view = await control({ op: 'inspect', process: processes[i] });
            expect(view.ok).toBe(true);
            expect(view.journalLast).toBe(String(PER));
            expect(view.staleDrops).toBe(0);
        }
        const stats = await control({ op: 'stats' });
        expect(stats.active).toBeGreaterThanOrEqual(N);
        await Promise.all(clients.map(c => c.close()));
    }, 120_000);
});

describe('summary correlation', () => {
    it('round-trips unicode and large revisions', async () => {
        const { client, process } = await makeClient('pane-sum-ok');
        client.onSummary = async () => ({ text: 'Synthetic summary ↔ünicode—done ✓' });
        await client.connect();
        const sum = await control({
            op: 'summarize',
            process,
            session: 'sess-pane-sum-ok',
            leaf: 'leaf-pane-sum-ok',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(sum.ok, sum.error).toBe(true);
        const out = await control({ op: 'await', process, requestId: sum.requestId, timeoutMs: 10_000 });
        expect(out.ok).toBe(true);
        expect(out.kind).toBe('summary');
        expect(out.text).toBe('Synthetic summary ↔ünicode—done ✓');
        await client.close();
    }, 30_000);
    it('delivers command errors without closing the stream', async () => {
        const { client, process } = await makeClient('pane-sum-err');
        client.onSummary = async () => ({
            error: { code: CommandErrorCode.COMMAND_ERROR_BUSY, safeMessage: 'busy' },
        });
        await client.connect();
        const sum = await control({
            op: 'summarize',
            process,
            session: 'sess-pane-sum-err',
            leaf: 'leaf-pane-sum-err',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(sum.ok).toBe(true);
        const out = await control({ op: 'await', process, requestId: sum.requestId, timeoutMs: 10_000 });
        expect(out.kind).toBe('error');
        expect(out.code).toBe(CommandErrorCode.COMMAND_ERROR_BUSY);
        // Stream stays healthy for a second command.
        client.onSummary = async () => ({ text: 'second' });
        const sum2 = await control({
            op: 'summarize',
            process,
            session: 'sess-pane-sum-err',
            leaf: 'leaf-pane-sum-err',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(sum2.ok).toBe(true);
        const out2 = await control({ op: 'await', process, requestId: sum2.requestId, timeoutMs: 10_000 });
        expect(out2.kind).toBe('summary');
        await client.close();
    }, 30_000);
    it('rejects stale duplicates without provider work', async () => {
        const { client } = await makeClient('pane-sum-dup');
        let calls = 0;
        client.onSummary = async () => {
            calls += 1;
            return { text: 'once' };
        };
        await client.connect();
        expect(client.tracker.noteSummarize(41n)).toBe('accept');
        expect(client.tracker.noteSummarize(41n)).toBe('duplicate');
        client.tracker.complete(41n);
        expect(client.tracker.noteSummarize(41n)).toBe('duplicate');
        expect(calls).toBe(0);
        await client.close();
    }, 20_000);
});

describe('concurrency', () => {
    it('enforces one per pane and two server-wide', async () => {
        const a = await makeClient('pane-conc-a');
        const b = await makeClient('pane-conc-b');
        const c = await makeClient('pane-conc-c');
        // Never-resolving handlers hold the admission slots.
        const hold = () => new Promise<{ text?: string }>(() => {});
        a.client.onSummary = hold;
        b.client.onSummary = hold;
        c.client.onSummary = hold;
        await Promise.all([a.client.connect(), b.client.connect(), c.client.connect()]);
        const first = await control({
            op: 'summarize',
            process: a.process,
            session: 'sess-pane-conc-a',
            leaf: 'leaf-pane-conc-a',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(first.ok).toBe(true);
        const secondSamePane = await control({
            op: 'summarize',
            process: a.process,
            session: 'sess-pane-conc-a',
            leaf: 'leaf-pane-conc-a',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 2000,
        });
        expect(secondSamePane.ok).toBe(false);
        expect(secondSamePane.code).toBe(GrpcStatus.RESOURCE_EXHAUSTED);
        const secondPane = await control({
            op: 'summarize',
            process: b.process,
            session: 'sess-pane-conc-b',
            leaf: 'leaf-pane-conc-b',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(secondPane.ok).toBe(true);
        const thirdServer = await control({
            op: 'summarize',
            process: c.process,
            session: 'sess-pane-conc-c',
            leaf: 'leaf-pane-conc-c',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 2000,
        });
        expect(thirdServer.ok).toBe(false);
        expect(thirdServer.code).toBe(GrpcStatus.RESOURCE_EXHAUSTED);
        await control({ op: 'revoke', process: a.process });
        await control({ op: 'revoke', process: b.process });
        await control({ op: 'revoke', process: c.process });
        await Promise.all([a.client.close(), b.client.close(), c.client.close()]);
    }, 30_000);
});

describe('cancellation', () => {
    it('is idempotent and frees only on terminal evidence', async () => {
        const { client, process } = await makeClient('pane-cancel');
        let cancels = 0;
        client.onCancel = () => {
            cancels += 1;
        };
        // Hold the slot until the test acks.
        client.onSummary = () => new Promise(() => {});
        await client.connect();
        const unknown = await control({ op: 'cancel', process, requestId: '999', reason: 1 });
        expect(unknown.ok).toBe(true);
        const sum = await control({
            op: 'summarize',
            process,
            session: 'sess-pane-cancel',
            leaf: 'leaf-pane-cancel',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(sum.ok).toBe(true);
        const id = BigInt(sum.requestId!);
        await control({ op: 'cancel', process, requestId: sum.requestId, reason: 1 });
        await control({ op: 'cancel', process, requestId: sum.requestId, reason: 1 });
        await new Promise(r => setTimeout(r, 300));
        expect(cancels).toBeGreaterThanOrEqual(1);
        const before = await control({ op: 'inspect', process });
        expect(before.pending).toBe(1);
        client.sendCancelAck(id, SummaryTermination.SUMMARY_TERMINATION_ENDED);
        const out = await control({ op: 'await', process, requestId: sum.requestId, timeoutMs: 10_000 });
        expect(out.kind).toBe('cancelAck');
        const after = await control({ op: 'inspect', process });
        expect(after.pending).toBe(0);
        await client.close();
    }, 30_000);
});

describe('message bounds', () => {
    it('rejects overlong summary text with InvalidArgument', async () => {
        const { client, process } = await makeClient('pane-big-text');
        client.onSummary = () => new Promise(() => {});
        await client.connect();
        const sum = await control({
            op: 'summarize',
            process,
            session: 'sess-pane-big-text',
            leaf: 'leaf-pane-big-text',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(sum.ok).toBe(true);
        client.sendSummaryResult(BigInt(sum.requestId!), 'sess-pane-big-text', 'leaf-pane-big-text', 'x'.repeat(129 * 1024), 0n);
        const status = await client.awaitStatus();
        expect(status?.code).toBe(GrpcStatus.INVALID_ARGUMENT);
        await client.close();
    }, 30_000);
    it('rejects overlong safe messages with InvalidArgument', async () => {
        const { client, process } = await makeClient('pane-big-safe');
        client.onSummary = () => new Promise(() => {});
        await client.connect();
        const sum = await control({
            op: 'summarize',
            process,
            session: 'sess-pane-big-safe',
            leaf: 'leaf-pane-big-safe',
            provider: 'p',
            model: 'm',
            revision: '0',
            deadlineMs: 10_000,
        });
        expect(sum.ok).toBe(true);
        client.sendCommandError(BigInt(sum.requestId!), CommandErrorCode.COMMAND_ERROR_INTERNAL, 'e'.repeat(3 * 1024));
        const status = await client.awaitStatus();
        expect(status?.code).toBe(GrpcStatus.INVALID_ARGUMENT);
        await client.close();
    }, 30_000);
});

describe('slow peer', () => {
    it('bounds one slow reader while others continue', async () => {
        const slow = await makeClient('pane-slow');
        const healthy = await makeClient('pane-healthy');
        await slow.client.connect();
        await healthy.client.connect();
        slow.client.pause();
        const flood = await control({ op: 'flood', process: slow.process, count: 32 });
        expect(flood.ok).toBe(true);
        const status = await slow.client.awaitStatus();
        expect(status?.code).toBe(GrpcStatus.RESOURCE_EXHAUSTED);
        // Healthy peer continues lifecycle traffic.
        for (let seq = 1; seq <= 4; seq++) {
            healthy.client.sendStarted(BigInt(seq), BigInt(seq));
        }
        await new Promise(r => setTimeout(r, 500));
        const view = await control({ op: 'inspect', process: healthy.process });
        expect(view.journalLast).toBe('4');
        await slow.client.close();
        await healthy.client.close();
    }, 30_000);
});

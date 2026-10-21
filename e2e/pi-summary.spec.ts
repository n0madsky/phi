import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { expect, test, type APIRequestContext } from '@playwright/test';
import { startPhi } from './_server.js';

// Summary-capable synthetic Pi backend: loads the REAL bridge bundle,
// injects the REAL host summary helpers, and answers model calls with a
// scripted stream. Stdin lines:
//   start   -> agent_start (busy)
//   settle  -> agent_settled (completed, idle)
//   exit    -> session_shutdown and process exit
// The scripted summary embeds a link and an image to prove restricted
// rendering; only the Pi host surface and the model stream are faked.
function syntheticSummarySource(bundlePath: string, piPath: string): string {
    return `#!/usr/bin/env node
const factory = require(${JSON.stringify(bundlePath)});
const { SessionManager, buildSessionContext, generateSummaryWithUsage } = require(${JSON.stringify(piPath)});
const handlers = new Map();
let running = false;
const mockPi = {
    on(event, handler) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event).push(handler);
        return () => {};
    },
};
const session = SessionManager.inMemory(require('os').tmpdir());
session.appendMessage({ role: 'user', content: 'Synthetic goal: verify summaries.', timestamp: 1 });
session.appendMessage({ role: 'user', content: 'Synthetic progress: backend online.', timestamp: 2 });
const SUMMARY_TEXT = '# Session summary\\n\\nAll good. [details](http://evil.example) ![chart](http://evil.example/t.png)';
function scriptedStream() {
    const deltas = ['# Session', ' summary\\n\\nAll ', 'good. [details](http://evil.example) ![chart](http://evil.example/t.png)'];
    let i = 0;
    return {
        [Symbol.asyncIterator]() {
            return {
                next: async () =>
                    i < deltas.length
                        ? { value: { type: 'text_delta', contentIndex: 0, delta: deltas[i++], partial: {} }, done: false }
                        : { value: undefined, done: true },
            };
        },
        result: async () => ({
            role: 'assistant',
            content: [{ type: 'text', text: SUMMARY_TEXT }],
            api: 'e2e', provider: 'e2e-provider', model: 'e2e-model',
            usage: { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15,
                cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.001 } },
            stopReason: 'stop', timestamp: Date.now(),
        }),
    };
}
const mockCtx = () => ({
    sessionManager: {
        getSessionId: () => 'e2e-summary-session',
        getLeafId: () => 'e2e-leaf-1',
        getBranch: () => session.getBranch(),
    },
    model: { provider: 'e2e-provider', id: 'e2e-model', maxTokens: 200000, reasoning: false },
    modelRegistry: { streamSimple: () => scriptedStream() },
    isIdle: () => !running,
    hasPendingMessages: () => false,
    getContextUsage: () => ({ tokens: 100, contextWindow: 200000 }),
    thinkingLevel: undefined,
});
const emit = (event, payload) => {
    for (const handler of handlers.get(event) || []) handler(payload, mockCtx());
};
factory.withHelpers({ buildSessionContext, generateSummaryWithUsage })(mockPi);
emit('session_start', { type: 'session_start', reason: 'startup' });
console.log('synthetic pi ready');
let buffer = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
    buffer += chunk;
    let idx;
    while ((idx = buffer.search(/[\\r\\n]/)) >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line) continue;
        if (line === 'start') {
            running = true;
            emit('agent_start', { type: 'agent_start' });
            console.log('run started');
        } else if (line === 'settle') {
            running = false;
            emit('agent_settled', { type: 'agent_settled', aborted: false });
            console.log('run settled');
        } else if (line === 'exit') {
            emit('session_shutdown', { type: 'session_shutdown' });
            process.exit(0);
        }
    }
});
process.stdin.on('end', () => process.exit(0));
`;
}

interface BridgeView {
    state: string;
    running?: boolean;
    model?: string;
    capabilities?: string[];
}

async function paneBridge(
    request: APIRequestContext,
    url: string,
    paneID: string,
): Promise<BridgeView | undefined> {
    const response = await request.get(`${url}/api/terminals`);
    if (!response.ok()) return undefined;
    const tabs = (await response.json()) as Array<{
        id: string;
        pi_bridge?: BridgeView;
    }>;
    return tabs.find((tab) => tab.id === paneID)?.pi_bridge;
}

async function paneInput(
    request: APIRequestContext,
    url: string,
    paneID: string,
    text: string,
) {
    const response = await request.post(
        `${url}/api/terminals/${paneID}/input`,
        { data: { text } },
    );
    expect(response.ok()).toBe(true);
}

test('Pi summary consents, generates, and renders restricted', async ({
    page,
    request,
}) => {
    test.setTimeout(180_000);
    const bundlePath = join(
        process.cwd(),
        'bonus',
        'pi-phi',
        'dist',
        'pi-phi-extension.cjs',
    );
    expect(existsSync(bundlePath)).toBe(true);
    const piPath = join(
        process.cwd(),
        'node_modules',
        '@earendil-works',
        'pi-coding-agent',
    );
    expect(existsSync(piPath)).toBe(true);

    const phi = await startPhi({
        setup(dir) {
            const cwd = join(dir, 'work dir with spaces');
            mkdirSync(cwd, { recursive: true });
            const home = join(dir, 'home');
            const backends = join(home, '.phi', 'backends');
            mkdirSync(backends, { recursive: true });
            const synthetic = join(dir, 'synthetic-pi.cjs');
            writeFileSync(
                synthetic,
                syntheticSummarySource(bundlePath, piPath),
                { mode: 0o755 },
            );
            writeFileSync(
                join(backends, 'pi.json'),
                JSON.stringify({ id: 'pi', command: synthetic }),
            );
        },
    });

    try {
        const cwd = join(phi.dir, 'work dir with spaces');
        await page.setViewportSize({ width: 1440, height: 900 });

        const spawned = await request.post(`${phi.url}/api/terminals`, {
            data: { coder: 'pi', cwd, title: 'E2E Summary' },
        });
        expect(spawned.ok()).toBe(true);
        const pane = (await spawned.json()) as { pane_id: string };

        // The summary capability arrives with the hello handshake.
        await expect
            .poll(() => paneBridge(request, phi.url, pane.pane_id), {
                timeout: 15_000,
            })
            .toMatchObject({
                state: 'managed',
                running: false,
                model: 'e2e-provider/e2e-model',
                capabilities: ['pi.lifecycle.v1', 'pi.summary.v1'],
            });

        await page.goto(phi.url);
        const piTab = page.locator(`.tab[data-pane-id="${pane.pane_id}"]`);
        await expect(piTab).toBeVisible({ timeout: 10_000 });
        await piTab.click();

        const summarize = page.locator('.pi-summary-btn');
        await expect(summarize).toBeVisible({ timeout: 10_000 });
        await expect(summarize).toBeEnabled({ timeout: 10_000 });

        // A running agent disables the control with a busy reason.
        await paneInput(request, phi.url, pane.pane_id, 'start\n');
        await expect(summarize).toBeDisabled({ timeout: 10_000 });
        await expect(summarize).toHaveAttribute('title', 'Agent is busy');

        // Settlement re-enables it via the state push.
        await paneInput(request, phi.url, pane.pane_id, 'settle\n');
        await expect(summarize).toBeEnabled({ timeout: 10_000 });

        // Consent names the provider model and warns about cost.
        let dialogMessage = '';
        page.on('dialog', async (dialog) => {
            dialogMessage = dialog.message();
            await dialog.accept();
        });
        await summarize.click();
        expect(dialogMessage).toContain('e2e-provider/e2e-model');
        expect(dialogMessage).toContain('cost');

        // The real helper plus the scripted stream produce the modal.
        const modal = page.locator('#md-modal');
        await expect(modal).toBeVisible({ timeout: 30_000 });
        await expect(page.locator('#md-modal-title')).toContainText(
            'E2E Summary',
        );
        const body = page.locator('#md-modal-body');
        await expect(body).toContainText('All good.');
        // Restricted: link and image URLs never reach the DOM.
        await expect(body.locator('a')).toHaveCount(0);
        await expect(body.locator('img')).toHaveCount(0);
        const html = await body.innerHTML();
        expect(html).not.toContain('evil.example');

        await paneInput(request, phi.url, pane.pane_id, 'exit\n');
    } finally {
        await phi.stop();
    }
});

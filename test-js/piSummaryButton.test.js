// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { TabManager } from '../web/terminal.js';
import { stripActiveContent } from '../web/md-render.js';

function summaryManager(over = {}) {
    const showToast = vi.fn();
    const openRawMarkdown = vi.fn();
    const manager = {
        app: { showToast, markdownManager: { openRawMarkdown } },
        tabs: new Map(),
        activePaneId: 'pane-1',
        getActiveTab() {
            return this.tabs.get(this.activePaneId);
        },
        renderPresets: vi.fn(),
        updateDocumentTitle: vi.fn(),
        _piSummaryButton: TabManager.prototype._piSummaryButton,
        _requestPiSummary: TabManager.prototype._requestPiSummary,
        _onPiSummaryResult: TabManager.prototype._onPiSummaryResult,
        _piSummaryError: TabManager.prototype._piSummaryError,
        _acceptPiBridgeState: TabManager.prototype._acceptPiBridgeState,
        _acceptPiBridgeSettled: TabManager.prototype._acceptPiBridgeSettled,
        _piBridgeRunIsNew: TabManager.prototype._piBridgeRunIsNew,
        _piBridgeHasBaseline: TabManager.prototype._piBridgeHasBaseline,
        ...over,
    };
    return { manager, showToast, openRawMarkdown };
}

function piTab(over = {}) {
    return {
        paneId: 'pane-1',
        title: 'pi session',
        coder: 'pi',
        userTaskActive: false,
        piBridge: {
            state: 'managed',
            running: false,
            model: 'p/m',
            capabilities: ['pi.lifecycle.v1', 'pi.summary.v1'],
        },
        ...over,
    };
}

function jsonResponse(body, status = 200) {
    return Promise.resolve({
        ok: status >= 200 && status < 300,
        status,
        json: () => Promise.resolve(body),
    });
}

beforeEach(() => {
    vi.stubGlobal('confirm', () => true);
});

afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
});

describe('pi summary button states', () => {
    it('enables Summarize for a managed idle pane with the capability', () => {
        const { manager } = summaryManager();
        const button = manager._piSummaryButton(piTab());
        expect(button.textContent).toBe('Summarize');
        expect(button.disabled).toBe(false);
        expect(button.title).toContain('p/m');
    });

    it('disables while the agent is busy', () => {
        const { manager } = summaryManager();
        const running = manager._piSummaryButton(
            piTab({
                piBridge: {
                    state: 'managed',
                    running: true,
                    capabilities: ['pi.summary.v1'],
                },
            }),
        );
        expect(running.disabled).toBe(true);
        expect(running.title).toBe('Agent is busy');
        const heuristic = manager._piSummaryButton(
            piTab({ userTaskActive: true }),
        );
        expect(heuristic.disabled).toBe(true);
        expect(heuristic.title).toBe('Agent is busy');
    });

    it('explains unavailable bridges and missing capabilities', () => {
        const { manager } = summaryManager();
        const unavailable = manager._piSummaryButton(
            piTab({ piBridge: { state: 'unavailable', reason: 'no runtime' } }),
        );
        expect(unavailable.disabled).toBe(true);
        expect(unavailable.title).toBe('no runtime');
        const plain = manager._piSummaryButton(piTab({ piBridge: undefined }));
        expect(plain.disabled).toBe(true);
        expect(plain.title).toContain('restart the tab');
        const nocaps = manager._piSummaryButton(
            piTab({
                piBridge: {
                    state: 'managed',
                    running: false,
                    capabilities: ['pi.lifecycle.v1'],
                },
            }),
        );
        expect(nocaps.disabled).toBe(true);
        expect(nocaps.title).toContain('not supported');
    });

    it('reuses the control to cancel a running summary', () => {
        const { manager } = summaryManager();
        const abort = vi.fn();
        manager._piSummaryPending = new Set(['pane-1']);
        manager._piSummaryAbort = new Map([['pane-1', { abort }]]);
        const button = manager._piSummaryButton(piTab());
        expect(button.textContent).toBe('Cancel summary');
        expect(button.disabled).toBe(false);
        button.click();
        expect(abort).toHaveBeenCalledTimes(1);
    });
});

describe('pi summary bridge state', () => {
    it('merges running, model, and capabilities', () => {
        const { manager } = summaryManager();
        const tab = piTab({ piBridge: undefined });
        manager._acceptPiBridgeState(tab, {
            type: 'pi-bridge-state',
            pane_id: 'pane-1',
            state: 'managed',
            running: 'true',
            model: 'q/n',
            capabilities: 'pi.lifecycle.v1,pi.summary.v1',
        });
        expect(tab.piBridge.running).toBe(true);
        expect(tab.piBridge.model).toBe('q/n');
        expect(tab.piBridge.capabilities).toEqual([
            'pi.lifecycle.v1',
            'pi.summary.v1',
        ]);
    });

    it('clears running on settlement', () => {
        const { manager } = summaryManager();
        const tab = piTab();
        tab.piBridge.running = true;
        // Aborted skips the bell but still ends the run.
        manager._acceptPiBridgeSettled(tab, {
            type: 'pi-bridge-settled',
            pane_id: 'pane-1',
            run: '3',
            outcome: 'aborted',
        });
        expect(tab.piBridge.running).toBe(false);
    });
});

describe('pi summary request flow', () => {
    it('confirms, posts, and shows the result restricted', async () => {
        const { manager, openRawMarkdown } = summaryManager();
        const tab = piTab();
        manager.tabs.set('pane-1', tab);
        const confirm = vi.fn(() => true);
        vi.stubGlobal('confirm', confirm);
        const fetch = vi.fn(() =>
            jsonResponse({ text: '# Done\n\nAll good.', request_id: '1' }),
        );
        vi.stubGlobal('fetch', fetch);

        manager._requestPiSummary(tab);
        expect(confirm).toHaveBeenCalledTimes(1);
        expect(confirm.mock.calls[0][0]).toContain('p/m');
        expect(confirm.mock.calls[0][0]).toContain('cost');
        await vi.waitFor(() =>
            expect(openRawMarkdown).toHaveBeenCalledTimes(1),
        );

        const [url, opts] = fetch.mock.calls[0];
        expect(url).toBe('/api/terminals/pane-1/pi/summary');
        expect(opts.method).toBe('POST');
        expect(JSON.parse(opts.body)).toEqual({ confirm: true });
        const [title, text, options] = openRawMarkdown.mock.calls[0];
        expect(title).toContain('pi session');
        expect(text).toContain('All good.');
        expect(options).toEqual({ restricted: true });
        expect(manager._piSummaryPending.has('pane-1')).toBe(false);
    });

    it('toasts with a View action when the pane is inactive', async () => {
        const { manager, showToast, openRawMarkdown } = summaryManager();
        const tab = piTab();
        manager.tabs.set('pane-1', tab);
        manager.activePaneId = 'other';
        vi.stubGlobal('fetch', () => jsonResponse({ text: 'late summary' }));

        manager._requestPiSummary(tab);
        await vi.waitFor(() => expect(showToast).toHaveBeenCalledTimes(1));
        expect(openRawMarkdown).not.toHaveBeenCalled();
        const [message, opts] = showToast.mock.calls[0];
        expect(message).toContain('Summary ready');
        expect(opts.action.text).toBe('View summary');
        opts.action.callback();
        expect(openRawMarkdown).toHaveBeenCalledTimes(1);
        expect(openRawMarkdown.mock.calls[0][2]).toEqual({ restricted: true });
    });

    it('aborts without an error toast on user cancel', async () => {
        const { manager, showToast } = summaryManager();
        const tab = piTab();
        manager.tabs.set('pane-1', tab);
        vi.stubGlobal(
            'fetch',
            (_url, opts) =>
                new Promise((_resolve, reject) => {
                    opts.signal.addEventListener('abort', () => {
                        const err = new Error('aborted');
                        err.name = 'AbortError';
                        reject(err);
                    });
                }),
        );
        manager._requestPiSummary(tab);
        await vi.waitFor(() =>
            expect(manager._piSummaryPending.has('pane-1')).toBe(true),
        );
        manager._piSummaryAbort.get('pane-1').abort();
        await vi.waitFor(() =>
            expect(manager._piSummaryPending.has('pane-1')).toBe(false),
        );
        expect(showToast).not.toHaveBeenCalled();
    });

    it('reports server failures once', async () => {
        const { manager, showToast } = summaryManager();
        const tab = piTab();
        manager.tabs.set('pane-1', tab);
        vi.stubGlobal('fetch', () =>
            jsonResponse({ error: 'busy', message: 'agent is busy' }, 409),
        );
        manager._requestPiSummary(tab);
        await vi.waitFor(() => expect(showToast).toHaveBeenCalledTimes(1));
        const [message, opts] = showToast.mock.calls[0];
        expect(message).toContain('agent is busy');
        expect(opts.type).toBe('error');
    });

    it('does nothing when consent is declined', () => {
        const { manager } = summaryManager();
        vi.stubGlobal('confirm', () => false);
        const fetch = vi.fn();
        vi.stubGlobal('fetch', fetch);
        manager._requestPiSummary(piTab());
        expect(fetch).not.toHaveBeenCalled();
        expect(manager._piSummaryPending).toBeUndefined();
    });
});

describe('restricted summary rendering', () => {
    it('keeps text but drops links, images, and media', () => {
        document.body.innerHTML =
            '<div id="r"><a href="http://evil.example">click</a><img src="http://evil.example/t.png" alt="chart"><video src="v.mp4"></video><p>kept</p></div>';
        const container = document.getElementById('r');
        stripActiveContent(container);
        expect(container.querySelector('a')).toBeNull();
        expect(container.querySelector('img')).toBeNull();
        expect(container.querySelector('video')).toBeNull();
        expect(container.textContent).toContain('click');
        expect(container.textContent).toContain('chart');
        expect(container.textContent).toContain('kept');
        expect(container.innerHTML).not.toContain('evil.example');
    });
});

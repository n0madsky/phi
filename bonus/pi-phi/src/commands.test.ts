import { describe, expect, it } from 'vitest';
import { CommandTracker } from './commands.js';

describe('CommandTracker', () => {
    it('accepts increasing IDs and rejects repeats', () => {
        const t = new CommandTracker();
        expect(t.noteSummarize(1n)).toBe('accept');
        expect(t.noteSummarize(1n)).toBe('duplicate');
        expect(t.noteSummarize(2n)).toBe('accept');
        expect(t.highWaterMark).toBe(2n);
    });
    it('rejects older and zero IDs', () => {
        const t = new CommandTracker();
        expect(t.noteSummarize(5n)).toBe('accept');
        expect(t.noteSummarize(4n)).toBe('duplicate');
        expect(t.noteSummarize(0n)).toBe('duplicate');
    });
    it('rejects completed IDs before provider work', () => {
        const t = new CommandTracker();
        expect(t.noteSummarize(7n)).toBe('accept');
        t.complete(7n);
        expect(t.noteSummarize(7n)).toBe('duplicate');
        expect(t.noteSummarize(8n)).toBe('accept');
    });
    it('keeps the high-water mark across runtime replacement', () => {
        const t = new CommandTracker();
        expect(t.noteSummarize(9n)).toBe('accept');
        t.resetRuntime();
        expect(t.pendingCount).toBe(0);
        expect(t.highWaterMark).toBe(9n);
        expect(t.noteSummarize(9n)).toBe('duplicate');
        expect(t.noteSummarize(10n)).toBe('accept');
    });
    it('cancel is idempotent and scoped to one request', () => {
        const t = new CommandTracker();
        expect(t.noteCancel(3n)).toBe(false);
        t.noteSummarize(3n);
        t.noteSummarize(4n);
        expect(t.noteCancel(3n)).toBe(true);
        expect(t.isCancelled(3n)).toBe(true);
        expect(t.isCancelled(4n)).toBe(false);
        expect(t.noteCancel(3n)).toBe(true);
        t.complete(3n);
        expect(t.noteCancel(3n)).toBe(false);
    });
});

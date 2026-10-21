// Extension-side command correlation. The high-water mark rejects repeated
// or older Summarize IDs, including completed requests, before provider
// work. It lives in the process-global owner slot across runtime
// replacement; a runtime reset clears pending work but never the mark.

export type SummarizeDisposition = 'accept' | 'duplicate';

export class CommandTracker {
    private highWater = 0n;
    private pending = new Map<bigint, { cancelled: boolean }>();

    /** Highest accepted Summarize ID so far. Zero when none accepted. */
    get highWaterMark(): bigint {
        return this.highWater;
    }

    get pendingCount(): number {
        return this.pending.size;
    }

    /**
     * Note one incoming Summarize ID. Returns 'duplicate' when the ID is
     * zero, repeated, or older than the high-water mark. Accepted IDs
     * advance the mark and enter pending until complete/cancel settles.
     */
    noteSummarize(requestId: bigint): SummarizeDisposition {
        if (requestId === 0n) return 'duplicate';
        if (requestId <= this.highWater) return 'duplicate';
        if (this.pending.has(requestId)) return 'duplicate';
        this.highWater = requestId;
        this.pending.set(requestId, { cancelled: false });
        return 'accept';
    }

    /** Complete one pending request. Unknown IDs are a no-op. */
    complete(requestId: bigint): void {
        this.pending.delete(requestId);
    }

    /**
     * Note one incoming Cancel. Idempotent: unknown or completed IDs are a
     * no-op success. Never starts work or affects another request. Returns
     * true when the ID names a pending request.
     */
    noteCancel(requestId: bigint): boolean {
        const entry = this.pending.get(requestId);
        if (entry === undefined) return false;
        entry.cancelled = true;
        return true;
    }

    /** True when the pending request was cancelled. False for unknown IDs. */
    isCancelled(requestId: bigint): boolean {
        return this.pending.get(requestId)?.cancelled ?? false;
    }

    /**
     * Clear pending work for a runtime replacement. The high-water mark is
     * retained so delayed duplicates cannot re-execute provider work.
     */
    resetRuntime(): void {
        this.pending.clear();
    }
}

// Extension-side command correlation. The high-water mark rejects repeated
// or older Summarize IDs, including completed requests, before provider
// work. It lives in the process-global owner slot across runtime
// replacement; a runtime reset clears pending work but never the mark.

export type SummarizeDisposition = 'accept' | 'duplicate';

/** Who requested cancellation: Phi over the bridge, or the Pi host side. */
export type CancelSource = 'phi' | 'host';

interface PendingEntry {
    cancelled: boolean;
    /** True once a bridge Cancel named this request, whatever ended it. */
    phiCancel: boolean;
    controller: AbortController | null;
}

export class CommandTracker {
    private highWater = 0n;
    private pending = new Map<bigint, PendingEntry>();

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
        this.pending.set(requestId, {
            cancelled: false,
            phiCancel: false,
            controller: null,
        });
        return 'accept';
    }

    /** Complete one pending request. Unknown IDs are a no-op. */
    complete(requestId: bigint): void {
        this.pending.delete(requestId);
    }

    /**
     * Register the abort controller for a pending request. When the
     * request was already cancelled, aborts immediately and returns
     * false. Unknown IDs return false without aborting.
     */
    setController(requestId: bigint, controller: AbortController): boolean {
        const entry = this.pending.get(requestId);
        if (entry === undefined) return false;
        if (entry.cancelled) {
            controller.abort();
            return false;
        }
        entry.controller = controller;
        return true;
    }

    /**
     * Note one incoming Cancel. Idempotent: unknown or completed IDs are a
     * no-op success. Never starts work or affects another request. Returns
     * true when the ID names a pending request.
     */
    noteCancel(requestId: bigint, source: CancelSource = 'phi'): boolean {
        const entry = this.pending.get(requestId);
        if (entry === undefined) return false;
        entry.cancelled = true;
        if (source === 'phi') entry.phiCancel = true;
        entry.controller?.abort();
        return true;
    }

    /** True when the pending request was cancelled. False for unknown IDs. */
    isCancelled(requestId: bigint): boolean {
        return this.pending.get(requestId)?.cancelled ?? false;
    }

    /**
     * True when a bridge Cancel named this pending request. The caller
     * owes CancelAcknowledged instead of a command error.
     */
    phiCancelled(requestId: bigint): boolean {
        return this.pending.get(requestId)?.phiCancel ?? false;
    }

    /**
     * Abort pending work for a runtime replacement or shutdown. The
     * high-water mark is retained so delayed duplicates cannot
     * re-execute provider work.
     */
    resetRuntime(): void {
        for (const entry of this.pending.values()) {
            entry.cancelled = true;
            entry.controller?.abort();
        }
        this.pending.clear();
    }
}

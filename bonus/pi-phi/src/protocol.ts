// Canonical bridge constants. Mirrors internal/pibridge/limits.go.
// Change only through a reviewed plan revision when acceptance changes.

export const PROTOCOL_VERSION = 1;

export const CAPABILITY_LIFECYCLE = 'pi.lifecycle.v1';
export const CAPABILITY_SUMMARY = 'pi.summary.v1';

export const MAX_HELLO_BYTES = 16 * 1024;
export const MAX_METADATA_BYTES = 8 * 1024;
export const MAX_MESSAGE_BYTES = 256 * 1024;
export const MAX_QUEUED_MESSAGES = 16;
export const MAX_QUEUED_BYTES = 256 * 1024;
export const BLOCKED_SEND_TIMEOUT_MS = 5_000;
export const HELLO_DEADLINE_MS = 5_000;
export const MAX_JOURNAL_RECORDS = 64;
export const MAX_SAFE_MESSAGE_BYTES = 2 * 1024;
export const MAX_SUMMARY_TEXT_BYTES = 128 * 1024;
export const SUMMARY_DEADLINE_MS = 120_000;
export const MAX_SUMMARY_PER_PANE = 1;
export const MAX_SUMMARY_SERVER = 2;

export const METADATA_TOKEN_KEY = 'phi-pi-token';
export const METADATA_PANE_KEY = 'phi-pi-pane';
export const METADATA_PROCESS_KEY = 'phi-pi-process';

const SUPPORTED = new Set([CAPABILITY_LIFECYCLE, CAPABILITY_SUMMARY]);

/** Intersect advertised names with the supported set. Unknown names are ignored. */
export function negotiateCapabilities(advertised: readonly string[]): string[] {
    const out: string[] = [];
    for (const c of advertised) {
        if (SUPPORTED.has(c) && !out.includes(c)) out.push(c);
    }
    return out;
}

/** True when name is a known bridge capability. */
export function isKnownCapability(name: string): boolean {
    return SUPPORTED.has(name);
}

/** Bound safe_message text. Returns an error message when overlong, else null. */
export function checkSafeMessage(text: string): string | null {
    if (Buffer.byteLength(text, 'utf8') > MAX_SAFE_MESSAGE_BYTES) {
        return `safe message exceeds ${MAX_SAFE_MESSAGE_BYTES} bytes`;
    }
    return null;
}

/** Zero is reserved for absent IDs; lifecycle and command IDs must be nonzero. */
export function requireNonzeroId(value: bigint, what: string): void {
    if (value === 0n) throw new Error(`${what} id must be nonzero`);
    if (value < 0n) throw new Error(`${what} id must be unsigned`);
}

/** Format a uint64 ID as decimal for JSON control surfaces. Never a JS number. */
export function idToString(id: bigint): string {
    return id.toString(10);
}

/** Parse a decimal uint64 ID. Rejects empty, negative, and out-of-range input. */
export function idFromString(text: string): bigint {
    if (!/^\d+$/.test(text)) throw new Error(`invalid id ${JSON.stringify(text)}`);
    const value = BigInt(text);
    if (value > 18446744073709551615n) throw new Error('id out of uint64 range');
    return value;
}

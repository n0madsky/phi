import { describe, expect, it } from 'vitest';
import {
    CAPABILITY_LIFECYCLE,
    CAPABILITY_SUMMARY,
    MAX_SAFE_MESSAGE_BYTES,
    checkSafeMessage,
    idFromString,
    idToString,
    isKnownCapability,
    negotiateCapabilities,
    requireNonzeroId,
} from './protocol.js';

describe('negotiateCapabilities', () => {
    it('accepts known and ignores unknown', () => {
        expect(negotiateCapabilities([CAPABILITY_LIFECYCLE, CAPABILITY_SUMMARY, 'future.v9'])).toEqual([
            CAPABILITY_LIFECYCLE,
            CAPABILITY_SUMMARY,
        ]);
    });
    it('dedupes and preserves order', () => {
        expect(negotiateCapabilities([CAPABILITY_SUMMARY, CAPABILITY_SUMMARY])).toEqual([CAPABILITY_SUMMARY]);
    });
    it('yields empty for empty', () => {
        expect(negotiateCapabilities([])).toEqual([]);
        expect(isKnownCapability('nope')).toBe(false);
        expect(isKnownCapability(CAPABILITY_LIFECYCLE)).toBe(true);
    });
});

describe('checkSafeMessage', () => {
    it('accepts short and rejects overlong', () => {
        expect(checkSafeMessage('ok')).toBeNull();
        expect(checkSafeMessage('e'.repeat(MAX_SAFE_MESSAGE_BYTES + 1))).toContain('exceeds');
    });
});

describe('ids', () => {
    it('rejects zero and negative', () => {
        expect(() => requireNonzeroId(0n, 'run')).toThrow();
        expect(() => requireNonzeroId(-1n, 'run')).toThrow();
        requireNonzeroId(1n, 'run');
    });
    it('round-trips decimal strings beyond the safe-number range', () => {
        const big = 9007199254740993n;
        expect(idFromString(idToString(big))).toBe(big);
        expect(idFromString('18446744073709551615')).toBe(18446744073709551615n);
        expect(() => idFromString('')).toThrow();
        expect(() => idFromString('-1')).toThrow();
        expect(() => idFromString('18446744073709551616')).toThrow();
    });
});

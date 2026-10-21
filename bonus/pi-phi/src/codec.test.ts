import { describe, expect, it } from 'vitest';
import {
    LogicalState,
    PiToPhi,
    RunOutcome,
    SummaryResult,
} from './gen/bridge.js';

describe('generated codec', () => {
    it('round-trips uint64 beyond the safe-number range', () => {
        const big = 18446744073709551615n;
        const overSafe = 9007199254740993n;
        const msg = PiToPhi.create({
            paneId: 'pane',
            processId: 'proc',
            runtimeGeneration: big,
            payload: { $case: 'agentStarted', value: { eventSequence: overSafe, runId: big } },
        });
        const back = PiToPhi.decode(PiToPhi.encode(msg).finish());
        expect(back.runtimeGeneration).toBe(big);
        expect(back.payload?.$case).toBe('agentStarted');
        if (back.payload?.$case === 'agentStarted') {
            expect(back.payload.value.eventSequence).toBe(overSafe);
            expect(back.payload.value.runId).toBe(big);
        }
    });
    it('round-trips unicode summary text', () => {
        const text = 'Synthetic summary ↔ünicode—done ✓\nline2\ttab';
        const msg = PiToPhi.create({
            paneId: 'pane',
            processId: 'proc',
            runtimeGeneration: 1n,
            payload: {
                $case: 'summaryResult',
                value: SummaryResult.create({
                    requestId: 7n,
                    sessionId: 'sess',
                    leafId: 'leaf',
                    text,
                    stateRevision: 3n,
                }),
            },
        });
        const back = PiToPhi.decode(PiToPhi.encode(msg).finish());
        expect(back.payload?.$case).toBe('summaryResult');
        if (back.payload?.$case === 'summaryResult') {
            expect(back.payload.value.text).toBe(text);
        }
    });
    it('keeps lifecycle enums stable', () => {
        expect(RunOutcome.RUN_OUTCOME_COMPLETED).toBe(1);
        expect(LogicalState.LOGICAL_STATE_RUNNING).toBe(2);
    });
});

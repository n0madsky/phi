import { describe, expect, it } from 'vitest';
import { InvalidTargetError } from './endpoint.js';
import {
    BridgeChannel,
    type ChannelCreds,
    type ClientFactory,
} from './channel.js';
import { Hello, LogicalState, type PiToPhi } from './gen/bridge.js';

const CREDS: ChannelCreds = {
    target: 'unix:/tmp/phi-pi-abc/bridge.sock',
    pane: 'pane',
    process: 'proc',
    token: 'tok',
};

function hello(): PiToPhi {
    return {
        paneId: 'pane',
        processId: 'proc',
        runtimeGeneration: 1n,
        payload: {
            $case: 'hello',
            value: Hello.create({
                protocolVersion: 1,
                sessionId: 'sess',
                runtimeId: 'rt',
                capabilities: [],
                currentState: {
                    state: LogicalState.LOGICAL_STATE_IDLE,
                } as never,
                journalFirstSequence: 0n,
                journalLastSequence: 0n,
            }),
        },
    };
}

/** Client factory that records calls and returns an unusable stream. */
function countingFactory(): { factory: ClientFactory; calls: () => number } {
    let made = 0;
    const factory: ClientFactory = () => {
        made += 1;
        return {
            connect: () => {
                throw new Error('no stream in this test');
            },
            close: () => {},
        };
    };
    return { factory, calls: () => made };
}

describe('BridgeChannel target validation', () => {
    it('refuses to dial a target that is not a local socket', async () => {
        const { factory, calls } = countingFactory();
        for (const target of [
            'dns:///evil.example:443',
            'tcp://127.0.0.1:9000',
            'unix:relative/path.sock',
            'unix://authority/path.sock',
        ]) {
            const channel = new BridgeChannel(
                { ...CREDS, target },
                { onMessage: () => {}, onStatus: () => {} },
                factory,
            );
            await expect(channel.connect(hello())).rejects.toBeInstanceOf(
                InvalidTargetError,
            );
            channel.close();
        }
        expect(calls()).toBe(0);
    });

    it('dials a valid local target', async () => {
        const { factory, calls } = countingFactory();
        const channel = new BridgeChannel(
            CREDS,
            { onMessage: () => {}, onStatus: () => {} },
            factory,
        );
        // The stub client has no stream, so connect fails after the
        // target check passed. The client must exist by then.
        await channel.connect(hello()).catch(() => {});
        expect(calls()).toBe(1);
        channel.close();
    });
});

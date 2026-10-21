// Phi's maintained Pi integration extension.
// M1 exposes only the wire contract, validation, and bounded-queue
// helpers. The Pi extension factory, lifecycle runtime, and summary
// adapter arrive in M2/M3 on top of this surface.

export {
    PROTOCOL_VERSION,
    CAPABILITY_LIFECYCLE,
    CAPABILITY_SUMMARY,
    MAX_HELLO_BYTES,
    MAX_MESSAGE_BYTES,
    MAX_QUEUED_MESSAGES,
    MAX_QUEUED_BYTES,
    BLOCKED_SEND_TIMEOUT_MS,
    MAX_JOURNAL_RECORDS,
    MAX_SAFE_MESSAGE_BYTES,
    MAX_SUMMARY_TEXT_BYTES,
    negotiateCapabilities,
    isKnownCapability,
    checkSafeMessage,
    requireNonzeroId,
    idToString,
    idFromString,
} from './protocol.js';
export { parseTarget } from './endpoint.js';
export type { IpcTargetKind, ParsedTarget } from './endpoint.js';
export { BoundedQueue } from './queue.js';
export type { SizedMessage } from './queue.js';
export { CommandTracker } from './commands.js';
export type { CancelSource, SummarizeDisposition } from './commands.js';
export {
    SummaryAdapter,
    hasSummarySurface,
    SUMMARY_INSTRUCTIONS,
} from './summary.js';
export type { SummaryHelpers } from './summary.js';
export { createExtension, createExtensionWithHelpers } from './extension.js';

export const PI_PHI_PACKAGE_VERSION = '0.1.0';

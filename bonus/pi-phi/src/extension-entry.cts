// Bundle entry: Pi loads extensions via jiti.import(path, {default:true})
// and calls the result as a function. jiti returns module.exports as-is
// for CJS, so an ESM default export (bundled as {default: fn}) is not
// callable. Assign the factory as the module itself. Only type imports
// reference the Pi package, so the bundle carries no Pi runtime code.
// Host summary helpers arrive via withHelpers from the entry shim,
// which the host loader transforms so Pi imports resolve to the host.
import factory, { createExtensionWithHelpers } from './extension.js';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import type { SummaryHelpers } from './summary.js';

interface FactoryWithHelpers {
    (pi: ExtensionAPI): void;
    withHelpers(helpers: SummaryHelpers | null): (pi: ExtensionAPI) => void;
}

const entry = factory as FactoryWithHelpers;
entry.withHelpers = (helpers: SummaryHelpers | null) => (pi: ExtensionAPI) =>
    createExtensionWithHelpers(pi, helpers);

module.exports = entry;

// Pi entry shim. Shipped as TypeScript source, NOT bundled: the host
// loader transforms this file, so the Pi imports below resolve to the
// host's own package. A bundled .cjs cannot import Pi the same way —
// the loader passes native CJS through without aliasing. The shim
// resolves the two public summary helpers and injects them into the
// sibling dependency-free bundle. Missing helpers disable only
// summaries; the lifecycle runtime stays intact.
//
// The bundle filename is part of this contract: materialization must
// place pi-phi-extension.cjs next to this file under its dist name.

import {
    buildSessionContext,
    generateSummaryWithUsage,
} from '@earendil-works/pi-coding-agent';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';

// The sibling bundle is a build artifact; require keeps this source
// typecheckable without resolving it.
const { withHelpers } = require('./pi-phi-extension.cjs') as {
    withHelpers: (
        helpers: {
            buildSessionContext: typeof buildSessionContext;
            generateSummaryWithUsage: typeof generateSummaryWithUsage;
        } | null,
    ) => (pi: ExtensionAPI) => void;
};

const helpers =
    typeof buildSessionContext === 'function' &&
    typeof generateSummaryWithUsage === 'function'
        ? { buildSessionContext, generateSummaryWithUsage }
        : null;

export default withHelpers(helpers);

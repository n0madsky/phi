package pibridge

import "time"

// Resource bounds from the reviewed plan (section 3.4). Change them only
// through a reviewed plan revision when acceptance behavior changes.
const (
	// ProtocolVersion is the only supported wire protocol version.
	ProtocolVersion uint32 = 1

	// maxHelloBytes bounds the application data of the first Hello message.
	maxHelloBytes = 16 * 1024
	// helloDeadline bounds the time from stream admission to a valid Hello.
	helloDeadline = 5 * time.Second
	// maxMetadataBytes bounds gRPC metadata (headers) per RPC.
	maxMetadataBytes = 8 * 1024
	// maxMessageBytes bounds one application message in either direction.
	maxMessageBytes = 256 * 1024
	// maxQueuedMessages bounds the outbound application queue per peer.
	maxQueuedMessages = 16
	// maxQueuedBytes bounds the outbound application queue bytes per peer.
	maxQueuedBytes = 256 * 1024
	// blockedSendTimeout stops a stream whose outbound write makes no
	// progress for this long.
	blockedSendTimeout = 5 * time.Second
	// maxJournalRecords bounds retained lifecycle records per runtime.
	maxJournalRecords = 64
	// maxSafeMessageBytes bounds safe_message text in errors.
	maxSafeMessageBytes = 2 * 1024
	// maxSummaryTextBytes bounds UTF-8 summary output text.
	maxSummaryTextBytes = 128 * 1024
	// summaryDeadline is the default logical-summary deadline.
	summaryDeadline = 120 * time.Second
	// maxSummaryPerPane bounds concurrent summaries per pane, including
	// unresolved canceled work, without a waiting queue.
	maxSummaryPerPane = 1
	// maxSummaryServer bounds concurrent summaries across the Phi server.
	maxSummaryServer = 2
	// maxRecentOutcomes bounds retained terminal outcomes per lease so
	// an Await that loses the race with a fast peer still reports the
	// outcome instead of a phantom revocation.
	maxRecentOutcomes = 8
	// shutdownDrainTimeout bounds gRPC drain before force-stop.
	shutdownDrainTimeout = 5 * time.Second

	// metadataTokenKey carries the hex-encoded per-process token.
	metadataTokenKey = "phi-pi-token"
	// metadataPaneKey carries the non-secret pane identity.
	metadataPaneKey = "phi-pi-pane"
	// metadataProcessKey carries the non-secret process identity.
	metadataProcessKey = "phi-pi-process"

	// CapabilityLifecycle is the lifecycle-event capability name.
	CapabilityLifecycle = "pi.lifecycle.v1"
	// CapabilitySummary is the separate-summary capability name.
	CapabilitySummary = "pi.summary.v1"
)

package com.workflowengine.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Published by workflow-service (via its outbox) every time it saves the instance — the sole
 * source of truth for instance state since Phase 4. Consumed by api-service to upsert its local
 * read-model {@code InstanceSummaryView}, and by worker-service to keep a local replica it needs
 * for idempotency checks and — critically — to have the instance payload available at all
 * (BusinessStepHandlers like FinalizeLoanHandler read payload fields; nothing else in
 * worker-service ever receives the payload). {@code payload} is carried as the raw JSON string
 * (matching how the source-of-truth entity itself stores it) so consumers can store it straight
 * through without a deserialize/reserialize round trip.
 */
public record InstanceStateChanged(UUID instanceId, UUID definitionId, String definitionName, int definitionVersion,
                                    String status, String currentStepId, String payload, Instant updatedAt) {
}

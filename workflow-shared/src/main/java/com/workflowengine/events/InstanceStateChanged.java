package com.workflowengine.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Published by workflow-service (via its outbox) every time it saves the instance — the sole
 * source of truth for instance state since Phase 4. api-service consumes this to upsert its local
 * read-model {@code InstanceSummaryView}, since it can no longer query workflow-service's table.
 */
public record InstanceStateChanged(UUID instanceId, String definitionName, int definitionVersion,
                                    String status, String currentStepId, Instant updatedAt) {
}

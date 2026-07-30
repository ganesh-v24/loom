package com.workflowengine.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Published by workflow-service (via its outbox) whenever it would previously have written a
 * local audit log entry. api-service consumes this to append to its local read-model
 * {@code AuditLogView} — since Phase 4, workflow-service keeps no local audit table of its own at
 * all (nobody read it once tables stopped being shared), so this event is now the only record.
 */
public record AuditEventOccurred(UUID instanceId, String eventType, String detail, Instant timestamp) {
}

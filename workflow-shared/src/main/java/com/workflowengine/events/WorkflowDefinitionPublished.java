package com.workflowengine.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Published by api-service (the sole source of truth for definitions) to
 * {@link Topics#DEFINITION_PUBLISHED} whenever one is submitted. workflow-service and
 * worker-service each consume it to keep a local, read-only replica — they can no longer query
 * api-service's table directly now that each service has its own database.
 *
 * Known trade-off: starting an instance immediately after submitting its definition can race this
 * replication, same as reading from any eventually-consistent replica.
 */
public record WorkflowDefinitionPublished(UUID id, String name, int version, String body, Instant createdAt) {
}

package com.workflowengine.events;

import java.util.UUID;

/**
 * Published by workflow-service to {@link Topics#COMPENSATION_REQUESTED}: "undo this
 * already-succeeded step" — part of the Saga rollback triggered by a later step's permanent
 * failure. {@code compensateHandler} is the bean name in worker-service to invoke.
 */
public record CompensationRequested(UUID instanceId, UUID definitionId, String stepId, String compensateHandler) {
}

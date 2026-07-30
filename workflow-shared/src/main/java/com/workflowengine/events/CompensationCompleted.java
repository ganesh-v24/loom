package com.workflowengine.events;

import java.util.UUID;

/** Published by worker-service to {@link Topics#COMPENSATION_COMPLETED} once one step's undo action has run. */
public record CompensationCompleted(UUID instanceId, UUID definitionId, String stepId, boolean success, String errorMessage) {
}

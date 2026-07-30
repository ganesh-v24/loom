package com.workflowengine.events;

import java.util.Map;
import java.util.UUID;

/** Published to {@link Topics#STEP_COMPLETED} once a step's outcome (success or exhausted-retry failure) is known. */
public record StepExecutionCompleted(UUID instanceId, UUID definitionId, String stepId, int attempt,
                                      boolean success, Map<String, Object> output, String errorMessage) {
}

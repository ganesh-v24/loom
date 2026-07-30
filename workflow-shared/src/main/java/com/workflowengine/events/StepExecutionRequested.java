package com.workflowengine.events;

import java.util.UUID;

/** Published to {@link Topics#STEP_EXECUTE}: "run this step, this attempt number." */
public record StepExecutionRequested(UUID instanceId, UUID definitionId, String stepId, int attempt) {
}

package com.workflowengine.engine;

import java.util.Map;
import java.util.UUID;

public record StepContext(UUID instanceId, String stepId, int attempt, Map<String, Object> payload) {
}

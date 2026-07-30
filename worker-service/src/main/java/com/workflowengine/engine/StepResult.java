package com.workflowengine.engine;

import java.util.Map;

public record StepResult(boolean success, Map<String, Object> output, String errorMessage) {

    public static StepResult ok(Map<String, Object> output) {
        return new StepResult(true, output, null);
    }

    public static StepResult ok() {
        return ok(Map.of());
    }

    public static StepResult failure(String errorMessage) {
        return new StepResult(false, Map.of(), errorMessage);
    }
}

package com.workflowengine.definition;

import java.util.List;

public record WorkflowDefinitionSpec(String name, String start, List<StepSpec> steps) {

    public StepSpec getStep(String stepId) {
        return steps.stream()
                .filter(s -> s.id().equals(stepId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown step id: " + stepId));
    }
}

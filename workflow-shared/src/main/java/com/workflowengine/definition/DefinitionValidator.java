package com.workflowengine.definition;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
public class DefinitionValidator {

    public void validate(WorkflowDefinitionSpec spec) {
        if (spec.steps() == null || spec.steps().isEmpty()) {
            throw new DefinitionValidationException("Workflow must contain at least one step");
        }

        Map<String, StepSpec> byId = new HashMap<>();
        for (StepSpec step : spec.steps()) {
            if (byId.putIfAbsent(step.id(), step) != null) {
                throw new DefinitionValidationException("Duplicate step id: " + step.id());
            }
        }

        if (spec.start() == null || !byId.containsKey(spec.start())) {
            throw new DefinitionValidationException("start references unknown step id: " + spec.start());
        }

        for (StepSpec step : spec.steps()) {
            if (step.type() == StepType.AUTOMATIC && (step.handler() == null || step.handler().isBlank())) {
                throw new DefinitionValidationException("AUTOMATIC step '" + step.id() + "' requires a handler");
            }
            if (step.next() != null && !byId.containsKey(step.next())) {
                throw new DefinitionValidationException("Step '" + step.id() + "' has unknown next: " + step.next());
            }
        }

        Set<String> visited = new HashSet<>();
        String current = spec.start();
        while (current != null) {
            if (!visited.add(current)) {
                throw new DefinitionValidationException("Cycle detected in workflow at step: " + current);
            }
            current = byId.get(current).next();
        }
    }
}

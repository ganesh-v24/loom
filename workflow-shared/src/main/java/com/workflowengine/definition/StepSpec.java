package com.workflowengine.definition;

/**
 * {@code compensate}: optional bean name of a compensation handler that undoes this step's
 * effect. Only meaningful for steps that already succeeded when a later step in the same
 * instance fails permanently — see WorkflowCoordinatorService in workflow-service.
 */
public record StepSpec(String id, String name, StepType type, String handler, String next, RetrySpec retry,
                        String compensate) {
}

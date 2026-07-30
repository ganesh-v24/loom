package com.workflowengine.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Published by worker-service and notification-service whenever either writes a step-execution
 * row. api-service consumes this to upsert its local read-model {@code StepExecutionView} — the
 * one place the full step history (automatic + email + approval steps) is aggregated for display.
 *
 * {@code status} is a plain String, deliberately not the internal StepExecutionStatus enum used
 * by each service's own persistence — decouples this event's contract from any one service's
 * internal representation, so refactoring that enum can't silently break the event schema.
 */
public record StepExecutionRecorded(UUID instanceId, String stepId, String stepName, String status, int attempt,
                                     String input, String output, String errorMessage,
                                     Instant startedAt, Instant finishedAt) {
}

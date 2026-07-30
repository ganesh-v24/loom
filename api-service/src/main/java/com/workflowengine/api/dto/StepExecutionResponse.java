package com.workflowengine.api.dto;

import java.time.Instant;

public record StepExecutionResponse(String stepId, String stepName, String status, int attempt,
                                     Instant startedAt, Instant finishedAt, String errorMessage) {
}

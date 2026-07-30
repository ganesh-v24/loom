package com.workflowengine.api.dto;

import java.time.Instant;
import java.util.UUID;

public record InstanceSummaryResponse(UUID id, String definitionName, int definitionVersion, String status,
                                       String currentStepId, Instant createdAt, Instant updatedAt) {
}

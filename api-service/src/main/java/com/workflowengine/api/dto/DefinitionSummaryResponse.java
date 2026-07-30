package com.workflowengine.api.dto;

import java.time.Instant;
import java.util.UUID;

public record DefinitionSummaryResponse(UUID id, String name, int version, Instant createdAt) {
}

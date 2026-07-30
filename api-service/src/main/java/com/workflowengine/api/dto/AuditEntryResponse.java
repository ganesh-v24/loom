package com.workflowengine.api.dto;

import java.time.Instant;

public record AuditEntryResponse(String eventType, String detail, Instant timestamp) {
}

package com.workflowengine.api.dto;

import java.util.List;

public record InstanceDetailResponse(InstanceSummaryResponse summary, List<StepExecutionResponse> steps,
                                      List<AuditEntryResponse> audit) {
}

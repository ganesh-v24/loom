package com.workflowengine.events;

import java.util.Map;
import java.util.UUID;

/**
 * Published by api-service to {@link Topics#INSTANCE_LIFECYCLE} whenever it can't decide the
 * next step itself — a new instance starting, or an approval that has a next step to dispatch.
 * workflow-service consumes this and reacts exactly like the in-process beginOrResumeAt used to.
 *
 * Since Phase 4 (database-per-service), api-service no longer owns the instance row — it assigns
 * the instance id itself but workflow-service creates the actual row, so {@code reason=STARTED}
 * carries everything needed to create it (definitionName/definitionVersion/payload). Those three
 * fields are unused for {@code reason=APPROVED}, where the row already exists.
 */
public record InstanceLifecycleRequested(UUID instanceId, UUID definitionId, String stepId, Reason reason,
                                          String definitionName, int definitionVersion, Map<String, Object> payload) {

    public enum Reason {
        STARTED,
        APPROVED
    }
}

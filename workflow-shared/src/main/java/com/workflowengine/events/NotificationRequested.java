package com.workflowengine.events;

import java.util.Map;
import java.util.UUID;

/** Published by worker-service to {@link Topics#NOTIFICATION_REQUESTED} for EMAIL_NOTIFY steps. */
public record NotificationRequested(UUID instanceId, UUID definitionId, String stepId, String stepName,
                                     int attempt, Map<String, Object> payload) {
}

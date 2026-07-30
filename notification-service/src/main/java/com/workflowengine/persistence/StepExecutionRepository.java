package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/** Write-only here: notification-service owns the StepExecutionEntity row for EMAIL_NOTIFY steps. */
public interface StepExecutionRepository extends JpaRepository<StepExecutionEntity, UUID> {

    boolean existsByInstanceIdAndStepIdAndAttemptAndStatus(UUID instanceId, String stepId, int attempt, StepExecutionStatus status);
}

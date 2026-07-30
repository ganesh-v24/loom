package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * A read-only replica, populated by StepExecutionReplicaListener consuming StepExecutionRecorded
 * — workflow-service's own coordinator logic never writes here directly, only reads this history
 * to decide compensation order.
 */
public interface StepExecutionRepository extends JpaRepository<StepExecutionEntity, UUID> {

    List<StepExecutionEntity> findByInstanceIdOrderByStartedAtAsc(UUID instanceId);

    Optional<StepExecutionEntity> findByInstanceIdAndStepIdAndAttempt(UUID instanceId, String stepId, int attempt);
}

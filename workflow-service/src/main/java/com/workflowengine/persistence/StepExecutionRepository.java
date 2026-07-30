package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/** Read-only here: workflow-service reads step history to decide compensation order, never writes it. */
public interface StepExecutionRepository extends JpaRepository<StepExecutionEntity, UUID> {

    List<StepExecutionEntity> findByInstanceIdOrderByStartedAtAsc(UUID instanceId);
}

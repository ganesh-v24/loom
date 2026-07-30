package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/** Read-only here: worker-service only checks current status/currentStepId for idempotency guards. */
public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstanceEntity, UUID> {
}

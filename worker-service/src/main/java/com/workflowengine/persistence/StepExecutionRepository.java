package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface StepExecutionRepository extends JpaRepository<StepExecutionEntity, UUID> {

    List<StepExecutionEntity> findByInstanceIdOrderByStartedAtAsc(UUID instanceId);
}

package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface StepExecutionViewRepository extends JpaRepository<StepExecutionView, UUID> {

    List<StepExecutionView> findByInstanceIdOrderByStartedAtAsc(UUID instanceId);

    Optional<StepExecutionView> findByInstanceIdAndStepIdAndAttempt(UUID instanceId, String stepId, int attempt);
}

package com.workflowengine.engine;

import com.workflowengine.events.StepExecutionRecorded;
import com.workflowengine.events.Topics;
import com.workflowengine.persistence.StepExecutionEntity;
import com.workflowengine.persistence.StepExecutionRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Keeps a local, read-only replica of step execution history — this service can no longer query
 * worker-service's/notification-service's tables directly now that each has its own database.
 * WorkflowCoordinatorService reads this to decide Saga compensation order (which already-succeeded
 * steps declare a compensate handler), so without this replica compensation can never trigger.
 */
@Component
@RequiredArgsConstructor
public class StepExecutionReplicaListener {

    private final StepExecutionRepository stepExecutionRepository;

    @KafkaListener(topics = Topics.STEP_EXECUTION_RECORDED)
    public void onStepExecutionRecorded(StepExecutionRecorded event) {
        StepExecutionEntity execution = stepExecutionRepository
                .findByInstanceIdAndStepIdAndAttempt(event.instanceId(), event.stepId(), event.attempt())
                .orElseGet(() -> StepExecutionEntity.builder()
                        .instanceId(event.instanceId())
                        .stepId(event.stepId())
                        .build());

        execution.setStepName(event.stepName());
        execution.setStatus(StepExecutionStatus.valueOf(event.status()));
        execution.setAttempt(event.attempt());
        execution.setInput(event.input());
        execution.setOutput(event.output());
        execution.setErrorMessage(event.errorMessage());
        execution.setStartedAt(event.startedAt());
        execution.setFinishedAt(event.finishedAt());
        stepExecutionRepository.save(execution);
    }
}

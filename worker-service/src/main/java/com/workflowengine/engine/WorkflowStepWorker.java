package com.workflowengine.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workflowengine.definition.RetrySpec;
import com.workflowengine.definition.StepSpec;
import com.workflowengine.definition.StepType;
import com.workflowengine.definition.WorkflowDefinitionSpec;
import com.workflowengine.events.CompensationRequested;
import com.workflowengine.events.StepExecutionRequested;
import com.workflowengine.events.Topics;
import com.workflowengine.persistence.InstanceStatus;
import com.workflowengine.persistence.StepExecutionEntity;
import com.workflowengine.persistence.StepExecutionRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Consumes {@code workflow.step.execute} — the "worker pool" from the roadmap. concurrency=3
 * simulates multiple worker threads/nodes pulling from the same consumer group; partitioning by
 * instanceId keeps one instance's steps ordered on one partition while different instances
 * process in parallel. Also runs compensation actions requested by workflow-service's Saga logic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WorkflowStepWorker {

    private final WorkflowInstanceRepository instanceRepository;
    private final StepExecutionRepository stepExecutionRepository;
    private final WorkflowSpecLoader specLoader;
    private final ApplicationContext applicationContext;
    private final WorkflowEventProducer eventProducer;
    private final ObjectMapper jsonMapper;

    @KafkaListener(topics = Topics.STEP_EXECUTE, concurrency = "3")
    public void onStepExecutionRequested(StepExecutionRequested event) {
        WorkflowInstanceEntity instance = instanceRepository.findById(event.instanceId()).orElse(null);
        if (instance == null) {
            // InstanceStateChanged (a different topic, no ordering guarantee relative to this
            // one) may not have replicated yet — throw so the container's retry/backoff gives it
            // a chance to arrive, instead of silently dropping this step execution forever.
            throw new IllegalStateException("Instance replica not yet available: " + event.instanceId());
        }
        if (instance.getStatus() != InstanceStatus.RUNNING) {
            log.debug("Ignoring stale step-execute event for instance {} (not RUNNING)", event.instanceId());
            return;
        }
        if (!instance.getCurrentStepId().equals(event.stepId())) {
            log.debug("Ignoring stale step-execute event for instance {} (already past step {})",
                    event.instanceId(), event.stepId());
            return;
        }
        if (alreadySucceeded(event)) {
            log.debug("Ignoring duplicate step-execute event for instance {} step {} attempt {} (already succeeded)",
                    event.instanceId(), event.stepId(), event.attempt());
            return;
        }

        WorkflowDefinitionSpec spec = specLoader.load(event.definitionId());
        StepSpec step = spec.getStep(event.stepId());

        if (step.type() == StepType.EMAIL_NOTIFY) {
            // notification-service owns EMAIL_NOTIFY entirely: runs it, writes its own
            // StepExecutionEntity row, and publishes StepExecutionCompleted itself.
            eventProducer.publishNotificationRequested(event.instanceId(), event.definitionId(), step.id(), step.name(),
                    event.attempt(), readPayload(instance.getPayload()));
            return;
        }

        RetrySpec retry = step.retry() != null ? step.retry() : RetrySpec.DEFAULT;
        StepResult result = executeOnce(instance, step, event.attempt());

        if (result.success()) {
            eventProducer.publishStepCompleted(event.instanceId(), event.definitionId(), step.id(), event.attempt(),
                    true, result.output(), null);
            return;
        }

        if (event.attempt() < retry.maxAttempts()) {
            if (retry.backoffMs() > 0) {
                sleep(retry.backoffMs());
            }
            eventProducer.requestStepExecution(event.instanceId(), event.definitionId(), step.id(), event.attempt() + 1);
        } else {
            eventProducer.publishToDlq(event, result.errorMessage());
            eventProducer.publishStepCompleted(event.instanceId(), event.definitionId(), step.id(), event.attempt(),
                    false, Map.of(), result.errorMessage());
        }
    }

    /**
     * Runs a Saga compensation (undo) action. Best-effort, single attempt, no retry — a
     * compensation that itself keeps failing shouldn't block the rest of the rollback; see
     * WorkflowCoordinatorService's COMPENSATION_FAILED handling. Real compensation handlers
     * should be idempotent, since this event can be redelivered like any other Kafka message.
     */
    @KafkaListener(topics = Topics.COMPENSATION_REQUESTED)
    public void onCompensationRequested(CompensationRequested event) {
        StepResult result;
        try {
            BusinessStepHandler handler = applicationContext.getBean(event.compensateHandler(), BusinessStepHandler.class);
            result = handler.execute(new StepContext(event.instanceId(), event.stepId(), 1, Map.of()));
        } catch (Exception ex) {
            result = StepResult.failure(ex.getMessage());
        }
        eventProducer.publishCompensationCompleted(event.instanceId(), event.definitionId(), event.stepId(),
                result.success(), result.errorMessage());
    }

    private boolean alreadySucceeded(StepExecutionRequested event) {
        return stepExecutionRepository.findByInstanceIdOrderByStartedAtAsc(event.instanceId()).stream()
                .anyMatch(e -> e.getStepId().equals(event.stepId()) && e.getAttempt() == event.attempt()
                        && e.getStatus() == StepExecutionStatus.SUCCEEDED);
    }

    StepResult executeOnce(WorkflowInstanceEntity instance, StepSpec step, int attempt) {
        Map<String, Object> payload = readPayload(instance.getPayload());

        StepExecutionEntity execution = StepExecutionEntity.builder()
                .instanceId(instance.getId())
                .stepId(step.id())
                .stepName(step.name())
                .status(StepExecutionStatus.RUNNING)
                .attempt(attempt)
                .input(writePayload(payload))
                .startedAt(Instant.now())
                .build();

        StepResult result;
        try {
            result = resolveHandler(step).execute(new StepContext(instance.getId(), step.id(), attempt, payload));
        } catch (Exception ex) {
            result = StepResult.failure(ex.getMessage());
        }
        execution.setFinishedAt(Instant.now());

        if (result.success()) {
            execution.setStatus(StepExecutionStatus.SUCCEEDED);
            execution.setOutput(writePayload(result.output()));
        } else {
            execution.setStatus(StepExecutionStatus.FAILED);
            execution.setErrorMessage(result.errorMessage());
        }
        stepExecutionRepository.save(execution);
        eventProducer.publishStepExecutionRecorded(execution.getInstanceId(), execution.getStepId(), execution.getStepName(),
                execution.getStatus().name(), execution.getAttempt(), execution.getInput(), execution.getOutput(),
                execution.getErrorMessage(), execution.getStartedAt(), execution.getFinishedAt());
        return result;
    }

    private BusinessStepHandler resolveHandler(StepSpec step) {
        return applicationContext.getBean(step.handler(), BusinessStepHandler.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readPayload(String json) {
        if (json == null || json.isBlank()) {
            return new HashMap<>();
        }
        try {
            return jsonMapper.readValue(json, Map.class);
        } catch (Exception e) {
            throw new IllegalStateException("Corrupt payload JSON", e);
        }
    }

    private String writePayload(Map<String, Object> payload) {
        try {
            return jsonMapper.writeValueAsString(payload == null ? Map.of() : payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize payload", e);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.workflowengine.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workflowengine.definition.StepSpec;
import com.workflowengine.definition.StepType;
import com.workflowengine.definition.WorkflowDefinitionSpec;
import com.workflowengine.events.CompensationCompleted;
import com.workflowengine.events.InstanceLifecycleRequested;
import com.workflowengine.events.StepExecutionCompleted;
import com.workflowengine.persistence.InstanceStatus;
import com.workflowengine.persistence.StepExecutionRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.workflowengine.events.Topics.COMPENSATION_COMPLETED;
import static com.workflowengine.events.Topics.INSTANCE_LIFECYCLE;
import static com.workflowengine.events.Topics.STEP_COMPLETED;

/**
 * The state machine "brain": the sole source of truth for instance state since Phase 4 (database-
 * per-service). Class-level @Transactional so every instance-table write and its corresponding
 * outbox insert (via WorkflowEventProducer) commit atomically in one Postgres transaction — see
 * OutboxEventEntity for why that atomicity matters.
 *
 * currentStepId is reused as "the step this instance is currently occupied with" — normally the
 * next step to run, but while status=COMPENSATING it points at the compensation target currently
 * being undone. Deliberate field reuse, not an accident.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@Transactional
public class WorkflowCoordinatorService {

    private final WorkflowInstanceRepository instanceRepository;
    private final StepExecutionRepository stepExecutionRepository;
    private final WorkflowSpecLoader specLoader;
    private final WorkflowEventProducer eventProducer;
    private final ObjectMapper jsonMapper;

    @KafkaListener(topics = INSTANCE_LIFECYCLE)
    public void onInstanceLifecycleRequested(InstanceLifecycleRequested event) {
        if (event.reason() == InstanceLifecycleRequested.Reason.STARTED) {
            onInstanceStarted(event);
            return;
        }

        WorkflowInstanceEntity instance = instanceRepository.findById(event.instanceId()).orElse(null);
        if (instance == null || instance.getStatus() != InstanceStatus.RUNNING) {
            log.debug("Ignoring stale lifecycle event for instance {} (not RUNNING)", event.instanceId());
            return;
        }
        if (!instance.getCurrentStepId().equals(event.stepId())) {
            log.debug("Ignoring stale lifecycle event for instance {} (already past step {})",
                    event.instanceId(), event.stepId());
            return;
        }
        beginOrResumeAt(instance, specLoader.load(event.definitionId()));
    }

    private void onInstanceStarted(InstanceLifecycleRequested event) {
        if (instanceRepository.existsById(event.instanceId())) {
            log.debug("Ignoring duplicate instance-creation event for instance {}", event.instanceId());
            return;
        }

        Instant now = Instant.now();
        WorkflowInstanceEntity instance = WorkflowInstanceEntity.builder()
                .id(event.instanceId())
                .workflowDefinitionId(event.definitionId())
                .definitionName(event.definitionName())
                .definitionVersion(event.definitionVersion())
                .status(InstanceStatus.RUNNING)
                .currentStepId(event.stepId())
                .payload(writeJson(event.payload()))
                .createdAt(now)
                .updatedAt(now)
                .build();

        beginOrResumeAt(instance, specLoader.load(event.definitionId()));
    }

    @KafkaListener(topics = STEP_COMPLETED)
    public void onStepExecutionCompleted(StepExecutionCompleted event) {
        WorkflowInstanceEntity instance = instanceRepository.findById(event.instanceId()).orElse(null);
        if (instance == null || instance.getStatus() != InstanceStatus.RUNNING) {
            log.debug("Ignoring stale step-completed event for instance {} (not RUNNING)", event.instanceId());
            return;
        }
        if (!instance.getCurrentStepId().equals(event.stepId())) {
            log.debug("Ignoring stale step-completed event for instance {} (already past step {})",
                    event.instanceId(), event.stepId());
            return;
        }

        WorkflowDefinitionSpec spec = specLoader.load(event.definitionId());
        StepSpec step = spec.getStep(event.stepId());

        if (!event.success()) {
            startFailureHandling(instance, spec, step, event.errorMessage());
            return;
        }

        eventProducer.publishAuditEvent(instance.getId(), "STEP_SUCCEEDED", "Step succeeded: " + step.name());

        if (step.next() == null) {
            instance.setStatus(InstanceStatus.COMPLETED);
            instance.setUpdatedAt(Instant.now());
            saveInstance(instance);
            eventProducer.publishAuditEvent(instance.getId(), "INSTANCE_COMPLETED", "Workflow instance completed");
            return;
        }

        instance.setCurrentStepId(step.next());
        instance.setStatus(InstanceStatus.RUNNING);
        beginOrResumeAt(instance, spec);
    }

    @KafkaListener(topics = COMPENSATION_COMPLETED)
    public void onCompensationCompleted(CompensationCompleted event) {
        WorkflowInstanceEntity instance = instanceRepository.findById(event.instanceId()).orElse(null);
        if (instance == null || instance.getStatus() != InstanceStatus.COMPENSATING) {
            log.debug("Ignoring stale compensation-completed event for instance {} (not COMPENSATING)", event.instanceId());
            return;
        }
        if (!instance.getCurrentStepId().equals(event.stepId())) {
            log.debug("Ignoring stale compensation-completed event for instance {} (not compensating step {})",
                    event.instanceId(), event.stepId());
            return;
        }

        eventProducer.publishAuditEvent(instance.getId(), event.success() ? "STEP_COMPENSATED" : "COMPENSATION_FAILED",
                (event.success() ? "Compensated step: " : "Compensation failed for step (continuing anyway): ")
                        + event.stepId() + (event.errorMessage() != null ? " - " + event.errorMessage() : ""));

        WorkflowDefinitionSpec spec = specLoader.load(event.definitionId());
        Optional<String> next = findNextCompensationTarget(instance.getId(), spec, event.stepId());

        if (next.isPresent()) {
            instance.setCurrentStepId(next.get());
            instance.setUpdatedAt(Instant.now());
            saveInstance(instance);
            StepSpec compensableStep = spec.getStep(next.get());
            eventProducer.requestCompensation(instance.getId(), event.definitionId(), next.get(), compensableStep.compensate());
        } else {
            instance.setStatus(InstanceStatus.COMPENSATED);
            instance.setUpdatedAt(Instant.now());
            saveInstance(instance);
            eventProducer.publishAuditEvent(instance.getId(), "INSTANCE_COMPENSATED", "Saga compensation complete; instance rolled back");
        }
    }

    /**
     * Given an instance whose currentStepId/status have just been set (start, approval-resume,
     * or advance-to-next-step), either dispatches a StepExecutionRequested or pauses the instance
     * for manual approval — whichever the current step calls for.
     */
    public void beginOrResumeAt(WorkflowInstanceEntity instance, WorkflowDefinitionSpec spec) {
        instance.setUpdatedAt(Instant.now());
        saveInstance(instance);

        StepSpec step = spec.getStep(instance.getCurrentStepId());
        if (step.type() == StepType.MANUAL_APPROVAL) {
            instance.setStatus(InstanceStatus.WAITING_APPROVAL);
            instance.setUpdatedAt(Instant.now());
            saveInstance(instance);
            eventProducer.publishAuditEvent(instance.getId(), "WAITING_APPROVAL", "Paused for manual approval at step: " + step.name());
        } else {
            eventProducer.requestStepExecution(instance.getId(), instance.getWorkflowDefinitionId(), step.id(), 1);
        }
    }

    private void startFailureHandling(WorkflowInstanceEntity instance, WorkflowDefinitionSpec spec, StepSpec step, String errorMessage) {
        List<String> compensable = compensableStepIdsAscending(instance.getId(), spec);

        if (compensable.isEmpty()) {
            instance.setStatus(InstanceStatus.FAILED);
            instance.setUpdatedAt(Instant.now());
            saveInstance(instance);
            eventProducer.publishAuditEvent(instance.getId(), "INSTANCE_FAILED", "Step failed permanently: " + step.name() + " - " + errorMessage);
            return;
        }

        String newest = compensable.get(compensable.size() - 1);
        instance.setStatus(InstanceStatus.COMPENSATING);
        instance.setCurrentStepId(newest);
        instance.setUpdatedAt(Instant.now());
        saveInstance(instance);
        eventProducer.publishAuditEvent(instance.getId(), "COMPENSATING", "Step failed permanently: " + step.name() + " - " + errorMessage
                + "; rolling back starting with: " + newest);

        StepSpec compensableStep = spec.getStep(newest);
        eventProducer.requestCompensation(instance.getId(), instance.getWorkflowDefinitionId(), newest, compensableStep.compensate());
    }

    /** SUCCEEDED steps (oldest first) that declare a compensate handler — the Saga rollback candidates. */
    private List<String> compensableStepIdsAscending(UUID instanceId, WorkflowDefinitionSpec spec) {
        return stepExecutionRepository.findByInstanceIdOrderByStartedAtAsc(instanceId).stream()
                .filter(e -> e.getStatus() == StepExecutionStatus.SUCCEEDED)
                .map(com.workflowengine.persistence.StepExecutionEntity::getStepId)
                .distinct()
                .filter(stepId -> spec.getStep(stepId).compensate() != null)
                .toList();
    }

    private Optional<String> findNextCompensationTarget(UUID instanceId, WorkflowDefinitionSpec spec, String justCompensatedStepId) {
        List<String> ascending = compensableStepIdsAscending(instanceId, spec);
        int idx = ascending.indexOf(justCompensatedStepId);
        if (idx <= 0) {
            return Optional.empty();
        }
        return Optional.of(ascending.get(idx - 1));
    }

    private void saveInstance(WorkflowInstanceEntity instance) {
        instanceRepository.save(instance);
        eventProducer.publishInstanceStateChanged(instance);
    }

    private String writeJson(Map<String, Object> payload) {
        try {
            return jsonMapper.writeValueAsString(payload == null ? Map.of() : payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize payload", e);
        }
    }
}

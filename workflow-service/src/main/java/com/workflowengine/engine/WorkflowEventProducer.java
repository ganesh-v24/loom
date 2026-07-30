package com.workflowengine.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workflowengine.events.AuditEventOccurred;
import com.workflowengine.events.CompensationRequested;
import com.workflowengine.events.InstanceStateChanged;
import com.workflowengine.events.StepExecutionRequested;
import com.workflowengine.events.Topics;
import com.workflowengine.persistence.OutboxEventEntity;
import com.workflowengine.persistence.OutboxRepository;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Every outbound event from workflow-service goes through the transactional outbox instead of a
 * direct KafkaTemplate.send() — see OutboxEventEntity for why. Callers (WorkflowCoordinatorService)
 * must run inside the same transaction as their instance-table writes for the atomicity guarantee
 * to hold; that's arranged via @Transactional on the coordinator, not here.
 */
@Component
@RequiredArgsConstructor
public class WorkflowEventProducer {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper jsonMapper;

    public void requestStepExecution(UUID instanceId, UUID definitionId, String stepId, int attempt) {
        enqueue(Topics.STEP_EXECUTE, instanceId.toString(),
                new StepExecutionRequested(instanceId, definitionId, stepId, attempt));
    }

    public void requestCompensation(UUID instanceId, UUID definitionId, String stepId, String compensateHandler) {
        enqueue(Topics.COMPENSATION_REQUESTED, instanceId.toString(),
                new CompensationRequested(instanceId, definitionId, stepId, compensateHandler));
    }

    public void publishInstanceStateChanged(WorkflowInstanceEntity instance) {
        enqueue(Topics.INSTANCE_STATE_CHANGED, instance.getId().toString(),
                new InstanceStateChanged(instance.getId(), instance.getDefinitionName(), instance.getDefinitionVersion(),
                        instance.getStatus().name(), instance.getCurrentStepId(), instance.getUpdatedAt()));
    }

    public void publishAuditEvent(UUID instanceId, String eventType, String detail) {
        enqueue(Topics.AUDIT_EVENT_OCCURRED, instanceId.toString(),
                new AuditEventOccurred(instanceId, eventType, detail, Instant.now()));
    }

    private void enqueue(String topic, String key, Object event) {
        try {
            outboxRepository.save(OutboxEventEntity.builder()
                    .topic(topic)
                    .messageKey(key)
                    .payload(jsonMapper.writeValueAsString(event))
                    .createdAt(Instant.now())
                    .build());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to enqueue outbox event", e);
        }
    }
}

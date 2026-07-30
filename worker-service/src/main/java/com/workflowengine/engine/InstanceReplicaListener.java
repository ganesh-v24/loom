package com.workflowengine.engine;

import com.workflowengine.events.InstanceStateChanged;
import com.workflowengine.events.Topics;
import com.workflowengine.persistence.InstanceStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Keeps a local, read-only replica of instance state — this service can no longer query
 * workflow-service's table directly now that each service has its own database. This one matters
 * for more than just the idempotency guard in WorkflowStepWorker: it's the only way this service
 * ever receives the instance's payload, which BusinessStepHandlers read (e.g. FinalizeLoanHandler
 * checks payload.simulateFailure).
 */
@Component
@RequiredArgsConstructor
public class InstanceReplicaListener {

    private final WorkflowInstanceRepository instanceRepository;

    @KafkaListener(topics = Topics.INSTANCE_STATE_CHANGED)
    public void onInstanceStateChanged(InstanceStateChanged event) {
        WorkflowInstanceEntity instance = instanceRepository.findById(event.instanceId())
                .orElseGet(() -> WorkflowInstanceEntity.builder()
                        .id(event.instanceId())
                        .workflowDefinitionId(event.definitionId())
                        .createdAt(event.updatedAt())
                        .build());

        instance.setDefinitionName(event.definitionName());
        instance.setDefinitionVersion(event.definitionVersion());
        instance.setStatus(InstanceStatus.valueOf(event.status()));
        instance.setCurrentStepId(event.currentStepId());
        instance.setPayload(event.payload());
        instance.setUpdatedAt(event.updatedAt());
        instanceRepository.save(instance);
    }
}

package com.workflowengine.api;

import com.workflowengine.events.AuditEventOccurred;
import com.workflowengine.events.InstanceStateChanged;
import com.workflowengine.events.StepExecutionRecorded;
import com.workflowengine.events.Topics;
import com.workflowengine.persistence.AuditLogView;
import com.workflowengine.persistence.AuditLogViewRepository;
import com.workflowengine.persistence.InstanceStatus;
import com.workflowengine.persistence.InstanceSummaryView;
import com.workflowengine.persistence.InstanceSummaryViewRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import com.workflowengine.persistence.StepExecutionView;
import com.workflowengine.persistence.StepExecutionViewRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Keeps api-service's local read model in sync with data other services own — the CQRS read
 * side. Everything here is an upsert: these events can be redelivered like any other Kafka
 * message, and this service never treats itself as the source of truth for what it consumes here.
 */
@Component
@RequiredArgsConstructor
public class ReadModelUpdater {

    private final InstanceSummaryViewRepository instanceSummaryViewRepository;
    private final StepExecutionViewRepository stepExecutionViewRepository;
    private final AuditLogViewRepository auditLogViewRepository;

    @KafkaListener(topics = Topics.INSTANCE_STATE_CHANGED)
    public void onInstanceStateChanged(InstanceStateChanged event) {
        InstanceSummaryView view = instanceSummaryViewRepository.findById(event.instanceId())
                .orElseGet(() -> InstanceSummaryView.builder()
                        .id(event.instanceId())
                        .workflowDefinitionId(event.definitionId())
                        .createdAt(event.updatedAt())
                        .build());

        view.setDefinitionName(event.definitionName());
        view.setDefinitionVersion(event.definitionVersion());
        view.setStatus(InstanceStatus.valueOf(event.status()));
        view.setCurrentStepId(event.currentStepId());
        view.setUpdatedAt(event.updatedAt());
        instanceSummaryViewRepository.save(view);
    }

    @KafkaListener(topics = Topics.STEP_EXECUTION_RECORDED)
    public void onStepExecutionRecorded(StepExecutionRecorded event) {
        StepExecutionView view = stepExecutionViewRepository
                .findByInstanceIdAndStepIdAndAttempt(event.instanceId(), event.stepId(), event.attempt())
                .orElseGet(() -> StepExecutionView.builder()
                        .instanceId(event.instanceId())
                        .stepId(event.stepId())
                        .build());

        view.setStepName(event.stepName());
        view.setStatus(StepExecutionStatus.valueOf(event.status()));
        view.setAttempt(event.attempt());
        view.setInput(event.input());
        view.setOutput(event.output());
        view.setErrorMessage(event.errorMessage());
        view.setStartedAt(event.startedAt());
        view.setFinishedAt(event.finishedAt());
        stepExecutionViewRepository.save(view);
    }

    @KafkaListener(topics = Topics.AUDIT_EVENT_OCCURRED)
    public void onAuditEventOccurred(AuditEventOccurred event) {
        auditLogViewRepository.save(AuditLogView.builder()
                .instanceId(event.instanceId())
                .eventType(event.eventType())
                .detail(event.detail())
                .timestamp(event.timestamp())
                .build());
    }
}

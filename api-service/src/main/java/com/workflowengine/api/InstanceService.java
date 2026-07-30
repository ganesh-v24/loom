package com.workflowengine.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workflowengine.api.dto.AuditEntryResponse;
import com.workflowengine.api.dto.DefinitionSummaryResponse;
import com.workflowengine.api.dto.InstanceDetailResponse;
import com.workflowengine.api.dto.InstanceSummaryResponse;
import com.workflowengine.api.dto.StepExecutionResponse;
import com.workflowengine.definition.DefinitionParser;
import com.workflowengine.definition.DefinitionValidator;
import com.workflowengine.definition.StepSpec;
import com.workflowengine.definition.WorkflowDefinitionSpec;
import com.workflowengine.events.InstanceLifecycleRequested;
import com.workflowengine.persistence.AuditLogView;
import com.workflowengine.persistence.AuditLogViewRepository;
import com.workflowengine.persistence.InstanceStatus;
import com.workflowengine.persistence.InstanceSummaryView;
import com.workflowengine.persistence.InstanceSummaryViewRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import com.workflowengine.persistence.StepExecutionView;
import com.workflowengine.persistence.StepExecutionViewRepository;
import com.workflowengine.persistence.WorkflowDefinitionEntity;
import com.workflowengine.persistence.WorkflowDefinitionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Shared logic behind both the JSON REST API and the HTML dashboard.
 *
 * Since Phase 4 (database-per-service), api-service is the source of truth only for definitions.
 * Instance state is owned by workflow-service; the *View repositories here are a CQRS read model,
 * kept in sync by ReadModelUpdater consuming InstanceStateChanged/StepExecutionRecorded/
 * AuditEventOccurred. Where this service performs its own action (approving a step), it still
 * writes its own read-model rows directly and optimistically updates InstanceSummaryView — no
 * event round-trip needed for data it generates itself — then lets the next authoritative
 * InstanceStateChanged event confirm or correct the guess.
 */
@Service
@RequiredArgsConstructor
public class InstanceService {

    private final WorkflowDefinitionRepository definitionRepository;
    private final InstanceSummaryViewRepository instanceSummaryViewRepository;
    private final StepExecutionViewRepository stepExecutionViewRepository;
    private final AuditLogViewRepository auditLogViewRepository;
    private final DefinitionParser definitionParser;
    private final DefinitionValidator definitionValidator;
    private final ApiEventProducer eventProducer;
    private final ObjectMapper jsonMapper;

    public DefinitionSummaryResponse submitDefinition(String name, String rawBody) {
        WorkflowDefinitionSpec spec = definitionParser.parse(rawBody);
        definitionValidator.validate(spec);

        int nextVersion = definitionRepository.findTopByNameOrderByVersionDesc(name)
                .map(e -> e.getVersion() + 1)
                .orElse(1);

        WorkflowDefinitionEntity entity = WorkflowDefinitionEntity.builder()
                .name(name)
                .version(nextVersion)
                .body(rawBody)
                .createdAt(Instant.now())
                .build();
        entity = definitionRepository.save(entity);

        eventProducer.publishDefinition(entity.getId(), entity.getName(), entity.getVersion(), entity.getBody(), entity.getCreatedAt());

        return new DefinitionSummaryResponse(entity.getId(), entity.getName(), entity.getVersion(), entity.getCreatedAt());
    }

    public List<DefinitionSummaryResponse> listDefinitions() {
        return definitionRepository.findAllByOrderByNameAscVersionDesc().stream()
                .map(e -> new DefinitionSummaryResponse(e.getId(), e.getName(), e.getVersion(), e.getCreatedAt()))
                .toList();
    }

    public InstanceSummaryResponse startInstance(String definitionName, Integer version, Map<String, Object> payload) {
        WorkflowDefinitionEntity definition = (version != null
                ? definitionRepository.findByNameAndVersion(definitionName, version)
                : definitionRepository.findTopByNameOrderByVersionDesc(definitionName))
                .orElseThrow(() -> new NoSuchElementException("No such workflow definition: " + definitionName));

        WorkflowDefinitionSpec spec = definitionParser.parse(definition.getBody());
        Instant now = Instant.now();
        UUID instanceId = UUID.randomUUID();
        Map<String, Object> effectivePayload = payload == null ? Map.of() : payload;

        // Optimistic local write: id is assigned here (not by a database) precisely so this row
        // can exist before workflow-service's authoritative one does — see InstanceSummaryView.
        InstanceSummaryView view = InstanceSummaryView.builder()
                .id(instanceId)
                .workflowDefinitionId(definition.getId())
                .definitionName(definition.getName())
                .definitionVersion(definition.getVersion())
                .status(InstanceStatus.RUNNING)
                .currentStepId(spec.start())
                .createdAt(now)
                .updatedAt(now)
                .build();
        instanceSummaryViewRepository.save(view);

        eventProducer.requestLifecycle(instanceId, definition.getId(), spec.start(), InstanceLifecycleRequested.Reason.STARTED,
                definition.getName(), definition.getVersion(), effectivePayload);

        return toSummary(view);
    }

    public List<InstanceSummaryResponse> listInstances() {
        return instanceSummaryViewRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(this::toSummary)
                .toList();
    }

    public InstanceDetailResponse getInstanceDetail(UUID id) {
        InstanceSummaryView view = instanceSummaryViewRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("No such instance: " + id));

        List<StepExecutionResponse> steps = stepExecutionViewRepository.findByInstanceIdOrderByStartedAtAsc(id).stream()
                .map(e -> new StepExecutionResponse(e.getStepId(), e.getStepName(), e.getStatus().name(), e.getAttempt(),
                        e.getStartedAt(), e.getFinishedAt(), e.getErrorMessage()))
                .toList();

        List<AuditEntryResponse> audit = auditLogViewRepository.findByInstanceIdOrderByTimestampAsc(id).stream()
                .map(e -> new AuditEntryResponse(e.getEventType(), e.getDetail(), e.getTimestamp()))
                .toList();

        return new InstanceDetailResponse(toSummary(view), steps, audit);
    }

    public void approveInstance(UUID id) {
        InstanceSummaryView view = instanceSummaryViewRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("No such instance: " + id));

        if (view.getStatus() != InstanceStatus.WAITING_APPROVAL) {
            throw new IllegalStateException("Instance is not waiting for approval: " + id);
        }

        WorkflowDefinitionEntity definition = definitionRepository.findById(view.getWorkflowDefinitionId())
                .orElseThrow(() -> new NoSuchElementException("Missing definition for instance: " + id));
        WorkflowDefinitionSpec spec = definitionParser.parse(definition.getBody());

        StepSpec step = spec.getStep(view.getCurrentStepId());
        Instant now = Instant.now();

        stepExecutionViewRepository.save(StepExecutionView.builder()
                .instanceId(id)
                .stepId(step.id())
                .stepName(step.name())
                .status(StepExecutionStatus.SUCCEEDED)
                .attempt(1)
                .output(writeJson(Map.of("approved", true)))
                .startedAt(now)
                .finishedAt(now)
                .build());

        auditLogViewRepository.save(AuditLogView.builder()
                .instanceId(id)
                .eventType("STEP_APPROVED")
                .detail("Manually approved step: " + step.name())
                .timestamp(now)
                .build());

        if (step.next() == null) {
            view.setStatus(InstanceStatus.COMPLETED);
            view.setUpdatedAt(now);
            instanceSummaryViewRepository.save(view);
            auditLogViewRepository.save(AuditLogView.builder()
                    .instanceId(id)
                    .eventType("INSTANCE_COMPLETED")
                    .detail("Workflow instance completed")
                    .timestamp(now)
                    .build());
        } else {
            // Optimistic, same as startInstance — workflow-service is the source of truth from
            // here on and will confirm or correct this via the next InstanceStateChanged event.
            view.setCurrentStepId(step.next());
            view.setStatus(InstanceStatus.RUNNING);
            view.setUpdatedAt(now);
            instanceSummaryViewRepository.save(view);
            eventProducer.requestLifecycle(id, view.getWorkflowDefinitionId(), step.next(), InstanceLifecycleRequested.Reason.APPROVED,
                    view.getDefinitionName(), view.getDefinitionVersion(), Map.of());
        }
    }

    private InstanceSummaryResponse toSummary(InstanceSummaryView view) {
        return new InstanceSummaryResponse(view.getId(), view.getDefinitionName(), view.getDefinitionVersion(),
                view.getStatus().name(), view.getCurrentStepId(), view.getCreatedAt(), view.getUpdatedAt());
    }

    private String writeJson(Map<String, Object> value) {
        try {
            return jsonMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize payload", e);
        }
    }
}

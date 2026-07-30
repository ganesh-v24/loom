package com.workflowengine.engine;

import com.workflowengine.events.CompensationCompleted;
import com.workflowengine.events.NotificationRequested;
import com.workflowengine.events.StepExecutionCompleted;
import com.workflowengine.events.StepExecutionRecorded;
import com.workflowengine.events.StepExecutionRequested;
import com.workflowengine.events.Topics;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class WorkflowEventProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public void requestStepExecution(UUID instanceId, UUID definitionId, String stepId, int attempt) {
        kafkaTemplate.send(Topics.STEP_EXECUTE, instanceId.toString(),
                new StepExecutionRequested(instanceId, definitionId, stepId, attempt));
    }

    public void publishStepCompleted(UUID instanceId, UUID definitionId, String stepId, int attempt,
                                      boolean success, Map<String, Object> output, String errorMessage) {
        kafkaTemplate.send(Topics.STEP_COMPLETED, instanceId.toString(),
                new StepExecutionCompleted(instanceId, definitionId, stepId, attempt, success, output, errorMessage));
    }

    /** Business-level dead letter: a step exhausted its own retry policy. Distinct from Kafka's infra-level DLT. */
    public void publishToDlq(StepExecutionRequested exhaustedRequest, String errorMessage) {
        kafkaTemplate.send(Topics.STEP_DLQ, exhaustedRequest.instanceId().toString(),
                new StepExecutionCompleted(exhaustedRequest.instanceId(), exhaustedRequest.definitionId(),
                        exhaustedRequest.stepId(), exhaustedRequest.attempt(), false, Map.of(), errorMessage));
    }

    public void publishNotificationRequested(UUID instanceId, UUID definitionId, String stepId, String stepName,
                                              int attempt, Map<String, Object> payload) {
        kafkaTemplate.send(Topics.NOTIFICATION_REQUESTED, instanceId.toString(),
                new NotificationRequested(instanceId, definitionId, stepId, stepName, attempt, payload));
    }

    public void publishCompensationCompleted(UUID instanceId, UUID definitionId, String stepId,
                                              boolean success, String errorMessage) {
        kafkaTemplate.send(Topics.COMPENSATION_COMPLETED, instanceId.toString(),
                new CompensationCompleted(instanceId, definitionId, stepId, success, errorMessage));
    }

    /** Feeds api-service's read-model StepExecutionView — this service can no longer be queried directly. */
    public void publishStepExecutionRecorded(UUID instanceId, String stepId, String stepName, String status,
                                              int attempt, String input, String output, String errorMessage,
                                              Instant startedAt, Instant finishedAt) {
        kafkaTemplate.send(Topics.STEP_EXECUTION_RECORDED, instanceId.toString(),
                new StepExecutionRecorded(instanceId, stepId, stepName, status, attempt, input, output, errorMessage,
                        startedAt, finishedAt));
    }
}

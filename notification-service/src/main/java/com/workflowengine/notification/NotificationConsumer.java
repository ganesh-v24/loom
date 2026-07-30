package com.workflowengine.notification;

import com.workflowengine.events.NotificationRequested;
import com.workflowengine.events.StepExecutionCompleted;
import com.workflowengine.events.StepExecutionRecorded;
import com.workflowengine.events.Topics;
import com.workflowengine.persistence.StepExecutionEntity;
import com.workflowengine.persistence.StepExecutionRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * Consumes EMAIL_NOTIFY step requests independently of the general worker pool — its own
 * consumer group, its own StepExecutionEntity rows, its own producer call back onto
 * workflow.step.completed. From workflow-service's point of view it's just another producer on a
 * topic it already consumes; it never knows this ran in a different service than worker-service.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final StepExecutionRepository stepExecutionRepository;
    private final EmailService emailService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @KafkaListener(topics = Topics.NOTIFICATION_REQUESTED, concurrency = "2")
    public void onNotificationRequested(NotificationRequested request) {
        if (stepExecutionRepository.existsByInstanceIdAndStepIdAndAttemptAndStatus(
                request.instanceId(), request.stepId(), request.attempt(), StepExecutionStatus.SUCCEEDED)) {
            log.debug("Ignoring duplicate notification-requested event for instance {} step {} attempt {}",
                    request.instanceId(), request.stepId(), request.attempt());
            return;
        }

        Instant now = Instant.now();
        StepExecutionEntity execution = StepExecutionEntity.builder()
                .instanceId(request.instanceId())
                .stepId(request.stepId())
                .stepName(request.stepName())
                .status(StepExecutionStatus.RUNNING)
                .attempt(request.attempt())
                .startedAt(now)
                .build();

        emailService.send(request);

        execution.setStatus(StepExecutionStatus.SUCCEEDED);
        execution.setFinishedAt(Instant.now());
        stepExecutionRepository.save(execution);

        kafkaTemplate.send(Topics.STEP_COMPLETED, request.instanceId().toString(),
                new StepExecutionCompleted(request.instanceId(), request.definitionId(), request.stepId(),
                        request.attempt(), true, Map.of("emailSent", true), null));

        // Feeds api-service's read-model StepExecutionView — this service can no longer be queried directly.
        kafkaTemplate.send(Topics.STEP_EXECUTION_RECORDED, request.instanceId().toString(),
                new StepExecutionRecorded(request.instanceId(), request.stepId(), request.stepName(),
                        execution.getStatus().name(), request.attempt(), null, null, null,
                        execution.getStartedAt(), execution.getFinishedAt()));
    }
}

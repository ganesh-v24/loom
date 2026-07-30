package com.workflowengine.api;

import com.workflowengine.events.InstanceLifecycleRequested;
import com.workflowengine.events.Topics;
import com.workflowengine.events.WorkflowDefinitionPublished;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The two things api-service ever needs another service to react to: a definition was published
 * (workflow-service/worker-service each keep a local replica), or an instance needs its next step
 * decided (workflow-service owns that decision).
 */
@Component
@RequiredArgsConstructor
public class ApiEventProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public void requestLifecycle(UUID instanceId, UUID definitionId, String stepId, InstanceLifecycleRequested.Reason reason,
                                  String definitionName, int definitionVersion, Map<String, Object> payload) {
        kafkaTemplate.send(Topics.INSTANCE_LIFECYCLE, instanceId.toString(),
                new InstanceLifecycleRequested(instanceId, definitionId, stepId, reason, definitionName, definitionVersion, payload));
    }

    public void publishDefinition(UUID id, String name, int version, String body, Instant createdAt) {
        kafkaTemplate.send(Topics.DEFINITION_PUBLISHED, id.toString(),
                new WorkflowDefinitionPublished(id, name, version, body, createdAt));
    }
}

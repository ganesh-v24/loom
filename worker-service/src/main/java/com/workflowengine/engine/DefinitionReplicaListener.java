package com.workflowengine.engine;

import com.workflowengine.events.Topics;
import com.workflowengine.events.WorkflowDefinitionPublished;
import com.workflowengine.persistence.WorkflowDefinitionEntity;
import com.workflowengine.persistence.WorkflowDefinitionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Keeps a local, read-only copy of every definition api-service publishes — this service can no
 * longer query api-service's table directly now that each service has its own database.
 */
@Component
@RequiredArgsConstructor
public class DefinitionReplicaListener {

    private final WorkflowDefinitionRepository definitionRepository;

    @KafkaListener(topics = Topics.DEFINITION_PUBLISHED)
    public void onDefinitionPublished(WorkflowDefinitionPublished event) {
        if (definitionRepository.existsById(event.id())) {
            return;
        }
        definitionRepository.save(WorkflowDefinitionEntity.builder()
                .id(event.id())
                .name(event.name())
                .version(event.version())
                .body(event.body())
                .createdAt(event.createdAt())
                .build());
    }
}

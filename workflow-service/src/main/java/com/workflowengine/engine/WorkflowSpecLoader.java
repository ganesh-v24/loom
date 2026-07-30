package com.workflowengine.engine;

import com.workflowengine.definition.DefinitionParser;
import com.workflowengine.definition.WorkflowDefinitionSpec;
import com.workflowengine.persistence.WorkflowDefinitionEntity;
import com.workflowengine.persistence.WorkflowDefinitionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Parses and caches {@link WorkflowDefinitionSpec} by definition id, shared by the worker and coordinator. */
@Component
@RequiredArgsConstructor
public class WorkflowSpecLoader {

    private final WorkflowDefinitionRepository definitionRepository;
    private final DefinitionParser definitionParser;

    private final Map<UUID, WorkflowDefinitionSpec> cache = new ConcurrentHashMap<>();

    public WorkflowDefinitionSpec load(UUID definitionId) {
        return cache.computeIfAbsent(definitionId, id -> {
            WorkflowDefinitionEntity entity = definitionRepository.findById(id)
                    .orElseThrow(() -> new IllegalStateException("Missing workflow definition: " + id));
            return definitionParser.parse(entity.getBody());
        });
    }
}

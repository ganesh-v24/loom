package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/** Read-only here: workflow-service only ever looks up a definition by id to parse its spec. */
public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinitionEntity, UUID> {
}

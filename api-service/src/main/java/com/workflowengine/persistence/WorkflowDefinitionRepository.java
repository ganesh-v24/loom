package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface WorkflowDefinitionRepository extends JpaRepository<WorkflowDefinitionEntity, UUID> {

    Optional<WorkflowDefinitionEntity> findTopByNameOrderByVersionDesc(String name);

    Optional<WorkflowDefinitionEntity> findByNameAndVersion(String name, int version);

    List<WorkflowDefinitionEntity> findAllByOrderByNameAscVersionDesc();
}

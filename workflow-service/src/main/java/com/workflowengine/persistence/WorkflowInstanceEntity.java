package com.workflowengine.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * {@code id} is application-assigned (no {@code @GeneratedValue}), not database-generated: since
 * Phase 4 api-service must return the instance id in its HTTP response before this row exists
 * (workflow-service creates it asynchronously), the id has to exist before any database sees it.
 */
@Entity
@Table(name = "workflow_instances")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WorkflowInstanceEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID workflowDefinitionId;

    @Column(nullable = false)
    private String definitionName;

    @Column(nullable = false)
    private int definitionVersion;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InstanceStatus status;

    @Column(nullable = false)
    private String currentStepId;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;
}

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
 * A read model, not a source of truth — workflow-service owns instance state since Phase 4; this
 * is api-service's local, event-populated copy for GET/list/dashboard, kept in sync by consuming
 * InstanceStateChanged. {@code id} matches the id api-service itself assigned when starting the
 * instance (see InstanceService.startInstance), written here optimistically before the
 * authoritative row exists anywhere, then reconciled by the first InstanceStateChanged event.
 */
@Entity
@Table(name = "instance_summary_view")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InstanceSummaryView {

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

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;
}

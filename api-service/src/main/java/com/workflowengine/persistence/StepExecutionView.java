package com.workflowengine.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;
import java.util.UUID;

/**
 * A read model aggregating three producers (worker-service and notification-service via
 * StepExecutionRecorded, plus api-service's own direct writes for approval steps) into one
 * displayable step history — this is the whole point of a CQRS read side: the detail view needs
 * data no single write-side service owns end to end. {@code id} is a generated surrogate key;
 * upserts happen by looking up (instanceId, stepId, attempt) first, see ReadModelUpdater.
 */
@Entity
@Table(name = "step_execution_view")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StepExecutionView {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(nullable = false)
    private UUID instanceId;

    @Column(nullable = false)
    private String stepId;

    @Column(nullable = false)
    private String stepName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private StepExecutionStatus status;

    @Column(nullable = false)
    private int attempt;

    @Column(columnDefinition = "TEXT")
    private String input;

    @Column(columnDefinition = "TEXT")
    private String output;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @Column(nullable = false)
    private Instant startedAt;

    private Instant finishedAt;
}

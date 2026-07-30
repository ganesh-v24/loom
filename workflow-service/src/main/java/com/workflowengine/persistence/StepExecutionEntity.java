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

@Entity
@Table(name = "step_executions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StepExecutionEntity {

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

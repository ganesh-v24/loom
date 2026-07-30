package com.workflowengine.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
 * A read model — since Phase 4, workflow-service keeps no local audit table of its own at all
 * (nobody read it once tables stopped being shared); this is now the only durable audit history,
 * populated by api-service's own direct writes plus consuming AuditEventOccurred.
 */
@Entity
@Table(name = "audit_log_view")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuditLogView {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(nullable = false)
    private UUID instanceId;

    @Column(nullable = false)
    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String detail;

    @Column(nullable = false)
    private Instant timestamp;
}

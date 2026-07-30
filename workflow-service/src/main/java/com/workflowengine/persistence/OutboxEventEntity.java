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
 * Transactional outbox: a row here is inserted in the SAME database transaction as whatever
 * instance-table write it accompanies, so "did we durably record the intent to publish" is
 * atomic with the write itself. A separate relay (OutboxRelay) polls unpublished rows and sends
 * them to Kafka independently, retrying until it succeeds — decoupling "committed to our own
 * database" (safe, transactional) from "made it onto Kafka" (best-effort, eventually consistent).
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEventEntity {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private String messageKey;

    /**
     * Fully-qualified class name of the original event record — replayed as the __TypeId__
     * Kafka header so JsonDeserializer on the consumer side can still resolve the type. The
     * relay sends via a plain String producer (see OutboxRelay), which unlike JsonSerializer
     * doesn't add that header on its own, so it has to be restored explicitly.
     */
    @Column(nullable = false)
    private String payloadType;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant publishedAt;
}

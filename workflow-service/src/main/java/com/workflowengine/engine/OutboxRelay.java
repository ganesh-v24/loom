package com.workflowengine.engine;

import com.workflowengine.persistence.OutboxEventEntity;
import com.workflowengine.persistence.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Polls unpublished outbox rows and sends each to Kafka, marking it published on success and
 * leaving it for the next poll on failure — retries indefinitely rather than losing the event.
 *
 * Uses a plain String-keyed/String-valued KafkaTemplate because the payload is already-serialized
 * JSON text: sending it through the JsonSerializer-configured template used elsewhere in this
 * codebase would serialize the JSON string a second time, double-encoding it.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> outboxKafkaTemplate;

    @Scheduled(fixedDelay = 500)
    public void relay() {
        List<OutboxEventEntity> pending = outboxRepository.findByPublishedAtIsNullOrderByCreatedAtAsc();
        for (OutboxEventEntity event : pending) {
            try {
                outboxKafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload()).get();
                event.setPublishedAt(Instant.now());
                outboxRepository.save(event);
            } catch (Exception e) {
                log.warn("Failed to relay outbox event {} to topic {}, will retry", event.getId(), event.getTopic(), e);
            }
        }
    }
}

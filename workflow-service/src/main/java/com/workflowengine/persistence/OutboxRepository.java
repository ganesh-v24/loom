package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByPublishedAtIsNullOrderByCreatedAtAsc();
}

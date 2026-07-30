package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AuditLogViewRepository extends JpaRepository<AuditLogView, UUID> {

    List<AuditLogView> findByInstanceIdOrderByTimestampAsc(UUID instanceId);
}

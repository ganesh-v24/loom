package com.workflowengine.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InstanceSummaryViewRepository extends JpaRepository<InstanceSummaryView, UUID> {

    List<InstanceSummaryView> findAllByOrderByCreatedAtDesc();
}

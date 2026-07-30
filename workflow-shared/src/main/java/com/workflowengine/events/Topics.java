package com.workflowengine.events;

/** Kafka topic names shared by every service — the one place they all must agree on. */
public final class Topics {

    private Topics() {
    }

    public static final String INSTANCE_LIFECYCLE = "workflow.instance.lifecycle";
    public static final String STEP_EXECUTE = "workflow.step.execute";
    public static final String STEP_COMPLETED = "workflow.step.completed";
    public static final String STEP_DLQ = "workflow.step.dlq";
    public static final String NOTIFICATION_REQUESTED = "workflow.notification.requested";
    public static final String COMPENSATION_REQUESTED = "workflow.step.compensate";
    public static final String COMPENSATION_COMPLETED = "workflow.step.compensated";

    // Phase 4 — CQRS read-model events (database-per-service)
    public static final String DEFINITION_PUBLISHED = "workflow.definition.published";
    public static final String INSTANCE_STATE_CHANGED = "workflow.instance.state-changed";
    public static final String STEP_EXECUTION_RECORDED = "workflow.step.recorded";
    public static final String AUDIT_EVENT_OCCURRED = "workflow.audit.occurred";
}

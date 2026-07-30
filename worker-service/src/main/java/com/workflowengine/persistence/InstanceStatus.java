package com.workflowengine.persistence;

public enum InstanceStatus {
    RUNNING,
    WAITING_APPROVAL,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED
}

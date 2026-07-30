package com.workflowengine.definition;

public record RetrySpec(int maxAttempts, long backoffMs) {
    public static final RetrySpec DEFAULT = new RetrySpec(1, 0);
}

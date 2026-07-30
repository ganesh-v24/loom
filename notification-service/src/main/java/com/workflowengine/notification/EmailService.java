package com.workflowengine.notification;

import com.workflowengine.events.NotificationRequested;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Simulated — logs instead of sending real email, same as Phase 1/2, just relocated to its own service. */
@Slf4j
@Component
public class EmailService {

    public void send(NotificationRequested request) {
        log.info("Emailing customer for workflow instance {} (step {})", request.instanceId(), request.stepId());
    }
}

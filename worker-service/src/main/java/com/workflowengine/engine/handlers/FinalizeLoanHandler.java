package com.workflowengine.engine.handlers;

import com.workflowengine.engine.BusinessStepHandler;
import com.workflowengine.engine.StepContext;
import com.workflowengine.engine.StepResult;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Demo trigger for the Saga path: fails immediately (no retries — see its RetrySpec in the YAML)
 * when the instance's payload carries {@code simulateFailure: true}, otherwise succeeds. Lets one
 * definition demonstrate both the happy path and compensation without two separate workflows.
 */
@Component("finalizeLoan")
public class FinalizeLoanHandler implements BusinessStepHandler {

    @Override
    public StepResult execute(StepContext context) {
        if (Boolean.TRUE.equals(context.payload().get("simulateFailure"))) {
            return StepResult.failure("Simulated post-disbursement failure (payload.simulateFailure=true)");
        }
        return StepResult.ok(Map.of("finalized", true));
    }
}

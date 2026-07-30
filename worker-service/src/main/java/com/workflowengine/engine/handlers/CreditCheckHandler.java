package com.workflowengine.engine.handlers;

import com.workflowengine.engine.BusinessStepHandler;
import com.workflowengine.engine.StepContext;
import com.workflowengine.engine.StepResult;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Simulates a flaky external credit bureau call: fails on the first attempt so the
 * demo workflow visibly exercises the retry policy before succeeding.
 */
@Component("creditCheck")
public class CreditCheckHandler implements BusinessStepHandler {

    @Override
    public StepResult execute(StepContext context) {
        if (context.attempt() < 2) {
            return StepResult.failure("Credit bureau timeout (simulated transient failure)");
        }
        return StepResult.ok(Map.of("creditScore", 720));
    }
}

package com.workflowengine.engine.handlers;

import com.workflowengine.engine.BusinessStepHandler;
import com.workflowengine.engine.StepContext;
import com.workflowengine.engine.StepResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/** Saga compensation for {@code disbursement}: simulates clawing back the disbursed funds. */
@Slf4j
@Component("reverseDisbursement")
public class ReverseDisbursementHandler implements BusinessStepHandler {

    @Override
    public StepResult execute(StepContext context) {
        log.info("Reversing disbursement for instance {}", context.instanceId());
        return StepResult.ok(Map.of("disbursed", false, "reversed", true));
    }
}

package com.workflowengine.engine.handlers;

import com.workflowengine.engine.BusinessStepHandler;
import com.workflowengine.engine.StepContext;
import com.workflowengine.engine.StepResult;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("disbursement")
public class DisbursementHandler implements BusinessStepHandler {

    @Override
    public StepResult execute(StepContext context) {
        return StepResult.ok(Map.of("disbursed", true));
    }
}

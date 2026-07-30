package com.workflowengine.engine.handlers;

import com.workflowengine.engine.BusinessStepHandler;
import com.workflowengine.engine.StepContext;
import com.workflowengine.engine.StepResult;
import org.springframework.stereotype.Component;

@Component("submitLoan")
public class SubmitLoanHandler implements BusinessStepHandler {

    @Override
    public StepResult execute(StepContext context) {
        return StepResult.ok(context.payload());
    }
}

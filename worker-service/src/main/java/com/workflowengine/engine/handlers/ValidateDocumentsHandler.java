package com.workflowengine.engine.handlers;

import com.workflowengine.engine.BusinessStepHandler;
import com.workflowengine.engine.StepContext;
import com.workflowengine.engine.StepResult;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("validateDocuments")
public class ValidateDocumentsHandler implements BusinessStepHandler {

    @Override
    public StepResult execute(StepContext context) {
        return StepResult.ok(Map.of("documentsValid", true));
    }
}

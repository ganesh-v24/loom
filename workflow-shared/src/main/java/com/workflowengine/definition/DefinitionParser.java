package com.workflowengine.definition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.stereotype.Component;

@Component
public class DefinitionParser {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    public WorkflowDefinitionSpec parse(String body) {
        try {
            return mapper.readValue(body, WorkflowDefinitionSpec.class);
        } catch (Exception e) {
            throw new DefinitionValidationException("Failed to parse workflow definition: " + e.getMessage());
        }
    }
}

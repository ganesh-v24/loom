package com.workflowengine.api;

import com.workflowengine.api.dto.DefinitionSummaryResponse;
import com.workflowengine.api.dto.SubmitDefinitionRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/workflow-definitions")
@RequiredArgsConstructor
public class WorkflowDefinitionController {

    private final InstanceService instanceService;

    @PostMapping
    public ResponseEntity<DefinitionSummaryResponse> submit(@Valid @RequestBody SubmitDefinitionRequest request) {
        DefinitionSummaryResponse response = instanceService.submitDefinition(request.name(), request.body());
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping
    public List<DefinitionSummaryResponse> list() {
        return instanceService.listDefinitions();
    }
}

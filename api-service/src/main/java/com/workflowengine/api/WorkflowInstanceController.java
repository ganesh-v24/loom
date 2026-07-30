package com.workflowengine.api;

import com.workflowengine.api.dto.InstanceDetailResponse;
import com.workflowengine.api.dto.InstanceSummaryResponse;
import com.workflowengine.api.dto.StartInstanceRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/instances")
@RequiredArgsConstructor
public class WorkflowInstanceController {

    private final InstanceService instanceService;

    @PostMapping
    public ResponseEntity<InstanceSummaryResponse> start(@Valid @RequestBody StartInstanceRequest request) {
        InstanceSummaryResponse response = instanceService.startInstance(
                request.definitionName(), request.definitionVersion(), request.payload());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping
    public List<InstanceSummaryResponse> list() {
        return instanceService.listInstances();
    }

    @GetMapping("/{id}")
    public InstanceDetailResponse detail(@PathVariable UUID id) {
        return instanceService.getInstanceDetail(id);
    }

    @PostMapping("/{id}/approve")
    public InstanceDetailResponse approve(@PathVariable UUID id) {
        instanceService.approveInstance(id);
        return instanceService.getInstanceDetail(id);
    }
}

package com.workflowengine.dashboard;

import com.workflowengine.api.InstanceService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.UUID;

@Controller
@RequiredArgsConstructor
public class DashboardController {

    private final InstanceService instanceService;

    @GetMapping("/")
    public String listInstances(Model model) {
        model.addAttribute("instances", instanceService.listInstances());
        return "instances";
    }

    @GetMapping("/instances/{id}")
    public String instanceDetail(@PathVariable UUID id, Model model) {
        model.addAttribute("detail", instanceService.getInstanceDetail(id));
        return "instance-detail";
    }

    @PostMapping("/instances/{id}/approve")
    public String approve(@PathVariable UUID id) {
        instanceService.approveInstance(id);
        return "redirect:/instances/" + id;
    }
}

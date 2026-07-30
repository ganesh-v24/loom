package com.workflowengine.api.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record StartInstanceRequest(@NotBlank String definitionName, Integer definitionVersion, Map<String, Object> payload) {
}

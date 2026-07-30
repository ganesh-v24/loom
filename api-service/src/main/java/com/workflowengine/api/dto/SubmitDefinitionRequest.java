package com.workflowengine.api.dto;

import jakarta.validation.constraints.NotBlank;

public record SubmitDefinitionRequest(@NotBlank String name, @NotBlank String body) {
}

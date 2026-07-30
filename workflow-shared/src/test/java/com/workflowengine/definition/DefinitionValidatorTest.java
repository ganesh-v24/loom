package com.workflowengine.definition;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefinitionValidatorTest {

    private final DefinitionValidator validator = new DefinitionValidator();

    @Test
    void validSpecPasses() {
        WorkflowDefinitionSpec spec = new WorkflowDefinitionSpec("demo", "a", List.of(
                new StepSpec("a", "A", StepType.AUTOMATIC, "handlerA", "b", null, null),
                new StepSpec("b", "B", StepType.AUTOMATIC, "handlerB", null, null, null)
        ));

        assertThatCode(() -> validator.validate(spec)).doesNotThrowAnyException();
    }

    @Test
    void unknownStartIsRejected() {
        WorkflowDefinitionSpec spec = new WorkflowDefinitionSpec("demo", "missing", List.of(
                new StepSpec("a", "A", StepType.AUTOMATIC, "handlerA", null, null, null)
        ));

        assertThatThrownBy(() -> validator.validate(spec)).isInstanceOf(DefinitionValidationException.class);
    }

    @Test
    void danglingNextIsRejected() {
        WorkflowDefinitionSpec spec = new WorkflowDefinitionSpec("demo", "a", List.of(
                new StepSpec("a", "A", StepType.AUTOMATIC, "handlerA", "missing", null, null)
        ));

        assertThatThrownBy(() -> validator.validate(spec)).isInstanceOf(DefinitionValidationException.class);
    }

    @Test
    void cycleIsRejected() {
        WorkflowDefinitionSpec spec = new WorkflowDefinitionSpec("demo", "a", List.of(
                new StepSpec("a", "A", StepType.AUTOMATIC, "handlerA", "b", null, null),
                new StepSpec("b", "B", StepType.AUTOMATIC, "handlerB", "a", null, null)
        ));

        assertThatThrownBy(() -> validator.validate(spec)).isInstanceOf(DefinitionValidationException.class);
    }

    @Test
    void automaticStepWithoutHandlerIsRejected() {
        WorkflowDefinitionSpec spec = new WorkflowDefinitionSpec("demo", "a", List.of(
                new StepSpec("a", "A", StepType.AUTOMATIC, null, null, null, null)
        ));

        assertThatThrownBy(() -> validator.validate(spec)).isInstanceOf(DefinitionValidationException.class);
    }

    @Test
    void duplicateStepIdIsRejected() {
        WorkflowDefinitionSpec spec = new WorkflowDefinitionSpec("demo", "a", List.of(
                new StepSpec("a", "A", StepType.AUTOMATIC, "handlerA", null, null, null),
                new StepSpec("a", "A2", StepType.AUTOMATIC, "handlerA2", null, null, null)
        ));

        assertThatThrownBy(() -> validator.validate(spec)).isInstanceOf(DefinitionValidationException.class);
    }
}

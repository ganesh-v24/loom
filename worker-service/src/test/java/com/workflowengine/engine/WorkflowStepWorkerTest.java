package com.workflowengine.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.workflowengine.definition.RetrySpec;
import com.workflowengine.definition.StepSpec;
import com.workflowengine.definition.StepType;
import com.workflowengine.definition.WorkflowDefinitionSpec;
import com.workflowengine.events.StepExecutionRequested;
import com.workflowengine.persistence.InstanceStatus;
import com.workflowengine.persistence.StepExecutionEntity;
import com.workflowengine.persistence.StepExecutionRepository;
import com.workflowengine.persistence.StepExecutionStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Retries happen by republishing to Kafka (attempt+1) rather than looping inside one call, so
 * each test drives {@code onStepExecutionRequested} once per attempt and asserts on what got
 * published, instead of asserting on a returned StepResult.
 */
class WorkflowStepWorkerTest {

    private final UUID instanceId = UUID.randomUUID();
    private final UUID definitionId = UUID.randomUUID();

    private WorkflowStepWorker newWorker(WorkflowInstanceRepository instanceRepository,
                                          StepExecutionRepository stepExecutionRepository,
                                          WorkflowSpecLoader specLoader,
                                          ApplicationContext applicationContext,
                                          WorkflowEventProducer eventProducer) {
        return new WorkflowStepWorker(instanceRepository, stepExecutionRepository, specLoader,
                applicationContext, eventProducer, new ObjectMapper());
    }

    private WorkflowInstanceEntity runningInstance(String currentStepId) {
        return WorkflowInstanceEntity.builder()
                .id(instanceId)
                .workflowDefinitionId(definitionId)
                .definitionName("demo")
                .definitionVersion(1)
                .status(InstanceStatus.RUNNING)
                .currentStepId(currentStepId)
                .payload("{}")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    private WorkflowDefinitionSpec specWithFlakyStep(RetrySpec retry) {
        StepSpec step = new StepSpec("flakyStep", "Flaky Step", StepType.AUTOMATIC, "flaky", null, retry, null);
        return new WorkflowDefinitionSpec("demo", "flakyStep", List.of(step));
    }

    @Test
    void republishesWithIncrementedAttemptWhenRetriesRemain() {
        WorkflowInstanceRepository instanceRepository = mock(WorkflowInstanceRepository.class);
        StepExecutionRepository stepExecutionRepository = mock(StepExecutionRepository.class);
        WorkflowSpecLoader specLoader = mock(WorkflowSpecLoader.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        WorkflowEventProducer eventProducer = mock(WorkflowEventProducer.class);

        when(instanceRepository.findById(instanceId)).thenReturn(Optional.of(runningInstance("flakyStep")));
        when(specLoader.load(definitionId)).thenReturn(specWithFlakyStep(new RetrySpec(3, 0)));
        when(applicationContext.getBean("flaky", BusinessStepHandler.class))
                .thenReturn(context -> StepResult.failure("boom"));
        when(stepExecutionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(stepExecutionRepository.findByInstanceIdOrderByStartedAtAsc(instanceId)).thenReturn(List.of());

        WorkflowStepWorker worker = newWorker(instanceRepository, stepExecutionRepository, specLoader, applicationContext, eventProducer);
        worker.onStepExecutionRequested(new StepExecutionRequested(instanceId, definitionId, "flakyStep", 1));

        verify(eventProducer).requestStepExecution(instanceId, definitionId, "flakyStep", 2);
        verify(eventProducer, never()).publishStepCompleted(any(), any(), any(), anyInt(), anyBoolean(), any(), any());
        verify(eventProducer, never()).publishToDlq(any(), any());
    }

    @Test
    void publishesDlqAndFailureCompletedAfterMaxAttempts() {
        WorkflowInstanceRepository instanceRepository = mock(WorkflowInstanceRepository.class);
        StepExecutionRepository stepExecutionRepository = mock(StepExecutionRepository.class);
        WorkflowSpecLoader specLoader = mock(WorkflowSpecLoader.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        WorkflowEventProducer eventProducer = mock(WorkflowEventProducer.class);

        when(instanceRepository.findById(instanceId)).thenReturn(Optional.of(runningInstance("flakyStep")));
        when(specLoader.load(definitionId)).thenReturn(specWithFlakyStep(new RetrySpec(3, 0)));
        when(applicationContext.getBean("flaky", BusinessStepHandler.class))
                .thenReturn(context -> StepResult.failure("boom"));
        when(stepExecutionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(stepExecutionRepository.findByInstanceIdOrderByStartedAtAsc(instanceId)).thenReturn(List.of());

        WorkflowStepWorker worker = newWorker(instanceRepository, stepExecutionRepository, specLoader, applicationContext, eventProducer);
        worker.onStepExecutionRequested(new StepExecutionRequested(instanceId, definitionId, "flakyStep", 3));

        verify(eventProducer).publishToDlq(any(), eq("boom"));
        verify(eventProducer).publishStepCompleted(eq(instanceId), eq(definitionId), eq("flakyStep"), eq(3), eq(false), any(), eq("boom"));
        verify(eventProducer, never()).requestStepExecution(any(), any(), any(), anyInt());
    }

    @Test
    void publishesCompletedSuccessOnHandlerSuccess() {
        WorkflowInstanceRepository instanceRepository = mock(WorkflowInstanceRepository.class);
        StepExecutionRepository stepExecutionRepository = mock(StepExecutionRepository.class);
        WorkflowSpecLoader specLoader = mock(WorkflowSpecLoader.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        WorkflowEventProducer eventProducer = mock(WorkflowEventProducer.class);

        when(instanceRepository.findById(instanceId)).thenReturn(Optional.of(runningInstance("flakyStep")));
        when(specLoader.load(definitionId)).thenReturn(specWithFlakyStep(new RetrySpec(3, 0)));
        when(applicationContext.getBean("flaky", BusinessStepHandler.class))
                .thenReturn(context -> StepResult.ok(Map.of("done", true)));
        when(stepExecutionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(stepExecutionRepository.findByInstanceIdOrderByStartedAtAsc(instanceId)).thenReturn(List.of());

        WorkflowStepWorker worker = newWorker(instanceRepository, stepExecutionRepository, specLoader, applicationContext, eventProducer);
        worker.onStepExecutionRequested(new StepExecutionRequested(instanceId, definitionId, "flakyStep", 1));

        verify(eventProducer).publishStepCompleted(eq(instanceId), eq(definitionId), eq("flakyStep"), eq(1), eq(true), any(), isNull());
        verify(eventProducer, never()).requestStepExecution(any(), any(), any(), anyInt());
        verify(eventProducer, never()).publishToDlq(any(), any());
    }

    @Test
    void ignoresEventWhenInstanceNotRunning() {
        WorkflowInstanceRepository instanceRepository = mock(WorkflowInstanceRepository.class);
        StepExecutionRepository stepExecutionRepository = mock(StepExecutionRepository.class);
        WorkflowSpecLoader specLoader = mock(WorkflowSpecLoader.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        WorkflowEventProducer eventProducer = mock(WorkflowEventProducer.class);

        WorkflowInstanceEntity completed = runningInstance("flakyStep");
        completed.setStatus(InstanceStatus.COMPLETED);
        when(instanceRepository.findById(instanceId)).thenReturn(Optional.of(completed));

        WorkflowStepWorker worker = newWorker(instanceRepository, stepExecutionRepository, specLoader, applicationContext, eventProducer);
        worker.onStepExecutionRequested(new StepExecutionRequested(instanceId, definitionId, "flakyStep", 1));

        verify(stepExecutionRepository, never()).save(any());
        verify(eventProducer, never()).publishStepCompleted(any(), any(), any(), anyInt(), anyBoolean(), any(), any());
    }

    @Test
    void ignoresEventWhenInstanceAlreadyPastThisStep() {
        WorkflowInstanceRepository instanceRepository = mock(WorkflowInstanceRepository.class);
        StepExecutionRepository stepExecutionRepository = mock(StepExecutionRepository.class);
        WorkflowSpecLoader specLoader = mock(WorkflowSpecLoader.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        WorkflowEventProducer eventProducer = mock(WorkflowEventProducer.class);

        when(instanceRepository.findById(instanceId)).thenReturn(Optional.of(runningInstance("someLaterStep")));

        WorkflowStepWorker worker = newWorker(instanceRepository, stepExecutionRepository, specLoader, applicationContext, eventProducer);
        worker.onStepExecutionRequested(new StepExecutionRequested(instanceId, definitionId, "flakyStep", 1));

        verify(stepExecutionRepository, never()).save(any());
    }

    @Test
    void ignoresDuplicateEventWhenAttemptAlreadySucceeded() {
        WorkflowInstanceRepository instanceRepository = mock(WorkflowInstanceRepository.class);
        StepExecutionRepository stepExecutionRepository = mock(StepExecutionRepository.class);
        WorkflowSpecLoader specLoader = mock(WorkflowSpecLoader.class);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        WorkflowEventProducer eventProducer = mock(WorkflowEventProducer.class);

        when(instanceRepository.findById(instanceId)).thenReturn(Optional.of(runningInstance("flakyStep")));
        when(specLoader.load(definitionId)).thenReturn(specWithFlakyStep(new RetrySpec(3, 0)));
        when(stepExecutionRepository.findByInstanceIdOrderByStartedAtAsc(instanceId)).thenReturn(List.of(
                StepExecutionEntity.builder()
                        .instanceId(instanceId)
                        .stepId("flakyStep")
                        .stepName("Flaky Step")
                        .status(StepExecutionStatus.SUCCEEDED)
                        .attempt(1)
                        .startedAt(Instant.now())
                        .finishedAt(Instant.now())
                        .build()));

        WorkflowStepWorker worker = newWorker(instanceRepository, stepExecutionRepository, specLoader, applicationContext, eventProducer);
        worker.onStepExecutionRequested(new StepExecutionRequested(instanceId, definitionId, "flakyStep", 1));

        verify(stepExecutionRepository, never()).save(any());
        verify(eventProducer, never()).publishStepCompleted(any(), any(), any(), anyInt(), anyBoolean(), any(), any());
    }
}

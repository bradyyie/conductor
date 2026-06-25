/*
 * Copyright 2026 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution.tasks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExclusiveJoinTest {

    private final ExclusiveJoin exclusiveJoin = new ExclusiveJoin();

    private static TaskModel task(String refName, String taskId, TaskModel.Status status) {
        TaskModel task = new TaskModel();
        task.setReferenceTaskName(refName);
        task.setTaskId(taskId);
        task.setStatus(status);
        task.setWorkflowTask(new WorkflowTask());
        return task;
    }

    /**
     * On completion the exclusive join must copy the FULL output of the joined task. The in-memory
     * TaskModel may only carry a partial/externalized payload, so the join reloads it via the
     * executor and uses that output (backported from OrkesExclusiveJoin).
     */
    @Test
    public void completesWithFullOutputReloadedFromExecutor() {
        TaskModel inMemoryJoinedTask = task("branchA", "branchA-id", TaskModel.Status.COMPLETED);
        inMemoryJoinedTask.setOutputData(new HashMap<>()); // not yet populated in memory

        WorkflowModel workflow = new WorkflowModel();
        workflow.setTasks(List.of(inMemoryJoinedTask));

        TaskModel exclusiveJoinTask = task("exclusiveJoin", "ej-id", TaskModel.Status.IN_PROGRESS);
        Map<String, Object> input = new HashMap<>();
        input.put("joinOn", List.of("branchA"));
        exclusiveJoinTask.setInputData(input);

        TaskModel reloaded = task("branchA", "branchA-id", TaskModel.Status.COMPLETED);
        Map<String, Object> fullOutput = new HashMap<>();
        fullOutput.put("result", 42);
        reloaded.setOutputData(fullOutput);

        WorkflowExecutor workflowExecutor = mock(WorkflowExecutor.class);
        when(workflowExecutor.getTask("branchA-id")).thenReturn(reloaded);

        boolean changed = exclusiveJoin.execute(workflow, exclusiveJoinTask, workflowExecutor);

        assertTrue(changed);
        assertEquals(TaskModel.Status.COMPLETED, exclusiveJoinTask.getStatus());
        assertEquals(fullOutput, exclusiveJoinTask.getOutputData());
        verify(workflowExecutor).getTask("branchA-id");
    }

    /** On failure the join must not reload output; it propagates the failure reason. */
    @Test
    public void failsWithoutReloadWhenJoinedTaskFailed() {
        TaskModel failedTask = task("branchA", "branchA-id", TaskModel.Status.FAILED);
        failedTask.setReasonForIncompletion("boom");

        WorkflowModel workflow = new WorkflowModel();
        workflow.setTasks(List.of(failedTask));

        TaskModel exclusiveJoinTask = task("exclusiveJoin", "ej-id", TaskModel.Status.IN_PROGRESS);
        Map<String, Object> input = new HashMap<>();
        input.put("joinOn", List.of("branchA"));
        exclusiveJoinTask.setInputData(input);

        WorkflowExecutor workflowExecutor = mock(WorkflowExecutor.class);

        boolean changed = exclusiveJoin.execute(workflow, exclusiveJoinTask, workflowExecutor);

        assertTrue(changed);
        assertEquals(TaskModel.Status.FAILED, exclusiveJoinTask.getStatus());
        assertTrue(exclusiveJoinTask.getReasonForIncompletion().contains("boom"));
        verify(workflowExecutor, never()).getTask(anyString());
    }
}

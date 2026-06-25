/*
 * Copyright 2022 Conductor Authors.
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
import java.util.Optional;

import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JoinTest {

    private static TaskModel taskWithJoinMode(WorkflowTask.JoinMode mode) {
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setJoinMode(mode);
        TaskModel task = new TaskModel();
        task.setWorkflowTask(workflowTask);
        return task;
    }

    @Test
    public void testSynchronousJoinModeEvaluationOffset() {
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getSystemTaskPostponeThreshold()).thenReturn(200);

        Join join = new Join(properties);
        TaskModel task = taskWithJoinMode(WorkflowTask.JoinMode.SYNC);

        // Synchronous mode should always return 0 offset
        task.setPollCount(100);
        Optional<Long> offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertEquals(0L, offset.get().longValue());

        // Even with high poll count, SYNC mode returns 0
        task.setPollCount(500);
        offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertEquals(0L, offset.get().longValue());
    }

    @Test
    public void testAsynchronousJoinModeEvaluationOffset() {
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getSystemTaskPostponeThreshold()).thenReturn(200);

        Join join = new Join(properties);
        TaskModel task = taskWithJoinMode(WorkflowTask.JoinMode.ASYNC);

        // Low poll count should return 0
        task.setPollCount(100);
        Optional<Long> offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertEquals(0L, offset.get().longValue());

        // High poll count should use exponential backoff
        task.setPollCount(250);
        offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertTrue(offset.get() > 0L);
    }

    @Test
    public void testDefaultAsyncBehavior() {
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getSystemTaskPostponeThreshold()).thenReturn(200);

        Join join = new Join(properties);

        // No joinMode on workflowTask — should default to async behavior
        TaskModel task = new TaskModel();
        task.setWorkflowTask(new WorkflowTask());

        // Low poll count should return 0
        task.setPollCount(100);
        Optional<Long> offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertEquals(0L, offset.get().longValue());

        // High poll count should use exponential backoff (default async behavior)
        task.setPollCount(250);
        offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertTrue(offset.get() > 0L);
    }

    @Test
    public void testNullWorkflowTaskDefaultsToAsync() {
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getSystemTaskPostponeThreshold()).thenReturn(200);

        Join join = new Join(properties);

        // No workflowTask at all — should default to async behavior
        TaskModel task = new TaskModel();

        task.setPollCount(250);
        Optional<Long> offset = join.getEvaluationOffset(task, 10000L);
        assertTrue(offset.isPresent());
        assertTrue(offset.get() > 0L);
    }

    @Test
    public void testIsAsync() {
        ConductorProperties properties = mock(ConductorProperties.class);
        Join join = new Join(properties);

        // isAsync should always return true
        assertTrue(join.isAsync());
    }

    private static TaskModel completedForkedTask(String refName, Map<String, Object> output) {
        TaskModel task = new TaskModel();
        task.setReferenceTaskName(refName);
        task.setStatus(TaskModel.Status.COMPLETED);
        task.setWorkflowTask(new WorkflowTask());
        task.setOutputData(output);
        return task;
    }

    @Test
    public void capturesForkedOutputsForSmallFork() {
        Join join = new Join(mock(ConductorProperties.class));
        Map<String, Object> o1 = new HashMap<>(Map.of("k", "v1"));
        Map<String, Object> o2 = new HashMap<>(Map.of("k", "v2"));
        WorkflowModel workflow = new WorkflowModel();
        workflow.setTasks(List.of(completedForkedTask("t1", o1), completedForkedTask("t2", o2)));

        TaskModel joinTask = new TaskModel();
        joinTask.setWorkflowTask(new WorkflowTask());
        Map<String, Object> input = new HashMap<>();
        input.put("joinOn", List.of("t1", "t2"));
        joinTask.setInputData(input);

        boolean done = join.execute(workflow, joinTask, mock(WorkflowExecutor.class));

        assertTrue(done);
        assertEquals(TaskModel.Status.COMPLETED, joinTask.getStatus());
        assertEquals(o1, joinTask.getOutputData().get("t1"));
        assertEquals(o2, joinTask.getOutputData().get("t2"));
    }

    @Test
    public void skipsForkedOutputsWhenCaptureOutputDisabled() {
        Join join = new Join(mock(ConductorProperties.class));
        WorkflowModel workflow = new WorkflowModel();
        workflow.setTasks(
                List.of(
                        completedForkedTask("t1", new HashMap<>(Map.of("k", "v1"))),
                        completedForkedTask("t2", new HashMap<>(Map.of("k", "v2")))));

        TaskModel joinTask = new TaskModel();
        joinTask.setWorkflowTask(new WorkflowTask());
        Map<String, Object> input = new HashMap<>();
        input.put("joinOn", List.of("t1", "t2"));
        input.put("captureOutput", false);
        joinTask.setInputData(input);

        boolean done = join.execute(workflow, joinTask, mock(WorkflowExecutor.class));

        // Join still completes; it just does not copy the forked outputs into its own output.
        assertTrue(done);
        assertEquals(TaskModel.Status.COMPLETED, joinTask.getStatus());
        assertFalse(joinTask.getOutputData().containsKey("t1"));
        assertFalse(joinTask.getOutputData().containsKey("t2"));
    }
}

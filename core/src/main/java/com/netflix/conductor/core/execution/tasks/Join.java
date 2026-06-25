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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.utils.TaskUtils;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_JOIN;

@Component(TASK_TYPE_JOIN)
public class Join extends WorkflowSystemTask {

    @VisibleForTesting static final double EVALUATION_OFFSET_BASE = 1.2;

    /**
     * Fan-out size at or above which the join stops copying every forked task's output into its own
     * output by default, to bound the join task's payload/memory for very large dynamic forks.
     * Overridable per task via the boolean {@code captureOutput} input parameter (backported from
     * Orkes).
     */
    @VisibleForTesting static final int LARGE_FORK_LIMIT = 500;

    private final ConductorProperties properties;

    public Join(ConductorProperties properties) {
        super(TASK_TYPE_JOIN);
        this.properties = properties;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        StringBuilder failureReason = new StringBuilder();
        StringBuilder optionalTaskFailures = new StringBuilder();
        List<String> joinOn = (List<String>) task.getInputData().get("joinOn");
        if (task.isLoopOverTask()) {
            // If join is part of loop over task, wait for specific iteration to get complete
            joinOn =
                    joinOn.stream()
                            .map(name -> TaskUtils.appendIteration(name, task.getIteration()))
                            .toList();
        }

        // Large-fork scaling (backported from Orkes): for very large fan-outs, skip copying every
        // forked task's output into the join output to bound memory/payload size. Callers can force
        // the behaviour either way via the boolean "captureOutput" task input.
        boolean captureOutput = joinOn.size() < LARGE_FORK_LIMIT;
        Object captureOutputOverride = task.getInputData().get("captureOutput");
        if (captureOutputOverride != null) {
            captureOutput = Boolean.parseBoolean(String.valueOf(captureOutputOverride));
        }

        boolean allTasksTerminal =
                joinOn.stream()
                        .map(workflow::getTaskByRefName)
                        .allMatch(t -> t != null && t.getStatus().isTerminal());

        for (String joinOnRef : joinOn) {
            TaskModel forkedTask = workflow.getTaskByRefName(joinOnRef);
            if (forkedTask == null) {
                // Continue checking other tasks if a referenced task is not yet scheduled
                continue;
            }

            TaskModel.Status taskStatus = forkedTask.getStatus();

            // Only add to task output if it's not empty and output capture is enabled
            if (captureOutput && !forkedTask.getOutputData().isEmpty()) {
                task.addOutput(joinOnRef, forkedTask.getOutputData());
            }

            // Determine if the join task fails immediately due to a non-optional, non-permissive
            // task failure,
            // or waits for all tasks to be terminal if the failed task is permissive.
            var isJoinFailure =
                    !taskStatus.isSuccessful()
                            && !forkedTask.getWorkflowTask().isOptional()
                            && (!forkedTask.getWorkflowTask().isPermissive() || allTasksTerminal);
            if (isJoinFailure) {
                final String failureReasons =
                        joinOn.stream()
                                .map(workflow::getTaskByRefName)
                                .filter(Objects::nonNull)
                                .filter(t -> !t.getStatus().isSuccessful())
                                .map(TaskModel::getReasonForIncompletion)
                                .collect(Collectors.joining(" "));
                failureReason.append(failureReasons);
                task.setReasonForIncompletion(failureReason.toString());
                task.setStatus(TaskModel.Status.FAILED);
                return true;
            }

            // check for optional task failures
            if (forkedTask.getWorkflowTask().isOptional()
                    && taskStatus == TaskModel.Status.COMPLETED_WITH_ERRORS) {
                optionalTaskFailures
                        .append(
                                String.format(
                                        "%s/%s",
                                        forkedTask.getTaskDefName(), forkedTask.getTaskId()))
                        .append(" ");
            }
        }

        // Finalize the join task's status based on the outcomes of all referenced tasks.
        if (allTasksTerminal) {
            if (!optionalTaskFailures.isEmpty()) {
                task.setStatus(TaskModel.Status.COMPLETED_WITH_ERRORS);
                optionalTaskFailures.append("completed with errors");
                task.setReasonForIncompletion(optionalTaskFailures.toString());
            } else {
                task.setStatus(TaskModel.Status.COMPLETED);
            }
            return true;
        }

        // Task execution not complete, waiting on more tasks to reach terminal state.
        return false;
    }

    @Override
    public Optional<Long> getEvaluationOffset(TaskModel taskModel, long maxOffset) {
        // Check if joinMode is set to SYNC — read directly from the workflow task definition
        // rather than from input data so the value is never duplicated into the task's payload.
        WorkflowTask workflowTask = taskModel.getWorkflowTask();
        if (workflowTask != null && WorkflowTask.JoinMode.SYNC == workflowTask.getJoinMode()) {
            // Synchronous mode: evaluate immediately every time (no backoff)
            return Optional.of(0L);
        }

        // Asynchronous mode (default): use exponential backoff
        int pollCount = taskModel.getPollCount();
        // Assuming pollInterval = 50ms and evaluationOffsetThreshold = 200 this will cause
        // a JOIN task to be evaluated continuously during the first 10 seconds and the FORK/JOIN
        // will end with minimal delay.
        if (pollCount <= properties.getSystemTaskPostponeThreshold()) {
            return Optional.of(0L);
        }

        double exp = pollCount - properties.getSystemTaskPostponeThreshold();
        return Optional.of(Math.min((long) Math.pow(EVALUATION_OFFSET_BASE, exp), maxOffset));
    }

    public boolean isAsync() {
        return true;
    }
}

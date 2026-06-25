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
package com.netflix.conductor.core.execution.mapper;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * An implementation of {@link TaskMapper} to map a {@link WorkflowTask} of type {@link
 * TaskType#SIMPLE} to a {@link TaskModel} with status {@link TaskModel.Status#SCHEDULED}.
 * <b>NOTE:</b> There is not type defined for simples task.
 */
@Component
public class SimpleTaskMapper implements TaskMapper {

    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleTaskMapper.class);
    private final ParametersUtils parametersUtils;
    private final MetadataDAO metadataDAO;

    public SimpleTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
        this.parametersUtils = parametersUtils;
        this.metadataDAO = metadataDAO;
    }

    @Override
    public String getTaskType() {
        return TaskType.SIMPLE.name();
    }

    /**
     * This method maps a {@link WorkflowTask} of type {@link TaskType#SIMPLE} to a {@link
     * TaskModel}
     *
     * @param taskMapperContext: A wrapper class containing the {@link WorkflowTask}, {@link
     *     WorkflowDef}, {@link WorkflowModel} and a string representation of the TaskId
     * @return a List with just one simple task
     */
    @Override
    public List<TaskModel> getMappedTasks(TaskMapperContext taskMapperContext) {

        LOGGER.debug("TaskMapperContext {} in SimpleTaskMapper", taskMapperContext);

        WorkflowTask workflowTask = taskMapperContext.getWorkflowTask();
        WorkflowModel workflowModel = taskMapperContext.getWorkflowModel();
        int retryCount = taskMapperContext.getRetryCount();
        String retriedTaskId = taskMapperContext.getRetryTaskId();

        // Resolve the task definition: prefer the one already on the workflow task; otherwise look
        // it up from the metadata store; otherwise fall back to a default keyed by the task name
        // (backported from Orkes). Cache it back on the workflow task for downstream use.
        TaskDef taskDefinition = workflowTask.getTaskDefinition();
        if (taskDefinition == null) {
            taskDefinition = metadataDAO.getTaskDef(workflowTask.getName());
            if (taskDefinition == null) {
                LOGGER.warn(
                        "Task {} does not have a definition, using defaults",
                        workflowTask.getName());
                taskDefinition = new TaskDef(workflowTask.getName());
            }
            workflowTask.setTaskDefinition(taskDefinition);
        }

        // A task definition may declare a base type, in which case the scheduled task takes the
        // base type rather than the task name (backported from Orkes; supports user-defined task
        // types backed by a base type).
        String taskType = workflowTask.getName();
        if (StringUtils.isNotBlank(taskDefinition.getBaseType())) {
            taskType = taskDefinition.getBaseType();
        }

        Map<String, Object> input =
                parametersUtils.getTaskInput(
                        workflowTask.getInputParameters(),
                        workflowModel,
                        taskDefinition,
                        taskMapperContext.getTaskId());
        TaskModel simpleTask = taskMapperContext.createTaskModel();
        simpleTask.setTaskType(taskType);
        simpleTask.setStartDelayInSeconds(workflowTask.getStartDelay());
        simpleTask.setInputData(input);
        simpleTask.setStatus(TaskModel.Status.SCHEDULED);
        simpleTask.setRetryCount(retryCount);
        simpleTask.setCallbackAfterSeconds(workflowTask.getStartDelay());
        simpleTask.setResponseTimeoutSeconds(taskDefinition.getResponseTimeoutSeconds());
        simpleTask.setRetriedTaskId(retriedTaskId);
        simpleTask.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
        simpleTask.setRateLimitFrequencyInSeconds(taskDefinition.getRateLimitFrequencyInSeconds());
        return List.of(simpleTask);
    }
}

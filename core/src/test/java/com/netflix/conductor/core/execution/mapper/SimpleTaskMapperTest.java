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

import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleTaskMapperTest {

    private SimpleTaskMapper simpleTaskMapper;
    private MetadataDAO metadataDAO;

    private IDGenerator idGenerator = new IDGenerator();

    @Before
    public void setUp() {
        ParametersUtils parametersUtils = mock(ParametersUtils.class);
        metadataDAO = mock(MetadataDAO.class);
        simpleTaskMapper = new SimpleTaskMapper(parametersUtils, metadataDAO);
    }

    @Test
    public void getMappedTasks() {

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("simple_task");
        workflowTask.setTaskDefinition(new TaskDef("simple_task"));

        String taskId = idGenerator.generate();
        String retriedTaskId = idGenerator.generate();

        WorkflowDef workflowDef = new WorkflowDef();
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(new HashMap<>())
                        .withRetryCount(0)
                        .withRetryTaskId(retriedTaskId)
                        .withTaskId(taskId)
                        .build();

        List<TaskModel> mappedTasks = simpleTaskMapper.getMappedTasks(taskMapperContext);
        assertNotNull(mappedTasks);
        assertEquals(1, mappedTasks.size());
    }

    @Test
    public void getMappedTasksWithNoTaskDefinition() {

        // Given a workflow task without a task definition
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("simple_task");
        String taskId = idGenerator.generate();
        String retriedTaskId = idGenerator.generate();

        WorkflowDef workflowDef = new WorkflowDef();
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(new HashMap<>())
                        .withRetryCount(0)
                        .withRetryTaskId(retriedTaskId)
                        .withTaskId(taskId)
                        .build();

        // when
        List<TaskModel> mappedTasks = simpleTaskMapper.getMappedTasks(taskMapperContext);

        // then a task is created with default task definition values
        assertNotNull(mappedTasks);
        assertEquals(1, mappedTasks.size());
        assertEquals(TaskModel.Status.SCHEDULED, mappedTasks.get(0).getStatus());
    }

    @Test
    public void scheduledTaskTypeDerivesFromBaseTypeAndDefIsFetchedFromMetadata() {
        // Workflow task with no inline definition; the metadata store returns a def whose baseType
        // overrides the task type used for the scheduled task.
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("my_user_defined_task");

        TaskDef metadataDef = new TaskDef("my_user_defined_task");
        metadataDef.setBaseType("HTTP");
        when(metadataDAO.getTaskDef("my_user_defined_task")).thenReturn(metadataDef);

        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowDefinition(new WorkflowDef());

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withTaskInput(new HashMap<>())
                        .withRetryCount(0)
                        .withRetryTaskId(idGenerator.generate())
                        .withTaskId(idGenerator.generate())
                        .build();

        List<TaskModel> mappedTasks = simpleTaskMapper.getMappedTasks(taskMapperContext);

        assertEquals(1, mappedTasks.size());
        // baseType wins over the task name for the scheduled task type
        assertEquals("HTTP", mappedTasks.get(0).getTaskType());
        // the resolved definition is cached back on the workflow task
        assertEquals(metadataDef, workflowTask.getTaskDefinition());
    }
}

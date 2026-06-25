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
package com.netflix.conductor.core.execution;

import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.listener.TaskStatusListener;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.WorkflowMessageQueueDAO;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.service.ExecutionLockService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies the open-core extension seams on {@link WorkflowExecutorOps}. */
public class WorkflowExecutorOpsSeamsTest {

    private final ExecutionDAOFacade executionDAOFacade = mock(ExecutionDAOFacade.class);
    private final ExecutionLockService lockService = mock(ExecutionLockService.class);
    private final MetadataMapperService metadataMapperService = mock(MetadataMapperService.class);

    private RecordingExecutor newExecutor() {
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getActiveWorkerLastPollTimeout()).thenReturn(Duration.ofSeconds(10));
        return new RecordingExecutor(
                mock(DeciderService.class),
                mock(MetadataDAO.class),
                mock(QueueDAO.class),
                metadataMapperService,
                mock(WorkflowStatusListener.class),
                mock(TaskStatusListener.class),
                executionDAOFacade,
                properties,
                lockService,
                mock(SystemTaskRegistry.class),
                mock(ParametersUtils.class),
                mock(IDGenerator.class),
                Optional.<WorkflowMessageQueueDAO>empty());
    }

    private static final class RecordingExecutor extends WorkflowExecutorOps {
        final AtomicReference<String> decidedId = new AtomicReference<>();
        boolean abortStart;
        boolean beforeStartCalled;

        RecordingExecutor(
                DeciderService deciderService,
                MetadataDAO metadataDAO,
                QueueDAO queueDAO,
                MetadataMapperService metadataMapperService,
                WorkflowStatusListener workflowStatusListener,
                TaskStatusListener taskStatusListener,
                ExecutionDAOFacade executionDAOFacade,
                ConductorProperties properties,
                ExecutionLockService executionLockService,
                SystemTaskRegistry systemTaskRegistry,
                ParametersUtils parametersUtils,
                IDGenerator idGenerator,
                Optional<WorkflowMessageQueueDAO> workflowMessageQueueDAO) {
            super(
                    deciderService,
                    metadataDAO,
                    queueDAO,
                    metadataMapperService,
                    workflowStatusListener,
                    taskStatusListener,
                    executionDAOFacade,
                    properties,
                    executionLockService,
                    systemTaskRegistry,
                    parametersUtils,
                    idGenerator,
                    workflowMessageQueueDAO);
        }

        @Override
        protected void onDecide(String workflowId) {
            decidedId.set(workflowId);
        }

        @Override
        protected void beforeStartWorkflow(
                StartWorkflowInput input, WorkflowDef workflowDefinition) {
            beforeStartCalled = true;
            if (abortStart) {
                throw new IllegalStateException("aborted by seam");
            }
        }
    }

    @Test
    public void onDecideHookReceivesWorkflowIdBeforeLoad() {
        RecordingExecutor executor = newExecutor();
        // Lock not acquired -> decide returns early, but the seam must already have run.
        when(lockService.acquireLock(anyString())).thenReturn(false);

        WorkflowModel result = executor.decide("wf-123");

        assertNull(result);
        assertEquals("wf-123", executor.decidedId.get());
    }

    @Test
    public void beforeStartWorkflowHookCanAbortStart() {
        RecordingExecutor executor = newExecutor();
        executor.abortStart = true;

        WorkflowDef def = new WorkflowDef();
        def.setName("wf");
        when(metadataMapperService.populateTaskDefinitions(any())).thenReturn(def);

        StartWorkflowInput input = new StartWorkflowInput();
        input.setWorkflowDefinition(def);
        input.setWorkflowInput(new HashMap<>());

        try {
            executor.startWorkflow(input);
            fail("expected the start to be aborted by the seam");
        } catch (IllegalStateException expected) {
            // expected
        }

        assertTrue(executor.beforeStartCalled);
        verify(executionDAOFacade, never()).createWorkflow(any());
    }
}

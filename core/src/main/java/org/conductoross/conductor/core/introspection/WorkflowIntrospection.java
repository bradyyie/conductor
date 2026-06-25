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
package org.conductoross.conductor.core.introspection;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Static facade for lightweight workflow execution introspection.
 *
 * <p>This is a generic, feature-agnostic extension seam exposed by OSS. The engine may call {@code
 * WorkflowIntrospection.record(...)} / {@code finalize(...)} anywhere; by default every call routes
 * to a {@link NoopWorkflowIntrospectionProvider} and returns the zero-overhead {@link
 * NoopRecordBuilder}. An enterprise edition can register a {@link WorkflowIntrospectionProvider}
 * Spring bean to capture real traces &mdash; OSS does not know, and must not know, why the seam
 * exists (no tenant/org/auth concepts here).
 *
 * <p>The provider is wired statically (mirroring how the engine calls these methods from static
 * contexts) by this {@link Component}'s constructor. If no provider bean is present, the no-op
 * default remains in effect.
 */
@Component
public class WorkflowIntrospection {

    private static volatile WorkflowIntrospectionProvider provider =
            new NoopWorkflowIntrospectionProvider();

    public WorkflowIntrospection(ObjectProvider<WorkflowIntrospectionProvider> providerProvider) {
        WorkflowIntrospectionProvider resolved = providerProvider.getIfAvailable();
        if (resolved != null) {
            provider = resolved;
        }
    }

    /** Test/enterprise hook to install (or reset) the active provider. */
    public static void setProvider(WorkflowIntrospectionProvider newProvider) {
        provider = newProvider == null ? new NoopWorkflowIntrospectionProvider() : newProvider;
    }

    public static RecordBuilder record(String name) {
        return provider.record(name);
    }

    public static RecordBuilder record(WorkflowModel workflow, String name) {
        return provider.record(workflow, name);
    }

    public static RecordBuilder record(WorkflowModel workflow, String name, TaskModel task) {
        return provider.record(workflow, name, task);
    }

    public static RecordBuilder record(TaskModel task, String name) {
        return provider.record(task, name);
    }

    public static RecordBuilder record(String name, String taskId) {
        return provider.record(name, taskId);
    }

    public static RecordBuilder finalize(String workflowId, String name) {
        return provider.finalizeRecord(workflowId, name);
    }

    public static RecordBuilder finalize(WorkflowModel workflow, String name) {
        return provider.finalizeRecord(workflow, name);
    }

    public static RecordBuilder finalize(TaskModel task, String name) {
        return provider.finalizeRecord(task, name);
    }

    public static RecordBuilder finalize(Task task, String name) {
        return provider.finalizeRecord(task, name);
    }
}

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

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Service Provider Interface backing the static {@link WorkflowIntrospection} facade.
 *
 * <p>This is a generic extension seam: the OSS engine calls {@link WorkflowIntrospection} freely,
 * and the call routes to whichever provider bean is registered. The OSS default ({@link
 * NoopWorkflowIntrospectionProvider}) returns {@link NoopRecordBuilder#INSTANCE} for everything, so
 * there is zero overhead. An enterprise edition can register its own {@code
 * WorkflowIntrospectionProvider} bean (e.g. a tracer) without OSS knowing why.
 *
 * <p>All methods default to no-op so an implementor overrides only what it needs. Tenant/request
 * context (if any) is the provider's own concern and never appears in this contract.
 */
public interface WorkflowIntrospectionProvider {

    default RecordBuilder record(String name) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder record(WorkflowModel workflow, String name) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder record(WorkflowModel workflow, String name, TaskModel task) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder record(TaskModel task, String name) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder record(String name, String taskId) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder finalizeRecord(String workflowId, String name) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder finalizeRecord(WorkflowModel workflow, String name) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder finalizeRecord(TaskModel task, String name) {
        return NoopRecordBuilder.INSTANCE;
    }

    default RecordBuilder finalizeRecord(Task task, String name) {
        return NoopRecordBuilder.INSTANCE;
    }
}

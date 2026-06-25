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

import java.util.function.Supplier;

import com.netflix.conductor.model.TaskModel;

/**
 * A fluent, {@link AutoCloseable} handle returned by {@link WorkflowIntrospection}. It is designed
 * to be used with try-with-resources so that a span/record is implicitly closed when the block
 * exits:
 *
 * <pre>{@code
 * try (var record = WorkflowIntrospection.record(workflow, "decide")) {
 *     ...
 * }
 * }</pre>
 *
 * <p>This is a generic, feature-agnostic extension seam. The default (OSS) implementation is a
 * no-op ({@link NoopRecordBuilder}); an enterprise edition may register a real tracer via {@link
 * WorkflowIntrospectionProvider} without the core engine knowing why the seam exists.
 */
public interface RecordBuilder extends AutoCloseable {

    RecordBuilder workflowId(String workflowId);

    RecordBuilder task(String taskId);

    RecordBuilder task(TaskModel task);

    RecordBuilder description(String description);

    RecordBuilder description(Supplier<String> descriptionSupplier);

    <T> RecordBuilder attribute(String name, T value);

    RecordBuilder newThread();

    @Override
    void close();
}

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
 * The default, zero-overhead {@link RecordBuilder}. Every method is a no-op and returns the
 * singleton, so call sites in the engine incur no cost when no real introspection provider is
 * registered (the OSS default).
 */
public final class NoopRecordBuilder implements RecordBuilder {

    public static final NoopRecordBuilder INSTANCE = new NoopRecordBuilder();

    private NoopRecordBuilder() {}

    @Override
    public RecordBuilder workflowId(String workflowId) {
        return this;
    }

    @Override
    public RecordBuilder task(String taskId) {
        return this;
    }

    @Override
    public RecordBuilder task(TaskModel task) {
        return this;
    }

    @Override
    public RecordBuilder description(String description) {
        return this;
    }

    @Override
    public RecordBuilder description(Supplier<String> descriptionSupplier) {
        return this;
    }

    @Override
    public <T> RecordBuilder attribute(String name, T value) {
        return this;
    }

    @Override
    public RecordBuilder newThread() {
        return this;
    }

    @Override
    public void close() {
        // no-op
    }
}

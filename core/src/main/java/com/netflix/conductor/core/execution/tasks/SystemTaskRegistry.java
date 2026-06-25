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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

/**
 * A container class that holds a mapping of system task types {@link
 * com.netflix.conductor.common.metadata.tasks.TaskType} to {@link WorkflowSystemTask} instances.
 */
@Component
@DependsOn("workerTaskAnnotationScanner")
public class SystemTaskRegistry {

    public static final String ASYNC_SYSTEM_TASKS_QUALIFIER = "asyncSystemTasks";

    private final Map<String, WorkflowSystemTask> registry;

    public SystemTaskRegistry(Set<WorkflowSystemTask> tasks) {
        this.registry = byType(tasks);
    }

    /**
     * Collates system tasks by their task type, resolving same-typed duplicates via {@link
     * WorkflowSystemTask#isOverride()} precedence. Exposed so other wiring (e.g. the async system
     * task set) resolves overrides identically.
     */
    public static Map<String, WorkflowSystemTask> byType(Collection<WorkflowSystemTask> tasks) {
        return tasks.stream()
                .collect(
                        Collectors.toMap(
                                WorkflowSystemTask::getTaskType,
                                Function.identity(),
                                SystemTaskRegistry::preferOverride));
    }

    /**
     * Resolves two system tasks registered for the same task type. Exactly one must be marked as an
     * override ({@link WorkflowSystemTask#isOverride()}); that one wins. If neither or both are
     * overrides it is an ambiguous configuration and we fail fast (this also preserves the historic
     * behaviour of rejecting duplicate non-override system task types).
     */
    private static WorkflowSystemTask preferOverride(WorkflowSystemTask a, WorkflowSystemTask b) {
        if (a.isOverride() == b.isOverride()) {
            throw new IllegalStateException(
                    "Multiple system tasks registered for type '"
                            + a.getTaskType()
                            + "': "
                            + a.getClass().getName()
                            + " and "
                            + b.getClass().getName()
                            + ". Mark exactly one with isOverride()=true to override the other.");
        }
        return a.isOverride() ? a : b;
    }

    public WorkflowSystemTask get(String taskType) {
        return Optional.ofNullable(registry.get(taskType))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        taskType + "not found in " + getClass().getSimpleName()));
    }

    public boolean isSystemTask(String taskType) {
        return registry.containsKey(taskType);
    }
}

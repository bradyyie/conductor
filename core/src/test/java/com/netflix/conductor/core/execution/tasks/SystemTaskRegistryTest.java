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
package com.netflix.conductor.core.execution.tasks;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SystemTaskRegistryTest {

    /** Minimal system task whose type, override flag and async flag are configurable. */
    private static final class FakeSystemTask extends WorkflowSystemTask {
        private final boolean override;
        private final boolean async;

        FakeSystemTask(String type, boolean override, boolean async) {
            super(type);
            this.override = override;
            this.async = async;
        }

        @Override
        public boolean isOverride() {
            return override;
        }

        @Override
        public boolean isAsync() {
            return async;
        }
    }

    @Test
    public void overrideTaskWinsOverDefault() {
        WorkflowSystemTask defaultTask = new FakeSystemTask("FOO", false, true);
        WorkflowSystemTask override = new FakeSystemTask("FOO", true, false);

        SystemTaskRegistry registry =
                new SystemTaskRegistry(new LinkedHashSet<>(List.of(defaultTask, override)));

        assertSame(override, registry.get("FOO"));
        assertTrue(registry.isSystemTask("FOO"));
    }

    @Test(expected = IllegalStateException.class)
    public void duplicateNonOverrideTasksFailFast() {
        Set<WorkflowSystemTask> tasks =
                new LinkedHashSet<>(
                        List.of(
                                new FakeSystemTask("FOO", false, true),
                                new FakeSystemTask("FOO", false, true)));
        new SystemTaskRegistry(tasks);
    }

    @Test
    public void byTypePrefersOverrideRegardlessOfOrder() {
        WorkflowSystemTask defaultTask = new FakeSystemTask("FOO", false, true);
        WorkflowSystemTask override = new FakeSystemTask("FOO", true, false);

        // override encountered first
        assertSame(override, SystemTaskRegistry.byType(List.of(override, defaultTask)).get("FOO"));
        // default encountered first
        assertSame(override, SystemTaskRegistry.byType(List.of(defaultTask, override)).get("FOO"));
    }

    @Test
    public void unknownTaskIsNotASystemTask() {
        SystemTaskRegistry registry =
                new SystemTaskRegistry(
                        new LinkedHashSet<>(List.of(new FakeSystemTask("FOO", false, true))));
        assertFalse(registry.isSystemTask("BAR"));
    }
}

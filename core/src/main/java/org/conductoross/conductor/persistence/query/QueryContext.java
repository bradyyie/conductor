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
package org.conductoross.conductor.persistence.query;

/**
 * Generic, feature-agnostic context passed to a persistence query-extension hook. It carries only
 * neutral facts about the query being built (the table, whether it is a read or a write, and the
 * originating request/entity if any). It deliberately contains no tenant/organization/auth concepts
 * &mdash; those are the concern of whatever add-on overrides the hook, not of OSS.
 *
 * @param table the primary table the query targets
 * @param operation whether the query reads or writes
 * @param request the originating request or entity, if useful to an extension (may be {@code null})
 */
public record QueryContext(String table, Operation operation, Object request) {

    public enum Operation {
        READ,
        WRITE
    }

    public static QueryContext read(String table) {
        return new QueryContext(table, Operation.READ, null);
    }

    public static QueryContext write(String table) {
        return new QueryContext(table, Operation.WRITE, null);
    }
}

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

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the generic persistence seam composes the way the enterprise edition relies on: an OSS
 * adapter builds a base query/insert, then a decorator (standing in for an enterprise subclass's
 * {@code applyQueryExtensions}/{@code applyWriteExtensions}) appends an {@code org_id} predicate /
 * column / conflict-target without breaking positional binds. OSS itself knows nothing of org_id;
 * this test only verifies the builder mechanics that make the override safe.
 */
class SqlBuilderSeamTest {

    @Test
    void readSeam_appendsPredicateWithOrderIndependentBinds() {
        // OSS adapter builds the base read.
        SqlQueryBuilder qb =
                SqlQueryBuilder.create()
                        .select("payload")
                        .from("workflow")
                        .where("workflow_id = :wid")
                        .bind("wid", "w1");

        // Enterprise "applyQueryExtensions" decorates it (super() already ran above).
        qb.and("org_id = :orgId").bind("orgId", "acme");

        SqlQueryBuilder.Rendered r = qb.render();
        assertEquals(
                "SELECT payload FROM workflow WHERE workflow_id = ? AND org_id = ?", r.sql());
        // Binds are positional in marker order regardless of bind() call order.
        assertEquals(List.of("w1", "acme"), r.binds());
    }

    @Test
    void readSeam_bindOrderDoesNotMatter() {
        SqlQueryBuilder qb =
                SqlQueryBuilder.create()
                        .select("*")
                        .from("task")
                        .where("a = :a")
                        .and("b = :b")
                        .bind("b", 2) // bound out of order on purpose
                        .bind("a", 1);
        SqlQueryBuilder.Rendered r = qb.render();
        assertEquals("SELECT * FROM task WHERE a = ? AND b = ?", r.sql());
        assertEquals(List.of(1, 2), r.binds());
    }

    @Test
    void writeSeam_composesColumnAndConflictTarget() {
        // OSS adapter builds the base upsert on its natural key.
        SqlInsertBuilder ib =
                SqlInsertBuilder.create()
                        .into("workflow")
                        .column("workflow_id", "w1")
                        .column("payload", "{}")
                        .onConflict("workflow_id")
                        .doUpdateSet("payload = EXCLUDED.payload");

        // Enterprise "applyWriteExtensions": add org_id column AND extend the conflict target.
        ib.column("org_id", "acme").onConflict("org_id");

        SqlInsertBuilder.Rendered r = ib.render();
        assertEquals(
                "INSERT INTO workflow (workflow_id, payload, org_id) VALUES (?, ?, ?) "
                        + "ON CONFLICT (workflow_id, org_id) DO UPDATE SET payload = EXCLUDED.payload",
                r.sql());
        // Column-insertion order: natural columns first, org_id appended.
        assertEquals(List.of("w1", "{}", "acme"), r.binds());
    }

    @Test
    void writeSeam_ossOnlyHasNoOrgConcept() {
        // With no decorator (OSS running standalone), the insert is unchanged.
        SqlInsertBuilder ib =
                SqlInsertBuilder.create()
                        .into("workflow")
                        .column("workflow_id", "w1")
                        .onConflict("workflow_id")
                        .onConflictDoNothing();
        String sql = ib.toSql();
        assertEquals(
                "INSERT INTO workflow (workflow_id) VALUES (?) ON CONFLICT (workflow_id) DO NOTHING",
                sql);
        assertTrue(!sql.contains("org_id"));
    }
}

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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlQueryBuilderTest {

    @Test
    public void rendersNamedParamsToPositionalInOrder() {
        SqlQueryBuilder query =
                SqlQueryBuilder.create()
                        .select("task_id", "json_data")
                        .from("task")
                        .where("task_def_name = :name")
                        .and("workflow_id = :wfId")
                        .bind("name", "myTask")
                        .bind("wfId", "wf-1");

        assertEquals(
                "SELECT task_id, json_data FROM task WHERE task_def_name = ? AND workflow_id = ?",
                query.toSql());
        assertEquals(List.of("myTask", "wf-1"), query.binds());
    }

    @Test
    public void bindsAreEmittedInRenderOrderRegardlessOfBindOrder() {
        SqlQueryBuilder query =
                SqlQueryBuilder.create()
                        .select("*")
                        .from("task")
                        .where("task_def_name = :name")
                        .and("workflow_id = :wfId")
                        .bind("wfId", "wf-1") // bound out of order on purpose
                        .bind("name", "myTask");

        assertEquals(List.of("myTask", "wf-1"), query.binds());
    }

    @Test
    public void subclassStyleAndPredicateAppendsAfterExistingBinds() {
        // Simulates an add-on appending an extra predicate (e.g. a tenant filter) on top of a base
        // query, without disturbing the base bind ordering.
        SqlQueryBuilder base =
                SqlQueryBuilder.create()
                        .select("*")
                        .from("workflow")
                        .where("status = :status")
                        .bind("status", "RUNNING");

        base.and("org_id = :orgId").bind("orgId", "0000");

        assertEquals("SELECT * FROM workflow WHERE status = ? AND org_id = ?", base.toSql());
        assertEquals(List.of("RUNNING", "0000"), base.binds());
    }

    @Test
    public void trailingClausesRenderAfterWherePredicates() {
        SqlQueryBuilder query =
                SqlQueryBuilder.create()
                        .select("task_id")
                        .from("task_in_progress")
                        .where("task_def_name = :name")
                        .orderBy("created_on")
                        .limit(5)
                        .bind("name", "myTask");
        // A predicate appended after orderBy/limit (as the extension hook does) still lands in
        // WHERE.
        query.and("org_id = :orgId").bind("orgId", "0000");

        assertEquals(
                "SELECT task_id FROM task_in_progress WHERE task_def_name = ? AND org_id = ? ORDER BY created_on LIMIT 5",
                query.toSql());
        assertEquals(List.of("myTask", "0000"), query.binds());
    }

    @Test
    public void repeatedNamedParamEmitsValuePerOccurrence() {
        SqlQueryBuilder query =
                SqlQueryBuilder.create().raw("SELECT * FROM s WHERE a = :v OR b = :v").bind("v", 7);

        assertEquals("SELECT * FROM s WHERE a = ? OR b = ?", query.toSql());
        assertEquals(List.of(7, 7), query.binds());
    }

    @Test
    public void postgresDoubleColonCastsAreNotTreatedAsNamedParams() {
        SqlQueryBuilder query =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE queue_message SET deliver_on = (current_timestamp + (:secs ||' seconds')::interval)")
                        .where("queue_name = :name")
                        .bind("secs", 30L)
                        .bind("name", "q1");
        // :secs / :name are real params; ::interval must stay literal (not parsed as :interval).
        assertEquals(
                "UPDATE queue_message SET deliver_on = (current_timestamp + (? ||' seconds')::interval) WHERE queue_name = ?",
                query.toSql());
        assertEquals(List.of(30L, "q1"), query.binds());
    }

    @Test(expected = IllegalStateException.class)
    public void missingBindFailsFast() {
        SqlQueryBuilder.create().select("*").from("t").where("a = :missing").toSql();
    }
}

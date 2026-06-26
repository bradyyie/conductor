/*
 * Copyright 2023 Conductor Authors.
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
package com.netflix.conductor.mysql.dao;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.conductoross.conductor.persistence.query.QueryContext;
import org.conductoross.conductor.persistence.query.SqlInsertBuilder;
import org.conductoross.conductor.persistence.query.SqlQueryBuilder;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.exception.ConflictException;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.mysql.config.MySQLProperties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class MySQLMetadataDAO extends MySQLBaseDAO implements MetadataDAO, EventHandlerDAO {

    private final ConcurrentHashMap<String, TaskDef> taskDefCache = new ConcurrentHashMap<>();
    private static final String CLASS_NAME = MySQLMetadataDAO.class.getSimpleName();

    public MySQLMetadataDAO(
            RetryTemplate retryTemplate,
            ObjectMapper objectMapper,
            DataSource dataSource,
            MySQLProperties properties) {
        super(retryTemplate, objectMapper, dataSource);

        long cacheRefreshTime = properties.getTaskDefCacheRefreshInterval().getSeconds();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(
                        this::refreshTaskDefs,
                        cacheRefreshTime,
                        cacheRefreshTime,
                        TimeUnit.SECONDS);
    }

    @Override
    public TaskDef createTaskDef(TaskDef taskDef) {
        validate(taskDef);
        insertOrUpdateTaskDef(taskDef);
        return taskDef;
    }

    @Override
    public TaskDef updateTaskDef(TaskDef taskDef) {
        validate(taskDef);
        insertOrUpdateTaskDef(taskDef);
        return taskDef;
    }

    @Override
    public TaskDef getTaskDef(String name) {
        Preconditions.checkNotNull(name, "TaskDef name cannot be null");
        TaskDef taskDef = taskDefCache.get(name);
        if (taskDef == null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Cache miss: {}", name);
            }
            taskDef = getTaskDefFromDB(name);
        }

        return taskDef;
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        return getWithRetriedTransactions(this::findAllTaskDefs);
    }

    @Override
    public void removeTaskDef(String name) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM meta_task_def")
                        .where("name = :name")
                        .bind("name", name);

        withTransaction(
                tx -> {
                    if (execute(tx, builder, QueryContext.write("meta_task_def")) == 0) {
                        throw new NotFoundException("No such task definition");
                    }

                    taskDefCache.remove(name);
                });
    }

    @Override
    public void createWorkflowDef(WorkflowDef def) {
        validate(def);

        withTransaction(
                tx -> {
                    if (workflowExists(tx, def)) {
                        throw new ConflictException(
                                "Workflow with " + def.key() + " already exists!");
                    }

                    insertOrUpdateWorkflowDef(tx, def);
                });
    }

    @Override
    public void updateWorkflowDef(WorkflowDef def) {
        validate(def);
        withTransaction(tx -> insertOrUpdateWorkflowDef(tx, def));
    }

    @Override
    public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_workflow_def")
                        .where("name = :name")
                        .and("version = latest_version")
                        .bind("name", name);

        return Optional.ofNullable(
                queryWithTransaction(
                        builder,
                        QueryContext.read("meta_workflow_def"),
                        q -> q.executeAndFetchFirst(WorkflowDef.class)));
    }

    @Override
    public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_workflow_def")
                        .where("name = :name")
                        .and("version = :version")
                        .bind("name", name)
                        .bind("version", version);
        return Optional.ofNullable(
                queryWithTransaction(
                        builder,
                        QueryContext.read("meta_workflow_def"),
                        q -> q.executeAndFetchFirst(WorkflowDef.class)));
    }

    @Override
    public void removeWorkflowDef(String name, Integer version) {
        SqlQueryBuilder delete =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM meta_workflow_def")
                        .where("name = :name")
                        .and("version = :version")
                        .bind("name", name)
                        .bind("version", version);

        withTransaction(
                tx -> {
                    // remove specified workflow
                    if (execute(tx, delete, QueryContext.write("meta_workflow_def")) == 0) {
                        throw new NotFoundException(
                                String.format(
                                        "No such workflow definition: %s version: %d",
                                        name, version));
                    }
                    // reset latest version based on remaining rows for this workflow
                    Optional<Integer> maxVersion = getLatestVersion(tx, name);
                    maxVersion.ifPresent(newVersion -> updateLatestVersion(tx, name, newVersion));
                });
    }

    public List<String> findAll() {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create().select("DISTINCT name").from("meta_workflow_def");
        return queryWithTransaction(
                builder,
                QueryContext.read("meta_workflow_def"),
                q -> q.executeAndFetch(String.class));
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefs() {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_workflow_def")
                        .orderBy("name", "version");

        return queryWithTransaction(
                builder,
                QueryContext.read("meta_workflow_def"),
                q -> q.executeAndFetch(WorkflowDef.class));
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefsLatestVersions() {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_workflow_def wd")
                        .where(
                                "wd.version = (SELECT MAX(version) FROM meta_workflow_def wd2 WHERE wd2.name = wd.name)");
        return queryWithTransaction(
                builder,
                QueryContext.read("meta_workflow_def"),
                q -> q.executeAndFetch(WorkflowDef.class));
    }

    public List<WorkflowDef> getAllLatest() {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_workflow_def")
                        .where("version = latest_version");

        return queryWithTransaction(
                builder,
                QueryContext.read("meta_workflow_def"),
                q -> q.executeAndFetch(WorkflowDef.class));
    }

    public List<WorkflowDef> getAllVersions(String name) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_workflow_def")
                        .where("name = :name")
                        .bind("name", name)
                        .orderBy("version");

        return queryWithTransaction(
                builder,
                QueryContext.read("meta_workflow_def"),
                q -> q.executeAndFetch(WorkflowDef.class));
    }

    @Override
    public void addEventHandler(EventHandler eventHandler) {
        Preconditions.checkNotNull(eventHandler.getName(), "EventHandler name cannot be null");

        withTransaction(
                tx -> {
                    if (getEventHandler(tx, eventHandler.getName()) != null) {
                        throw new ConflictException(
                                "EventHandler with name "
                                        + eventHandler.getName()
                                        + " already exists!");
                    }

                    SqlInsertBuilder insert =
                            SqlInsertBuilder.create()
                                    .into("meta_event_handler")
                                    .column("name", eventHandler.getName())
                                    .column("event", eventHandler.getEvent())
                                    .column("active", eventHandler.isActive())
                                    .column("json_data", toJson(eventHandler));

                    execute(tx, insert, QueryContext.write("meta_event_handler"));
                });
    }

    @Override
    public void updateEventHandler(EventHandler eventHandler) {
        Preconditions.checkNotNull(eventHandler.getName(), "EventHandler name cannot be null");

        withTransaction(
                tx -> {
                    EventHandler existing = getEventHandler(tx, eventHandler.getName());
                    if (existing == null) {
                        throw new NotFoundException(
                                "EventHandler with name " + eventHandler.getName() + " not found!");
                    }

                    SqlQueryBuilder update =
                            SqlQueryBuilder.create()
                                    .raw(
                                            "UPDATE meta_event_handler SET event = :event, active = :active, json_data = :jsonData, modified_on = CURRENT_TIMESTAMP")
                                    .where("name = :name")
                                    .bind("event", eventHandler.getEvent())
                                    .bind("active", eventHandler.isActive())
                                    .bind("jsonData", toJson(eventHandler))
                                    .bind("name", eventHandler.getName());

                    execute(tx, update, QueryContext.write("meta_event_handler"));
                });
    }

    @Override
    public void removeEventHandler(String name) {
        withTransaction(
                tx -> {
                    EventHandler existing = getEventHandler(tx, name);
                    if (existing == null) {
                        throw new NotFoundException(
                                "EventHandler with name " + name + " not found!");
                    }

                    SqlQueryBuilder delete =
                            SqlQueryBuilder.create()
                                    .raw("DELETE FROM meta_event_handler")
                                    .where("name = :name")
                                    .bind("name", name);

                    execute(tx, delete, QueryContext.write("meta_event_handler"));
                });
    }

    @Override
    public List<EventHandler> getAllEventHandlers() {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create().select("json_data").from("meta_event_handler");
        return queryWithTransaction(
                builder,
                QueryContext.read("meta_event_handler"),
                q -> q.executeAndFetch(EventHandler.class));
    }

    @Override
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_event_handler")
                        .where("event = :event")
                        .bind("event", event);
        return queryWithTransaction(
                builder,
                QueryContext.read("meta_event_handler"),
                q -> {
                    return q.executeAndFetch(
                            rs -> {
                                List<EventHandler> handlers = new ArrayList<>();
                                while (rs.next()) {
                                    EventHandler h = readValue(rs.getString(1), EventHandler.class);
                                    if (!activeOnly || h.isActive()) {
                                        handlers.add(h);
                                    }
                                }

                                return handlers;
                            });
                });
    }

    /**
     * Use {@link Preconditions} to check for required {@link TaskDef} fields, throwing a Runtime
     * exception if validations fail.
     *
     * @param taskDef The {@code TaskDef} to check.
     */
    private void validate(TaskDef taskDef) {
        Preconditions.checkNotNull(taskDef, "TaskDef object cannot be null");
        Preconditions.checkNotNull(taskDef.getName(), "TaskDef name cannot be null");
    }

    /**
     * Use {@link Preconditions} to check for required {@link WorkflowDef} fields, throwing a
     * Runtime exception if validations fail.
     *
     * @param def The {@code WorkflowDef} to check.
     */
    private void validate(WorkflowDef def) {
        Preconditions.checkNotNull(def, "WorkflowDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "WorkflowDef name cannot be null");
    }

    /**
     * Retrieve a {@link EventHandler} by {@literal name}.
     *
     * @param connection The {@link Connection} to use for queries.
     * @param name The {@code EventHandler} name to look for.
     * @return {@literal null} if nothing is found, otherwise the {@code EventHandler}.
     */
    private EventHandler getEventHandler(Connection connection, String name) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_event_handler")
                        .where("name = :name")
                        .bind("name", name);

        return query(
                connection,
                builder,
                QueryContext.read("meta_event_handler"),
                q -> q.executeAndFetchFirst(EventHandler.class));
    }

    /**
     * Check if a {@link WorkflowDef} with the same {@literal name} and {@literal version} already
     * exist.
     *
     * @param connection The {@link Connection} to use for queries.
     * @param def The {@code WorkflowDef} to check for.
     * @return {@literal true} if a {@code WorkflowDef} already exists with the same values.
     */
    private Boolean workflowExists(Connection connection, WorkflowDef def) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("COUNT(*)")
                        .from("meta_workflow_def")
                        .where("name = :name")
                        .and("version = :version")
                        .bind("name", def.getName())
                        .bind("version", def.getVersion());

        return query(connection, builder, QueryContext.read("meta_workflow_def"), q -> q.exists());
    }

    /**
     * Return the latest version that exists for the provided {@code name}.
     *
     * @param tx The {@link Connection} to use for queries.
     * @param name The {@code name} to check for.
     * @return {@code Optional.empty()} if no versions exist, otherwise the max {@link
     *     WorkflowDef#getVersion} found.
     */
    private Optional<Integer> getLatestVersion(Connection tx, String name) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("max(version) AS version")
                        .from("meta_workflow_def")
                        .where("name = :name")
                        .bind("name", name);

        Integer val =
                query(
                        tx,
                        builder,
                        QueryContext.read("meta_workflow_def"),
                        q ->
                                q.executeAndFetch(
                                        rs -> {
                                            if (!rs.next()) {
                                                return null;
                                            }

                                            return rs.getInt(1);
                                        }));

        return Optional.ofNullable(val);
    }

    /**
     * Update the latest version for the workflow with name {@code WorkflowDef} to the version
     * provided in {@literal version}.
     *
     * @param tx The {@link Connection} to use for queries.
     * @param name Workflow def name to update
     * @param version The new latest {@code version} value.
     */
    private void updateLatestVersion(Connection tx, String name, int version) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("UPDATE meta_workflow_def SET latest_version = :version")
                        .where("name = :name")
                        .bind("version", version)
                        .bind("name", name);

        execute(tx, builder, QueryContext.write("meta_workflow_def"));
    }

    private void insertOrUpdateWorkflowDef(Connection tx, WorkflowDef def) {
        Optional<Integer> version = getLatestVersion(tx, def.getName());
        if (!workflowExists(tx, def)) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("meta_workflow_def")
                            .column("name", def.getName())
                            .column("version", def.getVersion())
                            .column("json_data", toJson(def));
            execute(tx, insert, QueryContext.write("meta_workflow_def"));
        } else {
            SqlQueryBuilder update =
                    SqlQueryBuilder.create()
                            .raw(
                                    "UPDATE meta_workflow_def SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP")
                            .where("name = :name")
                            .and("version = :version")
                            .bind("jsonData", toJson(def))
                            .bind("name", def.getName())
                            .bind("version", def.getVersion());

            execute(tx, update, QueryContext.write("meta_workflow_def"));
        }
        int maxVersion = def.getVersion();
        if (version.isPresent() && version.get() > def.getVersion()) {
            maxVersion = version.get();
        }

        updateLatestVersion(tx, def.getName(), maxVersion);
    }

    /**
     * Query persistence for all defined {@link TaskDef} data, and cache it in {@link
     * #taskDefCache}.
     */
    private void refreshTaskDefs() {
        try {
            withTransaction(
                    tx -> {
                        Map<String, TaskDef> map = new HashMap<>();
                        findAllTaskDefs(tx).forEach(taskDef -> map.put(taskDef.getName(), taskDef));

                        synchronized (taskDefCache) {
                            taskDefCache.clear();
                            taskDefCache.putAll(map);
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("Refreshed {} TaskDefs", taskDefCache.size());
                        }
                    });
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "refreshTaskDefs");
            logger.error("refresh TaskDefs failed ", e);
        }
    }

    /**
     * Query persistence for all defined {@link TaskDef} data.
     *
     * @param tx The {@link Connection} to use for queries.
     * @return A new {@code List<TaskDef>} with all the {@code TaskDef} data that was retrieved.
     */
    private List<TaskDef> findAllTaskDefs(Connection tx) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create().select("json_data").from("meta_task_def");

        return query(
                tx,
                builder,
                QueryContext.read("meta_task_def"),
                q -> q.executeAndFetch(TaskDef.class));
    }

    /**
     * Explicitly retrieves a {@link TaskDef} from persistence, avoiding {@link #taskDefCache}.
     *
     * @param name The name of the {@code TaskDef} to query for.
     * @return {@literal null} if nothing is found, otherwise the {@code TaskDef}.
     */
    private TaskDef getTaskDefFromDB(String name) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("meta_task_def")
                        .where("name = :name")
                        .bind("name", name);

        return queryWithTransaction(
                builder,
                QueryContext.read("meta_task_def"),
                q -> q.executeAndFetchFirst(TaskDef.class));
    }

    private String insertOrUpdateTaskDef(TaskDef taskDef) {
        return getWithRetriedTransactions(
                tx -> {
                    SqlQueryBuilder update =
                            SqlQueryBuilder.create()
                                    .raw(
                                            "UPDATE meta_task_def SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP")
                                    .where("name = :name")
                                    .bind("jsonData", toJson(taskDef))
                                    .bind("name", taskDef.getName());
                    int result = execute(tx, update, QueryContext.write("meta_task_def"));
                    if (result == 0) {
                        SqlInsertBuilder insert =
                                SqlInsertBuilder.create()
                                        .into("meta_task_def")
                                        .column("name", taskDef.getName())
                                        .column("json_data", toJson(taskDef));
                        execute(tx, insert, QueryContext.write("meta_task_def"));
                    }

                    taskDefCache.put(taskDef.getName(), taskDef);
                    return taskDef.getName();
                });
    }
}

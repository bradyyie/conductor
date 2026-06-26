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
package com.netflix.conductor.postgres.dao;

import java.sql.Connection;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.conductoross.conductor.persistence.query.QueryContext;
import org.conductoross.conductor.persistence.query.SqlInsertBuilder;
import org.conductoross.conductor.persistence.query.SqlQueryBuilder;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.exception.NonTransientException;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.postgres.util.ExecutorsUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import jakarta.annotation.*;

public class PostgresExecutionDAO extends PostgresBaseDAO
        implements ExecutionDAO, RateLimitingDAO, ConcurrentExecutionLimitDAO {

    private final ScheduledExecutorService scheduledExecutorService;
    private final QueueDAO queueDAO;

    public PostgresExecutionDAO(
            RetryTemplate retryTemplate,
            ObjectMapper objectMapper,
            DataSource dataSource,
            QueueDAO queueDAO) {
        super(retryTemplate, objectMapper, dataSource);
        this.queueDAO = queueDAO;
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        ExecutorsUtil.newNamedThreadFactory("postgres-execution-"));
    }

    private static String dateStr(Long timeInMs) {
        Date date = new Date(timeInMs);
        return dateStr(date);
    }

    private static String dateStr(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(date);
    }

    @PreDestroy
    public void destroy() {
        try {
            this.scheduledExecutorService.shutdown();
            if (scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.debug("tasks completed, shutting down");
            } else {
                logger.warn("Forcing shutdown after waiting for 30 seconds");
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            logger.warn(
                    "Shutdown interrupted, invoking shutdownNow on scheduledExecutorService for removeWorkflowWithExpiry",
                    ie);
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public List<TaskModel> getPendingTasksByWorkflow(String taskDefName, String workflowId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("task_in_progress tip INNER JOIN task t ON t.task_id = tip.task_id")
                        .where("task_def_name = :taskDefName")
                        .and("workflow_id = :workflowId")
                        .bind("taskDefName", taskDefName)
                        .bind("workflowId", workflowId)
                        .trailing("FOR SHARE");

        return queryWithTransaction(
                builder,
                QueryContext.read("task_in_progress"),
                q -> q.executeAndFetch(TaskModel.class));
    }

    @Override
    public List<TaskModel> getTasks(String taskDefName, String startKey, int count) {
        List<TaskModel> tasks = new ArrayList<>(count);

        List<TaskModel> pendingTasks = getPendingTasksForTaskType(taskDefName);
        boolean startKeyFound = startKey == null;
        int found = 0;
        for (TaskModel pendingTask : pendingTasks) {
            if (!startKeyFound) {
                if (pendingTask.getTaskId().equals(startKey)) {
                    startKeyFound = true;
                    // noinspection ConstantConditions
                    if (startKey != null) {
                        continue;
                    }
                }
            }
            if (startKeyFound && found < count) {
                tasks.add(pendingTask);
                found++;
            }
        }

        return tasks;
    }

    private static String taskKey(TaskModel task) {
        return task.getReferenceTaskName() + "_" + task.getRetryCount();
    }

    @Override
    public List<TaskModel> createTasks(List<TaskModel> tasks) {
        List<TaskModel> created = Lists.newArrayListWithCapacity(tasks.size());

        withTransaction(
                connection -> {
                    for (TaskModel task : tasks) {

                        validate(task);

                        task.setScheduledTime(System.currentTimeMillis());

                        final String taskKey = taskKey(task);

                        boolean scheduledTaskAdded = addScheduledTask(connection, task, taskKey);

                        if (!scheduledTaskAdded) {
                            logger.trace(
                                    "Task already scheduled, skipping the run "
                                            + task.getTaskId()
                                            + ", ref="
                                            + task.getReferenceTaskName()
                                            + ", key="
                                            + taskKey);
                            continue;
                        }

                        insertOrUpdateTaskData(connection, task);
                        addWorkflowToTaskMapping(connection, task);
                        addTaskInProgress(connection, task);
                        updateTask(connection, task);

                        created.add(task);
                    }
                });

        return created;
    }

    @Override
    public void updateTask(TaskModel task) {
        withTransaction(connection -> updateTask(connection, task));
        Optional<TaskDef> taskDefinition = task.getTaskDefinition();
        if (taskDefinition.isPresent()
                && taskDefinition.get().concurrencyLimit() > 0
                && task.getStatus() != null
                && task.getStatus().isTerminal()) {
            String queueName = QueueUtils.getQueueName(task);
            List<String> nextIds = queueDAO.peekFirstIds(queueName, 1);
            if (nextIds != null && !nextIds.isEmpty()) {
                logger.debug(
                        "Concurrency slot freed for {}, releasing postponed task {}",
                        task.getTaskDefName(),
                        nextIds.get(0));
                queueDAO.resetOffsetTime(queueName, nextIds.get(0));
            }
        }
    }

    /**
     * This is a dummy implementation and this feature is not for Postgres backed Conductor
     *
     * @param task: which needs to be evaluated whether it is rateLimited or not
     */
    @Override
    public boolean exceedsRateLimitPerFrequency(TaskModel task, TaskDef taskDef) {
        return false;
    }

    @Override
    public boolean exceedsLimit(TaskModel task) {

        Optional<TaskDef> taskDefinition = task.getTaskDefinition();
        if (taskDefinition.isEmpty()) {
            return false;
        }

        TaskDef taskDef = taskDefinition.get();

        int limit = taskDef.concurrencyLimit();
        if (limit <= 0) {
            return false;
        }

        long current = getInProgressTaskCount(task.getTaskDefName());

        if (current >= limit) {
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
            return true;
        }

        logger.info(
                "Task execution count for {}: limit={}, current={}",
                task.getTaskDefName(),
                limit,
                getInProgressTaskCount(task.getTaskDefName()));

        String taskId = task.getTaskId();

        List<String> tasksInProgressInOrderOfArrival =
                findAllTasksInProgressInOrderOfArrival(task, limit);

        boolean rateLimited = !tasksInProgressInOrderOfArrival.contains(taskId);

        if (rateLimited) {
            logger.info(
                    "Task execution count limited. {}, limit {}, current {}",
                    task.getTaskDefName(),
                    limit,
                    getInProgressTaskCount(task.getTaskDefName()));
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
        }

        return rateLimited;
    }

    @Override
    public boolean removeTask(String taskId) {
        TaskModel task = getTask(taskId);

        if (task == null) {
            logger.warn("No such task found by id {}", taskId);
            return false;
        }

        final String taskKey = taskKey(task);

        withTransaction(
                connection -> {
                    removeScheduledTask(connection, task, taskKey);
                    removeWorkflowToTaskMapping(connection, task);
                    removeTaskInProgress(connection, task);
                    removeTaskData(connection, task);
                });
        return true;
    }

    @Override
    public TaskModel getTask(String taskId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("task")
                        .where("task_id = :taskId")
                        .bind("taskId", taskId);
        return queryWithTransaction(
                builder, QueryContext.read("task"), q -> q.executeAndFetchFirst(TaskModel.class));
    }

    @Override
    public List<TaskModel> getTasks(List<String> taskIds) {
        if (taskIds.isEmpty()) {
            return Lists.newArrayList();
        }
        return getWithRetriedTransactions(c -> getTasks(c, taskIds));
    }

    @Override
    public List<TaskModel> getPendingTasksForTaskType(String taskName) {
        Preconditions.checkNotNull(taskName, "task name cannot be null");
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("task_in_progress tip INNER JOIN task t ON t.task_id = tip.task_id")
                        .where("task_def_name = :taskName")
                        .bind("taskName", taskName)
                        .trailing("FOR UPDATE SKIP LOCKED");

        return queryWithTransaction(
                builder,
                QueryContext.read("task_in_progress"),
                q -> q.executeAndFetch(TaskModel.class));
    }

    @Override
    public List<TaskModel> getTasksForWorkflow(String workflowId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("task_id")
                        .from("workflow_to_task")
                        .where("workflow_id = :workflowId")
                        .bind("workflowId", workflowId)
                        .trailing("FOR SHARE");
        return getWithRetriedTransactions(
                tx ->
                        query(
                                tx,
                                builder,
                                QueryContext.read("workflow_to_task"),
                                q -> {
                                    List<String> taskIds = q.executeScalarList(String.class);
                                    return getTasks(tx, taskIds);
                                }));
    }

    @Override
    public String createWorkflow(WorkflowModel workflow) {
        return insertOrUpdateWorkflow(workflow, false);
    }

    @Override
    public String updateWorkflow(WorkflowModel workflow) {
        return insertOrUpdateWorkflow(workflow, true);
    }

    @Override
    public boolean removeWorkflow(String workflowId) {
        boolean removed = false;
        WorkflowModel workflow = getWorkflow(workflowId, true);
        if (workflow != null) {
            withTransaction(
                    connection -> {
                        removeWorkflowDefToWorkflowMapping(connection, workflow);
                        removeWorkflow(connection, workflowId);
                        removePendingWorkflow(connection, workflow.getWorkflowName(), workflowId);
                    });
            removed = true;

            for (TaskModel task : workflow.getTasks()) {
                if (!removeTask(task.getTaskId())) {
                    removed = false;
                }
            }
        }
        return removed;
    }

    /** Scheduled executor based implementation. */
    @Override
    public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
        scheduledExecutorService.schedule(
                () -> {
                    try {
                        removeWorkflow(workflowId);
                    } catch (Throwable e) {
                        logger.warn("Unable to remove workflow: {} with expiry", workflowId, e);
                    }
                },
                ttlSeconds,
                TimeUnit.SECONDS);

        return true;
    }

    @Override
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {
        withTransaction(connection -> removePendingWorkflow(connection, workflowType, workflowId));
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId) {
        return getWorkflow(workflowId, true);
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
        WorkflowModel workflow = getWithRetriedTransactions(tx -> readWorkflow(tx, workflowId));

        if (workflow != null) {
            if (includeTasks) {
                List<TaskModel> tasks = getTasksForWorkflow(workflowId);
                tasks.sort(Comparator.comparingInt(TaskModel::getSeq));
                workflow.setTasks(tasks);
            }
        }
        return workflow;
    }

    /**
     * @param workflowName name of the workflow
     * @param version the workflow version
     * @return list of workflow ids that are in RUNNING state <em>returns workflows of all versions
     *     for the given workflow name</em>
     */
    @Override
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("workflow_id")
                        .from("workflow_pending")
                        .where("workflow_type = :workflowName")
                        .bind("workflowName", workflowName)
                        .trailing("FOR SHARE SKIP LOCKED");

        return queryWithTransaction(
                builder,
                QueryContext.read("workflow_pending"),
                q -> q.executeScalarList(String.class));
    }

    /**
     * @param workflowName Name of the workflow
     * @param version the workflow version
     * @return list of workflows that are in RUNNING state
     */
    @Override
    public List<WorkflowModel> getPendingWorkflowsByType(String workflowName, int version) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        return getRunningWorkflowIds(workflowName, version).stream()
                .map(this::getWorkflow)
                .filter(workflow -> workflow.getWorkflowVersion() == version)
                .collect(Collectors.toList());
    }

    @Override
    public long getPendingWorkflowCount(String workflowName) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("COUNT(*)")
                        .from("workflow_pending")
                        .where("workflow_type = :workflowName")
                        .bind("workflowName", workflowName);

        return queryWithTransaction(
                builder, QueryContext.read("workflow_pending"), q -> q.executeCount());
    }

    @Override
    public long getInProgressTaskCount(String taskDefName) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("COUNT(*)")
                        .from("task_in_progress")
                        .where("task_def_name = :taskDefName")
                        .and("in_progress_status = true")
                        .bind("taskDefName", taskDefName);

        return queryWithTransaction(
                builder, QueryContext.read("task_in_progress"), q -> q.executeCount());
    }

    @Override
    public List<WorkflowModel> getWorkflowsByType(
            String workflowName, Long startTime, Long endTime) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        Preconditions.checkNotNull(startTime, "startTime cannot be null");
        Preconditions.checkNotNull(endTime, "endTime cannot be null");

        List<WorkflowModel> workflows = new LinkedList<>();

        withTransaction(
                tx -> {
                    SqlQueryBuilder builder =
                            SqlQueryBuilder.create()
                                    .select("workflow_id")
                                    .from("workflow_def_to_workflow")
                                    .where("workflow_def = :workflowName")
                                    .and("date_str BETWEEN :startDate AND :endDate")
                                    .bind("workflowName", workflowName)
                                    .bind("startDate", dateStr(startTime))
                                    .bind("endDate", dateStr(endTime))
                                    .trailing("FOR SHARE SKIP LOCKED");

                    List<String> workflowIds =
                            query(
                                    tx,
                                    builder,
                                    QueryContext.read("workflow_def_to_workflow"),
                                    q -> q.executeScalarList(String.class));
                    workflowIds.forEach(
                            workflowId -> {
                                try {
                                    WorkflowModel wf = getWorkflow(workflowId);
                                    if (wf.getCreateTime() >= startTime
                                            && wf.getCreateTime() <= endTime) {
                                        workflows.add(wf);
                                    }
                                } catch (Exception e) {
                                    logger.error(
                                            "Unable to load workflow id {} with name {}",
                                            workflowId,
                                            workflowName,
                                            e);
                                }
                            });
                });

        return workflows;
    }

    @Override
    public List<WorkflowModel> getWorkflowsByCorrelationId(
            String workflowName, String correlationId, boolean includeTasks) {
        Preconditions.checkNotNull(correlationId, "correlationId cannot be null");
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("w.json_data")
                        .from(
                                "workflow w left join workflow_def_to_workflow wd on w.workflow_id = wd.workflow_id")
                        .where("w.correlation_id = :correlationId")
                        .and("wd.workflow_def = :workflowName")
                        .bind("correlationId", correlationId)
                        .bind("workflowName", workflowName)
                        .trailing("FOR SHARE SKIP LOCKED");

        return queryWithTransaction(
                builder,
                QueryContext.read("workflow"),
                q -> q.executeAndFetch(WorkflowModel.class));
    }

    @Override
    public boolean canSearchAcrossWorkflows() {
        return true;
    }

    @Override
    public boolean addEventExecution(EventExecution eventExecution) {
        try {
            return getWithRetriedTransactions(tx -> insertEventExecution(tx, eventExecution));
        } catch (Exception e) {
            throw new NonTransientException(
                    "Unable to add event execution " + eventExecution.getId(), e);
        }
    }

    @Override
    public void removeEventExecution(EventExecution eventExecution) {
        try {
            withTransaction(tx -> removeEventExecution(tx, eventExecution));
        } catch (Exception e) {
            throw new NonTransientException(
                    "Unable to remove event execution " + eventExecution.getId(), e);
        }
    }

    @Override
    public void updateEventExecution(EventExecution eventExecution) {
        try {
            withTransaction(tx -> updateEventExecution(tx, eventExecution));
        } catch (Exception e) {
            throw new NonTransientException(
                    "Unable to update event execution " + eventExecution.getId(), e);
        }
    }

    public List<EventExecution> getEventExecutions(
            String eventHandlerName, String eventName, String messageId, int max) {
        try {
            List<EventExecution> executions = Lists.newLinkedList();
            withTransaction(
                    tx -> {
                        for (int i = 0; i < max; i++) {
                            String executionId =
                                    messageId + "_"
                                            + i; // see SimpleEventProcessor.handle to understand
                            // how the
                            // execution id is set
                            EventExecution ee =
                                    readEventExecution(
                                            tx,
                                            eventHandlerName,
                                            eventName,
                                            messageId,
                                            executionId);
                            if (ee == null) {
                                break;
                            }
                            executions.add(ee);
                        }
                    });
            return executions;
        } catch (Exception e) {
            String message =
                    String.format(
                            "Unable to get event executions for eventHandlerName=%s, eventName=%s, messageId=%s",
                            eventHandlerName, eventName, messageId);
            throw new NonTransientException(message, e);
        }
    }

    private List<TaskModel> getTasks(Connection connection, List<String> taskIds) {
        if (taskIds.isEmpty()) {
            return Lists.newArrayList();
        }

        // Build a variable-length IN clause using named markers so the binds stay positional and
        // the applyQueryExtensions hook can still scope the WHERE.
        SqlQueryBuilder builder = SqlQueryBuilder.create().select("json_data").from("task");
        List<String> markers = new ArrayList<>(taskIds.size());
        for (int i = 0; i < taskIds.size(); i++) {
            String name = "taskId" + i;
            markers.add(":" + name);
            builder.bind(name, taskIds.get(i));
        }
        builder.where("task_id IN (" + String.join(", ", markers) + ")")
                .and("json_data IS NOT NULL");

        return query(
                connection,
                builder,
                QueryContext.read("task"),
                q -> q.executeAndFetch(TaskModel.class));
    }

    private String insertOrUpdateWorkflow(WorkflowModel workflow, boolean update) {
        Preconditions.checkNotNull(workflow, "workflow object cannot be null");

        boolean terminal = workflow.getStatus().isTerminal();

        List<TaskModel> tasks = workflow.getTasks();
        workflow.setTasks(Lists.newLinkedList());

        withTransaction(
                tx -> {
                    if (!update) {
                        addWorkflow(tx, workflow);
                        addWorkflowDefToWorkflowMapping(tx, workflow);
                    } else {
                        updateWorkflow(tx, workflow);
                    }

                    if (terminal) {
                        removePendingWorkflow(
                                tx, workflow.getWorkflowName(), workflow.getWorkflowId());
                    } else {
                        addPendingWorkflow(
                                tx, workflow.getWorkflowName(), workflow.getWorkflowId());
                    }
                });

        workflow.setTasks(tasks);
        return workflow.getWorkflowId();
    }

    private void updateTask(Connection connection, TaskModel task) {
        Optional<TaskDef> taskDefinition = task.getTaskDefinition();

        if (taskDefinition.isPresent() && taskDefinition.get().concurrencyLimit() > 0) {
            boolean inProgress =
                    task.getStatus() != null
                            && task.getStatus().equals(TaskModel.Status.IN_PROGRESS);
            updateInProgressStatus(connection, task, inProgress);
        }

        insertOrUpdateTaskData(connection, task);

        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            removeTaskInProgress(connection, task);
        }

        addWorkflowToTaskMapping(connection, task);
    }

    private WorkflowModel readWorkflow(Connection connection, String workflowId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("workflow")
                        .where("workflow_id = :workflowId")
                        .bind("workflowId", workflowId);

        return query(
                connection,
                builder,
                QueryContext.read("workflow"),
                q -> q.executeAndFetchFirst(WorkflowModel.class));
    }

    private void addWorkflow(Connection connection, WorkflowModel workflow) {
        // Built via SqlInsertBuilder so the generic applyWriteExtensions seam runs before render
        // (no-op in OSS; an enterprise subclass adds e.g. an org_id column). Renders identically to
        // the previous hand-written INSERT.
        SqlInsertBuilder insert =
                SqlInsertBuilder.create()
                        .into("workflow")
                        .column("workflow_id", workflow.getWorkflowId())
                        .column("correlation_id", workflow.getCorrelationId())
                        .column("json_data", toJson(workflow));
        execute(connection, insert, QueryContext.write("workflow"));
    }

    private void updateWorkflow(Connection connection, WorkflowModel workflow) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE workflow SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP")
                        .where("workflow_id = :workflowId")
                        .bind("jsonData", toJson(workflow))
                        .bind("workflowId", workflow.getWorkflowId());

        execute(connection, builder, QueryContext.write("workflow"));
    }

    private void removeWorkflow(Connection connection, String workflowId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM workflow")
                        .where("workflow_id = :workflowId")
                        .bind("workflowId", workflowId);
        execute(connection, builder, QueryContext.write("workflow"));
    }

    private void addPendingWorkflow(Connection connection, String workflowType, String workflowId) {

        SqlQueryBuilder existsQuery =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("workflow_pending")
                        .where("workflow_type = :workflowType")
                        .and("workflow_id = :workflowId")
                        .bind("workflowType", workflowType)
                        .bind("workflowId", workflowId);

        boolean exists =
                query(
                        connection,
                        existsQuery,
                        QueryContext.read("workflow_pending"),
                        q -> q.exists());

        if (!exists) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("workflow_pending")
                            .column("workflow_type", workflowType)
                            .column("workflow_id", workflowId)
                            .onConflict("workflow_type", "workflow_id")
                            .onConflictDoNothing();

            execute(connection, insert, QueryContext.write("workflow_pending"));
        }
    }

    private void removePendingWorkflow(
            Connection connection, String workflowType, String workflowId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM workflow_pending")
                        .where("workflow_type = :workflowType")
                        .and("workflow_id = :workflowId")
                        .bind("workflowType", workflowType)
                        .bind("workflowId", workflowId);

        execute(connection, builder, QueryContext.write("workflow_pending"));
    }

    private void insertOrUpdateTaskData(Connection connection, TaskModel task) {
        /*
         * Most times the row will be updated so let's try the update first. This used to be an 'INSERT/ON CONFLICT do update' sql statement. The problem with that
         * is that if we try the INSERT first, the sequence will be increased even if the ON CONFLICT happens.
         */
        SqlQueryBuilder update =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE task SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP")
                        .where("task_id = :taskId")
                        .bind("jsonData", toJson(task))
                        .bind("taskId", task.getTaskId());
        int rowsUpdated = execute(connection, update, QueryContext.write("task"));

        if (rowsUpdated == 0) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("task")
                            .column("task_id", task.getTaskId())
                            .column("json_data", toJson(task))
                            .columnRaw("modified_on", "CURRENT_TIMESTAMP")
                            .onConflict("task_id")
                            .doUpdateSet(
                                    "json_data = excluded.json_data",
                                    "modified_on = excluded.modified_on");
            execute(connection, insert, QueryContext.write("task"));
        }
    }

    private void removeTaskData(Connection connection, TaskModel task) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM task")
                        .where("task_id = :taskId")
                        .bind("taskId", task.getTaskId());
        execute(connection, builder, QueryContext.write("task"));
    }

    private void addWorkflowToTaskMapping(Connection connection, TaskModel task) {

        SqlQueryBuilder existsQuery =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("workflow_to_task")
                        .where("workflow_id = :workflowId")
                        .and("task_id = :taskId")
                        .bind("workflowId", task.getWorkflowInstanceId())
                        .bind("taskId", task.getTaskId());

        boolean exists =
                query(
                        connection,
                        existsQuery,
                        QueryContext.read("workflow_to_task"),
                        q -> q.exists());

        if (!exists) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("workflow_to_task")
                            .column("workflow_id", task.getWorkflowInstanceId())
                            .column("task_id", task.getTaskId())
                            .onConflict("workflow_id", "task_id")
                            .onConflictDoNothing();

            execute(connection, insert, QueryContext.write("workflow_to_task"));
        }
    }

    private void removeWorkflowToTaskMapping(Connection connection, TaskModel task) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM workflow_to_task")
                        .where("workflow_id = :workflowId")
                        .and("task_id = :taskId")
                        .bind("workflowId", task.getWorkflowInstanceId())
                        .bind("taskId", task.getTaskId());

        execute(connection, builder, QueryContext.write("workflow_to_task"));
    }

    private void addWorkflowDefToWorkflowMapping(Connection connection, WorkflowModel workflow) {
        SqlInsertBuilder insert =
                SqlInsertBuilder.create()
                        .into("workflow_def_to_workflow")
                        .column("workflow_def", workflow.getWorkflowName())
                        .column("date_str", dateStr(workflow.getCreateTime()))
                        .column("workflow_id", workflow.getWorkflowId());

        execute(connection, insert, QueryContext.write("workflow_def_to_workflow"));
    }

    private void removeWorkflowDefToWorkflowMapping(Connection connection, WorkflowModel workflow) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM workflow_def_to_workflow")
                        .where("workflow_def = :workflowDef")
                        .and("date_str = :dateStr")
                        .and("workflow_id = :workflowId")
                        .bind("workflowDef", workflow.getWorkflowName())
                        .bind("dateStr", dateStr(workflow.getCreateTime()))
                        .bind("workflowId", workflow.getWorkflowId());

        execute(connection, builder, QueryContext.write("workflow_def_to_workflow"));
    }

    @VisibleForTesting
    boolean addScheduledTask(Connection connection, TaskModel task, String taskKey) {

        SqlQueryBuilder existsQuery =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("task_scheduled")
                        .where("workflow_id = :workflowId")
                        .and("task_key = :taskKey")
                        .bind("workflowId", task.getWorkflowInstanceId())
                        .bind("taskKey", taskKey);

        boolean exists =
                query(
                        connection,
                        existsQuery,
                        QueryContext.read("task_scheduled"),
                        q -> q.exists());

        if (!exists) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("task_scheduled")
                            .column("workflow_id", task.getWorkflowInstanceId())
                            .column("task_key", taskKey)
                            .column("task_id", task.getTaskId())
                            .onConflict("workflow_id", "task_key")
                            .onConflictDoNothing();

            int count = execute(connection, insert, QueryContext.write("task_scheduled"));
            return count > 0;
        } else {
            return false;
        }
    }

    private void removeScheduledTask(Connection connection, TaskModel task, String taskKey) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM task_scheduled")
                        .where("workflow_id = :workflowId")
                        .and("task_key = :taskKey")
                        .bind("workflowId", task.getWorkflowInstanceId())
                        .bind("taskKey", taskKey);
        execute(connection, builder, QueryContext.write("task_scheduled"));
    }

    private void addTaskInProgress(Connection connection, TaskModel task) {
        SqlQueryBuilder existsQuery =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("task_in_progress")
                        .where("task_def_name = :taskDefName")
                        .and("task_id = :taskId")
                        .bind("taskDefName", task.getTaskDefName())
                        .bind("taskId", task.getTaskId());

        boolean exists =
                query(
                        connection,
                        existsQuery,
                        QueryContext.read("task_in_progress"),
                        q -> q.exists());

        if (!exists) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("task_in_progress")
                            .column("task_def_name", task.getTaskDefName())
                            .column("task_id", task.getTaskId())
                            .column("workflow_id", task.getWorkflowInstanceId());

            execute(connection, insert, QueryContext.write("task_in_progress"));
        }
    }

    private void removeTaskInProgress(Connection connection, TaskModel task) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM task_in_progress")
                        .where("task_def_name = :taskDefName")
                        .and("task_id = :taskId")
                        .bind("taskDefName", task.getTaskDefName())
                        .bind("taskId", task.getTaskId());

        execute(connection, builder, QueryContext.write("task_in_progress"));
    }

    private void updateInProgressStatus(Connection connection, TaskModel task, boolean inProgress) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE task_in_progress SET in_progress_status = :inProgress, modified_on = CURRENT_TIMESTAMP")
                        .where("task_def_name = :taskDefName")
                        .and("task_id = :taskId")
                        .bind("inProgress", inProgress)
                        .bind("taskDefName", task.getTaskDefName())
                        .bind("taskId", task.getTaskId());

        execute(connection, builder, QueryContext.write("task_in_progress"));
    }

    private boolean insertEventExecution(Connection connection, EventExecution eventExecution) {
        SqlInsertBuilder insert =
                SqlInsertBuilder.create()
                        .into("event_execution")
                        .column("event_handler_name", eventExecution.getName())
                        .column("event_name", eventExecution.getEvent())
                        .column("message_id", eventExecution.getMessageId())
                        .column("execution_id", eventExecution.getId())
                        .column("json_data", toJson(eventExecution))
                        .onConflictDoNothing();
        int count = execute(connection, insert, QueryContext.write("event_execution"));
        return count > 0;
    }

    private void updateEventExecution(Connection connection, EventExecution eventExecution) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE event_execution SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP")
                        .where("event_handler_name = :eventHandlerName")
                        .and("event_name = :eventName")
                        .and("message_id = :messageId")
                        .and("execution_id = :executionId")
                        .bind("jsonData", toJson(eventExecution))
                        .bind("eventHandlerName", eventExecution.getName())
                        .bind("eventName", eventExecution.getEvent())
                        .bind("messageId", eventExecution.getMessageId())
                        .bind("executionId", eventExecution.getId());

        execute(connection, builder, QueryContext.write("event_execution"));
    }

    private void removeEventExecution(Connection connection, EventExecution eventExecution) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM event_execution")
                        .where("event_handler_name = :eventHandlerName")
                        .and("event_name = :eventName")
                        .and("message_id = :messageId")
                        .and("execution_id = :executionId")
                        .bind("eventHandlerName", eventExecution.getName())
                        .bind("eventName", eventExecution.getEvent())
                        .bind("messageId", eventExecution.getMessageId())
                        .bind("executionId", eventExecution.getId());

        execute(connection, builder, QueryContext.write("event_execution"));
    }

    private EventExecution readEventExecution(
            Connection connection,
            String eventHandlerName,
            String eventName,
            String messageId,
            String executionId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("json_data")
                        .from("event_execution")
                        .where("event_handler_name = :eventHandlerName")
                        .and("event_name = :eventName")
                        .and("message_id = :messageId")
                        .and("execution_id = :executionId")
                        .bind("eventHandlerName", eventHandlerName)
                        .bind("eventName", eventName)
                        .bind("messageId", messageId)
                        .bind("executionId", executionId);
        return query(
                connection,
                builder,
                QueryContext.read("event_execution"),
                q -> q.executeAndFetchFirst(EventExecution.class));
    }

    private List<String> findAllTasksInProgressInOrderOfArrival(TaskModel task, int limit) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("task_id")
                        .from("task_in_progress")
                        .where("task_def_name = :taskDefName")
                        .bind("taskDefName", task.getTaskDefName())
                        .orderBy("created_on")
                        .limit(limit);

        return queryWithTransaction(
                builder,
                QueryContext.read("task_in_progress"),
                q -> q.executeScalarList(String.class));
    }

    private void validate(TaskModel task) {
        Preconditions.checkNotNull(task, "task object cannot be null");
        Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
        Preconditions.checkNotNull(
                task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
        Preconditions.checkNotNull(
                task.getReferenceTaskName(), "Task reference name cannot be null");
    }
}

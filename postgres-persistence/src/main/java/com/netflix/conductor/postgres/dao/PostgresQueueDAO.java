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

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.postgres.config.PostgresProperties;
import com.netflix.conductor.postgres.util.ExecutorsUtil;
import com.netflix.conductor.postgres.util.PostgresQueueListener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import jakarta.annotation.*;

public class PostgresQueueDAO extends PostgresBaseDAO implements QueueDAO {

    private static final Long UNACK_SCHEDULE_MS = 60_000L;

    private final ScheduledExecutorService scheduledExecutorService;

    private PostgresQueueListener queueListener;

    public PostgresQueueDAO(
            RetryTemplate retryTemplate,
            ObjectMapper objectMapper,
            DataSource dataSource,
            PostgresProperties properties) {
        super(retryTemplate, objectMapper, dataSource);

        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        ExecutorsUtil.newNamedThreadFactory("postgres-queue-"));
        this.scheduledExecutorService.scheduleAtFixedRate(
                this::processAllUnacks,
                UNACK_SCHEDULE_MS,
                UNACK_SCHEDULE_MS,
                TimeUnit.MILLISECONDS);
        logger.debug("{} is ready to serve", PostgresQueueDAO.class.getName());

        if (properties.getExperimentalQueueNotify()) {
            this.queueListener = new PostgresQueueListener(dataSource, properties);
        }
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
                    "Shutdown interrupted, invoking shutdownNow on scheduledExecutorService for processAllUnacks",
                    ie);
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void push(String queueName, String messageId, long offsetTimeInSecond) {
        push(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public void push(String queueName, String messageId, int priority, long offsetTimeInSecond) {
        withTransaction(
                tx -> pushMessage(tx, queueName, messageId, null, priority, offsetTimeInSecond));
    }

    @Override
    public void push(String queueName, List<Message> messages) {
        withTransaction(
                tx ->
                        messages.forEach(
                                message ->
                                        pushMessage(
                                                tx,
                                                queueName,
                                                message.getId(),
                                                message.getPayload(),
                                                message.getPriority(),
                                                0)));
    }

    @Override
    public boolean pushIfNotExists(String queueName, String messageId, long offsetTimeInSecond) {
        return pushIfNotExists(queueName, messageId, 0, offsetTimeInSecond);
    }

    @Override
    public boolean pushIfNotExists(
            String queueName, String messageId, int priority, long offsetTimeInSecond) {
        return getWithRetriedTransactions(
                tx -> {
                    if (!existsMessage(tx, queueName, messageId)) {
                        pushMessage(tx, queueName, messageId, null, priority, offsetTimeInSecond);
                        return true;
                    }
                    return false;
                });
    }

    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        return pollMessages(queueName, count, timeout).stream()
                .map(Message::getId)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> peekFirstIds(String queueName, int count) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("message_id")
                        .from("queue_message")
                        .where("queue_name = :queueName")
                        .and("popped = false")
                        .bind("queueName", queueName)
                        .orderBy("deliver_on", "priority DESC", "created_on")
                        .limit(count);
        return queryWithTransaction(
                builder,
                QueryContext.read("queue_message"),
                q -> q.executeScalarList(String.class));
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        if (timeout < 1) {
            List<Message> messages =
                    getWithTransactionWithOutErrorPropagation(
                            tx -> popMessages(tx, queueName, count, timeout));
            if (messages == null) {
                return new ArrayList<>();
            }
            return messages;
        }

        long start = System.currentTimeMillis();
        final List<Message> messages = new ArrayList<>();

        while (true) {
            List<Message> messagesSlice =
                    getWithTransactionWithOutErrorPropagation(
                            tx -> popMessages(tx, queueName, count - messages.size(), timeout));
            if (messagesSlice == null) {
                logger.warn(
                        "Unable to poll {} messages from {} due to tx conflict, only {} popped",
                        count,
                        queueName,
                        messages.size());
                // conflict could have happened, returned messages popped so far
                return messages;
            }

            messages.addAll(messagesSlice);
            if (messages.size() >= count || ((System.currentTimeMillis() - start) > timeout)) {
                return messages;
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void remove(String queueName, String messageId) {
        withTransaction(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public int getSize(String queueName) {
        if (queueListener != null) {
            Optional<Integer> size = queueListener.getSize(queueName);
            if (size.isPresent()) {
                return size.get();
            }
        }

        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("COUNT(*)")
                        .from("queue_message")
                        .where("queue_name = :queueName")
                        .bind("queueName", queueName);
        return queryWithTransaction(
                builder,
                QueryContext.read("queue_message"),
                q -> ((Long) q.executeCount()).intValue());
    }

    @Override
    public boolean ack(String queueName, String messageId) {
        return getWithRetriedTransactions(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE queue_message SET offset_time_seconds = :offset, deliver_on = (current_timestamp + (:offset ||' seconds')::interval)")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .bind("offset", updatedOffsetTimeInSecond)
                        .bind("queueName", queueName)
                        .bind("messageId", messageId);

        return queryWithTransaction(
                        builder, QueryContext.write("queue_message"), q -> q.executeUpdate())
                == 1;
    }

    @Override
    public boolean setUnackTimeoutIfShorter(String queueName, String messageId, long unackTimeout) {
        long updatedOffsetTimeInSecond = unackTimeout / 1000;

        // Only update when the proposed deliver_on is earlier than what is already set,
        // mirroring the ZADD LT semantics used by the Redis implementation.
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE queue_message SET offset_time_seconds = :offset, deliver_on = (current_timestamp + (:offset ||' seconds')::interval)")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .and("deliver_on > (current_timestamp + (:offset ||' seconds')::interval)")
                        .bind("offset", updatedOffsetTimeInSecond)
                        .bind("queueName", queueName)
                        .bind("messageId", messageId);

        return queryWithTransaction(
                        builder, QueryContext.write("queue_message"), q -> q.executeUpdate())
                == 1;
    }

    @Override
    public void flush(String queueName) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM queue_message")
                        .where("queue_name = :queueName")
                        .bind("queueName", queueName);
        withTransaction(tx -> execute(tx, builder, QueryContext.write("queue_message")));
    }

    @Override
    public Map<String, Long> queuesDetail() {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "SELECT queue_name, (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size FROM queue q")
                        .trailing("FOR SHARE SKIP LOCKED");
        return queryWithTransaction(
                builder,
                QueryContext.read("queue"),
                q ->
                        q.executeAndFetch(
                                rs -> {
                                    Map<String, Long> detail = Maps.newHashMap();
                                    while (rs.next()) {
                                        String queueName = rs.getString("queue_name");
                                        Long size = rs.getLong("size");
                                        detail.put(queueName, size);
                                    }
                                    return detail;
                                }));
    }

    @Override
    public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
        // @formatter:off
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "SELECT queue_name, \n"
                                        + "       (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size,\n"
                                        + "       (SELECT count(*) FROM queue_message WHERE popped = true AND queue_name = q.queue_name) AS uacked \n"
                                        + "FROM queue q")
                        .trailing("FOR SHARE SKIP LOCKED");
        // @formatter:on

        return queryWithTransaction(
                builder,
                QueryContext.read("queue"),
                q ->
                        q.executeAndFetch(
                                rs -> {
                                    Map<String, Map<String, Map<String, Long>>> result =
                                            Maps.newHashMap();
                                    while (rs.next()) {
                                        String queueName = rs.getString("queue_name");
                                        Long size = rs.getLong("size");
                                        Long queueUnacked = rs.getLong("uacked");
                                        result.put(
                                                queueName,
                                                ImmutableMap.of(
                                                        "a",
                                                        ImmutableMap
                                                                .of( // sharding not implemented,
                                                                        // returning only
                                                                        // one shard with all the
                                                                        // info
                                                                        "size",
                                                                        size,
                                                                        "uacked",
                                                                        queueUnacked)));
                                    }
                                    return result;
                                }));
    }

    /**
     * Un-pop all un-acknowledged messages for all queues.
     *
     * @since 1.11.6
     */
    public void processAllUnacks() {
        logger.trace("processAllUnacks started");

        getWithRetriedTransactions(
                tx -> {
                    SqlQueryBuilder lockTasks =
                            SqlQueryBuilder.create()
                                    .select("queue_name", "message_id")
                                    .from("queue_message")
                                    .where("popped = true")
                                    .and(
                                            "(deliver_on + (60 ||' seconds')::interval) < current_timestamp")
                                    .limit(1000)
                                    .trailing("FOR UPDATE SKIP LOCKED");

                    List<QueueMessage> messages =
                            query(
                                    tx,
                                    lockTasks,
                                    QueryContext.read("queue_message"),
                                    p ->
                                            p.executeAndFetch(
                                                    rs -> {
                                                        List<QueueMessage> results =
                                                                new ArrayList<QueueMessage>();
                                                        while (rs.next()) {
                                                            QueueMessage qm = new QueueMessage();
                                                            qm.queueName =
                                                                    rs.getString("queue_name");
                                                            qm.messageId =
                                                                    rs.getString("message_id");
                                                            results.add(qm);
                                                        }
                                                        return results;
                                                    }));

                    if (messages.size() == 0) {
                        return 0;
                    }

                    Map<String, List<String>> queueMessageMap = new HashMap<String, List<String>>();
                    for (QueueMessage qm : messages) {
                        if (!queueMessageMap.containsKey(qm.queueName)) {
                            queueMessageMap.put(qm.queueName, new ArrayList<String>());
                        }
                        queueMessageMap.get(qm.queueName).add(qm.messageId);
                    }

                    int totalUnacked = 0;
                    for (String queueName : queueMessageMap.keySet()) {
                        Integer unacked = 0;
                        ;
                        try {
                            final List<String> msgIds = queueMessageMap.get(queueName);
                            SqlQueryBuilder updatePopped =
                                    SqlQueryBuilder.create()
                                            .raw("UPDATE queue_message SET popped = false")
                                            .where("queue_name = :queueName")
                                            .bind("queueName", queueName);
                            List<String> markers = new ArrayList<>(msgIds.size());
                            for (int i = 0; i < msgIds.size(); i++) {
                                String name = "msgId" + i;
                                markers.add(":" + name);
                                updatePopped.bind(name, msgIds.get(i));
                            }
                            updatePopped.and("message_id IN (" + String.join(", ", markers) + ")");

                            unacked =
                                    execute(tx, updatePopped, QueryContext.write("queue_message"));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        totalUnacked += unacked;
                        logger.debug("Unacked {} messages from all queues", unacked);
                    }

                    if (totalUnacked > 0) {
                        logger.debug("Unacked {} messages from all queues", totalUnacked);
                    }
                    return totalUnacked;
                });
    }

    @Override
    public void processUnacks(String queueName) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("UPDATE queue_message SET popped = false")
                        .where("queue_name = :queueName")
                        .and("popped = true")
                        .and("(current_timestamp - (60 ||' seconds')::interval) > deliver_on")
                        .bind("queueName", queueName);
        withTransaction(tx -> execute(tx, builder, QueryContext.write("queue_message")));
    }

    @Override
    public boolean resetOffsetTime(String queueName, String messageId) {
        long offsetTimeInSecond = 0; // Reset to 0
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE queue_message SET offset_time_seconds = :offset, deliver_on = (current_timestamp + (:offset ||' seconds')::interval)")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .bind("offset", offsetTimeInSecond)
                        .bind("queueName", queueName)
                        .bind("messageId", messageId);

        return queryWithTransaction(
                        builder, QueryContext.write("queue_message"), q -> q.executeUpdate())
                == 1;
    }

    private boolean existsMessage(Connection connection, String queueName, String messageId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("queue_message")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .bind("queueName", queueName)
                        .bind("messageId", messageId)
                        .trailing("FOR SHARE");
        return query(connection, builder, QueryContext.read("queue_message"), q -> q.exists());
    }

    private void pushMessage(
            Connection connection,
            String queueName,
            String messageId,
            String payload,
            Integer priority,
            long offsetTimeInSecond) {

        createQueueIfNotExists(connection, queueName);

        SqlQueryBuilder update =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE queue_message SET payload = :payload, deliver_on = (current_timestamp + (:offset ||' seconds')::interval)")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .bind("payload", payload)
                        .bind("offset", offsetTimeInSecond)
                        .bind("queueName", queueName)
                        .bind("messageId", messageId);
        int rowsUpdated = execute(connection, update, QueryContext.write("queue_message"));

        if (rowsUpdated == 0) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("queue_message")
                            .columnRaw(
                                    "deliver_on",
                                    "(current_timestamp + (? ||' seconds')::interval)",
                                    List.of(offsetTimeInSecond))
                            .column("queue_name", queueName)
                            .column("message_id", messageId)
                            .column("priority", priority)
                            .column("offset_time_seconds", offsetTimeInSecond)
                            .column("payload", payload)
                            .onConflict("queue_name", "message_id")
                            .doUpdateSet(
                                    "payload = excluded.payload",
                                    "deliver_on = excluded.deliver_on");
            execute(connection, insert, QueryContext.write("queue_message"));
        }
    }

    private boolean removeMessage(Connection connection, String queueName, String messageId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("DELETE FROM queue_message")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .bind("queueName", queueName)
                        .bind("messageId", messageId);
        return query(
                connection, builder, QueryContext.write("queue_message"), q -> q.executeDelete());
    }

    private List<Message> popMessages(
            Connection connection, String queueName, int count, int timeout) {

        if (this.queueListener != null) {
            if (!this.queueListener.hasMessagesReady(queueName)) {
                return new ArrayList<>();
            }
        }

        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "WITH cte AS ("
                                        + "    SELECT queue_name, message_id "
                                        + "    FROM queue_message "
                                        + "    WHERE queue_name = :queueName "
                                        + "      AND popped = false "
                                        + "      AND deliver_on <= (current_timestamp + (1000 || ' microseconds')::interval) "
                                        + "    ORDER BY deliver_on, priority DESC, created_on "
                                        + "    LIMIT :count "
                                        + "    FOR UPDATE SKIP LOCKED "
                                        + ") "
                                        + "UPDATE queue_message "
                                        + "   SET popped = true "
                                        + "   FROM cte "
                                        + "   WHERE queue_message.queue_name = cte.queue_name "
                                        + "     AND queue_message.message_id = cte.message_id "
                                        + "     AND queue_message.popped = false "
                                        + "   RETURNING queue_message.message_id, queue_message.priority, queue_message.payload")
                        .bind("queueName", queueName)
                        .bind("count", count);

        return query(
                connection,
                builder,
                QueryContext.read("queue_message"),
                p ->
                        p.executeAndFetch(
                                rs -> {
                                    List<Message> results = new ArrayList<>();
                                    while (rs.next()) {
                                        Message m = new Message();
                                        m.setId(rs.getString("message_id"));
                                        m.setPriority(rs.getInt("priority"));
                                        m.setPayload(rs.getString("payload"));
                                        results.add(m);
                                    }
                                    return results;
                                }));
    }

    @Override
    public boolean containsMessage(String queueName, String messageId) {
        return getWithRetriedTransactions(tx -> existsMessage(tx, queueName, messageId));
    }

    private void createQueueIfNotExists(Connection connection, String queueName) {
        logger.trace("Creating new queue '{}'", queueName);
        SqlQueryBuilder existsQuery =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("queue")
                        .where("queue_name = :queueName")
                        .bind("queueName", queueName)
                        .trailing("FOR SHARE");
        boolean exists =
                query(connection, existsQuery, QueryContext.read("queue"), q -> q.exists());
        if (!exists) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("queue")
                            .column("queue_name", queueName)
                            .onConflict("queue_name")
                            .onConflictDoNothing();
            execute(connection, insert, QueryContext.write("queue"));
        }
    }

    private class QueueMessage {
        public String queueName;
        public String messageId;
    }
}

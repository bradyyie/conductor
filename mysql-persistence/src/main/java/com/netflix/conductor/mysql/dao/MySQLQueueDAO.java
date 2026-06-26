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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.conductoross.conductor.persistence.query.QueryContext;
import org.conductoross.conductor.persistence.query.SqlInsertBuilder;
import org.conductoross.conductor.persistence.query.SqlQueryBuilder;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.QueueDAO;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;

public class MySQLQueueDAO extends MySQLBaseDAO implements QueueDAO {

    private static final Long UNACK_SCHEDULE_MS = 60_000L;

    public MySQLQueueDAO(
            RetryTemplate retryTemplate, ObjectMapper objectMapper, DataSource dataSource) {
        super(retryTemplate, objectMapper, dataSource);

        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        this::processAllUnacks,
                        UNACK_SCHEDULE_MS,
                        UNACK_SCHEDULE_MS,
                        TimeUnit.MILLISECONDS);
        logger.debug(MySQLQueueDAO.class.getName() + " is ready to serve");
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
    public List<String> pop(String queueName, int count, int timeout) {
        List<Message> messages =
                getWithTransactionWithOutErrorPropagation(
                        tx -> popMessages(tx, queueName, count, timeout));
        if (messages == null) {
            return new ArrayList<>();
        }
        return messages.stream().map(Message::getId).collect(Collectors.toList());
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        List<Message> messages =
                getWithTransactionWithOutErrorPropagation(
                        tx -> popMessages(tx, queueName, count, timeout));
        if (messages == null) {
            return new ArrayList<>();
        }
        return messages;
    }

    @Override
    public void remove(String queueName, String messageId) {
        withTransaction(tx -> removeMessage(tx, queueName, messageId));
    }

    @Override
    public int getSize(String queueName) {
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
                                "UPDATE queue_message SET offset_time_seconds = :offset, deliver_on = TIMESTAMPADD(SECOND, :offset, CURRENT_TIMESTAMP)")
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
                                "UPDATE queue_message SET offset_time_seconds = :offset, deliver_on = TIMESTAMPADD(SECOND, :offset, CURRENT_TIMESTAMP)")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .and("deliver_on > TIMESTAMPADD(SECOND, :offset, CURRENT_TIMESTAMP)")
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
                                "SELECT queue_name, (SELECT count(*) FROM queue_message WHERE popped = false AND queue_name = q.queue_name) AS size FROM queue q");
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
                                        + "FROM queue q");
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

        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("UPDATE queue_message SET popped = false")
                        .where("popped = true")
                        .and("TIMESTAMPADD(SECOND, -60, CURRENT_TIMESTAMP) > deliver_on");
        withTransaction(tx -> execute(tx, builder, QueryContext.write("queue_message")));
    }

    @Override
    public void processUnacks(String queueName) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw("UPDATE queue_message SET popped = false")
                        .where("queue_name = :queueName")
                        .and("popped = true")
                        .and("TIMESTAMPADD(SECOND, -60, CURRENT_TIMESTAMP) > deliver_on")
                        .bind("queueName", queueName);
        withTransaction(tx -> execute(tx, builder, QueryContext.write("queue_message")));
    }

    @Override
    public boolean resetOffsetTime(String queueName, String messageId) {
        long offsetTimeInSecond = 0; // Reset to 0
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .raw(
                                "UPDATE queue_message SET offset_time_seconds = :offset, deliver_on = TIMESTAMPADD(SECOND, :offset, CURRENT_TIMESTAMP)")
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
                        .bind("messageId", messageId);
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
                                "UPDATE queue_message SET payload = :payload, deliver_on = TIMESTAMPADD(SECOND, :offset, CURRENT_TIMESTAMP)")
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
                                    "TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP)",
                                    List.of(offsetTimeInSecond))
                            .column("queue_name", queueName)
                            .column("message_id", messageId)
                            .column("priority", priority)
                            .column("offset_time_seconds", offsetTimeInSecond)
                            .column("payload", payload)
                            .doUpdateSet(
                                    "payload = VALUES(payload)", "deliver_on = VALUES(deliver_on)");
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

    private List<Message> peekMessages(Connection connection, String queueName, int count) {
        if (count < 1) {
            return Collections.emptyList();
        }

        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("message_id", "priority", "payload")
                        .from("queue_message use index(combo_queue_message)")
                        .where("queue_name = :queueName")
                        .and("popped = false")
                        .and("deliver_on <= TIMESTAMPADD(MICROSECOND, 1000, CURRENT_TIMESTAMP)")
                        .bind("queueName", queueName)
                        .orderBy("priority DESC", "deliver_on", "created_on")
                        .limit(count);

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

    private List<Message> popMessages(
            Connection connection, String queueName, int count, int timeout) {
        long start = System.currentTimeMillis();
        List<Message> messages = peekMessages(connection, queueName, count);

        while (messages.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            messages = peekMessages(connection, queueName, count);
        }

        if (messages.isEmpty()) {
            return messages;
        }

        List<Message> poppedMessages = new ArrayList<>();
        for (Message message : messages) {
            SqlQueryBuilder builder =
                    SqlQueryBuilder.create()
                            .raw("UPDATE queue_message SET popped = true")
                            .where("queue_name = :queueName")
                            .and("message_id = :messageId")
                            .and("popped = false")
                            .bind("queueName", queueName)
                            .bind("messageId", message.getId());
            int result = execute(connection, builder, QueryContext.write("queue_message"));

            if (result == 1) {
                poppedMessages.add(message);
            }
        }
        return poppedMessages;
    }

    private void createQueueIfNotExists(Connection connection, String queueName) {
        logger.trace("Creating new queue '{}'", queueName);
        SqlQueryBuilder existsQuery =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("queue")
                        .where("queue_name = :queueName")
                        .bind("queueName", queueName);
        boolean exists =
                query(connection, existsQuery, QueryContext.read("queue"), q -> q.exists());
        if (!exists) {
            SqlInsertBuilder insert =
                    SqlInsertBuilder.create()
                            .into("queue")
                            .column("queue_name", queueName)
                            .onConflictDoNothing();
            execute(connection, insert, QueryContext.write("queue"));
        }
    }

    @Override
    public boolean containsMessage(String queueName, String messageId) {
        SqlQueryBuilder builder =
                SqlQueryBuilder.create()
                        .select("1")
                        .from("queue_message")
                        .where("queue_name = :queueName")
                        .and("message_id = :messageId")
                        .bind("queueName", queueName)
                        .bind("messageId", messageId);
        return queryWithTransaction(builder, QueryContext.read("queue_message"), q -> q.exists());
    }
}

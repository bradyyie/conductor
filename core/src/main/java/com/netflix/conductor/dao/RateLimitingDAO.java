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
package com.netflix.conductor.dao;

import java.util.Optional;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.model.TaskModel;

/** An abstraction to enable different Rate Limiting implementations */
public interface RateLimitingDAO {

    /**
     * Checks if the Task is rate limited or not based on the {@link
     * TaskModel#getRateLimitPerFrequency()} and {@link TaskModel#getRateLimitFrequencyInSeconds()}
     *
     * @param task: which needs to be evaluated whether it is rateLimited or not
     * @return true: If the {@link TaskModel} is rateLimited false: If the {@link TaskModel} is not
     *     rateLimited
     */
    boolean exceedsRateLimitPerFrequency(TaskModel task, TaskDef taskDef);

    /**
     * Computes how long (in seconds) a task should be postponed so that it lands in the next rate
     * limit bucket, based on the task/{@link TaskDef} rate limit configuration.
     *
     * @param task the task being evaluated
     * @param taskDef the task definition (may be null, in which case the task's own rate limit
     *     settings are used)
     * @return the postpone duration in seconds, or {@code 0} if the task is not rate limited
     */
    static long getPostponeDurationForTask(TaskModel task, TaskDef taskDef) {
        ImmutablePair<Integer, Integer> rateLimitPair =
                Optional.ofNullable(taskDef)
                        .map(
                                definition ->
                                        new ImmutablePair<>(
                                                definition.getRateLimitPerFrequency(),
                                                definition.getRateLimitFrequencyInSeconds()))
                        .orElse(
                                new ImmutablePair<>(
                                        task.getRateLimitPerFrequency(),
                                        task.getRateLimitFrequencyInSeconds()));

        int rateLimitPerFrequency = rateLimitPair.getLeft();
        int rateLimitFrequencyInSeconds = rateLimitPair.getRight();
        if (rateLimitPerFrequency > 0 && rateLimitFrequencyInSeconds > 0) {
            long currentTimeEpoch = System.currentTimeMillis() / 1000L;
            long currentTimeEpochRateLimitBucket =
                    (currentTimeEpoch / rateLimitFrequencyInSeconds) + 1L;
            return currentTimeEpochRateLimitBucket * rateLimitFrequencyInSeconds - currentTimeEpoch;
        }
        return 0L;
    }
}

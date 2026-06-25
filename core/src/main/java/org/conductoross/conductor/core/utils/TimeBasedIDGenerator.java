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
package org.conductoross.conductor.core.utils;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.core.util.UuidUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.utils.IDGenerator;

/**
 * A time-ordered {@link IDGenerator} that produces RFC&nbsp;4122 version-1 (time-based) UUIDs.
 *
 * <p>Opt-in via {@code conductor.id.generator=time_based}. When unset (or any other value) the
 * default {@link IDGenerator} (random UUID&nbsp;v4) registered by {@code
 * ConductorCoreConfiguration} is used instead &mdash; this bean only materialises when the property
 * matches, so the default {@code @ConditionalOnMissingBean} factory is suppressed.
 *
 * <p>Why time-based IDs: identifiers minted close in time sort close together, which improves
 * primary-key/index locality for inserts and lets the engine derive a stable, creation-ordered FIFO
 * priority from the id (see {@link #getDate(String)}). The generated value is still a valid {@link
 * UUID} string, so it remains compatible with stores that require UUIDs (e.g. Cassandra).
 *
 * <p>This generator is feature-agnostic: it does NOT encode any organization/tenant information.
 * Tenant-aware id schemes are an enterprise concern layered on top via a subclass.
 */
@Component
@ConditionalOnProperty(name = "conductor.id.generator", havingValue = "time_based")
public class TimeBasedIDGenerator extends IDGenerator {

    /** 100-ns intervals per millisecond (v1 UUID timestamp granularity). */
    private static final long HUNDRED_NANOS_PER_MILLI = 10_000L;

    /** Milliseconds between the Gregorian UUID epoch (1582-10-15) and the Unix epoch. */
    private static final long GREGORIAN_EPOCH_MILLIS;

    static {
        Calendar gregorianStart = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        gregorianStart.clear();
        gregorianStart.set(1582, Calendar.OCTOBER, 15, 0, 0, 0);
        GREGORIAN_EPOCH_MILLIS = gregorianStart.getTimeInMillis();
    }

    @Override
    public String generate() {
        return UuidUtil.getTimeBasedUuid().toString();
    }

    /**
     * Extracts the creation time (epoch millis) encoded in a time-based (v1) UUID string. Returns
     * {@code 0} when the id is not a parseable version-1 UUID (e.g. a random v4 id), so callers can
     * treat "unknown" as "no time-based ordering".
     */
    public static long getDate(String id) {
        UUID uuid = UUID.fromString(id);
        if (uuid.version() != 1) {
            return 0;
        }
        return (uuid.timestamp() / HUNDRED_NANOS_PER_MILLI) + GREGORIAN_EPOCH_MILLIS;
    }
}

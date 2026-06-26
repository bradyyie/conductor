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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Composable {@code INSERT ... ON CONFLICT ... DO UPDATE} builder with column-ordered positional
 * binds. Feature-agnostic OSS seam (P2): the OSS adapter builds the base insert, then calls {@code
 * applyWriteExtensions(insert, context)} (no-op in OSS). An enterprise subclass overrides that hook
 * to {@link #column(String, Object) add a column} (e.g. {@code org_id}) and {@link
 * #onConflict(String...) extend the conflict target} — composing safely with the OSS conflict
 * target instead of overwriting it.
 *
 * <p>Mirrors {@link SqlQueryBuilder}: a concrete, dependency-free builder so it can live in {@code
 * conductor-core} and be used by every {@code *-persistence} adapter. {@link #render()} emits
 * {@code ?} placeholders and the matching positional bind list (column-insertion order, then {@code
 * DO UPDATE SET} value binds).
 */
public final class SqlInsertBuilder {

    private String table;
    private final Map<String, ColumnValue> columns = new LinkedHashMap<>();
    private final List<String> conflictTargets = new ArrayList<>();
    private final List<String> updateAssignments = new ArrayList<>();
    private final List<Object> updateBinds = new ArrayList<>();
    private boolean doNothingOnConflict = false;

    /** A column value: either a positional bind ({@code ?}) or a raw SQL expression. */
    private record ColumnValue(Object value, boolean raw) {}

    public static SqlInsertBuilder create() {
        return new SqlInsertBuilder();
    }

    public SqlInsertBuilder into(String table) {
        this.table = table;
        return this;
    }

    /**
     * Adds (or replaces) a column bound as a positional {@code ?}. Enterprise overrides add {@code
     * org_id} here.
     */
    public SqlInsertBuilder column(String name, Object value) {
        columns.put(name, new ColumnValue(value, false));
        return this;
    }

    /**
     * Adds (or replaces) a column whose VALUE is a raw SQL expression rendered verbatim (no bind),
     * e.g. {@code columnRaw("modified_on", "CURRENT_TIMESTAMP")}.
     */
    public SqlInsertBuilder columnRaw(String name, String sqlExpression) {
        columns.put(name, new ColumnValue(sqlExpression, true));
        return this;
    }

    /**
     * Appends conflict-target columns. Called by the OSS adapter for its natural key, then
     * (optionally) by an enterprise override to add {@code org_id} — the targets compose rather
     * than overwrite.
     */
    public SqlInsertBuilder onConflict(String... targetColumns) {
        for (String c : targetColumns) {
            if (!conflictTargets.contains(c)) {
                conflictTargets.add(c);
            }
        }
        return this;
    }

    /**
     * {@code ON CONFLICT (...) DO UPDATE SET} assignments. Each may contain {@code :name} markers.
     */
    public SqlInsertBuilder doUpdateSet(String... assignments) {
        for (String a : assignments) {
            updateAssignments.add(a);
        }
        return this;
    }

    /** Binds a value used by a {@link #doUpdateSet} assignment expression, in declaration order. */
    public SqlInsertBuilder bindUpdate(Object value) {
        updateBinds.add(value);
        return this;
    }

    /** {@code ON CONFLICT (...) DO NOTHING}. */
    public SqlInsertBuilder onConflictDoNothing() {
        this.doNothingOnConflict = true;
        return this;
    }

    public record Rendered(String sql, List<Object> binds) {}

    public Rendered render() {
        if (table == null || columns.isEmpty()) {
            throw new IllegalStateException("INSERT requires a table and at least one column");
        }
        List<Object> binds = new ArrayList<>();
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        sql.append(String.join(", ", columns.keySet())).append(") VALUES (");
        StringBuilder placeholders = new StringBuilder();
        for (ColumnValue column : columns.values()) {
            if (placeholders.length() > 0) {
                placeholders.append(", ");
            }
            if (column.raw()) {
                placeholders.append((String) column.value());
            } else {
                placeholders.append("?");
                binds.add(column.value());
            }
        }
        sql.append(placeholders).append(")");
        if (!conflictTargets.isEmpty()) {
            sql.append(" ON CONFLICT (").append(String.join(", ", conflictTargets)).append(")");
            if (doNothingOnConflict || updateAssignments.isEmpty()) {
                sql.append(" DO NOTHING");
            } else {
                sql.append(" DO UPDATE SET ").append(String.join(", ", updateAssignments));
                binds.addAll(updateBinds);
            }
        } else if (doNothingOnConflict) {
            // Target-less ON CONFLICT DO NOTHING (e.g. event_execution): fires on any unique
            // violation. Composes with an enterprise org_id column without needing a conflict
            // target.
            sql.append(" ON CONFLICT DO NOTHING");
        }
        return new Rendered(sql.toString(), binds);
    }

    public String toSql() {
        return render().sql();
    }

    public List<Object> binds() {
        return render().binds();
    }
}

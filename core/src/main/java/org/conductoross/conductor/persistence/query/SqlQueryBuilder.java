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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A small, composable SQL builder using <em>named</em> binds ({@code :name}) that render to
 * positional {@code ?} placeholders in left-to-right order.
 *
 * <p>This is the foundation for safe, additive query extension. Because predicates are appended by
 * name rather than by fragile positional index, a subclass DAO (e.g. an enterprise edition) can
 * append a predicate &mdash; e.g. {@code .and("org_id = :orgId").bind("orgId", ...)} &mdash; on top
 * of a base query without having to know or preserve the existing bind ordering. OSS itself remains
 * unaware of <em>why</em> any such predicate is added.
 *
 * <p>The same named bind may be referenced multiple times; its value is emitted once per
 * occurrence, in render order. Rendering fails fast if a referenced name has no bound value.
 *
 * <p>Note: named markers are recognised anywhere in the assembled SQL, so callers should not embed
 * a literal {@code :word} inside a string literal in a fragment passed to this builder.
 */
public final class SqlQueryBuilder {

    private static final Pattern NAMED_PARAM = Pattern.compile(":([a-zA-Z_][a-zA-Z0-9_]*)");

    /**
     * Statement head: {@code SELECT ... FROM ...} (incl. joins), or an {@code UPDATE}/{@code
     * DELETE} head.
     */
    private final StringBuilder head = new StringBuilder();

    /**
     * {@code WHERE} predicates, AND-joined. This list is the seam: {@link #where}/{@link #and}
     * append here.
     */
    private final List<String> wherePredicates = new ArrayList<>();

    /**
     * Trailing clauses ({@code ORDER BY}/{@code LIMIT}/locking) — always rendered AFTER the {@code
     * WHERE}.
     */
    private final StringBuilder tail = new StringBuilder();

    private final Map<String, Object> binds = new HashMap<>();

    public static SqlQueryBuilder create() {
        return new SqlQueryBuilder();
    }

    public SqlQueryBuilder select(String... columns) {
        head.append("SELECT ").append(String.join(", ", columns));
        return this;
    }

    public SqlQueryBuilder from(String table) {
        head.append(" FROM ").append(table);
        return this;
    }

    public SqlQueryBuilder where(String predicate) {
        wherePredicates.add(predicate);
        return this;
    }

    /**
     * Appends a {@code WHERE} predicate (AND-joined). This is the primary seam for adding a filter:
     * predicates are collected and rendered together inside the {@code WHERE} clause, so a
     * predicate appended here always lands before any trailing {@code ORDER BY}/{@code
     * LIMIT}/locking clause.
     */
    public SqlQueryBuilder and(String predicate) {
        wherePredicates.add(predicate);
        return this;
    }

    public SqlQueryBuilder orderBy(String... columns) {
        tail.append(" ORDER BY ").append(String.join(", ", columns));
        return this;
    }

    public SqlQueryBuilder limit(int count) {
        tail.append(" LIMIT ").append(count);
        return this;
    }

    /**
     * Appends a trailing clause rendered after {@code WHERE}/{@code ORDER BY}/{@code LIMIT} — e.g.
     * a row-locking clause such as {@code FOR UPDATE SKIP LOCKED} or {@code FOR SHARE}.
     */
    public SqlQueryBuilder trailing(String clause) {
        tail.append(" ").append(clause);
        return this;
    }

    /**
     * Appends a raw SQL fragment verbatim to the statement head (before {@code WHERE}), e.g. an
     * {@code UPDATE t SET ...}/{@code DELETE FROM t} head or a join. May contain {@code :name}
     * markers.
     */
    public SqlQueryBuilder raw(String fragment) {
        head.append(fragment);
        return this;
    }

    /** Binds a value to a named marker. Order-independent of where the marker appears. */
    public SqlQueryBuilder bind(String name, Object value) {
        binds.put(name, value);
        return this;
    }

    /**
     * The rendered SQL (named markers replaced by positional {@code ?}) and its positional binds.
     */
    public record Rendered(String sql, List<Object> binds) {}

    public Rendered render() {
        StringBuilder assembled = new StringBuilder(head);
        if (!wherePredicates.isEmpty()) {
            assembled.append(" WHERE ").append(String.join(" AND ", wherePredicates));
        }
        assembled.append(tail);

        Matcher matcher = NAMED_PARAM.matcher(assembled);
        StringBuilder rendered = new StringBuilder();
        List<Object> positional = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group(1);
            if (!binds.containsKey(name)) {
                throw new IllegalStateException(
                        "No value bound for named parameter ':" + name + "'");
            }
            positional.add(binds.get(name));
            matcher.appendReplacement(rendered, "?");
        }
        matcher.appendTail(rendered);
        return new Rendered(rendered.toString(), positional);
    }

    public String toSql() {
        return render().sql();
    }

    public List<Object> binds() {
        return render().binds();
    }
}

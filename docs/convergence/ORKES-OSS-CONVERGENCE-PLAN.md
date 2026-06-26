# Orkes ↔ Conductor OSS Convergence Plan

> Branch (both repos): **`orkes-converge-fork-conductor`**
> OSS repo: `~/workprojects/conductor` (`github.com/conductor-oss/conductor`, base `origin/main` @ `a3b9feec3`)
> Enterprise repo: `~/workprojects/orkes-conductor` (`github.com/orkes-io/orkes-conductor`, base `main`)
> Status: **DRAFT for review** — no production code changed yet. This document is the authoritative guideline.

---

## 0. How to read this document

This is the master plan. It is intentionally exhaustive ("down to the contracts") because the work is mission‑critical and will be executed incrementally by multiple people over many PRs. Sections:

1. Executive summary
2. Goals & non‑negotiable principles
3. Repos, branches, and the open‑core model
4. Target architecture (open‑core + hexagonal)
5. The reference pattern (OSS scheduler) — the recipe we copy everywhere
6. **The contracts** — existing ports/SPIs we reuse + new seams we add (with Java signatures)
7. Engine backport plan (Orkes → OSS native), task‑by‑task disposition
8. Persistence convergence + `org_id` strategy (extension by inheritance)
9. Database migrations strategy
10. Module map (OSS vs Enterprise) + namespacing rules
11. Phased execution plan (the actual steps, with commands & acceptance criteria)
12. Build, publish (maven local), and composite consumption
13. Testing & verification
14. Risks & open decisions
15. Appendix: authoritative file‑path index

Terminology: **OSS** = `conductor-oss/conductor` (Apache‑2.0, public, `com.netflix.conductor.*` / `org.conductoross.conductor.*`). **Enterprise** = `orkes-io/orkes-conductor` (private, `io.orkes.conductor.*`). **Port** = an interface owned by a domain module. **Adapter** = a concrete implementation of a port in a technology module. **Seam** = a generic, feature‑agnostic extension point exposed by OSS.

---

## 1. Executive summary

The Orkes fork diverged from Conductor OSS by (a) re‑implementing the engine inside a private `oss-core` module, (b) collapsing OSS's clean per‑concern adapter modules into technology **monoliths** (`postgres-persistence` does execution + queue + metadata + scheduler + human + webhook + integration + gateway + RBAC + org), and (c) weaving multi‑tenancy (`org_id`), RBAC, SKU limits, and an introspection tracer through the engine.

Meanwhile **OSS already implements the architecture we want**: ports in `core`, adapters in technology modules, the `scheduler/core` + `scheduler/{db}-persistence` ports‑and‑adapters split, Spring‑Boot `AutoConfiguration.imports` plugin discovery, and feature‑agnostic SPIs (`WorkflowStatusListener`, `TaskStatusListener`, `Lock`, `ExternalPayloadStorage`, `EventQueueProvider`).

**The convergence is bi‑directional:**

- **Backport (Orkes → OSS):** the Orkes engine improvements (`OrkesJoin`, `OrkesDoWhile`, `OrkesExclusiveJoin`, `OrkesSubWorkflowTaskMapper`, `OrkesForkJoinDynamicTaskMapper`, the parallel scheduling in `OrkesForkJoinTaskMapper`, and the engine core of `OrkesWorkflowExecutor`) are cleaned of `org_id`/RBAC/limits/introspection and become the **native OSS implementations**.
- **Plug (Enterprise → on top of OSS):** enterprise behavior (`org_id`, RBAC, SKU limits, CDC, secrets, integrations, webhooks, human, api‑gateway, archival) is re‑introduced as **subclasses that call `super()`** and via **generic OSS seams** + Spring conditions/auto‑config — never inside OSS.

**End state:**

- OSS gains the Orkes engine improvements as native code (Apache‑2.0).
- Orkes **deletes `oss-core`** and consumes published `org.conductoross:conductor-*` artifacts.
- Orkes keeps only enterprise modules, which layer onto OSS via `super()`/seams/auto‑config.
- Two repos; OSS publishes artifacts (maven local for dev, GitHub Packages/S3 for CI); Orkes is a **composite** of OSS artifacts + private modules.

---

## 2. Goals & non‑negotiable principles

**P1 — Dependencies point inward.** Everything may depend on `core`/`common` and `*-api` port modules. **No implementation module depends on another implementation module.** Forbidden today and must be removed: `postgres-persistence → scheduler-*`, `postgres-persistence → api-gateway/integration/human/api-orchestration`, `redis-persistence → api-gateway`.

**P2 — OSS is feature‑agnostic.** OSS contains **no** knowledge of organizations, tenants, multi‑tenancy, authentication, or authorization. OSS exposes **generic** seams (e.g. `applyQueryExtensions(builder, request)`); the enterprise layer decides *why* the seam exists. We do **not** add `@OrgScoped`, `OrgScope`, `OrgContext`, or `org_id` anywhere in OSS.

**P3 — Extension by inheritance + composition.** Enterprise extends OSS concrete classes and calls `super()` first, then adds behavior. OSS classes are therefore designed for extension (non‑final, `protected` hooks, beans overridable via `@ConditionalOnMissingBean`/`@Primary`).

**P4 — Namespacing.** Anything we create or change **on the OSS side as new Orkes‑originated code** uses `org.conductoross.conductor.*`. Pre‑existing Netflix code stays `com.netflix.conductor.*` even when we enhance it in place. Enterprise code stays `io.orkes.conductor.*`.

**P5 — Backport beats override.** If an Orkes class is a pure engine improvement, backport it into the OSS native class and **delete the Orkes override**. Keep an enterprise subclass only when genuinely enterprise behavior remains. This shrinks the override/`excludeFilters` surface over time.

**P6 — One‑way, pluggable, auto‑discovered.** New backends are added by dropping a jar that implements a port and ships an `AutoConfiguration.imports`; no engine code changes. Selection is by property (`conductor.db.type`, feature flags) and/or classpath.

**P7 — Every port has a contract test.** Use `java-test-fixtures` to publish an abstract contract test per port (the OSS scheduler already does this); each adapter subclasses it.

---

## 3. Repos, branches, and the open‑core model

### 3.1 Branches (created)

| Repo | Path | Branch | Base |
|---|---|---|---|
| OSS | `~/workprojects/conductor` | `orkes-converge-fork-conductor` | `origin/main` @ `a3b9feec3` (clean upstream) |
| Enterprise | `~/workprojects/orkes-conductor` | `orkes-converge-fork-conductor` | `main` |

### 3.2 Distribution model — two repos + published artifacts

- OSS builds and publishes `org.conductoross:conductor-*` jars.
  - **Dev loop:** `./gradlew publishToMavenLocal` in the OSS repo; Orkes consumes via `mavenLocal()` (already present in `orkes-conductor/build.gradle` repositories).
  - **CI/release:** GitHub Packages (`maven.pkg.github.com/orkes-io/*`, already configured) and/or S3 (`s3://orkes-artifacts-repo`, already used by `api-orchestration`/`server`/`workers`).
- Enterprise consumes the OSS artifacts as normal dependencies and adds private modules → branded composite server.
- Today Orkes **excludes** `org.conductoross:conductor-core` and re‑implements it (`oss-core`). Convergence **reverses** this: stop excluding, delete `oss-core`, depend on the published artifact.

---

## 4. Target architecture (open‑core + hexagonal)

### 4.1 Layering

```
OSS (org.conductoross / com.netflix) — published artifacts
  conductor-common         models, events, exceptions, ExternalPayloadStorage SPI
  conductor-core           engine + PORTS (ExecutionDAO, MetadataDAO, QueueDAO, ...) +
                           SPIs (listeners, Lock, EventQueueProvider) + WorkflowExecutor(Ops) +
                           system tasks/mappers (native, with backported Orkes improvements) +
                           IDGenerator + NEW generic seams (QueryBuilder, applyQueryExtensions)
  conductor-<db>-persistence   adapters: ExecutionDAO/MetadataDAO/QueueDAO/PollDataDAO impls
  conductor-scheduler-core + conductor-scheduler-<db>-persistence   (reference pattern)
  conductor-http-task, conductor-json-jq-task, *-storage, *-event-queue, listeners
  conductor-server, conductor-server-lite

ENTERPRISE (io.orkes.conductor) — private, depends on OSS artifacts
  enterprise-common        OrgContext, IDTools (org-encoding), audit, FeatureFlags
  enterprise-security      RBAC, auth providers, SCIM, federation, @PreAuthorize REST
  enterprise-persistence-<db>   subclasses of OSS adapters: override applyQueryExtensions → org_id;
                                 plus enterprise stores (rbac/webhook/human/integration/gateway/org/archive)
  enterprise-engine        OrkesWorkflowExecutor extends WorkflowExecutorOps (org_id/RBAC/limits/CDC),
                           enterprise task subclasses where enterprise behavior remains
  webhooks, human, integration, event-processor, event-integration,
  api-gateway, api-orchestration, archive-persistence, ssm-properties, workers (external)
  scheduler-enterprise     extends conductor-scheduler-core
  server-enterprise        composite assembly + branding
```

### 4.2 Dependency rules (enforced — see §11 Phase 0)

- Allowed: `adapter → *-api → core → common`; `domain → core + *-api`; `server → anything`.
- Forbidden: adapter → adapter; adapter → domain; anything → server; sideways technology edges.
- Enforcement: ArchUnit tests + a Gradle dependency‑rule check that fails on illegal `project(':...')` edges. Baseline current violations and burn down per phase.

### 4.3 Namespacing rules (P4 restated with examples)

| Situation | Namespace |
|---|---|
| Enhance existing OSS `Join` in place (backport) | stays `com.netflix.conductor.core.execution.tasks.Join` |
| New OSS SPI/seam we introduce (e.g. `SqlQueryBuilder`, `MetadataChangeListener`) | `org.conductoross.conductor.*` |
| New OSS adapter we contribute (e.g. a new store) | `org.conductoross.conductor.*` |
| Enterprise subclass / enterprise‑only module | `io.orkes.conductor.*` |

> Pre‑existing exception in OSS: the scheduler ports use `io.orkes.conductor.dao.scheduler.*` (already merged upstream, Apache‑licensed). Leave as‑is; do **not** retrofit. New ports follow `org.conductoross.conductor.*`.

### 4.4 The org_id rule (critical)

OSS never sees `org_id`. The enterprise edition injects it by **overriding generic seams**. When the enterprise edition runs single‑tenant, its `OrgContext` returns the default (`"0000"`); OSS code paths never ask for it. The previously discussed "OSS returns 0000" is therefore implemented as an **enterprise single‑tenant default**, not an OSS concept.

---

## 5. The reference pattern (OSS scheduler) — copy this everywhere

This is verified, working OSS code. Every domain we converge follows it.

**Module split:** `feature/core` (ports + domain + service + REST + wiring) and one `feature/<db>-persistence` per backend.

**Step 1 — Port(s) in `-core`** (plain interfaces over domain models; no Spring/JDBC):
`scheduler/core/src/main/java/io/orkes/conductor/dao/scheduler/SchedulerDAO.java`, `SchedulerCacheDAO.java`, `scheduler/core/.../dao/archive/SchedulerArchivalDAO.java`.

**Step 2 — Business logic in `-core`** depends on the port, not adapters (`SchedulerService` takes `SchedulerDAO`/`SchedulerArchivalDAO` via constructor). `-core` keeps Spring Boot `compileOnly` and lists **no** persistence adapter.

**Step 3 — Core wiring + feature flag.** `-core` ships `SchedulerOssConfiguration` registered via `…/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. It **consumes** the DAO beans (does not declare them), supplies safe defaults via `@ConditionalOnMissingBean` (`NoOpSchedulerCacheDAO`) and an optional `@Primary` decorator (`CachingSchedulerDAO`). Gated by `@Conditional(SchedulerConditions)` → `conductor.scheduler.enabled=true`.

**Step 4 — Adapter in `-<db>-persistence`:** `PostgresSchedulerDAO extends PostgresBaseDAO implements SchedulerDAO`. Depends on `:conductor-scheduler-core` + `:conductor-common` + `:conductor-core` + the per‑DB main module (`:conductor-postgres-persistence`) + driver + DB‑specific Flyway. **No other adapter.**

**Step 5 — Adapter wiring:** `PostgresSchedulerConfiguration` is `@AutoConfiguration`, listed in the adapter's own `AutoConfiguration.imports`, gated by
`@ConditionalOnExpression("'${conductor.db.type:}' == 'postgres' && '${conductor.scheduler.enabled:false}' == 'true'")`,
and declares `@Bean SchedulerDAO` / `@Bean SchedulerArchivalDAO`.

**Step 6 — Shared DataSource, isolated migrations:** inject the shared `DataSource` + `@Qualifier("postgresRetryTemplate")` from the per‑DB main module; add only a private `Flyway` with its own history table (`flyway_schema_history_scheduler`) and location (`classpath:db/migration_scheduler`); make DAO beans `@DependsOn` it.

**Step 7 — Runtime selection = two properties:** `conductor.db.type=<postgres|mysql|…>` + `conductor.<feature>.enabled=true`. Exactly one adapter's condition is true → exactly one set of port beans exists.

**Step 8 — Discovery:** `Conductor.java` component‑scans `com.netflix.conductor`, `io.orkes.conductor`, `org.conductoross.conductor`; `@AutoConfiguration` classes come in via the imports files.

**Step 9 — Contract tests:** `-core` uses `java-test-fixtures` to export `AbstractSchedulerDAOTest` etc.; each adapter adds `testImplementation testFixtures(project(':conductor-scheduler-core'))` and writes a thin subclass.

> Correction to common assumption: `common-persistence` in OSS is an (almost empty) **test** module, not the DataSource provider. The shared `DataSource`/Flyway/base‑DAO live in each per‑DB main module (`postgres-persistence`, `mysql-persistence`, …), which `@Import(DataSourceAutoConfiguration.class)` gated by `conductor.db.type`.

---

## 6. The contracts

### 6.1 Existing OSS ports we REUSE (do not reinvent)

Package `com.netflix.conductor.dao` in `conductor-core` unless noted. These are the persistence ports the enterprise adapters implement/subclass.

- **`ExecutionDAO`** — `core/src/main/java/com/netflix/conductor/dao/ExecutionDAO.java`
  tasks CRUD (`createTasks`, `updateTask`, `getTask(s)`, `getPendingTasksByWorkflow`, `getPendingTasksForTaskType`, `getTasksForWorkflow`), workflow CRUD (`createWorkflow`, `updateWorkflow`, `removeWorkflow`, `removeWorkflowWithExpiry`, `getWorkflow(s)`, `getRunningWorkflowIds`, `getPendingWorkflowsByType`, counts), and events (`addEventExecution`, `updateEventExecution`, `removeEventExecution`).
- **`MetadataDAO`** — task/workflow def CRUD; `getLatestWorkflowDef`, `getWorkflowDef(name,version)`, `getAllWorkflowDefsLatestVersions`, default `getWorkflowNames`/`getWorkflowVersions`.
- **`QueueDAO`** — `push`/`pushIfNotExists`/`pop`/`pollMessages`/`ack`/`remove`/`flush`/`queuesDetail(Verbose)`/`processUnacks`/`postpone`/`peekFirstIds`.
- **`PollDataDAO`** — `updateLastPollData`, `getPollData(...)`, default `getAllPollData`.
- **`RateLimitingDAO`** — `exceedsRateLimitPerFrequency(TaskModel, TaskDef)`.
- **`ConcurrentExecutionLimitDAO`** — `addTaskToLimit`/`removeTaskFromLimit`/`exceedsLimit`.
- **`IndexDAO`** — sync+async indexing/search of workflows, tasks, logs, events, messages.
- **`EventHandlerDAO`** — `add/update/removeEventHandler`, `getAllEventHandlers`, `getEventHandlersForEvent`.
- **`WorkflowMessageQueueDAO`** (WMQ, optional; injected as `Optional<>`).
- **`FileMetadataDAO`** — `org.conductoross.conductor.dao.FileMetadataDAO` (file‑storage feature).
- **DAL facade `ExecutionDAOFacade`** — `core/src/main/java/com/netflix/conductor/core/dal/ExecutionDAOFacade.java` (`@Component`). The engine talks to this, not to DAOs directly. Constructor: `(ExecutionDAO, QueueDAO, IndexDAO, RateLimitingDAO, ConcurrentExecutionLimitDAO, PollDataDAO, ObjectMapper, ConductorProperties, ExternalPayloadStorageUtils)`.

### 6.2 Existing OSS extension SPIs we REUSE

| SPI | Path / package | Default (no‑op) | Gate |
|---|---|---|---|
| `WorkflowStatusListener` | `core/.../core/listener/` (`com.netflix.conductor.core.listener`) | `WorkflowStatusListenerStub` | `conductor.workflow-status-listener.type=stub` (matchIfMissing) |
| `TaskStatusListener` | `core/.../core/listener/` | `TaskStatusListenerStub` | `conductor.task-status-listener.type=stub` |
| `MetadataChangeListener` | `core/.../org/conductoross/conductor/core/listener/` | `MetadataChangeListenerStub` | `conductor.metadata-change-listener.type=stub` |
| `ExternalPayloadStorage` | `common/.../common/utils/` | `DummyPayloadStorage` | `conductor.external-payload-storage.type=dummy` |
| `EventQueueProvider` | `core/.../core/events/` | (supplied by queue modules) | collected into `EventQueues` map |
| `Lock` (+ `ExecutionLockService`) | `core/.../core/sync/` | `NoopLock` | `conductor.workflow-execution-lock.type=noop_lock` |
| `IDGenerator` | `core/.../core/utils/IDGenerator.java` | UUID v4 + deterministic `generateSubWorkflowId` | `@ConditionalOnMissingBean(IDGenerator)` |

**These are the templates for new seams: an interface in OSS + a stub default selected by `@ConditionalOnProperty(..., matchIfMissing=true)` or `@ConditionalOnMissingBean`.** Enterprise registers a real bean and wins.

### 6.3 NEW generic seams we must ADD to OSS (with proposed contracts)

All new seams are **feature‑agnostic** (P2) and `org.conductoross.conductor.*` (P4).

#### 6.3.1 `SqlQueryBuilder` — composable SQL with **named** binds (enabler for org_id injection)

Why: enterprise must append predicates/columns to native SQL without breaking positional binds, and the org_id sits in PRIMARY KEY / `ON CONFLICT` targets (not just `WHERE`). The current positional `Query` util cannot support safe sub‑class appends. Introduce a builder in OSS (used by the OSS `*-persistence` modules) so enterprise subclasses can decorate queries.

Proposed package: `org.conductoross.conductor.persistence.query` in `conductor-core` (or a small `conductor-persistence-api` module).

```java
public interface SqlQueryBuilder {
    SqlQueryBuilder select(String... columns);
    SqlQueryBuilder from(String table);
    SqlQueryBuilder where(String predicate);          // e.g. "workflow_id = :workflowId"
    SqlQueryBuilder and(String predicate);            // appended safely; enterprise uses this
    SqlQueryBuilder bind(String name, Object value);  // named bind; order-independent
    SqlQueryBuilder orderBy(String... columns);
    SqlQueryBuilder limit(int n);
    String toSql();                                   // renders :name -> ? and records bind order
    List<Object> binds();                             // resolved positional binds in render order
}

public interface SqlInsertBuilder {
    SqlInsertBuilder into(String table);
    SqlInsertBuilder column(String name, Object value);     // enterprise adds org_id here
    SqlInsertBuilder onConflict(String... targetColumns);   // enterprise adds org_id to target
    SqlInsertBuilder doUpdateSet(String... assignments);
    String toSql();
    List<Object> binds();
}
```

#### 6.3.2 Query decoration hooks on OSS base DAO (generic, no‑op in OSS)

Add to the OSS per‑DB base DAO (e.g. `PostgresBaseDAO`) or to each OSS store, **protected, no‑op**, named generically:

```java
// OSS — com.netflix.conductor.postgres.dao.PostgresBaseDAO (enhanced in place; stays com.netflix)
/** Generic extension point. No-op in OSS. Subclasses may add predicates/binds. */
protected void applyQueryExtensions(SqlQueryBuilder query, QueryContext context) { /* no-op */ }
/** Generic extension point for writes. No-op in OSS. */
protected void applyWriteExtensions(SqlInsertBuilder insert, QueryContext context) { /* no-op */ }
```

`QueryContext` is generic (carries table name, operation type READ/WRITE, and the originating request/entity) and contains **no** org/tenant vocabulary:

```java
// OSS — org.conductoross.conductor.persistence.query.QueryContext
public record QueryContext(String table, Operation operation, Object request) {
    public enum Operation { READ, WRITE }
}
```

Enterprise overrides (private repo):

```java
// Enterprise — io.orkes.conductor.dao.postgres.OrkesPostgresExecutionDAO
@Override
protected void applyQueryExtensions(SqlQueryBuilder query, QueryContext ctx) {
    super.applyQueryExtensions(query, ctx);
    query.and("org_id = :orgId").bind("orgId", OrgContext.current().orgId());
}
@Override
protected void applyWriteExtensions(SqlInsertBuilder insert, QueryContext ctx) {
    super.applyWriteExtensions(insert, ctx);
    insert.column("org_id", OrgContext.current().orgId());
    insert.onConflict("org_id");   // composes with the OSS conflict target
}
```

> Acceptance: with OSS migrations (no `org_id` column) and no enterprise override, every query runs unchanged. With enterprise migrations + override, `org_id` is appended uniformly. The OSS DAO calls `applyQueryExtensions(qb, ctx)` right before render in **every** read/write path, and all raw‑JDBC methods are migrated onto the builder so the hook is universal.

#### 6.3.3 `IDGenerator` override (already a seam) — keep OSS plain; enterprise prepends org

OSS keeps `com.netflix.conductor.core.utils.IDGenerator` (UUID v4 + deterministic `generateSubWorkflowId`). If we want time‑based IDs natively in OSS, add an **OSS** time‑based generator selected by property (`conductor.id.generator=time_based`) — with **no org prefix**:

```java
// OSS (new) — org.conductoross.conductor.core.utils.TimeBasedIDGenerator extends IDGenerator
@Override public String generate() { return UuidUtil.getTimeBasedUuid().toString(); }
```

Enterprise keeps the org‑prefixing generator (`io.orkes.conductor.id.TimeBasedUUIDGenerator`) that prepends `orgId` when not default, and consolidate the 3 duplicate copies (dead `common/IDTools`, `core/TimeBasedUUIDGenerator`, `workers/TimeBasedUUIDGenerator`) into the enterprise module. `getOrgId(id)`/`getDate(id)` decoding stays enterprise.

> Decision needed (see §14): does FIFO priority by creation time (`getWorkflowFIFOPriority` from `TimeBasedUUIDGenerator.getDate`) become native OSS behavior (engine decision) or stay enterprise? Recommended: backport a time‑based priority that derives from `IDGenerator`‑produced IDs *only if* the time‑based generator is active; otherwise priority defaults to 0 (OSS current behavior).

#### 6.3.4 WorkflowExecutor extension points (for the enterprise executor subclass)

`OrkesWorkflowExecutor` will become `WorkflowExecutorOps` (native) + an enterprise subclass `OrkesWorkflowExecutor extends WorkflowExecutorOps`. To let enterprise inject org/RBAC/limits cleanly, `WorkflowExecutorOps` must expose `protected` seams instead of inlined logic. Proposed (all no‑op/default in OSS):

```java
// OSS — com.netflix.conductor.core.execution.WorkflowExecutorOps (enhanced in place)
protected void beforeStartWorkflow(StartWorkflowInput input) {}      // enterprise: RBAC check, SKU limits
protected void afterWorkflowScheduled(WorkflowModel wf) {}            // enterprise: stamp metadata
protected void beforeScheduleTasks(WorkflowModel wf, List<TaskModel> tasks) {} // enterprise: per-task RBAC, queue routing
protected int resolveWorkflowPriority(String workflowId, int priority) { return priority; } // enterprise: FIFO-by-time
protected void onDecideContext(String workflowId) {}                  // enterprise: set OrgContext from workflowId
```

Enterprise subclass overrides these, calling `super` where appropriate. This removes all `OrkesRequestContext`/RBAC/limits/introspection from the OSS executor while preserving behavior.

#### 6.3.5 CDC / change events (enterprise) — OSS exposes listener seams only

OSS already has `WorkflowStatusListener`/`TaskStatusListener`/`MetadataChangeListener`. The Orkes CDC publisher (`OrkesCDCEventPublisher`/`OrkesCDCEventSink`) stays enterprise and is wired by implementing those OSS listener SPIs (no new OSS interface needed). If CDC needs finer hooks than the listeners provide, add **generic** listener methods to the OSS SPIs (still feature‑agnostic).

### 6.4 Enterprise‑only contracts (stay private, `io.orkes.conductor.*`)

RBAC (`AccessControlServiceV2`, `OrkesPermissionEvaluator`, `RbacDAO`, …), `OrgContext`/multi‑tenancy, `SecretsDAO`, `WebhookDAO`, `IntegrationDAO` family, `GatewayConfigDAO`, `ServiceRegistryDAO`, archival (`ArchiveDAO`/`DocumentStoreDAO`/`ObjectStoreDAO`/`PartitionManager`), `EnterpriseSchedulerDAO`, `HumanTaskDAO`, `ConductorLimitsDAO`/OCU, audit. These have **no** OSS counterpart and never move to OSS.

---

## 7. Engine backport plan (Orkes → OSS native)

The Orkes engine overrides live in `orkes-conductor/server/src/main/java/com/netflix/conductor/**` (61 classes) and supersede OSS via `@Primary` (executor/facade/services) or `@Component("<TASK_TYPE>")` + `excludeFilters` (system tasks/mappers).

### 7.1 Disposition table

Legend — **Enterprise concerns** = org_id / RBAC / multi‑tenant / SKU‑limits / audit / CDC. *Introspection* (`io.orkes.conductor.introspection.*`) and the Orkes *evaluator stack* (`OrkesJavascriptEvaluator`/`graaljs`/`ConsoleBridge`) are engine‑adjacent and noted separately.

| Orkes class | OSS target | What to backport | Enterprise concerns? | Disposition |
|---|---|---|---|---|
| `OrkesExclusiveJoin` | `ExclusiveJoin` | default‑exclusive‑join fallback; full output on completion | No | **Backport as‑is** |
| `OrkesForkJoinDynamicTaskMapper` | `ForkJoinDynamicTaskMapper` | already ~95% in OSS; port only `setParentTaskReferenceName` + type‑safe `getStringInput` | No | **Backport (2 small items)** |
| `OrkesSubWorkflowTaskMapper` | `SubWorkflowTaskMapper` | runtime version resolution (`isResolveSubWorkflowVersionAtRuntime`), v0→null, idempotency‑key inheritance | No | **Backport (merge w/ OSS inline‑def + priority)** |
| `OrkesJoin` | `Join` | script/JS join + console capture; large‑fork (>500) scaling w/ `captureOutput` | No (introspection + evaluator) | **Backport** (strip introspection; ship evaluator stack to OSS; keep OSS permissive/SYNC/backoff) |
| `OrkesDoWhile` | `DoWhile` | pluggable condition evaluators + console capture; retry‑aware failure detection; task reload | No (introspection + evaluator) | **Backport** (strip introspection; OSS already has richer list‑iteration) |
| `DeciderTask` | (none) | `Runnable` wrapper w/ id‑based equals/hashCode | No | **Backport as new OSS file** |
| `OrkesForkJoinTaskMapper` | `ForkJoinTaskMapper` | **parallel** branch scheduling via ExecutorService | Yes (org_id ContextPropagation) | **Both:** backport parallel scheduling; org_id propagation → enterprise |
| `OrkesSimpleTaskMapper` | `SimpleTaskMapper` | `taskDefinition.getBaseType()`; MetadataDAO fallback | Yes (`TaskMapperRBAC`) | **Both:** backport baseType/fallback; RBAC `validate()` → enterprise subclass |
| `OrkesExecutionDAOFacade` | `ExecutionDAOFacade` | decider‑queue push on create w/ FIFO priority; rate‑limit notify; terminal timestamping | Partial (CDC, introspection) | **Both:** backport queue/rate‑limit; CDC behind listener SPI; strip introspection |
| `OrkesWorkflowExecutor` | `WorkflowExecutorOps` | sync exec, idempotency strategies, sync subworkflow, rate‑limit‑aware scheduling, schema validation, parallel decide/start | **Yes** (org_id, RBAC/impersonation, SKU/OCU, per‑org metrics, introspection) | **Both:** backport engine core into `WorkflowExecutorOps` + add seams (§6.3.4); keep `OrkesWorkflowExecutor extends WorkflowExecutorOps` |
| `OrkesInline/StartWorkflow/Terminate/SetVariable/JSONJQTransform/PublishMetric/TerminateWorkflow/SubWorkflowSync` | corresponding OSS tasks | misc engine tweaks | Mostly No (introspection) | **Backport** (strip introspection) |
| `OrkesDoWhileTaskMapper/JoinTaskMapper/SwitchTaskMapper/DynamicTaskMapper/UserDefinedTaskMapper/WaitTaskMapper/GetWorkflowTaskMapper/YieldTaskMapper/TerminateWorkflowTaskMapper` | corresponding OSS mappers | engine mapper variants | No | **Backport** |
| `OrkesExecutionService` | `ExecutionService` | poll‑timeout cap, batch poll | Yes (org_id→executionNamespace, auth) | **Keep enterprise subclass** (backport minor poll deltas) |
| `OrkesMetadataService` | `MetadataServiceImpl` | — | Yes (RBAC, by‑org queries, createdBy) | **Keep enterprise subclass** |
| `OrkesGetWorkflow/OrkesUpdateTask/OrkesUpdateSecret(+mapper)/OrkesQueryProcessorTaskMapper` | (new task types) | — | Yes (RBAC/secrets/audit) | **Keep enterprise** |
| `OrkesCDCEventPublisher/Sink` | (impl of CDC ifaces) | — | Yes (org_id, introspection) | **Keep enterprise** (wire via OSS listener SPIs) |
| `OrkesHuman`, `OrkesHumanTaskMapper` | `Human`, `HumanTaskMapper` | — | Human is enterprise | **Keep enterprise** (OSS retains base `Human`/no‑op) |
| Integration mappers (`OrkesAWSLambda/BusinessRule/HTTPPoll/OpsGenie/Sendgrid/PublishMetric/UpdateTask`) | (new) | — | Enterprise connectors | **Keep enterprise** |
| Redis/in‑memory caches (`*ExecutionCache`, interfaces) | (none) | interfaces + in‑memory (drop orgId param) | Yes (tenant‑keyed Redis) | **Both:** interfaces+in‑memory → OSS; tenant‑keyed Redis → enterprise |
| Utilities (`EnvUtils`, `ExecutorUtils`, `NotificationResult`, `UniqueBlockingQueue`, `WorkflowRepairer`, `WorkflowTaskTypeConstraint`, …) | mixed | case‑by‑case | Mostly No | **Backport case‑by‑case** |

### 7.2 Two cross‑cutting decisions before backporting

1. **Introspection** (`io.orkes.conductor.introspection.*`) is the most pervasive non‑tenancy coupling (~50 call sites in the executor alone, plus Join/DoWhile/etc.). **Decision (recommended):** add a **no‑op OSS SPI** `org.conductoross.conductor.core.introspection.WorkflowIntrospection` with a stub default, so backported call sites compile and run in OSS with zero overhead; enterprise provides the real tracer. Alternative: strip call sites entirely (higher churn, loses the hook). → see §14.
2. **`TimeBasedUUIDGenerator`** is dual‑purpose: `getOrgId()` (tenant — strip) and `getDate()`/FIFO‑priority (engine). Resolve §6.3.3 before stripping the executor.

### 7.3 Bean‑override mechanics during/after backport

- **System tasks/mappers:** OSS registers `@Component("<TASK_TYPE>")`; `SystemTaskRegistry(Set<WorkflowSystemTask>)` collates by type and **throws on duplicate keys**. Therefore, while both an OSS `Join` and an `OrkesJoin` exist, the Orkes app must keep the `excludeFilters` entry. **After** a class is fully backported and the Orkes override deleted, remove it from `excludeFilters` (P5 — the list shrinks toward empty). For tasks that remain enterprise‑only subclasses, keep the exclude (or, preferred, make the OSS bean `@ConditionalOnMissingBean` so the enterprise subclass wins without an exclude — apply where the OSS task can tolerate optional override).
- **`@Primary` beans** (`WorkflowExecutorOps`/`ExecutionDAOFacade`/services): enterprise subclass annotated `@Primary` continues to win; no exclude needed.

---

## 8. Persistence convergence + `org_id` strategy

### 8.1 From technology monoliths to OSS adapters + enterprise subclasses

Today Orkes `postgres-persistence` (and mysql/redis) implement **both** OSS concerns and enterprise concerns and depend sideways on domain modules. Target:

- **OSS** `conductor-postgres-persistence` (consumed as an artifact) provides the OSS adapters (`ExecutionDAO`, `MetadataDAO`, `QueueDAO`, `PollDataDAO`, optional `IndexDAO`/`Lock`/`FileMetadataDAO`) with the generic `applyQueryExtensions`/`applyWriteExtensions` seams.
- **Enterprise** `enterprise-persistence-postgres` provides:
  - subclasses of the OSS adapters that override the seams to add `org_id`;
  - the enterprise stores that have **no** OSS counterpart (RBAC, webhook, human, integration, gateway, org‑config, archive, registry, audit, limits), each implementing its enterprise port.
- The enterprise persistence module depends only on OSS persistence artifacts + enterprise ports — **not** on other technology modules.

### 8.2 The org_id mechanism (extension by inheritance) — restated end‑to‑end

1. OSS adapter builds the base query via `SqlQueryBuilder`/`SqlInsertBuilder`.
2. OSS adapter calls `applyQueryExtensions(qb, ctx)` / `applyWriteExtensions(ib, ctx)` (no‑op in OSS) right before render, in **every** path.
3. Enterprise subclass overrides those hooks: `super(...)` then append `org_id = :orgId` (reads `OrgContext.current().orgId()`), add `org_id` to insert columns and `ON CONFLICT` target.
4. Cross‑org/admin queries (e.g. `getAllEventHandlersAllOrgs`, API‑gateway `1=1` bypass) are explicit enterprise methods that do **not** call the hook (or pass a context that suppresses it).
5. Raw‑JDBC methods in the current Orkes DAOs are migrated onto the builder so the hook is universal (today some bypass the query wrapper).

### 8.3 What about the runtime `hasTableOrgId` probe?

Today Orkes branches SQL on whether a partition has `org_id` (`PostgresPartitionManagerDAO.hasTableOrgId`). This is an enterprise concern; it stays in the enterprise subclass/override and is invisible to OSS. New enterprise tables are born with `org_id`; legacy probing remains enterprise‑side.

---

## 9. Database migrations strategy

- **OSS** ships its own Flyway locations per technology (e.g. `classpath:db/migration_postgres`, scheduler `classpath:db/migration_scheduler` with history table `flyway_schema_history_scheduler`). OSS tables have **no** `org_id`.
- **Enterprise** ships an **additional** Flyway location (e.g. `classpath:db/migration_orkes_postgres`) with its own history table, that:
  - adds `org_id` to OSS tables and recomposes PKs (today's `V714/V802/V803` pattern), and
  - creates enterprise‑only tables (rbac/webhook/human/integration/gateway/org/archive…), born with `org_id`.
- The enterprise server configures Flyway with **both** locations (today merged in `PostgresArchiveDAOConfiguration`); the OSS server configures only OSS locations.
- Keep enterprise migrations idempotent (`IF NOT EXISTS`) and out‑of‑order tolerant (already the case).

---

## 10. Module map (OSS vs Enterprise) + cleanup

### 10.1 OSS (public, consumed as artifacts)

Reused from `conductor-oss/conductor`: `conductor-common`, `conductor-core`, `conductor-<db>-persistence` (postgres/mysql/cassandra/sqlite/redis), `conductor-redis-*`, `conductor-scheduler-core` + `conductor-scheduler-<db>-persistence`, `conductor-http-task`, `conductor-json-jq-task`, `*-storage`, `*-event-queue`, listeners, `conductor-rest`, `conductor-grpc*`, `conductor-server`/`server-lite`. Enhanced in place by the backport (§7) and by new seams (§6.3).

### 10.2 Enterprise (private, layered on OSS)

`enterprise-common` (OrgContext/IDTools/audit/flags), `enterprise-security` (RBAC/auth/SCIM/federation), `enterprise-engine` (`OrkesWorkflowExecutor` + remaining enterprise task subclasses), `enterprise-persistence-<db>` (org_id subclasses + enterprise stores + enterprise migrations), `scheduler-enterprise`, `webhooks`, `human`, `integration`, `event-processor`, `event-integration`, `api-gateway`, `api-orchestration`, `archive-persistence`, `ssm-properties`, `workers` (external), `server-enterprise`.

### 10.3 Delete (stale build‑only dirs in orkes‑conductor; not in `settings.gradle`)

`auth/`, `rest/`, `metrics/`, `core-api/`, `core-decider/`, `oss-decider/`, `orkes-decider/` (each contains only `build/`). Also remove `oss-core/` once Orkes consumes the OSS `conductor-core` artifact.

### 10.4 Namespacing during moves

When a class moves from `oss-core` (re‑implemented `com.netflix.conductor.*`) to consuming the OSS artifact, it simply **disappears** from Orkes (the OSS artifact provides it). When a new OSS seam is added, it is `org.conductoross.conductor.*`. Enterprise stays `io.orkes.conductor.*`.

---

## 11. Phased execution plan

Each phase is independently shippable, gated by tests + the dependency checker. **Hexagonal decoupling and backporting lead; the repo/artifact cleave follows.**

### Phase 0 — Guardrails & cleanup (no behavior change)
**OSS & Enterprise.**
- Add ArchUnit + a Gradle dependency‑rule task that fails on illegal `project(':...')` edges; baseline current violations (record them, don't fix yet).
- Delete stale build‑only dirs in Orkes (§10.3) — except `oss-core` (removed in Phase 6).
- Stand up `docs/convergence/` (this doc) and a CHANGELOG for the effort.
- Acceptance: both repos build; checker runs and prints the baseline; no functional change.

### Phase 1 — New OSS seams (additive, no behavior change)
**OSS.**
- Add `SqlQueryBuilder`/`SqlInsertBuilder` + `QueryContext` (`org.conductoross.conductor.persistence.query`) and wire the OSS `*-persistence` base DAOs to render via the builder; add no‑op `applyQueryExtensions`/`applyWriteExtensions` hooks called in every path.
- Add the no‑op `WorkflowIntrospection` SPI + stub (if Decision §14.1 = shim).
- Add `protected` seams to `WorkflowExecutorOps` (§6.3.4) with default behavior identical to today.
- Optionally add OSS `TimeBasedIDGenerator` (property‑gated).
- Acceptance: OSS unit + contract tests pass unchanged; `publishToMavenLocal` succeeds.

### Phase 2 — Backport pure‑engine improvements (Orkes → OSS)
**OSS** (source the diffs from Orkes `server/.../com/netflix/conductor/**`).
- Backport, in order of safety: `OrkesExclusiveJoin`→`ExclusiveJoin`; `ForkJoinDynamicTaskMapper` two items; `OrkesSubWorkflowTaskMapper`→`SubWorkflowTaskMapper`; `DeciderTask` (new); `OrkesJoin`→`Join` (+ evaluator stack, strip introspection); `OrkesDoWhile`→`DoWhile`; the clean mappers/tasks; `OrkesForkJoinTaskMapper` parallel scheduling (without org_id propagation); engine core of `OrkesWorkflowExecutor`→`WorkflowExecutorOps` (using the Phase‑1 seams).
- Each backport: port behavior, strip introspection (→ no‑op SPI) and any org_id, keep/merge existing OSS improvements, add/extend OSS tests.
- Acceptance: OSS test‑harness green; behavior parity demonstrated by ported Orkes tests where applicable.

### Phase 3 — Prove enterprise layering on one vertical (executor + one store)
**Enterprise** (still inside `orkes-conductor`, consuming OSS via mavenLocal for the touched modules).
- Re‑express `OrkesWorkflowExecutor` as `extends WorkflowExecutorOps` overriding the Phase‑1 seams (org_id/RBAC/limits/CDC/introspection). Delete the duplicated engine code.
- Convert one OSS store (e.g. postgres `ExecutionDAO`) to an enterprise subclass overriding `applyQueryExtensions` for org_id; migrate its raw‑JDBC methods to the builder.
- Acceptance: enterprise e2e for that vertical passes; `excludeFilters` shrinks by the backported tasks.

### Phase 4 — Decouple persistence (remove sideways deps) + scheduler to OSS pattern
**Enterprise.**
- Split each technology persistence monolith into: OSS‑adapter consumption + `enterprise-persistence-<db>` (subclasses + enterprise stores). Remove `postgres/mysql/redis → scheduler/api-gateway/integration/human/api-orchestration` edges (move those stores into their enterprise feature modules or the enterprise persistence module that owns the port).
- Replace `scheduler-oss`/`scheduler-enterprise` coupling: consume OSS `conductor-scheduler-core` + `conductor-scheduler-<db>-persistence`; `scheduler-enterprise` extends OSS scheduler‑core.
- Acceptance: dependency checker shows zero sideways edges for persistence; scheduler runs via OSS adapters.

### Phase 5 — Evict org/tenant from remaining OSS‑bound code; finalize enterprise modules
**Enterprise.**
- Move `OrgContext`/orgId/`IDTools`(org‑encoding)/audit‑with‑org into `enterprise-common`; consolidate ID‑generator duplicates.
- Ensure webhooks/human/integration/event‑*/api‑gateway/api‑orchestration/archive‑persistence/ssm‑properties/security are clean enterprise modules depending only on OSS artifacts + enterprise ports.
- Acceptance: no `io.orkes`/org_id references remain in any code destined for OSS; enterprise builds against OSS artifacts only.

### Phase 6 — Cut over Orkes to OSS artifacts; delete `oss-core`
**Enterprise.**
- Remove the root `build.gradle` excludes for `org.conductoross:conductor-core` / `com.netflix.conductor`; add dependencies on the published OSS artifacts; **delete `oss-core/`**.
- Update `settings.gradle` and module `build.gradle`s accordingly.
- Acceptance: full enterprise server boots and passes e2e using OSS `conductor-core` artifact + enterprise modules; no re‑implemented engine remains.

### Phase 7 — Publish, compose, harden
**Both.**
- OSS: finalize artifact coordinates/versioning; publish to GitHub Packages/S3 (CI).
- Enterprise: pin OSS artifact version; produce branded composite `server-enterprise`.
- Enforce ArchUnit/dependency gates in CI for both repos; run parity + migration + e2e suites for both editions.
- Acceptance: OSS edition (org‑free, noauth) and enterprise edition (today's behavior) both green in CI.

---

## 12. Build, publish (maven local), and composite consumption

**OSS → maven local (dev loop):**
```bash
# in ~/workprojects/conductor (branch orkes-converge-fork-conductor)
./gradlew publishToMavenLocal -x test            # publishes org.conductoross:conductor-* to ~/.m2
```

**Enterprise consumes mavenLocal:** `orkes-conductor/build.gradle` already lists `mavenLocal()` first in `subprojects.repositories`. During migration, point the relevant module dependencies at the OSS coordinates, e.g.:
```gradle
implementation "org.conductoross:conductor-core:<version>"          // replaces project(':orkes-conductor-oss-core')
implementation "org.conductoross:conductor-postgres-persistence:<version>"
```
Remove the global excludes in `orkes-conductor/build.gradle` (`exclude group: 'org.conductoross', module: 'conductor-core'`, and the `com.netflix.conductor` exclude) as modules cut over.

**Version pinning:** use a single `revConductor` property in `gradle.properties`; bump deliberately. For CI, publish from the OSS branch to GitHub Packages and have the enterprise CI resolve that version.

---

## 13. Testing & verification

- **Port contract tests:** reuse OSS `java-test-fixtures` (`AbstractSchedulerDAOTest`, etc.); add equivalents for any new ports. Enterprise adapters subclass the OSS contract tests to prove `super()` behavior is preserved.
- **Engine parity:** port the Orkes engine unit tests for each backported class into OSS; keep them green. For enterprise subclasses, add tests proving org_id/RBAC/limits apply on top.
- **Migration tests:** OSS migration set (no org_id) and enterprise migration set (adds org_id) both verified with Testcontainers.
- **Two‑edition e2e:** OSS edition boots org‑free + noauth and runs canonical workflows; enterprise edition matches current behavior (RBAC, multi‑tenant, CDC).
- **Dependency gate:** ArchUnit + Gradle checker in CI; fail on illegal edges.
- **JaCoCo:** keep aggregated coverage; ensure backported classes retain/raise coverage.

---

## 14. Decisions (locked) & remaining risks

### 14.0 Locked decisions (confirmed via review)
- **D1 — Introspection:** OSS gets a no‑op SPI `org.conductoross.conductor.core.introspection.WorkflowIntrospection` + stub default; enterprise registers the real tracer. All backported call sites target this SPI.
- **D2 — Time‑based IDs:** OSS gains a property‑gated `org.conductoross.conductor.core.utils.TimeBasedIDGenerator` (no org prefix), selected by `conductor.id.generator=time_based`; default stays UUID v4. FIFO‑by‑creation‑time priority is active only when the time‑based generator is selected. Enterprise keeps the org‑prefixing subclass + `getOrgId`/`getDate` decode.
- **D3 — System‑task override:** OSS system‑task/mapper beans become overridable via `@ConditionalOnMissingBean` so enterprise subclasses win without `excludeFilters`; the enterprise exclude list is retired as classes are backported.
- **D4 — REST authZ:** OSS controllers are auth‑free (noauth/permit‑all default); enterprise applies authorization as a security layer (filters + method‑security) and adds protected endpoints.
- **D5 — Backport target:** backports land on the Orkes‑controlled fork branch (`orkes-converge-fork-conductor` on `bradyyie/conductor`) and are published from there; upstreaming to `conductor-oss/conductor` is decided later.

### 14.1 Remaining engineering risks (not user decisions)
- **Write‑path org_id in PK/`ON CONFLICT` (§6.3.2/§8.2):** the hardest seam; `SqlInsertBuilder` must compose conflict targets. Validate on `ExecutionDAO` insert/upsert first.
- **Raw‑JDBC migration completeness:** some Orkes DAO methods bypass the query wrapper; all must move onto the builder or the org_id hook will miss them.
- **Cross‑repo extension requires stable OSS APIs:** OSS superclasses stay non‑final with stable `protected` seams; treat as public API (semver).
- **Release coordination:** OSS artifact version pinned in enterprise.
- **`scheduler/core` namespace quirk:** OSS scheduler ports are `io.orkes.conductor.*` (already upstream); leave as‑is; new ports use `org.conductoross.conductor.*`.

---

## 15. Appendix — authoritative file‑path index

### OSS (`~/workprojects/conductor`)
- Ports: `core/src/main/java/com/netflix/conductor/dao/{ExecutionDAO,MetadataDAO,QueueDAO,PollDataDAO,RateLimitingDAO,ConcurrentExecutionLimitDAO,IndexDAO,EventHandlerDAO,WorkflowMessageQueueDAO}.java`; `core/.../org/conductoross/conductor/dao/FileMetadataDAO.java`
- DAL facade: `core/src/main/java/com/netflix/conductor/core/dal/ExecutionDAOFacade.java`
- Engine: `core/.../core/execution/WorkflowExecutor.java`, `WorkflowExecutorOps.java`, `AsyncSystemTaskExecutor.java`, `DeciderService.java`
- System tasks: `core/.../core/execution/tasks/{WorkflowSystemTask,SystemTaskRegistry,SystemTaskWorkerCoordinator,SystemTaskWorker,Join,ExclusiveJoin,DoWhile,SubWorkflow,Event,Wait,SetVariable,Terminate,Switch,Inline,StartWorkflow,Human,Fork,Noop,PullWorkflowMessages}.java`
- Mappers: `core/.../core/execution/mapper/{ForkJoinDynamicTaskMapper,ForkJoinTaskMapper,JoinTaskMapper,SubWorkflowTaskMapper,SimpleTaskMapper,SwitchTaskMapper,DoWhileTaskMapper,WaitTaskMapper,...}.java`
- SPIs: `core/.../core/listener/{WorkflowStatusListener,TaskStatusListener}.java` (+ `…Stub`); `core/.../org/conductoross/conductor/core/listener/MetadataChangeListener.java`; `common/.../common/utils/ExternalPayloadStorage.java`; `core/.../core/events/{EventQueueProvider,EventQueues}.java`; `core/.../core/sync/Lock.java` + `…/sync/noop/NoopLock.java`; `core/.../service/ExecutionLockService.java`
- IDs: `core/.../core/utils/IDGenerator.java`
- Core config: `core/.../core/config/ConductorCoreConfiguration.java`
- Postgres adapter: `postgres-persistence/.../postgres/config/PostgresConfiguration.java`, `postgres-persistence/.../postgres/dao/PostgresBaseDAO.java`
- Scheduler reference: `scheduler/core/.../dao/scheduler/SchedulerDAO.java`, `…/dao/scheduler/SchedulerCacheDAO.java`, `…/dao/archive/SchedulerArchivalDAO.java`, `…/scheduler/config/{SchedulerOssConfiguration,SchedulerConditions}.java`, `scheduler/core/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`; adapter `scheduler/postgres-persistence/.../scheduler/postgres/{dao/PostgresSchedulerDAO,config/PostgresSchedulerConfiguration}.java` + its `AutoConfiguration.imports` + `db/migration_scheduler/`
- App entrypoint: `server/src/main/java/com/netflix/conductor/Conductor.java`

### Enterprise (`~/workprojects/orkes-conductor`)
- Engine overrides: `server/src/main/java/com/netflix/conductor/core/execution/{OrkesWorkflowExecutor,OrkesExecutionDAOFacade,DeciderTask,WorkflowRepairer}.java`, `.../tasks/Orkes*.java`, `.../mapper/Orkes*.java`
- App + excludeFilters: `server/src/main/java/io/orkes/conductor/OrkesConductorApplication.java`
- Context/IDs: `common/.../common/context/OrkesRequestContext.java`, `common/.../id/IDTools.java`, `core/.../id/TimeBasedUUIDGenerator.java`, `workers/.../event/TimeBasedUUIDGenerator.java`
- Feature flags/conditions: `common/.../config/{FeatureFlags,PermissionsDisabledCondition,AuthnAndAuthzEnabledCondition}.java`
- Security: `core/.../security/*`, `server/.../security/*`
- Persistence (to split): `postgres-persistence/.../dao/postgres/**`, `mysql-persistence/**`, `redis-persistence/**`; org filter: `postgres-persistence/.../gateway/OrgFilterHelper.java`; query util: `postgres-persistence/.../util/Query.java`
- Flyway (enterprise): `server/.../dao/config/PostgresArchiveDAOConfiguration.java`, `postgres-persistence/src/main/resources/db/migration_archive_postgres/`

---

---

## 16. Execution log

### Phase 0 — guardrails & cleanup ✅ (orkes branch)
- `moduleDependencyReport` Gradle task (report-only) + tracked baseline (`docs/convergence/baseline-module-dependencies.txt`); 29 sideways violations recorded to burn down.
- Removed 7 stale build-only orphan dirs (`auth/`, `rest/`, `metrics/`, `core-api/`, `core-decider/`, `oss-decider/`, `orkes-decider/`).

### Phase 1 — OSS seams ✅ (OSS branch, additive, no behavior change)
- `org.conductoross.conductor.core.introspection.*` — `WorkflowIntrospection` static facade + `RecordBuilder` + `WorkflowIntrospectionProvider` SPI + NoOp default (decision D1).
- `org.conductoross.conductor.core.utils.TimeBasedIDGenerator` — opt-in `conductor.id.generator=time_based`, v1 time-ordered UUIDs, no org prefix (decision D2).
- Verified: compiles, spotless clean, `publishToMavenLocal` → `org.conductoross:conductor-core:3.30.2-rc1` contains both seams.

### D3 — system-task / mapper override seam ✅ (OSS branch)
- `WorkflowSystemTask.isOverride()` / `TaskMapper.isOverride()` (default false).
- `SystemTaskRegistry.byType()` collates by type preferring the override (ambiguous duplicates still fail fast); `asyncSystemTasks` derives from the override-resolved set; `getTaskMappers` prefers an overriding mapper while keeping non-override first-wins (preserves duplicate `KafkaPublishTaskMapper` tolerance). `SystemTaskRegistryTest` added.
- Effect: enterprise can replace a built-in task/mapper via an `isOverride()=true` bean instead of `excludeFilters`; the Orkes `excludeFilters` list can be retired as overrides are backported or re-expressed.

### Additional OSS seams ✅ (OSS branch, additive, tested)
- **WorkflowExecutorOps extension seams** — `protected beforeStartWorkflow(input, def)` (invoked in `startWorkflow` + `startWorkflowIdempotent`; may throw to abort) and `protected onDecide(workflowId)` (top of `decide(String)`). No-ops in OSS; enable the enterprise executor to subclass instead of replacing. `WorkflowExecutorOpsSeamsTest` added.
- **SqlQueryBuilder named-bind seam (§6.3.1 foundation)** — `org.conductoross.conductor.persistence.query.{SqlQueryBuilder,QueryContext}`: named `:markers` → positional `?`, so a subclass DAO can `.and("org_id = :orgId").bind(...)` on top of a base query without disturbing bind ordering. `QueryContext` is feature-neutral. `SqlQueryBuilderTest` added. (DAO adoption + `applyQueryExtensions` hooks remain the follow-up — the hardest seam, best done with a DB/Testcontainers environment and the enterprise subclasses.)

Full `conductor-core` suite green after all of the above (813 tests, 0 failures, 7 skipped); `org.conductoross:conductor-core:3.30.2-rc1` re-published to maven local.

### Phase 6 — cutover analysis (done) & reconciliation (in progress)

Ground truth measured against the published OSS `conductor-core`:
- All 39 OSS artifacts published to **maven local** (`conductor-core:3.30.2-rc1`, …) — Orkes can resolve them.
- OSS green under Docker: `conductor-core` (814) + `conductor-postgres-persistence` (Testcontainers) pass.
- Orkes baseline: `oss-core` + `core` compile today.
- `oss-core` (150 files) vs OSS `conductor-core` (182): **20 Orkes-only**, **89 shared-but-modified (~9,400 diff lines), bidirectional** — OSS is *ahead* on several engine files (`ParametersUtils` recursive `${}`, `Event` two-phase, `WaitTaskMapper`, WMQ, file-storage), Orkes ahead on others.

Cutover constraint: a class cannot live in both `oss-core` and `conductor-core` (duplicate-class/classpath conflict), so **`oss-core` can only be deleted once OSS `conductor-core` is a superset of every engine API the Orkes enterprise modules consume.** Therefore the path is: (1) backport Orkes engine deltas into OSS (de-enterprised, OSS staying green) until superset; (2) relocate the 20 Orkes-only enterprise classes (`CDCEventPublisher/Sink`, `OrgConfigDAO`, `ResourceSharingDAO`, `IdempotencyDAO`, `EnvironmentDAO`, `WorkflowRateLimiterDAO`, `MetadataExecutionDAO`, Orkes evaluator stack, `ApplicationException`) into Orkes `core`; (3) flip 19 modules' `project(':orkes-conductor-oss-core')` → `org.conductoross:conductor-core`, delete `oss-core`, iterate compile→fix; (4) green both suites (Orkes harness/e2e via Postgres+Redis in Docker). Steps are individually verifiable; neither suite is left broken.

### Phase 6 — cutover EXECUTION status (in progress)

**Done & verified:**
- Cutover proven mechanical (not a 9.4k-line rewrite): Orkes `core` compiles against published OSS `conductor-core` with only a tiny API gap.
- Relocated all 20 Orkes-only `oss-core` classes → Orkes `core` (+ `ApplicationException` → `common`, the universal base); renamed Orkes `ConsoleBridge` → `OrkesConsoleBridge` (OSS owns the name); added GraalVM deps to `core`.
- Flipped all 19 modules `oss-core` → `org.conductoross:conductor-core`; bumped `revConductor=3.30.2-rc1`; dropped the `conductor-core` exclude; **deleted `oss-core`** (settings + dir).
- Backported to OSS `conductor-core` (additive, **OSS suite green: 815/0**): listener `onWorkflowCreated`/`onTasksCreated`; `WorkflowModel` fields `rateLimitKey`/`rateLimited`/`systemMetadata`/`history`/`workflowConsistency`; `WorkflowConsistency` enum; `StartWorkflowInput.workflowConsistency`; `WorkflowExecutor.addTaskToQueue`; `WorkflowSystemTask.getIdentity`; `ParametersUtils.substituteSecret`; `ExecutionDAO` event helpers/enums.
- Modules compiling green: `core`, `common`, `queues`, `redis-configuration`, `scheduler-oss`, `proto`.

**Remaining: 364 compile errors from a bounded root set** (many fan out from one symbol, e.g. `Monitors.CriticalError` → 62):
1. **Clean backports to OSS** (mechanical, ~majority of errors): `Monitors` additions (`CriticalError` enum + `recordCriticalError`, `recordWorkflowRateLimited`, `recordBatchJobExecutionTime`, `recordDatadogPublishFailure`, `recordWorkflowStart/Complete`, `recordTaskCreated/StartCount/Complete`, and their event-counter/timer helpers); `ConductorProperties` getters (`getEventExecutionRetentionDuration`, `getMetadataCacheTTL`, `getEventCleanupLoopSize`, `isFallbackEnabled`); `ExecutionDAOFacade` methods (`getTaskIdsForWorkflow`, `getWorkflowWithTasks`, `removeWorkflowPayload`, `removeTaskPayload`, `getRunningWorkflowCountByName`, `getInProgressTaskCountByName`, `writesToArchiveShards`).
2. **Enterprise/OSS split decisions** (NOT plain backports — must stay enterprise or be de-orged): `Monitors` org-tagged size metrics (`recordTaskSize`/`recordWorkflowSize` use `sizeMetricOrgId()` → keep enterprise `OrkesMonitors`, route Orkes callers there); Orkes-typed `ExecutionDAO`/`ExecutionDAOFacade` event-message methods (`addEventMessage`/`updateEventMessage`/`getEventMessages`/`getEventExecutions` take `io.orkes…EventMessage`/`ExtendedEventExecution`) → define an enterprise `OrkesExecutionDAO extends ExecutionDAO`, have the Orkes Postgres/Redis DAOs implement it, and reference that type at the Orkes call sites.
3. After compile-green: run Orkes test-harness/e2e (Postgres+Redis via Docker) and fix runtime/bean wiring (the `isOverride()` seam + `@Primary` replace the old `excludeFilters`).

Estimate: the clean backports are a few focused hours; the enterprise-split items are deliberate (interface extraction + call-site retyping). Both Orkes and OSS branches are committed at each green checkpoint; OSS never left broken.

### Phase 6 — cutover progress log (live)

Reduced Orkes from a feared ~9.4k‑line engine merge to a **bounded, enumerated remainder**. Structural cutover is DONE (oss‑core deleted; 19 modules flipped to `org.conductoross:conductor-core`; 20 classes relocated). OSS is a superset for ~35 engine APIs and **stays green (815/0)**. Orkes main‑compile error count drove **9.4k‑fear → 364 → 286 → 218 → 206 → 160**.

Backported to OSS (additive): listeners `onWorkflowCreated`/`onTasksCreated`; `WorkflowModel` `rateLimitKey`/`rateLimited`/`systemMetadata`/`history`/`workflowConsistency`; `WorkflowConsistency`; `StartWorkflowInput.workflowConsistency`; `WorkflowExecutor.addTaskToQueue`; `WorkflowSystemTask.getIdentity`; `ParametersUtils.substituteSecret`; `ExecutionDAO` event helpers + `getTaskIdsForWorkflow`/`getWorkflowWithTasks`/`removeWorkflowPayload`/`removeTaskPayload`/`getRunningWorkflowCountByName`/`getInProgressTaskCountByName`/`writesToArchiveShards`; `ExecutionDAOFacade` delegators; `Monitors` `CriticalError`+recorders (feature‑agnostic); `ConductorProperties` event/cache props. Enterprise EventMessage split DONE via `io.orkes.conductor.dao.OrkesExecutionDAO`.

**Remaining 160 main‑compile errors — three classes, each needing deliberate handling:**
1. **Signature CONFLICTS (~16)** — Orkes changed shared‑interface return types/args that clash with OSS and can't both exist: `ExecutionDAO.removeTask(String)` (Orkes `TaskModel` vs OSS `boolean`); `MetadataDAO.updateTaskDef(TaskDef)` (Orkes `String` vs OSS `TaskDef`); `getRunningWorkflowIds` arity. **Decision needed:** adopt OSS signature and adapt Orkes callers, OR add parallel Orkes methods on `OrkesExecutionDAO`/`OrkesMetadataDAO` (e.g. `TaskModel removeTaskAndReturn(String)`). Do NOT change OSS return types (breaks OSS impls/tests).
2. **`@Override` mismatches (~90)** — Orkes DAO impls override Orkes‑interface methods absent from OSS. Per method: backport (OSS‑typed) to OSS as opt‑in default, or move to an Orkes‑extended DAO interface (like `OrkesExecutionDAO`) + retype callers, or drop `@Override` where it's a genuine extra.
3. **conductor‑ai version skew (~30+)** — AI config constructors (`OpenAIConfiguration`, `AnthropicConfiguration`, …) changed between conductor‑ai `3.30.0.rc8`→`3.30.2-rc1` (from the `revConductor` bump). **Decision:** pin conductor‑ai independently, or update the Orkes AI configs to the new constructors.

After main‑compile green: `compileTestJava` (a few test‑ctor/`verify(...)` updates from the EventMessage retyping), then Orkes test‑harness/e2e under Docker, then bean‑wiring via `isOverride()`/`@Primary` (retire `excludeFilters`). Every OSS change is committed green; Orkes is committed at each WIP checkpoint with the exact remainder recorded.

### Phase 2 — engine backports (OSS branch) — in progress
Done (clean, independent, tested):
- **ExclusiveJoin** — reload joined task via `WorkflowExecutor.getTask(id)` for full output on completion; kept OSS permissive + failure aggregation. `ExclusiveJoinTest` added.
- **ForkJoinDynamicTaskMapper** — nested-fork `parentTaskReferenceName` stamping + type-safe `getStringInput` (clear `TerminateWorkflowException` instead of `ClassCastException`). Type-safety test added.
- **SubWorkflowTaskMapper** — version 0→null, null-safe name, optional runtime version resolution via new `conductor.app.resolve-sub-workflow-version-at-runtime` (default false). Deferred-resolution test added.
- **Join (large-fork)** — `captureOutput` toggle: skip copying every forked output into the JOIN output when `joinOn.size() >= LARGE_FORK_LIMIT` (500), overridable per task. Preserves OSS async model + permissive/SYNC/backoff. Capture-toggle tests added.
- **SimpleTaskMapper** — metadata-store fallback when a SIMPLE task has no inline definition (then default `TaskDef(name)`, cached back), and `baseType` override (scheduled task takes the def's base type). RBAC stays enterprise via the D3 `isOverride()` seam. baseType/fallback test added.

Assessed — no clean backport needed:
- **DoWhile** — OSS `DoWhile` is already *ahead* of `OrkesDoWhile`: richer list-iteration (`items`/`loopItem`/`loopIndex`), retry handling via highest-retry-count `relevantTasks` selection, and the successor-scheduling guard. The only Orkes-unique bits are the pluggable-evaluator/console path (the larger evaluator-stack decision below) and a per-iteration task reload (unclear value). No model-independent improvement to port.

Remaining (larger, dedicated PRs / decisions — some may stay enterprise):
- **Join sync model + script/JS join, and `JoinTaskMapper`** — the Orkes "EfficientJoin v2" is *synchronous* (`isAsync()=false`) with expression-based joins via the evaluator stack (`OrkesJavascriptEvaluator`/`graaljs`/`ConsoleBridge`) + `addTaskExecLog` console capture. OSS already ships the evaluator stack (incl. `ConsoleBridge`), but reconciling the sync model + script path with OSS's async `Join` is an opinionated change (may stay enterprise). If pursued, `JoinTaskMapper` must seed join input from the JOIN task's declared `inputParameters` **by copy** (the Orkes version mutates the definition in place) and keep OSS `isolationGroupId` — so it lands with `Join`.
- **WorkflowExecutorOps engine core + `protected` seams (§6.3.4) + DeciderTask** — backport sync exec / idempotency / sync-subworkflow / parallel decide-start; `DeciderTask` (Runnable wrapper) lands with the parallel executor that consumes it. Enterprise re-expresses `OrkesWorkflowExecutor extends WorkflowExecutorOps` over the seams.

### Deferred follow-ups
- ~~Add a persisted `idempotencyKey` to `WorkflowModel` to enable sub-workflow idempotency-key inheritance.~~ ✅ Done: `WorkflowModel.idempotencyKey` (set on start, auto-mapped in `toWorkflow()`); `SubWorkflowTaskMapper` now inherits the parent key when a strategy is set but no key. (Persistence json round-trip validated by the persistence modules' Testcontainers tests.)

### Status snapshot & the remaining program

**OSS branch — landed & verified (full `conductor-core` suite green: 814 tests, 0 failures):**
- Seams: introspection SPI, `TimeBasedIDGenerator`, D3 system-task/mapper override (`isOverride()`), `WorkflowExecutorOps` `beforeStartWorkflow`/`onDecide`, `SqlQueryBuilder` named-bind query seam.
- Backports: ExclusiveJoin, ForkJoinDynamicTaskMapper, SubWorkflowTaskMapper, Join (large-fork), SimpleTaskMapper.
- `org.conductoross:conductor-core:3.30.2-rc1` published to maven local with all of the above.

**Orkes branch — landed:** convergence plan + execution log, Phase 0 dependency-rule guardrail + baseline (29 sideways violations recorded).

**Remaining work is the larger structural program** (each requires the enterprise build + a DB/Docker environment, and in most cases the Phase 6 artifact cutover, so it cannot be completed purely OSS-side):
1. **Persistence org_id seam adoption** — migrate OSS `*-persistence` DAOs onto `SqlQueryBuilder` and add no-op `applyQueryExtensions`/`applyWriteExtensions` hooks; validate on `ExecutionDAO` insert/upsert (org_id in PK / `ON CONFLICT`) with Testcontainers. Hardest seam.
2. **WorkflowExecutorOps engine-core backport** — sync exec / idempotency strategies / sync sub-workflow / parallel decide-start + `DeciderTask`; the executor seams are already in place for the enterprise subclass.
3. **Enterprise re-expression** — once OSS artifacts are consumed (Phase 6): `OrkesWorkflowExecutor extends WorkflowExecutorOps`, enterprise task/mapper subclasses marked `isOverride()=true` (retire `excludeFilters`), enterprise persistence subclasses overriding the query hooks for org_id.
4. **Hexagonal decoupling (orkes)** — burn down the 29 sideways violations (persistence → scheduler/api-gateway/integration/human/api-orchestration, etc.); flip `moduleDependencyReport` to fail-on-new.
5. **Repo cutover & publishing (Phase 6)** — delete `oss-core`, drop the `org.conductoross:conductor-core` / `com.netflix.conductor` excludes, depend on published OSS artifacts; stand up CI publishing (GitHub Packages/S3); branded composite `server-enterprise`.


### Phase 6 — cutover COMPLETE: both repos compile + core suites green ✅

**Structural cutover (orkes branch):** `oss-core` deleted; 20 Orkes-only classes relocated into `core` (+ `ApplicationException` into `common`); 19 modules flipped to `org.conductoross:conductor-core:3.30.2-rc1` (`revConductor`=3.30.2-rc1, `revConductorAi` pinned 3.30.0.rc8); `conductor-core`/`com.netflix.conductor` excludes removed.

**Enterprise interfaces (can't live on feature-agnostic OSS):**
- `io.orkes.conductor.dao.OrkesExecutionDAO extends ExecutionDAO` — Orkes EventMessage/ExtendedEventExecution persistence (addEventMessage/updateEventMessage/getEventMessages/getEventExecutions + addEventExecution/updateEventExecution/removeEventExecution overloads). Implemented by Postgres/MySQL/Redis/Archive execution DAOs.
- `io.orkes.conductor.dao.OrkesMetadataDAO extends MetadataDAO` — org/permission lookups (getWorkflowDefs(name,subjects,accesses), getTaskDefsWithPermissions, getShortenedWorkflowDefs, getWorkflowsByOrg, getTasksByOrg). Implemented by AbstractMetadataDAO (postgres) + MySQLMetadataDAO.

**OSS superset backports (additive, neutral — no org concepts):** `ExecutionDAOFacade` extension seams (protected pushToDeciderQueue/removeFromDeciderQueue, createOnly, removeEventTask, sendTaskStatusChange no-op, getRunningWorkflowCountByName(int)); `RateLimitingDAO.getPostponeDurationForTask` (static); `IndexDAO.searchTask` default; `TaskModel.statusListenerSink`; `StartWorkflowInput.createdBy/notifications`; `WorkflowExecutor.upgradeRunningWorkflowToVersion` default; `ConductorProperties` (taskUpdate/workflowStart executor thread counts, syncSystemTaskMaxCallbackAfterSeconds, honorSyncSystemTaskCallbackAfter); `Monitors.recordWorkflowStart/Complete` + **payload-size summaries** (`*_size_bytes/_ratio`, gated by `setSizeLimitMetricsEnabled`, neutral tags).

**Enterprise overrides reconciled to OSS signatures:** `OrkesExecutionDAOFacade` (9-arg super ctor, overrides the new OSS seams), `OrkesWorkflowExecutor` (implements `startWorkflowIdempotent`, overrides `upgradeRunningWorkflowToVersion`), `OrkesMetadataService` (OrkesMetadataDAO + `MetadataChangeListener` ctor arg + `getLatestWorkflow(orgId,name)`), `SubWorkflowSync` (IDGenerator), archive configs (primaryExecutionDAO typed OrkesExecutionDAO).

**Not-found exception regression fixed:** old Orkes `NotFoundException extends ApplicationException` (Code.NOT_FOUND → 404 via `ApplicationExceptionMapper`); OSS `NotFoundException extends RuntimeException` (no Code, not mapped). Restored by throwing `com.netflix.conductor.core.exception.ApplicationException(Code.NOT_FOUND,…)` at the 12 Orkes not-found throw-sites (ServiceRegistryService, MySQLMetadataDAO, SchedulerService; scheduler-oss now depends on orkes-conductor-common).

**Verification (this milestone):**
- OSS `conductor-core`: **815 tests, 0 failures** (7 skipped) — invariant held across all backports.
- Orkes `compileJava` + `compileTestJava`: **BUILD SUCCESSFUL** (all modules). Test-compile fixes were test-only (retype to enterprise DAOs, removeTask→boolean, createTaskDef/updateTaskDef→TaskDef, ctor arity updates, CustomRetryPolicy→Postgres/MySQLConfiguration).
- Orkes runtime suites green: postgres-persistence **435/0**; mysql metadata+ratelimiter+execution+scheduler **246/0**; redis OrkesRedisExecutionDAOTest **26/0** (incl. 3 payload-size-metric tests); archive ArchivedExecutionDAO* incl. distributed-chaos **green**.
- NOTE: full mysql-persistence suite spins one container per class and exhausts Docker locally (env, not code) — validate affected classes or run with constrained parallelism. A corrupted local `postgres:14` layer caused 2 transient audit-test failures; fixed by `docker pull`.

**Additional runtime validation:** event-processor OrkesEventProcessorTest 4/0; api-orchestration ServiceRegistryServiceTest 56/0; scheduler-oss 36/0 + scheduler-enterprise SchedulerServiceTest 35/0; server engine units TestSimpleActionProcessor 6/0, TestWorkflowExecutor 61/63.

**Real backport bug fixed:** `WorkflowModel.setNotifications(null)` NPE'd (`new ConcurrentHashMap<>(null)`); Orkes never hit it because `StartWorkflowInput.notifications` defaulted to an empty map. Fixed both (default + null-guard) — resolved TestWorkflowExecutor.testStartWorkflow/testErrorHandlingOnCreateWorkflowFailure.

**Two server-test divergences — RESOLVED:**
1. WAIT async-ness is now a config property `conductor.app.wait-task-async` (OSS `Wait` has `Wait()`=async default, `Wait(boolean)`, and `@Autowired Wait(ConductorProperties)`). Orkes `TestWorkflowExecutor` uses `new Wait(false)` (sync) → 63/0. OSS suite unaffected (default async).
2. GraalVM bumped to 25.0.2 in Orkes (matches OSS polyglot/truffle; protobuf stays 3.x) → `TestLambda` 1/0 and other script tasks load.

**Spring context load (full `@SpringBootTest`) — RESOLVED:** post-cutover, OSS `conductor-core` beans are scanned alongside Orkes overrides, which broke context load for ~90 integration tests (one cascading root cause). Fixes in `OrkesConductorApplication.excludeFilters` + one `@Primary`:
- Bean-NAME clashes excluded: OSS `GraalJSEvaluator`/`JavascriptEvaluator` ("graaljs"/"javascript" owned by Orkes' Graal evaluator), OSS `VersionService`/`WorkflowTestService`/`WorkflowMonitor`/`WorkflowIntrospection` (same default name as Orkes versions).
- Bean-TYPE ambiguities: excluded OSS `TimeBasedIDGenerator` (D2 — Orkes' org-aware `TimeBasedUUIDGenerator` wins; both `@Component` on `conductor.id.generator=time_based`); `@Primary` on `OrkesCDCEventPublisher` (implements WorkflowStatusListener+TaskStatusListener; OSS ships `matchIfMissing=true` stub listeners → single-injection was ambiguous).
- Verified across diverse `@SpringBootTest` classes: ParamUtilsSecretJSONTest 6/0, OrkesPermissionEvaluatorTest 2/0, RoleResourceTest 4/0, AuthorizationResourceTest 14/0, EventExecutionResourceTest 1/0, GatewayServiceControllerTest 42/0, ApiGatewayAccessControlTest 21/0, WorkflowSizeServiceTest 5/0. (Full server suite spins one Postgres container per `@SpringBootTest` class — run with constrained `--max-workers` to avoid Docker exhaustion.)

**(historical) Two server-test divergences originally flagged (now resolved above):**
1. `TestWorkflowExecutor.testScheduleTask` — OSS `Wait.isAsync()==true` (async; queued, not started inline) vs old Orkes `Wait` which was sync (base default `false`, no override). Orkes now uses OSS `Wait`, so `WAIT` is async at runtime → the test's inline-start counts (`startedTaskCount==2`, `queuedTaskCount==1`) reflect the old sync behavior. Decision: adopt OSS async `Wait` (update the test counts to 1 started / 2 queued) OR preserve Orkes sync `Wait` via a D3 `isOverride()` `OrkesWait extends Wait` with `isAsync()==false`.
2. `TestLambda` (+ likely other script tasks) — `ExceptionInInitializerError: Polyglot version compatibility check failed`: GraalVM Polyglot version skew between the OSS `conductor-core` (OSS `Lambda`→`ScriptEvaluator`→graal polyglot) and the Orkes server runtime classpath. Decision: align the `org.graalvm.polyglot`/`js` versions in the Orkes server (dependency, not logic).

**Additional module validation (green):** human 85/0; webhooks Postgres DAO 6/0 (+32 unit); workers 258 (incl. `OrkesConductorWorkersApplicationTest.contextLoads` — fixed by `conductor.app.sweeper.enabled=false`; the workers runtime must not run the OSS WorkflowSweeper, which is `matchIfMissing=true` and needs the sweeperExecutor); api-orchestration HTTP-client + service tests green.

**Local Docker/Testcontainers note:** if `/var/run/docker.sock` is absent (Docker Desktop), Testcontainers' pinned `UnixSocketClientProviderStrategy` fails. Workaround for container suites: `~/.testcontainers.properties` → `docker.host=unix://<~/.docker/run/docker.sock>`, env `TESTCONTAINERS_RYUK_DISABLED=true` (Ryuk can't bind-mount the Desktop socket), and `--no-daemon` so env propagates. Also constrain `--max-workers` (each `@SpringBootTest`/DAO test spins its own Postgres/MySQL container).

**OSS side validated repo-wide (~1771 tests, 0 failures across 13 modules):** `compileJava`+`compileTestJava` BUILD SUCCESSFUL across all ~40 OSS modules; suites green — conductor-core 815/0, ai 592/0, postgres-persistence 118/0, rest 86/0, common 50/0, amqp 26/0, http-task 26/0, redis-lock 13/0, grpc-server 11/0, redis-concurrency-limit 10/0, postgres-external-storage 9/0, json-jq-task 7/0, local-file-storage 6/0, grpc 2/0 (the lone `PostgresLockDAOTest` lease-timing blip under Ryuk-disabled container load passes 8/0 in isolation — flaky, not a regression). Confirms the additive `ExecutionDAO`/`MetadataDAO`/`Monitors`/`Wait` core changes don't break OSS's own reference modules.

**Retiring `excludeFilters` (D3) — DONE for 21 task/mapper beans:** the `SystemTaskRegistry.byType` and `getTaskMappers`/`preferOverridingMapper` seams (both `isOverride()`-based, unit-tested) are now actually used instead of component-scan exclusion:
- 9 WorkflowSystemTask overrides: OrkesJoin/OrkesExclusiveJoin/SubWorkflowSync/OrkesHuman/OrkesDoWhile/OrkesSetVariable/OrkesStartWorkflow/OrkesTerminate/OrkesInline — given distinct `@Component` bean names (`orkesJoin`, …) + `isOverride()=true`; OSS Join/ExclusiveJoin/SubWorkflow/Human/DoWhile/SetVariable/StartWorkflow/Terminate/Inline dropped from excludeFilters.
- 12 TaskMapper overrides: OrkesSimple/Join/Switch/Wait/DoWhile/Dynamic/UserDefined/SubWorkflow/ForkJoin/ForkJoinDynamic/Human + `EventTaskMapper` — `isOverride()=true` (default bean names already distinct except EventTaskMapper→`orkesEventTaskMapper`); OSS mappers dropped from excludeFilters.
- Resolution is deterministic (exactly one override per type); OSS bean + Orkes override coexist and the override wins by `getTaskType()`. Verified: server compiles; context loads; GatewayServiceControllerTest 42/0, ApiGatewayAccessControlTest 21/0, WorkflowSizeServiceTest 5/0, OrkesPermissionEvaluatorTest 2/0, ParamUtilsSecretJSONTest 6/0 — no regression.
- **Still excluded (justified):** Event + Wait system tasks (Orkes uses a timer-based WAIT model, not the inline `Wait`), the event-processor/queue-manager/scheduler infra (`DefaultEventProcessor`, `DefaultEventQueueManager`, `SchedulerConfiguration`, `ConductorEventQueueProvider`), the OSS evaluators (name-only `graaljs`/`javascript` clashes owned by the Orkes Graal evaluator), and the OSS re-implementations (`VersionService`, `WorkflowTestService`, `WorkflowMonitor`, `WorkflowIntrospection`, `TimeBasedIDGenerator`). The remaining excludeFilters set is now minimal and each entry has a concrete reason.

**Orkes side validated:** integration 40/0; api-orchestration 13+56/0; event-processor 4/0; human 85/0; webhooks 6/0(+32); workers 258/0; scheduler 36/0+35/0; persistence (postgres 435, mysql 246, redis 26, archive); server engine units + diverse `@SpringBootTest` (context-load) green.

**Hexagonal burndown (Phase 4) — guardrail refined; persistence split is the real fix (NOT port extraction):**
- `moduleDependencyReport` made accurate: counts PRODUCTION coupling only (test-scope wiring is composition-root, not a violation); treats `*-oss` as foundation (enterprise→oss is intended open-core layering); `workers` recognised as a `@SpringBootApplication` composition root. Net: the stale 29 (which still referenced the deleted `oss-core`) → **13 real production violations**.
- **Correction (a wrong turn was reverted):** an `api-gateway-api` sibling port module was briefly created and then **reverted**. Per §6.4 + §8 + Phase 4, enterprise DAO ports (`GatewayConfigDAO`, `HumanTaskDAO`, `EnterpriseSchedulerDAO`, …) are enterprise contracts that **belong to their feature** (gateway port in `api-gateway`, human port in `human`) and never move to OSS or to generic `*-api` siblings. The `postgres/mysql→{api-gateway,human,scheduler-enterprise,integration,api-orchestration}` edges are a symptom of the **persistence monolith**, not of port placement.
- **The correct fix = §8 persistence split, built on the §6.3 org_id seam.** Kept (legitimate, independent): the guardrail-accuracy refinements + the redis-persistence unused `api-gateway` dep removal.

### Phase 1 §6.3 — org_id seam FOUNDATION landed ✅ (OSS branch)
- Concrete **`SqlInsertBuilder`** (`org.conductoross.conductor.persistence.query`): `INSERT … ON CONFLICT … DO UPDATE` with **composing** `column()` + `onConflict()` (enterprise adds `org_id` without clobbering the OSS natural key); mirrors the existing `SqlQueryBuilder`.
- **`PostgresBaseDAO`** gains `protected` no-op `applyQueryExtensions(SqlQueryBuilder, QueryContext)` / `applyWriteExtensions(SqlInsertBuilder, QueryContext)` + builder-aware `query()/execute()` helpers that invoke the hook immediately before render (all additive).
- **`SqlBuilderSeamTest`** proves the enterprise-override composes (4/0) with order-independent positional binds; OSS standalone has no org concept.
- **First real path migrated:** `PostgresExecutionDAO.addWorkflow` now builds via `SqlInsertBuilder` → byte-identical SQL, but runs the write hook. `PostgresExecutionDAOTest` 10/0, no regression. Published to mavenLocal.

### Phase 1 §6.3 — ALL OSS SQL adapters migrated ✅ (OSS branch) — step 1 DONE
- **Postgres adapter fully on the seam:** `PostgresExecutionDAO` (every read/write/update/delete), `PostgresMetadataDAO`, `PostgresQueueDAO`, `PostgresPollDataDAO`. EXISTS-checks rewritten as `SELECT 1 … + Query.exists()` so the hook can scope the `WHERE`; locking (`FOR UPDATE SKIP LOCKED`/`FOR SHARE`), `ORDER BY`/`LIMIT`, correlated-subquery detail queries, the CTE `popMessages`, and dynamic `IN (…)` lists all carried through. Full `postgres-persistence` suite **118/0** under Testcontainers.
- **MySQL adapter fully on the seam (dialect-aware):** `MySQLBaseDAO` got the hooks + helpers; `MySQLExecutionDAO` (incl. PollData), `MySQLMetadataDAO`, `MySQLQueueDAO` migrated. `SqlInsertBuilder` now renders MySQL `INSERT IGNORE`/`ON DUPLICATE KEY UPDATE` (default POSTGRES unchanged); `TIMESTAMPADD(...)` interval expressions carried via `columnRaw(expr, binds)`. MySQL DAO suites **8/0 + 7/0 + 10/0**.
- **Builder hardening (additive):** clause-structured `SqlQueryBuilder` (+`trailing()`); `::type`-cast-safe named-param regex; `SqlInsertBuilder.columnRaw[,+binds]`, target-less `DO NOTHING`, `Dialect`; null-safe `Query.addParameters` in both postgres+mysql `Query` utils. Core builder tests `SqlBuilderSeamTest` 9/0, `SqlQueryBuilderTest` 7/0; `conductor-core` full **826/0/7**.
- **Also fixed:** a latent spotless-clean violation set in 5 previously-backported `core` files (separate `style(core)` commit); a real latent null-bind NPE for nullable text columns (queue `payload`, `correlation_id`).
- Published `org.conductoross:conductor-{core,postgres-persistence,mysql-persistence}:3.30.2-rc1` to mavenLocal — ready for the step-2 enterprise subclasses.
- **Local-flake note:** `PostgresLockDAOTest`'s lease assertion compares host-JVM vs container wall clocks with ~ms tolerance and flakes under Docker-Desktop VM clock skew; `PostgresLockDAO` uses none of the migrated code (verified) — environmental, not a regression.

### Phase 3 — execution blueprint & findings (DECIDED: hybrid; ready to execute)
Decision (product owner): **hybrid** — `OrkesPostgresExecutionDAO extends` OSS `PostgresExecutionDAO`; override the hooks for `org_id` and delete the duplicated SQL on the identical-modulo-`org_id` paths; full-override the structurally-diverged paths. Full read of the 1474-line Orkes DAO yields this exact map:

**Refined finding (important — bigger than "override two hooks"):** the divergence runs through the **base-DAO/Query layer**, not just SQL. The Orkes DAO extends `io.orkes…dao.postgres.base.PostgresBaseDAO` and uses `io.orkes…dao.postgres.util.Query`, whose extras the diverged overrides depend on: `addParameterArray(coll,"text")` (binds a Postgres `text[]` for `index_data`), `updateWithTransaction(...)`, partition-aware serialization (`workflowWriter`/`skipTasks` mixin), `addParameterArray`. So "extends the OSS DAO" must reconcile that layer. Reconciliation:
- `updateWithTransaction(sql, fn)` ≡ inherited `queryWithTransaction(sql, q -> q.executeUpdate())` — no OSS change, just use the inherited helper.
- `addParameterArray(Collection, sqlType)` is **generic** (typed SQL array bind) → **backport to OSS `com.netflix.conductor.postgres.util.Query`** (feature-agnostic, additive). Then the inherited `query()/execute()` helpers hand the overrides a `Query` that can bind arrays.
- Custom workflow serialization (`skipTasks` filter) stays in the Orkes subclass (enterprise).

**OSS enablers (additive, keep OSS green):** make these `PostgresExecutionDAO` helpers `protected` so the Orkes subclass composes them in its overridden `createTasks`/`removeTask` instead of re-implementing: `addWorkflowToTaskMapping`, `removeWorkflowToTaskMapping`, `addScheduledTask`, `removeScheduledTask`, `addTaskInProgress`, `removeTaskInProgress`, `updateInProgressStatus`. Plus the `Query.addParameterArray` backport above.

**Inherit as-is (delete the Orkes override) — org_id supplied by the hook:**
- `getInProgressTaskCount` (task_in_progress + `org_id`), `exceedsLimit` (uses the inherited `getInProgressTaskCount` + private `findAllTasksInProgressInOrderOfArrival`, both task_in_progress + `org_id`), `getTasks(name,startKey,count)` (pure logic), `getPendingWorkflowsByType` (delegates to overridden `getRunningWorkflowIds`/`getWorkflow` via virtual dispatch), `canSearchAcrossWorkflows`.

**Reuse the inherited `protected` helpers (delete the Orkes SQL) inside the overridden `createTasks`/`removeTask`:** the **mapping/in-progress/scheduled** tables are byte-for-byte OSS + `org_id`: `workflow_to_task` (`ON CONFLICT (org_id, workflow_id, task_id)`), `task_scheduled` (`ON CONFLICT (org_id, workflow_id, task_key)`), `task_in_progress` (`(org_id, task_def_name, task_id, workflow_id)`) — the `applyWriteExtensions` (`column("org_id") + onConflict("org_id")`) / `applyQueryExtensions` (`and("org_id = :orgId")`) hooks reproduce them exactly.

**Full overrides stay (genuine divergence):** all **workflow** storage (date-partition tables, `json_data::text`, extra columns `workflow_name/status/idempotency_key/index_data/created_by`, `to_json(?::json)`, status-column pending vs `workflow_pending`): `addWorkflow`/`updateWorkflow`/`removeWorkflow`/`readWorkflow`/`getWorkflow`/`getRunningWorkflowIds`/`getPendingWorkflowCount`/`getWorkflowsByType`/`insertOrUpdateWorkflow`; all **task** storage (date-shard tables via `PartitionManager`+`resolveTaskShard`, `hasTaskShardOrgId` probe, `json_data::text`, `index_data`, `ON CONFLICT … WHERE modified_on` upsert): `getTask`/`getTasks(ids)`/`getTasksForWorkflow`/`getPendingTasks*`/`insertOrUpdateTaskData`/`removeTaskData`/`batchFetchTasksByShard`/`createTasks`/`updateTask`; **rate-limit** (`try_acquire_rate_limit` fn); **event executions** (`ExtendedEventExecution`, `org_id` from the entity not the request context); and the enterprise-only `OrkesExecutionDAO` methods (`getTaskIdsForWorkflow`, `addEventTask`/`removeEventTask`/`getEventTasks`, `getEventExecutions(orgId,…)`, `getEventMessages`/`addEventMessage`/`updateEventMessage`, `restoreWorkflow`, `getWorkflowWithTasks`, `writesToArchiveShards`, `removeWorkflowPayload/TaskPayload`, `removeWorkflowWithExpiry`).

**org_id hook source:** the request-scoped paths read `OrkesRequestContext.get().getOrgId()` — the enterprise `applyQueryExtensions`/`applyWriteExtensions` overrides read the same, so inherited mapping/in-progress/scheduled writes are org-scoped identically. (Event-execution paths use the entity's `org_id` and remain explicit overrides.)

**Wiring:** Orkes `postgres-persistence` must add `implementation "org.conductoross:conductor-postgres-persistence:${revConductor}"`; reconcile constructors (OSS `(RetryTemplate, ObjectMapper, DataSource, QueueDAO)` vs Orkes `(RetryTemplate, DataSource, ConductorLimitsDAO, QueueDAO, PartitionManager, cacheExecutions)` — call `super(...)` with the OSS-required args, keep the enterprise fields). **Acceptance:** Orkes `postgres-persistence` suite green (was 435/0) with the mapping/in-progress/scheduled SQL deleted and org_id applied via the seam; OSS suite unchanged. **Net dedup for ExecutionDAO is partial by design** — the sharded/partitioned workflow+task storage is a real enterprise architecture and stays overridden; the clean wins are the mapping/in-progress/scheduled helpers + 5 inherited methods.

> The same `extends`-OSS + override-hooks pattern then repeats for the smaller Orkes stores that are closer to OSS (e.g. metadata/queue/polldata if present enterprise-side), where the dedup ratio is much higher.

### Phase 3 — attempt 1: code integration PROVEN, three build/semantic blockers surfaced
The OSS-side enablers are landed + published (additive, OSS green): `Query.addParameterArray` (generic typed array bind), `getWithRetriedTransactions` widened to `protected`, the 7 mapping/in-progress/scheduled helpers made `protected`, and **`SqlInsertBuilder.extendConflictTarget`** (append `org_id` to an upsert's natural key only when an explicit conflict target already exists — so a plain `task_in_progress` insert is NOT turned into a bogus `ON CONFLICT (org_id)`; `SqlBuilderSeamTest` 10/0). The enterprise hook is therefore: `applyQueryExtensions` → `query.and("org_id = :orgId")`; `applyWriteExtensions` → `insert.column("org_id", …).extendConflictTarget("org_id")`.

**Pass A (reparent + reconcile, all overrides kept) COMPILES cleanly** — `OrkesPostgresExecutionDAO extends com.netflix.conductor.postgres.dao.PostgresExecutionDAO`, `super(retryTemplate, objectMapper, dataSource, queueDAO)`, `updateWithTransaction`→inherited `queryWithTransaction`, hooks added: `compileJava` + `compileTestJava` 0 errors. The DAO test then runs **11/13 pass** (2 pre-skipped). So the *code-level* reparenting is sound. Three integration blockers remain, each needing deliberate handling (NOT a code bug):

1. **Flyway version conflict (worked around).** OSS `conductor-postgres-persistence` depends on flyway **10.15.2** (`flyway-core` + `flyway-database-postgresql`); Orkes force-pins all `org.flywaydb` to **9.0.4** (where `flyway-database-postgresql` doesn't exist as a module → unresolvable). Workaround used: `exclude group: 'org.flywaydb'` on the OSS dep (Orkes keeps its own `flyway-core` 9.0.4; the OSS *DAO* classes don't use flyway). **Real fix:** align flyway (bump Orkes to 10.x, or pin a shared flyway across the composite) — Orkes already half-hit this via the OSS `mysql-persistence` transitive (`flyway-mysql`).
2. **OSS persistence autoconfig is component-scanned (server-boot blocker).** Orkes `@ComponentScan(basePackages = {"com.netflix.conductor", …})` would pick up OSS `com.netflix.conductor.postgres.config.PostgresConfiguration` and create OSS `DataSource`/DAO/Flyway beans that conflict with Orkes's bespoke postgres setup. (Not hit by the `AbstractTestDAO`-based DAO test — it constructs the DAO directly with no Spring scan — but it blocks full server boot.) **Fix:** exclude the OSS postgres config from `OrkesConductorApplication.excludeFilters` (same playbook already used for other OSS beans).
3. **Base-DAO transaction-wrapper exception semantics (needs a decision).** Orkes `io.orkes…dao.postgres.base.PostgresBaseDAO` wraps in-transaction failures in `ApplicationException` (mapped to HTTP codes by `ApplicationExceptionMapper`); OSS `PostgresBaseDAO` wraps in `NonTransientException extends RuntimeException` (unrelated type). Reparenting therefore changes the exception type for every overridden method's in-transaction failure — breaking `testCreateTaskException`/`2` (which `assertThrows(ApplicationException.class)` on `createTasks` validation) and shifting the REST error contract. **Decision needed:** (a) adopt OSS exception semantics (update the Orkes mapper + tests), or (b) the enterprise subclass re-wraps (e.g. validate-and-throw `ApplicationException` outside the transaction; or a thin protected `wrap()` seam in OSS the subclass overrides).

### Phase 3 — DAO vertical COMPLETE ✅ (org_id seam proven end-to-end on a real enterprise store)
All three blockers resolved (decision on #3: **adopt OSS exception semantics + teach the mapper**) and the dedup landed + verified:
- **Pass A (reparent)** — `OrkesPostgresExecutionDAO extends com.netflix.conductor.postgres.dao.PostgresExecutionDAO`, `super(…, queueDAO)`, `updateWithTransaction`→inherited `queryWithTransaction`, OSS `Query` (incl. `addParameterArray`), and the org_id hooks (`applyQueryExtensions` → `org_id = :orgId`; `applyWriteExtensions` → `column("org_id", …).extendConflictTarget("org_id")`). `#1` worked around (`exclude group: 'org.flywaydb'`). `#3` adopted: in-transaction failures surface as OSS `NonTransientException`; `testCreateTaskException/2` updated.
- **Pass B (dedup, −261 lines)** — deleted the 7 mapping/in-progress/scheduled helpers (`add/removeWorkflowToTaskMapping`, `add/removeScheduledTask`, `add/removeTaskInProgress`, `updateInProgressStatus`) + 5 inherited methods (`getInProgressTaskCount`, `exceedsLimit` [+orphaned `findAll…`], `getTasks(name,startKey,count)`, `getPendingWorkflowsByType`, `canSearchAcrossWorkflows`); call sites retargeted to the inherited no-`orgId` helpers, org_id supplied by the hook. The sharded/partition workflow+task storage, rate-limit, event-execution and enterprise-only methods stay full overrides (genuine divergence).
- **`#2` (server boot)** — `OrkesConductorApplication.excludeFilters` excludes OSS `PostgresConfiguration` (mirrors the existing `MySQLConfiguration` exclusion, already proven for the OSS mysql artifact); `postgres-persistence` is now `java-library` + `api` so the server sees the OSS superclass; `ApplicationExceptionMapper` maps `NonTransientException`→500.
- **Verified:** `PostgresExecutionDAOTest` **15 (13 pass / 2 skipped) / 0 failures** under Testcontainers (org_id applied through the seam against the real Orkes tables, duplicated SQL gone); whole Orkes project `compileJava` + `compileTestJava` **0 errors**; OSS suites unchanged. Full server-boot context-load is CI-scale (container-per-`@SpringBootTest`) — the `excludeFilter` follows the proven mysql pattern.

This is the first enterprise store fully reparented onto an OSS adapter with `org_id` via the seam — proving the §8/§14.1 hardest path.

### Phase 4(a) — engine-DAO reparent: PollData collapsed ✅; Queue/Metadata are partial-only
Repeated the `extends`-OSS pattern across the engine DAOs and assessed each:
- **`PostgresPollDataDAO` ✅ COLLAPSED (−171 lines, 266→95).** Structurally identical to OSS + `org_id` (no sharding): a thin subclass — constructor (adapts Orkes `PostgresProperties`→OSS), the two `org_id` hooks, and the cross-org `getAllPollData` admin override (intentionally un-scoped); all `poll_data` CRUD + read/write cache + flush lifecycle inherited. `PostgresPollDataDAOCacheTest` 2/0 + `NoCacheTest` 6/0.
- **`PostgresQueueDAO` — poor reparent candidate (skipped).** Diverges from OSS beyond `org_id`: `priority ASC` vs OSS `DESC` (different priority semantics), `popMessages` re-stamps `deliver_on` (visibility-timeout-on-pop), an org-keyed `queueCache`, millisecond (not second) intervals, `getSize` is cross-org, and extra enterprise methods (`push(…Duration)`, `get`, `setQueueUnackTime`). Reparenting would be ~90% full overrides for a ~3-method dedup — net-negative. Stays standalone.
- **`AbstractMetadataDAO`/`OrkesPgMetadataDAO` — partial-only.** Basic `meta_task_def`/`meta_workflow_def` CRUD is OSS + `org_id`, but the **upsert strategy diverges** (Orkes `INSERT … ON CONFLICT (org_id,name) DO UPDATE` vs OSS update-then-insert) and it carries enterprise RBAC-aware queries (`getWorkflowDefs(name,subjects,accesses)`, resource-sharing joins) via `OrkesMetadataDAO`. A reparent would dedup a little and override a lot — defer.

**Conclusion:** the clean OSS-adapter *collapse* wins are PollData (done) + ExecutionDAO's mapping/in-progress/scheduled subset (done). Queue/Metadata and the enterprise feature stores diverge enough that the **remaining persistence-convergence value is Phase 4 (the monolith split / 13 sideways edges → zero)**, not more reparenting.

### Phase 4 — monolith split: base modules + gateway & human features done ✅ (13 → 9 sideways edges)
The recipe (proven on two features, both DBs):
1. **Foundation base modules extracted** (one-time, done): `orkes-conductor-postgres-base` (`PostgresBaseDAO` + `Query` + functional interfaces) and `orkes-conductor-mysql-base` (`OrkesBaseDAO`, which extends the OSS `MySQLBaseDAO`). Same packages → zero import churn; added to the `moduleDependencyReport` `foundation` set; exposed via `api` (incl. spring-retry — `RetryTemplate` is in the base-DAO ctor). The persistence monoliths now expose `*-base` via `api` so existing consumers still see `PostgresBaseDAO`/`Query`.
2. **Per-feature store move:** for each feature edge, move the Postgres + MySQL store(s) (and any feature-only helper, e.g. gateway's `OrgFilterHelper`) from the persistence monolith into the **feature module**; the feature module gains an `implementation project(':orkes-conductor-{postgres,mysql}-base')` dep; drop the monolith's `implementation project(':orkes-conductor-<feature>')` production edge. SQL unchanged — pure relocation. The feature's DAO test stays in the persistence module via a **test-scope** dep on the feature (test edges aren't violations).

Done (13 → 5 sideways edges):
- **gateway ✅** — `Postgres/MySQLGatewayConfigDAO` (+ `OrgFilterHelper`) → `api-gateway`; both `→ api-gateway` edges gone (13→11). `PostgresGatewayConfigDAOTest` 26/0, `MySQLGatewayConfigDAOTest` 11/0.
- **human ✅** — `Postgres/MySQLHumanTaskDAO` + `*TemplateDAO` → `human` (uses `io.orkes.conductor.model.*` locally); both `→ human` edges gone (11→9). `MySQLHumanTaskTemplateDAOTest` 10/0.
- **registry ✅** — `Postgres/MySQLServiceRegistryDAO` + `*ProtoRegistryDAO` → `api-orchestration`; both `→ api-orchestration` edges gone (9→7). `PostgresServiceRegistryDAOTest` 12/0.
- **integration ✅ (unused-dep removal)** — the integration DAOs implement ports that live in **core**, not the integration module, so the monoliths' `→ integration` production edge was an UNUSED declared dependency. Reclassified to `testImplementation` (the test infra references integration); no store move (7→5). A finding worth noting: not every "monolith → feature" edge is a real store coupling — some are stale unused deps.
- Whole Orkes project `compileJava` + `compileTestJava` 0 errors after each.

**Remaining 5:**
- **scheduler (2 edges) — entangled tests; deferred.** `Postgres/MySQLSchedulerDAO` + `PostgresSchedulerArchiveDAO` genuinely use scheduler-enterprise types (real move to `scheduler-enterprise`, which already has postgres-base-eligible needs and `api scheduler-oss`). But the scheduler tests live in **postgres-persistence** test sources (`io.orkes.conductor.scheduler.service.*`) and `scheduler-enterprise` already `testImplementation`-deps `postgres-persistence` — so the store move needs the scheduler tests relocated to `scheduler-enterprise` first to avoid a test-scope cycle. Mechanical but careful; do it as its own step.
- **3 feature-to-feature edges** (`api-gateway`→`integration`, `event-integration`→`integration`, `event-processor`→`event-integration`): separate category — likely intended collaboration; needs a product call ("ports stay with features"; `api-gateway-api` was reverted). If accepted, baseline them in the checker.

**Also still open:** `OrkesWorkflowExecutor extends WorkflowExecutorOps` (needs the sync-exec/idempotency engine-core backport).

**Also still open:** `OrkesWorkflowExecutor extends WorkflowExecutorOps` (needs the sync-exec/idempotency engine-core backport).

---

## WHAT'S LEFT (authoritative, supersedes earlier "Remaining" notes)

**Done:** engine cutover (`oss-core` deleted, Orkes on OSS `conductor-core`); OSS superset/seam backports; D3 excludeFilters retirement (21 task/mapper beans); Spring context loads; OSS validated repo-wide (~1771 tests, 13 modules); Orkes validated module-by-module; dependency guardrail accurate (**13 real production violations**); **org_id seam UNIVERSAL across every OSS postgres + mysql adapter — step 1 complete** (§16/§17), artifacts re-published to mavenLocal.

**Remaining — the persistence convergence is the bulk (§8), in strict order:**

1. ~~**Finish Phase 1 §6.3 (OSS) — migrate the rest of the OSS SQL adapters onto the builder so the hook is universal.**~~ ✅ **DONE** (see §16 "ALL OSS SQL adapters migrated" + §17). Both postgres and mysql adapters render every read/write through a builder that calls the hook; OSS suites green; artifacts re-published. (`WorkflowExecutorOps` `beforeStartWorkflow`/`onDecide` seams were already added; the remaining sync-exec/idempotency engine-core backport lands with step 2/Phase 2.)

2. **Phase 3 (Enterprise) — prove the full vertical.** Convert **one** Orkes store (postgres `ExecutionDAO`) from a standalone re-implementation to `extends` the OSS `PostgresExecutionDAO`, overriding `applyQueryExtensions`/`applyWriteExtensions` to inject `org_id` (read filter + insert column + `ON CONFLICT` target via `OrgContext`); **delete the duplicated SQL**. Re-express `OrkesWorkflowExecutor` as `extends WorkflowExecutorOps` over the §6.3.4 seams (delete duplicated engine code). Requires the Orkes module to consume the OSS `conductor-postgres-persistence` artifact. Acceptance: Orkes postgres execution e2e green with org_id applied via the seam, duplicated code gone.

3. **Phase 4 (Enterprise) — split the monoliths → the clean module graph.** For postgres/mysql/redis: (a) the engine DAOs become thin OSS subclasses (step 2); (b) the **enterprise feature stores move out of the persistence monolith to live with their feature** (`PostgresGatewayConfigDAO`→ gateway vertical, `PostgresHumanTaskDAO`→ human, `PostgresSchedulerDAO`→ scheduler, integration/registry likewise), each depending on its feature's port. This is what removes the **13 sideways edges** (`postgres/mysql→{api-gateway,human,scheduler-enterprise,integration,api-orchestration}` + the event/gateway feature edges) and yields "api-gateway is just api-gateway." Also adopt OSS `conductor-scheduler-core`/`-<db>-persistence` so `scheduler-enterprise` extends OSS scheduler-core. Acceptance: dependency checker shows **zero** sideways edges.

4. **Phase 2 (OSS) — finish the pure-engine backports** per the §7.1 disposition table (strip introspection→SPI / org_id from the remaining task/mapper deltas), shrinking the Orkes override surface further.

5. **Phase 5 — finalize enterprise modules:** consolidate `OrgContext`/`IDTools`/ID-generator duplicates into `enterprise-common`; confirm every OSS-bound path is org-free.

6. **Phase 7 / ops — CI + publishing:** run the full container-heavy server/integration suites in CI (container-per-`@SpringBootTest` is impractical locally); OSS publishing (GitHub Packages/S3); branded composite `server-enterprise`; enforce ArchUnit/dependency gates.

**Smaller known follow-ups:** the residual justified `excludeFilters` entries (Event/Wait timer-model tasks, event/scheduler infra, evaluator name-clashes, OSS re-implementations) can move to `@ConditionalOnMissingBean` seams where the OSS bean tolerates optional override (D3); each is correct as-is for now.

### §17 — OSS DAO builder-migration inventory (for step 1) — ✅ COMPLETE
Per-method migration was the long pole. **Every OSS SQL adapter is now on the builder seam** (every read/write/update/delete renders through `SqlQueryBuilder`/`SqlInsertBuilder` and invokes the no-op `applyQueryExtensions`/`applyWriteExtensions` hooks). The §14.1 risk is closed for these adapters — no raw-JDBC bypass remains.
- `PostgresExecutionDAO` ✅ (all paths; `PostgresExecutionDAOTest` 10/0, full module 118/0).
- `PostgresMetadataDAO` ✅ (9/0), `PostgresQueueDAO` ✅ (10/0), `PostgresPollDataDAO` ✅ (8/0).
- `MySQLBaseDAO` hooks/helpers ✅; `MySQLExecutionDAO` (incl. PollData) ✅ (8/0), `MySQLMetadataDAO` ✅ (7/0), `MySQLQueueDAO` ✅ (10/0).
- Builder enhancements landed (additive, feature-agnostic): `SqlQueryBuilder` restructured to head + WHERE-predicate-list + tail (hook-appended predicates land in `WHERE`, before trailing `ORDER BY`/`LIMIT`/`FOR UPDATE`/`FOR SHARE`); named-param regex skips Postgres `::type` casts; `SqlInsertBuilder` gained `columnRaw(expr[,binds])` (e.g. `CURRENT_TIMESTAMP`, interval/`TIMESTAMPADD` expressions), target-less `ON CONFLICT DO NOTHING`, and **dialect awareness** (POSTGRES `ON CONFLICT` vs MYSQL `INSERT IGNORE`/`ON DUPLICATE KEY UPDATE`); `Query.addParameters` is now null-safe (nullable `payload`/`correlation_id`). `SqlBuilderSeamTest` 9/0, `SqlQueryBuilderTest` 7/0; `org.conductoross:conductor-{core,postgres-persistence,mysql-persistence}:3.30.2-rc1` re-published to mavenLocal.
- **Patterns proven for the enterprise override (step 2):** read filter via `applyQueryExtensions` → `query.and("org_id = :orgId")`; write via `applyWriteExtensions` → `insert.column("org_id", ...)` (+ `.onConflict("org_id")` on Postgres; MySQL implies it via the unique key). `QueryContext.table()` is the discriminator for per-table org-scoping; joined/EXISTS/CTE methods that the hook can't decorate generically are explicit enterprise full-overrides.

> Known non-blocking caveat surfaced during step 1: the enterprise Orkes `ExecutionDAO` has **structurally diverged** beyond `org_id` (date/shard partition tables + `hasTableOrgId`, `json_data::text` casts, a status-column pending model vs OSS `workflow_pending`). So in step 2 the "delete the duplicated SQL" applies cleanly to the paths that are structurally identical modulo `org_id`; the diverged paths stay full overrides (or require a separate data-model convergence decision). This refines, not contradicts, §8.2.

---

*End of plan. This document is the single source of truth for the convergence effort; update it via PR as decisions in §14 are resolved.*

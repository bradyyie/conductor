# Agent Handoff — Orkes ↔ Conductor OSS Convergence

> Read this first, then `docs/convergence/ORKES-OSS-CONVERGENCE-PLAN.md` (the authoritative plan).
> This handoff is mirrored in both repos at `docs/convergence/AGENT-HANDOFF.md`.

---

## 1. TL;DR — where things stand

**Goal:** open-core split. Backport Orkes engine/perf improvements into OSS `conductor-oss/conductor` (de-enterprised, feature-agnostic), keep only enterprise features/overrides in Orkes, delete `oss-core`, and have BOTH repos pass their existing test suites.

**Engine: CONVERGED ✅.** `oss-core` is deleted; Orkes consumes the published OSS `conductor-core`; the executor/system-tasks/mappers layer onto OSS via `@Primary` / `isOverride()`. Both repos compile (main + test). OSS validated repo-wide (~1771 tests, 13 modules, 0 failures). Orkes validated module-by-module. Spring context loads.

**Persistence: NOT converged ❌ — this is the remaining mountain (plan §8).** Orkes `postgres/mysql/redis-persistence` still re-implement OSS's execution/metadata/queue concerns AND bundle every enterprise store (gateway/human/scheduler/integration/registry) = the monolith. The `org_id` seam **foundation** is now built + proven on one path; the rest is the work.

**Do not re-derive the architecture — it's settled. Read the plan, especially §4 (architecture), §5 (the OSS-scheduler reference pattern we copy), §6.3 (the seams), §8 (persistence + org_id), §14 (locked decisions D1–D5), and §16 "WHAT'S LEFT".**

---

## 2. Repos, branches, the plan

| Repo | Path | Branch |
|---|---|---|
| OSS | `~/workprojects/conductor` (`conductor-oss/conductor`) | `orkes-converge-fork-conductor` |
| Enterprise | `~/workprojects/orkes-conductor` (`orkes-io/orkes-conductor`) | `orkes-converge-fork-conductor` |

- Plan: `docs/convergence/ORKES-OSS-CONVERGENCE-PLAN.md` (mirrored in both repos; keep them in sync — edit orkes, `cp` to OSS, commit both).
- Both repos are currently **clean and committed**. Commit at every green checkpoint; keep the plan's §16 log + §17 inventory current.

---

## 3. Non-negotiable rules (internalize these — an earlier agent burned time getting them wrong)

- **P2 — OSS is feature-agnostic.** NO org/tenant/org_id/auth/RBAC/gateway concepts anywhere in OSS. OSS exposes **generic** seams; the enterprise layer decides *why*.
- **Placement rule (confirmed by the product owner):**
  - *Generic engine capability* → **OSS** (`org.conductoross.conductor.*` for new code; pre-existing Netflix stays `com.netflix.conductor.*`).
  - *Enterprise feature* → **Orkes** (`io.orkes.conductor.*`), and it **belongs with its feature**. Enterprise DAO ports (`GatewayConfigDAO`→`api-gateway`, `HumanTaskDAO`→`human`, `EnterpriseSchedulerDAO`→`scheduler-enterprise`) **never move to OSS and never get yanked into sibling `*-api` modules.** (An earlier agent created `api-gateway-api` and had to revert it — don't repeat that.)
- **Anti-divergence mechanism = the OSS seam left OPEN for override**, not port extraction: `super()` + `applyQueryExtensions`/`applyWriteExtensions`, `@ConditionalOnMissingBean`/`@Primary`, the `isOverride()` task/mapper precedence seam, the no-op `WorkflowIntrospection` SPI.
- **P3 — extension by inheritance:** enterprise extends OSS concrete classes, calls `super()` first. OSS classes stay non-final with `protected` hooks.
- **OSS must NEVER be left broken.** Its suite (conductor-core 815, +reference modules) must stay green after every OSS change. Additive only; never change an OSS signature that breaks OSS impls/tests.

---

## 4. The immediate next task (persistence convergence, plan §16 "WHAT'S LEFT" + §17)

The `org_id` seam foundation is in OSS and proven on `PostgresExecutionDAO.addWorkflow`. Two viable next moves (pick per product owner):

**(A) Keep grinding Phase 1 §6.3** — migrate the remaining OSS `PostgresExecutionDAO` methods onto the builder so the hook is universal (then `MetadataDAO`/`QueueDAO`, then mysql). Track in §17. Mechanical but careful: JSON columns use `addJsonParameter` = `addParameter(toJson(x))`, so pre-serialize with the base DAO's `toJson(...)` into `SqlInsertBuilder.column(...)`. Verify each batch with `PostgresExecutionDAOTest` (no regression — the builder must render byte-identical SQL/binds for OSS).

**(B) Prove the full vertical first (Phase 3)** — convert one Orkes store (`io.orkes.conductor.dao.postgres.execution.PostgresExecutionDAO`) to `extends` the OSS `com.netflix.conductor.postgres.dao.PostgresExecutionDAO`, override `applyQueryExtensions`/`applyWriteExtensions` to inject `org_id` (read filter + insert column + `ON CONFLICT` target from `OrgContext`), and **delete the duplicated SQL**. This requires Orkes `postgres-persistence` to depend on the OSS `conductor-postgres-persistence` artifact (it currently doesn't). Shows the end-state ("duplicated code gone, org_id via override") before scaling out.

Then **Phase 4**: split the monoliths — move `PostgresGatewayConfigDAO`→gateway vertical, `PostgresHumanTaskDAO`→human, etc. → the **13 sideways dependency violations** go to zero → the clean module graph the owner wants ("api-gateway is just api-gateway").

The seam scaffolding (read this code first):
- OSS `core/.../org/conductoross/conductor/persistence/query/`: `SqlQueryBuilder`, `SqlInsertBuilder`, `QueryContext`.
- OSS `postgres-persistence/.../postgres/dao/PostgresBaseDAO.java`: the `applyQueryExtensions`/`applyWriteExtensions` hooks + builder-aware `query()`/`execute()` helpers.
- OSS `core/src/test/.../persistence/query/SqlBuilderSeamTest.java`: proves the override composes — model new tests on this.

---

## 5. Build / test loops (use these exact patterns)

**OSS → publish → Orkes (dev loop):**
```bash
# after editing OSS:
cd ~/workprojects/conductor && ./gradlew :conductor-core:publishToMavenLocal -x test -x javadoc --console=plain
# (publish the specific module you changed, e.g. :conductor-postgres-persistence)
cd ~/workprojects/orkes-conductor && ./gradlew :orkes-conductor-server:compileJava --console=plain --continue > /tmp/sv.txt 2>&1
```
- Redirect with `> file 2>&1` (NOT `2>&1 > file`).
- Count errors: `grep -cE '^/.*\.java:[0-9]+: error:' /tmp/sv.txt`; group by file: `grep -E '\.java:[0-9]+: error:' /tmp/sv.txt | grep -oE '/[^:]+\.java' | sort | uniq -c | sort -rn`.

**Container tests on this machine (Docker Desktop, `/var/run/docker.sock` is MISSING):**
```bash
cp ~/.testcontainers.properties /tmp/tc.props.bak
printf 'docker.host=unix:///Users/dbrady/.docker/run/docker.sock\n' > ~/.testcontainers.properties
cd <repo> && ./gradlew --stop
TESTCONTAINERS_RYUK_DISABLED=true DOCKER_HOST=unix:///Users/dbrady/.docker/run/docker.sock \
  ./gradlew --no-daemon :<module>:test --tests '<FQCN>' --max-workers=2 --console=plain
docker ps -q | xargs -r docker kill   # cleanup (Ryuk disabled → containers don't auto-reap)
cp /tmp/tc.props.bak ~/.testcontainers.properties  # ALWAYS restore the user's global file
```
- The pinned `UnixSocketClientProviderStrategy` ignores `DOCKER_HOST`; the `docker.host` property is what makes it connect. Ryuk can't bind-mount the Desktop socket → disable it. `--no-daemon` so env propagates.
- **Each `@SpringBootTest`/DAO test spins its OWN Postgres/MySQL container** → the full server/mysql suites are ~minutes/class and exhaust Docker locally. Run affected classes or constrain `--max-workers`. The full container suites are a CI job, not a local one.
- Parse results: glob `*/build/test-results/test/*.xml`, read `tests/failures/errors/skipped` attrs.

**Shell gotchas (zsh + BSD):** bare `grep --include=*.java` fails ("no matches found") — use the dedicated grep tool, `rg`, or `find … -exec`. BSD `sed` lacks `\b` — use `perl -i -pe`. Per-file "error" counts are inflated by warning lines — count only lines matching `: error:`.

---

## 6. Key facts a new agent will need (so you don't rediscover them)

- **Enterprise interfaces that can't live on OSS:** `io.orkes.conductor.dao.OrkesExecutionDAO extends ExecutionDAO` (EventMessage/ExtendedEventExecution), `OrkesMetadataDAO extends MetadataDAO` (org/permission lookups). Test stubs implement these.
- **D3 override seam is live:** `SystemTaskRegistry.byType` + `getTaskMappers`/`preferOverridingMapper` (both `isOverride()`-based, unit-tested in OSS `SystemTaskRegistryTest`). 21 Orkes task/mapper overrides use distinct bean names + `isOverride()=true` instead of excludeFilters. Remaining excludeFilters entries are justified (Event/Wait timer-model tasks, event/scheduler infra, evaluator name-clashes, OSS re-implementations).
- **Bean-clash playbook** (when an OSS bean collides with an Orkes one at context load): same default bean name → exclude OSS in `OrkesConductorApplication.excludeFilters` or rename + `isOverride()`; same TYPE with stub default (`matchIfMissing=true`) → `@Primary` the Orkes bean (e.g. `OrkesCDCEventPublisher`).
- **Config gotchas:** `conductor.app.wait-task-async` (OSS `Wait` async-ness, default true; Orkes test uses `new Wait(false)`); `conductor.app.sweeper.enabled=false` in workers (no orchestrator sweeper); Orkes `revGraalVM=25.0.2` (matches OSS; protobuf stays 3.x); `revConductor=3.30.2-rc1`, `revConductorAi` pinned `3.30.0.rc8`.
- **Exception hierarchy:** OSS `NotFoundException extends RuntimeException` (no `Code`, not caught by Orkes `ApplicationExceptionMapper`). Orkes not-found paths throw `com.netflix.conductor.core.exception.ApplicationException(Code.NOT_FOUND,…)` for 404.
- **`moduleDependencyReport`** (Gradle task in orkes root `build.gradle`) prints production vs test edges + sideways violations; baseline at `docs/convergence/baseline-module-dependencies.txt`. Currently **13 real production violations** — all symptoms of the persistence monolith; they go to zero when Phase 4 splits it.

---

## 7. Working style that fits this owner

- They know this codebase deeply and will course-correct. When unsure about architecture/placement, **re-read the plan** and confirm rather than inventing structure.
- Prefer the plan's reference pattern (§5) and seams over clever one-offs. Backport-beats-override (P5): if an Orkes class is pure engine, backport it and delete the override.
- Verify every step (compile + the relevant tests), commit at green, keep the plan's §16/§17 honest. Don't claim "done" without running it.

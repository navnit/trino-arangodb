# Schema-cache TTL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `ArangoMetadata`'s resolved-schema cache a configurable TTL (`arangodb.schema.cache-ttl`, default `5m`), closing the M1-deferred shortcut against master spec §4.3.

**Architecture:** Replace the unbounded `ConcurrentHashMap` memoization with a Guava `Cache` using `expireAfterWrite(ttl)`, built from a new airlift `Duration` config knob. A `Ticker` seam on a package-private constructor makes expiry deterministically testable without `Thread.sleep`. `resolve()` keeps single-flight loading (`Cache.get(key, loader)`) and unwraps Guava's `ExecutionException`/`UncheckedExecutionException` so the existing error translation still sees the raw loader exception.

**Tech Stack:** Java 24, Guava 33.3.1 (`com.google.common.cache`), airlift `configuration`/`units` 1.10 (`io.airlift.units.Duration`), JUnit 5. No mocking framework — hand-written test doubles.

## Global Constraints

- **Java 24** (`maven.compiler.release=24`). Build via `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` is missing.
- **Docker must be running** for the full `mvn test` (container-backed ITs) — but every test in this plan uses hand-written doubles and needs **no** container.
- **No mocking framework.** Tests use hand-written subclasses as doubles (existing convention in `ArangoMetadataTest`).
- **Spotless is ratcheted to `origin/master`.** Editing `ArangoMetadata.java` / `ArangoConfig.java` / the two existing test files makes them "changed", so the **whole file** is subject to google-java-format **AOSP (4-space)** enforcement. After editing each file, run `mvn spotless:apply` and expect some reflow churn (import ordering, line wrapping) beyond the logical diff — this is inherent to the ratchet, not optional. Finish with `mvn spotless:check` green.
- **Checkstyle/SpotBugs** grandfather pre-existing files by path (`config/checkstyle/suppressions.xml`, `config/spotbugs/spotbugs-exclude.xml`). Prefer adding tests to the **existing** `ArangoMetadataTest`/`ArangoConfigTest` files (already suppressed) rather than new files, to avoid tripping enforcement on brand-new source.
- **Config property name and default are copied verbatim from spec §4.5:** `arangodb.schema.cache-ttl`, default `5m`.

---

### Task 1: Config knob `arangodb.schema.cache-ttl`

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Produces: `Duration ArangoConfig.getSchemaCacheTtl()` and `ArangoConfig setSchemaCacheTtl(Duration)` — consumed by Task 2's constructor.

- [ ] **Step 1: Write failing test** — extend the two `ConfigAssertions` blocks in `ArangoConfigTest`.
  In the `recordDefaults(...)` chain add:
  ```java
  .setSchemaCacheTtl(new Duration(5, MINUTES))
  ```
  In the `ImmutableMap` of overrides add:
  ```java
  .put("arangodb.schema.cache-ttl", "10m")
  ```
  and in the expected-object chain add:
  ```java
  .setSchemaCacheTtl(new Duration(10, MINUTES))
  ```
  Add imports: `import io.airlift.units.Duration;` and `import static java.util.concurrent.TimeUnit.MINUTES;`.

- [ ] **Step 2: Run test, verify it fails**
  Run: `mvn test -Dtest=ArangoConfigTest`
  Expected: FAIL (compile error — `setSchemaCacheTtl` undefined).

- [ ] **Step 3: Add the config accessor** to `ArangoConfig.java`, matching the existing `@Config` accessor style (e.g. `getShardsPerSplit`).
  Add imports:
  ```java
  import io.airlift.units.Duration;
  import io.airlift.units.MinDuration;
  import static java.util.concurrent.TimeUnit.MINUTES;
  ```
  Add a field beside the other schema settings:
  ```java
  private Duration schemaCacheTtl = new Duration(5, MINUTES);
  ```
  Add the accessor pair:
  ```java
  @NotNull
  @MinDuration("0ms")
  public Duration getSchemaCacheTtl() {
      return schemaCacheTtl;
  }

  @Config("arangodb.schema.cache-ttl")
  @ConfigDescription("How long a resolved collection schema is cached before re-sampling")
  public ArangoConfig setSchemaCacheTtl(Duration schemaCacheTtl) {
      this.schemaCacheTtl = schemaCacheTtl;
      return this;
  }
  ```

- [ ] **Step 4: Run test, verify it passes**
  Run: `mvn test -Dtest=ArangoConfigTest`
  Expected: PASS.

- [ ] **Step 5: Format + commit**
  ```bash
  source ~/.sdkman/bin/sdkman-init.sh
  mvn spotless:apply && mvn spotless:check
  git add src/main/java/io/arango/trino/ArangoConfig.java src/test/java/io/arango/trino/ArangoConfigTest.java
  git commit -m "feat(config): add arangodb.schema.cache-ttl (default 5m)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: TTL cache in `ArangoMetadata`

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java:51-60` (field + constructor) and the `resolve()` method (near line 345)
- Test: `src/test/java/io/arango/trino/ArangoMetadataTest.java`

**Interfaces:**
- Consumes: `ArangoConfig.getSchemaCacheTtl()` (Task 1); `SchemaResolver.resolveColumns(String, CollectionInfo)`; `ArangoColumn(String name, Type type, boolean hidden)`.
- Produces: package-private constructor `ArangoMetadata(ArangoClient, SchemaResolver, ArangoConfig, Ticker)` used by the test; public `@Inject` constructor signature is unchanged so Guice wiring is untouched.

- [ ] **Step 1: Write failing test** — add to `ArangoMetadataTest.java`.
  **Reuse the existing `CountingSchemaResolver` double** already in this file (`super(null, null, null)`; overrides `resolveColumns` to bump an `AtomicInteger calls` and return canned columns). The existing `columnCacheMemoizesSchemaResolutionPerTable` test already proves within-TTL single-flight via the 3-arg constructor (default 5m), so the only new behavior to cover is **expiry after the TTL**. Add one manual `Ticker` helper and one test:
  ```java
  private static final class ManualTicker extends com.google.common.base.Ticker {
      long nanos;

      @Override
      public long read() {
          return nanos;
      }

      void advance(long amount, java.util.concurrent.TimeUnit unit) {
          nanos += unit.toNanos(amount);
      }
  }

  @Test
  void schemaCacheReResolvesAfterTtl() {
      CountingSchemaResolver resolver =
              new CountingSchemaResolver(List.of(new ArangoColumn("name", VARCHAR, false)));
      ArangoConfig config = new ArangoConfig().setSchemaCacheTtl(new Duration(5, MINUTES));
      ManualTicker ticker = new ManualTicker();
      ArangoMetadata metadata = new ArangoMetadata(null, resolver, config, ticker);
      ArangoTableHandle handle =
              new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());

      metadata.getColumnHandles(null, handle);
      metadata.getColumnHandles(null, handle);
      assertThat(resolver.calls.get()).isEqualTo(1); // single-flight within TTL

      ticker.advance(6, MINUTES);
      metadata.getColumnHandles(null, handle);
      assertThat(resolver.calls.get()).isEqualTo(2); // re-resolved after TTL
  }
  ```
  Add imports if not present: `import io.airlift.units.Duration;` and `import static java.util.concurrent.TimeUnit.MINUTES;`. (`VARCHAR`, `assertThat`, `ArangoTableHandle`, `TupleDomain`, `OptionalLong`, `ArangoColumn` are already used in this file.)

- [ ] **Step 2: Run test, verify it fails**
  Run: `mvn test -Dtest=ArangoMetadataTest#schemaCacheReResolvesAfterTtl`
  Expected: FAIL (compile error — 4-arg `ArangoMetadata` constructor undefined).

- [ ] **Step 3: Implement the cache** in `ArangoMetadata.java`.
  Replace the field:
  ```java
  // M1: unbounded per-connector memoization ... (delete this comment)
  private final Map<SchemaTableName, List<ArangoColumn>> columnCache = new ConcurrentHashMap<>();
  ```
  with:
  ```java
  // Resolved-column cache with a configurable TTL (spec §4.3): one SELECT samples a
  // collection once; entries expire a fixed span after resolution so a stale schema
  // surfaces as a normal NULL on a missing field, not an error.
  private final Cache<SchemaTableName, List<ArangoColumn>> columnCache;
  ```
  Replace the single constructor with a delegating pair:
  ```java
  @Inject
  public ArangoMetadata(ArangoClient client, SchemaResolver schemaResolver, ArangoConfig config) {
      this(client, schemaResolver, config, Ticker.systemTicker());
  }

  @VisibleForTesting
  ArangoMetadata(
          ArangoClient client, SchemaResolver schemaResolver, ArangoConfig config, Ticker ticker) {
      this.client = client;
      this.schemaResolver = schemaResolver;
      this.config = config;
      this.columnCache =
              CacheBuilder.newBuilder()
                      .expireAfterWrite(config.getSchemaCacheTtl().toMillis(), MILLISECONDS)
                      .ticker(ticker)
                      .build();
  }
  ```
  Replace `resolve()` (preserve single-flight; unwrap Guava's wrapper so the raw loader exception still reaches `ArangoMetadata`'s error translation):
  ```java
  private List<ArangoColumn> resolve(ArangoTableHandle handle) {
      try {
          return columnCache.get(
                  handle.schemaTableName(),
                  () ->
                          schemaResolver.resolveColumns(
                                  handle.schema(),
                                  new CollectionInfo(handle.table(), handle.edge(), false)));
      } catch (ExecutionException | UncheckedExecutionException e) {
          Throwable cause = e.getCause();
          throwIfUnchecked(cause); // resolveColumns throws only unchecked; this rethrows it as-is
          throw new RuntimeException(cause); // unreachable, required for the compiler
      }
  }
  ```
  Update imports: remove `import java.util.concurrent.ConcurrentHashMap;`; add
  ```java
  import com.google.common.annotations.VisibleForTesting;
  import com.google.common.base.Ticker;
  import com.google.common.cache.Cache;
  import com.google.common.cache.CacheBuilder;
  import com.google.common.util.concurrent.UncheckedExecutionException;
  import java.util.concurrent.ExecutionException;
  import static com.google.common.base.Throwables.throwIfUnchecked;
  import static java.util.concurrent.TimeUnit.MILLISECONDS;
  ```
  (Keep `import java.util.Map;` — still used elsewhere in the file.)
  Also update the now-stale comment above `columnCacheMemoizesSchemaResolutionPerTable` (it says "if computeIfAbsent were replaced") — the mechanism is now `Cache.get(key, loader)`.

- [ ] **Step 4: Run test, verify it passes**
  Run: `mvn test -Dtest=ArangoMetadataTest#schemaCacheReResolvesAfterTtl`
  Expected: PASS.

- [ ] **Step 5: Run the full metadata test class** to confirm no regression to error-path tests (they depend on raw exception propagation through `resolve()`).
  Run: `mvn test -Dtest=ArangoMetadataTest`
  Expected: PASS (all methods).

- [ ] **Step 6: Format + commit**
  ```bash
  source ~/.sdkman/bin/sdkman-init.sh
  mvn spotless:apply && mvn spotless:check
  git add src/main/java/io/arango/trino/ArangoMetadata.java src/test/java/io/arango/trino/ArangoMetadataTest.java
  git commit -m "feat: TTL the ArangoMetadata schema cache (spec §4.3)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Docs

**Files:**
- Modify: `README.md` (Limitations bullet + config table)
- Modify: `CLAUDE.md` (`ArangoMetadata` description + `ArangoConfig` settings list)

- [ ] **Step 1: Update `README.md`.**
  - Remove the Limitations bullet "**Schema cache has no TTL** — resolved column metadata is memoized per table for the connector's lifetime." (or amend to note it now expires after `arangodb.schema.cache-ttl`).
  - Add a row to the config table:
    ```
    | `arangodb.schema.cache-ttl` | `5m` | How long a resolved collection schema is cached before re-sampling. |
    ```

- [ ] **Step 2: Update `CLAUDE.md`.**
  - In the `ArangoMetadata` read-path description, replace the "memoizes the result per `SchemaTableName` in an unbounded `ConcurrentHashMap` (a real TTL cache is deferred past M1)" clause with wording that it now caches per `SchemaTableName` in a Guava `Cache` with `expireAfterWrite(arangodb.schema.cache-ttl)` (default 5m).
  - Add `arangodb.schema.cache-ttl` to the enumerated `ArangoConfig` settings, noting the default `5m`.

- [ ] **Step 3: Commit** (docs are markdown — not subject to Spotless).
  ```bash
  git add README.md CLAUDE.md
  git commit -m "docs: document arangodb.schema.cache-ttl; drop no-TTL limitation

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Final verification

- [ ] **Step 1: Full static-analysis gate.**
  ```bash
  source ~/.sdkman/bin/sdkman-init.sh
  mvn spotless:check && mvn checkstyle:check && mvn compile spotbugs:check
  ```
  Expected: all green.

- [ ] **Step 2: Full test suite** (Docker running).
  Run: `mvn test`
  Expected: PASS (previous green count + 1 new `ArangoMetadataTest` method; `ArangoConfigTest` still one method).

- [ ] **Step 3:** Push branch and open PR against `master` when the user asks.

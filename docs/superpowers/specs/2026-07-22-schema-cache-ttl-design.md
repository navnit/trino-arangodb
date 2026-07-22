# Schema-cache TTL: design

**Status:** design (approved in-session 2026-07-22).
**Scope:** cleanup — close the M1-deferred schema-cache shortcut against master
spec §4.3 ("Determinism"). Not a numbered milestone; a self-contained hardening
of the resolved-column memoization in `ArangoMetadata`.

Exit: *resolved schemas expire a configurable time after resolution
(`arangodb.schema.cache-ttl`, default `5m`); within the TTL a table is sampled
at most once; a stale schema surfaces as a normal `NULL` on a missing field, not
an error.*

---

## 1. Scope

**In scope:**
- Add `arangodb.schema.cache-ttl` config (default `5m`) to `ArangoConfig`.
- Replace `ArangoMetadata`'s unbounded `ConcurrentHashMap` column cache with a
  Guava `Cache` keyed on `SchemaTableName`, expiring `expireAfterWrite(ttl)`.
- Preserve single-flight-per-table loading and exact loader-exception
  propagation through the cache.
- Deterministic expiry test via an injected `Ticker` seam.

**Out of scope (unchanged):**
- Size-bounding / `maximumSize` — spec §4.3 asks for TTL only (scope decision A,
  in-session). A pathological catalog growing the cache unbounded within one TTL
  window is accepted, matching today's behavior for that window.
- A `cache-ttl=0`-disables convention — not requested; `0ms` is a valid (already
  expired) TTL and needs no special path.
- DDL invalidation — the connector is read-only; there is no metadata mutation
  to invalidate on.
- Any change to the read path, `ArangoSplitManager`, `AqlBuilder`, or
  `SchemaResolver`.

---

## 2. Config — `ArangoConfig`

Add one setting following the existing `@Config` accessor pattern:

- Property: `arangodb.schema.cache-ttl`
- Type: `io.airlift.units.Duration` (idiomatic airlift/Trino duration binding;
  airlift `configuration` is already a dependency).
- Default: `new Duration(5, MINUTES)`.
- Validation: `@NotNull` + `@MinDuration("0ms")` (a zero TTL is legal — every
  entry is immediately expired, i.e. no effective caching).
- Getter: `getSchemaCacheTtl()`; fluent setter `setSchemaCacheTtl(Duration)`.

The default and property name come verbatim from master spec §4.5's config
table (`arangodb.schema.cache-ttl`, default `5m`).

## 3. Cache — `ArangoMetadata`

Replace:

```java
private final Map<SchemaTableName, List<ArangoColumn>> columnCache =
        new ConcurrentHashMap<>();
```

with a Guava `Cache<SchemaTableName, List<ArangoColumn>>` built once in the
constructor from the config TTL:

```java
this.columnCache = CacheBuilder.newBuilder()
        .expireAfterWrite(config.getSchemaCacheTtl().toMillis(), MILLISECONDS)
        .ticker(ticker)
        .build();
```

`expireAfterWrite` (not `expireAfterAccess`) is the correct policy: an entry
expires a fixed wall-clock span after it was resolved, independent of read
traffic, so a long-running series of queries still sees one stable schema for
the TTL window and then re-samples — exactly spec §4.3's "a query sees one
stable schema; a stale schema surfaces as a normal `NULL`."

### 3.1 `resolve()` — two invariants

`resolve()` moves from `Map.computeIfAbsent` to `Cache.get(key, loader)`:

- **Single-flight per table.** `Cache.get(K, Callable)` loads at most once per
  key under concurrency, preserving the current guarantee that one `SELECT`
  samples a collection once even with concurrent metadata calls.
- **Exact loader-exception propagation.** Guava wraps a loader-thrown exception
  in `ExecutionException` (checked) / `UncheckedExecutionException` (for a
  `RuntimeException` cause). `ArangoMetadata`'s error translation (driver error
  1228 → treat as not-found; every other `ArangoDBException` →
  `TrinoException(GENERIC_INTERNAL_ERROR, …)`) depends on the *original*
  exception reaching the caller. So `resolve()` unwraps the Guava wrapper and
  rethrows the cause — a small hand-rolled equivalent of Trino's
  `trino-cache` `uncheckedCacheGet` (that module is not on a plugin's
  `trino-spi`-only classpath, so it is inlined). `resolveColumns` throws only
  unchecked exceptions today, so the unwrap rethrows the `RuntimeException`
  cause directly and never has to wrap a checked exception.

## 4. Testing (TDD)

The cache is constructed with an injectable `com.google.common.base.Ticker`:
the production constructor passes `Ticker.systemTicker()`; a package-private
test-only constructor accepts a manual ticker. This makes expiry deterministic
with no `Thread.sleep`.

Tests (extending the existing `ArangoMetadataTest` hand-written-double style —
no container required; a counting `SchemaResolver`/`ArangoClient` double records
resolution calls):

1. **Within TTL → resolved once.** Two `getColumnHandles`/`getTableMetadata`
   calls inside the TTL window resolve the collection exactly once.
2. **After TTL → re-resolved.** Advancing the manual ticker past the TTL causes
   the next call to re-sample (resolution count increments).
3. **Config binding + default.** `arangodb.schema.cache-ttl` binds a `Duration`;
   the default is `5m` (asserted via `ConfigAssertions` in `ArangoConfigTest`).

## 5. Docs

- `README.md` Limitations: remove / amend the "Schema cache has no TTL" bullet;
  add `arangodb.schema.cache-ttl` to the config table.
- `CLAUDE.md`: update the `ArangoMetadata` description (memoization is now a
  TTL cache, not an "unbounded `ConcurrentHashMap` … a real TTL cache is
  deferred past M1") and add the config key to `ArangoConfig`'s enumerated
  settings.

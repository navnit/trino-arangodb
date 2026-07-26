# M6-B — AQL passthrough `query()` table function: design

**Status:** design (approved in-session 2026-07-26).
**Milestone:** M6, slice B. Master spec §7. Exit criterion inherited from the M6 row: *graph reachable via `query()`*.

Master spec §7 fixes the *shape* of the feature — a Mongo/JDBC-style polymorphic table function, single-split execution, a read-only check, a kill switch. Two of its mechanisms do not survive contact with a live server, and correcting them is the substance of this design:

- §7 proposes validating read-only-ness by walking the **parse AST** for data-modification nodes. The driver's `AqlParseEntity.getCollections()` returns bare names with no access mode, so an AST walk is a *denylist* over undocumented node-type strings — structurally the same weakness as the keyword scan §7 rejects, one level deeper. The explain plan carries a per-collection access mode, which makes the same check an *allowlist* (§3).
- §7 derives the result schema by wrapping the user's query: `FOR r IN (<query>) LIMIT @k RETURN r`. **This is a syntax error for cluster graph traversals** — the motivating use case — because `WITH` is legal only at the top of a query (§4, measured).

---

## 1. Scope

**In scope:**
- `arango.system.query(database, query)` as `ArangoQueryFunction`, a polymorphic table function returning `GENERIC_TABLE`.
- `AqlReadOnlyGate` — the explain-plan allowlist that is the one place M6-B's safety invariant lives.
- Result-schema derivation from the first result batch, reusing `TypeMapper.merge`.
- `ArangoQueryHandle` — a **separate** `ConnectorTableHandle` type, not a field on `ArangoTableHandle`.
- Single-split execution; explicit declines on all four pushdown hooks.
- New config `arangodb.query-function-enabled` (default `true`).
- Permanent pinning of the explain-plan access-mode semantics in `AqlPassthroughAssumptionsTest`.

**Out of scope (recorded decisions, not deferred bugs):**
- The explicit `columns => DESCRIPTOR(...)` argument proposed in §7. Its stated justification was avoiding the double execution caused by the subquery wrapper; §4 replaces the wrapper, and the remaining benefit — overriding a wrongly-inferred type — is speculative. `DescriptorArgumentSpecification` remains available in `trino-spi` for a follow-up.
- Bind-parameter arguments. The PTF takes a query string only, matching the Mongo/JDBC precedent.
- Any pushdown *into* a passthrough query (§6).
- Writes; multi-split fan-out of a passthrough; `getTableStatistics` (M6-A); schema sources (M6-C).

---

## 2. Relationship to the precedent

The Trino **MongoDB connector's `query()` is not a raw-query passthrough** (verified against tag 483, `io.trino.plugin.mongodb.ptf.Query`). It takes `(database, collection, filter)` and reuses the *existing* table's resolved schema via `metadata.getTableSchema(...)`. It therefore never derives a schema, and the hardest part of this design has no precedent to copy.

What does transfer, verbatim:

| Element | Mongo `ptf.Query` | Here |
|---|---|---|
| Binding | `newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class)` | same, conditional on the config flag (§7) |
| Naming | `SCHEMA_NAME = "system"`, `NAME = "query"` | same → `arango.system.query` |
| Return spec | `GENERIC_TABLE` | same |
| Handle | `QueryFunctionHandle implements ConnectorTableFunctionHandle`, wrapping a `ConnectorTableHandle` | same, wrapping `ArangoQueryHandle` |
| `applyTableFunction` | `instanceof` check → `Optional.empty()` for foreign handles → `TableFunctionApplicationResult` | same |

Base-JDBC's `ptf.Query` is the reference for *raw*-query passthrough plumbing, but its read-only story is unrelated: it delegates to the remote SQL engine's own permissions.

---

## 3. The read-only gate — an allowlist, not a denylist

> **A passthrough query is admitted only if every collection in its execution plan is accessed `read`.**

Measured against ArangoDB 3.12.4 (probe provenance in Appendix B), `POST /_api/explain` returns `plan.collections[]` with a per-entry `type`:

| Query | `plan.collections[]` | write nodes in `plan.nodes[]` |
|---|---|---|
| `FOR d IN users RETURN d` | `users:read` | — |
| `RETURN 1..10` | *(empty)* | — |
| `FOR v IN 1..1 OUTBOUND "users/a" follows RETURN v` | `follows:read` | — |
| `FOR v IN 1..1 OUTBOUND "users/a" GRAPH "social" RETURN v` | `follows:read`, `users:read` | — |
| `FOR d IN users FILTER d.name == "INSERT INTO" RETURN d` | `users:read` | — |
| `INSERT {x:1} INTO users` | `users:**write**` | `InsertNode` |
| `FOR d IN users UPDATE d WITH {x:1} IN users` | `users:**write**` | `UpdateNode` |
| `FOR d IN users REMOVE d IN users` | `users:**write**` | `RemoveNode` |
| `FOR d IN users REPLACE d WITH {y:2} IN users` | `users:**write**` | `ReplaceNode` |
| `UPSERT {_key:"a"} INSERT {...} UPDATE {...} IN users` | `users:**write**` | `UpsertNode` |
| `FOR d IN users LET x = (INSERT {q:1} INTO users RETURN NEW) RETURN x` | `users:**write**` | `InsertNode` |
| `FOR d IN users INSERT {c: d.name} INTO follows` | `follows:**write**`, `users:read` | `InsertNode` |

Two properties make this sound rather than heuristic:

1. **It is closed by construction.** ArangoDB must know a query's write collections up front in order to take locks, so a query cannot mutate a collection it has not declared. There is no enumeration to keep in sync as AQL grows new constructs — the contrast with an AST/keyword denylist, where a missed construct silently opens the gate.
2. **It has no false positives from text.** A string literal containing `INSERT INTO` plans as `read` (row 5), which is exactly the failure mode §7 cites against keyword scanning.

The gate reads `plan.collections[]` only. Write **node** types are recorded above as corroboration and are *not* part of the check — checking them would reintroduce the denylist.

**Defence in depth.** This gate is a guard rail, not the primary control. The deployment guidance remains a read-only ArangoDB user, and §7's kill switch removes the surface entirely.

### 3.0 Ordering invariant

> **The gate runs to completion before anything executes the user's query.**

This is a correctness requirement, not a convenience. `firstBatch` (§4) executes the query *for real* — if the two steps were transposed, a query the gate is about to reject would already have written. `analyze()` must therefore call `explainQuery` → `AqlReadOnlyGate.check` → and only on a clean verdict `firstBatch`. The e2e test in §11 asserts this by checking that a rejected `INSERT` left the target collection's count unchanged, which is the assertion that fails if the ordering ever inverts; asserting only that the error was raised would not catch it.

### 3.1 Undeclared bind parameters

Explain **rejects** a query with an undeclared bind parameter (HTTP 400, `no value specified for declared bind parameter 'minAge'`). Since the PTF accepts no bind values, such a query could never execute either. It is therefore a normal, tested rejection path (§8), not a gate limitation.

---

## 4. Schema derivation — first batch, not a subquery wrapper

§7's `FOR r IN (<user query>) LIMIT @k RETURN r` fails on the case that motivates the whole feature. Measured:

```
FAIL  FOR r IN (WITH users FOR v IN 1..1 OUTBOUND "users/a" follows RETURN v) LIMIT 5 RETURN r
      -> syntax error, unexpected WITH keyword near 'WITH users FOR v IN 1..1 ...' at position 1:11
OK    WITH users FOR v IN 1..1 OUTBOUND "users/a" follows RETURN v          [same query, unwrapped]
FAIL  FOR r IN (FOR d IN users RETURN d;) LIMIT 5 RETURN r                  [trailing semicolon]
```

`WITH` is legal only as a query prologue, and cluster graph traversals require it. String-level composition treats the user's text as an *expression*, but an AQL query is a *statement* with prologue rules — so the wrapper is not fixable by escaping.

**Instead:** execute the user's query verbatim with `batchSize = k`, read the first batch, then dispose the cursor. Measured to work for every case including the one the wrapper cannot express:

```
plain read       201  [{"_key":"a",...}]
WITH traversal   201  [{"_key":"b",...}]     <- unrepresentable under the wrapper
non-object rows  201  ["ann","bob"]
empty result     201  []
```

**Honest accounting of cost.** This does *not* eliminate the double execution §7 concedes. `analyze()` runs on the coordinator at planning time; the page source runs on a worker at execution time; a cursor cannot be handed between them, so the query runs twice. What the change buys is (a) correctness on `WITH`, and (b) a planning-time run that pulls `k` rows instead of materializing the entire result — though a query with a blocking operator (`SORT`, `COLLECT`) computes its full result server-side regardless.

**Cursor disposal is mandatory,** not hygiene: a cursor left open holds server resources for its TTL. `firstBatch` disposes via `DELETE /_api/cursor/<id>` in a `finally`.

### 4.1 Rules

| Case | Behaviour |
|---|---|
| Rows are objects | Field union across the batch, per-field type via `TypeMapper.merge` — identical to `SchemaResolver`, so the same data infers the same types whether scanned or passed through |
| Rows are non-objects (`RETURN d.name` → `"ann"`) | **Reject** — `INVALID_FUNCTION_ARGUMENT`, message directing the user to `RETURN {name: d.name}`. A Trino table needs named columns and there is no defensible synthetic name |
| Empty result set | **Reject** — a zero-column table is not representable in Trino. Recorded limitation (§10): a legitimately-empty traversal cannot be planned |
| `_key`/`_id`/`_rev` present | Ordinary **visible** columns. The hidden-system-attribute rule in `SchemaResolver` is a property of a *collection* table; a passthrough result has no collection identity |
| Execution rows diverge in type from the derivation batch | Falls through to `ValueMaterializer`: `lenient` → `NULL`, `strict` → `ARANGODB_TYPE_CONVERSION_ERROR`. Pre-existing behaviour, documented here rather than newly specified |

---

## 5. Handle modelling — separate type

`ArangoQueryHandle` is a new `ConnectorTableHandle` record `(String database, String query, List<ArangoColumnHandle> columns)`, Jackson-serializable like the other handles.

The rejected alternative was an `Optional<ArangoPassthrough>` field on `ArangoTableHandle`, following M5's `ArangoAggregation` precedent. Two reasons against it:

1. The aggregation field touched 42 sites. A second optional field compounds that, and every *combination* — passthrough + aggregation, passthrough + pushed `TupleDomain`, passthrough + limit — becomes a state that must be proven impossible by test.
2. More decisively: with a shared handle, each of `applyFilter`/`applyLimit`/`applyProjection`/`applyAggregation` must *remember* to decline a passthrough, and forgetting means `AqlBuilder` appends a `FILTER` to opaque user AQL — silently wrong results. With a separate type those states are unrepresentable.

Cost accepted: `instanceof` dispatch at four SPI entry points, matching what `MongoMetadata.applyTableFunction` already does.

### 5.1 The query travels on the handle, not the split

`ConnectorPageSourceProvider.createPageSource` receives the **table handle** alongside the split, so `ArangoQueryHandle.query()` is readable at execution time and `ArangoSplit` needs no new field. A passthrough emits `new ArangoSplit(List.of())` — the existing record unchanged, whose empty `shardIds` already carries the meaning "no shard restriction."

Two existing unconditional casts are the concrete dispatch points to fix, and each is a `ClassCastException` waiting on a passthrough handle rather than a hypothetical:

- `ArangoPageSourceProvider.createPageSource` — `ArangoTableHandle handle = (ArangoTableHandle) table;`
- `ArangoSplitManager.getSplits` — the equivalent cast before shard discovery.

### 5.2 The handle carries no `SchemaTableName`

Mongo's PTF sidesteps this: its `QueryFunctionHandle` wraps a real `MongoTableHandle`, which already has a `SchemaTableName`. `ArangoQueryHandle` is `(database, query, columns)` and has no table identity, so any `ConnectorMetadata` method that needs a name must synthesize one: **`new SchemaTableName(database, "query")`** — stable, and it renders legibly in `EXPLAIN` output.

**Open, to settle in the plan's first task:** whether Trino invokes `getTableMetadata` / `getColumnHandles` on a PTF-derived handle at all, given that `TableFunctionAnalysis`'s `Descriptor` already fixes the returned type and `applyTableFunction` returns the column handles directly. If it does not, the corresponding rows in §8 are work that isn't needed and should be dropped rather than written defensively. This is a cheap empirical check against `DistributedQueryRunner`, not a design question.

---

## 6. Decline surface

| Hook | Behaviour on `ArangoQueryHandle` |
|---|---|
| `applyFilter` | `Optional.empty()` |
| `applyLimit` | `Optional.empty()` |
| `applyProjection` | `Optional.empty()` |
| `applyAggregation` | `Optional.empty()` |
| `ArangoSplitManager.getSplits` | **exactly one split, before any shard discovery** |

The single-split rule has the same justification as M5's aggregated handle (master spec §6.4) plus one of its own: the query is opaque, so there is no collection to enumerate shards for and no way to rewrite it per shard.

Each row above is a test, not a comment. This is the failure family that produced both the `TupleDomain.none()` finding (PR #35) and the N-splits/N-duplicate-rows reasoning in M5.

---

## 7. Configuration

| Property | Default | Purpose |
|---|---|---|
| `arangodb.query-function-enabled` | `true` | Registers/unregisters `arango.system.query` |

**Disabled means unregistered.** The Guice binding is conditional, so when the flag is false the function is not added to the multibinder and `arango.system.query` does not exist. The tradeoff, stated: the user sees Trino's generic "table function not registered" rather than a tailored message. Accepted deliberately — a security kill switch should remove the surface rather than leave a live entry point that self-reports as disabled.

**Sample size `k` reuses `arangodb.schema.sample-size`** (default 1000) rather than introducing a property; it is the same concept — how many documents to infer from — and one knob is better than two. **Recorded reservation:** 1000 rows is heavy to pull at *planning* time for an expensive traversal. If that proves painful in practice the remedy is a dedicated `arangodb.query-function.sample-size` with a smaller default; this is a config addition, not a redesign.

---

## 8. Components changed

| File | Change |
|---|---|
| `io/arango/trino/ptf/ArangoQueryFunction.java` | **new** — `Provider<ConnectorTableFunction>`; inner `QueryFunction extends AbstractConnectorTableFunction`; `QueryFunctionHandle` |
| `io/arango/trino/ptf/AqlReadOnlyGate.java` | **new** — pure verdict over an explain result |
| `io/arango/trino/handle/ArangoQueryHandle.java` | **new** — record |
| `io/arango/trino/client/ArangoClient.java` | `explainQuery(db, aql)`; `firstBatch(db, aql, k)` with guaranteed cursor disposal |
| `io/arango/trino/ArangoConnector.java` | `getTableFunctions()` returning the injected set |
| `io/arango/trino/ArangoModule.java` | conditional multibinder binding |
| `io/arango/trino/ArangoConfig.java` | `arangodb.query-function-enabled` |
| `io/arango/trino/ArangoMetadata.java` | `applyTableFunction`; `getTableMetadata`/`getColumnHandles` for the new handle; four declines |
| `io/arango/trino/ArangoSplitManager.java` | single-split short-circuit for the new handle |
| `io/arango/trino/ArangoPageSourceProvider.java` | dispatch: run the stored query verbatim, no `AqlBuilder` |
| `io/arango/trino/ArangoErrorCode.java` | `ARANGODB_QUERY_NOT_READ_ONLY(1, USER_ERROR)` |

`ArangoPageSource`, `ValueMaterializer`, `TypeMapper`, and `AqlBuilder` are **untouched**.

**Spotless sequencing constraint** (same as M5 §8.1): the ratchet is file-granular against `origin/master`, so every file in the table above will be reformatted to AOSP google-java-format on first edit. New files are unaffected; the edits to existing files must expect a full-file reflow in their diff.

---

## 9. Errors

| Condition | Result |
|---|---|
| A plan collection has `type != "read"` | `ARANGODB_QUERY_NOT_READ_ONLY`, naming the offending collection |
| Explain rejects the query (syntax error, undeclared bind parameter) | `INVALID_FUNCTION_ARGUMENT` carrying the server's message |
| Non-object or empty result batch | `INVALID_FUNCTION_ARGUMENT` with corrective guidance (§4.1) |
| Database not found (driver error 1228) | `TableNotFoundException`, consistent with `ArangoMetadata`'s existing 1228 handling |
| Collection named in the query does not exist (driver error 1203) | `INVALID_FUNCTION_ARGUMENT` carrying the server's message. This is a *user* error in a user-supplied string — routing it to `GENERIC_INTERNAL_ERROR` would misreport a typo as a connector fault |
| Any other `ArangoDBException` | `GENERIC_INTERNAL_ERROR`, consistent with the existing translation rule |

`analyze()` is the first path on which `ArangoClient` is called from **planning**, on the coordinator. These errors therefore surface during analysis rather than execution, which changes where a user sees them.

---

## 10. Accepted limitations

1. **A query returning no rows cannot be planned** (§4.1). Trino has no zero-column table.
2. **The query executes twice** — once at planning for schema, once at execution (§4).
3. **Schema is inferred from a prefix.** A field appearing only after row `k`, or a type that changes later in the result, is not in the derived schema; the former is absent, the latter degrades through `ValueMaterializer`'s existing coercion policy.
4. **Non-deterministic queries** (`SORT RAND()`, concurrent writers) may derive a schema from rows the execution run does not produce. Same mechanism as (3).
5. **`query()` bypasses Trino table- and column-level security** — inherited from the Mongo/JDBC precedent, and the reason the kill switch exists.

---

## 11. Testing

| Test | Kind | Covers |
|---|---|---|
| `AqlReadOnlyGateTest` | unit, pure | Verdict over explain fixtures: all-read admits; any write rejects; empty collections admits |
| `AqlPassthroughAssumptionsTest` | container | Pins every row of §3's table, plus the §3.1 bind-parameter rejection. The test that fails if an ArangoDB upgrade changes the invariant — analogue of `AqlSemanticsAssumptionsTest` |
| `ArangoQueryFunctionTest` | container | `analyze()`: every rule in §4.1; error paths in §9 |
| `ArangoQueryHandleTest` | unit | Jackson round-trip |
| `ArangoMetadataPassthroughTest` | unit | All four hooks decline (§6) |
| `ArangoSplitManagerTest` | unit | One split, no shard discovery invoked |
| `ArangoConnectorQueryFunctionTest` | e2e (`DistributedQueryRunner`) | Traversal returns correct rows; an `INSERT` is rejected **and the target collection's count is unchanged** (§3.0); disabled flag hides the function |
| `PassthroughClusterIT` | cluster | A `WITH`-declared traversal end-to-end — the case that killed §7's wrapper |

---

## 12. Decisions recorded (for review-gate attention)

1. **Explain-plan allowlist over parse-AST denylist** (§3) — a correction to master spec §7, driven by `AqlParseEntity.getCollections()` carrying no access mode.
2. **First-batch derivation over subquery wrapping** (§4) — a correction to master spec §7, driven by the measured `WITH` syntax error.
3. **Separate handle type over a handle field** (§5) — diverges from M5's precedent, deliberately.
4. **Descriptor argument deferred** (§1) — its justification was removed by decision 2.
5. **Reject rather than synthesize on empty/non-object results** (§4.1).
6. **Disabled means unregistered** (§7), accepting a worse error message.
7. **`k` reuses `arangodb.schema.sample-size`** (§7), with the reservation recorded there.

---

## Appendix A — Verified SPI and driver surface

`trino-spi` 483, `io/trino/spi/function/table/` (checked via `unzip -l`, 2026-07-26): `AbstractConnectorTableFunction`, `ConnectorTableFunction`, `ConnectorTableFunctionHandle`, `ScalarArgumentSpecification`, `DescriptorArgumentSpecification`, `Descriptor`/`Descriptor$Field`, `TableFunctionAnalysis`, `ReturnTypeSpecification$GenericTable` — all present.

`com.arangodb:core` 7.13.0 (checked via `javap`, 2026-07-26):
- `ArangoDatabase.explainQuery(String, Map, AqlQueryExplainOptions) → AqlExecutionExplainEntity`
- `AqlExecutionExplainEntity$ExecutionPlan.getCollections() → Collection<ExecutionCollection>`
- `AqlExecutionExplainEntity$ExecutionCollection.getName() / .getType()` ← the access mode the gate reads
- `ArangoDatabase.parseQuery(String) → AqlParseEntity`; `AqlParseEntity.getCollections() → Collection<String>` ← names only, no access mode: the reason §3 rejects the parse route

## Appendix B — Probe provenance

All measurements in §3 and §4 were taken 2026-07-26 against `arangodb/arangodb:3.12` (reported `3.12.4-3`), single-server, over the HTTP API — the same image the test suite pins in `TestingArangoServer` and `arangodb-cluster-compose.yml`. Fixtures: collection `users` (2 docs), edge collection `follows` (1 edge), named graph `social`. The `WITH`-in-subquery result (§4) is a parser-level rule and reproduces on a single server; the cluster IT in §11 exists to prove the end-to-end path, not to re-establish the syntax finding.

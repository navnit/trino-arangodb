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

1. **It is closed by construction.** ArangoDB must know a query's write collections up front in order to take locks, so a query cannot mutate a collection it has not declared. There is no enumeration to keep in sync as AQL grows new constructs — the contrast with an AST/keyword denylist, where a missed construct silently opens the gate. §3.2 measures this mechanism directly rather than assuming it.
2. **It has no false positives from text.** A string literal containing `INSERT INTO` plans as `read` (row 5), which is exactly the failure mode §7 cites against keyword scanning.

**Fail closed.** The rule is *admit only if every entry's `type` is exactly `read`*. An absent, null, or unrecognized `type` — ArangoDB's transaction API also knows `exclusive` — rejects. Strictness under novelty is the whole advantage of an allowlist, so it is pinned as its own unit test rather than left implied.

The gate reads `plan.collections[]` only. Write **node** types are recorded above as corroboration and are *not* part of the check — checking them would reintroduce the denylist.

**Defence in depth.** This gate is a guard rail, not the primary control. The deployment guidance remains a read-only ArangoDB user, and §7's kill switch removes the surface entirely.

### 3.1 Ordering invariant

> **The gate runs to completion before anything executes the user's query.**

This is a correctness requirement, not a convenience. `firstBatch` (§4) executes the query *for real* — if the two steps were transposed, a query the gate is about to reject would already have written. `analyze()` must therefore run the explain request → `AqlReadOnlyGate.check` → and only on a clean verdict `firstBatch`. The e2e test in §11 asserts this by checking that a rejected `INSERT` left the target collection's count unchanged, which is the assertion that fails if the ordering ever inverts; asserting only that the error was raised would not catch it.

### 3.2 The lock mechanism, measured — AQL user-defined functions

The closure argument in (1) is an argument about *AQL-level* data-modification operations. AQL **user-defined functions** are the construct that most plausibly escapes it: a UDF is registered JavaScript, invoked as `NS::FN(...)` or dynamically through `CALL()`/`APPLY()`, and a query calling one declares **no** collections at all — so the gate admits it. Measured on **both 3.12.4 and 3.11.14** (the connector's minimum supported server, per M3's version pin):

```
explain  RETURN EVIL::WRITE("pwned")
         plan.collections = (empty)
         plan.nodes       = [CalculationNode, ReturnNode, SingletonNode]
         gate verdict     = ADMIT

execute  RETURN EVIL::WRITE("pwned")            -- UDF body calls db.users.save(...)
         result      = ["BLOCKED: unregistered collection used in transaction: users [write]"]
         users count = 2 -> 2                   -- no write

execute  RETURN CALL("EVIL::WRITE", "dyn")      users count 2 -> 2, plan.collections = (empty)
execute  RETURN APPLY("EVIL::WRITE", ["dyn2"])  users count 2 -> 2, plan.collections = (empty)
```

The server's own error — *"unregistered collection used in transaction: users [write]"* — **is** the lock-declaration mechanism the closure argument invokes, refusing a write to a collection the query never declared. This converts (1) from an assertion into a measurement, and it covers the dynamic-invocation forms that a node-type denylist would separately have to enumerate.

Consequently the gate needs no UDF-specific rule. The residual exposure is a UDF that *reads* something the caller shouldn't see; that is bounded by the read-only DB user's grants, which remain the primary control. `AqlPassthroughAssumptionsTest` pins this case — if a future ArangoDB relaxed the transaction-registration rule, that test fails and the gate's soundness argument is revisited rather than silently lost.

### 3.3 System collections are *not* excluded by the gate

`ArangoMetadata` deliberately hides system collections (`ArangoMetadata.java:103` and `:185` filter `isSystem`), but a passthrough query reading `_users` plans as an ordinary `read` and is admitted. That is the connector's *own* convention being bypassed, which §10.5's "bypasses Trino table- and column-level security" does not cover.

**Decision:** the gate additionally rejects a plan whose `collections[]` contains a name beginning with `_`, keeping the passthrough consistent with what `listTables` will show. Stated honestly, this is **hardening, not a guarantee**: `DOCUMENT("_users/x")` resolves its collection at runtime and does not appear in the plan, so it slips through. The real control remains the read-only user's grants, and the deployment guidance says so.

### 3.4 Declared-but-unbound bind parameters

Explain **rejects** a query carrying a bind parameter for which no value is supplied (HTTP 400, `no value specified for declared bind parameter 'minAge'` — the parameter is *declared by the query text* and left unbound, which is why explain refuses to plan it). Since the PTF accepts no bind values, such a query could never execute either. It is therefore a normal, tested rejection path (§9), not a gate limitation.

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

**Instead:** execute the user's query verbatim with `batchSize = k` **and `stream = true`**, read the first batch, then dispose the cursor. Measured to work for every case including the one the wrapper cannot express:

```
plain read       201  [{"_key":"a",...}]
WITH traversal   201  [{"_key":"b",...}]     <- unrepresentable under the wrapper
non-object rows  201  ["ann","bob"]
empty result     201  []
```

**`stream = true` is load-bearing, not a tuning knob.** With ArangoDB's default (non-stream) cursor the server executes the query and **materializes the complete result** before serving the first batch — `batchSize` would bound only *transfer*, not computation or server memory. Without `stream`, deriving a schema for an expensive traversal would cost its full result server-side at planning time, which is the cost this section claims to avoid. `AqlQueryOptions.stream(Boolean)` and `.batchSize(Integer)` both exist in driver 7.13 (verified via `javap`).

**Honest accounting of cost.** This does *not* eliminate the double execution §7 concedes. `analyze()` runs on the coordinator at planning time; the page source runs on a worker at execution time; a cursor cannot be handed between them, so the query runs twice. What the change buys is (a) correctness on `WITH`, and (b) with `stream = true`, a planning-time run that computes roughly `k` rows rather than the whole result — though a query with a blocking operator (`SORT`, `COLLECT`, `COLLECT AGGREGATE`) must compute its full input server-side regardless of streaming.

**Cursor disposal is mandatory,** not hygiene — and `stream = true` raises the stakes: a stream cursor holds a server-side query snapshot open until it is disposed or its TTL expires. `firstBatch` disposes via `DELETE /_api/cursor/<id>` in a `finally`.

### 4.1 Rules

| Case | Behaviour |
|---|---|
| Rows are objects | Field union across the batch, per-field type via `TypeMapper.merge` — identical to `SchemaResolver`, so the same data infers the same types whether scanned or passed through |
| Rows are non-objects (`RETURN d.name` → `"ann"`) | **Reject** — `INVALID_FUNCTION_ARGUMENT`, message directing the user to `RETURN {name: d.name}`. A Trino table needs named columns and there is no defensible synthetic name |
| Empty result set | **Reject** — a zero-column table is not representable in Trino. Recorded limitation (§10): a legitimately-empty traversal cannot be planned |
| Batch mixes object and non-object rows | **Reject**, same rule and message as the all-non-object case. Reachable: `FOR x IN [{a:1}, 42, "str", null] RETURN x` returns exactly that (measured) |
| An attribute key is the empty string | **Reject** — `INVALID_FUNCTION_ARGUMENT` with guidance. Not hypothetical: ArangoDB accepts and returns `{"": 1}` (measured), and `Descriptor.Field` throws `IllegalArgumentException("name is empty")`, so without this rule the failure escapes as an engine internal error instead of a user error |
| Keys differ only in case (`Name` and `name`) | Both become columns, matching what `SchemaResolver` already does for collection tables. Unquoted SQL references to either are then ambiguous — a Trino-level error, and parity with the existing path is the right behaviour |
| `_key`/`_id`/`_rev` present | Ordinary **visible** columns. The hidden-system-attribute rule in `SchemaResolver` is a property of a *collection* table; a passthrough result has no collection identity |
| `ArangoColumnHandle.path` for a derived column | `List.of(name)`. `path` exists for `AqlBuilder.buildReturnClause`'s document accessor, and a passthrough never calls it; the read path extracts values by **name** (`ArangoPageSource.java:47` does `row.get(col.name())`), which matches the user's `RETURN` object keys by construction. Non-null is required by the record, so it is set consistently rather than left empty |
| Trino requests a subset of columns | Works unchanged. Projection cannot be pushed into opaque AQL (§6), so the cursor yields full rows and `ArangoPageSource` reads only the requested handles' keys — the behaviour it already has |
| Execution rows diverge in *value* type from the derivation batch | Falls through to `ValueMaterializer`: `lenient` → `NULL`, `strict` → `ARANGODB_TYPE_CONVERSION_ERROR`. Pre-existing behaviour, documented here rather than newly specified |
| Execution returns a *non-object row* that the derivation batch did not contain | Not a `ValueMaterializer` case: the cursor is typed `Map.class`, so the driver raises a deserialization failure before materialization. Surfaces per the §9 row for it |

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

**Settled — Trino does call it.** `PlanPrinter`'s `TableInfoSupplier.apply()` calls `Metadata.getTableName(...)` on *every* `TableScanNode`, including one produced by `RewriteTableFunctionToTableScan`, and `MetadataManager.getTableName` delegates to `ConnectorMetadata.getTableName`, whose default chains through `getTableSchema` → `getTableMetadata` (verified against `trino-main` 483). So `EXPLAIN SELECT * FROM TABLE(arango.system.query(...))` reaches `getTableMetadata` with an `ArangoQueryHandle`, and without the §8 rows it is a `ClassCastException`. Those rows stay, and §11 adds an `EXPLAIN`-over-passthrough case — currently the *only* caller of that path, so nothing else would catch its absence.

`getTableProperties` needs no work: `ArangoMetadata` does not override it and the SPI default constructs a fresh `ConnectorTableProperties`.

Note that `SchemaTableName` lowercases both components, so a mixed-case ArangoDB database renders lowercased in `EXPLAIN` output. Cosmetic, and consistent with how the rest of the connector's names already render.

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
| `io/arango/trino/client/ArangoClient.java` | `explainPlan(db, aql)` — a **raw `Request` to `POST /_api/explain`**, not the driver's `explainQuery` (§8.1); `firstBatch(db, aql, k)` with `stream(true)` and guaranteed cursor disposal |
| `io/arango/trino/ArangoConnector.java` | `getTableFunctions()` returning the injected set (a `Connector` default in SPI 483; must be wired even when the set is empty) |
| `io/arango/trino/ArangoModule.java` | **refactor to `AbstractConfigurationAwareModule`** + `conditionalModule(...)` (§8.2), then the multibinder binding |
| `io/arango/trino/ArangoConfig.java` | `arangodb.query-function-enabled` |
| `io/arango/trino/ArangoMetadata.java` | `applyTableFunction`; `getTableMetadata`/`getColumnHandles` for the new handle; four declines |
| `io/arango/trino/ArangoSplitManager.java` | single-split short-circuit for the new handle |
| `io/arango/trino/ArangoPageSourceProvider.java` | dispatch: run the stored query verbatim, no `AqlBuilder` |
| `io/arango/trino/ArangoErrorCode.java` | `ARANGODB_QUERY_NOT_READ_ONLY(1, USER_ERROR)` |

`ArangoPageSource`, `ValueMaterializer`, `TypeMapper`, and `AqlBuilder` are **untouched**.

### 8.1 The explain call does not use the driver's typed API

The gate reads a field the driver's *non-deprecated* API does not expose. Verified via `javap -v` on `com.arangodb:core` 7.13.0:

- `ArangoDatabase.explainQuery(...) → AqlExecutionExplainEntity` carries `Deprecated: true`. Its `ExecutionCollection` has the typed `getName()` / `getType()` the gate wants.
- Its replacement `explainAqlQuery(...) → AqlQueryExplainEntity` has an `ExecutionCollection` exposing **only** `add(String, Object)` / `get(String)` — no typed accessors at all.

Building the safety-critical component on a deprecated method invites exactly the rework this repo's Dependabot history makes likely (a driver major removing it). **Decision: issue a raw `Request` to `POST /_api/explain` and read the JSON directly**, following the pattern `ArangoClient.listShardIds` already uses (`ArangoClient.java:79-90`). This is also the most honest fit: every measurement in §3 was taken against that HTTP endpoint, so the code reads precisely what the spec pins.

### 8.2 The conditional binding forces a module refactor

`ArangoModule` is a plain `com.google.inject.Module` (`ArangoModule.java:13`), which cannot read `ArangoConfig` at `configure()` time — configuration is bound inside the same module. The Airlift pattern is `AbstractConfigurationAwareModule` plus `conditionalModule(ArangoConfig.class, ArangoConfig::isQueryFunctionEnabled, ...)`. This is a real refactor of the module's base class, not a one-line binding, and the plan should size it as such.

### 8.3 Spotless sequencing constraint

Same as M5 §8.1: the ratchet is file-granular against `origin/master`, so every file in the table above will be reformatted to AOSP google-java-format on first edit. New files are unaffected; the edits to existing files must expect a full-file reflow in their diff.

---

## 9. Errors

| Condition | Result |
|---|---|
| A plan collection has `type != "read"` | `ARANGODB_QUERY_NOT_READ_ONLY`, naming the offending collection |
| Explain rejects the query (syntax error, unbound bind parameter) | `INVALID_FUNCTION_ARGUMENT` carrying the server's message |
| Non-object, mixed, empty-keyed, or empty result batch | `INVALID_FUNCTION_ARGUMENT` with corrective guidance (§4.1) |
| A plan collection name begins with `_` | `ARANGODB_QUERY_NOT_READ_ONLY` is wrong here — use `INVALID_FUNCTION_ARGUMENT`, naming the system collection (§3.3) |
| Database not found (driver error 1228) | `SchemaNotFoundException`. `ArangoMetadata`'s existing 1228 rule is about *classification* — not-found rather than internal — and the missing thing here is a database, so `TableNotFoundException` would render the misleading "Table 'db.query' does not exist" |
| Collection named in the query does not exist (driver error 1203) | `INVALID_FUNCTION_ARGUMENT` carrying the server's message. This is a *user* error in a user-supplied string — routing it to `GENERIC_INTERNAL_ERROR` would misreport a typo as a connector fault |
| Any other `ArangoDBException` | `GENERIC_INTERNAL_ERROR`, consistent with the existing translation rule |

One row is missing from the table above and belongs to the execution path: a query whose *later* rows are non-objects fails in the driver's deserialization against `Map.class`, before `ValueMaterializer` is reached (§4.1). It surfaces as `INVALID_FUNCTION_ARGUMENT` with the same guidance as the planning-time rule, so both paths tell the user the same thing.

These errors surface during **analysis** rather than execution, which changes where a user encounters them. This is *not*, however, the first time `ArangoClient` is called from the planning path — `SchemaResolver.resolveColumns` already samples documents from the coordinator via `ArangoMetadata.resolve` (`ArangoMetadata.java:492`). The genuinely new property is narrower: this is the first path that **executes a user-supplied query string** at planning time.

---

## 10. Accepted limitations

1. **A query returning no rows cannot be planned** (§4.1). Trino has no zero-column table — and the alternative is genuinely foreclosed, not merely inconvenient: for a `GENERIC_TABLE` return spec, Trino's `StatementAnalyzer` requires a typed descriptor (`field.getType().orElseThrow(...)`), so with no rows and no user-supplied descriptor there is nothing legal to return.

   **This limitation is the direct cost of deferring the `DESCRIPTOR(...)` argument (§12/4), not an independent decision.** A traversal returning zero rows is an ordinary production state, so a query that worked in development fails *at planning time* the day its result goes empty. The descriptor argument is the standard remedy — supplied columns would make the empty case representable, and would also skip `firstBatch` entirely for those callers, removing the double execution. Recorded here so the deferral's real price is visible rather than split across two sections that each look defensible alone.
2. **The query executes twice** — once at planning for schema, once at execution (§4).
3. **Schema is inferred from a prefix.** A field appearing only after row `k`, or a type that changes later in the result, is not in the derived schema; the former is absent, the latter degrades through `ValueMaterializer`'s existing coercion policy.
4. **Non-deterministic queries** (`SORT RAND()`, concurrent writers) may derive a schema from rows the execution run does not produce. Same mechanism as (3).
5. **`query()` bypasses Trino table- and column-level security** — inherited from the Mongo/JDBC precedent, and the reason the kill switch exists.
6. **System-collection hiding is only partially enforceable** (§3.3). The plan-based `_`-prefix rejection catches the direct form; `DOCUMENT("_users/x")` resolves at runtime and is not in the plan. An ArangoSearch view linked to a system collection is the same bypass class: the view's own name need not be `_`-prefixed, so a view read admits (`AqlPassthroughAssumptionsTest.arangoSearchViewReadAdmits`) while surfacing data from the underlying system collection. Bounded by the read-only user's grants.
7. **A UDF may read what the caller should not see** (§3.2). Writes are blocked by the server's transaction registration, but a registered UDF's *reads* are bounded only by the DB user's grants.
8. **A passthrough result is unbounded server-side.** `LIMIT` cannot be pushed into opaque AQL, and the execution cursor is deliberately non-streaming (matching the existing scan path's cursor behavior), so the server materializes the full passthrough result regardless of any Trino-side `LIMIT`.

---

## 11. Testing

| Test | Kind | Covers |
|---|---|---|
| `AqlReadOnlyGateTest` | unit, pure | Verdict over explain fixtures: all-read admits; any write rejects; empty collections admits; **absent / null / `exclusive` `type` rejects** (fail closed, §3); `_`-prefixed name rejects (§3.3) |
| `AqlPassthroughAssumptionsTest` | container | Pins every row of §3's table; the §3.2 UDF result (registered UDF admitted by the gate, write blocked by the server, collection count unchanged, plus `CALL()`/`APPLY()`); the §3.4 unbound-parameter rejection; and rows the current table lacks — an ArangoSearch view read (`FOR d IN view SEARCH ...`) and a `SHORTEST_PATH` form. The test that fails if an ArangoDB upgrade changes the invariant — analogue of `AqlSemanticsAssumptionsTest` |
| `ArangoQueryFunctionTest` | container | `analyze()`: every rule in §4.1 including the empty-key, mixed-batch, and case-collision cases; error paths in §9 |
| `ArangoQueryHandleTest` | unit | Jackson round-trip of `ArangoQueryHandle` **and** of `QueryFunctionHandle` — both cross the coordinator/worker boundary |
| `ArangoMetadataPassthroughTest` | unit | All four hooks decline (§6) |
| `ArangoSplitManagerTest` | unit | One split, no shard discovery invoked |
| `ArangoConnectorQueryFunctionTest` | e2e (`DistributedQueryRunner`) | Traversal returns correct rows; an `INSERT` is rejected **and the target collection's count is unchanged** (§3.1); `EXPLAIN SELECT * FROM TABLE(...)` succeeds — the only caller of the `getTableMetadata` path (§5.2); disabled flag hides the function |
| `PassthroughClusterIT` | cluster | A `WITH`-declared traversal end-to-end — the case that killed §7's wrapper — **and the gate itself against a coordinator's distributed plan**: one read-typed and one write-typed `plan.collections` row, plus INSERT-rejected-with-count-unchanged. Every §3 measurement is single-server; the motivating use case is cluster-only, so this converts that generalization into a measurement. Include a single-document write (`INSERT {_key:"x"} INTO c`), whose plan the `optimize-cluster-single-document-operations` rule rewrites |

---

## 12. Decisions recorded (for review-gate attention)

1. **Explain-plan allowlist over parse-AST denylist** (§3) — a correction to master spec §7, driven by `AqlParseEntity.getCollections()` carrying no access mode.
2. **First-batch derivation over subquery wrapping** (§4) — a correction to master spec §7, driven by the measured `WITH` syntax error.
3. **Separate handle type over a handle field** (§5) — diverges from M5's precedent, deliberately.
4. **Descriptor argument deferred** (§1) — its cost-based justification was removed by decision 2. **Its remaining cost is limitation §10.1**: an empty result cannot be planned at all. The two are one decision, not two.
5. **Reject rather than synthesize on empty/non-object results** (§4.1) — a consequence of decision 4, not independent of it.
6. **Disabled means unregistered** (§7), accepting a worse error message.
7. **`k` reuses `arangodb.schema.sample-size`** (§7), with the reservation recorded there.
8. **Explain is issued as a raw HTTP request, not via the driver's typed API** (§8.1) — the typed accessors exist only on a deprecated method.
9. **The gate rejects `_`-prefixed collections** (§3.3) — hardening toward the connector's own hiding convention, explicitly not a guarantee.
10. **`SchemaNotFoundException` for a missing database** (§9), departing from `ArangoMetadata`'s `TableNotFoundException` because the synthesized table name would read as nonsense.

---

## Appendix A — Verified SPI and driver surface

`trino-spi` 483, `io/trino/spi/function/table/` (checked via `unzip -l`, 2026-07-26): `AbstractConnectorTableFunction`, `ConnectorTableFunction`, `ConnectorTableFunctionHandle`, `ScalarArgumentSpecification`, `DescriptorArgumentSpecification`, `Descriptor`/`Descriptor$Field`, `TableFunctionAnalysis`, `ReturnTypeSpecification$GenericTable` — all present.

Also verified present: `TableFunctionApplicationResult(T, List<ColumnHandle>)`, the four-argument `analyze(ConnectorSession, ConnectorTransactionHandle, Map<String, Argument>, ConnectorAccessControl)`, and `Connector.getTableFunctions()` as an SPI default. `Descriptor.Field` throws `IllegalArgumentException("name is empty")` on an empty name — the reason §4.1 has an empty-key rule.

`com.arangodb:core` 7.13.0 (checked via `javap -v`, 2026-07-26):
- `ArangoDatabase.explainQuery(String, Map, AqlQueryExplainOptions) → AqlExecutionExplainEntity` — **`Deprecated: true`**. Its `ExecutionCollection` has the typed `getName()` / `getType()` the gate wants.
- `ArangoDatabase.explainAqlQuery(...) → AqlQueryExplainEntity` — the non-deprecated replacement. Its `ExecutionCollection` exposes **only** `add(String, Object)` / `get(String)`.
- Together these are why §8.1 chooses a raw `POST /_api/explain` over either.
- `AqlQueryOptions.stream(Boolean)` and `.batchSize(Integer)` — both present; §4 requires both.
- `ArangoDatabase.parseQuery(String) → AqlParseEntity`; `AqlParseEntity.getCollections() → Collection<String>` ← names only, no access mode: the reason §3 rejects the parse route.

`trino-main` 483 (source, 2026-07-26): `PlanPrinter.TableInfoSupplier.apply()` → `Metadata.getTableName` → `MetadataManager.getTableName` → `ConnectorMetadata.getTableName` → `getTableSchema` → `getTableMetadata`, on every `TableScanNode` including one from `RewriteTableFunctionToTableScan` — the basis for §5.2.

## Appendix B — Probe provenance

All measurements in §3 and §4 were taken 2026-07-26 against `arangodb/arangodb:3.12` (reported `3.12.4-3`), single-server, over the HTTP API — the same image the test suite pins in `TestingArangoServer` and `arangodb-cluster-compose.yml`. Fixtures: collection `users` (2 docs), edge collection `follows` (1 edge), named graph `social`.

The §3.2 UDF probe was additionally run against `arangodb:3.11` (reported `3.11.14`) — the connector's minimum supported server under M3's version pin — with **identical results**, so the transaction-registration mechanism the gate's soundness rests on is measured across the supported range rather than at its top end only.

§4.1's pathological-input rows are measured, not hypothesized: ArangoDB accepted and returned a document with an empty-string attribute key; `RETURN {Name:1, name:2}` returned both; and `FOR x IN [{a:1}, 42, "str", null] RETURN x` returned the mixed batch verbatim.

**Generalization boundaries, stated rather than assumed.** The `WITH`-in-subquery result (§4) is a parser-level rule and reproduces on a single server, so the cluster IT exists to prove the end-to-end path, not to re-establish that finding. By contrast, `plan.collections[]`'s shape on a **coordinator's distributed plan** is an inference from single-server measurement — which is why §11 moves it into `PassthroughClusterIT` rather than leaving it inferred.

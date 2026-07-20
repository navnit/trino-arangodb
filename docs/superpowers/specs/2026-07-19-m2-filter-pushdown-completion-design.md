# M2 Filter-Pushdown Completion — Design Spec

- **Status:** Approved (brainstorming)
- **Date:** 2026-07-19
- **Parent spec:** `docs/superpowers/specs/2026-07-18-arangodb-trino-connector-design.md` (§4.4 coercion policy, §6.1 `applyFilter`)
- **Scope:** Widen filter pushdown from the current BOOLEAN-only subset to the full M2 §6.1 intent — equality/IN for all scalar types plus numeric range — and ship the `arangodb.type-coercion` (lenient|strict) policy that makes it provably correct. Read-only; no split/aggregation changes.

---

## 1. Background & motivation

M2 shipped filter pushdown narrowed to **BOOLEAN equality/IN only** (commit `74dd3f3`). The narrowing was a correct response to a real bug found in whole-branch review: `ArangoMetadata.isPushable` classified VARCHAR/BIGINT/DOUBLE predicates as pushable, but the read path silently disagreed with AQL, so pushed queries could drop or wrongly include rows with no error.

The root cause is **`ArangoPageSource.appendValue` coerces on read**, while AQL's `==`/`IN` compare **type-strictly**:

- VARCHAR branch: `String.valueOf(value)` — a stored number `42` becomes `"42"`.
- BIGINT branch: `Number.longValue()` — a stored `42.5` silently **truncates** to `42`.
- DOUBLE branch: `Number.doubleValue()` — already exact enough (any `Number` is representable).
- BOOLEAN branch: `value instanceof Boolean` — exact, no coercion.

Because AQL `d.f == '42'` never matches a stored number `42`, and AQL `d.f == 42` never matches a stored `42.5`, the pushed (AQL) answer and the residual (post-coercion, in-engine) answer can diverge — and because the predicate was reported fully enforced, nothing re-checks it. BOOLEAN is the one branch where read-coercion is already type-exact, so it was the only safe type.

Crucially, the current `String.valueOf(...)` / `longValue()` behavior matches **neither** of the parent spec's §4.4 policies. §4.4 "lenient" is defined as *type-mismatched value reads as NULL* — not "coerce it." So today's coercion is effectively a bug relative to §4.4, and fixing it to be spec-correct is the change that unlocks safe pushdown.

### 1.1 Vendor alignment (why this design, not a looser one)

Both Trino and ArangoDB independently prescribe the conservative, correctness-first approach this design takes:

- **Trino** (`ConnectorMetadata.applyFilter` SPI contract): pushdown is a semantically transparent optimization. The connector enforces only what it can reproduce with identical semantics and returns the rest as the **remaining** (residual) filter, which the engine re-applies. The result set must be identical whether or not pushdown occurs. When in doubt, push nothing.
- **ArangoDB** (official Spark datasource): pushes filters by dynamically generating AQL, but explicitly documents the numeric-value-vs-string-schema mismatch as a hazard, and recommends only pushing when the schema type matches the stored value type; otherwise the filter falls back to engine-side execution (correct, just slower).

The one place our design must do **more** than ArangoDB's Spark connector: Spark trusts an *explicit, user-supplied* schema, so it can assume a BIGINT column holds only integers and omit runtime guards. Our schema is **sampled/inferred** (`SchemaResolver`), so we cannot assume an unsampled document lacks a stray `42.5`. We therefore move the correctness proof to **runtime** via per-type AQL guards (§4.3).

### 1.2 Precedent: `trino-mongodb` (closest analog) and where we deliberately diverge

The Trino MongoDB connector (schemaless, sampled schema, document store — the parent spec's named reference) was read directly. Findings:

- **It pushes by column *type*, not value-guard.** `MongoMetadata.applyFilter` splits domains on `isPushdownSupportedType(type)` and claims supported types fully (empty residual). `MongoSession.buildPredicate` emits plain `$gt`/`$lt` with no `IS_NUMBER`-style guard.
- **It gets range guards for free from BSON type-bracketing.** MongoDB's comparison operators only match same-type values (`{age:{$gt:30}}` matches numeric ages only). **AQL is not bracketed** — it uses a single total order `null < bool < number < string`, so `d.age > 30` would match strings. This is precisely why our design must add `IS_NUMBER`/`FLOOR` guards that MongoDB can omit; it empirically confirms parent spec §6.1.
- **Its read-coercion has the identical inconsistency we found, shipped as a known wart.** `MongoPageSource` uses `String.valueOf(value)` for VARCHAR (number `42`→`"42"`) and `((Number)value).longValue()` for BIGINT (truncates `42.5`→`42`), catching `ClassCastException`→NULL with a literal `// TODO remove (fail clearly), or hide behind a toggle`. So Mongo pushes `col='42'` type-strictly (excludes a stored number) while reading it unpushed as `"42"` (includes it) — the exact pushed-vs-residual divergence this spec closes.

**Our deliberate divergences (correctness over breadth):** we make read-coercion type-exact instead of coercing (closing the truncation / `String.valueOf` holes Mongo leaves open); we add the explicit AQL guards MongoDB gets from BSON; and the `arangodb.type-coercion=lenient|strict` toggle (§4.1) is exactly the "fail clearly, or hide behind a toggle" fix the MongoDB connector's own `TODO` requests. We push IS NULL and string ranges *less* than Mongo (they stay residual): under mismatch→NULL coercion, AQL `d.f == null` tests the raw value while Trino reads a type-mismatched value as NULL, so pushing IS NULL would diverge for the same reason — Mongo tolerates this, we do not. Matching Mongo's broader pushdown would mean accepting its known wart; Trino's `applyFilter` transparency contract and ArangoDB's Spark guidance both say not to.

---

## 2. The core invariant

> **The AQL-side guard must admit *exactly* the set of stored values that `appendValue` writes as non-NULL.**

When this holds, the pushed (AQL) filter and Trino's own post-read filter cannot disagree — "agree by construction." This invariant is the acceptance criterion for every pushable-predicate decision below, and it is exactly Trino's transparency contract expressed for a sampled-schema connector.

---

## 3. Scope

**In scope**
- `arangodb.type-coercion` = `lenient` (default) | `strict` config, and the read-path rewrite that honors it.
- Type-exact read coercion in `ArangoPageSource.appendValue` for BOOLEAN/BIGINT/DOUBLE/VARCHAR.
- `isPushable` widening: equality/IN for BOOLEAN/VARCHAR/BIGINT/DOUBLE (in lenient mode).
- Numeric range (`<`,`>`,`<=`,`>=`) pushdown for BIGINT/DOUBLE with per-type AQL guards.
- `AqlBuilder.renderDomain` range rendering (it currently throws on any non-discrete-set domain).
- Strict-mode rule: decline all filter pushdown when `type-coercion=strict` (§5).
- `ArangoErrorCode` enum with `ARANGODB_TYPE_CONVERSION_ERROR` (strict-mode failure).

**Out of scope (deferred, unchanged)**
- **String range** pushdown — stays residual (parent spec §6.1: ArangoDB ICU collation vs Trino codepoint ordering can disagree; deferred behind a future `arangodb.string-range-pushdown` flag).
- IS NULL / IS NOT NULL pushdown — stays residual (existing `isNullAllowed()` rejection preserved).
- DECIMAL/ARRAY/ROW value materialization — still rejected upstream by `checkMaterializable`.
- Shard-parallel splits (M3), aggregation pushdown (M4).

---

## 4. Component design

### 4.1 `ArangoConfig` — new `type-coercion` property

Add `arangodb.type-coercion` = `lenient` | `strict`, default `lenient`, following the existing `@Config`/enum pattern used for `mixed-type-strategy`:

```java
public enum TypeCoercion { LENIENT, STRICT }
private TypeCoercion typeCoercion = TypeCoercion.LENIENT;
// @Config("arangodb.type-coercion") setter + getter
```

### 4.2 `ArangoPageSource.appendValue` — type-exact coercion + policy

Rewrite the per-type branches to accept only type-matching values, and route every mismatch through the policy. `appendValue` gains access to the coercion mode by storing it as a **final field on the `ArangoPageSource` instance**, set in the constructor from a `TypeCoercion` passed by `ArangoPageSourceProvider`; `appendValue` becomes a non-static instance method reading that field. `ArangoPageSourceProvider` does **not** currently receive `ArangoConfig` — its constructor is extended to take `ArangoConfig` (Guice injects the config-bound instance; no `ArangoModule` change needed since `ArangoConfig` is already bound via `configBinder`) so it can pass the mode down.

| Column type | Accept (write value) | Mismatch → policy |
|---|---|---|
| BOOLEAN | real `Boolean` | anything else |
| BIGINT | `Number` that is **integral and in `long` range** (incl. `42.0` → `42`) | `42.5`, `"42"`, `true` |
| DOUBLE | any `Number` | `"x"`, `true` |
| VARCHAR | real `String` (or `Slice`) | `42`, `true` |

- **`lenient` (default):** mismatch → `out.appendNull()` (unchanged control flow; the difference is that mismatches that used to coerce now NULL).
- **`strict`:** mismatch → `throw new TrinoException(ARANGODB_TYPE_CONVERSION_ERROR, <column, expected type, actual value type>)`.

The existing `catch (RuntimeException)` → lenient-NULL fallback stays for genuinely unanticipated failures in lenient mode; in strict mode a coercion mismatch is raised explicitly rather than swallowed.

**Behavior change (accepted).** This changes M1 read semantics: `SELECT col` (no filter) on a VARCHAR column holding number `42` now returns NULL (lenient) or errors (strict), instead of `"42"`. Likewise a BIGINT column holding `42.5` returns NULL/errors instead of a truncated `42`. Existing M1 read tests that assert the old coerced output are updated — they encode the pre-fix behavior. This aligns the read path with parent spec §4.4.

### 4.3 `ArangoMetadata.isPushable` — widening

`isPushable` gains access to the coercion mode by becoming a **non-static instance method** reading the mode from an `ArangoConfig` field. `ArangoMetadata` does **not** currently receive `ArangoConfig` — its constructor is extended to take it (Guice injects the config-bound instance; no `ArangoModule` change needed). `isPushable` is currently `private static`. Rules, evaluated in order:

1. `domain.isAll()` → false (unchanged).
2. **`type-coercion == STRICT` → false** (decline all pushdown in strict mode; see §5).
3. `domain.isNullAllowed()` → false (unchanged — preserves the `flag = true OR flag IS NULL` safety; IS NULL / IS NOT NULL stay residual).
4. `domain.getValues().isAll()` → false (unchanged).
5. **Equality / IN** (`domain.getValues().isDiscreteSet()`): pushable for BOOLEAN, VARCHAR, BIGINT, DOUBLE — **no guard needed** (AQL `==`/`IN` are type-strict; §4.2 makes the read side agree).
6. **Numeric range** (a non-discrete-set ordered domain on BIGINT or DOUBLE): pushable **with a guard** (§4.4). VARCHAR range → not pushable (residual).

Anything not matched → residual.

### 4.4 `AqlBuilder` — range rendering with per-type guards

`renderDomain` currently handles only discrete sets and throws otherwise. Add a range branch that walks the domain's ranges and emits guarded comparisons. The guard is chosen so the AQL-admitted set equals the `appendValue`-non-NULL set (§2):

- **DOUBLE range:** `IS_NUMBER(d.f) AND d.f > @v` (any numeric value round-trips through the DOUBLE branch).
- **BIGINT range:** `IS_NUMBER(d.f) AND d.f == FLOOR(d.f) AND d.f > @v` — the **integrality check is essential**: without it a stored `35.5` passes `IS_NUMBER … > 30` in AQL (row included) but reads back NULL under the integral-strict BIGINT coercion, resurrecting the exact bug that forced the original narrowing.

Equality/IN rendering is unchanged (`==` / `IN`), now reached for all scalar types rather than only BOOLEAN. The `renderDomain` guard comment (currently describing the BOOLEAN-only narrowing) is rewritten to describe the widened, guarded contract.

Multi-range domains (e.g. `x < 10 OR x > 20`) render as an OR of guarded comparisons within the column's parenthesized clause; each column clause is still AND-joined across columns in `buildScan` (unchanged).

### 4.5 `ArangoErrorCode`

New enum implementing `ErrorCodeSupplier` with `ARANGODB_TYPE_CONVERSION_ERROR` (the strict-mode failure). Minimal — one code now; the full §9.1 set (`ARANGODB_CONNECTION_ERROR`, etc.) can be introduced when those paths are built.

---

## 5. Strict mode × pushdown

In strict mode a pushed filter can **hide a type-mismatch error**: `WHERE age = 30` on a doc with `age = "30"` → AQL excludes the row server-side → it never reaches Trino → no error; but without pushdown, Trino reads that row and strict mode **errors**. Same query, different outcome depending on the planner's pushdown choice.

**Rule: when `type-coercion = strict`, `isPushable` returns false for every predicate** — all filters stay residual and Trino reads each candidate row. This keeps the result (including whether a mismatch error fires) independent of pushdown decisions, honoring Trino's transparency contract. Strict mode is a data-quality / debugging mode, not a performance mode, so surrendering pushdown there is the right trade. Lenient mode (the default) gets the full pushdown widening.

---

## 6. Error handling

- Strict-mode coercion mismatch → `TrinoException(ARANGODB_TYPE_CONVERSION_ERROR, …)` naming the column, expected Trino type, and actual value's runtime type.
- Lenient mode → NULL, as today (no error).
- Existing error translation in `ArangoMetadata` (database-not-found vs `GENERIC_INTERNAL_ERROR`) is unchanged.

---

## 7. Testing

Extends existing suites; all container-backed tests require Docker.

- **Validation spike (plan step 0):** a throwaway test against the live Testcontainers ArangoDB confirming the AQL premises before guards are written — `42 == 42.0` is true, `42 == "42"` is false, `IS_NUMBER`/`FLOOR` behavior, and the `null < bool < number < string` ordering. Cheap insurance against a wrong premise; removed or folded into `AqlBuilderTest` afterward.
- **Per-type agreement tests** (`ArangoConnectorPushdownTest`): seed a collection with off-type outliers — a VARCHAR column with a numeric doc, a BIGINT column with `42.5` and `"42"` — and assert **pushed result === residual result === expected**, for equality, IN, and numeric range. This is the direct test of the §2 invariant.
- **Pushdown classification** (`ArangoMetadataTest` / `ArangoConnectorPushdownTest`): `isFullyPushedDown()` for eq/IN and numeric range; `isNotFullyPushedDown()` for string range, null-allowed domains, and IS NULL / IS NOT NULL.
- **AQL rendering** (`AqlBuilderTest`): equality/IN for each scalar type; BIGINT range emits the `IS_NUMBER … == FLOOR …` guard; DOUBLE range emits the `IS_NUMBER` guard; multi-range OR.
- **Coercion** (new test in `type/` or `ArangoPageSourceProviderTest`): lenient → NULL and strict → `ARANGODB_TYPE_CONVERSION_ERROR`, on both a plain scan and under a filter, for VARCHAR-holds-number and BIGINT-holds-float.
- **Strict-mode declines pushdown** (`ArangoMetadataTest`): with `type-coercion=strict`, `applyFilter` leaves the predicate residual (asserts `isPushable` returns false); end-to-end, the mismatch errors rather than being silently filtered out.
- **`ArangoConfigTest`:** `type-coercion` parses `lenient`/`strict`, defaults to `lenient`.
- **M1 read-behavior update:** existing read tests asserting the old coerced output (`"42"`, truncated ints) are updated to expect NULL under the corrected lenient policy.

---

## 8. Files touched

- `src/main/java/io/arango/trino/ArangoConfig.java` — `type-coercion` property + `TypeCoercion` enum.
- `src/main/java/io/arango/trino/ArangoPageSource.java` — type-exact `appendValue` + policy.
- `src/main/java/io/arango/trino/ArangoPageSourceProvider.java` — constructor takes `ArangoConfig`; threads coercion mode into `ArangoPageSource`.
- `src/main/java/io/arango/trino/ArangoMetadata.java` — constructor takes `ArangoConfig`; `isPushable` non-static, widened + strict-mode decline.
- `src/main/java/io/arango/trino/aql/AqlBuilder.java` — range rendering with per-type guards.
- `src/main/java/io/arango/trino/ArangoErrorCode.java` — new enum (`ARANGODB_TYPE_CONVERSION_ERROR`).
- Tests: `ArangoConnectorPushdownTest`, `AqlBuilderTest`, `ArangoMetadataTest`, `ArangoConfigTest`, `ArangoPageSourceProviderTest`, plus a coercion test and the validation spike.

---

## 9. Success criteria

- `SELECT … WHERE col = <v>` / `col IN (…)` for VARCHAR/BIGINT/DOUBLE/BOOLEAN reports `isFullyPushedDown()` in lenient mode.
- `SELECT … WHERE numcol > <v>`: **DOUBLE** range reports `isFullyPushedDown()` with the correct guard; **BIGINT** range is pushed as an AQL prefilter and re-checked in Trino's residual, so it reports `isNotFullyPushedDown()` (see §10). Both still reduce rows over the wire.
- A collection seeded with off-type outliers returns **identical rows** whether the filter is pushed or residual (the §2 invariant), for eq/IN and numeric range.
- `type-coercion=strict` errors on a type mismatch and pushes nothing; `type-coercion=lenient` reads mismatches as NULL and pushes fully.
- String range, IS NULL/IS NOT NULL, and null-allowed domains remain residual and return SQL-correct rows.
- Full suite green (`mvn test`) with Docker running.

---

## 10. Post-review amendment: BIGINT range is a prefilter, not fully pushed (finding C2)

Plan-review (Fable) found a residual correctness gap in §4.4's BIGINT range design: the guard `IS_NUMBER(d.f) AND d.f == FLOOR(d.f)` still admits integral values ≥ 2⁶³, which `appendValue` reads as NULL (out of `long` range). A fully-pushed BIGINT range would therefore emit a NULL row that the residual path excludes — a pushed-vs-residual divergence, the exact bug class this milestone closes.

**Resolution (chosen):** BIGINT range is pushed to AQL as a **prefilter for wire reduction** *and* kept in Trino's residual filter, so Trino re-checks post-read. This is unconditionally correct regardless of ArangoDB's int64/double boundary comparison semantics (no probe needed), at the cost of `isNotFullyPushedDown` for BIGINT range. `applyFilter` routes such "prefilter-only" domains to both the pushed handle and the remaining filter (`isPrefilterOnly` = BIGINT ∧ non-discrete). Equality/IN and **DOUBLE** range remain fully enforced (DOUBLE via the `+ 0.0` promotion added in §11 — its original "`IS_NUMBER` guard matches the read path exactly" justification was incomplete; see finding C1). The core invariant (§2) is generalized: a guard that admits a *superset* of `appendValue`'s non-NULL set is allowed **iff** the predicate is also retained residually. Implemented per `docs/superpowers/plans/2026-07-19-m2-filter-pushdown-completion.md` Task 4.

## 11. Post-review amendment: DOUBLE comparisons must be promoted into double space (finding C1)

The independent final review (Fable) probed a live ArangoDB 3.12 container and refuted a load-bearing premise of §4.3/§4.4: that AQL numeric comparison agrees with the read path for a DOUBLE column. It does not for stored `int64` values beyond 2⁵³. A DOUBLE column legitimately holds stored `int64`s (it is inferred DOUBLE precisely because the sample saw both ints and floats), and `ArangoPageSource.appendValue` reads such a value **rounded** via `n.doubleValue()`. But ArangoDB compares `int64`-vs-`double` by **exact mathematical value**, not in double space — verified: `9007199254740993 == 9007199254740993.0` → `false`, `9007199254740993 > 9007199254740992.0` → `true`. So a bare pushed `d.f <op> @v` diverges from the rounded read path in **both** directions:

- **False include** (range): stored `2⁵³+1` satisfies `> 2⁵³` in AQL but reads back as `2⁵³` (not `>`). DOUBLE range was `isFullyPushedDown`, so nothing re-checked → a row displayed that violates the predicate.
- **False miss** (equality/inclusive bound): stored `2⁵⁴−1` (reads back as `2⁵⁴`) is excluded server-side by `== 2⁵⁴`. This is a *subset* AQL never emits, so — unlike the §10 BIGINT case — the prefilter+residual mechanism structurally **cannot** recover it.

**Resolution (chosen):** `AqlBuilder` renders every DOUBLE comparison (equality/IN **and** range) as `IS_NUMBER(d.f) AND (d.f + 0.0) <op> @v`. The `+ 0.0` promotes the stored operand into double space, so AQL compares exactly the value `appendValue` emits (probed: `(2⁵³+1 + 0.0) > 2⁵³` → false, `(2⁵⁴−1 + 0.0) == 2⁵⁴` → true — both now matching the read path). The `IS_NUMBER` guard precedes the arithmetic because `+ 0.0` coerces non-numbers (`"abc" + 0.0 == 0.0`); AQL's `AND` short-circuits, so the guard is sufficient. DOUBLE therefore stays **fully enforced** (no residual). The promotion is **DOUBLE-only**: BIGINT's read path is exact (`longValue()`, no rounding) against `long` binds, so ArangoDB's exact comparison is what agrees there — promoting BIGINT would reintroduce the same divergence in reverse. Both the C1 divergence and the promotion fix, plus the BIGINT mirror-safety property, are pinned in `AqlSemanticsAssumptionsTest`; end-to-end agreement is pinned in `ArangoConnectorPushdownTest#doublePushdownAgreesWithReadPathForStoredInt64BeyondPrecision`.

## 12. Post-review amendment: BIGINT range must not use a `FLOOR` integrality guard (finding C3)

The re-review (Fable, re-probing the §11 fix against a live 3.12 container) exposed a second instance of the same int64/double class — this time in the BIGINT range guard from §4.4. The guard was rendered `IS_NUMBER(d.f) AND d.f == FLOOR(d.f) AND d.f <op> @v`, the `== FLOOR(...)` conjunct intended to admit only integral values. But AQL `FLOOR()` returns a **double** (probed: `FLOOR(9007199254740993)` → `9007199254740992.0`), and ArangoDB compares `int64`-vs-`double` by exact value — so for a stored `int64` that isn't exactly double-representable (about half of the values in (2⁵³, 2⁵⁴), more above), `d.f == FLOOR(d.f)` is **false** even though the value is integral. Such a row is dropped server-side, though `appendValue` reads it exactly via `longValue()`. Reproduced with the exact production render: seeding `{age: 9007199254740993}` and running the connector's BIGINT-range scan for `age > 100` returned `[]`. It is a server-side false-**miss**, so — like the §11 DOUBLE case — the §10 prefilter+residual mechanism structurally cannot recover it.

**Resolution (chosen):** drop the `== FLOOR(...)` conjunct; the BIGINT range guard is now the bare `IS_NUMBER(d.f) AND d.f <op> @v`. This is safe precisely because BIGINT range is already **prefilter-only** (§10): the pushed AQL only needs to admit a *superset* of what the read path keeps, and Trino's residual re-check is the correctness backstop. The bare guard admits a slightly wider superset — a fractional `35.5` in range, or an integer outside signed-64-bit range, now passes the AQL prefilter, reaches `appendValue`, reads back `NULL`, and is excluded by the residual (probed: dropping the conjunct returns the previously-missing `int64` while `residualFilterIsCorrectOnSampleTypeSkewedColumn` still yields `{10, 20}`, now with the fractional `42.5` caught by the residual rather than the guard). We do **not** attempt a corrected integrality guard: no AQL integrality test avoids `FLOOR`'s double return without its own edge cases, and the residual already guarantees correctness. `AqlSemanticsAssumptionsTest` pins the `FLOOR`-returns-double trap; `ArangoConnectorPushdownTest#bigintRangeReturnsInt64BeyondPrecisionThatFloorGuardWouldDrop` pins end-to-end recovery of the value.

## 13. Known limitation: non-finite stored doubles under the DOUBLE `+ 0.0` promotion (accepted)

The C3 re-review, probing with hand-crafted VelocyPack, found one residual divergence in the DOUBLE promotion of §11. A stored *non-finite* double — `Infinity` or `NaN` — passes `IS_NUMBER`, but the `(d.f + 0.0)` promotion **nullifies** it (`Infinity + 0.0` → `null` in AQL). So a fully-pushed DOUBLE predicate excludes such a row server-side, while `appendValue` would read the real `Double.POSITIVE_INFINITY`/`NaN` back and Trino's filter would keep it (e.g. `WHERE v > 1e307` on a stored `Infinity`) — a false-miss the fully-pushed form cannot recover.

**Decision: accept and document, do not engineer around it in M2.** The case is unreachable through the connector's normal ingestion surface: ArangoDB cannot represent `Infinity`/`NaN` in JSON (it renders them as `(non-representable type double)`), so only a document written directly through a native-VelocyPack driver can hold one. Closing it would require making DOUBLE pushdown prefilter-only (like BIGINT range) with the pushed AQL re-admitting non-finites via explicit disjuncts — real complexity for a case no JSON writer can produce. It is recorded here and in `CLAUDE.md` as a known limitation; whether to engineer around it is deferred to a later milestone if a native-VPack ingestion path is ever added.

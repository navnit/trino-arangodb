# M5 — Aggregation pushdown: design

**Status:** design (approved in-session 2026-07-26).
**Milestone:** M5 — "`applyAggregation` → **single-split** COUNT/SUM/MIN/MAX/AVG + GROUP BY." Exit: *aggregates correct vs reference; aggregated handle = 1 split.*

Master spec §6.4/§6.5 fixes the *execution model* (single split, because Trino treats connector aggregation output as final). It does **not** address the *value model* — §6.4 never inherits §6.1's type-guard reasoning or §4.4's coercion contract. That gap is the substance of this design, and closing it is what decides which of the five aggregates are actually claimable.

---

## 1. Scope

**In scope:**
- `ArangoMetadata.applyAggregation` for `count`/`sum`/`min`/`max`/`avg` and single-grouping-set `GROUP BY`, restricted by the exactness matrix in §5.
- `ColumnGuard` — the AQL rendering of `ValueMaterializer`'s coercion, shared by aggregate inputs and grouping keys.
- `AqlBuilder.buildAggregate` — `FOR → FILTER → COLLECT/AGGREGATE → LIMIT → RETURN`.
- `ArangoTableHandle` aggregation descriptor; `ArangoSplitManager` single-split rule for aggregated handles.
- Guards on the existing hooks: `applyFilter` and `applyProjection` decline on an aggregated handle; `applyAggregation` declines when a limit is already pushed; `applyLimit` reports `limitGuaranteed = true` for an aggregated handle.
- New config `arangodb.aggregation-pushdown-enabled` (default `true`).
- Permanent pinning of the AQL semantics in §4 into `AqlSemanticsAssumptionsTest`.

**Out of scope (declined, not deferred bugs):**
- `DISTINCT` aggregates, `HAVING`, `GROUPING SETS`/`CUBE`/`ROLLUP`, ordered aggregates, aggregate `FILTER (WHERE ...)` — all decline per §6 (master spec §6.4 names DISTINCT and complex grouping forms explicitly).
- `sum`/`avg` over `BIGINT`, `min`/`max` over `VARCHAR` — declined on correctness grounds (§5), not scheduling grounds.
- Aggregates over `ARRAY`/`ROW`/`DECIMAL` columns, and any argument that is not a plain column reference.
- `getTableStatistics` (M6), `query()` passthrough (M6), dynamic filtering (M7).

---

## 2. Relationship to the precedent

The Trino **MongoDB connector does not implement `applyAggregation` at all** (verified against tag 483: `MongoMetadata` implements only `applyDelete`/`applyLimit`/`applyFilter`/`applyProjection`/`applyTableFunction`). So the usual precedent is silent here, and `DefaultJdbcMetadata.applyAggregation` is the reference for **SPI plumbing only** — synthetic output columns, `projections`/`assignments` construction, declining as a whole on the first unsupported aggregate. Its *semantics* transfer not at all: base-JDBC pushes into SQL engines with SQL's own null and numeric semantics, whereas AQL's aggregates differ from SQL's in ways measured in §4.

---

## 3. The core semantic (the spine)

M1's type-exactness invariant, carried to the aggregate:

> **A pushed aggregate must compute over exactly the values the read path would have emitted.** Every aggregate input and every grouping key is wrapped in an AQL guard that reproduces `ValueMaterializer` exactly: values the read path would materialize pass through unchanged; values it would read as `NULL` become AQL `null`, which AQL's aggregates ignore — matching Trino's own NULL handling.

The consequence is stricter than for filters: `applyFilter` can push a *prefilter* and let Trino re-check the residual (that is how `BIGINT` range works today). **Aggregation has no residual.** Trino replaces the aggregation node outright, so anything claimed must be exact on the first try — a wrong aggregate is simply a wrong answer. Every decline in §5 follows from that.

Unguarded, the divergence is not theoretical. Measured over a collection whose field holds mixed types (§4, probe 4):

| AQL, unguarded | returns | Trino (read path) |
|---|---|---|
| `MIN(d.v)` | `true` — a boolean | ignores it (`NULL`), min of the numbers |
| `MAX(d.v)` | `"x"` — a string | ignores it (`NULL`), max of the numbers |
| `SUM(d.v)` | `null` — one string poisons the whole sum | sum of the numbers |
| `COUNT(d.v)` | `14` — counts nulls and missing fields | `count(v)` counts non-null only |

AQL's total cross-type ordering (`null < bool < number < string < array < object`) is the same fact §6.1 already guards filters against; it reaches aggregates through `MIN`/`MAX`.

---

## 4. AQL semantics pinned empirically

Measured against **ArangoDB 3.12.4-3** (the version `TestingArangoServer` runs) on 2026-07-26, over documents inserted as raw JSON so stored values are genuine VelocyPack int64/uint64/double — not query-string literals. These findings are the evidence base for §5–§7 and become assertions in `AqlSemanticsAssumptionsTest`.

| # | Observation | Consequence |
|---|---|---|
| 1 | `COUNT` is an alias of `LENGTH`: `COUNT([1,null,2])` = `3` | Trino's `count(col)` **cannot** map to AQL `COUNT`; renders as `SUM(pred ? 1 : 0)` |
| 2 | `SUM` over a group with rows but zero non-null values = **`0`**; Trino's `sum` = `NULL` | `sum` needs a companion count and a `RETURN`-level null fix (§7) |
| 3 | `SUM` over **zero rows** (empty collection, global aggregation) = `null` | `count(col)` needs a `(x == null ? 0 : x)` wrap, or an empty table reports `NULL` instead of `0` |
| 4 | `AVERAGE`/`MIN`/`MAX` over all-null or zero rows = `null` | matches Trino; no fix needed |
| 5 | `SUM`/`MIN`/`MAX`/`AVERAGE` ignore `null` inputs | this is what makes the guard-to-`null` strategy work at all |
| 6 | Global `COLLECT AGGREGATE` over an empty collection still emits **exactly one row** | matches Trino's global-aggregation contract |
| 7 | `v == FLOOR(v)` is `false` for a stored int64 `2⁵³+1` (the C3 finding, already pinned) | bare `FLOOR` is unusable as an integrality test |
| 8 | `ABS(v) >= 9007199254740992 OR v == FLOOR(v)` is correct for every stored value tested | **BIGINT integrality *is* exactly expressible** — no double ≥ 2⁵³ can carry a fractional part, so above that threshold everything is integral |
| 9 | `9223372036854775807 < 9223372036854775808` is `true` | ArangoDB compares int64-vs-double by **exact mathematical value** (the fact behind C1), so the long-range bound is exact rather than double-approximate |
| 10 | Grouping on `-0.0` and `0.0` yields **two** groups; with `+ 0.0` applied, **one** | the existing C1 `+ 0.0` promotion is *required* for `DOUBLE` grouping keys, not merely nice — Trino normalizes `-0.0` to `0.0` when grouping |
| 11 | Grouping on a stored `null` and on a **missing** attribute yields one shared group | matches Trino (both are `NULL`) |
| 12 | Grouping on int `42` and double `42.0` yields one group; string `"42"` is separate. **This probe is vacuous on its own**: VelocyPack normalizes a stored `42.0` to int `42` at insert (`TO_STRING` returns `"42"`), so it never tested int-vs-double grouping — see §4/24 | matches both the `BIGINT` and `DOUBLE` read paths, but for the reason in §4/24 |
| 13 | `AGGREGATE c = LENGTH(1)` produces the **same `CollectNode` plan** as `COLLECT WITH COUNT INTO c` | `count(*)` needs no special case; one uniform code path (master spec §6.4's `COLLECT WITH COUNT INTO` form is an equivalent, not a requirement) |
| 14 | `SUM([1.797e308, 1.797e308])` = **`0`**, not `Infinity` | accepted limitation (§10) |
| 15 | `SUM([2⁵³+1, 1])` = `9007199254740992.0`; `SUM([int64_max, int64_max])` silently = `1.8446744073709552e19` | AQL sums in double space ⇒ `sum(BIGINT)` is not claimable (§5) |
| 16 | Guarded `MIN`/`MAX` over a `BIGINT` column return exact int64 (`9223372036854775807`, unrounded) | `min`/`max` on `BIGINT` **are** claimable |
| 17 | `COLLECT` with zero `AGGREGATE` terms is legal | pure `GROUP BY` / `DISTINCT` shapes render fine |
| 18 | **`COLLECT`'s two execution methods disagree.** Grouping `{int 0, double -0.0, double 0.0}` on a bare accessor gives **one** group under `method: "sorted"` and **two** under `method: "hash"`. The optimizer picks `hash` for M5's shape (`collectOptions: {method: "hash", fixed: true}`) | a bare `BIGINT` grouping key is a **live** wrong-row-count bug, not a theoretical one (§5, review finding B1); every grouping probe must be pinned under *both* methods |
| 19 | Normalizing the key as `(a == 0 ? 0 : a)` yields one group under **both** methods | the fix is method-independent, so no `OPTIONS { method: ... }` hint is needed |
| 20 | int64 `2⁵³+1` and double `2⁵³` stay **separate** groups under both methods | `BIGINT` grouping is exact at the boundary, as the read path requires |
| 21 | AQL `==` is **byte-exact**, not collation-based: `"ab" == "a­b"`, NFC `"é"` == NFD `"é"`, `"a" == "A"` are all `false`, and `COLLECT` keeps all seven test strings distinct under both methods | `VARCHAR` grouping keys are exact (review finding B2 refuted); independently confirms that **M2's already-shipped `VARCHAR` equality pushdown is sound** |
| 22 | Rounding is monotone in practice: `MAX([2⁵³+1, 2⁵³+3])` bare, then promoted on read, equals the promoted `MAX` | the `+ 0.0` promotion is **not** needed for `min`/`max(DOUBLE)` (§7) |
| 23 | `null > 0` is `false` | the `sum` empty-table fix-up `(aNn > 0 ? aN : null)` is load-bearing on this; pinned directly rather than derived from the ordering rule |
| 24 | **`COLLECT` groups by numeric value, not by stored representation.** int64 `9007199254740992` and double `9007199254740992.0` — verified genuinely distinct in storage, since `TO_STRING` keeps the `.0` at this magnitude where it does not at `42` — land in **one** group under both methods | a `BIGINT` grouping key needs no representation canonicalizer beyond the signed-zero normalization; without this measurement the §4/12 evidence was ambiguous, and the alternative outcome would have been B1 over again |

Probe 8's full guard, applied to a deliberately dirty column, keeps exactly `{42, 2⁵³+1, -(2⁵³+1), int64_max, int64_min, 0, 2⁵³-as-double}` and drops exactly `{42.5, -0.5, 1e19, -1e19, uint64_max, "x", true, null, missing}` — which is, value for value, `ValueMaterializer.isIntegralInLongRange`.

---

## 5. What is claimed (the exactness matrix)

| Trino aggregate | Input column type | Claimed? | Reason |
|---|---|---|---|
| `count()` (zero args) | — | **yes** | no input ⇒ no coercion surface |
| `count(col)` | `BOOLEAN`, `VARCHAR`, `BIGINT`, `DOUBLE` | **yes** | guard is a predicate only; ordering and precision never enter |
| `min` / `max` | `BIGINT`, `DOUBLE` | **yes** | guarded comparison is exact (§4/16); `+ 0.0` makes `DOUBLE` agree with `doubleValue()` |
| `min` / `max` | `VARCHAR` | **no** | string *ordering* depends on the server's collation configuration (`--icu-language`), whereas Trino orders by codepoint — §6.1's existing reason for declining string range. Note this is about ordering only: string *equality* was measured byte-exact (§4/21), which is why `VARCHAR` grouping keys are fine |
| `min` / `max` | `BOOLEAN` | **no** | legal in Trino and would be correct, but the value is negligible; kept out to hold the surface small |
| `sum` / `avg` | `DOUBLE` | **yes** | both sides accumulate in double; see §10 on associativity |
| `sum` / `avg` | `BIGINT` | **no** | AQL accumulates in double (§4/15): precision is lost past 2⁵³ and `sum(bigint)` overflow, which Trino raises on, is silent |
| any | `ARRAY`, `ROW`, `DECIMAL` | **no** | no exact AQL guard exists; consistent with `isPushable` never admitting them |
| **grouping key** | `BOOLEAN`, `VARCHAR`, `BIGINT`, `DOUBLE` | **yes** | grouping is equality-based and AQL equality is byte-exact (§4/21), so collation never enters; `VARCHAR` is fine here even though `min`/`max` on it is not. `BIGINT` keys require the signed-zero normalization in §7 |
| **grouping key** | `ARRAY`, `ROW`, `DECIMAL` | **no** | as above |

`avg(BIGINT)` returns `DOUBLE` in Trino but is declined for the §4/15 reason: AQL would compute the mean of double-rounded inputs.

**Zero-aggregate grouping is claimed.** `SELECT DISTINCT city FROM t` and a bare `GROUP BY city` arrive as `aggregates = []` with `groupingSets = [[city]]`. AQL allows `COLLECT` with no `AGGREGATE` terms (§4/17) and returns exactly the distinct groups, so these push whenever the grouping columns pass the matrix above. The degenerate `aggregates = []` **with** `groupingSets = [[]]` declines (§6/9) — it is a global aggregation with nothing to aggregate, which Trino knows produces exactly one row anyway.

---

## 6. Decline rules

`applyAggregation` returns `Optional.empty()` — all-or-nothing, matching base-JDBC's contract — when any of:

1. `arangodb.aggregation-pushdown-enabled = false`.
2. `arangodb.type-coercion = strict`. Same one-line rule and same reason as `isPushable`: a pushed aggregate silently swallows the type mismatch that strict mode exists to raise as `ARANGODB_TYPE_CONVERSION_ERROR`.
3. `handle.aggregation().isPresent()` — Trino may call the hook again; a second push would aggregate an aggregate.
4. `handle.limit().isPresent()` — `LIMIT n` then `GROUP BY` is not `GROUP BY` then `LIMIT n`, and the single-`FOR` AQL body can only express the latter. (Directly parallel to the existing `applyFilter` decline on a limited handle.)
5. `groupingSets.size() != 1` — no `GROUPING SETS`/`CUBE`/`ROLLUP`.
6. Any `AggregateFunction` with `isDistinct()`, a present `getFilter()`, or non-empty `getSortItems()`.
7. Any aggregate whose argument list is neither empty (`count(*)`) nor a single `Variable` resolving through `assignments` to an `ArangoColumnHandle`.
8. Any aggregate or grouping column failing the §5 matrix.
9. `aggregates.isEmpty() && groupingSets.equals([[]])` — a global aggregation with no aggregate functions. Nothing to compute; base-JDBC treats the same shape as unreachable.
10. **Any domain on the handle's constraint that is `isPrefilterOnly`** (`BIGINT` + non-discrete, `ArangoMetadata:300`). Such a constraint is enforced *jointly* by the pushed AQL and Trino's residual re-check; an aggregate computed on top of the AQL side alone would include rows — fractional or out-of-long-range values in the *filter* column — that the residual would have dropped. §10/8 argues the planner never offers this shape, but that argument depends on `PushAggregationIntoTableScan`'s pattern and could be invalidated by a Trino upgrade. This decline makes the guarantee local instead of planner-dependent, and costs one line.

Rule 8's matrix is keyed by function name, so `AggregatePushdown` carries an **explicit allowlist** of `count`/`sum`/`min`/`max`/`avg`; anything else (`approx_distinct`, `count_if`, `arbitrary`, `checksum`, …) declines by construction rather than by falling through a chain of `if`s.

**Reciprocal guards on the existing hooks.** Both are new declines on hooks that already exist, and both are easy to miss:

- **`applyFilter` must decline when `handle.aggregation().isPresent()`.** A filter arriving after aggregation is a `HAVING`; `AqlBuilder` renders pushed filters *before* `COLLECT`, so pushing it would silently convert `HAVING` into `WHERE`.
- **`applyProjection` must decline when `handle.aggregation().isPresent()`.** It currently declines such calls only incidentally — its `!progress` exit happens to fire because aggregate outputs and grouping keys are all scalars, so no `FieldDereference` can resolve against them. That is safety by coincidence; make it explicit so a later widening of the §5 matrix to structured grouping keys cannot silently turn it into a dereference pushed against a `COLLECT` variable.

**`applyLimit` on an aggregated handle is safe and exact.** A `LIMIT` after `COLLECT` on a single split is the final limit, so `limitGuaranteed` is `true` for aggregated handles (`!config.isShardParallelismEnabled() || handle.aggregation().isPresent()`).

---

## 7. AQL generation

`ColumnGuard` exposes two renderings per Trino type, both `Optional` (empty ⇒ decline):

| Type | `predicate(accessor)` | `value` for `sum`/`avg` (DOUBLE only) and grouping keys | `value` for `min`/`max` |
|---|---|---|---|
| `BOOLEAN` | `IS_BOOL(a)` | `a` | n/a (declined) |
| `VARCHAR` | `IS_STRING(a)` | `a` | n/a (declined) |
| `DOUBLE` | `IS_NUMBER(a)` | `(a + 0.0)` | `a` |
| `BIGINT` | `IS_NUMBER(a) AND a >= -9223372036854775808 AND a < 9223372036854775808 AND (ABS(a) >= 9007199254740992 OR a == FLOOR(a))` | `(a == 0 ? 0 : a)` | `a` |

with `coerce(a) = ((predicate) ? value : null)`. The `BIGINT` predicate is `isIntegralInLongRange` transliterated.

**Why `value` differs by context** — both divergences were found by review and confirmed by measurement:

- **`BIGINT` grouping keys must normalize signed zero.** A stored double `-0.0` passes the guard (it is fraction-free and in range) and the read path reads it as `0` via `longValue()`. But `COLLECT` under the `hash` method — which the optimizer *chooses* for M5's shape — puts `-0.0` in its own group (§4/18). Two AQL groups would then both materialize to key `0`, and since aggregation output is final, Trino would emit two rows for one group: a wrong row set. `(a == 0 ? 0 : a)` maps `-0.0` to integer `0` by exact numeric equality while leaving `2⁵³+1` untouched, and makes the result identical under both `COLLECT` methods (§4/19).
- **`min`/`max(DOUBLE)` must *not* promote.** `+ 0.0` turns a stored `-0.0` into `0.0`, so `min` would return `0.0` pushed and `-0.0` unpushed. The promotion is also unnecessary: double rounding is monotone, so the bare `MIN`/`MAX`, rounded when the read path applies `doubleValue()`, already equals Trino's min/max over the read values (§4/22). `sum`/`avg` keep the promotion — there the C1 argument applies unchanged, since summation is over the values themselves.

For `DOUBLE` grouping keys the promotion stays and is load-bearing in the opposite direction: it is what collapses `-0.0` and `0.0` into one group (§4/10), matching Trino's normalization.

Per-aggregate rendering, with `Aⁿ` the guard applied to that aggregate's input column:

| Trino | AQL `AGGREGATE` term | `RETURN` expression |
|---|---|---|
| `count()` | `aN = LENGTH(1)` | `aN` |
| `count(col)` | `aN = SUM(predicate ? 1 : 0)` | `(aN == null ? 0 : aN)` — §4/3 |
| `min(col)` | `aN = MIN(coerce)` | `aN` |
| `max(col)` | `aN = MAX(coerce)` | `aN` |
| `avg(col)` | `aN = AVERAGE(coerce)` | `aN` |
| `sum(col)` | `aN = SUM(coerce)`, `aNn = SUM(predicate ? 1 : 0)` | `(aNn > 0 ? aN : null)` — §4/2 |

The `AGGREGATE` terms come from the handle's `ArangoAggregation` descriptor, **not** from the requested column list: the `sum` companion counts are never requested columns, and Trino may prune aggregate outputs it does not need. The `RETURN` object is built from the requested columns instead, in their order, with each looked up against the descriptor. `buildAggregate` therefore does not reuse `buildScan`'s "project the requested columns" shape, and must not assume the two lists agree in length or order.

Grouping keys render as `COLLECT gN = coerce`. Synthetic AQL variable names (`g0…`, `a0…`) are used deliberately so a column name that isn't a legal AQL identifier — e.g. `applyProjection`'s nested `address$city` — never has to be one; the *object keys* in `RETURN` carry the real names, quoted, and are what `ArangoPageSource` looks up.

Clause order is `FOR → FILTER → COLLECT/AGGREGATE → LIMIT → RETURN`. Full shape (validated end-to-end against 3.12.4-3):

```aql
FOR d IN @@col
  FILTER IS_NUMBER(d["age"]) AND d["age"] >= @v0
  COLLECT g0 = ((IS_STRING(d["city"])) ? d["city"] : null)
  AGGREGATE a0 = LENGTH(1),
            a1 = SUM((IS_NUMBER(d["score"])) ? d["score"] + 0.0 : null),
            a1n = SUM((IS_NUMBER(d["score"])) ? 1 : 0)
  RETURN { "city": g0, "agg_0": a0, "agg_1": (a1n > 0 ? a1 : null) }
```

Aggregate output columns are named `agg_<ordinal>`; if that name collides with a grouping column's name, `_<n>` is appended with `n` incrementing from 1 until the name is unique within the query.

---

## 8. Components changed

| File | Change |
|---|---|
| `aggregation/ColumnGuard` (**new**) | `Optional<String> predicate(Type, String accessor)`, `String value(Type, String accessor)`, `Optional<String> coerce(...)`. The single place the read-path↔pushdown invariant lives. Container-free unit tests. |
| `aggregation/AggregateSpec` (**new**) | Record: aggregate kind, optional input `ArangoColumnHandle`, output column name, output `Type`. Jackson-serializable (it rides in the table handle). |
| `aggregation/ArangoAggregation` (**new**) | Record: `List<ArangoColumnHandle> groupingColumns`, `List<AggregateSpec> aggregates`. Presence on the handle *is* the `aggregated` flag — no separate boolean to keep in sync. |
| `aggregation/AggregatePushdown` (**new**) | The §5/§6 gate: `Optional<ArangoAggregation> plan(config, aggregates, assignments, groupingSets, handle)`. Pure, container-free. |
| `ArangoTableHandle` (modify) | New `Optional<ArangoAggregation> aggregation` component + `withAggregation`. |
| `ArangoMetadata` (modify) | `applyAggregation` implemented; `applyFilter` declines on an aggregated handle; `applyLimit`'s `limitGuaranteed` accounts for it. |
| `AqlBuilder` (modify) | `buildAggregate` path; `buildScan` unchanged for non-aggregated handles. |
| `ArangoSplitManager` (modify) | `handle.aggregation().isPresent()` ⇒ single split, checked **first** — before shard discovery, eligibility, and the count-sum capability probe, none of which should run for an aggregated scan. |
| `ArangoConfig` (modify) | `arangodb.aggregation-pushdown-enabled`, default `true`. |
| `README.md` / `CLAUDE.md` | Pushdown documentation; limitations updated. |
| Master spec §6.4 | Dated note that "all of COUNT/SUM/MIN/MAX/AVG are safe to push" is about re-aggregation only, pointing at §5 of this document for the value-coercion restriction that actually governs. The milestone table §10 needs no change — M5's row already reads correctly. |
| `config/checkstyle`, `config/spotbugs` | Nothing — new files are enforced, not grandfathered. |

**`ArangoPageSource` and `ValueMaterializer` are unchanged.** The page source already does `row.get(col.name())` and materializes by `col.type()`; an aggregate output is just an `ArangoColumnHandle` carrying `AggregateFunction.getOutputType()`, so the existing machinery reads it. `ArangoPageSourceProvider` is unchanged apart from dispatching to `buildAggregate`.

### 8.1 Spotless ratchet — a sequencing constraint, not a design choice

Six pre-existing files are modified (`ArangoTableHandle`, `ArangoMetadata`, `AqlBuilder`, `ArangoSplitManager`, `ArangoConfig`, plus their tests). Spotless is ratcheted `ratchetFrom=origin/master` and is **file-granular**: touching any line of a file puts the whole file under google-java-format AOSP, which is precisely why the hand-tuned M1–M3 source was left alone until now (`AqlBuilder` especially — its long explanatory comment lines and compact blocks will reflow). Adding a component to `ArangoTableHandle` additionally breaks every construction site, including `ArangoMetadataTest` (805 lines), `ArangoMetadataLimitTest`, `AqlBuilderTest`, and `ArangoConnectorPushdownTest`.

**Therefore the implementation plan opens with a formatting-only commit** so that the reviewable M5 diff is logic rather than two thousand lines of reindentation with edits buried in it. Doing this after the fact means rewriting the branch's history.

The mechanism matters: a bare `mvn spotless:apply` at the branch point is a **no-op**, because the ratchet restricts Spotless to files that differ from `origin/master` and at that moment none do. The step is therefore: temporarily neutralize `ratchetFrom` in `pom.xml`, run `spotless:apply` restricted to exactly the M5 files, restore `pom.xml`, and commit the reformatting alone. (`-DspotlessFiles` by itself does not help — the ratchet filter still applies on top of it.)

SPI result: `new AggregationApplicationResult<>(newHandle, projections, assignments, ImmutableMap.of(), false)` — the empty `groupingColumnMapping` because grouping columns keep their original handles (as base-JDBC does), and `precalculateStatistics = false` since M5 ships no statistics.

---

## 9. Interaction with M3 shard parallelism

An aggregated handle emits exactly one split (master spec §6.4: Trino treats connector aggregate output as final, so N splits would emit N duplicate "final" rows). ArangoDB still parallelizes the single AQL across its own shards internally, so database-side parallelism survives; only Trino-worker-level parallelism is traded away. The check goes first in `splitsFor` so an aggregated query never triggers `ShardFanoutCapability`'s count-sum probe — that probe costs a round trip per collection and is meaningless for a query that will not fan out.

---

## 10. Accepted limitations

1. **Double overflow reads as `0`.** `SUM`/`AVERAGE` over `DOUBLE` values whose total exceeds `DBL_MAX` returns `0` in AQL where Trino's `sum(double)` returns `Infinity` (§4/14). Root cause is the one already accepted in M2: JSON/VelocyPack cannot carry non-finite doubles. Requires summands within a few orders of magnitude of `1.8e308`. Documented, not closed.
2. **Floating-point associativity.** `sum`/`avg` over `DOUBLE` may differ in the last bits between the pushed and non-pushed plan, because summation order differs. Trino's own multi-split `sum(double)` is already non-deterministic this way; pushdown makes the result *deterministic but potentially different*, which is the same class of difference, not a new one.
3. **`min`/`max` on `VARCHAR`, `sum`/`avg` on `BIGINT` are never pushed** — Trino computes them, correctly, at full scan cost.
4. **No `HAVING` pushdown.** Post-aggregation filters are evaluated by Trino (§6, reciprocal guards).
5. **`min`/`max(DOUBLE)` over a column holding both `-0.0` and `0.0` may differ in the sign of the returned zero.** AQL's `MIN`/`MAX` treat them as equal (they compare equal) and keep whichever the plan reaches first, whereas Trino's comparison orders `-0.0` before `0.0`. The two results are equal under SQL `=`; only the printed sign differs. Declining `min`/`max(DOUBLE)` over this would cost far more than the defect is worth.
6. **`count(col)` is exact up to 2⁵³ non-null rows.** It renders as `SUM(pred ? 1 : 0)` and AQL accumulates sums in double (§4/15). This is the same argument that declined `sum(BIGINT)`; it is tolerated here only because the bound is 9 quadrillion rows in a single collection.
7. **`SELECT DISTINCT col ... LIMIT n` does not push.** Trino plans it as a `DistinctLimitNode`, which `PushAggregationIntoTableScan` does not match, so the zero-aggregate claim in §5 does not reach it. Missed optimization only.
8. **A residual filter can block aggregation pushdown entirely.** Trino's `PushAggregationIntoTableScan` matches `aggregation(tableScan())` and `aggregation(project(tableScan()))`; a residual predicate leaves a `FilterNode` in between. Because `BIGINT` range is deliberately *prefilter-only* — pushed to AQL **and** kept residual for Trino's re-check — `... WHERE bigint_col > 100 GROUP BY city` can fail to push its aggregate while the otherwise-identical `... WHERE double_col > 100 ...` (fully enforced, no residual) pushes. The asymmetry is inherited from M2's C2/C3 resolution, not introduced here, and it is a missed optimization rather than a wrong answer. §11 pins the actual behavior with a test so the documented asymmetry matches the planner rather than an assumption about it.

---

## 11. Testing

**Container-free units** — `ColumnGuardTest` (every type's rendering, and that unsupported types decline), `AggregatePushdownTest` (the full §5 matrix, all ten §6 decline rules, and that an unrecognized function name declines), `AqlBuilderAggregateTest` (rendered AQL string and bind vars for global, grouped, filtered, and limited shapes), `ArangoSplitManagerTest` (aggregated ⇒ one split, and the shard pipeline is not consulted — asserted via a client test double that fails if `getShardingInfo` is called), `ArangoMetadataTest` additions (the `applyFilter`-declines-on-aggregated and `limitGuaranteed` rules).

**`AqlSemanticsAssumptionsTest` additions** — every row of §4 pinned as an assertion against a live container, so a future ArangoDB upgrade that changes any of them fails loudly rather than silently changing results. This is the same instrument that pinned C1/C3 and is what keeps this design honest. Two rules for these additions:

- **Every grouping-related assertion runs under both `OPTIONS { method: "hash" }` and `OPTIONS { method: "sorted" }` and asserts they agree.** §4/18 is the proof that this matters: the two methods genuinely disagree on a bare accessor, and the optimizer's choice is not ours to make. A grouping rendering that passes under only one method is not correct.
- The signed-zero, `2⁵³`-boundary, and byte-exact-string cases (§4/18–21) are pinned explicitly, not left implied by the numeric-friendly cases — that gap is what review finding B1 exploited.

**Correctness ITs (`ArangoConnectorAggregationTest`, via `DistributedQueryRunner`)** — the decisive tests, over a deliberately dirty collection where each column holds a mix of matching, mismatched, null, and absent values, including int64 beyond 2⁵³:
- every claimed aggregate returns **identical results with pushdown enabled and disabled** (`arangodb.aggregation-pushdown-enabled` toggled), which is the reference comparison the milestone's exit criterion asks for;
- `isFullyPushedDown()` for claimed shapes; `isNotFullyPushedDown()` for each declined one;
- empty-table global aggregation returns one row with `count = 0` and `sum = NULL` (§4/3, §4/6);
- a group whose values are all type-mismatched returns `sum = NULL`, `count = 0` (§4/2);
- `GROUP BY` on a `DOUBLE` column containing `-0.0`, `0.0`, and `0` yields a single group (§4/10);
- **`GROUP BY` on a `BIGINT` column containing `-0.0`, `0.0`, and `0` yields a single group** (§4/18–19 — the review-finding-B1 regression test), and one containing int64 `2⁵³+1` alongside double `2⁵³` yields two (§4/20);
- `min`/`max` on a `DOUBLE` column containing int64 values beyond 2⁵³ agree pushed and unpushed (§4/22);
- `count(1)` — pinning whether Trino 483 canonicalizes a constant argument to zero-arg `count`, since rule 7 otherwise declines a common query shape (if it does not canonicalize, widening rule 7 to accept a non-null `Constant` is a two-line follow-up);
- `GROUP BY` where the column has stored-null and absent values yields one shared `NULL` group (§4/11);
- `SELECT DISTINCT col` and bare `GROUP BY col` (zero aggregates) push and return the right groups;
- **the residual-filter interaction (§10/8)**: an aggregate over a `DOUBLE`-range predicate and the same aggregate over a `BIGINT`-range predicate, asserting each one's actual pushdown status, so §10/8's text is pinned to observed planner behavior. This test is written early — before the rest of the IT suite — because if the `BIGINT` case does not push, every later `isFullyPushedDown()` assertion combining a filter with an aggregate has to be written accordingly.

---

## 12. Decisions recorded (for review-gate attention)

1. **`BIGINT` is claimable for `min`/`max`/`count` and grouping** on the strength of §4/8–9, which is a *new* result — the existing code comments (and `AqlSemanticsAssumptionsTest:49-54`) state only that bare `FLOOR` is unusable, which remains true. The compound guard is the part worth reviewing hardest.
2. **`sum`/`avg` are `DOUBLE`-only.** Reviewers who expect master spec §6.4's "all of COUNT/SUM/MIN/MAX/AVG are safe to push" should note that §6.4 is reasoning solely about partial/final re-aggregation, not about value coercion.
3. **`count(col)` renders as `SUM(pred ? 1 : 0)` wrapped against `null`**, because AQL `COUNT` is `LENGTH` (§4/1) and AQL `SUM` over zero rows is `null` (§4/3).
4. **`sum` carries a companion count** solely to convert AQL's `0` into SQL's `NULL` (§4/2).
5. **Strict coercion declines all aggregation pushdown**, mirroring `isPushable`.
6. **`applyFilter` gains a decline on aggregated handles** — a behavior change to an existing hook, motivated by `HAVING`.
7. **Double-overflow-to-`0` is accepted** (§10/1) rather than closed by declining `sum`/`avg`; decided in-session 2026-07-26.
8. **`applyProjection` gains an explicit decline on aggregated handles** — today it declines only by coincidence (§6).
9. **The branch opens with a formatting-only commit** (§8.1). This is the first milestone to modify ratcheted M1–M3 files, so the ratchet's cost is paid once, visibly, and separately from the logic diff.
10. **Zero-aggregate grouping (`DISTINCT`, bare `GROUP BY`) is claimed** (§5), on the strength of §4/17.
11. **`BIGINT` grouping keys normalize signed zero; `min`/`max(DOUBLE)` drop the `+ 0.0` promotion** (§7). Both came from review findings (B1, S1) and both are now measured (§4/18–19, §4/22). B1 was a live wrong-row-count defect under the `COLLECT` method the optimizer actually picks.
12. **`VARCHAR` grouping keys survived review by measurement, not argument** (§4/21): AQL `==` and `COLLECT` are byte-exact, so ICU collation does not reach grouping. The same measurement independently confirms that M2's shipped `VARCHAR` equality pushdown is sound — worth noting because a contrary result would have been a defect in released behavior, not just in this design.
13. **Aggregation declines over a prefilter-only constraint** (§6/10), making the §10/8 safety property local rather than dependent on Trino's planner shape.

---

## Appendix A — Verified SPI surface (`trino-spi` 483, checked via `javap` 2026-07-26)

```java
// ConnectorMetadata
default Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
        ConnectorSession session,
        ConnectorTableHandle handle,
        List<AggregateFunction> aggregates,
        Map<String, ColumnHandle> assignments,
        List<List<ColumnHandle>> groupingSets)

// io.trino.spi.connector.AggregationApplicationResult  (note: connector package, not expression)
AggregationApplicationResult(T handle,
                             List<ConnectorExpression> projections,
                             List<Assignment> assignments,
                             Map<ColumnHandle, ColumnHandle> groupingColumnMapping,
                             boolean precalculateStatistics)

// io.trino.spi.connector.AggregateFunction
String getFunctionName(); Type getOutputType(); List<ConnectorExpression> getArguments();
List<SortItem> getSortItems(); boolean isDistinct(); Optional<ConnectorExpression> getFilter();
```

Global aggregation arrives as `groupingSets = [[]]`; function names arrive lowercase (`count`, `sum`, `min`, `max`, `avg`).

## Appendix B — Probe provenance

The §4 table was produced on 2026-07-26 in three rounds against `arangodb/arangodb:3.12` (reported `3.12.4-3`, community) via the HTTP `/_api/cursor` and `/_api/explain` endpoints, with fixture documents inserted as raw JSON so that int64/uint64/double storage types are genuine rather than query-string literals. Every row is reproduced as an assertion in `AqlSemanticsAssumptionsTest` by this milestone; the transcript itself is not checked in, since the test is the durable artifact.

Rounds 1–2 (§4/1–17) established the aggregate-function semantics and validated the generated query shapes end to end. Round 3 (§4/18–23) was driven by an adversarial design review and targeted the half of the exactness claim that rounds 1–2 had established by *inference* rather than measurement — `COLLECT` grouping equality at the signed-zero and 2⁵³ boundaries, string equality against ICU-collation-edge pairs, and the `+ 0.0` promotion's effect on `min`/`max`. That round found one live defect (B1) and refuted one suspected defect (B2), which is the argument for extending the probe matrix rather than reasoning about it.

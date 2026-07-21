# M4 — Structured-type value materialization: design

**Status:** design (approved in-session 2026-07-21).
**Milestone:** M4 — "ARRAY/ROW/DECIMAL value materialization; `checkMaterializable` removed." Exit: *`SELECT` of any inferred column returns correct values; leaf-level mismatch semantics proven under both coercion modes.*

> **Milestone renumbering.** The master spec's milestone table (`2026-07-18-arangodb-trino-connector-design.md` §10) did not list structured materialization anywhere — it was an M1 implementation deferral (`checkMaterializable` punted on M1's "`SELECT *` returns correct rows/types" exit criterion), and "M4" in that table meant aggregation pushdown. This milestone is inserted as the new **M4** and the table renumbered (aggregation → M5, schema sources → M6, hardening → M7), with a dated note in the table itself. Earlier dated docs (e.g. the M3 design's "dynamic filtering lands in M6") reference pre-renumber numbers and are left untouched.

---

## 1. Scope

**In scope:**
- Recursive value materialization for `ARRAY`, `ROW`, and `DECIMAL(38,0)` columns — every type `TypeMapper` can infer becomes readable.
- Deletion of `ArangoPageSourceProvider.checkMaterializable` (and tests asserting its `NOT_SUPPORTED`).
- Extraction of all value coercion (existing scalar branches + new structured branches) from `ArangoPageSource.appendValue` into a new single-purpose `ValueMaterializer` class.
- README / CLAUDE.md limitation-text updates; master-spec milestone-table renumbering (see note above).

**Out of scope (unchanged by this milestone):**
- The `JSON` mixed-type strategy (config value exists, still not wired — later milestone).
- Schema cache TTL, writes, aggregation pushdown.
- `resolveDereference`'s decline of structured leaves (behavior kept; only its now-stale comment is updated — see §6).
- Filter pushdown: `isPushable` never admits structured/DECIMAL columns; nothing to change.

---

## 2. Relationship to the precedent (Trino MongoDB connector)

The MongoDB connector materializes arrays/rows recursively inside `MongoPageSource` (`writeBlock` → per-element/per-field recursion into `ArrayBlockBuilder`/`RowBlockBuilder` entries, missing row fields as null). M4 adopts that recursion shape and its leaf-level-null leniency, but **diverges structurally**: the logic lives in a dedicated `ValueMaterializer` class rather than inline in the page source. The MongoDB page source is ~500 lines mixing paging and coercion; this codebase's pattern is small single-purpose units (`AqlBuilder`, `TypeMapper`, the `split` package), and extraction makes the coercion algorithm unit-testable without a cursor or container.

---

## 3. The core semantic (the spine)

M1's type-exactness invariant, extended one level down, recursively:

> **A mismatch is handled at the exact depth it occurs.** Under `lenient`, only the offending element/field reads as `NULL` — surrounding good data survives (`[1, "oops", 3]` under `ARRAY(BIGINT)` reads `[1, NULL, 3]`). Under `strict`, a mismatch at any depth raises `ARANGODB_TYPE_CONVERSION_ERROR`, naming the column and the path to the leaf.

A *structural* mismatch (an `ARRAY` column holding a scalar, a `ROW` column holding a list) is simply a mismatch at that level: lenient nulls that whole value, strict raises. An **absent** ROW field is `NULL`, never a mismatch — identical to how a missing top-level field reads today (`row.get(name) == null` → `appendNull`), because the inferred schema is a union across sampled documents and absence is normal. Likewise a **stored `null`** at any depth (`[1, null, 3]`, a null row field) is `NULL` in both modes, never a mismatch — the null-check-first rule of the moved scalar code applies recursively; strict mode raises only on genuine type mismatches.

Pushed filters cannot disagree with any of this: `isPushable` only ever pushes `BOOLEAN`/`VARCHAR`/`BIGINT`/`DOUBLE` top-level columns, so structured values are never compared server-side.

---

## 4. Components changed

| File | Change |
|---|---|
| `type/ValueMaterializer` (**new**) | All value coercion. Public surface: `ValueMaterializer(TypeCoercion)`, `writeValue(BlockBuilder, Type, Object, String columnName)`. Scalar branches move verbatim from `ArangoPageSource.appendValue` (incl. `isIntegralInLongRange`, `truncateForError`); structured branches added per §5. Lives in `io.arango.trino.type` as `TypeMapper`'s read-side dual: TypeMapper maps runtime values → Trino types at inference, ValueMaterializer maps (type, runtime value) → block writes at read. |
| `ArangoPageSource` (modify) | Shrinks to pure cursor/paging: constructs `new ValueMaterializer(coercion)`, the row loop delegates to `writeValue`. Public constructor signature unchanged. |
| `ArangoPageSourceProvider` (modify) | `checkMaterializable` + its call + comment deleted. |
| `ArangoMetadata` (comment only) | `resolveDereference`'s comment says structured leaves decline "consistent with checkMaterializable's rejection" — reworded: they decline because a structured leaf can't be a pushdown target, and Trino now evaluates such dereferences over the materialized parent column (correct either way). |
| `README.md` / `CLAUDE.md` | Limitations no longer list ARRAY/ROW/DECIMAL materialization; read-path docs describe recursive leaf-level semantics. Specifically stale and easy to miss: CLAUDE.md read-path item 7's "The structured-type branch in `appendValue` is dead in practice because `checkMaterializable` already rejects those columns upstream" (the branch becomes live and `checkMaterializable` is gone), and the equivalent code comment at `ArangoPageSource.java:91-92` (dies with the extraction). |
| Master spec §10 | Milestone table renumbered (see header note). |

No change to `AqlBuilder` (the `RETURN {"col": d["col"]}` projection already ships full nested values), `ArangoSplitManager`, `SchemaResolver`, or `TypeMapper`. The driver deserializes nested JSON into the same `Map`/`List`/`Integer`/`Long`/`Double`/`BigInteger` shapes `TypeMapper.inferType` sampled, so inference and read agree by construction — no new client surface.

---

## 5. `ValueMaterializer` branches

Dispatch on (Trino type, runtime value), recursing for containers:

| Column type | Accepted runtime value | Write |
|---|---|---|
| `BOOLEAN` / `BIGINT` / `DOUBLE` / `VARCHAR` | unchanged (moved verbatim) | unchanged |
| `ARRAY(T)` | `List<?>` | `((ArrayBlockBuilder) out).buildEntry(eb -> ...)`, recursing on `(eb, T, element)` per element (private recursive method; the public 4-arg `writeValue` is the entry point) |
| `ROW(f₁..fₙ)` | `Map<?,?>` | `((RowBlockBuilder) out).buildEntry(fbs -> ...)`, iterating the RowType's fields **in field order**, recursing on `map.get(fieldName)`; absent key → `appendNull` (not a mismatch, §3); document keys not in the row type are ignored |
| `DECIMAL(38,0)` | any integral number (§5.1) | `type.writeObject(out, Int128.valueOf(bigInteger))` |
| anything else | — | mismatch: lenient → `appendNull`, strict → raise (§7) |

`RowType` field names are always present (inference builds them from document keys; `RowType.field(name, type)`), so `getName().orElseThrow()` is safe — same assumption `mergeRows` already makes.

The recursive helper re-enters the **full** dispatch table — scalar, DECIMAL, and container branches alike — not just the container branches. `TypeMapper.inferType` recurses element/field types, so `ARRAY(DECIMAL(38,0))` and `ROW(x DECIMAL(38,0))` are reachable schemas and their leaves must materialize through the same DECIMAL/scalar rules as top-level columns.

### 5.1 DECIMAL acceptance

`TypeMapper` emits exactly one decimal type — `DECIMAL(38,0)`, always a **long decimal** (precision 38 > 18), from a `BigInteger` wider than 63 bits or a `bigint + decimal` merge. Because the merge is what usually creates the column, most stored values under it are plain `Long`s and must read back. Mirroring `BIGINT`'s integral rule:

- `Long` / `Integer` / `Short` / `Byte` → `Int128.valueOf(longValue)`.
- `BigInteger` → accepted iff `!Decimals.overflows(bi, 38)` (the `BigInteger` overload — checking *before* `Int128.valueOf`, which throws past 128 bits); wider is a mismatch (defensive only — JSON tops out at uint64's 20 digits; VelocyPack-native writers could exceed it).
- `Double` / `Float` → accepted iff finite and integral (`rint` test), converted via `BigDecimal.valueOf(d).toBigIntegerExact()` and then gated on `!Decimals.overflows(bigInteger, 38)` exactly like the `BigInteger` bullet; fractional, non-finite, or overflowing is a mismatch. **Do not reuse `isIntegralInLongRange`'s signed-64-bit bound here** (Opus review B1): it would wrongly null integral doubles in `[2⁶³, 10³⁸)` — e.g. `1e19`, precisely the uint64-range class a `DECIMAL(38,0)` column exists to hold — and without the overflow gate an integral double ≥ 10³⁸ would crash lenient mode via `Int128.valueOf`'s `ArithmeticException` instead of reading as `NULL`. `!overflows(bi, 38)` ⇒ `|bi| < 10³⁸ <` Int128 max, so one gate closes both failure modes.

Scale is always 0, so no rescaling arises.

---

## 6. Interactions with existing pushdown

- **`applyFilter` / `isPushable`:** untouched. Structured and DECIMAL columns were never pushable; all their predicates remain residual, evaluated by Trino over now-materialized values.
- **`applyProjection` / `resolveDereference`:** behavior untouched. A dereference bottoming at a structured leaf still declines pushdown; post-M4 Trino evaluates it over the materialized parent — correct, just unoptimized. Relaxing this (projecting structured leaves server-side) is a possible later optimization, deliberately not in scope. Comment updated only.
- **`applyLimit` / splits:** orthogonal; a materialized structured column changes nothing about split semantics.

---

## 7. Error handling & observability

- **Lenient:** leaf-level `appendNull`.
- Both modes maintain a segment deque (`ArrayDeque<String>`: pushed before descending into `[i]` / `.field`, popped after — cheap, no per-value allocation in steady state); it is rendered into a path string only when strict mode raises.
- **Strict:** raise at the mismatch site. Error shape: `Column 'a': value at a[2].b expected BIGINT but a document held "oops" of type String`, reusing `truncateForError`'s 100-char cap. Same `ARANGODB_TYPE_CONVERSION_ERROR` code as today.
- A top-level scalar mismatch keeps today's message shape (no path suffix) so existing strict-mode expectations stay recognizable.

---

## 8. Testing

**`ValueMaterializerTest` (pure unit, no container):** build blocks directly and read them back via the Trino `Type` API.
- Scalar parity: every branch that moved, incl. `42.0`→`BIGINT 42`, fractional/out-of-range mismatches — behavior identical to pre-M4 `appendValue`.
- Arrays: homogeneous, empty, nested (`ARRAY(ARRAY(BIGINT))`), leaf mismatch → `[1, NULL, 3]` lenient / raise strict.
- Rows: full match, absent field → `NULL` in **both** modes, extra document keys ignored, nested row-in-array-in-row.
- Structural mismatches: scalar under ARRAY column, list under ROW column — whole-value null lenient / raise strict.
- DECIMAL: `Long`, uint64 `BigInteger` (the real trigger case), integral double, fractional double mismatch, >38-digit `BigInteger` mismatch; boundary cases guarding B1 — an integral double in `[2⁶³, 10³⁸)` (e.g. `1e19`) **reads back**, and an integral double ≥ 10³⁸ (e.g. `1e39`) is a clean mismatch (lenient `NULL`, no `ArithmeticException`).
- Nested DECIMAL and scalar leaves: `ARRAY(DECIMAL(38,0))` and `ROW(x DECIMAL(38,0))` materialize through the same dispatch (§5).
- Stored `null` at depth: `[1, null, 3]` under `ARRAY(BIGINT)` and a null ROW field read as `NULL` in **both** modes — strict does not raise (§3).
- Strict paths: assert the exact path rendering at depth (`a[2].b`).

**`ArangoConnectorQueryTest` additions (end-to-end SQL, real container):**
- `SELECT` an array column, a row column, and a uint64-created DECIMAL column; assert values.
- Heterogeneous documents → leaf-level nulls visible through SQL.
- Row-field dereference (`SELECT address.city`) still pushed and correct (regression guard on §6).
- Strict mode: nested mismatch fails the query with `ARANGODB_TYPE_CONVERSION_ERROR`.

**Removed:** `ArangoPageSourceProviderTest` cases asserting `NOT_SUPPORTED` for structured columns (replaced by positive materialization coverage).

---

## 9. Decisions recorded (for review-gate attention)

1. **Leaf-level null over whole-value null** (user choice "A", 2026-07-21): preserves good data, mirrors per-column lenient semantics one level down, matches the MongoDB precedent.
2. **Extraction over in-place extension** (user-approved): one extra file buys direct unit tests and keeps `ArangoPageSource` at pure paging.
3. **Milestone renumbering** (header note): structured materialization becomes M4; the master table's M4–M6 shift to M5–M7.
4. **Absent ROW field is never a strict-mode error** — consistency with top-level absence; the union schema makes absence routine, and raising would make strict mode unusable on any non-uniform collection.

---

## Appendix A — Verified SPI surface (`trino-spi` 476, checked via javap 2026-07-21)

- `ArrayBlockBuilder.buildEntry(ArrayValueBuilder<E>)`; callback `ArrayValueBuilder.build(BlockBuilder)`.
- `RowBlockBuilder.buildEntry(RowValueBuilder<E>)`; callback `RowValueBuilder.build(List<BlockBuilder>)`.
- `Int128.valueOf(BigInteger)` / `Int128.valueOf(long)`.
- `Decimals.overflows(BigInteger, int precision)` (also an `(Int128, int)` overload; the `BigInteger` one is used so the check precedes `Int128.valueOf`, which throws past 128 bits).
- Long-decimal write path: `Type.writeObject(BlockBuilder, Object)` — there is **no** `Int128`-typed overload on `DecimalType`; the package-private `LongDecimalType` implementation casts the `Object` to `Int128` at runtime (precision 38 > `Decimals.MAX_SHORT_PRECISION` = 18, so DECIMAL(38,0) is always the long-decimal representation).

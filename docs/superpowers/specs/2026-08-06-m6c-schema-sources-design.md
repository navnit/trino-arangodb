# M6-C — Schema sources: `trino_schema` override reader + declared-only types: design

**Milestone:** M6-C (third M6 slice; A = statistics ✅, B = passthrough ✅). Master-spec anchor: §4 "Schema Resolution", precedence source 1 (explicit override collection) and the schema-declared rows of the §3.2 type table.

**Exit criteria:** an override doc in `trino_schema` fully determines a table's user columns (precedence over sampling honored, provably no sampling call); declared `decimal(p,s)` / `timestamp(3)` / `timestamp(3) with time zone` columns materialize correct values under both coercion modes; filters and aggregates over the new types provably stay in Trino.

---

## 1. Scope

**In scope:**

- `SchemaOverrideReader` — reads and validates override docs from the `arangodb.schema-collection` collection (default `trino_schema`), consulted by `SchemaResolver.resolveColumns` before sampling.
- **Replace semantics**: a matching override doc is the *complete* user-column set for that table; sampling does not run for it.
- New declarable scalar types and their read-path encode/decode contract (§5): `decimal(p,s)` (arbitrary precision/scale, short **and** long decimals), `timestamp(3)` (ISO-8601 local string), `timestamp(3) with time zone` (ISO-8601 string with `Z`/offset) — recursively nestable inside `array(...)`/`row(...)`.
- `TypeManager` bound into the injector (from `ConnectorContext` at factory time) to parse declared type strings, gated by a recursive allowlist.
- New error code `ARANGODB_SCHEMA_ERROR(2, USER_ERROR)` in `ArangoErrorCode` (the override doc is user-authored input; the existing enum has only `ARANGODB_TYPE_CONVERSION_ERROR(0)` and `ARANGODB_QUERY_NOT_READ_ONLY(1)`).
- Config: `arangodb.schema-collection` (default `trino_schema`).

**Out of scope (deferred, with reasons):**

- **Validation-rule hint+merge** (native collection JSON Schema, master-spec precedence source 2) — separable slice; sampling remains the only inference source.
- **`path` nested flattening** on override fields — touches `AqlBuilder` column addressing and the RETURN clause; deferred. The key is *rejected explicitly* ("not yet supported") rather than ignored, so docs written for a future version fail loudly today. **Consequence, stated plainly:** until `path` lands, a declared `name` is simultaneously the Trino column name and the exact, case-sensitive ArangoDB attribute name — an override cannot rename or re-case a field (e.g. it cannot expose attribute `placedAt` as column `placed_at`).
- **Epoch-millis timestamps** — an epoch-millis `timestamp(3)` is the *same Trino type* as an ISO-string `timestamp(3)`, and `ValueMaterializer` dispatches on type; distinguishing them requires a per-column (and per-nested-leaf) decode hint plumbed through `ArangoColumnHandle`. Deferred until wanted.
- Writes to the override collection (the connector only ever reads it); case-insensitive name matching (M7).

---

## 2. Precedent

Trino's MongoDB connector (`_schema` collection): same doc shape (`table` + `fields[{name,type,hidden}]`), same replace semantics (a schema doc is authoritative; no sampling), types parsed via the engine's type infrastructure. Deviations, both from the master spec: the collection default is `trino_schema` not `_schema` (ArangoDB reserves the `_` namespace for system collections and requires `isSystem:true` to create them), and validation is strict/fail-loud rather than lenient (§3).

---

## 3. Override contract

One doc per table in the `arangodb.schema-collection` collection of the *same database* as the described table:

```json
{ "table": "orders",
  "fields": [
    { "name": "total",     "type": "decimal(12,2)" },
    { "name": "placed_at", "type": "timestamp(3) with time zone", "hidden": false }
  ] }
```

- `table` — required, non-empty string: the described collection's name.
- `fields` — required, non-empty array of field objects:
  - `name` — required, non-empty string; must not start with `_` (system attributes are appended automatically; the `_` namespace is reserved); duplicates rejected **case-insensitively** (`Locale.ENGLISH`): Trino resolves column identifiers case-insensitively against connector-supplied names, so `Total` and `total` would be two columns the engine cannot tell apart at query time (`SELECT total` → ambiguous), even though `getColumnHandles`'s map would accept both.
  - `type` — required string; parsed by `TypeManager.fromSqlType`, then checked against the recursive allowlist (§5).
  - `hidden` — optional boolean, default `false`.
- System attributes are appended by `SchemaResolver` exactly as on the sampling path: `_key`/`_id`/`_rev` hidden `VARCHAR` always; `_from`/`_to` visible `VARCHAR` when the collection is an edge collection.
- The doc's own ArangoDB system attributes (`_key`/`_id`/`_rev`) are ignored, not treated as unknown keys.

**Strict validation — fail loudly.** The collection is user-curated; a typo must never silently change a schema. `ARANGODB_SCHEMA_ERROR` (message naming the table and offending field/key, with guidance) on: missing/wrong-typed `table`/`fields`/`name`/`type`/`hidden`; empty `fields`; unparseable or non-allowlisted `type` (§5); `_`-prefixed or (case-insensitively) duplicate `name`; **any unrecognized key** in the doc or a field object (this is what catches `"hiden": true`, and what rejects `path` as "not yet supported"); and **two or more docs claiming the same table** (ambiguity is never resolved silently).

**Case sensitivity of nested row fields.** SQL identifiers inside a type string are lowercased by the parser unless double-quoted: `row(placedAt timestamp(3))` parses as field `placedat`, which then never matches the stored attribute `placedAt` — an all-`NULL` column with no error (see the limitation below). Users must quote to preserve case: `row("placedAt" timestamp(3))`. The connector does **not** attempt to detect the unquoted-case mistake — that would require re-parsing the raw type string alongside `TypeManager` (a second parser, which TypeManager adoption exists to avoid). Instead: the behavior is documented here and in the README, and §8 pins the parser's canonicalization with a test so it can never change silently.

**Accepted limitation — declared names are not validated against stored documents.** The override exists to avoid sampling, so the connector cannot know which attributes actually exist. A misspelled `name` (or an unquoted-case nested row field) therefore yields an all-`NULL` column in **both** coercion modes — a stored ArangoDB "attribute absent" arrives as AQL `null`, and `ValueMaterializer` short-circuits `null` to SQL `NULL` before any mismatch check (`ValueMaterializer.write`), so `strict` cannot catch it either. Pinned by test (§8), documented in the README.

---

## 4. Components and data flow

### 4.1 `SchemaOverrideReader` (new, `io.arango.trino.schema`)

`Optional<List<ArangoColumn>> read(String database, String collection)`.

**Existence probe first.** The common deployment has no override collection at all; issuing the AQL unconditionally would make the *default* configuration an exception path (a failed round-trip + caught `ArangoDBException` per table per TTL, on the planning path). The reader therefore first checks that the override collection exists (cheap collection-metadata call via `ArangoClient`), cached per database in a small Guava cache with `expireAfterWrite(arangodb.schema.cache-ttl)` — the same TTL that governs schema staleness generally. Absent → `Optional.empty()` at DEBUG, no AQL.

When the collection exists, one targeted AQL through a new `ArangoClient` method:

```aql
FOR d IN @@sc FILTER d.table == @t LIMIT 2 RETURN d
```

`LIMIT 2` exists to make duplicates detectable. The reader owns all contract validation (§3) and type parsing/allowlisting (§5). README recommends a persistent index on `table` (without one, a no-match lookup scans the whole collection; bounded in practice — one doc per table — but the index makes it O(1)).

### 4.2 `SchemaResolver` precedence

`resolveColumns` consults the reader first; a present result is returned (plus system attributes); `Optional.empty()` falls through to today's sampling path unchanged. Overrides therefore slot in at exactly one point, and everything downstream (handles, pushdown, page source) sees only resolved Trino types — no knowledge of the schema's source leaks anywhere else.

### 4.3 Caching

Resolution already happens inside `ArangoMetadata.resolve`'s Guava cache (`expireAfterWrite(arangodb.schema.cache-ttl)`, default 5m). Override lookups ride that cache: at most one existence probe (per database) + one AQL fetch (per table) per TTL, and a query sees one stable schema. The probe cache in §4.1 is the only new cache.

### 4.4 `TypeManager` binding

`ArangoConnectorFactory.create` has `ConnectorContext`; bind `context.getTypeManager()` as an instance binding in the bootstrap (standard connector pattern). `SchemaOverrideReader` takes it via constructor injection.

### 4.5 Error translation (consolidated)

| Condition | Behavior |
|---|---|
| Override collection absent (existence probe; or server error 1203 from the AQL, a race window after the probe) or no doc for the table | Fall through to sampling, log DEBUG — the ordinary "no overrides configured" state |
| Malformed doc / bad type / duplicate docs (§3) | `ARANGODB_SCHEMA_ERROR` + guidance, raised lazily when the table is resolved — `SHOW TABLES` never fails on it (master spec §4.2; see §8 for the `information_schema.columns` caveat) |
| Insufficient permission on the override collection | **Fail loud**, but with a diagnostic message naming `arangodb.schema-collection` and saying "grant read on this collection (or drop it)" — a permission gap would otherwise surface as an opaque internal error on *every* table in the catalog. The implementation task verifies the actual ArangoDB error number for collection-level-forbidden against a real server and matches on it for the tailored message; it still rethrows (a *stable* misconfiguration should be fixed, not silently degraded around — degrading would nondeterministically flip column types between override and sampled on the day someone fixes the grant) |
| Any other failure reading the override collection | Rethrow as `GENERIC_INTERNAL_ERROR` — unlike M6-A statistics, schema is load-bearing |
| Stored value vs declared type mismatch at read time | Existing per-cell coercion policy, unchanged: `lenient` → `NULL`, `strict` → `ARANGODB_TYPE_CONVERSION_ERROR` with leaf path |

The 1203-vs-else split mirrors `isDatabaseNotFound` (1228): "legitimately doesn't exist" degrades, everything else fails loudly. The 1203-for-missing-collection assumption (specifically for a **bind-parameter** collection reference `@@sc`, a shape M6-B's literal-reference queries never exercised) is pinned by a container-backed assumption test (§8), not just assumed in a hand-written double.

---

## 5. Type vocabulary and the encode/decode contract

Declared type strings parse via `TypeManager.fromSqlType`, then a **recursive allowlist** admits only what `ValueMaterializer` can materialize exactly. Everything else → `ARANGODB_SCHEMA_ERROR` naming the field and stating the supported vocabulary.

**Catching parse failures.** `fromSqlType` failures surface as the *engine's* Guava `UncheckedExecutionException` wrapping either `io.trino.sql.parser.ParsingException` (syntactically invalid, e.g. `decimal(`) or `io.trino.spi.type.TypeNotFoundException` (valid syntax, unknown type). Neither can be caught by name from plugin code: `ParsingException` lives in `trino-parser`, outside the plugin-visible package set, and the wrapper is loaded by the engine's classloader, so it does not match the plugin's own bundled Guava class. The reader therefore wraps `fromSqlType` in `catch (RuntimeException e)` and re-raises `ARANGODB_SCHEMA_ERROR` naming the field and the offending string.

**Allowlist admission is by resolved `Type` object**, so alias spellings that resolve to an admitted type are admitted: bare `timestamp` (→ `timestamp(3)`, Trino's default precision), bare `decimal` (→ `decimal(38,0)`), and `timestamp(3) without time zone` (→ `timestamp(3)`) all pass. Documented rather than rejected — rejecting them would mean string-matching the raw spelling, i.e. the second parser §3 declines to build.

**Structural rules for `row(...)`** (checked recursively at every level): every field must be **named** (`fromSqlType("row(varchar)")` parses to an anonymous field, which would crash `ValueMaterializer`'s `field.getName().orElseThrow()` at scan time), non-empty, and free of case-insensitive duplicates (`fromSqlType` happily accepts `row(a varchar, a bigint)`). Violations → `ARANGODB_SCHEMA_ERROR`.

### 5.1 Per-type contract

Every conversion below is bounds-checked *before* writing; any failed check is an ordinary **per-cell mismatch** (lenient → `NULL`, strict → `ARANGODB_TYPE_CONVERSION_ERROR` with leaf path) — never an escaping exception. This is the §4.4-coercion-contract obligation: under `lenient`, no stored value may fail the query.

| Declared type | Contract |
|---|---|
| `boolean` / `bigint` / `double` / `varchar` (unbounded) | Exactly as these types read today — no behavior change. |
| `decimal(p,s)` | **Matching encodings:** a JSON number or a decimal string. A number converts via `new BigDecimal(double)` — the double's **exact binary value** (deliberately *not* `BigDecimal.valueOf`: `valueOf` takes the shortest round-trip representation, which would silently diverge from `ValueMaterializer`'s documented "read exactly what's stored" invariant — its `integralValueOf` rejects `valueOf` for precisely this reason, and since `ValueMaterializer` dispatches on `Type` alone, a declared `decimal(38,0)` *is* the sampled `DECIMAL(38,0)`; one code path, one exactness rule). Consequence, stated honestly: a stored double matches only when its exact binary value fits scale `s` — `0.25` at `s=2` matches, `12.34` does **not** (`new BigDecimal(12.34)` = `12.3399…`), integral doubles match at any `s`. **Strings are the real decimal path** — ArangoDB has no decimal type, so high-precision values are stored as strings; that is the point of declaring a decimal. A string converts via `new BigDecimal(String)` (accepts scientific notation and leading `+`; surrounding whitespace → `NumberFormatException` → mismatch). Then, for both encodings: `setScale(s)` with **no rounding** (inexact fit at `s` → mismatch), and precision gate `Decimals.overflows(unscaled, p)` → mismatch. **Write:** `p ≤ 18` is a **short** decimal → `writeLong(unscaled.longValueExact())`; `p > 18` → `writeObject(Int128.valueOf(unscaled))`. (The existing code writes Int128 unconditionally, correct only because inference emits `DECIMAL(38,0)` exclusively — `ShortDecimalType` does not implement `writeObject`, so the spec's own `decimal(12,2)` example would throw without the dual write.) A stored non-finite double (`Infinity`/`NaN`, native-VelocyPack-only) is a mismatch. Booleans, objects, arrays: mismatch. |
| `timestamp(3)` | **Matching encoding:** an ISO-8601 **local** date-time string, exactly what `LocalDateTime.parse` (`ISO_LOCAL_DATE_TIME`) accepts. Fractional seconds finer than millis → **mismatch, not rounded** (rounding would silently disagree with the declared precision). A string with `Z`/offset → mismatch (belongs under `with time zone`). **Write:** Trino's short `timestamp(3)` stores **epoch *micro*seconds** (not millis): `writeLong(epochSecond * 1_000_000 + nano / 1_000)`, computed with `Math.multiplyExact`/`addExact` — `LocalDateTime.parse` accepts extended years (`+999999999-…`) whose epoch-micros overflow a `long`, and that overflow must land as a mismatch, not a wrapped value or `ArithmeticException`. |
| `timestamp(3) with time zone` | **Matching encoding:** ISO-8601 date-time string **with** `Z`/offset, exactly what `OffsetDateTime.parse` (`ISO_OFFSET_DATE_TIME`) accepts. Same finer-than-millis rule; a local (offset-less) string → mismatch. **Write:** `packDateTimeWithZone(utcMillis, timeZoneKey)` — Trino's `timestamp(3) with time zone` is the short packed form (`MAX_SHORT_PRECISION = 3`), preserving the stored offset via its zone key. **Bounds checked before packing, each → mismatch:** offset not a whole minute (`ISO_OFFSET_DATE_TIME` accepts second-precision offsets like `+05:30:15`; `TimeZoneKey.getTimeZoneKeyForOffset` throws on them), offset outside ±14:00 (parser accepts up to ±18:00; Trino does not), and UTC millis outside the 52-bit packed range (parser accepts far-future years; `pack` throws "Millis overflow"). |
| `array(e)` / `row("f" t, ...)` | Recursive over this same table — `array(timestamp(3))`, `row("amount" decimal(10,2))` etc. `ValueMaterializer` already recurses; it gains the new leaves. |

**Rejected in v1** (each with a message saying so): timestamp precisions other than 3, **bounded** `varchar(n)` (rejected via `VarcharType::isUnbounded`, not `instanceof` — `instanceof` alone would admit `varchar(10)`, and the existing varchar write path writes slices unchecked against the bound) and `char`, `date`, `time`, `real`/`integer`/`smallint`/`tinyint`, `json`, `uuid`, `varbinary`, and any other parseable-but-unlisted type.

**Parser acceptance surface is pinned, not discovered** (§8): `LocalDateTime.parse` accepts lowercase `t` and missing seconds but rejects a space separator; `new BigDecimal(String)` accepts `1E+2` and `+12.34` but rejects whitespace. These by-reference acceptance decisions get an explicit accept/reject test matrix so a JDK change can never silently move the contract.

---

## 6. Pushdown interaction — nothing to change, by construction

- **Filter pushdown:** `isPushable` allowlists `BOOLEAN`/`VARCHAR`/`BIGINT`/`DOUBLE` only → any predicate over a new type is automatically residual (Trino evaluates it post-read).
- **Aggregation pushdown:** `AggregatePushdown.specFor` gates `count(col)` on `ColumnGuard.predicate` and `min`/`max`/`sum`/`avg` on its own type allowlists; grouping keys route through the same `ColumnGuard.predicate`, which returns `Optional.empty()` for anything not `BOOLEAN`/`VARCHAR`/`DOUBLE`/`BIGINT`. So `min(ts)`, `sum(dec)`, `count(ts)`, and `GROUP BY ts` all decline — four *separate* code paths, each pinned by test (§8), not assumed.
- A table whose columns resolve to already-pushable types *through an override* pushes down identically to a sampled table: pushdown keys off resolved Trino types, never the schema's source. §8 proves this positively (`count(*)` and `GROUP BY varchar_col` on an override-declared table still push).
- `applyLimit`, splits, statistics (M6-A): unaffected — the handle shape doesn't change.

---

## 7. Config

| Key | Default | Meaning |
|---|---|---|
| `arangodb.schema-collection` | `trino_schema` | Name of the per-database override collection the connector reads. No kill switch: an absent collection *is* the disabled state, detected by a per-database existence probe cached for `arangodb.schema.cache-ttl` — so the no-override deployment costs one metadata call per database per TTL, not a failed AQL per table. |

`ArangoConfigTest` gets the standard default + explicit-mapping rows (house style: every config key is pinned there).

---

## 8. Testing

- **`AqlSchemaOverrideAssumptionsTest`** (container-backed, house pattern of `AqlSemanticsAssumptionsTest`): `FOR d IN @@sc FILTER d.table == @t LIMIT 2 RETURN d` against an absent collection yields `errorNum == 1203` — pinning the bind-parameter-collection error shape §4.5 relies on; plus the parser-surface matrix from §5 (lowercase `t`, missing seconds, space separator, `1E+2`, whitespace decimal string, sub-minute offset, ±18:00 offset, far-future year) — accept/reject each, so the by-reference contracts are pinned against JDK/Trino drift.
- **`SchemaOverrideReaderTest`** (hand-written `ArangoClient` test double, no container — house style for metadata error paths). The `TypeManager` is real, not a double: `fromSqlType` is not implementable from SPI, and `trino-testing` (already test-scope) provides `InternalTypeManager` over a `TypeRegistry` — name this construction so the implementer doesn't reach for a container. Covers: the full §3 validation matrix (every rejection incl. unknown-key/`path`/`hiden` typo, case-insensitive duplicate names, `_`-prefix, empty fields, duplicate docs), `hidden` defaulting, row structural rules (anonymous field, duplicate row fields), syntactically-invalid (`decimal(`) vs unknown (`not_a_type`) type strings both → `ARANGODB_SCHEMA_ERROR`, existence-probe absent → empty, 1203 race → empty, permission-error → tailored loud failure, other exception → `GENERIC_INTERNAL_ERROR`, row-field case canonicalization (`row(myField varchar)` → field `myfield`; quoted preserves).
- **Type allowlist tests**: accepted vocabulary incl. nested forms and alias spellings (bare `timestamp`/`decimal`, `without time zone`); each rejected family with its message; `varchar(10)` rejected specifically.
- **`ValueMaterializerTest` additions**: new leaves under both coercion modes — short (`decimal(12,2)`) *and* long (`decimal(38,0)`, `decimal(20,4)`) decimal writes; exact-binary-fit doubles (`0.25` at `s=2` matches, `12.34` mismatches); integral doubles at any scale; string decimals incl. scientific notation; precision-overflow and inexact-scale mismatches; ISO local/offset parsing; local↔offset cross-mismatches; finer-than-millis mismatch; sub-minute offset, out-of-range offset, year-overflow → mismatch (never an exception, in either mode — the lenient path is the regression trap); nested `array(timestamp(3))` / `row("amount" decimal(10,2))` with leaf-path errors under `strict`; declared-name-matches-nothing → all-`NULL` in both modes (pins the §3 accepted limitation).
- **`SchemaResolver` precedence test**: override present → **zero** `sampleDocuments` calls (counting test double); absent → sampling result identical to today.
- **`ArangoConnectorQueryTest` e2e** (container): seed a `trino_schema` collection; `SELECT` decimal/timestamp columns with value assertions; hidden column absent from `SELECT *` but selectable by name; malformed doc → error on query of *that* table, `SHOW TABLES` still lists everything, **and** `SELECT * FROM information_schema.columns WHERE table_schema = ...` / `SHOW COLUMNS FROM <other_table>` behavior while the malformed doc exists is asserted — the connector doesn't override `streamRelationColumns`, so the engine's fallback determines whether one bad doc poisons schema-wide column enumeration; if it does, that's recorded as an accepted deviation from master-spec §4.2 (or handled, decided at implementation with the observed behavior in hand). `EXPLAIN`-proofs: timestamp/decimal filter residual; `min(timestamp)`, `sum(decimal)`, `count(timestamp)`, `GROUP BY timestamp` all decline; `count(*)` and `GROUP BY varchar` on the *same* override table still push. A table without an override doc behaves exactly as before while the collection exists. Decision: `trino_schema` itself is an ordinary non-system collection and **remains listed and queryable as a table** — hiding it would be magic behavior, and querying it is useful for debugging.
- **Docs tasks**: README config row + an "override collection" usage section (doc shape, type vocabulary, quoting rule for nested fields, index recommendation, misspelled-name limitation — first user-authored-input feature, so it needs real user docs); CLAUDE.md (precedence step in `SchemaResolver` §, `ARANGODB_SCHEMA_ERROR`, declared-type read semantics, config keys). NOTICE unchanged (nothing relocated).

---

## 9. Review log

**Opus 5 adversarial review, 2026-08-06 — 21 findings (7 blocker / 9 should-fix / 5 minor), all integrated:**

- **B1 decimal exactness**: spec's `BigDecimal.valueOf` contradicted `ValueMaterializer.integralValueOf`'s documented `new BigDecimal(d)` invariant (one type-dispatched code path — can't differ for declared vs inferred `decimal(38,0)`). Resolved: exact-binary-value rule everywhere; doubles match only on exact fit; strings are the primary decimal encoding (§5.1).
- **B2 short decimals**: `p ≤ 18` → `ShortDecimalType`, which lacks `writeObject`; dual write specified.
- **B3 row-field case-folding**: unquoted names lowercase into all-`NULL` columns; quoting documented + canonicalization pinned by test (detection rejected — needs a second parser).
- **B4 anonymous/duplicate row fields**: parser accepts both; structural rejection added to the allowlist.
- **B5/B6 timestamp bounds**: sub-minute/out-of-range offsets, 52-bit pack overflow, epoch-micros (not millis) encoding and its `long` overflow — all specified as bounds-checked per-cell mismatches, never escaping exceptions.
- **B7 parse-failure exception classes**: engine-classloader Guava wrapper + plugin-invisible `ParsingException` → `catch (RuntimeException)` specified.
- **S8/S21 cost**: existence probe (per-database, TTL-cached) replaces the always-throw AQL path; index recommendation added.
- **S9 permissions**: loud failure with tailored diagnostic naming the config key; actual error number verified against a container at implementation.
- **S10 1203 pinning**: container-backed assumption test for the `@@sc` bind-parameter shape.
- **S11** `ARANGODB_SCHEMA_ERROR(2)` added to scope. **S12** alias spellings + `isUnbounded` documented. **S13** all-`NULL` misspelled-name limitation documented + pinned. **S14** case-insensitive duplicate names. **S15** no-rename consequence of deferring `path` stated. **S16** `information_schema.columns`-with-malformed-doc e2e assertion added.
- **M17-M21**: parser-surface pin tests, `InternalTypeManager` construction named for unit tests, `ArangoConfigTest`/README/CLAUDE.md tasks, four extra pushdown-decline EXPLAIN cases + two positive pushes, index recommendation.

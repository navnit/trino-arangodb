# M6-C — Schema sources: `trino_schema` override reader + declared-only types: design

**Milestone:** M6-C (third M6 slice; A = statistics ✅, B = passthrough ✅). Master-spec anchor: §4 "Schema Resolution", precedence source 1 (explicit override collection) and the schema-declared rows of the §3.2 type table.

**Exit criteria:** an override doc in `trino_schema` fully determines a table's user columns (precedence over sampling honored, provably no sampling call); declared `decimal(p,s)` / `timestamp(3)` / `timestamp(3) with time zone` columns materialize correct values under both coercion modes; filters and aggregates over the new types provably stay in Trino.

---

## 1. Scope

**In scope:**

- `SchemaOverrideReader` — reads and validates override docs from the `arangodb.schema-collection` collection (default `trino_schema`), consulted by `SchemaResolver.resolveColumns` before sampling.
- **Replace semantics**: a matching override doc is the *complete* user-column set for that table; sampling does not run for it.
- New declarable scalar types and their read-path materialization: `decimal(p,s)` (arbitrary precision/scale), `timestamp(3)` (ISO-8601 local string), `timestamp(3) with time zone` (ISO-8601 string with `Z`/offset) — recursively nestable inside `array(...)`/`row(...)`.
- `TypeManager` bound into the injector (from `ConnectorContext` at factory time) to parse declared type strings, gated by a recursive allowlist.
- Config: `arangodb.schema-collection` (default `trino_schema`).

**Out of scope (deferred, with reasons):**

- **Validation-rule hint+merge** (native collection JSON Schema, master-spec precedence source 2) — separable slice; sampling remains the only inference source.
- **`path` nested flattening** on override fields — touches `AqlBuilder` column addressing and the RETURN clause; deferred. The key is *rejected explicitly* ("not yet supported") rather than ignored, so docs written for a future version fail loudly today.
- **Epoch-millis timestamps** — an epoch-millis `timestamp(3)` is the *same Trino type* as an ISO-string `timestamp(3)`, and `ValueMaterializer` dispatches on type; distinguishing them requires a per-column (and per-nested-leaf) decode hint plumbed through `ArangoColumnHandle`. Deferred until wanted.
- Writes to the override collection (the connector only ever reads it); case-insensitive name matching (M7).

---

## 2. Precedent

Trino's MongoDB connector (`_schema` collection): same doc shape (`table` + `fields[{name,type,hidden}]`), same replace semantics (a schema doc is authoritative; no sampling), types parsed via the engine's type infrastructure. Deviations, both from the master spec: the collection default is `trino_schema` not `_schema` (ArangoDB reserves the `_` namespace for system collections and requires `isSystem:true` to create them), and validation is strict/fail-loud rather than lenient (§4).

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
  - `name` — required, non-empty string; must not start with `_` (system attributes are appended automatically; the `_` namespace is reserved); duplicates rejected.
  - `type` — required string; parsed by `TypeManager.fromSqlType`, then checked against the recursive allowlist (§5).
  - `hidden` — optional boolean, default `false`.
- System attributes are appended by `SchemaResolver` exactly as on the sampling path: `_key`/`_id`/`_rev` hidden `VARCHAR` always; `_from`/`_to` visible `VARCHAR` when the collection is an edge collection.
- The doc's own ArangoDB system attributes (`_key`/`_id`/`_rev`) are ignored, not treated as unknown keys.

**Strict validation — fail loudly.** The collection is user-curated; a typo must never silently change a schema. `ARANGODB_SCHEMA_ERROR` (message naming the table and offending field/key, with guidance) on: missing/wrong-typed `table`/`fields`/`name`/`type`/`hidden`; empty `fields`; unparseable or non-allowlisted `type`; `_`-prefixed or duplicate `name`; **any unrecognized key** in the doc or a field object (this is what catches `"hiden": true`, and what rejects `path` as "not yet supported"); and **two or more docs claiming the same table** (ambiguity is never resolved silently).

---

## 4. Components and data flow

### 4.1 `SchemaOverrideReader` (new, `io.arango.trino.schema`)

`Optional<List<ArangoColumn>> read(String database, String collection)`. Fetches via one targeted AQL through a new `ArangoClient` method:

```aql
FOR d IN @@sc FILTER d.table == @t LIMIT 2 RETURN d
```

`LIMIT 2` exists to make duplicates detectable. The reader owns all contract validation (§3) and type parsing/allowlisting (§5). It has no cache of its own (§4.3).

### 4.2 `SchemaResolver` precedence

`resolveColumns` consults the reader first; a present result is returned (plus system attributes); `Optional.empty()` falls through to today's sampling path unchanged. Overrides therefore slot in at exactly one point, and everything downstream (handles, pushdown, page source) sees only resolved Trino types — no knowledge of the schema's source leaks anywhere else.

### 4.3 Caching

Resolution already happens inside `ArangoMetadata.resolve`'s Guava cache (`expireAfterWrite(arangodb.schema.cache-ttl)`, default 5m). Override lookups ride that cache: one AQL fetch per table per TTL, and a query sees one stable schema. No second cache.

### 4.4 `TypeManager` binding

`ArangoConnectorFactory.create` has `ConnectorContext`; bind `context.getTypeManager()` as an instance binding in the bootstrap (standard connector pattern). `SchemaOverrideReader` takes it via constructor injection.

### 4.5 Error translation (consolidated)

| Condition | Behavior |
|---|---|
| Override collection absent (server error 1203) or no doc for the table | Fall through to sampling, log DEBUG — this is the ordinary "no overrides configured" state |
| Malformed doc / bad type / duplicate docs (§3) | `ARANGODB_SCHEMA_ERROR` + guidance, raised lazily when the table is resolved — `SHOW TABLES` never fails on it (master spec §4.2) |
| Any other failure reading the override collection | Rethrow as `GENERIC_INTERNAL_ERROR` — unlike M6-A statistics, schema is load-bearing, and degrading to sampling on a transient error would nondeterministically flip a table's column types between override and sampled |
| Stored value vs declared type mismatch at read time | Existing per-cell coercion policy, unchanged: `lenient` → `NULL`, `strict` → `ARANGODB_TYPE_CONVERSION_ERROR` with leaf path |

The 1203-vs-else split mirrors `isDatabaseNotFound` (1228): "legitimately doesn't exist" degrades, everything else fails loudly.

---

## 5. Type vocabulary and read semantics

Declared type strings parse via `TypeManager.fromSqlType`, then a **recursive allowlist** admits only what `ValueMaterializer` can materialize exactly. Everything else → `ARANGODB_SCHEMA_ERROR` naming the field and stating the supported vocabulary.

| Declared type | Matching stored encoding (anything else = ordinary per-cell mismatch) |
|---|---|
| `boolean` / `bigint` / `double` / `varchar` | Exactly as these types read today — no behavior change |
| `decimal(p,s)`, any valid `p,s` | A JSON number **or** a decimal string, converted exactly: `BigDecimal.valueOf(double)` / `new BigDecimal(String)`, then `setScale(s)` with **no rounding** — an inexact fit at scale `s`, or a value overflowing precision `p`, is a mismatch. Strings are admitted deliberately: ArangoDB has no decimal type, so strings are how high-precision values are actually stored — they are the point of declaring a decimal. Integral `Long`/`BigInteger` values are likewise exact-converted. |
| `timestamp(3)` | ISO-8601 **local** date-time string, exactly what `LocalDateTime.parse` (`DateTimeFormatter.ISO_LOCAL_DATE_TIME`) accepts (e.g. `2026-08-05T12:34:56.789`). Fractional seconds finer than millis are a **mismatch, not rounded** — rounding would silently disagree with the declared precision. A string carrying `Z`/offset is a mismatch (it belongs under the `with time zone` type). |
| `timestamp(3) with time zone` | ISO-8601 date-time string **with** `Z`/offset, exactly what `OffsetDateTime.parse` (`DateTimeFormatter.ISO_OFFSET_DATE_TIME`) accepts. Same finer-than-millis rule; same local-vs-offset cross-mismatch (a local string is a mismatch here). Materializes as Trino packed millis + zone key preserving the stored offset. |
| `array(e)` / `row(f t, ...)` | Recursive over this same table — `array(timestamp(3))`, `row(amount decimal(10,2))` etc. `ValueMaterializer` already recurses; it gains the new leaves. |

**Rejected in v1** (each with a message saying so): timestamp precisions other than 3, bounded `varchar(n)`/`char`, `date`, `time`, `real`/`integer`/`smallint`/`tinyint`, `json`, `uuid`, `varbinary`, and any other parseable-but-unlisted type.

Notes:

- A declared `decimal(38,0)` behaves identically to the sampled-inference `DECIMAL(38,0)`: with `s=0`, "no fractional part" falls out of the no-rounding rule, and the existing `Decimals.overflows(..., 38)` gate is the `p` check.
- The decimal dual encoding (number **and** string) is a deliberate, documented exception to single-encoding type-exactness, confined to `decimal(p,s)`: both encodings convert *exactly* or mismatch, so the pushed-filter/read-path agreement argument is unaffected (decimal is not pushable anyway, §6).
- `BigDecimal.valueOf(double)` uses the double's shortest decimal representation (`Double.toString`), so a stored `12.34` declared `decimal(10,2)` reads as `12.34`, not `12.339999...`. A stored non-finite double (`Infinity`/`NaN`, native-VelocyPack-only, cf. the M2 known limitation) is a mismatch.

---

## 6. Pushdown interaction — nothing to change, by construction

- **Filter pushdown:** `isPushable` allowlists `BOOLEAN`/`VARCHAR`/`BIGINT`/`DOUBLE` only → any predicate over a new type is automatically residual (Trino evaluates it post-read).
- **Aggregation pushdown:** `AggregatePushdown.plan`/`ColumnGuard` are likewise allowlists → `min(placed_at)`, `sum(total)` etc. decline and compute in Trino.
- Both declines are **proven by tests** (§8), not assumed.
- A table whose columns resolve to already-pushable types *through an override* pushes down identically to a sampled table: pushdown keys off resolved Trino types, never the schema's source. This is the payoff of overrides slotting in only at `resolveColumns`.
- `applyLimit`, splits, statistics (M6-A): unaffected — the handle shape doesn't change.

---

## 7. Config

| Key | Default | Meaning |
|---|---|---|
| `arangodb.schema-collection` | `trino_schema` | Name of the per-database override collection the connector reads. No kill switch: an absent collection *is* the disabled state (1203 → sampling), and the lookup is one cheap AQL per table per schema-cache TTL. |

---

## 8. Testing

- **`SchemaOverrideReaderTest`** (hand-written `ArangoClient` test double, no container — house style for metadata error paths): full §3 validation matrix (every rejection incl. unknown-key/`path`/`hiden` typo cases, duplicate docs), `hidden` defaulting, 1203 → empty, other-exception → `GENERIC_INTERNAL_ERROR`.
- **Type allowlist tests**: accepted vocabulary incl. nested forms; each rejected family with its message.
- **`ValueMaterializerTest` additions**: new leaves under both coercion modes — exact decimal fits (number and string), no-rounding and precision-overflow mismatches, ISO local/offset parsing, local↔offset cross-mismatches, finer-than-millis mismatch, nested `array(timestamp(3))` / `row(... decimal ...)` leaf-path errors under `strict`.
- **`SchemaResolver` precedence test**: override present → **zero** `sampleDocuments` calls (counting test double); absent → sampling result identical to today.
- **`ArangoConnectorQueryTest` e2e** (container): seed a `trino_schema` collection; `SELECT` decimal/timestamp columns with value assertions; hidden column absent from `SELECT *` but selectable by name; malformed doc → error on query, but `SHOW TABLES` still lists the table; `EXPLAIN`-based proof that a timestamp/decimal filter stays residual and `sum(decimal)`/`min(timestamp)` do not push down; a table without an override doc behaves exactly as before while the override collection exists. Decision: `trino_schema` itself is an ordinary non-system collection and **remains listed and queryable as a table** — hiding it would be magic behavior, and querying it is useful for debugging.

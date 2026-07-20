# M3 — Shard-parallel splits: design

**Status:** design (elaborates the pre-reviewed §5.1 / §5.4 of `2026-07-18-arangodb-trino-connector-design.md`; does not re-decide them).
**Milestone:** M3 — "Shard discovery; `shardIds` execution; version pin + capability check + CI count-sum gate + single-split fallback; SmartGraph exclusion." Exit: *N shards ⇒ N splits, counts sum exactly; fallback proven.*

This document elaborates the shard-parallel split design already fixed in the master spec §5.1 (review blocker **B3**) and §5.4. Where the master spec pins a decision, this document conforms and cross-references it by number rather than re-deciding. Two genuine deviations from the reviewed spec are called out explicitly in §11.

---

## 1. Scope

**In scope (this milestone):**
- Master spec **§5.1** — cluster shard-parallel split generation.
- Master spec **§5.4** — the shard-splitting config knobs (`shards-per-split`, `max-splits`), plus one new kill-switch (§11).

**Out of scope (deferred, do not implement in M3):**
- §5.2 `_key`-range splitting for single-server (config-gated, off by default) — later, if ever.
- §5.3 dynamic filtering — that lands in **M6** ("best-effort dynamic filtering") per the milestone table.
- Aggregation single-split rule (§6.4) — **M4**. M3 touches only *non-aggregated* scans; aggregated/`query()` handles already emit exactly one split and M3 leaves that untouched.

---

## 2. Relationship to the precedent (Trino MongoDB connector)

The MongoDB connector — the named precedent for this whole design — does **not** fan out per shard. `MongoSplitManager.getSplits` emits a single `MongoSplit` (wrapped in a `FixedSplitSource`) carrying one server address, even against a sharded MongoDB cluster; any parallelism comes only from downstream readers, not from split-level fan-out.

M3 is a deliberate improvement over that baseline: a sharded ArangoDB collection is scanned by **N Trino workers in parallel**, one split per shard-group. Two concrete divergences:
- **Fan-out vs. single split.** Our §5.1 *fallback* (single whole-collection split) is exactly the MongoDB connector's steady-state behavior — i.e. we are never worse than the precedent, and better when the allowlist gate passes.
- **No routing address.** `MongoSplit` carries a server address; `ArangoSplit` deliberately carries **none** (master spec §72). ArangoDB per-shard reads go through the coordinator with the `shardIds` query option, not by addressing a dbserver directly.

---

## 3. The correctness invariant (the spine)

Everything below serves one invariant:

> **`∪(scan restricted to each split's shardIds) == full collection scan`** — every document read by exactly one split: no gaps, no duplicates.

`shardIds` is an **internal ArangoDB API** (master spec §5.1): battle-tested (ArangoDB's own Spark datasource reads per shard this way) but documented "use at your own risk." The failure mode is severe and *silent*: if a server ignores the option, **every split runs the full query → N× duplication**. So the design is fail-safe by construction — it fans out only when it can positively confirm the invariant holds, and otherwise falls back to the correct-but-serial single split. The five required mitigations (§8) exist solely to guarantee-or-refuse this invariant.

---

## 4. Components changed

| File | Change |
|---|---|
| `handle/ArangoSplit` | `record ArangoSplit(List<String> shardIds)` — empty list ⇒ whole-collection (fallback). Remains Jackson-serializable (shipped to workers); `getRetainedSizeInBytes` accounts for the list. |
| `ArangoSplitManager` | Real discovery: read sharding properties → allowlist gate (§6) → enumerate shard IDs → group into splits (§7). Otherwise one empty-`shardIds` split. |
| `client/ArangoClient` | `+ getShardingInfo(db, coll)` (collection properties); `+ listShardIds(db, coll)` (raw `execute(Request)`); `+ query(db, aql, bindVars, List<String> shardIds)` overload; `+ serverVersion()` for the version pin; `+ shardScopedCount(...)` for the capability probe. |
| `ArangoPageSourceProvider` | If `split.shardIds()` non-empty → pass `AqlQueryOptions().shardIds(...)`; else current path. |
| `aql/AqlBuilder` | **Unchanged.** Shard scoping is a query *option*, not an AQL-string change — clean separation from pushdown. |
| `ArangoConfig` | `+ arangodb.shards-per-split` (default 1), `+ arangodb.max-splits` (safety cap), `+ arangodb.shard-parallelism-enabled` (default true — new; see §11). |

---

## 5. Shard discovery (`ArangoClient`)

Two calls, both against the coordinator, both confirmed present in `arangodb-java-driver` 7.13.0 (`core` module):

1. **Sharding properties** — `arango.db(db).collection(coll).getProperties()` → `CollectionPropertiesEntity`, giving `getNumberOfShards()` (`Integer`), `getShardingStrategy()` (**`String`** internal name), `getShardKeys()`, `getReplicationFactor()`, `getSmartJoinAttribute()`.
2. **Shard-ID enumeration** — there is **no** clean `ArangoCollection.shards()` / `getShardIds()` in 7.13.0 (`getResponsibleShard` is per-document and useless here). Enumerate via a raw request: `arango.execute(Request<…> GET /_api/collection/{coll}/shards, Response<…>)` (database-scoped) → `{ "shards": [ "s100001", … ] }`. This endpoint is coordinator/cluster-only; on a single server it errors → fallback (§8.4).

---

## 6. The allowlist gate

Fan out **only** when *all* of the following hold; on any failure or thrown exception, fall back to a single split and log WARN (§8.4). This is an allowlist (positively confirm safe), not a denylist — an unknown or future sharding strategy falls back rather than risking silent duplication.

1. `arangodb.shard-parallelism-enabled == true` (§11 kill-switch).
2. `numberOfShards != null && numberOfShards > 1`. (Also transparently excludes single-server, Enterprise **OneShard** DBs, and **Satellite** collections — all of which are single-shard.)
3. `shardingStrategy` ∈ the non-smart hash set, built from **verified enum constants** `{ShardingStrategy.HASH, COMMUNITY_COMPAT, ENTERPRISE_COMPAT}` compared via `getInternalName()` against the returned string. The three excluded constants (`ENTERPRISE_HASH_SMART_EDGE`, `ENTERPRISE_HEX_SMART_VERTEX`, `ENTERPRISE_SMART_EDGE_COMPAT`) are the SmartGraph strategies — this discharges mitigation §8.5.
4. `smartJoinAttribute == null` (excludes SmartJoin collections).
5. Version pin satisfied (§8.1) and capability probe passed (§8.2).
6. Enumeration (§5.2) returns ≥ 1 shard.

> **Judgment call (surface at review):** the allowlist includes all three non-smart hash strategies, including the legacy `ENTERPRISE_COMPAT`, because each is one-shard-per-document hash sharding. The active capability probe (§8.2) and the CI count-sum gate (§9) are the hard guarantees regardless of strategy; the allowlist is defense-in-depth.

---

## 7. Grouping shards into splits

Let `N` = shard count, `S` = `arangodb.shards-per-split` (default 1), `M` = `arangodb.max-splits`.

```
splits = min( ceil(N / S), M )
```

Then **partition** the N shard IDs into `splits` balanced groups — each shard in exactly one group. Each group becomes one `ArangoSplit(groupShardIds)`; a group with >1 shard passes all its IDs to `shardIds(...)`.

**Precedence is explicit:** `max-splits` is a *hard cap*. When `ceil(N/S) > M`, the cap wins and forces **more than `S` shards into some splits** (rather than exceeding the cap or creating empty splits). `shards-per-split` is therefore a *target* group size, overridden by the cap when the two conflict.

The partition invariant (§3) survives **any** grouping, so long as it is a true partition (every shard once, no shard repeated). The grouping function is pure — unit-tested exhaustively (§9) with no cluster: for representative `(N, S, M)`, assert (a) split count `== min(ceil(N/S), M)`, (b) the union of groups equals the input shard set, (c) no shard appears twice, (d) group sizes differ by at most one.

---

## 8. The five required mitigations (master spec §5.1)

All five are required; each maps to a concrete implementation here.

**8.1 Version pin.** Assert a minimum ArangoDB server version at connector startup (master spec §0 assumes ≥ 3.11); refuse shard-splitting below it (→ single split, WARN). Read via `ArangoClient.serverVersion()`. The minimum is a named constant, documented in `CLAUDE.md` and the README.

**8.2 Capability probe.** Before the first fan-out, actively verify the server honors `shardIds` — a shard-scoped count on a single shard of an eligible (>1-shard) collection must be `< ` the full count. Implementation: **lazy on first fan-out, cached for the connector's process lifetime** (see §11 — reinterprets the reviewed spec's "startup" wording, because no guaranteed multi-shard collection exists at boot). Probe the very collection about to be fanned out: `Σ(per-shard counts) == full count` **and** at least one shard's count `< full count`. If the probe is inconclusive or the server ignored the option (shard-scoped count `==` full count), disable fan-out connector-wide, cache that verdict, WARN, and fall back.

**8.3 CI correctness gate.** An automated test asserting per-shard counts **sum to** the collection count, on a real cluster (§9).

**8.4 Safe fallback.** Any inconclusive check, thrown exception, non-cluster server, gate rejection, or probe failure ⇒ a **single split** (correct, just serial) — never risk duplication. Always paired with a WARN naming the reason.

**8.5 SmartGraph exclusion.** SmartGraph / Enterprise smart-edge collections store remote edges twice across internal `_from_`/`_to_` sub-shards (AQL normally dedupes; naive per-shard enumeration double-counts). Excluded via the strategy allowlist (§6.3) + `smartJoinAttribute` check (§6.4), pending explicit future verification.

---

## 9. Testing

**`ShardParallelClusterIT`** (Maven Failsafe IT, following the existing `PackagingSmokeIT` pattern) brings up a real ArangoDB **cluster via Testcontainers `ComposeContainer`** — agency + coordinator + 2 dbservers — per the chosen topology.

- **Count-sum gate (§8.3):** seed a collection with `numberOfShards = 3` and K documents; assert `Σ(per-shard scan counts) == full collection count` **and** each `_key` appears in exactly one shard's scan (proves *no gaps and no dupes*, not just a matching total).
- **N ⇒ splits:** `getSplits` on the 3-shard collection yields the expected split count for the configured `(S, M)`.
- **Cap grouping:** with `M < ceil(N/S)`, assert exactly `M` splits and the count-sum still holds.
- **Fallback proven:** (a) a single-server collection ⇒ 1 split; (b) `shard-parallelism-enabled=false` ⇒ 1 split even on the cluster; (c) a gate-rejected strategy ⇒ 1 split.
- **Capability probe:** a positive-path assertion that the probe passes on a real cluster.

**Unit tests (no cluster):** the pure grouping/partition function (§7) and the allowlist-gate predicate (§6) over representative property combinations, including every `ShardingStrategy` constant and the `numberOfShards ∈ {null, 1, >1}` boundaries.

---

## 10. Per-shard query execution

`ArangoPageSourceProvider.createPageSource` reads `split.shardIds()`:
- **Non-empty** → `client.query(db, aql, bindVars, shardIds)`, which builds `new AqlQueryOptions().shardIds(shardIds.toArray(String[]::new))` and calls the confirmed `db.query(aql, Map.class, bindVars, options)` overload.
- **Empty** → today's `client.query(db, aql, bindVars)` (no options) — the fallback path, byte-for-byte the current behavior.

`AqlBuilder` is untouched: the AQL string and bind vars are identical for a shard-scoped and a whole-collection scan; only the query *option* differs. This keeps shard scoping fully orthogonal to M2 pushdown.

---

## 11. Deviations from the reviewed spec (for review-gate attention)

1. **New knob `arangodb.shard-parallelism-enabled` (default true)** — not in §5.4. Justification: it bypasses the internal `shardIds` code path *entirely*, which `max-splits=1` does **not** (that still emits one split whose scan passes shard IDs through the internal API). A true "never touch the internal API" kill-switch is distinct operator-facing semantics — the escape hatch if the internal option ever misbehaves in production. Explicitly requested for M3.
2. **"Startup capability check" reinterpreted as lazy-first-fan-out, process-lifetime-cached** (§8.2) — the reviewed §5.1 says "startup." A strict boot-time probe has no guaranteed multi-shard collection to probe; probing the first collection actually being fanned out is more robust and equally protective (it still runs before any fan-out emits). The verdict is cached for the connector process lifetime.

---

## 12. Error handling & observability

- Discovery failure, gate rejection, or probe failure on a cluster collection ⇒ WARN naming the reason (e.g. "collection X: sharding strategy `enterprise-hash-smart-edge` not eligible for shard-parallel scan; scanning serially"), never a query failure.
- A shard moved or removed mid-query maps to the already-documented non-goal **§1.3** (no cross-split snapshot consistency): splits execute as independent AQL queries at different wall-clock times. Documented, not solved.

---

## Appendix A — Verified driver surface (`arangodb-java-driver` / `core` 7.13.0)

- `AqlQueryOptions.shardIds(String...)` — per-shard scan mechanism.
- `ArangoDatabase.query(String, Class<T>, Map, AqlQueryOptions)` — the overload that carries the option.
- `ArangoDB.execute(Request<?>, Class<T>)` with `com.arangodb.Request` / `com.arangodb.Response` — raw request for `GET /_api/collection/{name}/shards` enumeration.
- `CollectionPropertiesEntity` — `getNumberOfShards():Integer`, `getShardingStrategy():String`, `getShardKeys():Collection<String>`, `getReplicationFactor():ReplicationFactor`, `getSmartJoinAttribute():String`.
- `ShardingStrategy` (enum) constants: `HASH`, `COMMUNITY_COMPAT`, `ENTERPRISE_COMPAT`, `ENTERPRISE_HASH_SMART_EDGE`, `ENTERPRISE_HEX_SMART_VERTEX`, `ENTERPRISE_SMART_EDGE_COMPAT`; `getInternalName():String`.
- `ArangoCollection.getResponsibleShard(Object):ShardEntity` — per-document only; **not** an enumeration path (noted to prevent misuse).

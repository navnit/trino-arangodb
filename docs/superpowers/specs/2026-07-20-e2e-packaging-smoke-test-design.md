# E2E Packaging Smoke Test — Design

**Date:** 2026-07-20
**Status:** Approved (design), pending implementation plan

## Goal

Add one integration test that boots the real `trinodb/trino:476` Docker image
with the **packaged** plugin directory mounted, points it at a real ArangoDB
container, and runs SQL over JDBC — proving the built artifact loads under
genuine per-plugin classloader isolation and answers a query.

## Motivation

The existing query tests (`ArangoConnectorQueryTest`,
`ArangoConnectorPushdownTest`) exercise the connector through Trino's in-JVM
`DistributedQueryRunner` with `installPlugin(new ArangoPlugin())`. That path is
a real Trino engine, but it loads the plugin on a **flat test classpath** — the
packaged artifact is never dropped into a standalone Trino server and started.

`mvn package` (verified 2026-07-20) produces an exploded
`target/trino-arangodb-0.1.0-SNAPSHOT/` of 64 jars. The runtime dependencies
(`arangodb-java-driver`, `guice`, `bootstrap`, `guava`, `jackson-databind`, …)
are bundled; the `provided`-scoped ones (`trino-spi`, `jackson-annotations`,
`opentelemetry-api`, `opentelemetry-context`) are correctly **absent** —
Trino's own classloader supplies them at runtime.

On the flat test classpath every one of those is present regardless of scope, so
a mis-scoped dependency is invisible:

- a dependency accidentally bundled **and** also on Trino's classpath → a
  `LinkageError` on a real server, silent in-JVM;
- a class the connector needs that was left out of the bundle → `NoClassDefFound`
  at plugin load on a real server, silent in-JVM.

`check-spi-dependencies` (the `trino-maven-plugin` goal) already validates
`provided` scope **against `trino-spi` at build time**. The net-new coverage of
this test is therefore precise and honestly scoped:

1. real-server **per-plugin classloader isolation** of the packaged bundle, and
2. actual plugin **discovery + connector startup** against the official image.

It is not a re-test of query/pushdown/type semantics — those are owned by the
`DistributedQueryRunner` suite and stay there.

## Approach

Testcontainers, consistent with the existing suite. Two alternatives were
considered and rejected:

- **`docker compose` + `trino-cli` shell script** — more moving parts, not
  wired into the build as a gate, and no lifecycle guarantee the artifact under
  test was the one just built.
- **A surefire test that unpacks the zip itself** — surefire's `test` phase runs
  *before* `package`, so the artifact would not yet exist. Failsafe in the
  `verify` phase is the canonical fix.

## Components

### 1. Maven lifecycle — `maven-failsafe-plugin`, `verify` phase

Failsafe's `integration-test` goal binds **after** `package` in the default
lifecycle, so the exploded `target/trino-arangodb-<version>/` exists when the
test runs. The test class is named `*IT.java`, which failsafe includes and
surefire excludes — so `mvn test` stays fast and unchanged; the smoke test runs
only under `mvn verify`.

Two configuration details:

- **Docker API workaround must be replicated.** Surefire sets
  `<api.version>${docker.api.version}</api.version>` so the Testcontainers-shaded
  docker-java client can talk to the daemon. Failsafe is a separate plugin with
  separate configuration; the same `<systemPropertyVariables>` block is copied
  into failsafe's config. (Surefire's `--add-opens` argLine is **not** needed —
  those are for Trino-in-JVM; here Trino runs in a container and the JVM only
  hosts a JDBC client.)
- **Plugin dir passed as a property, not globbed.** Failsafe passes
  `-Dplugin.dir=${project.build.directory}/${project.build.finalName}`
  (= `target/trino-arangodb-0.1.0-SNAPSHOT`) so the test never hardcodes or
  globs a version string.

Add `io.trino:trino-jdbc:476` at `test` scope as the client.

`-DskipITs` skips failsafe locally without touching the surefire suite.

### 2. Networking — shared network, two addresses

A shared Testcontainers `Network`. The ArangoDB container joins it with the
network alias `arangodb`. The critical detail is that the **same** ArangoDB
container is reachable at two addresses:

- the **JVM seeds** it via `container.getHost()` + `getMappedPort(8529)` (host
  perspective, an ephemeral host port);
- the **Trino container's catalog** points at `arangodb:8529` — the alias plus
  the **internal** container port, because Trino resolves it from inside the
  shared Docker network, not from the host.

### 3. Trino container

`GenericContainer("trinodb/trino:476")` on the same network, with:

- a **read-only bind mount** of the exploded plugin dir →
  `/usr/lib/trino/plugin/arangodb/`;
- a generated `arango.properties` mounted → `/etc/trino/catalog/arango.properties`
  containing `connector.name=arangodb`, `arangodb.hosts=arangodb:8529`,
  `arangodb.user=root`, `arangodb.password=<seeded password>`;
- port `8080` exposed;
- a **readiness wait on `/v1/info` reporting `"starting":false`**. A bare
  port-open / HTTP-200 check flakes: Trino answers HTTP while still starting and
  rejects queries during that window.

### 4. ArangoDB seeding

Reuse the existing `TestingArangoServer` container image/config but on the
shared network with the `arangodb` alias. Seed (from the JVM, via the mapped
host port) a minimal fixture matching the existing query test: database `shop`,
collection `users` with two documents (`{name:ada, age:36}`,
`{name:bob, age:41}`). No edge collection is needed for this smoke test.

### 5. Assertions — minimal

Over a JDBC connection to the Trino container's mapped `8080`:

- `SHOW CATALOGS` contains `arango` — proves the catalog wired up and the
  packaged plugin loaded under real classloader isolation.
- `SELECT name, age FROM arango.shop.users ORDER BY age` returns exactly the two
  seeded rows in order — proves an end-to-end query round-trip through the
  packaged connector.

Nothing more. Pushdown, type coercion, hidden columns, edge `_from`/`_to`, and
error paths remain owned by the `DistributedQueryRunner` tests.

### 6. CI

`.github/workflows/ci.yml` changes its build step from `mvn ... package` to
`mvn ... verify` so the smoke test runs on every push and pull request. The
marginal cost over today's run is one additional Trino container (plus a
one-time image pull); the runner already provisions Docker and an ArangoDB
container. A packaging or classloader regression then fails CI immediately,
which is the entire reason for the test. The artifact-upload step is unchanged.

## Out of scope

- Multi-node / worker fan-out, shard splits.
- Any write path.
- Re-testing query semantics already covered in-JVM.
- Testing the `.zip` unpack path specifically (we mount the exploded dir the
  same `package` run produces; the zip is the same content archived).

## Testing approach

The test itself is the deliverable. It is validated by: (a) it passes green
against the current, correct pom; and (b) a deliberate negative check during
development — temporarily mis-scoping one `provided` dependency to `compile`
(so it gets bundled) should make the smoke test fail at plugin load while the
in-JVM tests stay green, confirming the test actually exercises the isolation
boundary. The negative check is a manual development-time confirmation, not a
committed test.

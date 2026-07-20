# E2E Packaging Smoke Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one integration test that boots the real `trinodb/trino:476` image with the packaged plugin directory mounted, points it at a real ArangoDB container, and runs SQL over JDBC — verifying the built artifact loads under genuine per-plugin classloader isolation and answers a query.

**Architecture:** A `maven-failsafe-plugin` binds a `*IT.java` test to the `verify` phase, which runs *after* `package` builds the exploded `target/trino-arangodb-<version>/` plugin dir. The test starts ArangoDB and Trino containers on a shared Docker network (Trino reaches ArangoDB at the internal alias `arangodb:8529`; the JVM seeds ArangoDB via its host-mapped port), mounts the packaged plugin dir into the Trino container, and queries it over the Trino JDBC driver.

**Tech Stack:** Java 24, Maven (surefire + failsafe 3.5.2), Trino 476 (`trino-jdbc` client), Testcontainers 1.20.4 (`trinodb/trino:476`, `arangodb/arangodb:3.12`), JUnit Jupiter 5.11.3, AssertJ.

## Global Constraints

- Build requires Java 24: `source ~/.sdkman/bin/sdkman-init.sh` before `mvn`. `maven.compiler.release=24`.
- Docker must be running (Testcontainers). The pom pins `docker.api.version=1.51` (suits a modern local Engine); CI overrides it with the daemon's reported max via `-Ddocker.api.version=$(docker version --format '{{.Server.APIVersion}}')`.
- **Do NOT change any existing dependency version pin or scope in `pom.xml`.** Only *add* the `trino-jdbc` test dependency and the failsafe plugin. The pom's `provided`/`compile` scope choices are load-bearing (see the long comments in `pom.xml`).
- Trino version is `476` everywhere (`${dep.trino.version}`); Trino image `trinodb/trino:476`; ArangoDB image `arangodb/arangodb:3.12`.
- The failsafe `<systemPropertyVariables>` MUST include `api.version=${docker.api.version}` (the Docker workaround does not carry over from surefire — failsafe is a separate plugin) and `plugin.dir=${project.build.directory}/${project.build.finalName}`.
- Test class named `PackagingSmokeIT` (the `*IT` suffix routes it to failsafe, and keeps it out of surefire's `mvn test`).
- No mocking framework — real containers only.
- Assertions stay minimal: `SHOW CATALOGS` contains `arango`, plus one `SELECT … FROM arango.shop.users` round-trip. No pushdown/type/edge re-testing (owned by the in-JVM `DistributedQueryRunner` tests).
- The existing surefire suite must remain unchanged and green.
- End every commit message with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## File Structure

- **Modify** `pom.xml` — add `io.trino:trino-jdbc:476` (test scope); add `maven-failsafe-plugin` 3.5.2 bound to `integration-test`+`verify` with the two system properties.
- **Modify** `src/test/java/io/arango/trino/TestingArangoServer.java` — add a `Network`-aware constructor (backward-compatible; the no-arg constructor keeps its current behavior).
- **Create** `src/test/java/io/arango/trino/PackagingSmokeIT.java` — the integration test.
- **Modify** `.github/workflows/ci.yml` — change the build step from `mvn … package` to `mvn … verify`.

---

## Task 1: Maven wiring (trino-jdbc dependency + failsafe plugin)

**Files:**
- Modify: `pom.xml` (dependencies block and `build/plugins`)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the `verify` phase now runs failsafe; `plugin.dir` and `api.version` are set for any `*IT` test; `io.trino.jdbc.TrinoDriver` is on the test classpath.

- [ ] **Step 1: Add the trino-jdbc test dependency**

In `pom.xml`, immediately after the `trino-testing` dependency block (the one ending `</dependency>` right before the `io.airlift:jaxrs` test dep, around line 108), add:

```xml
        <!-- JDBC client for the end-to-end packaging smoke test (PackagingSmokeIT):
             connects to a real trinodb/trino container over its mapped HTTP port. -->
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-jdbc</artifactId>
            <version>${dep.trino.version}</version>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: Add the failsafe plugin**

In `pom.xml`, inside `<build><plugins>`, after the `maven-surefire-plugin` block (closing `</plugin>` near line 246) and before `</plugins>`, add:

```xml
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <version>3.5.2</version>
                <configuration>
                    <!-- Docker Remote API workaround, same as surefire: failsafe is a
                         separate plugin and does NOT inherit surefire's config, so the
                         Testcontainers-shaded docker-java client needs api.version here too. -->
                    <systemPropertyVariables>
                        <api.version>${docker.api.version}</api.version>
                        <!-- Absolute path to the exploded plugin dir produced by `package`
                             (target/trino-arangodb-<version>). PackagingSmokeIT mounts it
                             into the Trino container. Passed as a property so the test never
                             globs or hardcodes a version string. -->
                        <plugin.dir>${project.build.directory}/${project.build.finalName}</plugin.dir>
                    </systemPropertyVariables>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
```

- [ ] **Step 3: Verify the pom parses, the plugin binds, and the artifact still builds**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn -q -B --no-transfer-progress -DskipTests verify`
Expected: `BUILD SUCCESS`. (`-DskipTests` skips both surefire and failsafe; this step only proves the pom is valid, the failsafe execution binds, and `package` still produces `target/trino-arangodb-0.1.0-SNAPSHOT/`.)

- [ ] **Step 4: Verify trino-jdbc resolves at test scope**

Run: `mvn -q -B dependency:tree -Dincludes=io.trino:trino-jdbc`
Expected: output contains `io.trino:trino-jdbc:jar:476:test`.

- [ ] **Step 5: Commit**

```bash
git add pom.xml
git commit -m "test: wire failsafe + trino-jdbc for packaging smoke test

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Network-aware `TestingArangoServer`

**Files:**
- Modify: `src/test/java/io/arango/trino/TestingArangoServer.java`
- Test: existing `src/test/java/io/arango/trino/ArangoConnectorQueryTest.java` (must still pass — proves backward compatibility)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `new TestingArangoServer(Network network, String alias)` — starts the ArangoDB container joined to `network` with `alias` as a network alias, so a container on the same network reaches it at `alias:8529`. The existing no-arg `new TestingArangoServer()` behavior is unchanged. Accessors `host()`, `port()`, `hostPort()`, `rootPassword()`, `close()` are unchanged.

- [ ] **Step 1: Add the import**

In `src/test/java/io/arango/trino/TestingArangoServer.java`, add to the imports (after the `GenericContainer` import on line 3):

```java
import org.testcontainers.containers.Network;
```

- [ ] **Step 2: Replace the constructor with a no-arg delegate + a network-aware constructor**

Replace this exact block:

```java
    @SuppressWarnings("resource")
    public TestingArangoServer() {
        container = new GenericContainer<>(DockerImageName.parse("arangodb/arangodb:3.12"))
                .withExposedPorts(PORT)
                .withEnv("ARANGO_ROOT_PASSWORD", ROOT_PASSWORD)
                .waitingFor(Wait.forHttp("/_api/version").forStatusCode(200)
                        .forPort(PORT).withBasicCredentials("root", ROOT_PASSWORD));
        container.start();
    }
```

with:

```java
    public TestingArangoServer() {
        this(null, null);
    }

    /**
     * Starts ArangoDB joined to {@code network} under {@code alias}, so a container on the
     * same network reaches it at {@code alias:8529}. Pass {@code (null, null)} for a
     * host-only container (the no-arg constructor). The host-mapped port is still published
     * either way, so JVM-side seeding via {@link #hostPort()} works in both modes.
     */
    @SuppressWarnings("resource")
    public TestingArangoServer(Network network, String alias) {
        GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse("arangodb/arangodb:3.12"))
                .withExposedPorts(PORT)
                .withEnv("ARANGO_ROOT_PASSWORD", ROOT_PASSWORD)
                .waitingFor(Wait.forHttp("/_api/version").forStatusCode(200)
                        .forPort(PORT).withBasicCredentials("root", ROOT_PASSWORD));
        if (network != null) {
            c = c.withNetwork(network).withNetworkAliases(alias);
        }
        container = c;
        container.start();
    }
```

- [ ] **Step 3: Verify existing query test still passes (backward compatibility)**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn -q -B --no-transfer-progress test -Dtest=ArangoConnectorQueryTest`
Expected: `BUILD SUCCESS`, 0 failures/errors — the existing query test (which uses the no-arg `new TestingArangoServer()`) still passes unchanged.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/io/arango/trino/TestingArangoServer.java
git commit -m "test: add network-aware constructor to TestingArangoServer

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `PackagingSmokeIT`

**Files:**
- Create: `src/test/java/io/arango/trino/PackagingSmokeIT.java`
- Depends on: Task 1 (failsafe + `plugin.dir` + `trino-jdbc`), Task 2 (network-aware `TestingArangoServer`)

**Interfaces:**
- Consumes: `System.getProperty("plugin.dir")` (set by failsafe); `new TestingArangoServer(Network, String)`; `ArangoClient.createDatabaseForTest/createDocumentCollectionForTest/insertForTest` (existing seeding helpers, as used by `ArangoConnectorQueryTest`).
- Produces: the end-to-end gate (terminal deliverable — nothing consumes it).

- [ ] **Step 1: Create the integration test**

Create `src/test/java/io/arango/trino/PackagingSmokeIT.java` with exactly:

```java
package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end packaging smoke test. Mounts the *packaged* plugin directory
 * (target/trino-arangodb-&lt;version&gt;, passed as -Dplugin.dir by failsafe) into a real
 * trinodb/trino:476 container, points it at a real ArangoDB container over a shared Docker
 * network, and runs SQL over JDBC.
 *
 * <p>Unlike the in-JVM {@code DistributedQueryRunner} tests (which load the plugin on a flat
 * test classpath), this exercises real per-plugin classloader isolation of the packaged bundle
 * and actual plugin discovery/startup. The {@code *IT} suffix routes it to failsafe (verify
 * phase, after {@code package}), so it is excluded from {@code mvn test}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PackagingSmokeIT {

    private static final String ARANGO_ALIAS = "arangodb";
    private static final int ARANGO_PORT = 8529;
    private static final int TRINO_PORT = 8080;

    private Network network;
    private TestingArangoServer arango;
    private GenericContainer<?> trino;

    @BeforeAll
    void setup() throws Exception {
        String pluginDir = System.getProperty("plugin.dir");
        if (pluginDir == null || !Files.isDirectory(Path.of(pluginDir))) {
            throw new IllegalStateException(
                    "plugin.dir must point at the packaged plugin directory "
                            + "(target/trino-arangodb-<version>). Run via `mvn verify` "
                            + "(failsafe sets it). Got: " + pluginDir);
        }

        network = Network.newNetwork();

        // ArangoDB on the shared network under a stable alias; seeded via the host-mapped port.
        arango = new TestingArangoServer(network, ARANGO_ALIAS);
        try (ArangoClient seed = new ArangoClient(new ArangoConfig()
                .setHosts(arango.hostPort()).setUser("root").setPassword(arango.rootPassword()))) {
            seed.createDatabaseForTest("shop");
            seed.createDocumentCollectionForTest("shop", "users");
            seed.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
            seed.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
        }

        // Catalog the Trino container reads at startup. It reaches ArangoDB at the *internal*
        // network address (alias + container port), NOT the JVM-visible host-mapped port.
        String catalog = "connector.name=arangodb\n"
                + "arangodb.hosts=" + ARANGO_ALIAS + ":" + ARANGO_PORT + "\n"
                + "arangodb.user=root\n"
                + "arangodb.password=" + arango.rootPassword() + "\n";

        trino = new GenericContainer<>(DockerImageName.parse("trinodb/trino:476"))
                .withNetwork(network)
                .withExposedPorts(TRINO_PORT)
                .withFileSystemBind(pluginDir, "/usr/lib/trino/plugin/arangodb", BindMode.READ_ONLY)
                .withCopyToContainer(Transferable.of(catalog), "/etc/trino/catalog/arango.properties")
                // Trino answers HTTP while still starting and rejects queries during that window,
                // so wait for /v1/info to report the server is done starting.
                .waitingFor(Wait.forHttp("/v1/info").forPort(TRINO_PORT).forStatusCode(200)
                        .forResponsePredicate(body -> body.contains("\"starting\":false"))
                        .withStartupTimeout(Duration.ofMinutes(3)));
        trino.start();
    }

    @AfterAll
    void teardown() {
        if (trino != null) trino.stop();
        if (arango != null) arango.close();
        if (network != null) network.close();
    }

    private Connection connect() throws Exception {
        String url = "jdbc:trino://" + trino.getHost() + ":" + trino.getMappedPort(TRINO_PORT);
        Properties props = new Properties();
        props.setProperty("user", "test"); // no auth on the default image; any user is accepted
        return DriverManager.getConnection(url, props);
    }

    @Test
    void packagedPluginRegistersCatalog() throws Exception {
        try (Connection conn = connect();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW CATALOGS")) {
            List<String> catalogs = new ArrayList<>();
            while (rs.next()) {
                catalogs.add(rs.getString(1));
            }
            assertThat(catalogs).contains("arango");
        }
    }

    @Test
    void packagedPluginAnswersQuery() throws Exception {
        try (Connection conn = connect();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT name, age FROM arango.shop.users ORDER BY age")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("ada");
            assertThat(rs.getLong("age")).isEqualTo(36L);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("bob");
            assertThat(rs.getLong("age")).isEqualTo(41L);
            assertThat(rs.next()).isFalse();
        }
    }
}
```

- [ ] **Step 2: Run the full verify (surefire suite + the new IT), green**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn -B --no-transfer-progress verify`
Expected: `BUILD SUCCESS`. The `maven-failsafe-plugin:integration-test` line reports `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0` for `PackagingSmokeIT`, and the surefire suite stays green.

> Faster dev iteration (build once, then run only the IT): `mvn -B -DskipTests package` then `mvn -B failsafe:integration-test failsafe:verify -Dit.test=PackagingSmokeIT`. If Docker rejects the API version locally, append `-Ddocker.api.version=$(docker version --format '{{.Server.APIVersion}}')`.

- [ ] **Step 3: Negative check that the test actually exercises the packaging boundary (MANUAL — do NOT commit)**

This confirms the IT catches a packaging/classloader break the in-JVM tests cannot. Do it by hand and revert; nothing here is committed.

1. In `pom.xml`, temporarily change the `arangodb-java-driver` dependency scope from its current default (compile) to `<scope>provided</scope>` — this removes the driver jar from the packaged bundle while leaving it on the flat test classpath.
2. Rebuild the bundle: `mvn -q -B -DskipTests package`.
3. Run the IT: `mvn -B failsafe:integration-test failsafe:verify -Dit.test=PackagingSmokeIT`.
   Expected: **FAILS** — Trino cannot start the `arango` catalog (the connector's `arangodb-java-driver` classes are missing from the isolated plugin classloader), so the container never reports `"starting":false` and setup fails (or, if it starts, the query fails). Check the Trino container log for a `ClassNotFoundException`/`NoClassDefFoundError` around `com.arangodb`.
4. Confirm the in-JVM test stays green with the same broken pom: `mvn -q -B test -Dtest=ArangoConnectorQueryTest` → PASS (the driver is on the flat test classpath, so the in-JVM path can't see the packaging break).
5. **Revert** the `pom.xml` scope change (`git checkout pom.xml`) and re-run `mvn -q -B -DskipTests package` to confirm the correct bundle rebuilds.

- [ ] **Step 4: Commit (IT only — confirm pom.xml is reverted first)**

```bash
git status --short   # must show only src/test/java/io/arango/trino/PackagingSmokeIT.java
git add src/test/java/io/arango/trino/PackagingSmokeIT.java
git commit -m "test: add end-to-end packaging smoke test against real Trino

Mounts the packaged plugin dir into a real trinodb/trino:476 container,
points it at a real ArangoDB container over a shared network, and runs
SHOW CATALOGS + a SELECT over JDBC. Covers real-server classloader
isolation and plugin discovery/startup — the layer the in-JVM
DistributedQueryRunner tests cannot reach.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: CI runs the smoke test (`package` → `verify`)

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 3's `*IT` now runs under `verify`.
- Produces: every push/PR runs the packaging smoke test.

- [ ] **Step 1: Change the build step to `verify` and update the comment**

In `.github/workflows/ci.yml`, replace this exact block:

```yaml
      # 'package' runs the full test phase first: the suite is almost entirely
      # Testcontainers-based (a real arangodb/arangodb:3.12 container per test class,
      # plus DistributedQueryRunner end-to-end tests), which needs Docker. GitHub's
      # ubuntu-latest runners ship Docker, so no extra service setup is required.
      #
      # pom.xml pins the Testcontainers-shaded docker-java client's Remote API version
      # (surefire api.version property) to work around its lack of negotiation. Its
      # default (1.51) suits a modern local Docker Engine but is rejected as "too new"
      # by an older Engine, so here we hand Maven the runner daemon's own reported max
      # via -Ddocker.api.version. The value is echoed first, unconditionally, so the
      # log shows exactly which API version this runner offered.
      - name: Build and test
        run: |
          DOCKER_API=$(docker version --format '{{.Server.APIVersion}}')
          echo "Runner Docker Server API version: ${DOCKER_API}"
          mvn -B --no-transfer-progress -Ddocker.api.version="${DOCKER_API}" package
```

with:

```yaml
      # 'verify' runs the full test phase first: the suite is almost entirely
      # Testcontainers-based (a real arangodb/arangodb:3.12 container per test class,
      # plus DistributedQueryRunner end-to-end tests), which needs Docker. It then runs
      # the failsafe integration test (PackagingSmokeIT), which boots a real
      # trinodb/trino:476 container with the just-packaged plugin mounted and queries it
      # over JDBC. GitHub's ubuntu-latest runners ship Docker, so no extra setup is needed.
      #
      # pom.xml pins the Testcontainers-shaded docker-java client's Remote API version
      # (surefire/failsafe api.version property) to work around its lack of negotiation. Its
      # default (1.51) suits a modern local Docker Engine but is rejected as "too new"
      # by an older Engine, so here we hand Maven the runner daemon's own reported max
      # via -Ddocker.api.version. The value is echoed first, unconditionally, so the
      # log shows exactly which API version this runner offered.
      - name: Build and test
        run: |
          DOCKER_API=$(docker version --format '{{.Server.APIVersion}}')
          echo "Runner Docker Server API version: ${DOCKER_API}"
          mvn -B --no-transfer-progress -Ddocker.api.version="${DOCKER_API}" verify
```

- [ ] **Step 2: Verify the workflow still parses and `verify` succeeds locally (mirrors CI)**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn -B --no-transfer-progress -Ddocker.api.version=$(docker version --format '{{.Server.APIVersion}}') verify`
Expected: `BUILD SUCCESS`, including `PackagingSmokeIT` under failsafe. (The `if: success()` artifact-upload step is unchanged; `verify` still runs `package`, so `target/*.zip` still exists.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: run packaging smoke test via mvn verify

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review notes (author)

- **Spec coverage:** every design component maps to a task — lifecycle/failsafe + jdbc (Task 1), two-address networking helper (Task 2), Trino container + mounts + readiness + minimal assertions (Task 3), CI always-on (Task 4). The honest value framing and the negative check are in Task 3 Step 3.
- **Placeholders:** none — all code and commands are literal.
- **Type/name consistency:** `TestingArangoServer(Network, String)` is defined in Task 2 and consumed in Task 3; `plugin.dir`/`api.version` are set in Task 1 and read in Task 3; image tags, ports, and the `arango`/`shop`/`users` fixtures match across tasks and the existing `ArangoConnectorQueryTest`.

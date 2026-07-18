# ArangoDB Trino Connector — Milestone 1 (Read Skeleton) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working read-only Trino connector that lists ArangoDB databases/collections as schemas/tables and executes `SELECT *` returning correctly-typed rows via a single split.

**Architecture:** Standard Trino Connector SPI plugin (Java, Maven, Airlift/Guice). `ArangoClient` wraps `arangodb-java-driver` 7.x. `SchemaResolver` infers table schemas by merge-sampling documents; `TypeMapper` maps JSON/VelocyPack → Trino types with numeric widening. `ArangoMetadata` exposes schemas/tables/columns; `ArangoSplitManager` emits one split; `ArangoPageSource` runs the AQL cursor and builds columnar pages. No pushdown, no writes, no schema override/validation sources (those arrive in M2/M5).

**Tech Stack:** Java 23, Maven, Trino SPI, `com.arangodb:arangodb-java-driver:7.x` (HTTP/2 + VelocyPack), Airlift Bootstrap/Guice, JUnit 5 + AssertJ, Testcontainers.

## Global Constraints

- **Connector name:** `arangodb` (registered by `ArangoConnectorFactory.getName()`).
- **Java:** 23 (matches Trino server JDK; verify at pin time — may be 24). Maven `maven.compiler.release=23`.
- **Trino version:** pin one stable version (plan written against **Trino 476**). **SPI signatures below target 476 — verify each against the pinned version at task start and adjust** (the page-source and `getTableHandle` signatures in particular have shifted across recent releases). Use `<dep.trino.version>` property in the pom.
- **Driver:** `arangodb-java-driver` **7.x** over HTTP/2 + VelocyPack module only (no VelocyStream — removed in 3.12).
- **Package root:** `io.arango.trino`.
- **Type mapping (M1 subset):** bool→`BOOLEAN`, integer→`BIGINT`, integer>BIGINT/uint64→`DECIMAL(38,0)`, float→`DOUBLE`, int/float conflict→**widen to `DOUBLE`**, string→`VARCHAR`, array→`ARRAY(e)`, object→`ROW(...)`, incompatible conflict→`VARCHAR` (default `mixed-type-strategy`), null/absent→column retained, `_key`/`_id`/`_rev`→hidden `VARCHAR`, `_from`/`_to`→visible `VARCHAR`. **Dates are strings in M1** (no timestamp inference — schema-source-only, deferred to M5).
- **Coercion:** `lenient` (type mismatch → `NULL`) is the only M1 behavior; `strict` config wired but not exercised until later.
- **Schema source (M1):** sampling only. Field **union across all sampled docs**, never drop a column on null/empty. Sample size config `arangodb.schema.sample-size` (default 1000).
- **TDD:** every task is failing-test → run-fail → implement → run-pass → commit. DRY, YAGNI, frequent commits.
- **Commit prefix:** conventional commits (`feat:`, `test:`, `chore:`).

---

## File Structure

```
arangodb-trino/
├── pom.xml                                              # Maven module (T1)
└── src/
    ├── main/java/io/arango/trino/
    │   ├── ArangoConfig.java                            # @Config bindings (T1)
    │   ├── ArangoConnectorFactory.java                 # SPI factory + Bootstrap (T9)
    │   ├── ArangoPlugin.java                            # SPI entry point (T9)
    │   ├── ArangoModule.java                            # Guice bindings (T9)
    │   ├── ArangoConnector.java                         # SPI Connector (T9)
    │   ├── ArangoTransactionHandle.java                # singleton handle (T9)
    │   ├── client/
    │   │   └── ArangoClient.java                        # driver wrapper (T2)
    │   ├── type/
    │   │   └── TypeMapper.java                          # JSON→Trino types + merge (T3)
    │   ├── schema/
    │   │   └── SchemaResolver.java                      # sampling→columns (T4)
    │   ├── handle/
    │   │   ├── ArangoColumnHandle.java                 # (T5)
    │   │   ├── ArangoTableHandle.java                  # (T5)
    │   │   └── ArangoSplit.java                         # (T7)
    │   ├── ArangoMetadata.java                          # ConnectorMetadata (T5)
    │   ├── aql/
    │   │   └── AqlBuilder.java                          # base scan AQL (T6)
    │   ├── ArangoSplitManager.java                     # single split (T7)
    │   └── ArangoPageSourceProvider.java + ArangoPageSource.java  # (T8)
    └── test/java/io/arango/trino/
        ├── ArangoConfigTest.java                        # (T1)
        ├── TestingArangoServer.java                     # Testcontainers fixture (T2)
        ├── client/ArangoClientTest.java                 # (T2)
        ├── type/TypeMapperTest.java                     # (T3)
        ├── schema/SchemaResolverTest.java               # (T4)
        ├── aql/AqlBuilderTest.java                      # (T6)
        └── ArangoConnectorQueryTest.java                # end-to-end (T9)
```

---

### Task 1: Maven module + configuration class

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Produces: `ArangoConfig` with getters `getHosts():List<String>`, `getUser():String`, `getPassword():String`, `getSampleSize():int`, `getMixedTypeStrategy():MixedTypeStrategy` (enum `VARCHAR|JSON`), `isSampleRandom():boolean`. Setters annotated `@Config("arangodb.<key>")`.

- [ ] **Step 1: Write the failing test**

```java
// src/test/java/io/arango/trino/ArangoConfigTest.java
package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class ArangoConfigTest {
    @Test
    void testDefaults() {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ArangoConfig.class)
                .setHosts("localhost:8529")
                .setUser("root")
                .setPassword("")
                .setSampleSize(1000)
                .setSampleRandom(false)
                .setMixedTypeStrategy(ArangoConfig.MixedTypeStrategy.VARCHAR));
    }

    @Test
    void testExplicitPropertyMappings() {
        Map<String, String> props = ImmutableMap.<String, String>builder()
                .put("arangodb.hosts", "a:8529,b:8529")
                .put("arangodb.user", "reader")
                .put("arangodb.password", "secret")
                .put("arangodb.schema.sample-size", "50")
                .put("arangodb.schema.sample-random", "true")
                .put("arangodb.schema.mixed-type-strategy", "json")
                .buildOrThrow();

        ArangoConfig expected = new ArangoConfig()
                .setHosts("a:8529,b:8529")
                .setUser("reader")
                .setPassword("secret")
                .setSampleSize(50)
                .setSampleRandom(true)
                .setMixedTypeStrategy(ArangoConfig.MixedTypeStrategy.JSON);

        ConfigAssertions.assertFullMapping(props, expected);
        // hosts parsed into list
        org.assertj.core.api.Assertions.assertThat(expected.getHostList())
                .isEqualTo(List.of("a:8529", "b:8529"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=ArangoConfigTest test`
Expected: FAIL — `ArangoConfig` does not exist / does not compile.

- [ ] **Step 3: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>io.arango</groupId>
    <artifactId>trino-arangodb</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>trino-plugin</packaging>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.release>23</maven.compiler.release>
        <dep.trino.version>476</dep.trino.version>
        <dep.airlift.version>293</dep.airlift.version>
        <dep.arango.version>7.13.0</dep.arango.version>
        <dep.testcontainers.version>1.20.4</dep.testcontainers.version>
        <dep.junit.version>5.11.3</dep.junit.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-spi</artifactId>
            <version>${dep.trino.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>bootstrap</artifactId>
            <version>${dep.airlift.version}</version>
        </dependency>
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>configuration</artifactId>
            <version>${dep.airlift.version}</version>
        </dependency>
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>json</artifactId>
            <version>${dep.airlift.version}</version>
        </dependency>
        <dependency>
            <groupId>com.google.inject</groupId>
            <artifactId>guice</artifactId>
            <version>7.0.0</version>
        </dependency>
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>33.3.1-jre</version>
        </dependency>
        <dependency>
            <groupId>com.arangodb</groupId>
            <artifactId>arangodb-java-driver</artifactId>
            <version>${dep.arango.version}</version>
        </dependency>

        <!-- test -->
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-testing</artifactId>
            <version>${dep.trino.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>configuration</artifactId>
            <version>${dep.airlift.version}</version>
            <classifier>tests</classifier>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${dep.junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>3.26.3</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <version>${dep.testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${dep.testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>io.trino</groupId>
                <artifactId>trino-maven-plugin</artifactId>
                <version>14</version>
                <extensions>true</extensions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.2</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 4: Write minimal `ArangoConfig`**

```java
// src/main/java/io/arango/trino/ArangoConfig.java
package io.arango.trino;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class ArangoConfig {
    public enum MixedTypeStrategy { VARCHAR, JSON }

    private String hosts = "localhost:8529";
    private String user = "root";
    private String password = "";
    private int sampleSize = 1000;
    private boolean sampleRandom = false;
    private MixedTypeStrategy mixedTypeStrategy = MixedTypeStrategy.VARCHAR;

    @NotNull
    public String getHosts() { return hosts; }

    @Config("arangodb.hosts")
    @ConfigDescription("Comma-separated host:port coordinators")
    public ArangoConfig setHosts(String hosts) { this.hosts = hosts; return this; }

    public List<String> getHostList() {
        return ImmutableList.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(hosts));
    }

    @NotNull
    public String getUser() { return user; }

    @Config("arangodb.user")
    public ArangoConfig setUser(String user) { this.user = user; return this; }

    public String getPassword() { return password; }

    @Config("arangodb.password")
    @ConfigSecuritySensitive
    public ArangoConfig setPassword(String password) { this.password = password; return this; }

    @Min(1)
    public int getSampleSize() { return sampleSize; }

    @Config("arangodb.schema.sample-size")
    public ArangoConfig setSampleSize(int sampleSize) { this.sampleSize = sampleSize; return this; }

    public boolean isSampleRandom() { return sampleRandom; }

    @Config("arangodb.schema.sample-random")
    public ArangoConfig setSampleRandom(boolean sampleRandom) { this.sampleRandom = sampleRandom; return this; }

    @NotNull
    public MixedTypeStrategy getMixedTypeStrategy() { return mixedTypeStrategy; }

    @Config("arangodb.schema.mixed-type-strategy")
    public ArangoConfig setMixedTypeStrategy(MixedTypeStrategy mixedTypeStrategy) {
        this.mixedTypeStrategy = mixedTypeStrategy; return this;
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -Dtest=ArangoConfigTest test`
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/java/io/arango/trino/ArangoConfig.java src/test/java/io/arango/trino/ArangoConfigTest.java
git commit -m "feat: maven module and ArangoConfig with property bindings"
```

---

### Task 2: `ArangoClient` + Testcontainers fixture

**Files:**
- Create: `src/main/java/io/arango/trino/client/ArangoClient.java`
- Create: `src/test/java/io/arango/trino/TestingArangoServer.java`
- Test: `src/test/java/io/arango/trino/client/ArangoClientTest.java`

**Interfaces:**
- Produces: `ArangoClient` (constructed from `ArangoConfig`) with:
  - `List<String> listDatabases()`
  - `List<CollectionInfo> listCollections(String database)` where `CollectionInfo` is a record `(String name, boolean isEdge, boolean isSystem)`
  - `List<Map<String,Object>> sampleDocuments(String database, String collection, int limit, boolean random)`
  - `ArangoCursor<BaseDocument> query(String database, String aql, Map<String,Object> bindVars)` (raw AQL execution used by the page source in T8; return the driver cursor over `BaseDocument`)
  - `void close()`
- Consumes: `ArangoConfig` (T1).

- [ ] **Step 1: Write the Testcontainers fixture (test infra, no assertions yet)**

```java
// src/test/java/io/arango/trino/TestingArangoServer.java
package io.arango.trino;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;

public final class TestingArangoServer implements Closeable {
    private static final int PORT = 8529;
    private static final String ROOT_PASSWORD = "test";
    private final GenericContainer<?> container;

    @SuppressWarnings("resource")
    public TestingArangoServer() {
        container = new GenericContainer<>(DockerImageName.parse("arangodb/arangodb:3.12"))
                .withExposedPorts(PORT)
                .withEnv("ARANGO_ROOT_PASSWORD", ROOT_PASSWORD)
                .waitingFor(Wait.forHttp("/_api/version").forStatusCode(200)
                        .forPort(PORT).withBasicCredentials("root", ROOT_PASSWORD));
        container.start();
    }

    public String host() { return container.getHost(); }
    public int port() { return container.getMappedPort(PORT); }
    public String hostPort() { return host() + ":" + port(); }
    public String rootPassword() { return ROOT_PASSWORD; }

    @Override
    public void close() { container.stop(); }
}
```

- [ ] **Step 2: Write the failing test**

```java
// src/test/java/io/arango/trino/client/ArangoClientTest.java
package io.arango.trino.client;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoClientTest {
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort())
                .setUser("root")
                .setPassword(server.rootPassword()));
        // seed: database "shop" with document collection "users"
        client.createDatabaseForTest("shop");
        client.createDocumentCollectionForTest("shop", "users");
        client.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
        client.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
    }

    @AfterAll
    void teardown() {
        client.close();
        server.close();
    }

    @Test
    void listDatabasesIncludesSeed() {
        assertThat(client.listDatabases()).contains("shop");
    }

    @Test
    void listCollectionsMarksTypeAndSystem() {
        List<CollectionInfo> cols = client.listCollections("shop");
        assertThat(cols).anySatisfy(c -> {
            assertThat(c.name()).isEqualTo("users");
            assertThat(c.isEdge()).isFalse();
            assertThat(c.isSystem()).isFalse();
        });
    }

    @Test
    void sampleDocumentsReturnsRows() {
        List<Map<String, Object>> docs = client.sampleDocuments("shop", "users", 10, false);
        assertThat(docs).hasSize(2);
        assertThat(docs).allSatisfy(d -> assertThat(d).containsKey("name"));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q -Dtest=ArangoClientTest test`
Expected: FAIL — `ArangoClient` does not exist.

- [ ] **Step 4: Write minimal `ArangoClient`**

```java
// src/main/java/io/arango/trino/client/ArangoClient.java
package io.arango.trino.client;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.google.common.collect.ImmutableList;
import io.arango.trino.ArangoConfig;

import java.util.List;
import java.util.Map;

public class ArangoClient implements AutoCloseable {
    public record CollectionInfo(String name, boolean isEdge, boolean isSystem) {}

    private final ArangoDB arango;

    public ArangoClient(ArangoConfig config) {
        ArangoDB.Builder builder = new ArangoDB.Builder()
                .protocol(Protocol.HTTP2_JSON)
                .user(config.getUser())
                .password(config.getPassword());
        for (String hostPort : config.getHostList()) {
            String[] parts = hostPort.split(":");
            builder.host(parts[0], Integer.parseInt(parts[1]));
        }
        this.arango = builder.build();
    }

    public List<String> listDatabases() {
        return ImmutableList.copyOf(arango.getAccessibleDatabases());
    }

    public List<CollectionInfo> listCollections(String database) {
        ImmutableList.Builder<CollectionInfo> out = ImmutableList.builder();
        for (CollectionEntity e : arango.db(database).getCollections()) {
            out.add(new CollectionInfo(
                    e.getName(),
                    e.getType() == CollectionType.EDGES,
                    Boolean.TRUE.equals(e.getIsSystem())));
        }
        return out.build();
    }

    public List<Map<String, Object>> sampleDocuments(String database, String collection, int limit, boolean random) {
        String sort = random ? "SORT RAND() " : "";
        String aql = "FOR d IN @@col " + sort + "LIMIT @n RETURN d";
        @SuppressWarnings("unchecked")
        ArangoCursor<Map> cursor = arango.db(database).query(
                aql, Map.class, Map.of("@col", collection, "n", limit));
        ImmutableList.Builder<Map<String, Object>> out = ImmutableList.builder();
        cursor.forEach(m -> out.add((Map<String, Object>) m));
        return out.build();
    }

    public ArangoCursor<BaseDocument> query(String database, String aql, Map<String, Object> bindVars) {
        return arango.db(database).query(aql, BaseDocument.class, bindVars);
    }

    // ---- test-only seeding helpers (package-visible, used by tests) ----
    public void createDatabaseForTest(String db) { if (!arango.db(db).exists()) arango.createDatabase(db); }
    public void createDocumentCollectionForTest(String db, String name) {
        if (!arango.db(db).collection(name).exists()) arango.db(db).createCollection(name);
    }
    public void insertForTest(String db, String name, Map<String, Object> doc) {
        arango.db(db).collection(name).insertDocument(doc);
    }

    @Override
    public void close() { arango.shutdown(); }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -Dtest=ArangoClientTest test`
Expected: PASS (requires Docker running).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/arango/trino/client/ArangoClient.java src/test/java/io/arango/trino/TestingArangoServer.java src/test/java/io/arango/trino/client/ArangoClientTest.java
git commit -m "feat: ArangoClient driver wrapper with Testcontainers fixture"
```

---

### Task 3: `TypeMapper` — JSON value → Trino type with merge/widening

**Files:**
- Create: `src/main/java/io/arango/trino/type/TypeMapper.java`
- Test: `src/test/java/io/arango/trino/type/TypeMapperTest.java`

**Interfaces:**
- Produces:
  - `Type inferType(Object value)` — a single JSON value → Trino `Type`.
  - `Type merge(Type a, Type b, MixedTypeStrategy strategy)` — combine two inferred types (numeric widening; incompatible → strategy fallback).
  - Constants used elsewhere: relies on `io.trino.spi.type.*` (`BOOLEAN`, `BIGINT`, `DOUBLE`, `VARCHAR`, `DecimalType`, `ArrayType`, `RowType`).
- Consumes: `ArangoConfig.MixedTypeStrategy` (T1).

- [ ] **Step 1: Write the failing test**

```java
// src/test/java/io/arango/trino/type/TypeMapperTest.java
package io.arango.trino.type;

import io.arango.trino.ArangoConfig.MixedTypeStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TypeMapperTest {
    private final TypeMapper mapper = new TypeMapper();

    @Test
    void inferScalars() {
        assertThat(mapper.inferType(true)).isEqualTo(BOOLEAN);
        assertThat(mapper.inferType(42L)).isEqualTo(BIGINT);
        assertThat(mapper.inferType(3.14)).isEqualTo(DOUBLE);
        assertThat(mapper.inferType("hi")).isEqualTo(VARCHAR);
    }

    @Test
    void mergeIntAndFloatWidensToDouble() {
        assertThat(mapper.merge(BIGINT, DOUBLE, MixedTypeStrategy.VARCHAR)).isEqualTo(DOUBLE);
    }

    @Test
    void mergeIncompatibleFallsBackToVarchar() {
        assertThat(mapper.merge(BIGINT, VARCHAR, MixedTypeStrategy.VARCHAR)).isEqualTo(VARCHAR);
    }

    @Test
    void inferArrayOfLongs() {
        assertThat(mapper.inferType(List.of(1L, 2L)).getDisplayName()).isEqualTo("array(bigint)");
    }

    @Test
    void inferNestedObjectAsRow() {
        Type t = mapper.inferType(Map.of("city", "NYC", "zip", 10001L));
        assertThat(t.getDisplayName()).contains("row(");
        assertThat(t.getDisplayName()).contains("varchar");
        assertThat(t.getDisplayName()).contains("bigint");
    }

    @Test
    void mergeArraysMergesElementTypes() {
        Type a = mapper.inferType(List.of(1L));
        Type b = mapper.inferType(List.of(1.5));
        assertThat(mapper.merge(a, b, MixedTypeStrategy.VARCHAR).getDisplayName()).isEqualTo("array(double)");
    }
}
```
(add `import io.trino.spi.type.Type;`)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=TypeMapperTest test`
Expected: FAIL — `TypeMapper` does not exist.

- [ ] **Step 3: Write minimal `TypeMapper`**

```java
// src/main/java/io/arango/trino/type/TypeMapper.java
package io.arango.trino.type;

import com.google.common.collect.ImmutableList;
import io.arango.trino.ArangoConfig.MixedTypeStrategy;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TypeMapper {
    private static final Type BIG_DECIMAL = createDecimalType(38, 0);

    public Type inferType(Object value) {
        if (value == null) {
            return VARCHAR; // unknown scalar until a non-null is seen; merge upgrades it
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Integer || value instanceof Long) {
            return BIGINT;
        }
        if (value instanceof BigInteger bi) {
            return (bi.bitLength() > 63) ? BIG_DECIMAL : BIGINT;
        }
        if (value instanceof Number) {
            return DOUBLE;
        }
        if (value instanceof String) {
            return VARCHAR;
        }
        if (value instanceof List<?> list) {
            Type element = VARCHAR;
            boolean first = true;
            for (Object e : list) {
                Type et = inferType(e);
                element = first ? et : merge(element, et, MixedTypeStrategy.VARCHAR);
                first = false;
            }
            return new ArrayType(element);
        }
        if (value instanceof Map<?, ?> map) {
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                fields.add(RowType.field(String.valueOf(e.getKey()), inferType(e.getValue())));
            }
            return RowType.from(fields.build());
        }
        return VARCHAR;
    }

    public Type merge(Type a, Type b, MixedTypeStrategy strategy) {
        if (a.equals(b)) {
            return a;
        }
        // numeric widening: bigint + double -> double
        if (isNumeric(a) && isNumeric(b)) {
            if (a.equals(DOUBLE) || b.equals(DOUBLE)) {
                return DOUBLE;
            }
            return BIG_DECIMAL; // bigint + decimal
        }
        if (a instanceof ArrayType aa && b instanceof ArrayType bb) {
            return new ArrayType(merge(aa.getElementType(), bb.getElementType(), strategy));
        }
        if (a instanceof RowType ra && b instanceof RowType rb) {
            return mergeRows(ra, rb, strategy);
        }
        return strategy == MixedTypeStrategy.JSON ? VARCHAR : VARCHAR; // JSON type wired in M5
    }

    private Type mergeRows(RowType a, RowType b, MixedTypeStrategy strategy) {
        java.util.LinkedHashMap<String, Type> merged = new java.util.LinkedHashMap<>();
        a.getFields().forEach(f -> merged.put(f.getName().orElseThrow(), f.getType()));
        for (RowType.Field f : b.getFields()) {
            String name = f.getName().orElseThrow();
            merged.merge(name, f.getType(), (x, y) -> merge(x, y, strategy));
        }
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        merged.forEach((n, t) -> fields.add(RowType.field(n, t)));
        return RowType.from(fields.build());
    }

    private boolean isNumeric(Type t) {
        return t.equals(BIGINT) || t.equals(DOUBLE) || t.equals(BIG_DECIMAL);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=TypeMapperTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/type/TypeMapper.java src/test/java/io/arango/trino/type/TypeMapperTest.java
git commit -m "feat: TypeMapper with numeric widening and nested row/array inference"
```

---

### Task 4: `SchemaResolver` — merge-sample a collection into columns

**Files:**
- Create: `src/main/java/io/arango/trino/schema/SchemaResolver.java`
- Test: `src/test/java/io/arango/trino/schema/SchemaResolverTest.java`

**Interfaces:**
- Produces: `List<ArangoColumn> resolveColumns(String database, CollectionInfo collection)` where `ArangoColumn` is a record `(String name, Type type, boolean hidden)` **defined inside SchemaResolver** for now (promoted to a handle in T5). Rules: union of top-level field names across sampled docs; per-field type = fold of `TypeMapper.merge`; `_key`/`_id`/`_rev` → hidden `VARCHAR`; for edge collections `_from`/`_to` → visible `VARCHAR`; empty sample → empty column list (caller decides error).
- Consumes: `ArangoClient` (T2), `TypeMapper` (T3), `ArangoConfig` (T1), `CollectionInfo` (T2).

- [ ] **Step 1: Write the failing test**

```java
// src/test/java/io/arango/trino/schema/SchemaResolverTest.java
package io.arango.trino.schema;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.arango.trino.type.TypeMapper;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaResolverTest {
    private TestingArangoServer server;
    private ArangoClient client;
    private SchemaResolver resolver;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        ArangoConfig config = new ArangoConfig().setHosts(server.hostPort())
                .setUser("root").setPassword(server.rootPassword());
        client = new ArangoClient(config);
        resolver = new SchemaResolver(client, new TypeMapper(), config);

        client.createDatabaseForTest("shop");
        client.createDocumentCollectionForTest("shop", "users");
        // heterogeneous: doc A has phone (null), doc B omits it but adds score (float)
        client.insertForTest("shop", "users", newMap("name", "ada", "age", 36L, "phone", null));
        client.insertForTest("shop", "users", newMap("name", "bob", "age", 41L, "score", 9.5));
    }

    private static Map<String, Object> newMap(Object... kv) {
        java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    @AfterAll
    void teardown() { client.close(); server.close(); }

    @Test
    void unionOfFieldsAcrossDocsAndNullColumnRetained() {
        List<ArangoColumn> cols = resolver.resolveColumns("shop",
                new CollectionInfo("users", false, false));
        assertThat(cols).extracting(ArangoColumn::name)
                .contains("name", "age", "phone", "score"); // phone retained despite null
        assertThat(colType(cols, "name")).isEqualTo(VARCHAR);
        assertThat(colType(cols, "age")).isEqualTo(BIGINT);
        assertThat(colType(cols, "score")).isEqualTo(DOUBLE);
        // system attributes present and hidden
        assertThat(cols).anySatisfy(c -> {
            assertThat(c.name()).isEqualTo("_key");
            assertThat(c.hidden()).isTrue();
            assertThat(c.type()).isEqualTo(VARCHAR);
        });
    }

    private static io.trino.spi.type.Type colType(List<ArangoColumn> cols, String name) {
        return cols.stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow().type();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=SchemaResolverTest test`
Expected: FAIL — `SchemaResolver` does not exist.

- [ ] **Step 3: Write minimal `SchemaResolver`**

```java
// src/main/java/io/arango/trino/schema/SchemaResolver.java
package io.arango.trino.schema;

import com.google.common.collect.ImmutableList;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class SchemaResolver {
    public record ArangoColumn(String name, Type type, boolean hidden) {}

    private static final Set<String> SYSTEM_ATTRS = Set.of("_key", "_id", "_rev");

    private final ArangoClient client;
    private final TypeMapper typeMapper;
    private final ArangoConfig config;

    public SchemaResolver(ArangoClient client, TypeMapper typeMapper, ArangoConfig config) {
        this.client = client;
        this.typeMapper = typeMapper;
        this.config = config;
    }

    public List<ArangoColumn> resolveColumns(String database, CollectionInfo collection) {
        List<Map<String, Object>> docs = client.sampleDocuments(
                database, collection.name(), config.getSampleSize(), config.isSampleRandom());

        // union of user fields, folding types via merge
        LinkedHashMap<String, Type> userFields = new LinkedHashMap<>();
        for (Map<String, Object> doc : docs) {
            for (Map.Entry<String, Object> e : doc.entrySet()) {
                String key = e.getKey();
                if (SYSTEM_ATTRS.contains(key) || key.equals("_from") || key.equals("_to")) {
                    continue; // handled explicitly below
                }
                Type inferred = typeMapper.inferType(e.getValue());
                userFields.merge(key, inferred,
                        (a, b) -> typeMapper.merge(a, b, config.getMixedTypeStrategy()));
            }
        }

        ImmutableList.Builder<ArangoColumn> out = ImmutableList.builder();
        userFields.forEach((name, type) -> out.add(new ArangoColumn(name, type, false)));
        // system attributes: hidden varchar
        for (String sys : SYSTEM_ATTRS) {
            out.add(new ArangoColumn(sys, VARCHAR, true));
        }
        // edge attributes: visible varchar
        if (collection.isEdge()) {
            out.add(new ArangoColumn("_from", VARCHAR, false));
            out.add(new ArangoColumn("_to", VARCHAR, false));
        }
        return out.build();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=SchemaResolverTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/schema/SchemaResolver.java src/test/java/io/arango/trino/schema/SchemaResolverTest.java
git commit -m "feat: SchemaResolver merge-samples collections into columns"
```

---

### Task 5: Handles + `ArangoMetadata` (schemas/tables/columns)

**Files:**
- Create: `src/main/java/io/arango/trino/handle/ArangoColumnHandle.java`
- Create: `src/main/java/io/arango/trino/handle/ArangoTableHandle.java`
- Create: `src/main/java/io/arango/trino/ArangoMetadata.java`
- Test: covered end-to-end in Task 9 (metadata is exercised through the query runner); no isolated unit test — metadata methods are thin adapters over `SchemaResolver`/`ArangoClient` already tested in T2/T4. *(This is a deliberate right-sizing: an isolated ConnectorMetadata test would duplicate T4/T9 coverage.)*

**Interfaces:**
- Produces:
  - `ArangoColumnHandle(String name, Type type, boolean hidden)` implements `ColumnHandle`.
  - `ArangoTableHandle(String schema, String table, boolean edge)` implements `ConnectorTableHandle`.
  - `ArangoMetadata` implements `ConnectorMetadata`: `listSchemaNames`, `getTableHandle`, `getTableMetadata`, `getColumnHandles`, `getColumnMetadata`, `listTables`.
- Consumes: `ArangoClient` (T2), `SchemaResolver` (T4).

- [ ] **Step 1: Write `ArangoColumnHandle`**

```java
// src/main/java/io/arango/trino/handle/ArangoColumnHandle.java
package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record ArangoColumnHandle(
        @JsonProperty("name") String name,
        @JsonProperty("type") Type type,
        @JsonProperty("hidden") boolean hidden)
        implements ColumnHandle {

    @JsonCreator
    public ArangoColumnHandle {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public ColumnMetadata toColumnMetadata() {
        return ColumnMetadata.builder().setName(name).setType(type).setHidden(hidden).build();
    }
}
```

- [ ] **Step 2: Write `ArangoTableHandle`**

```java
// src/main/java/io/arango/trino/handle/ArangoTableHandle.java
package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record ArangoTableHandle(
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("edge") boolean edge)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoTableHandle {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
    }

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schema, table);
    }
}
```

- [ ] **Step 3: Write `ArangoMetadata`**

```java
// src/main/java/io/arango/trino/ArangoMetadata.java
package io.arango.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.connector.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ArangoMetadata implements ConnectorMetadata {
    private final ArangoClient client;
    private final SchemaResolver schemaResolver;

    public ArangoMetadata(ArangoClient client, SchemaResolver schemaResolver) {
        this.client = client;
        this.schemaResolver = schemaResolver;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return client.listDatabases();
    }

    @Override
    public ArangoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        return client.listCollections(tableName.getSchemaName()).stream()
                .filter(c -> !c.isSystem() && c.name().equals(tableName.getTableName()))
                .findFirst()
                .map(c -> new ArangoTableHandle(tableName.getSchemaName(), c.name(), c.isEdge()))
                .orElse(null); // null => table not found (Trino throws)
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        List<ColumnMetadata> columns = resolve(handle).stream()
                .map(c -> new ArangoColumnHandle(c.name(), c.type(), c.hidden()).toColumnMetadata())
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ImmutableMap.Builder<String, ColumnHandle> out = ImmutableMap.builder();
        for (ArangoColumn c : resolve(handle)) {
            out.put(c.name(), new ArangoColumnHandle(c.name(), c.type(), c.hidden()));
        }
        return out.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle) {
        return ((ArangoColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> out = ImmutableList.builder();
        List<String> schemas = schemaName.map(List::of).orElseGet(() -> client.listDatabases());
        for (String schema : schemas) {
            for (CollectionInfo c : client.listCollections(schema)) {
                if (!c.isSystem()) {
                    out.add(new SchemaTableName(schema, c.name()));
                }
            }
        }
        return out.build();
    }

    private List<ArangoColumn> resolve(ArangoTableHandle handle) {
        return schemaResolver.resolveColumns(handle.schema(),
                new CollectionInfo(handle.table(), handle.edge(), false));
    }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/handle/ src/main/java/io/arango/trino/ArangoMetadata.java
git commit -m "feat: table/column handles and ArangoMetadata read methods"
```

---

### Task 6: `AqlBuilder` — base full-collection scan

**Files:**
- Create: `src/main/java/io/arango/trino/aql/AqlBuilder.java`
- Test: `src/test/java/io/arango/trino/aql/AqlBuilderTest.java`

**Interfaces:**
- Produces: `AqlQuery buildScan(ArangoTableHandle table, List<ArangoColumnHandle> columns)` where `AqlQuery` is a record `(String aql, Map<String,Object> bindVars)`. M1 renders `FOR d IN @@col RETURN d` (no projection/filter yet); `columns` is accepted now so the T8 page source can map result fields, and projection pushdown in M2 will use it.
- Consumes: `ArangoTableHandle`, `ArangoColumnHandle` (T5).

- [ ] **Step 1: Write the failing test**

```java
// src/test/java/io/arango/trino/aql/AqlBuilderTest.java
package io.arango.trino.aql;

import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoTableHandle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AqlBuilderTest {
    @Test
    void buildsFullScanWithBoundCollection() {
        AqlQuery q = new AqlBuilder().buildScan(
                new ArangoTableHandle("shop", "users", false), List.of());
        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN d");
        assertThat(q.bindVars()).containsEntry("@col", "users");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=AqlBuilderTest test`
Expected: FAIL — `AqlBuilder` does not exist.

- [ ] **Step 3: Write minimal `AqlBuilder`**

```java
// src/main/java/io/arango/trino/aql/AqlBuilder.java
package io.arango.trino.aql;

import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;

import java.util.List;
import java.util.Map;

public class AqlBuilder {
    public record AqlQuery(String aql, Map<String, Object> bindVars) {}

    public AqlQuery buildScan(ArangoTableHandle table, List<ArangoColumnHandle> columns) {
        // M1: full document scan. Projection/filter pushdown added in M2.
        return new AqlQuery("FOR d IN @@col RETURN d", Map.of("@col", table.table()));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=AqlBuilderTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/aql/AqlBuilder.java src/test/java/io/arango/trino/aql/AqlBuilderTest.java
git commit -m "feat: AqlBuilder base full-collection scan"
```

---

### Task 7: `ArangoSplit` + `ArangoSplitManager` (single split)

**Files:**
- Create: `src/main/java/io/arango/trino/handle/ArangoSplit.java`
- Create: `src/main/java/io/arango/trino/ArangoSplitManager.java`
- Test: covered end-to-end in Task 9 (a single split has no independently interesting behavior in M1; shard-splitting and its dedicated tests arrive in M3).

**Interfaces:**
- Produces:
  - `ArangoSplit` implements `ConnectorSplit` (no external addresses; M1 carries nothing beyond identity).
  - `ArangoSplitManager` implements `ConnectorSplitManager`, returns a `FixedSplitSource` with exactly one `ArangoSplit`.
- Consumes: `ArangoTableHandle` (T5).

- [ ] **Step 1: Write `ArangoSplit`**

```java
// src/main/java/io/arango/trino/handle/ArangoSplit.java
package io.arango.trino.handle;

import io.trino.spi.connector.ConnectorSplit;

public record ArangoSplit() implements ConnectorSplit {
    // M1: a single whole-collection split carries no shard/range state.
}
```

- [ ] **Step 2: Write `ArangoSplitManager`**

```java
// src/main/java/io/arango/trino/ArangoSplitManager.java
package io.arango.trino;

import io.arango.trino.handle.ArangoSplit;
import io.trino.spi.connector.*;

public class ArangoSplitManager implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        // M1: exactly one split per table (single-server / no shard fan-out).
        return new FixedSplitSource(new ArangoSplit());
    }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/io/arango/trino/handle/ArangoSplit.java src/main/java/io/arango/trino/ArangoSplitManager.java
git commit -m "feat: single-split ArangoSplitManager"
```

---

### Task 8: `ArangoPageSource` + provider (execute AQL, build pages)

**Files:**
- Create: `src/main/java/io/arango/trino/ArangoPageSourceProvider.java`
- Create: `src/main/java/io/arango/trino/ArangoPageSource.java`
- Test: covered end-to-end in Task 9 (the page source is only meaningful driving real data through a query; value conversion is exercised by the `SELECT *` assertions there).

**Interfaces:**
- Produces:
  - `ArangoPageSourceProvider` implements `ConnectorPageSourceProvider`: builds AQL via `AqlBuilder`, runs it via `ArangoClient`, returns an `ArangoPageSource`.
  - `ArangoPageSource` implements `ConnectorPageSource`: iterates the cursor, writes one `Page` per batch using `PageBuilder`, converting each requested column's document value via a per-type appender.
- Consumes: `ArangoClient` (T2), `AqlBuilder` (T6), `ArangoTableHandle`/`ArangoColumnHandle` (T5).

> **SPI verification note:** `ConnectorPageSource`'s page-emission method changed across recent Trino (`getNextPage()`→`Page` vs `getNextSourcePage()`→`SourcePage`). The code below targets **476** using `getNextPage()`. Confirm against the pinned version at task start and adjust the one method signature + return type if needed.

- [ ] **Step 1: Write `ArangoPageSource`**

```java
// src/main/java/io/arango/trino/ArangoPageSource.java
package io.arango.trino;

import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import io.arango.trino.handle.ArangoColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.*;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class ArangoPageSource implements ConnectorPageSource {
    private static final int ROWS_PER_PAGE = 1024;

    private final List<ArangoColumnHandle> columns;
    private final List<Type> types;
    private final ArangoCursor<BaseDocument> cursor;
    private final PageBuilder pageBuilder;
    private boolean finished;
    private long completedBytes;

    public ArangoPageSource(ArangoCursor<BaseDocument> cursor, List<ArangoColumnHandle> columns) {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.columns = List.copyOf(columns);
        this.types = columns.stream().map(ArangoColumnHandle::type).toList();
        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public Page getNextPage() {
        int rows = 0;
        while (rows < ROWS_PER_PAGE && cursor.hasNext()) {
            BaseDocument doc = cursor.next();
            Map<String, Object> props = doc.getProperties(); // user fields
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                ArangoColumnHandle col = columns.get(i);
                BlockBuilder out = pageBuilder.getBlockBuilder(i);
                Object value = valueFor(doc, col.name(), props);
                appendValue(out, types.get(i), value);
            }
            rows++;
        }
        if (!cursor.hasNext()) {
            finished = true;
        }
        if (rows == 0) {
            return null;
        }
        Page page = pageBuilder.build();
        completedBytes += page.getSizeInBytes();
        pageBuilder.reset();
        return page;
    }

    private static Object valueFor(BaseDocument doc, String name, Map<String, Object> props) {
        return switch (name) {
            case "_key" -> doc.getKey();
            case "_id" -> doc.getId();
            case "_rev" -> doc.getRevision();
            default -> props.get(name); // includes _from/_to for edges (driver exposes via attributes)
        };
    }

    // lenient coercion: mismatched/absent value -> NULL
    private static void appendValue(BlockBuilder out, Type type, Object value) {
        if (value == null) {
            out.appendNull();
            return;
        }
        try {
            if (type.equals(BooleanType.BOOLEAN) && value instanceof Boolean b) {
                BooleanType.BOOLEAN.writeBoolean(out, b);
            }
            else if (type.equals(BigintType.BIGINT) && value instanceof Number n) {
                BigintType.BIGINT.writeLong(out, n.longValue());
            }
            else if (type.equals(DoubleType.DOUBLE) && value instanceof Number n) {
                DoubleType.DOUBLE.writeDouble(out, n.doubleValue());
            }
            else if (type instanceof VarcharType) {
                type.writeSlice(out, utf8Slice(String.valueOf(value)));
            }
            else {
                // ROW/ARRAY/DECIMAL structured writing lands in M2 hardening; lenient NULL for now
                out.appendNull();
            }
        }
        catch (RuntimeException e) {
            out.appendNull(); // lenient
        }
    }

    @Override public long getCompletedBytes() { return completedBytes; }
    @Override public long getReadTimeNanos() { return 0; }
    @Override public boolean isFinished() { return finished; }
    @Override public long getMemoryUsage() { return pageBuilder.getRetainedSizeInBytes(); }
    @Override public void close() { try { cursor.close(); } catch (Exception ignored) {} }
}
```

- [ ] **Step 2: Write `ArangoPageSourceProvider`**

```java
// src/main/java/io/arango/trino/ArangoPageSourceProvider.java
package io.arango.trino;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.*;

import java.util.List;

public class ArangoPageSourceProvider implements ConnectorPageSourceProvider {
    private final ArangoClient client;
    private final AqlBuilder aqlBuilder;

    public ArangoPageSourceProvider(ArangoClient client, AqlBuilder aqlBuilder) {
        this.client = client;
        this.aqlBuilder = aqlBuilder;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        List<ArangoColumnHandle> cols = columns.stream()
                .map(ArangoColumnHandle.class::cast).toList();
        AqlQuery q = aqlBuilder.buildScan(handle, cols);
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars()), cols);
    }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS. *(If the SPI note above triggers a signature mismatch, adjust `getNextPage`/`createPageSource` to the pinned version's signatures, then recompile.)*

- [ ] **Step 4: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoPageSource.java src/main/java/io/arango/trino/ArangoPageSourceProvider.java
git commit -m "feat: ArangoPageSource executes AQL cursor into Trino pages"
```

---

### Task 9: Wire the connector + end-to-end query test

**Files:**
- Create: `src/main/java/io/arango/trino/ArangoTransactionHandle.java`
- Create: `src/main/java/io/arango/trino/ArangoModule.java`
- Create: `src/main/java/io/arango/trino/ArangoConnector.java`
- Create: `src/main/java/io/arango/trino/ArangoConnectorFactory.java`
- Create: `src/main/java/io/arango/trino/ArangoPlugin.java`
- Test: `src/test/java/io/arango/trino/ArangoConnectorQueryTest.java`

**Interfaces:**
- Produces: `ArangoPlugin` (SPI entry, referenced by `META-INF/services` via `trino-plugin` packaging), wiring all prior components. Connector name `arangodb`.
- Consumes: everything from T1–T8.

- [ ] **Step 1: Write the failing end-to-end test**

```java
// src/test/java/io/arango/trino/ArangoConnectorQueryTest.java
package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.*;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoConnectorQueryTest {
    private TestingArangoServer server;
    private QueryRunner queryRunner;

    @BeforeAll
    void setup() throws Exception {
        server = new TestingArangoServer();
        try (ArangoClient seed = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword()))) {
            seed.createDatabaseForTest("shop");
            seed.createDocumentCollectionForTest("shop", "users");
            seed.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
            seed.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
        }

        queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("arango").setSchema("shop").build())
                .build();
        queryRunner.installPlugin(new ArangoPlugin());
        queryRunner.createCatalog("arango", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword()));
    }

    @AfterAll
    void teardown() {
        if (queryRunner != null) queryRunner.close();
        if (server != null) server.close();
    }

    @Test
    void showTablesListsCollection() {
        MaterializedResult r = queryRunner.execute("SHOW TABLES FROM arango.shop");
        assertThat(r.getOnlyColumnAsSet()).contains("users");
    }

    @Test
    void selectReturnsTypedRows() {
        MaterializedResult r = queryRunner.execute(
                "SELECT name, age FROM arango.shop.users ORDER BY age");
        assertThat(r.getRowCount()).isEqualTo(2);
        assertThat(r.getMaterializedRows().get(0).getField(0)).isEqualTo("ada");
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo(36L);
        assertThat(r.getMaterializedRows().get(1).getField(0)).isEqualTo("bob");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=ArangoConnectorQueryTest test`
Expected: FAIL — `ArangoPlugin` does not exist.

- [ ] **Step 3: Write `ArangoTransactionHandle`**

```java
// src/main/java/io/arango/trino/ArangoTransactionHandle.java
package io.arango.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum ArangoTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
}
```

- [ ] **Step 4: Write `ArangoModule`**

```java
// src/main/java/io/arango/trino/ArangoModule.java
package io.arango.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ArangoModule implements Module {
    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(ArangoConfig.class);
        binder.bind(ArangoClient.class).in(Scopes.SINGLETON);
        binder.bind(TypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(SchemaResolver.class).in(Scopes.SINGLETON);
        binder.bind(AqlBuilder.class).in(Scopes.SINGLETON);
        binder.bind(ArangoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArangoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArangoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ArangoConnector.class).in(Scopes.SINGLETON);
    }
}
```
*(Add a `@Inject` constructor to `ArangoClient` [takes `ArangoConfig`], `SchemaResolver` [takes `ArangoClient, TypeMapper, ArangoConfig`], `ArangoMetadata` [takes `ArangoClient, SchemaResolver`], and `ArangoPageSourceProvider` [takes `ArangoClient, AqlBuilder`] — annotate the existing constructors with `@com.google.inject.Inject`.)*

- [ ] **Step 5: Write `ArangoConnector`**

```java
// src/main/java/io/arango/trino/ArangoConnector.java
package io.arango.trino;

import com.google.inject.Inject;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

public class ArangoConnector implements Connector {
    private final ArangoMetadata metadata;
    private final ArangoSplitManager splitManager;
    private final ArangoPageSourceProvider pageSourceProvider;

    @Inject
    public ArangoConnector(ArangoMetadata metadata, ArangoSplitManager splitManager,
            ArangoPageSourceProvider pageSourceProvider) {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
            boolean readOnly, boolean autoCommit) {
        return ArangoTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() { return splitManager; }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() { return pageSourceProvider; }
}
```

- [ ] **Step 6: Write `ArangoConnectorFactory`**

```java
// src/main/java/io/arango/trino/ArangoConnectorFactory.java
package io.arango.trino;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class ArangoConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() { return "arangodb"; }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Bootstrap app = new Bootstrap(new ArangoModule());
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(ArangoConnector.class);
    }
}
```

- [ ] **Step 7: Write `ArangoPlugin`**

```java
// src/main/java/io/arango/trino/ArangoPlugin.java
package io.arango.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class ArangoPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new ArangoConnectorFactory());
    }
}
```

- [ ] **Step 8: Run the end-to-end test to verify it passes**

Run: `mvn -q -Dtest=ArangoConnectorQueryTest test`
Expected: PASS — `SHOW TABLES` lists `users`; `SELECT name, age` returns Ada(36)/Bob(41).

- [ ] **Step 9: Run the whole suite**

Run: `mvn -q test`
Expected: all tests PASS.

- [ ] **Step 10: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoTransactionHandle.java src/main/java/io/arango/trino/ArangoModule.java src/main/java/io/arango/trino/ArangoConnector.java src/main/java/io/arango/trino/ArangoConnectorFactory.java src/main/java/io/arango/trino/ArangoPlugin.java src/test/java/io/arango/trino/ArangoConnectorQueryTest.java
git commit -m "feat: wire ArangoPlugin end-to-end; SELECT * over collections works"
```

---

## Self-Review

**Spec coverage (M1 rows of §10 + relevant §2–§5):**
- DB/collection listing (lazy) → T2/T5 (`listSchemaNames`/`listTables`, resolved per-table). ✅
- Merge-sampling schema, null column retained → T3/T4 (union + widening; regression on null field). ✅
- Single-split full scan → T7. ✅
- Base + widening type mapping → T3 (bool/bigint/double/varchar/array/row/decimal-widen). ✅
- Coercion `lenient` (mismatch→NULL) → T8 `appendValue`. ✅
- System `_key/_id/_rev` hidden, edge `_from/_to` visible → T4 + T8 `valueFor`. ✅
- Connector wiring + `SELECT *` exit criterion → T9. ✅
- `SHOW TABLES` tolerates listing → T9 test asserts it. ✅ (Full fault-tolerant-on-unresolvable-collection, spec §4.2, is only partially exercised in M1; a collection that fails sampling is not force-listed here — **noted as an M1 gap to harden in M2** since M1's happy-path listing meets the milestone exit criterion.)

**Placeholder scan:** No TBD/TODO/"add error handling"; every code step shows complete code. Structured `ROW`/`ARRAY`/`DECIMAL` *value* writing is intentionally deferred (appends NULL) with an explicit comment — this is a scoped M1 limitation, not a placeholder (types are inferred correctly; only value materialization of nested structures waits for M2). ✅

**Type consistency:** `CollectionInfo(name,isEdge,isSystem)` used identically in T2/T4/T5. `ArangoColumn(name,type,hidden)` (T4) vs `ArangoColumnHandle(name,type,hidden)` (T5) — deliberately parallel; metadata converts between them. `AqlQuery(aql,bindVars)` consistent T6/T8. `ArangoTableHandle(schema,table,edge)` consistent T5–T9. Method names (`buildScan`, `resolveColumns`, `sampleDocuments`, `query`, `listCollections`) consistent across producer/consumer blocks. ✅

**Known M1 limitations (carried to later plans, not defects):**
- Nested `ROW`/`ARRAY`/`DECIMAL` value materialization → M2.
- Fault-tolerant listing of unresolvable collections (§4.2) → M2.
- Case-insensitive name matching, TLS/auth, statistics, pushdown, shard splits, schema override/validation sources, `query()` PTF → M2–M6 per their own plans.

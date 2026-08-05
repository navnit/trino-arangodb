# M6-C Schema Sources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `trino_schema` override collection fully determines a table's columns (replacing sampling), unlocking declared-only types — `decimal(p,s)`, `timestamp(3)`, `timestamp(3) with time zone` — with exact, bounds-checked materialization.

**Architecture:** A new `SchemaOverrideReader` (existence-probe + one AQL + strict doc validation) is consulted by `SchemaResolver.resolveColumns` before sampling. Declared type strings parse via Trino's `TypeManager` (newly bound from `ConnectorContext`) behind a recursive allowlist (`DeclaredTypes`). `ValueMaterializer` gains three leaf contracts (dual-form decimal, two timestamps); pushdown needs zero changes (allowlist gates auto-decline the new types) but gets decline-proof tests.

**Tech Stack:** Java 25, Trino 483 SPI, ArangoDB Java driver 7.x, Guava cache, JUnit 5 + AssertJ, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-08-06-m6c-schema-sources-design.md` — read it before starting any task; §5.1 is the per-type contract every materializer step implements.

## Global Constraints

- Maven needs `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` is not found. Build/test requires JDK 25 and a **running Docker daemon** (Testcontainers).
- Run a single test class: `mvn test -Dtest=ClassName`; single method: `mvn test -Dtest=ClassName#method`.
- Before every commit: `mvn spotless:apply` (ratcheted google-java-format, AOSP 4-space), then `git add` only your files.
- Conventional commits (`feat:` / `test:` / `docs:`); every commit message ends with the two `Co-Authored-By`-free lines used by this repo's history (plain message is fine).
- New code must pass `mvn checkstyle:check` and `mvn compile spotbugs:check` (allowlisted new files are enforced).
- Error-code doctrine: user-authored input errors → `ARANGODB_SCHEMA_ERROR`; server/unknown failures → `GENERIC_INTERNAL_ERROR`; per-cell read mismatches → existing coercion policy (`lenient` NULL / `strict` `ARANGODB_TYPE_CONVERSION_ERROR`). Under `lenient`, **no stored value may ever fail a query** — every conversion is bounds-checked to a mismatch, never an escaping exception.
- `java.time` semantics note: `LocalDateTime.toEpochSecond(ZoneOffset.UTC)` is total seconds with nanos excluded; `OffsetDateTime.toInstant().toEpochMilli()` throws `ArithmeticException` on overflow — that throw is *relied on* in Task 7.

---

### Task 1: `arangodb.schema-collection` config key

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Produces: `ArangoConfig.getSchemaCollection()` → `String`, default `"trino_schema"`.

- [ ] **Step 1: Write the failing test.** In `ArangoConfigTest`, extend the existing `assertRecordedDefaults` chain with `.setSchemaCollection("trino_schema")` and the `assertFullMapping` props with `.put("arangodb.schema-collection", "my_overrides")` / expected `.setSchemaCollection("my_overrides")` (follow the exact style of the surrounding rows, e.g. `arangodb.schema.cache-ttl`).

- [ ] **Step 2: Run to verify it fails.** `mvn test -Dtest=ArangoConfigTest` — expected: compile error, `setSchemaCollection` undefined.

- [ ] **Step 3: Implement.** In `ArangoConfig`, next to the other schema settings:

```java
private String schemaCollection = "trino_schema";

@NotNull
public String getSchemaCollection() {
    return schemaCollection;
}

@Config("arangodb.schema-collection")
@ConfigDescription(
        "Per-database collection holding user-curated schema override documents")
public ArangoConfig setSchemaCollection(String schemaCollection) {
    this.schemaCollection = schemaCollection;
    return this;
}
```

- [ ] **Step 4: Run to verify it passes.** `mvn test -Dtest=ArangoConfigTest` — expected: PASS.

- [ ] **Step 5: Commit.** `mvn spotless:apply`, then commit `feat: arangodb.schema-collection config key (M6-C)`.

---

### Task 2: `ArangoClient` override-fetch methods + server-assumption pins

**Files:**
- Modify: `src/main/java/io/arango/trino/client/ArangoClient.java`
- Create: `src/test/java/io/arango/trino/schema/AqlSchemaOverrideAssumptionsTest.java`

**Interfaces:**
- Produces: `ArangoClient.collectionExists(String database, String collection)` → `boolean`; `ArangoClient.fetchSchemaOverrideDocs(String database, String schemaCollection, String table)` → `List<Map<String, Object>>` (raw docs, `LIMIT 2`); test helper `ArangoClient.setCollectionAccessForTest(String username, String db, String collection, String grant)`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests.** New container-backed class (model the boilerplate on `SchemaResolverTest`: `@TestInstance(PER_CLASS)`, `TestingArangoServer` in `@BeforeAll`, `server.close()`-equivalent teardown as that class does):

```java
package io.arango.trino.schema;

// imports per SchemaResolverTest, plus com.arangodb.ArangoDBException,
// org.assertj.core.api.Assertions.assertThatThrownBy / catchThrowableOfType

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AqlSchemaOverrideAssumptionsTest {
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig().setHosts(server.hostPort())
                .setUser("root").setPassword(server.rootPassword()));
        client.createDatabaseForTest("ovr");
        client.createDocumentCollectionForTest("ovr", "trino_schema");
        client.insertForTest("ovr", "trino_schema", Map.of(
                "table", "orders",
                "fields", List.of(Map.of("name", "total", "type", "decimal(12,2)"))));
    }

    @Test
    void missingCollectionViaBindParameterIs1203() {
        // Spec §4.5: the @@sc BIND-PARAMETER shape was never exercised by M6-B's
        // literal-reference queries; pin that it really is 1203, not some plan-time code.
        ArangoDBException e = Assertions.catchThrowableOfType(
                () -> client.fetchSchemaOverrideDocs("ovr", "no_such_collection", "orders"),
                ArangoDBException.class);
        assertThat(e.getErrorNum()).isEqualTo(1203);
    }

    @Test
    void fetchReturnsMatchingDocOnly() {
        assertThat(client.fetchSchemaOverrideDocs("ovr", "trino_schema", "orders"))
                .hasSize(1);
        assertThat(client.fetchSchemaOverrideDocs("ovr", "trino_schema", "nope"))
                .isEmpty();
    }

    @Test
    void collectionExistsProbe() {
        assertThat(client.collectionExists("ovr", "trino_schema")).isTrue();
        assertThat(client.collectionExists("ovr", "no_such_collection")).isFalse();
    }

    @Test
    void forbiddenCollectionErrorShape() {
        // Spec §4.5 mandates verifying the real errorNum for a collection-level
        // "grant: none" against a live server, so the reader can give a tailored message.
        client.createReadOnlyUserForTest("ovr", "limited", "pw");
        client.setCollectionAccessForTest("limited", "ovr", "trino_schema", "none");
        try (ArangoClient restricted = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("limited").setPassword("pw"))) {
            ArangoDBException e = Assertions.catchThrowableOfType(
                    () -> restricted.fetchSchemaOverrideDocs("ovr", "trino_schema", "orders"),
                    ArangoDBException.class);
            // First run: temporarily print e.getErrorNum()/e.getResponseCode(), then pin
            // the observed values here AND use the same constants in Task 4's isForbidden.
            // Expected observation: errorNum 11 ("forbidden") and/or HTTP 403.
            assertThat(e.getResponseCode()).isEqualTo(403);
        }
    }
}
```

- [ ] **Step 2: Run to verify failure.** `mvn test -Dtest=AqlSchemaOverrideAssumptionsTest` — expected: compile error (`fetchSchemaOverrideDocs`, `collectionExists`, `setCollectionAccessForTest` undefined).

- [ ] **Step 3: Implement in `ArangoClient`** (production methods near `sampleDocuments`; test helper in the test-only section):

```java
/** Cheap collection-metadata existence probe (no AQL) for the override collection. */
public boolean collectionExists(String database, String collection) {
    return arango.db(database).collection(collection).exists();
}

/**
 * Override docs for one table from the schema-override collection. LIMIT 2, not 1:
 * a second row is how SchemaOverrideReader detects duplicate claims (spec M6-C §4.1).
 */
@SuppressWarnings("unchecked")
public List<Map<String, Object>> fetchSchemaOverrideDocs(
        String database, String schemaCollection, String table) {
    ArangoCursor<Map> cursor = arango.db(database).query(
            "FOR d IN @@sc FILTER d.table == @t LIMIT 2 RETURN d",
            Map.class,
            Map.of("@sc", schemaCollection, "t", table));
    ImmutableList.Builder<Map<String, Object>> out = ImmutableList.builder();
    cursor.forEach(m -> out.add((Map<String, Object>) m));
    return out.build();
}
```

Test helper (mirror `createReadOnlyUserForTest`'s use of the raw `Request` builder as in `listShardIds`):

```java
public void setCollectionAccessForTest(
        String username, String db, String collection, String grant) {
    Request<Map<String, String>> req = new Request.Builder<Map<String, String>>()
            .db("_system")
            .method(Request.Method.PUT)
            .path("/_api/user/" + username + "/database/" + db + "/" + collection)
            .body(Map.of("grant", grant))
            .build();
    arango.execute(req, Void.class);
}
```

- [ ] **Step 4: Run — observe, then pin.** `mvn test -Dtest=AqlSchemaOverrideAssumptionsTest`. If `forbiddenCollectionErrorShape` fails on the pinned value, replace with the observed `errorNum`/`responseCode` (record BOTH in an assertion + comment) — Task 4 depends on these constants. All four tests must pass.

- [ ] **Step 5: Commit.** `feat: ArangoClient override-collection fetch + existence probe; pin 1203/forbidden shapes (M6-C)`.

---

### Task 3: `ARANGODB_SCHEMA_ERROR` + `DeclaredTypes` allowlist

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoErrorCode.java`
- Create: `src/main/java/io/arango/trino/schema/DeclaredTypes.java`
- Test: `src/test/java/io/arango/trino/schema/DeclaredTypesTest.java`

**Interfaces:**
- Consumes: `TypeManager.fromSqlType(String)` (SPI); in tests, `io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER` (already used by `ArangoQueryHandleTest` — trino-main is test-scope via trino-testing; do NOT reach for a container or try to hand-write a `TypeManager` double, `fromSqlType` is not implementable from SPI).
- Produces: `ArangoErrorCode.ARANGODB_SCHEMA_ERROR`; `static Type DeclaredTypes.parse(TypeManager typeManager, String table, String field, String typeString)` — returns the resolved Trino `Type` or throws `TrinoException(ARANGODB_SCHEMA_ERROR, ...)` naming table, field, and the offending string.

- [ ] **Step 1: Write the failing test** (no container — pure JVM):

```java
package io.arango.trino.schema;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.ArangoErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.*;
import org.junit.jupiter.api.Test;

class DeclaredTypesTest {
    private static Type parse(String s) {
        return DeclaredTypes.parse(TESTING_TYPE_MANAGER, "orders", "f", s);
    }

    private static void assertRejected(String s, String messagePart) {
        assertThatThrownBy(() -> parse(s))
                .isInstanceOf(TrinoException.class)
                .hasFieldOrPropertyWithValue(
                        "errorCode", ArangoErrorCode.ARANGODB_SCHEMA_ERROR.toErrorCode())
                .hasMessageContaining("orders").hasMessageContaining("f")
                .hasMessageContaining(messagePart);
    }

    @Test
    void scalarVocabulary() {
        assertThat(parse("boolean")).isEqualTo(BooleanType.BOOLEAN);
        assertThat(parse("bigint")).isEqualTo(BigintType.BIGINT);
        assertThat(parse("double")).isEqualTo(DoubleType.DOUBLE);
        assertThat(parse("varchar")).isEqualTo(VarcharType.VARCHAR);
        assertThat(parse("decimal(12,2)")).isEqualTo(DecimalType.createDecimalType(12, 2));
        assertThat(parse("timestamp(3)")).isEqualTo(TimestampType.TIMESTAMP_MILLIS);
        assertThat(parse("timestamp(3) with time zone"))
                .isEqualTo(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
    }

    @Test
    void aliasSpellingsResolveToAdmittedTypes() {
        // Admission is by resolved Type object (spec §5): bare spellings pass.
        assertThat(parse("timestamp")).isEqualTo(TimestampType.TIMESTAMP_MILLIS);
        assertThat(parse("decimal")).isEqualTo(DecimalType.createDecimalType(38, 0));
        assertThat(parse("timestamp(3) without time zone"))
                .isEqualTo(TimestampType.TIMESTAMP_MILLIS);
    }

    @Test
    void nestedForms() {
        assertThat(parse("array(timestamp(3))"))
                .isEqualTo(new ArrayType(TimestampType.TIMESTAMP_MILLIS));
        assertThat(parse("row(\"amount\" decimal(10,2))")).isInstanceOf(RowType.class);
    }

    @Test
    void rowFieldCaseCanonicalizationPinned() {
        // Spec §3: unquoted identifiers are LOWERCASED by the parser; quoted preserve case.
        // This test pins the behavior the README documents.
        RowType unquoted = (RowType) parse("row(myField varchar)");
        assertThat(unquoted.getFields().get(0).getName().orElseThrow()).isEqualTo("myfield");
        RowType quoted = (RowType) parse("row(\"myField\" varchar)");
        assertThat(quoted.getFields().get(0).getName().orElseThrow()).isEqualTo("myField");
    }

    @Test
    void rejectedFamilies() {
        assertRejected("timestamp(6)", "timestamp");
        assertRejected("timestamp(6) with time zone", "timestamp");
        assertRejected("varchar(10)", "varchar");   // bounded: isUnbounded() gate
        assertRejected("char(3)", "char(3)");
        assertRejected("date", "date");
        assertRejected("time", "time");
        assertRejected("real", "real");
        assertRejected("integer", "integer");
        assertRejected("smallint", "smallint");
        assertRejected("tinyint", "tinyint");
        assertRejected("uuid", "uuid");
        assertRejected("varbinary", "varbinary");
        assertRejected("json", "json");
        assertRejected("array(date)", "date");            // recursion into element
        assertRejected("row(\"a\" time)", "time");        // recursion into field
    }

    @Test
    void parseFailures() {
        assertRejected("decimal(", "decimal(");      // syntactically invalid
        assertRejected("not_a_type", "not_a_type");  // valid syntax, unknown type
    }

    @Test
    void rowStructuralRules() {
        assertRejected("row(varchar)", "named");                  // anonymous field
        assertRejected("row(a varchar, a bigint)", "duplicate");  // duplicate
        assertRejected("row(a varchar, \"A\" bigint)", "duplicate"); // case-insensitive dup
    }
}
```

- [ ] **Step 2: Run to verify failure.** `mvn test -Dtest=DeclaredTypesTest` — expected: compile error (`DeclaredTypes`, `ARANGODB_SCHEMA_ERROR` undefined).

- [ ] **Step 3: Implement.** `ArangoErrorCode`: add `ARANGODB_SCHEMA_ERROR(2, USER_ERROR)` to the enum (comma after `ARANGODB_QUERY_NOT_READ_ONLY(1, USER_ERROR)`). Then:

```java
package io.arango.trino.schema;

import static io.arango.trino.ArangoErrorCode.ARANGODB_SCHEMA_ERROR;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Parses and admits declared type strings from schema-override docs (M6-C spec §5). Parsing is
 * TypeManager.fromSqlType (never a second hand-rolled grammar); admission is a recursive
 * allowlist of exactly what ValueMaterializer can materialize. Alias spellings that resolve to
 * an admitted Type (bare "timestamp"/"decimal", "without time zone") are admitted by design.
 */
final class DeclaredTypes {
    static final String SUPPORTED =
            "boolean, bigint, double, varchar, decimal(p,s), timestamp(3), "
                    + "timestamp(3) with time zone, array(...), row(...)";

    private DeclaredTypes() {}

    static Type parse(TypeManager typeManager, String table, String field, String typeString) {
        Type type;
        try {
            type = typeManager.fromSqlType(typeString);
        } catch (RuntimeException e) {
            // fromSqlType failures arrive as the ENGINE's Guava UncheckedExecutionException
            // wrapping trino-parser's ParsingException or SPI TypeNotFoundException; neither
            // the wrapper (engine classloader) nor ParsingException (plugin-invisible package)
            // can be caught by name from plugin code (spec §5), hence RuntimeException.
            throw error(table, field, "cannot parse type '" + typeString + "'");
        }
        validate(type, table, field);
        return type;
    }

    private static void validate(Type type, String table, String field) {
        if (type.equals(BooleanType.BOOLEAN)
                || type.equals(BigintType.BIGINT)
                || type.equals(DoubleType.DOUBLE)
                || type instanceof DecimalType) {
            return;
        }
        if (type instanceof VarcharType varchar) {
            if (varchar.isUnbounded()) {
                return; // varchar(n) must NOT slip through: the write path is unchecked
            }
            throw error(table, field, "bounded varchar is not supported, use varchar");
        }
        if (type instanceof TimestampType ts && ts.getPrecision() == 3) {
            return;
        }
        if (type instanceof TimestampWithTimeZoneType tz && tz.getPrecision() == 3) {
            return;
        }
        if (type instanceof ArrayType array) {
            validate(array.getElementType(), table, field);
            return;
        }
        if (type instanceof RowType row) {
            Set<String> seen = new HashSet<>();
            for (RowType.Field f : row.getFields()) {
                Optional<String> name = f.getName();
                if (name.isEmpty() || name.get().isEmpty()) {
                    throw error(table, field, "every row field must be named");
                }
                if (!seen.add(name.get().toLowerCase(Locale.ENGLISH))) {
                    throw error(table, field, "duplicate row field '" + name.get() + "'");
                }
                validate(f.getType(), table, field);
            }
            return;
        }
        throw error(
                table, field, "type '" + type.getDisplayName() + "' is not supported");
    }

    private static TrinoException error(String table, String field, String reason) {
        return new TrinoException(
                ARANGODB_SCHEMA_ERROR,
                "Schema override for table '%s', field '%s': %s. Supported types: %s"
                        .formatted(table, field, reason, SUPPORTED));
    }
}
```

- [ ] **Step 4: Run to verify it passes.** `mvn test -Dtest=DeclaredTypesTest` — PASS. If a rejection message assertion fails on wording, align the test's `messagePart` with the implementation (the *code* and *classification* are the contract, not exact prose).

- [ ] **Step 5: Commit.** `feat: ARANGODB_SCHEMA_ERROR + DeclaredTypes recursive allowlist (M6-C)`.

---

### Task 4: `SchemaOverrideReader`

**Files:**
- Create: `src/main/java/io/arango/trino/schema/SchemaOverrideReader.java`
- Test: `src/test/java/io/arango/trino/schema/SchemaOverrideReaderTest.java`

**Interfaces:**
- Consumes: `ArangoClient.collectionExists` / `fetchSchemaOverrideDocs` (Task 2), `DeclaredTypes.parse` (Task 3), `ArangoConfig.getSchemaCollection()` (Task 1), `SchemaResolver.ArangoColumn(String name, Type type, boolean hidden)`.
- Produces: `class SchemaOverrideReader` with constructor `(ArangoClient client, TypeManager typeManager, ArangoConfig config)` (`@com.google.inject.Inject`) and `public Optional<List<SchemaResolver.ArangoColumn>> read(String database, String table)` — user columns only, system attrs are the resolver's job. Methods non-final so tests can subclass `ArangoClient` doubles (house style of `ArangoMetadataTest`).

- [ ] **Step 1: Write the failing test.** No container; hand-written `ArangoClient` subclass doubles + `TESTING_TYPE_MANAGER`:

```java
package io.arango.trino.schema;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.arangodb.ArangoDBException;
import io.arango.trino.ArangoConfig;
import io.arango.trino.ArangoErrorCode;
import io.arango.trino.client.ArangoClient;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaOverrideReaderTest {
    private static final ArangoConfig CONFIG =
            new ArangoConfig().setHosts("localhost:1");

    /** Double: probe result + fetched docs (or a throwing fetch). */
    private static ArangoClient client(boolean exists, List<Map<String, Object>> docs) {
        return new ArangoClient(CONFIG) {
            @Override
            public boolean collectionExists(String db, String c) {
                return exists;
            }

            @Override
            public List<Map<String, Object>> fetchSchemaOverrideDocs(
                    String db, String sc, String t) {
                return docs;
            }
        };
    }

    private static SchemaOverrideReader reader(ArangoClient client) {
        return new SchemaOverrideReader(client, TESTING_TYPE_MANAGER, CONFIG);
    }

    private static Map<String, Object> doc(Object fields) {
        Map<String, Object> d = new HashMap<>();
        d.put("table", "orders");
        d.put("fields", fields);
        d.put("_key", "k");   // the doc's own system attrs are ignored, not unknown keys
        d.put("_id", "trino_schema/k");
        d.put("_rev", "r");
        return d;
    }

    private static Map<String, Object> field(String name, String type) {
        return Map.of("name", name, "type", type);
    }

    @Test
    void absentCollectionIsEmpty() {
        assertThat(reader(client(false, List.of())).read("db", "orders")).isEmpty();
    }

    @Test
    void noMatchingDocIsEmpty() {
        assertThat(reader(client(true, List.of())).read("db", "orders")).isEmpty();
    }

    @Test
    void happyPathParsesColumns() {
        var cols = reader(client(true, List.of(doc(List.of(
                        field("total", "decimal(12,2)"),
                        Map.of("name", "placed_at",
                                "type", "timestamp(3) with time zone",
                                "hidden", true)))))
                .read("db", "orders")
                .orElseThrow();
        assertThat(cols).hasSize(2);
        assertThat(cols.get(0).name()).isEqualTo("total");
        assertThat(cols.get(0).type()).isEqualTo(DecimalType.createDecimalType(12, 2));
        assertThat(cols.get(0).hidden()).isFalse();   // hidden defaults false
        assertThat(cols.get(1).type())
                .isEqualTo(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
        assertThat(cols.get(1).hidden()).isTrue();
    }

    @Test
    void duplicateDocsRejected() {
        assertSchemaError(client(true, List.of(doc(List.of(field("a", "varchar"))),
                doc(List.of(field("a", "varchar"))))), "more than one");
    }

    private static void assertSchemaError(ArangoClient client, String messagePart) {
        assertThatThrownBy(() -> reader(client).read("db", "orders"))
                .isInstanceOf(TrinoException.class)
                .hasFieldOrPropertyWithValue("errorCode",
                        ArangoErrorCode.ARANGODB_SCHEMA_ERROR.toErrorCode())
                .hasMessageContaining("orders")
                .hasMessageContaining(messagePart);
    }

    private static void assertDocRejected(Map<String, Object> document, String messagePart) {
        assertSchemaError(client(true, List.of(document)), messagePart);
    }

    @Test
    void validationMatrix() {
        assertDocRejected(doc(null), "fields");                       // missing fields
        assertDocRejected(doc("nope"), "fields");                     // wrong-typed fields
        assertDocRejected(doc(List.of()), "fields");                  // empty fields
        assertDocRejected(doc(List.of("nope")), "field");             // non-object field
        assertDocRejected(doc(List.of(Map.of("type", "varchar"))), "name");
        assertDocRejected(doc(List.of(Map.of("name", "", "type", "varchar"))), "name");
        assertDocRejected(doc(List.of(Map.of("name", 42, "type", "varchar"))), "name");
        assertDocRejected(doc(List.of(field("_key", "varchar"))), "_");
        assertDocRejected(doc(List.of(Map.of("name", "a"))), "type");
        assertDocRejected(doc(List.of(Map.of("name", "a", "type", 42))), "type");
        assertDocRejected(doc(List.of(field("a", "date"))), "date");  // allowlist wired in
        assertDocRejected(doc(List.of(
                Map.of("name", "a", "type", "varchar", "hidden", "yes"))), "hidden");
        // case-INSENSITIVE duplicate names (Trino folds identifiers):
        assertDocRejected(doc(List.of(field("Total", "varchar"), field("total", "bigint"))),
                "duplicate");
        // unknown keys anywhere -- the "hiden" typo and the deferred "path" key:
        Map<String, Object> hiden = new HashMap<>(field("a", "varchar"));
        hiden.put("hiden", true);
        assertDocRejected(doc(List.of(hiden)), "hiden");
        Map<String, Object> path = new HashMap<>(field("a", "varchar"));
        path.put("path", "x.y");
        assertDocRejected(doc(List.of(path)), "path");
        Map<String, Object> extraTop = doc(List.of(field("a", "varchar")));
        extraTop.put("tabel", "orders");
        assertDocRejected(extraTop, "tabel");
    }

    @Test
    void raceWindow1203IsEmpty() {
        // Collection dropped between probe and fetch: degrade like the probe would have.
        ArangoClient dropped = new ArangoClient(CONFIG) {
            @Override
            public boolean collectionExists(String db, String c) {
                return true;
            }

            @Override
            public List<Map<String, Object>> fetchSchemaOverrideDocs(
                    String db, String sc, String t) {
                throw arangoError(1203, 404);
            }
        };
        assertThat(reader(dropped).read("db", "orders")).isEmpty();
    }

    @Test
    void forbiddenGetsTailoredLoudFailure() {
        ArangoClient forbidden = new ArangoClient(CONFIG) {
            @Override
            public boolean collectionExists(String db, String c) {
                throw arangoError(11, 403);
            }
        };
        assertThatThrownBy(() -> reader(forbidden).read("db", "orders"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("arangodb.schema-collection")
                .hasMessageContaining("grant");
    }

    @Test
    void otherFailuresRethrowGeneric() {
        ArangoClient broken = new ArangoClient(CONFIG) {
            @Override
            public boolean collectionExists(String db, String c) {
                throw arangoError(1000, 500);
            }
        };
        assertThatThrownBy(() -> reader(broken).read("db", "orders"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("trino_schema");
    }

    @Test
    void probeIsCachedPerDatabase() {
        int[] probes = {0};
        ArangoClient counting = new ArangoClient(CONFIG) {
            @Override
            public boolean collectionExists(String db, String c) {
                probes[0]++;
                return false;
            }
        };
        SchemaOverrideReader r = reader(counting);
        r.read("db", "orders");
        r.read("db", "customers");
        r.read("other_db", "orders");
        assertThat(probes[0]).isEqualTo(2); // one per database within the TTL
    }

    /** Build a real ArangoDBException carrying an errorNum/responseCode. */
    private static ArangoDBException arangoError(int errorNum, int responseCode) {
        return new ArangoDBException(
                new com.arangodb.entity.ErrorEntity(), responseCode) {
            @Override
            public Integer getErrorNum() {
                return errorNum;
            }

            @Override
            public Integer getResponseCode() {
                return responseCode;
            }
        };
    }
}
```

Note: if `ErrorEntity`/`ArangoDBException` construction differs in driver 7.13 (constructor visibility), use whatever construction `ArangoMetadataTest`'s error-path doubles already use for 1228 — copy that pattern exactly; the contract under test is `getErrorNum()`/`getResponseCode()`, not the construction.

- [ ] **Step 2: Run to verify failure.** `mvn test -Dtest=SchemaOverrideReaderTest` — compile error, `SchemaOverrideReader` undefined.

- [ ] **Step 3: Implement:**

```java
package io.arango.trino.schema;

import static io.arango.trino.ArangoErrorCode.ARANGODB_SCHEMA_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.arangodb.ArangoDBException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Reads and validates user-curated schema-override docs (M6-C spec §3/§4). Consulted by
 * SchemaResolver before sampling; a present result IS the complete user-column set.
 * Validation is strict and fail-loud: the collection is user-authored, so a typo must
 * never silently change a schema — unknown keys anywhere are rejected.
 */
public class SchemaOverrideReader {
    private static final Logger log = Logger.get(SchemaOverrideReader.class);
    private static final Set<String> DOC_KEYS = Set.of("table", "fields", "_key", "_id", "_rev");
    private static final Set<String> FIELD_KEYS = Set.of("name", "type", "hidden");
    // ArangoDB "forbidden" shape pinned by AqlSchemaOverrideAssumptionsTest.
    private static final int ERROR_FORBIDDEN = 11;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int ERROR_COLLECTION_NOT_FOUND = 1203;

    private final ArangoClient client;
    private final TypeManager typeManager;
    private final ArangoConfig config;
    // The no-override deployment must not be an exception path per table (spec §4.1):
    // probe the collection's existence once per database, cached for the schema TTL.
    private final Cache<String, Boolean> existsCache;

    @com.google.inject.Inject
    public SchemaOverrideReader(ArangoClient client, TypeManager typeManager, ArangoConfig config) {
        this.client = client;
        this.typeManager = typeManager;
        this.config = config;
        this.existsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getSchemaCacheTtl().toMillis(), MILLISECONDS)
                .build();
    }

    public Optional<List<ArangoColumn>> read(String database, String table) {
        String overrideCollection = config.getSchemaCollection();
        boolean exists;
        try {
            exists = existsCache.get(
                    database, () -> client.collectionExists(database, overrideCollection));
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw translate(e.getCause(), overrideCollection);
        }
        if (!exists) {
            log.debug("No schema-override collection '%s' in %s", overrideCollection, database);
            return Optional.empty();
        }
        List<Map<String, Object>> docs;
        try {
            docs = client.fetchSchemaOverrideDocs(database, overrideCollection, table);
        } catch (ArangoDBException e) {
            if (e.getErrorNum() != null && e.getErrorNum() == ERROR_COLLECTION_NOT_FOUND) {
                return Optional.empty(); // dropped between probe and fetch
            }
            throw translate(e, overrideCollection);
        }
        if (docs.isEmpty()) {
            return Optional.empty();
        }
        if (docs.size() > 1) {
            throw error(table, "more than one document claims this table; keep exactly one");
        }
        return Optional.of(parse(table, docs.get(0)));
    }

    private List<ArangoColumn> parse(String table, Map<String, Object> doc) {
        for (String key : doc.keySet()) {
            if (!DOC_KEYS.contains(key)) {
                throw error(table, "unrecognized key '" + key + "'"
                        + ("path".equals(key) ? " ('path' is not yet supported)" : ""));
            }
        }
        if (!(doc.get("fields") instanceof List<?> fields) || fields.isEmpty()) {
            throw error(table, "'fields' must be a non-empty array");
        }
        ImmutableList.Builder<ArangoColumn> out = ImmutableList.builder();
        Set<String> seen = new HashSet<>();
        for (Object raw : fields) {
            if (!(raw instanceof Map<?, ?> field)) {
                throw error(table, "every field must be an object");
            }
            for (Object key : field.keySet()) {
                if (!FIELD_KEYS.contains(String.valueOf(key))) {
                    throw error(table, "unrecognized field key '" + key + "'"
                            + ("path".equals(key) ? " ('path' is not yet supported)" : ""));
                }
            }
            if (!(field.get("name") instanceof String name) || name.isEmpty()) {
                throw error(table, "every field needs a non-empty string 'name'");
            }
            if (name.startsWith("_")) {
                throw error(table, "field '" + name + "': the '_' namespace is reserved"
                        + " (system attributes are added automatically)");
            }
            if (!seen.add(name.toLowerCase(Locale.ENGLISH))) {
                // Trino resolves column identifiers case-insensitively; 'Total' and 'total'
                // would be indistinguishable at query time.
                throw error(table, "duplicate field name '" + name + "'"
                        + " (names are compared case-insensitively)");
            }
            if (!(field.get("type") instanceof String typeString)) {
                throw error(table, "field '" + name + "' needs a string 'type'");
            }
            Type type = DeclaredTypes.parse(typeManager, table, name, typeString);
            Object hidden = field.get("hidden");
            if (hidden != null && !(hidden instanceof Boolean)) {
                throw error(table, "field '" + name + "': 'hidden' must be a boolean");
            }
            out.add(new ArangoColumn(name, type, Boolean.TRUE.equals(hidden)));
        }
        return out.build();
    }

    private TrinoException translate(Throwable cause, String overrideCollection) {
        if (cause instanceof ArangoDBException e && isForbidden(e)) {
            return new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Cannot read schema-override collection '" + overrideCollection
                            + "' (arangodb.schema-collection): grant read on it, or drop it: "
                            + e.getMessage(), cause);
        }
        return new TrinoException(GENERIC_INTERNAL_ERROR,
                "Failed reading schema-override collection '" + overrideCollection + "': "
                        + cause.getMessage(), cause);
    }

    private static boolean isForbidden(ArangoDBException e) {
        return (e.getErrorNum() != null && e.getErrorNum() == ERROR_FORBIDDEN)
                || (e.getResponseCode() != null && e.getResponseCode() == HTTP_FORBIDDEN);
    }

    private TrinoException error(String table, String reason) {
        return new TrinoException(ARANGODB_SCHEMA_ERROR,
                "Schema override for table '%s' in '%s': %s"
                        .formatted(table, config.getSchemaCollection(), reason));
    }
}
```

If Task 2's observed forbidden shape differed from `11`/`403`, use the observed constants here.

- [ ] **Step 4: Run to verify it passes.** `mvn test -Dtest=SchemaOverrideReaderTest` — PASS. Also `mvn test -Dtest=DeclaredTypesTest` (unchanged, still green).

- [ ] **Step 5: Commit.** `feat: SchemaOverrideReader — strict doc validation, probe cache, error translation (M6-C)`.

---

### Task 5: Wire it up — `TypeManager` binding + `SchemaResolver` precedence

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConnectorFactory.java`
- Modify: `src/main/java/io/arango/trino/ArangoModule.java`
- Modify: `src/main/java/io/arango/trino/schema/SchemaResolver.java`
- Test: `src/test/java/io/arango/trino/schema/SchemaResolverTest.java`

**Interfaces:**
- Consumes: `SchemaOverrideReader.read` (Task 4).
- Produces: `SchemaResolver` constructor becomes `(ArangoClient, TypeMapper, ArangoConfig, SchemaOverrideReader)`; `ArangoModule` constructor becomes `ArangoModule(TypeManager typeManager)`. Every later task sees the same resolver behavior; the container e2e (Task 9) proves the Guice graph.

- [ ] **Step 1: Write the failing tests.** In `SchemaResolverTest`:

(a) **Precedence with zero sampling** — a counting `ArangoClient` double (no container round-trip for this case):

```java
@Test
void overridePresentMeansNoSampling() {
    int[] samples = {0};
    ArangoConfig config = new ArangoConfig().setHosts("localhost:1");
    ArangoClient counting = new ArangoClient(config) {
        @Override
        public boolean collectionExists(String db, String c) {
            return true;
        }

        @Override
        public List<Map<String, Object>> fetchSchemaOverrideDocs(
                String db, String sc, String t) {
            return List.of(Map.of("table", t, "fields",
                    List.of(Map.of("name", "total", "type", "decimal(12,2)"))));
        }

        @Override
        public List<Map<String, Object>> sampleDocuments(
                String db, String c, int limit, boolean random) {
            samples[0]++;
            return List.of();
        }
    };
    SchemaResolver r = new SchemaResolver(counting, new TypeMapper(), config,
            new SchemaOverrideReader(counting,
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER, config));
    List<ArangoColumn> cols =
            r.resolveColumns("db", new CollectionInfo("orders", false, false));
    assertThat(samples[0]).isZero();
    // user column + the three hidden system attrs, in that order
    assertThat(cols).extracting(ArangoColumn::name)
            .containsExactly("total", "_key", "_id", "_rev");
    assertThat(cols.get(0).type())
            .isEqualTo(io.trino.spi.type.DecimalType.createDecimalType(12, 2));
}

@Test
void overrideOnEdgeCollectionAppendsFromTo() {
    int[] samples = {0};
    ArangoConfig config = new ArangoConfig().setHosts("localhost:1");
    ArangoClient counting = new ArangoClient(config) {
        @Override
        public boolean collectionExists(String db, String c) {
            return true;
        }

        @Override
        public List<Map<String, Object>> fetchSchemaOverrideDocs(
                String db, String sc, String t) {
            return List.of(Map.of("table", t, "fields",
                    List.of(Map.of("name", "total", "type", "decimal(12,2)"))));
        }

        @Override
        public List<Map<String, Object>> sampleDocuments(
                String db, String c, int limit, boolean random) {
            samples[0]++;
            return List.of();
        }
    };
    SchemaResolver r = new SchemaResolver(counting, new TypeMapper(), config,
            new SchemaOverrideReader(counting,
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER, config));
    List<ArangoColumn> cols =
            r.resolveColumns("db", new CollectionInfo("orders", true, false));
    assertThat(samples[0]).isZero();
    assertThat(cols).extracting(ArangoColumn::name)
            .containsExactly("total", "_key", "_id", "_rev", "_from", "_to");
    // edge attrs are VISIBLE varchar, exactly as on the sampling path
    assertThat(cols.get(4).hidden()).isFalse();
    assertThat(cols.get(5).hidden()).isFalse();
}
```

(b) **Existing container fixtures still resolve identically** — the class's existing tests already cover this; they now need the new constructor arg. Update the `@BeforeAll` construction to:

```java
resolver = new SchemaResolver(client, new TypeMapper(), config,
        new SchemaOverrideReader(client,
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER, config));
```

(no `trino_schema` collection exists in the `shop` fixture database, so every existing assertion must stay green through the probe-miss path — that IS the "absent → sampling unchanged" spec test.)

- [ ] **Step 2: Run to verify failure.** `mvn test -Dtest=SchemaResolverTest` — compile error (constructor arity).

- [ ] **Step 3: Implement.**

`SchemaResolver`: add the field + constructor param (`SchemaOverrideReader overrideReader`), and at the top of `resolveColumns`:

```java
Optional<List<ArangoColumn>> override = overrideReader.read(database, collection.name());
if (override.isPresent()) {
    ImmutableList.Builder<ArangoColumn> out = ImmutableList.builder();
    out.addAll(override.get());
    appendSystemColumns(out, collection);
    return out.build();
}
```

Extract the existing system/edge-attr tail of `resolveColumns` into `private static void appendSystemColumns(ImmutableList.Builder<ArangoColumn> out, CollectionInfo collection)` and call it from both paths (DRY — the two paths must never drift).

`ArangoConnectorFactory.create`: `new Bootstrap(new ArangoModule(context.getTypeManager()))`.

`ArangoModule`: constructor storing `private final TypeManager typeManager;` (requireNonNull), and in `setup`:

```java
binder.bind(TypeManager.class).toInstance(typeManager);
binder.bind(io.arango.trino.schema.SchemaOverrideReader.class).in(Scopes.SINGLETON);
```

- [ ] **Step 4: Run.** `mvn test -Dtest=SchemaResolverTest` — PASS (new tests and all pre-existing ones). Then `mvn test -Dtest=ArangoConnectorQueryTest` — the `DistributedQueryRunner` boot proves the new Guice graph resolves (`TypeManager` instance binding + reader singleton); expected PASS with zero behavior change.

- [ ] **Step 5: Commit.** `feat: bind TypeManager, wire SchemaOverrideReader precedence into SchemaResolver (M6-C)`.

---

### Task 6: `ValueMaterializer` — `decimal(p,s)` contract (dual write, exact fit)

**Files:**
- Modify: `src/main/java/io/arango/trino/type/ValueMaterializer.java`
- Test: `src/test/java/io/arango/trino/type/ValueMaterializerTest.java`

**Interfaces:**
- Consumes/Produces: no signature changes — `writeValue(BlockBuilder, Type, Object, String)` unchanged; behavior per spec §5.1.

- [ ] **Step 1: Read the existing decimal tests.** Open `ValueMaterializerTest` and find every `DECIMAL(38,0)` case. One expectation flips **deliberately** (spec §5.1, one-code-path rule): a numeric **string** under a decimal column previously read as mismatch → it now converts exactly (`"123"` → `123`). Update that assertion in place with a comment citing spec §5.1 ("strings are the real decimal path — ArangoDB has no decimal type"). Every other 38,0 expectation must stay green: fractional double → mismatch (now via inexact `setScale(0)`), `Infinity`/`NaN` → mismatch, integral double ≥ 2⁵³ reads its **exact binary value** (`new BigDecimal(d)`, never `valueOf`), > 38-digit → mismatch.

- [ ] **Step 2: Write the failing tests** (follow the test class's existing block-building/assertion helpers exactly — read two existing decimal tests first and reuse their helper style):

Cases, each under BOTH coercion modes unless stated (lenient asserts NULL, strict asserts `ARANGODB_TYPE_CONVERSION_ERROR`):

```text
decimal(12,2)  [SHORT decimal — the new write path]
  "12.34" (String)        -> 1234 unscaled, value 12.34
  "1E+2" (String)         -> 100.00 (scientific notation accepted)
  "+12.34" (String)       -> 12.34 (leading + accepted)
  " 12.34" (String)       -> mismatch (whitespace rejected by new BigDecimal)
  "abc" (String)          -> mismatch
  0.25 (Double)           -> 0.25 (exact binary fit at s=2)
  12.34 (Double)          -> mismatch (exact binary value of the double is 12.3399…)
  42L (Long)              -> 42.00
  123456789012L (Long)    -> mismatch (precision overflow: 12 digits + scale 2 = 14 unscaled digits > p=12)
  true (Boolean)          -> mismatch
decimal(20,4)  [LONG decimal, fractional scale]
  "12345678901234.5678"   -> exact
decimal(38,0)  [regression: unchanged behavior except the string flip]
  1e19 (Double)           -> 10000000000000000000 (uint64 case, unchanged)
  "123" (String)          -> 123 (the DELIBERATE flip, Step 1)
```

- [ ] **Step 3: Run to verify failure.** `mvn test -Dtest=ValueMaterializerTest` — the `decimal(12,2)` cases fail with `UnsupportedOperationException` from `ShortDecimalType`'s default `writeObject` (this failure is itself review-finding B2 reproduced); string cases fail as mismatch.

- [ ] **Step 4: Implement.** Replace the `DecimalType` branch and `integralValueOf` with:

```java
if (type instanceof DecimalType decimalType) {
    BigInteger unscaled = unscaledExact(value, decimalType.getScale());
    // Overflow gate BEFORE writing (M4 review B1): an oversized value must be a
    // mismatch (lenient NULL), not an ArithmeticException/Int128 throw.
    if (unscaled != null && !Decimals.overflows(unscaled, decimalType.getPrecision())) {
        if (decimalType.isShort()) {
            // p <= 18 => |unscaled| < 10^18 < 2^63: longValueExact cannot throw here.
            decimalType.writeLong(out, unscaled.longValueExact());
        } else {
            decimalType.writeObject(out, Int128.valueOf(unscaled));
        }
        return;
    }
}
mismatch(out, type, value, columnName);
```

```java
/**
 * Exact conversion to the column's scale, or null on any mismatch (M6-C spec §5.1).
 * Doubles convert via new BigDecimal(d) — the double's EXACT binary value, never
 * BigDecimal.valueOf: valueOf's shortest round-trip repr would silently diverge from the
 * "read exactly what's stored" invariant for integral doubles >= 2^53 (M4). Consequence:
 * a stored double matches only when its exact binary value fits scale s (0.25 does at
 * s=2; 12.34 does not) — decimal STRINGS are the intended encoding. setScale with no
 * rounding is both the fractional gate (s=0 keeps DECIMAL(38,0)'s integral-only rule)
 * and the exact-fit gate.
 */
private static BigInteger unscaledExact(Object value, int scale) {
    BigDecimal dec;
    if (value instanceof Long || value instanceof Integer
            || value instanceof Short || value instanceof Byte) {
        dec = BigDecimal.valueOf(((Number) value).longValue());
    } else if (value instanceof BigInteger bi) {
        dec = new BigDecimal(bi);
    } else if (value instanceof Double || value instanceof Float) {
        double d = ((Number) value).doubleValue();
        if (!Double.isFinite(d)) {
            return null;
        }
        dec = new BigDecimal(d);
    } else if (value instanceof String s) {
        try {
            dec = new BigDecimal(s); // accepts 1E+2 and +x; rejects whitespace
        } catch (NumberFormatException e) {
            return null;
        }
    } else {
        return null;
    }
    try {
        return dec.setScale(scale).unscaledValue(); // no rounding: inexact fit => mismatch
    } catch (ArithmeticException e) {
        return null;
    }
}
```

Delete `integralValueOf` (its rationale comment lives on in `unscaledExact`'s Javadoc). Update the class-level or branch comment that said "TypeMapper only ever emits DECIMAL(38,0)" — no longer true, declared overrides emit arbitrary `(p,s)`.

- [ ] **Step 5: Run.** `mvn test -Dtest=ValueMaterializerTest` — PASS, including all pre-existing cases.

- [ ] **Step 6: Commit.** `feat: decimal(p,s) exact materialization with short/long dual write (M6-C)`.

---

### Task 7: `ValueMaterializer` — timestamp leaves, bounds-checked

**Files:**
- Modify: `src/main/java/io/arango/trino/type/ValueMaterializer.java`
- Test: `src/test/java/io/arango/trino/type/ValueMaterializerTest.java`

**Interfaces:**
- Consumes/Produces: `writeValue` signature unchanged. New accepted types: `TimestampType` precision 3 (writes epoch **micros** — short encoding), `TimestampWithTimeZoneType` precision 3 (writes `packDateTimeWithZone(millis, offsetKey)`).

- [ ] **Step 1: Write the failing tests.** Cases (both coercion modes for the mismatch rows; use `TimestampType.TIMESTAMP_MILLIS` / `TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS`; read back via the type's `getObjectValue`/block helpers in whatever style the class's existing assertions use):

```text
timestamp(3):
  "2026-08-05T12:34:56.789"      -> epochMicros of that LocalDateTime at UTC (assert exact long: LocalDateTime.of(...).toEpochSecond(UTC)*1_000_000 + 789_000)
  "2026-08-05t12:34"             -> accepted (lowercase t, missing seconds — parser surface, pinned)
  "2026-08-05 12:34:56"          -> mismatch (space separator)
  "2026-08-05T12:34:56.123456"   -> mismatch (finer than millis, never rounded)
  "2026-08-05T12:34:56+02:00"    -> mismatch (offset belongs to the tz type)
  "+999999999-12-31T23:59:59"    -> mismatch (epoch-micros long overflow, NOT an exception)
  1722854096789L (Number)        -> mismatch (epoch-millis deferred; numbers never match)
timestamp(3) with time zone:
  "2026-08-05T12:34:56.789+05:30" -> packed: unpackMillisUtc == the instant's epochMilli; unpackZoneKey == getTimeZoneKeyForOffset(330)
  "2026-08-05T12:34:56.789Z"      -> packed with UTC key
  "2026-08-05T12:34:56.789"       -> mismatch (local string under tz type)
  "2026-08-05T12:34:56+05:30:15"  -> mismatch (sub-minute offset: TimeZoneKey would throw)
  "2026-08-05T12:34:56+16:00"     -> mismatch (parser allows ±18:00; Trino only ±14:00)
  "2026-08-05T12:34:56.1234+01:00"-> mismatch (finer than millis)
  "+300000-01-01T00:00:00Z"       -> mismatch (52-bit packed-millis overflow, NOT an exception)
nested:
  array(timestamp(3)) with ["2026-08-05T12:00:00", "bad"] -> lenient: [ts, NULL]; strict: error path "col[1]"
  row("amount" decimal(10,2)) with {"amount": "12.34"}    -> reads 12.34 (nested new-leaf recursion works)
```

- [ ] **Step 2: Run to verify failure.** `mvn test -Dtest=ValueMaterializerTest` — new cases fail (timestamp types currently fall through to `mismatch` for ALL values — including valid ones).

- [ ] **Step 3: Implement.** Two new branches in `write(...)` (before the array/row branches), plus helpers:

```java
if (type instanceof TimestampType ts && ts.getPrecision() == 3 && value instanceof String s) {
    Long micros = localIsoToEpochMicros(s);
    if (micros != null) {
        ts.writeLong(out, micros); // short timestamp(3) encoding IS epoch MICROS (not millis)
        return;
    }
}
if (type instanceof TimestampWithTimeZoneType tz
        && tz.getPrecision() == 3
        && value instanceof String s) {
    Long packed = offsetIsoToPackedMillis(s);
    if (packed != null) {
        tz.writeLong(out, packed);
        return;
    }
}
```

```java
/**
 * ISO_LOCAL_DATE_TIME string -> epoch micros, or null on any mismatch (M6-C spec §5.1):
 * unparseable, finer-than-millis fractional seconds (never rounded — rounding would
 * silently disagree with the declared precision), or epoch-micros long overflow
 * (LocalDateTime.parse accepts +999999999-… years).
 */
private static Long localIsoToEpochMicros(String s) {
    LocalDateTime dateTime;
    try {
        dateTime = LocalDateTime.parse(s);
    } catch (DateTimeParseException e) {
        return null;
    }
    if (dateTime.getNano() % 1_000_000 != 0) {
        return null;
    }
    try {
        long micros = Math.multiplyExact(dateTime.toEpochSecond(ZoneOffset.UTC), 1_000_000L);
        return Math.addExact(micros, dateTime.getNano() / 1_000L);
    } catch (ArithmeticException e) {
        return null;
    }
}

/**
 * ISO_OFFSET_DATE_TIME string -> packDateTimeWithZone(millis, offset key), or null on any
 * mismatch: unparseable/local string, finer-than-millis, sub-minute offset (parser accepts
 * +05:30:15, TimeZoneKey does not), offset beyond ±14:00 (parser accepts ±18:00), epoch-milli
 * overflow (toEpochMilli throws ArithmeticException), or 52-bit packed-millis overflow
 * (pack throws IllegalArgumentException). Under lenient, NO stored string may throw.
 */
private static Long offsetIsoToPackedMillis(String s) {
    OffsetDateTime dateTime;
    try {
        dateTime = OffsetDateTime.parse(s);
    } catch (DateTimeParseException e) {
        return null;
    }
    if (dateTime.getNano() % 1_000_000 != 0) {
        return null;
    }
    int offsetSeconds = dateTime.getOffset().getTotalSeconds();
    if (offsetSeconds % 60 != 0) {
        return null;
    }
    int offsetMinutes = offsetSeconds / 60;
    if (offsetMinutes < -14 * 60 || offsetMinutes > 14 * 60) {
        return null;
    }
    try {
        return DateTimeEncoding.packDateTimeWithZone(
                dateTime.toInstant().toEpochMilli(),
                TimeZoneKey.getTimeZoneKeyForOffset(offsetMinutes));
    } catch (ArithmeticException | IllegalArgumentException e) {
        return null;
    }
}
```

Imports: `io.trino.spi.type.TimestampType`, `TimestampWithTimeZoneType`, `DateTimeEncoding`, `TimeZoneKey`; `java.time.LocalDateTime`, `OffsetDateTime`, `ZoneOffset`; `java.time.format.DateTimeParseException`.

- [ ] **Step 4: Run.** `mvn test -Dtest=ValueMaterializerTest` — PASS.

- [ ] **Step 5: Commit.** `feat: timestamp(3) and timestamp(3) with time zone materialization, bounds-checked (M6-C)`.

---

### Task 8: e2e — override-driven table through real SQL

**Files:**
- Modify: `src/test/java/io/arango/trino/ArangoConnectorQueryTest.java`

**Interfaces:**
- Consumes: everything above via the full connector stack (`DistributedQueryRunner` + container). No new interfaces.

- [ ] **Step 1: Write the failing tests.** In the existing fixture setup, add (following the class's established seeding style):

```java
client.createDocumentCollectionForTest("shop", "invoices");
client.insertForTest("shop", "invoices", newMap(
        "total", "12.34",
        "placed_at", "2026-08-05T12:34:56.789+05:30",
        "updated_at", "2026-08-05T12:34:56.789",
        "internal_note", "secret"));
// NOTE: the override below also declares "missplled" (sic), which matches NO stored
// attribute — pinning the spec §3 accepted limitation (all-NULL, both coercion modes).
client.createDocumentCollectionForTest("shop", "trino_schema");
client.insertForTest("shop", "trino_schema", newMap(
        "table", "invoices",
        "fields", List.of(
                Map.of("name", "total", "type", "decimal(12,2)"),
                Map.of("name", "placed_at", "type", "timestamp(3) with time zone"),
                Map.of("name", "updated_at", "type", "timestamp(3)"),
                Map.of("name", "internal_note", "type", "varchar", "hidden", true),
                Map.of("name", "missplled", "type", "varchar"))));
// malformed override for a DIFFERENT table: must only break that table, lazily
client.createDocumentCollectionForTest("shop", "broken");
client.insertForTest("shop", "broken", newMap("x", 1L));
client.insertForTest("shop", "trino_schema", newMap(
        "table", "broken",
        "fields", List.of(Map.of("name", "x", "type", "not_a_type"))));
```

Tests (use the class's existing query/assert helpers):

1. `SELECT total, updated_at, placed_at FROM invoices` — assert `12.34` (decimal), `TIMESTAMP '2026-08-05 12:34:56.789'`, and the tz value at `+05:30`.
2. `SELECT * FROM invoices` returns only the three non-hidden columns (`internal_note` absent); `SELECT internal_note FROM invoices` returns `secret`.
3. `SHOW TABLES` lists `invoices`, `broken`, **and** `trino_schema` (the override collection is an ordinary table — spec §8 decision).
4. `SELECT * FROM broken` fails with message containing `not_a_type` (the lazy `ARANGODB_SCHEMA_ERROR`); `SHOW TABLES` still succeeds.
5. **Observe and pin** `information_schema.columns` behavior with the malformed doc present: run `SELECT column_name FROM information_schema.columns WHERE table_schema = 'shop' AND table_name = 'invoices'` and `SHOW COLUMNS FROM invoices` — if either fails because the engine's fallback resolves `broken` schema-wide, pin the observed failure in the test with a comment marking it an **accepted deviation from master-spec §4.2** (and note it for Task 10's CLAUDE.md update); if they succeed, assert success. Either way the behavior is pinned, not incidental.
6. `SELECT missplled FROM invoices` returns NULL (the misspelled-`name` accepted limitation, spec §3 — pinned, not incidental).
7. A table with no override doc (the existing fixtures, e.g. `users`) resolves exactly as before while `trino_schema` exists — the class's pre-existing assertions double as this proof; state it in a comment.

- [ ] **Step 2: Run.** `mvn test -Dtest=ArangoConnectorQueryTest`. Tasks 1-7 already landed, so these should pass on first run — they are end-to-end proofs, not fail-first units. A failure here is a real integration bug: investigate it, never weaken the assertion.

- [ ] **Step 3: Run to verify it passes.** `mvn test -Dtest=ArangoConnectorQueryTest` — PASS, all pre-existing tests included.

- [ ] **Step 4: Commit.** `test: e2e override-driven table — declared types, hidden, lazy malformed-doc error (M6-C)`.

---

### Task 9: Pushdown decline/positive proofs

**Files:**
- Modify: `src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java` (filter residual proofs)
- Modify: `src/test/java/io/arango/trino/ArangoConnectorAggregationTest.java` (aggregation decline/positive proofs)

**Interfaces:**
- Consumes: Task 8's `invoices` + `trino_schema` fixture pattern (replicate the seeding in whichever class lacks it — do not share mutable fixtures across test classes; each class already owns its own fixture setup).

- [ ] **Step 1: Read both classes' existing EXPLAIN/plan-assertion helpers** (how they prove "pushed" vs "residual"/"declined") and reuse them verbatim.

- [ ] **Step 2: Write the failing-or-green tests.** Seed an override-declared table with a `decimal(12,2)` column, a `timestamp(3)` column, and a `varchar` column (Task 8's fixture shape). Cases — the four **separate** decline code paths plus two positive controls (spec §6):

```text
ArangoConnectorPushdownTest:
  WHERE ts_col > TIMESTAMP '2026-01-01 00:00:00'    -> residual (isPushable declines TimestampType)
  WHERE dec_col = DECIMAL '12.34'                   -> residual (isPushable declines DecimalType)
  WHERE varchar_col = 'x' (same override table)     -> PUSHED (positive: override source doesn't matter)
ArangoConnectorAggregationTest:
  min(ts_col)        -> declined (specFor min/max allowlist)
  sum(dec_col)       -> declined (specFor sum gate)
  count(ts_col)      -> declined (ColumnGuard.predicate gate)
  GROUP BY ts_col    -> declined (grouping-key ColumnGuard path)
  count(*) on the override table            -> PUSHED (positive)
  GROUP BY varchar_col on the override table -> PUSHED (positive)
```

Each declined case must also assert the **result is still correct** (Trino computes it), not just that pushdown didn't happen.

- [ ] **Step 3: Run.** `mvn test -Dtest=ArangoConnectorPushdownTest,ArangoConnectorAggregationTest` — PASS (these should be green immediately; they are proofs pinning "by construction" claims, and a failure means a real M6-C bug — investigate, do not weaken the test).

- [ ] **Step 4: Commit.** `test: pin auto-decline of declared types across all four pushdown paths (M6-C)`.

---

### Task 10: Docs — README + CLAUDE.md

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: README.** Add the config row `arangodb.schema-collection` (default `trino_schema`) to the config table, and a new "Schema overrides" section documenting: the doc shape (the §3 JSON example), replace semantics, the full type vocabulary incl. alias spellings, the **quoting rule for nested row fields** (`row("placedAt" timestamp(3))` — unquoted names are lowercased and will silently read NULL), the misspelled-`name` all-NULL limitation, that decimals should be stored as **strings**, the no-rename consequence of `path` being unsupported, the duplicate-doc error, and the recommendation to create a persistent index on `table`.

- [ ] **Step 2: CLAUDE.md.** Update: the `ArangoConfig` paragraph (new key); the `SchemaResolver` step in the read-path walkthrough (override precedence: probe → fetch → strict validation → replace; 1203/forbidden/other error routing; `ARANGODB_SCHEMA_ERROR`); the `ValueMaterializer` description (new leaves — decimal dual write + exact-binary rule *and the deliberate string-under-decimal flip*, timestamp micros-vs-packed-millis encodings, bounds-to-mismatch rule); the package layout (`SchemaOverrideReader`, `DeclaredTypes`); the milestone line (M6-C shipped); and, if Task 8 Step 1.5 observed an information_schema deviation, record it. NOTICE: unchanged (nothing relocated).

- [ ] **Step 3: Full verification.** `mvn spotless:apply && mvn test` — full suite green (Docker running). Then `mvn checkstyle:check && mvn compile spotbugs:check`.

- [ ] **Step 4: Commit.** `docs: README schema-override usage + CLAUDE.md M6-C architecture notes`.

---

## Execution notes

- Tasks 1→5 are strictly ordered (each consumes the previous task's interface). Tasks 6 and 7 depend only on Task 3's types being in the vocabulary story (they touch `ValueMaterializer` alone) but MUST land before Task 8. Task 9 needs Task 8's fixture pattern. Task 10 is last.
- If any pre-existing test breaks in a way a task didn't predict (the only *predicted* break is the string-under-decimal flip in Task 6 Step 1), STOP and investigate — do not adjust old assertions to make them pass.

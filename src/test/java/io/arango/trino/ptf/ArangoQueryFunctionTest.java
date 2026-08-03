package io.arango.trino.ptf;

import static io.arango.trino.ArangoErrorCode.ARANGODB_QUERY_NOT_READ_ONLY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.airlift.slice.Slices;
import io.arango.trino.ArangoConfig;
import io.arango.trino.ArangoTransactionHandle;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.ptf.ArangoQueryFunction.QueryFunctionHandle;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoQueryFunctionTest {
    private static final String DB = "ptf_test";
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "users");
        client.insertForTest(DB, "users", Map.of("_key", "a", "name", "ann", "age", 36L));
        client.insertForTest(DB, "users", Map.of("_key", "b", "name", "bob", "age", 41L));
    }

    @AfterAll
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private TableFunctionAnalysis analyze(String database, String aql) {
        return analyze(database, aql, new ArangoConfig());
    }

    private TableFunctionAnalysis analyze(String database, String aql, ArangoConfig config) {
        AbstractConnectorTableFunction fn =
                (AbstractConnectorTableFunction)
                        new ArangoQueryFunction(client, new TypeMapper(), config).get();
        // session and access control are unused by the implementation; the engine supplies
        // real ones in the e2e test
        return fn.analyze(
                null,
                ArangoTransactionHandle.INSTANCE,
                Map.of(
                        "DATABASE", new ScalarArgument(VARCHAR, Slices.utf8Slice(database)),
                        "QUERY", new ScalarArgument(VARCHAR, Slices.utf8Slice(aql))),
                null);
    }

    private static List<String> fieldNames(TableFunctionAnalysis analysis) {
        return analysis.getReturnedType().orElseThrow().getFields().stream()
                .map(f -> f.getName().orElseThrow())
                .toList();
    }

    // ---- §4.1: object rows derive a schema exactly like SchemaResolver would ----

    @Test
    void objectRowsDeriveNamedTypedColumns() {
        TableFunctionAnalysis analysis =
                analyze(DB, "FOR d IN users RETURN {name: d.name, age: d.age}");
        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        assertThat(fieldNames(analysis)).containsExactly("name", "age");
        assertThat(descriptor.getFields().get(0).getType()).contains(VarcharType.VARCHAR);
        assertThat(descriptor.getFields().get(1).getType()).contains(BigintType.BIGINT);

        QueryFunctionHandle handle = (QueryFunctionHandle) analysis.getHandle();
        ArangoQueryHandle table = handle.tableHandle();
        assertThat(table.database()).isEqualTo(DB);
        assertThat(table.query()).isEqualTo("FOR d IN users RETURN {name: d.name, age: d.age}");
        assertThat(table.columns())
                .extracting(ArangoColumnHandle::name)
                .containsExactly("name", "age");
        // §4.1: path is List.of(name); the read path extracts by name
        assertThat(table.columns().get(0).path()).containsExactly("name");
        // derived columns are never hidden — a passthrough result has no collection identity
        assertThat(table.columns()).allMatch(c -> !c.hidden());
    }

    @Test
    void systemAttributesAreOrdinaryVisibleColumns() {
        TableFunctionAnalysis analysis = analyze(DB, "FOR d IN users RETURN d");
        assertThat(fieldNames(analysis)).contains("_key", "_id", "_rev", "name", "age");
    }

    @Test
    void nullOnlyFieldResolvesToVarchar() {
        TableFunctionAnalysis analysis = analyze(DB, "FOR x IN [{a: null}, {a: null}] RETURN x");
        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        assertThat(descriptor.getFields().get(0).getType()).contains(VarcharType.VARCHAR);
    }

    @Test
    void caseCollidingKeysBothBecomeColumns() {
        TableFunctionAnalysis analysis = analyze(DB, "RETURN {Name: 1, name: 2}");
        assertThat(fieldNames(analysis)).containsExactlyInAnyOrder("Name", "name");
    }

    @Test
    void schemaIsInferredFromThePrefixOnly() {
        // k = 2: the third row's extra field is invisible (§10.3, recorded limitation)
        TableFunctionAnalysis analysis =
                analyze(
                        DB,
                        "FOR x IN [{a: 1}, {a: 2}, {a: 3, b: 4}] RETURN x",
                        new ArangoConfig().setSampleSize(2));
        assertThat(fieldNames(analysis)).containsExactly("a");
    }

    // ---- §4.1: rejections ----

    @Test
    void nonObjectRowsReject() {
        assertThatThrownBy(() -> analyze(DB, "FOR d IN users RETURN d.name"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("RETURN {");
    }

    @Test
    void mixedBatchRejects() {
        assertThatThrownBy(() -> analyze(DB, "FOR x IN [{a: 1}, 42, \"str\", null] RETURN x"))
                .hasMessageContaining("RETURN {");
    }

    @Test
    void emptyResultRejects() {
        assertThatThrownBy(() -> analyze(DB, "FOR d IN users FILTER false RETURN d"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("no rows");
    }

    @Test
    void emptyObjectsOnlyReject() {
        // zero derivable columns is the same zero-column-table problem as zero rows
        assertThatThrownBy(() -> analyze(DB, "RETURN {}")).hasMessageContaining("no columns");
    }

    @Test
    void emptyStringKeyRejects() {
        assertThatThrownBy(() -> analyze(DB, "RETURN {\"\": 1}"))
                .hasMessageContaining("empty-string attribute key");
    }

    @Test
    void nullArgumentRejects() {
        AbstractConnectorTableFunction fn =
                (AbstractConnectorTableFunction)
                        new ArangoQueryFunction(client, new TypeMapper(), new ArangoConfig()).get();
        assertThatThrownBy(
                        () ->
                                fn.analyze(
                                        null,
                                        ArangoTransactionHandle.INSTANCE,
                                        Map.of(
                                                "DATABASE",
                                                        new ScalarArgument(
                                                                VARCHAR, Slices.utf8Slice(DB)),
                                                "QUERY", new ScalarArgument(VARCHAR, null)),
                                        null))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("QUERY");
    }

    // ---- §3/§3.1/§3.3: the gate, and its ordering before firstBatch ----

    @Test
    void insertIsRejectedWithoutExecuting() {
        long before = client.countWithShardIds(DB, "users", List.of());
        assertThatThrownBy(() -> analyze(DB, "INSERT {x: 1} INTO users"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(ARANGODB_QUERY_NOT_READ_ONLY.toErrorCode()))
                .hasMessageContaining("users");
        // §3.1: gate runs to completion BEFORE firstBatch — the count-unchanged assertion is the
        // one that fails if the ordering ever inverts
        assertThat(client.countWithShardIds(DB, "users", List.of())).isEqualTo(before);
    }

    @Test
    void systemCollectionReadRejects() {
        // _graphs exists in every database; reading it plans as an ordinary read (§3.3)
        assertThatThrownBy(() -> analyze(DB, "FOR g IN _graphs RETURN g"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("_graphs");
    }

    // ---- §9: error translation ----

    @Test
    void unknownDatabaseThrowsSchemaNotFound() {
        assertThatThrownBy(() -> analyze("no_such_db", "FOR d IN users RETURN d"))
                .isInstanceOf(SchemaNotFoundException.class);
    }

    @Test
    void unknownCollectionIsAUserError() {
        assertThatThrownBy(() -> analyze(DB, "FOR d IN nope RETURN d"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));
    }

    @Test
    void syntaxErrorIsAUserErrorCarryingTheServerMessage() {
        assertThatThrownBy(() -> analyze(DB, "THIS IS NOT AQL"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("syntax");
    }
}

package io.arango.trino.schema;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
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

        // nested-UNKNOWN fixtures: object field with an always-null inner attribute,
        // and array field with an always-null element.
        client.createDocumentCollectionForTest("shop", "profiles");
        client.insertForTest("shop", "profiles", newMap(
                "name", "ada",
                "address", newMap("city", "NYC", "zip", null),
                "tags", java.util.Arrays.asList((Object) null)));
        client.insertForTest("shop", "profiles", newMap(
                "name", "bob",
                "address", newMap("city", "LA", "zip", null),
                "tags", java.util.Arrays.asList((Object) null)));

        // edge collection: _from/_to must be visible VARCHAR columns
        client.createEdgeCollectionForTest("shop", "orders");
        Map<String, Object> edgeDoc = newMap("name", "ada");
        edgeDoc.put("_from", "users/1");
        edgeDoc.put("_to", "users/2");
        client.insertForTest("shop", "orders", edgeDoc);

        // empty collection: sample yields zero docs
        client.createDocumentCollectionForTest("shop", "empty_col");
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

    @Test
    void nestedUnknownInsideRowTypeFieldResolvesToVarchar() {
        List<ArangoColumn> cols = resolver.resolveColumns("shop",
                new CollectionInfo("profiles", false, false));
        io.trino.spi.type.Type addressType = colType(cols, "address");
        assertThat(addressType).isInstanceOf(RowType.class);
        RowType rowType = (RowType) addressType;

        RowType.Field cityField = rowType.getFields().stream()
                .filter(f -> f.getName().orElseThrow().equals("city"))
                .findFirst().orElseThrow();
        assertThat(cityField.getType()).isEqualTo(VARCHAR);

        // "zip" was null in every sampled document: it must resolve to VARCHAR,
        // not be left as UNKNOWN buried inside the RowType.
        RowType.Field zipField = rowType.getFields().stream()
                .filter(f -> f.getName().orElseThrow().equals("zip"))
                .findFirst().orElseThrow();
        assertThat(zipField.getType()).isEqualTo(VARCHAR);
    }

    @Test
    void nestedUnknownInsideArrayTypeElementResolvesToVarchar() {
        List<ArangoColumn> cols = resolver.resolveColumns("shop",
                new CollectionInfo("profiles", false, false));
        io.trino.spi.type.Type tagsType = colType(cols, "tags");
        assertThat(tagsType).isInstanceOf(ArrayType.class);
        ArrayType arrayType = (ArrayType) tagsType;

        // "tags" was [null] in every sampled document: the element type must resolve
        // to VARCHAR, not be left as UNKNOWN.
        assertThat(arrayType.getElementType()).isEqualTo(VARCHAR);
    }

    @Test
    void edgeCollectionExposesFromAndToAsVisibleVarchar() {
        List<ArangoColumn> cols = resolver.resolveColumns("shop",
                new CollectionInfo("orders", true, false));
        assertThat(cols).anySatisfy(c -> {
            assertThat(c.name()).isEqualTo("_from");
            assertThat(c.hidden()).isFalse();
            assertThat(c.type()).isEqualTo(VARCHAR);
        });
        assertThat(cols).anySatisfy(c -> {
            assertThat(c.name()).isEqualTo("_to");
            assertThat(c.hidden()).isFalse();
            assertThat(c.type()).isEqualTo(VARCHAR);
        });
    }

    @Test
    void emptySampleYieldsOnlyHiddenSystemColumns() {
        List<ArangoColumn> cols = resolver.resolveColumns("shop",
                new CollectionInfo("empty_col", false, false));
        assertThat(cols).extracting(ArangoColumn::name)
                .containsExactlyInAnyOrder("_key", "_id", "_rev");
        assertThat(cols).allSatisfy(c -> assertThat(c.hidden()).isTrue());
    }

    private static io.trino.spi.type.Type colType(List<ArangoColumn> cols, String name) {
        return cols.stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow().type();
    }
}

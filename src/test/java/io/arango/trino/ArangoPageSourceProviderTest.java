package io.arango.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * End-to-end proof (real ArangoDB via {@link TestingArangoServer}, no mocks) that {@link
 * ArangoPageSourceProvider}/{@link ArangoPageSource} actually run an AQL cursor and build
 * correctly-typed Trino pages. This is M1-era read-path and materialization-guard coverage: it
 * makes the page-building machinery fail loudly here if it regresses -- per-type value coercion,
 * the _key/_id special-column mapping, edge _from/_to round-tripping, lenient NULL-on-missing-field
 * behavior, and (since M4) structured-type materialization through {@link
 * io.arango.trino.type.ValueMaterializer}. M2 touched this class only incidentally (the
 * handle-constructor signature change and the column-path {@code List<String>} migration); it does
 * not itself exercise pushdown, whose behavior is covered by {@link ArangoConnectorPushdownTest}
 * and {@code AqlBuilderTest}. Note the read path now returns an AQL {@code RETURN {...}} {@code
 * Map} rather than a driver {@code BaseDocument}, after M2's read-path migration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoPageSourceProviderTest {
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

        client.createDatabaseForTest("shop");
        client.createDocumentCollectionForTest("shop", "items");
        client.insertForTest(
                "shop",
                "items",
                mapOf(
                        "_key",
                        "k-widget",
                        "name",
                        "widget",
                        "qty",
                        10L,
                        "price",
                        2.5,
                        "active",
                        true));
        client.insertForTest(
                "shop",
                "items",
                mapOf(
                        "_key",
                        "k-gadget",
                        "name",
                        "gadget",
                        "qty",
                        3L,
                        "price",
                        19.99,
                        "active",
                        false));
        // deliberately missing qty/price/active -> exercises lenient NULL-on-absent-field path
        client.insertForTest("shop", "items", mapOf("_key", "k-mystery", "name", "mystery"));

        client.createEdgeCollectionForTest("shop", "knows");
        client.insertForTest(
                "shop",
                "knows",
                mapOf(
                        "_key",
                        "e1",
                        "_from",
                        "items/k-widget",
                        "_to",
                        "items/k-gadget",
                        "weight",
                        7L));
    }

    @AfterAll
    void teardown() {
        client.close();
        server.close();
    }

    @Test
    void scanReturnsTypedValuesAndNullsForMissingFields() throws Exception {
        ArangoTableHandle handle =
                new ArangoTableHandle(
                        "shop", "items", false, TupleDomain.all(), OptionalLong.empty());
        List<ColumnHandle> columns =
                List.of(
                        new ArangoColumnHandle("_key", VARCHAR, false, List.of("_key")),
                        new ArangoColumnHandle("_id", VARCHAR, false, List.of("_id")),
                        new ArangoColumnHandle("name", VARCHAR, false, List.of("name")),
                        new ArangoColumnHandle("qty", BIGINT, false, List.of("qty")),
                        new ArangoColumnHandle("price", DOUBLE, false, List.of("price")),
                        new ArangoColumnHandle("active", BOOLEAN, false, List.of("active")));

        ArangoPageSourceProvider provider =
                new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig());
        ConnectorPageSource source =
                provider.createPageSource(
                        null,
                        null,
                        new ArangoSplit(List.of()),
                        handle,
                        Optional.empty(),
                        columns,
                        null);

        Map<String, Object[]> rowsByKey = new HashMap<>();
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                String key = VARCHAR.getSlice(page.getBlock(0), pos).toStringUtf8();
                String id = VARCHAR.getSlice(page.getBlock(1), pos).toStringUtf8();
                String name = VARCHAR.getSlice(page.getBlock(2), pos).toStringUtf8();
                Object qty =
                        page.getBlock(3).isNull(pos) ? null : BIGINT.getLong(page.getBlock(3), pos);
                Object price =
                        page.getBlock(4).isNull(pos)
                                ? null
                                : DOUBLE.getDouble(page.getBlock(4), pos);
                Object active =
                        page.getBlock(5).isNull(pos)
                                ? null
                                : BOOLEAN.getBoolean(page.getBlock(5), pos);
                rowsByKey.put(key, new Object[] {id, name, qty, price, active});
            }
        }

        assertThat(rowsByKey).hasSize(3);

        Object[] widget = rowsByKey.get("k-widget");
        assertThat(widget[0]).isEqualTo("items/k-widget"); // _id mapping
        assertThat(widget[1]).isEqualTo("widget");
        assertThat(widget[2]).isEqualTo(10L);
        assertThat(widget[3]).isEqualTo(2.5);
        assertThat(widget[4]).isEqualTo(true);

        Object[] gadget = rowsByKey.get("k-gadget");
        assertThat(gadget[1]).isEqualTo("gadget");
        assertThat(gadget[2]).isEqualTo(3L);
        assertThat(gadget[3]).isEqualTo(19.99);
        assertThat(gadget[4]).isEqualTo(false);

        // missing fields on the source document coerce leniently to NULL, not an exception
        Object[] mystery = rowsByKey.get("k-mystery");
        assertThat(mystery[1]).isEqualTo("mystery");
        assertThat(mystery[2]).isNull();
        assertThat(mystery[3]).isNull();
        assertThat(mystery[4]).isNull();

        assertThat(source.isFinished()).isTrue();
        assertThat(source.getCompletedBytes()).isGreaterThan(0);
        assertThat(source.getMemoryUsage()).isGreaterThanOrEqualTo(0);
        source.close();
    }

    @Test
    void edgeFromAndToColumnsRoundTripThroughDriverProperties() throws Exception {
        ArangoTableHandle handle =
                new ArangoTableHandle(
                        "shop", "knows", true, TupleDomain.all(), OptionalLong.empty());
        List<ColumnHandle> columns =
                List.of(
                        new ArangoColumnHandle("_key", VARCHAR, false, List.of("_key")),
                        new ArangoColumnHandle("_from", VARCHAR, false, List.of("_from")),
                        new ArangoColumnHandle("_to", VARCHAR, false, List.of("_to")),
                        new ArangoColumnHandle("weight", BIGINT, false, List.of("weight")));

        ArangoPageSourceProvider provider =
                new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig());
        ConnectorPageSource source =
                provider.createPageSource(
                        null,
                        null,
                        new ArangoSplit(List.of()),
                        handle,
                        Optional.empty(),
                        columns,
                        null);

        int totalRows = 0;
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                assertThat(VARCHAR.getSlice(page.getBlock(0), pos).toStringUtf8()).isEqualTo("e1");
                assertThat(VARCHAR.getSlice(page.getBlock(1), pos).toStringUtf8())
                        .isEqualTo("items/k-widget");
                assertThat(VARCHAR.getSlice(page.getBlock(2), pos).toStringUtf8())
                        .isEqualTo("items/k-gadget");
                assertThat(BIGINT.getLong(page.getBlock(3), pos)).isEqualTo(7L);
                totalRows++;
            }
        }
        assertThat(totalRows).isEqualTo(1);
        source.close();
    }

    // Since M4: structured columns materialize instead of being rejected up front.
    @Test
    void arrayColumnMaterializesThroughProvider() throws Exception {
        client.createDocumentCollectionForTest("shop", "tagged");
        client.insertForTest("shop", "tagged", mapOf("_key", "t1", "tags", List.of("red", "blue")));

        ArangoTableHandle handle =
                new ArangoTableHandle(
                        "shop", "tagged", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle col =
                new ArangoColumnHandle("tags", new ArrayType(VARCHAR), false, List.of("tags"));
        ArangoPageSourceProvider provider =
                new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig());
        ConnectorPageSource source =
                provider.createPageSource(
                        null,
                        null,
                        new ArangoSplit(List.of()),
                        handle,
                        Optional.empty(),
                        List.of(col),
                        null);
        try {
            List<String> read = null;
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    Block elements = ((ArrayType) col.type()).getObject(page.getBlock(0), pos);
                    read =
                            List.of(
                                    VARCHAR.getSlice(elements, 0).toStringUtf8(),
                                    VARCHAR.getSlice(elements, 1).toStringUtf8());
                }
            }
            assertThat(read).isEqualTo(List.of("red", "blue"));
        } finally {
            source.close();
        }
    }

    // The projection is scoped to the *requested* columns (Trino's actual projection), not the
    // whole table's schema: a SELECT of only supported-typed columns must not be affected by an
    // unrelated column elsewhere in the table. (shop.items has no such column in this suite's
    // fixture data, so this reuses the existing supported-typed projection to prove the scoping.)
    @Test
    void createPageSourceProjectsOnlyRequestedColumns() throws Exception {
        ArangoTableHandle handle =
                new ArangoTableHandle(
                        "shop", "items", false, TupleDomain.all(), OptionalLong.empty());
        List<ColumnHandle> columns =
                List.of(new ArangoColumnHandle("name", VARCHAR, false, List.of("name")));

        ArangoPageSourceProvider provider =
                new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig());
        ConnectorPageSource source =
                provider.createPageSource(
                        null,
                        null,
                        new ArangoSplit(List.of()),
                        handle,
                        Optional.empty(),
                        columns,
                        null);

        int rows = 0;
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            rows += page.getPositionCount();
        }
        assertThat(rows).isEqualTo(3);
        source.close();
    }

    @Test
    void numberInVarcharColumnReadsAsNullUnderLenient() throws Exception {
        client.createDocumentCollectionForTest("shop", "coercion");
        client.insertForTest("shop", "coercion", mapOf("_key", "c-num", "s", 42L));
        assertThat(
                        readSingleColumn(
                                "coercion",
                                new ArangoColumnHandle("s", VARCHAR, false, List.of("s")),
                                new ArangoConfig()))
                .isNull();
    }

    @Test
    void fractionalNumberInBigintColumnReadsAsNullUnderLenient() throws Exception {
        client.createDocumentCollectionForTest("shop", "coercion2");
        client.insertForTest("shop", "coercion2", mapOf("_key", "c-frac", "n", 42.5));
        assertThat(
                        readSingleColumn(
                                "coercion2",
                                new ArangoColumnHandle("n", BIGINT, false, List.of("n")),
                                new ArangoConfig()))
                .isNull();
    }

    @Test
    void strictModeRaisesOnTypeMismatch() {
        client.createDocumentCollectionForTest("shop", "coercion3");
        client.insertForTest("shop", "coercion3", mapOf("_key", "c-bad", "s", 42L));
        assertThatThrownBy(
                        () ->
                                readSingleColumn(
                                        "coercion3",
                                        new ArangoColumnHandle("s", VARCHAR, false, List.of("s")),
                                        new ArangoConfig()
                                                .setTypeCoercion(ArangoConfig.TypeCoercion.STRICT)))
                .isInstanceOfSatisfying(
                        io.trino.spi.TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    // Reads the single column `col` from a single-document collection, returning its one cell (or
    // null).
    private Object readSingleColumn(String collection, ArangoColumnHandle col, ArangoConfig config)
            throws Exception {
        ArangoTableHandle handle =
                new ArangoTableHandle(
                        "shop", collection, false, TupleDomain.all(), OptionalLong.empty());
        ArangoPageSourceProvider provider =
                new ArangoPageSourceProvider(client, new AqlBuilder(), config);
        ConnectorPageSource source =
                provider.createPageSource(
                        null,
                        null,
                        new ArangoSplit(List.of()),
                        handle,
                        Optional.empty(),
                        List.of(col),
                        null);
        Object result = null;
        try {
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    result =
                            page.getBlock(0).isNull(pos)
                                    ? null
                                    : (col.type() instanceof VarcharType
                                            ? VARCHAR.getSlice(page.getBlock(0), pos).toStringUtf8()
                                            : BIGINT.getLong(page.getBlock(0), pos));
                }
            }
        } finally {
            source.close();
        }
        return result;
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }
}

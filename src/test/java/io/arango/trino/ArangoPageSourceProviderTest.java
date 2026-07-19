package io.arango.trino;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof (real ArangoDB via {@link TestingArangoServer}, no mocks) that
 * {@link ArangoPageSourceProvider}/{@link ArangoPageSource} actually run an AQL cursor and
 * build correctly-typed Trino pages. T8's brief defers exhaustive read-path coverage to T9's
 * query-runner tests, but this class exists to make the page-building machinery itself --
 * per-type value coercion, the _key/_id/_rev special columns, edge _from/_to exposure via
 * BaseDocument properties, and lenient NULL-on-missing-field behavior -- fail loudly here if
 * it regresses, rather than waiting for T9.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoPageSourceProviderTest {
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort())
                .setUser("root")
                .setPassword(server.rootPassword()));

        client.createDatabaseForTest("shop");
        client.createDocumentCollectionForTest("shop", "items");
        client.insertForTest("shop", "items", mapOf(
                "_key", "k-widget", "name", "widget", "qty", 10L, "price", 2.5, "active", true));
        client.insertForTest("shop", "items", mapOf(
                "_key", "k-gadget", "name", "gadget", "qty", 3L, "price", 19.99, "active", false));
        // deliberately missing qty/price/active -> exercises lenient NULL-on-absent-field path
        client.insertForTest("shop", "items", mapOf("_key", "k-mystery", "name", "mystery"));

        client.createEdgeCollectionForTest("shop", "knows");
        client.insertForTest("shop", "knows", mapOf(
                "_key", "e1", "_from", "items/k-widget", "_to", "items/k-gadget", "weight", 7L));
    }

    @AfterAll
    void teardown() {
        client.close();
        server.close();
    }

    @Test
    void scanReturnsTypedValuesAndNullsForMissingFields() throws Exception {
        ArangoTableHandle handle = new ArangoTableHandle("shop", "items", false);
        List<ColumnHandle> columns = List.of(
                new ArangoColumnHandle("_key", VARCHAR, false),
                new ArangoColumnHandle("_id", VARCHAR, false),
                new ArangoColumnHandle("name", VARCHAR, false),
                new ArangoColumnHandle("qty", BIGINT, false),
                new ArangoColumnHandle("price", DOUBLE, false),
                new ArangoColumnHandle("active", BOOLEAN, false));

        ArangoPageSourceProvider provider = new ArangoPageSourceProvider(client, new AqlBuilder());
        ConnectorPageSource source = provider.createPageSource(null, null, null, handle, columns, null);

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
                Object qty = page.getBlock(3).isNull(pos) ? null : BIGINT.getLong(page.getBlock(3), pos);
                Object price = page.getBlock(4).isNull(pos) ? null : DOUBLE.getDouble(page.getBlock(4), pos);
                Object active = page.getBlock(5).isNull(pos) ? null : BOOLEAN.getBoolean(page.getBlock(5), pos);
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
        ArangoTableHandle handle = new ArangoTableHandle("shop", "knows", true);
        List<ColumnHandle> columns = List.of(
                new ArangoColumnHandle("_key", VARCHAR, false),
                new ArangoColumnHandle("_from", VARCHAR, false),
                new ArangoColumnHandle("_to", VARCHAR, false),
                new ArangoColumnHandle("weight", BIGINT, false));

        ArangoPageSourceProvider provider = new ArangoPageSourceProvider(client, new AqlBuilder());
        ConnectorPageSource source = provider.createPageSource(null, null, null, handle, columns, null);

        int totalRows = 0;
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                assertThat(VARCHAR.getSlice(page.getBlock(0), pos).toStringUtf8()).isEqualTo("e1");
                assertThat(VARCHAR.getSlice(page.getBlock(1), pos).toStringUtf8()).isEqualTo("items/k-widget");
                assertThat(VARCHAR.getSlice(page.getBlock(2), pos).toStringUtf8()).isEqualTo("items/k-gadget");
                assertThat(BIGINT.getLong(page.getBlock(3), pos)).isEqualTo(7L);
                totalRows++;
            }
        }
        assertThat(totalRows).isEqualTo(1);
        source.close();
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }
}

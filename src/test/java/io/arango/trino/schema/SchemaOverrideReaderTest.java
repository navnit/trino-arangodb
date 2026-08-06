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
    private static final ArangoConfig CONFIG = new ArangoConfig().setHosts("localhost:1");

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
        d.put("_key", "k"); // the doc's own system attrs are ignored, not unknown keys
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
        var cols =
                reader(
                                client(
                                        true,
                                        List.of(
                                                doc(
                                                        List.of(
                                                                field("total", "decimal(12,2)"),
                                                                Map.of(
                                                                        "name",
                                                                        "placed_at",
                                                                        "type",
                                                                        "timestamp(3) with time zone",
                                                                        "hidden",
                                                                        true))))))
                        .read("db", "orders")
                        .orElseThrow();
        assertThat(cols).hasSize(2);
        assertThat(cols.get(0).name()).isEqualTo("total");
        assertThat(cols.get(0).type()).isEqualTo(DecimalType.createDecimalType(12, 2));
        assertThat(cols.get(0).hidden()).isFalse(); // hidden defaults false
        assertThat(cols.get(1).type()).isEqualTo(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
        assertThat(cols.get(1).hidden()).isTrue();
    }

    @Test
    void duplicateDocsRejected() {
        assertSchemaError(
                client(
                        true,
                        List.of(
                                doc(List.of(field("a", "varchar"))),
                                doc(List.of(field("a", "varchar"))))),
                "more than one");
    }

    private static void assertSchemaError(ArangoClient client, String messagePart) {
        assertThatThrownBy(() -> reader(client).read("db", "orders"))
                .isInstanceOf(TrinoException.class)
                .hasFieldOrPropertyWithValue(
                        "errorCode", ArangoErrorCode.ARANGODB_SCHEMA_ERROR.toErrorCode())
                .hasMessageContaining("orders")
                .hasMessageContaining(messagePart);
    }

    private static void assertDocRejected(Map<String, Object> document, String messagePart) {
        assertSchemaError(client(true, List.of(document)), messagePart);
    }

    @Test
    void validationMatrix() {
        assertDocRejected(doc(null), "fields"); // missing fields
        assertDocRejected(doc("nope"), "fields"); // wrong-typed fields
        assertDocRejected(doc(List.of()), "fields"); // empty fields
        assertDocRejected(doc(List.of("nope")), "field"); // non-object field
        assertDocRejected(doc(List.of(Map.of("type", "varchar"))), "name");
        assertDocRejected(doc(List.of(Map.of("name", "", "type", "varchar"))), "name");
        assertDocRejected(doc(List.of(Map.of("name", 42, "type", "varchar"))), "name");
        assertDocRejected(doc(List.of(field("_key", "varchar"))), "_");
        assertDocRejected(doc(List.of(Map.of("name", "a"))), "type");
        assertDocRejected(doc(List.of(Map.of("name", "a", "type", 42))), "type");
        assertDocRejected(doc(List.of(field("a", "date"))), "date"); // allowlist wired in
        assertDocRejected(
                doc(List.of(Map.of("name", "a", "type", "varchar", "hidden", "yes"))), "hidden");
        // case-INSENSITIVE duplicate names (Trino folds identifiers):
        assertDocRejected(
                doc(List.of(field("Total", "varchar"), field("total", "bigint"))), "duplicate");
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
        ArangoClient dropped =
                new ArangoClient(CONFIG) {
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
        ArangoClient forbidden =
                new ArangoClient(CONFIG) {
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
        ArangoClient broken =
                new ArangoClient(CONFIG) {
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
        ArangoClient counting =
                new ArangoClient(CONFIG) {
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

    /**
     * Fabricate an ArangoDBException carrying an errorNum/responseCode. The driver class and both
     * getters are non-final; the (String, Integer) constructor already surfaces the response code
     * via getResponseCode(), so only getErrorNum needs overriding.
     */
    private static ArangoDBException arangoError(int errorNum, int responseCode) {
        return new ArangoDBException("test error " + errorNum, responseCode) {
            @Override
            public Integer getErrorNum() {
                return errorNum;
            }
        };
    }
}

package io.arango.trino;

import com.arangodb.ArangoDBException;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Targeted coverage for the three plan-review-mandated behaviors of {@link ArangoMetadata}.
 * The brief right-sizes T5 to no isolated ConnectorMetadata test (full read-path coverage
 * comes from T9's end-to-end query-runner tests), so this class deliberately does not
 * duplicate that: each test below exists only to make one specific correctness-critical
 * behavior fail loudly if it regresses, not to re-verify SchemaResolver/ArangoClient (T2/T4
 * already do that).
 */
class ArangoMetadataTest {

    // Behavior 1: getTableHandle must reject Trino's table-versioning parameters
    // (ConnectorTableVersion) with TrinoException(NOT_SUPPORTED) -- this connector does not
    // support versioned tables in M1. The guard must fire before any client interaction, so
    // a metadata instance with a null client/resolver is sufficient: if the guard were
    // removed, this would NPE on the null client instead of throwing TrinoException.
    @Test
    void getTableHandleRejectsVersionedTableRequests() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ConnectorTableVersion version = new ConnectorTableVersion(PointerType.TEMPORAL, BIGINT, 1L);

        assertThatThrownBy(() -> metadata.getTableHandle(
                null, new SchemaTableName("shop", "users"), Optional.of(version), Optional.empty()))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()));

        assertThatThrownBy(() -> metadata.getTableHandle(
                null, new SchemaTableName("shop", "users"), Optional.empty(), Optional.of(version)))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()));
    }

    // Behavior 2: getTableHandle must catch the driver's ArangoDBException (e.g. database
    // not found) and translate it into a null return -- Trino's normal "table does not
    // exist" signal -- rather than letting the raw driver exception propagate. A stub
    // client that always throws ArangoDBException from listCollections deterministically
    // exercises the catch block without needing a real server or relying on guessing what a
    // real server does for an absent database: if the catch were removed, this test would
    // fail with an uncaught ArangoDBException instead of observing null.
    private static class ThrowingArangoClient extends ArangoClient {
        ThrowingArangoClient() {
            super(new ArangoConfig());
        }

        @Override
        public List<CollectionInfo> listCollections(String database) {
            throw new ArangoDBException("database not found");
        }
    }

    @Test
    void getTableHandleTranslatesArangoDBExceptionToNull() {
        ArangoMetadata metadata = new ArangoMetadata(new ThrowingArangoClient(), null);

        ArangoTableHandle handle = metadata.getTableHandle(
                null, new SchemaTableName("nosuchdb", "sometable"), Optional.empty(), Optional.empty());

        assertThat(handle).isNull();
    }

    // Behavior 3 (plan-review fix S9): resolve() must memoize SchemaResolver.resolveColumns
    // results per SchemaTableName in columnCache, so a single query samples a collection
    // once rather than once per resolve-backed metadata call. A counting SchemaResolver
    // subclass proves the cache is load-bearing: if computeIfAbsent were replaced with a
    // direct call, the second resolve-backed call would increment the counter again and the
    // final assertion would fail.
    private static class CountingSchemaResolver extends SchemaResolver {
        final AtomicInteger calls = new AtomicInteger();
        private final List<ArangoColumn> canned;

        CountingSchemaResolver(List<ArangoColumn> canned) {
            super(null, null, null);
            this.canned = canned;
        }

        @Override
        public List<ArangoColumn> resolveColumns(String database, CollectionInfo collection) {
            calls.incrementAndGet();
            return canned;
        }
    }

    @Test
    void columnCacheMemoizesSchemaResolutionPerTable() {
        CountingSchemaResolver resolver = new CountingSchemaResolver(
                List.of(new ArangoColumn("name", VARCHAR, false)));
        ArangoMetadata metadata = new ArangoMetadata(null, resolver);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false);

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(null, handle);
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(null, handle);

        assertThat(tableMetadata.getColumns()).hasSize(1);
        assertThat(columnHandles).containsKey("name");
        assertThat(resolver.calls.get()).isEqualTo(1);
    }
}

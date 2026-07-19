package io.arango.trino;

import com.arangodb.ArangoDBException;
import com.arangodb.entity.ErrorEntity;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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

    // The driver's two-arg ArangoDBException(String, Integer) constructor sets responseCode,
    // NOT errorNum (verified by decompiling core-7.13.0's ArangoDBException.<init>): errorNum
    // is only populated when the driver deserializes a real server error body into an
    // ErrorEntity and calls ArangoDBException(ErrorEntity). ErrorEntity itself exposes no
    // public constructor/setter for errorNum, so this reflectively sets the private field to
    // faithfully reproduce what a genuine "database not found" (errorNum 1228) response from
    // ArangoDB looks like once the driver has parsed it.
    private static ArangoDBException databaseNotFoundException(String message) {
        try {
            ErrorEntity entity = new ErrorEntity();
            setPrivateField(entity, "errorNum", 1228);
            setPrivateField(entity, "code", 404);
            setPrivateField(entity, "errorMessage", message);
            return new ArangoDBException(entity);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setPrivateField(Object target, String fieldName, Object value) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

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

    // Behavior 2: getTableHandle must catch the driver's ArangoDBException ONLY when it
    // represents ArangoDB's genuine "database not found" error (errorNum 1228) and translate
    // it into a null return -- Trino's normal "table does not exist" signal -- rather than
    // letting the raw driver exception propagate. A stub client that always throws
    // ArangoDBException(msg, 1228) from listCollections deterministically exercises the catch
    // block without needing a real server or relying on guessing what a real server does for
    // an absent database: if the catch were removed, this test would fail with an uncaught
    // ArangoDBException instead of observing null.
    private static class ThrowingArangoClient extends ArangoClient {
        ThrowingArangoClient() {
            super(new ArangoConfig());
        }

        @Override
        public List<CollectionInfo> listCollections(String database) {
            throw databaseNotFoundException("database not found");
        }
    }

    @Test
    void getTableHandleTranslatesArangoDBExceptionToNull() {
        ArangoMetadata metadata = new ArangoMetadata(new ThrowingArangoClient(), null);

        ArangoTableHandle handle = metadata.getTableHandle(
                null, new SchemaTableName("nosuchdb", "sometable"), Optional.empty(), Optional.empty());

        assertThat(handle).isNull();
    }

    // Behavior 2b (human-requested fix): getTableHandle must NOT swallow ArangoDBExceptions
    // that are not the genuine "database not found" case (e.g. auth failures, network
    // partitions surfaced by the driver during listCollections). Those must propagate as a
    // correctly-classified TrinoException rather than silently becoming "table not found". A
    // stub client that throws an ArangoDBException with a different (or absent) errorNum
    // simulates an unclassified/auth driver failure: if the catch were still blanket-matching
    // ArangoDBException, this test would observe null instead of a thrown TrinoException.
    private static class AuthFailingArangoClient extends ArangoClient {
        AuthFailingArangoClient() {
            super(new ArangoConfig());
        }

        @Override
        public List<CollectionInfo> listCollections(String database) {
            throw new ArangoDBException("not authorized", 401);
        }
    }

    @Test
    void getTableHandlePropagatesNonNotFoundArangoDBExceptionAsTrinoException() {
        ArangoMetadata metadata = new ArangoMetadata(new AuthFailingArangoClient(), null);

        assertThatThrownBy(() -> metadata.getTableHandle(
                null, new SchemaTableName("shop", "sometable"), Optional.empty(), Optional.empty()))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
    }

    // Behavior 2c (human-requested fix): listTables must apply the same narrowing -- a
    // database-not-found ArangoDBException for one schema means "zero tables in that schema"
    // (skip it), while any other ArangoDBException must propagate as a TrinoException rather
    // than being silently swallowed (listTables previously had no catch at all, so any
    // ArangoDBException -- including genuine not-found for a stale/dropped schema -- would
    // have propagated raw).
    private static class PerSchemaThrowingArangoClient extends ArangoClient {
        private final String notFoundSchema;
        private final String failingSchema;

        PerSchemaThrowingArangoClient(String notFoundSchema, String failingSchema) {
            super(new ArangoConfig());
            this.notFoundSchema = notFoundSchema;
            this.failingSchema = failingSchema;
        }

        @Override
        public List<String> listDatabases() {
            return List.of(notFoundSchema, failingSchema, "ok");
        }

        @Override
        public List<CollectionInfo> listCollections(String database) {
            if (database.equals(notFoundSchema)) {
                throw databaseNotFoundException("database not found");
            }
            if (database.equals(failingSchema)) {
                throw new ArangoDBException("not authorized", 401);
            }
            return List.of(new CollectionInfo("widgets", false, false));
        }
    }

    @Test
    void listTablesSkipsNotFoundSchemaButKeepsOthers() {
        ArangoMetadata metadata = new ArangoMetadata(
                new PerSchemaThrowingArangoClient("gone", "ok-does-not-fail"), null);

        List<SchemaTableName> tables = metadata.listTables(null, Optional.of("gone"));

        assertThat(tables).isEmpty();
    }

    @Test
    void listTablesPropagatesNonNotFoundArangoDBExceptionAsTrinoException() {
        ArangoMetadata metadata = new ArangoMetadata(
                new PerSchemaThrowingArangoClient("gone", "unauthorized-db"), null);

        assertThatThrownBy(() -> metadata.listTables(null, Optional.of("unauthorized-db")))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()));
    }

    @Test
    void listTablesAcrossAllSchemasSkipsOnlyNotFoundOnes() {
        ArangoMetadata metadata = new ArangoMetadata(
                new PerSchemaThrowingArangoClient("gone", "ok") {
                    @Override
                    public List<String> listDatabases() {
                        return List.of("gone", "ok");
                    }

                    @Override
                    public List<CollectionInfo> listCollections(String database) {
                        if (database.equals("gone")) {
                            throw databaseNotFoundException("database not found");
                        }
                        return List.of(new CollectionInfo("widgets", false, false));
                    }
                }, null);

        List<SchemaTableName> tables = metadata.listTables(null, Optional.empty());

        assertThat(tables).containsExactly(new SchemaTableName("ok", "widgets"));
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
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(null, handle);
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(null, handle);

        assertThat(tableMetadata.getColumns()).hasSize(1);
        assertThat(columnHandles).containsKey("name");
        assertThat(resolver.calls.get()).isEqualTo(1);
    }

    @Test
    void applyFilterPushesEqualityAndDropsFromResidual() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(age, Domain.singleValue(BIGINT, 30L))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isPresent();
        assertThat(result.get().getRemainingFilter().isAll()).isTrue();
        ArangoTableHandle newHandle = (ArangoTableHandle) result.get().getHandle();
        assertThat(newHandle.constraint().getDomains().orElseThrow()).containsEntry(age, Domain.singleValue(BIGINT, 30L));
    }

    @Test
    void applyFilterKeepsNullableRestrictedDomainInResidual() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        Domain nullableRestricted = Domain.create(io.trino.spi.predicate.ValueSet.of(BIGINT, 30L), true);
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(age, nullableRestricted)));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    void applyFilterReturnsEmptyWhenNothingNewToPush() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        TupleDomain<ColumnHandle> alreadyPushed = TupleDomain.withColumnDomains(
                Map.of(age, Domain.singleValue(BIGINT, 30L)));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, alreadyPushed, OptionalLong.empty());
        Constraint sameConstraint = new Constraint(alreadyPushed);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, sameConstraint);

        assertThat(result).isEmpty();
    }
}

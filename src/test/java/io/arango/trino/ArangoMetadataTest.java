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
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
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
    void applyFilterPushesBooleanEqualityAndDropsFromResidual() {
        // BOOLEAN equality is the one predicate still pushed down (only coercion-safe case; see
        // ArangoMetadata.isPushable). It moves onto the handle constraint and leaves no residual.
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle active = new ArangoColumnHandle("active", BOOLEAN, false, List.of("active"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(active, Domain.singleValue(BOOLEAN, true))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isPresent();
        assertThat(result.get().getRemainingFilter().isAll()).isTrue();
        ArangoTableHandle newHandle = (ArangoTableHandle) result.get().getHandle();
        assertThat(newHandle.constraint().getDomains().orElseThrow()).containsEntry(active, Domain.singleValue(BOOLEAN, true));
    }

    @Test
    void applyFilterKeepsBigintEqualityInResidual() {
        // Regression lock for the M2 final-review fix: BIGINT (and VARCHAR/DOUBLE) equality is no
        // longer pushed, because AQL's type-strict `==` can disagree with ArangoPageSource
        // .appendValue's lenient Number->long coercion for an out-of-sample stored value. With
        // nothing pushable, applyFilter has no new constraint to add and returns empty (Trino keeps
        // the whole predicate in its residual filter).
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(age, Domain.singleValue(BIGINT, 30L))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    void applyFilterKeepsNullableRestrictedDomainInResidual() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Domain nullableRestricted = Domain.create(io.trino.spi.predicate.ValueSet.of(BIGINT, 30L), true);
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(age, nullableRestricted)));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    void applyFilterKeepsIsNullInResidual() {
        // AQL's `== null` tests the raw stored value; ArangoPageSource.appendValue leniently
        // coerces type-mismatched values to Trino NULL. A value outside SchemaResolver's sample
        // (or one the inferred type doesn't fit) would answer this predicate differently in AQL
        // than in Trino, so IS NULL must stay residual rather than being pushed.
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(age, Domain.onlyNull(BIGINT))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    void applyFilterKeepsIsNotNullInResidual() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(age, Domain.notNull(BIGINT))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    void applyFilterReturnsEmptyWhenNothingNewToPush() {
        // Fixed-point guard: the pushable BOOLEAN domain is already on the handle, so re-invoking
        // applyFilter with the same constraint adds nothing new and returns empty (prevents an
        // infinite applyFilter loop). Uses BOOLEAN because that is the only type still pushed.
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoColumnHandle active = new ArangoColumnHandle("active", BOOLEAN, false, List.of("active"));
        TupleDomain<ColumnHandle> alreadyPushed = TupleDomain.withColumnDomains(
                Map.of(active, Domain.singleValue(BOOLEAN, true)));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, alreadyPushed, OptionalLong.empty());
        Constraint sameConstraint = new Constraint(alreadyPushed);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, sameConstraint);

        assertThat(result).isEmpty();
    }

    @Test
    void applyLimitPushesAndGuaranteesLimit() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());

        Optional<LimitApplicationResult<ConnectorTableHandle>> result = metadata.applyLimit(null, handle, 10L);

        assertThat(result).isPresent();
        assertThat(result.get().isLimitGuaranteed()).isTrue();
        ArangoTableHandle newHandle = (ArangoTableHandle) result.get().getHandle();
        assertThat(newHandle.limit()).isEqualTo(OptionalLong.of(10L));
    }

    @Test
    void applyLimitDeclinesWhenExistingLimitIsAlreadySmaller() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.of(5L));

        Optional<LimitApplicationResult<ConnectorTableHandle>> result = metadata.applyLimit(null, handle, 10L);

        assertThat(result).isEmpty();
    }

    @Test
    void applyLimitPushesTighterLimitOverExistingLooserOne() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.of(10L));

        Optional<LimitApplicationResult<ConnectorTableHandle>> result = metadata.applyLimit(null, handle, 5L);

        assertThat(result).isPresent();
        assertThat(result.get().isLimitGuaranteed()).isTrue();
        ArangoTableHandle newHandle = (ArangoTableHandle) result.get().getHandle();
        assertThat(newHandle.limit()).isEqualTo(OptionalLong.of(5L));
    }

    @Test
    void applyLimitDeclinesWhenExistingLimitEqualsRequestedLimit() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.of(10L));

        Optional<LimitApplicationResult<ConnectorTableHandle>> result = metadata.applyLimit(null, handle, 10L);

        assertThat(result).isEmpty();
    }

    @Test
    void applyProjectionPushesNestedFieldDereference() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        RowType addressType = RowType.rowType(
                RowType.field("city", VARCHAR),
                RowType.field("zip", VARCHAR));
        ArangoColumnHandle addressColumn = new ArangoColumnHandle("address", addressType, false, List.of("address"));

        Variable addressVar = new Variable("address_0", addressType);
        FieldDereference cityDeref = new FieldDereference(VARCHAR, addressVar, 0);
        Map<String, ColumnHandle> assignments = Map.of("address_0", addressColumn);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(cityDeref), assignments);

        assertThat(result).isPresent();
        List<Assignment> newAssignments = result.get().getAssignments();
        assertThat(newAssignments).hasSize(1);
        ArangoColumnHandle pushedColumn = (ArangoColumnHandle) newAssignments.get(0).getColumn();
        assertThat(pushedColumn.path()).isEqualTo(List.of("address", "city"));
        assertThat(pushedColumn.name()).isEqualTo("address$city");
        assertThat(pushedColumn.type()).isEqualTo(VARCHAR);
        assertThat(result.get().getProjections()).containsExactly(new Variable("address$city", VARCHAR));
    }

    @Test
    void applyProjectionDeclinesDereferenceIntoNonRowType() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle ageColumn = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        // FieldDereference's constructor requires its target's declared type to already be a
        // RowType (it unconditionally casts target.getType() to RowType), so ageVar can't be
        // declared BIGINT here even though the column it resolves to (ageColumn) is BIGINT. The
        // dummy RowType only satisfies that constructor precondition; resolveDereference's
        // decline path under test is driven by ageColumn.type() (BIGINT), not by ageVar's type.
        Variable ageVar = new Variable("age_0", RowType.rowType(RowType.field("x", DOUBLE)));
        FieldDereference bogusDeref = new FieldDereference(DOUBLE, ageVar, 0);
        Map<String, ColumnHandle> assignments = Map.of("age_0", ageColumn);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(bogusDeref), assignments);

        assertThat(result).isEmpty();
    }

    @Test
    void applyProjectionDeclinesPureVariablePassthrough() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle nameColumn = new ArangoColumnHandle("name", VARCHAR, false, List.of("name"));
        Variable nameVar = new Variable("name_0", VARCHAR);
        Map<String, ColumnHandle> assignments = Map.of("name_0", nameColumn);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(nameVar), assignments);

        assertThat(result).isEmpty();
    }

    @Test
    void applyProjectionDeclinesOnSyntheticNameCollisionWithRealColumn() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        RowType addressType = RowType.rowType(RowType.field("city", VARCHAR));
        ArangoColumnHandle addressColumn = new ArangoColumnHandle("address", addressType, false, List.of("address"));
        // A real top-level column whose literal name collides with the synthetic name
        // resolveDereference would produce for address.city ("address" + "$" + "city").
        ArangoColumnHandle collidingColumn = new ArangoColumnHandle("address$city", VARCHAR, false, List.of("address$city"));

        Variable addressVar = new Variable("address_0", addressType);
        FieldDereference cityDeref = new FieldDereference(VARCHAR, addressVar, 0);
        Variable collidingVar = new Variable("col_0", VARCHAR);
        Map<String, ColumnHandle> assignments = Map.of(
                "address_0", addressColumn,
                "col_0", collidingColumn);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(cityDeref, collidingVar), assignments);

        assertThat(result).isEmpty();
    }

    @Test
    void applyProjectionDeclinesWhenRootVariableIsUnresolvable() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        RowType addressType = RowType.rowType(RowType.field("city", VARCHAR));
        Variable unknownVar = new Variable("ghost_0", addressType);
        FieldDereference cityDeref = new FieldDereference(VARCHAR, unknownVar, 0);
        Map<String, ColumnHandle> assignments = Map.of();

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(cityDeref), assignments);

        assertThat(result).isEmpty();
    }

    @Test
    void applyProjectionMergesTwoDereferenceChainsToTheSameNestedColumn() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        RowType addressType = RowType.rowType(RowType.field("city", VARCHAR));
        ArangoColumnHandle addressColumn = new ArangoColumnHandle("address", addressType, false, List.of("address"));
        // Two distinct Variables, both bound to the same base column, dereferencing the same
        // field -- e.g. SELECT address.city, address.city. Same name AND same path: the
        // collision guard must not fire, and the two projections must collapse to one Assignment.
        Variable addressVarA = new Variable("address_0", addressType);
        Variable addressVarB = new Variable("address_1", addressType);
        FieldDereference cityDerefA = new FieldDereference(VARCHAR, addressVarA, 0);
        FieldDereference cityDerefB = new FieldDereference(VARCHAR, addressVarB, 0);
        Map<String, ColumnHandle> assignments = Map.of(
                "address_0", addressColumn,
                "address_1", addressColumn);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(cityDerefA, cityDerefB), assignments);

        assertThat(result).isPresent();
        assertThat(result.get().getAssignments()).hasSize(1);
        Assignment pushed = result.get().getAssignments().get(0);
        assertThat(pushed.getVariable()).isEqualTo("address$city");
        assertThat(((ArangoColumnHandle) pushed.getColumn()).path()).isEqualTo(List.of("address", "city"));
        assertThat(result.get().getProjections()).hasSize(2);
    }

    @Test
    void applyProjectionDeclinesWhenDereferenceLeafIsStructuredType() {
        ArangoMetadata metadata = new ArangoMetadata(null, null);
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        RowType geoType = RowType.rowType(RowType.field("lat", DOUBLE), RowType.field("lng", DOUBLE));
        RowType addressType = RowType.rowType(RowType.field("geo", geoType), RowType.field("city", VARCHAR));
        ArangoColumnHandle addressColumn = new ArangoColumnHandle("address", addressType, false, List.of("address"));

        Variable addressVar = new Variable("address_0", addressType);
        FieldDereference geoDeref = new FieldDereference(geoType, addressVar, 0);
        Map<String, ColumnHandle> assignments = Map.of("address_0", addressColumn);

        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(null, handle, List.of(geoDeref), assignments);

        assertThat(result).isEmpty();
    }
}

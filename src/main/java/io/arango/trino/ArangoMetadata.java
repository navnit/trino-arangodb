package io.arango.trino;

import com.arangodb.ArangoDBException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class ArangoMetadata implements ConnectorMetadata {
    // ArangoDB's own documented error number for "database not found" (stable across the
    // driver). Only this specific error should be translated into Trino's "not found" signal;
    // any other ArangoDBException (auth failure, network partition, etc.) is a real error and
    // must propagate as such rather than being silently misreported as "table doesn't exist".
    private static final int ERROR_DATABASE_NOT_FOUND = 1228;

    private final ArangoClient client;
    private final SchemaResolver schemaResolver;
    private final ArangoConfig config;
    // M1: unbounded per-connector memoization so one SELECT samples a collection once;
    // spec §4.3 TTL cache lands in a later milestone.
    private final Map<SchemaTableName, List<ArangoColumn>> columnCache = new ConcurrentHashMap<>();

    @Inject
    public ArangoMetadata(ArangoClient client, SchemaResolver schemaResolver, ArangoConfig config) {
        this.client = client;
        this.schemaResolver = schemaResolver;
        this.config = config;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return client.listDatabases();
    }

    @Override
    public ArangoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        try {
            return client.listCollections(tableName.getSchemaName()).stream()
                    .filter(c -> !c.isSystem() && c.name().equals(tableName.getTableName()))
                    .findFirst()
                    .map(c -> new ArangoTableHandle(tableName.getSchemaName(), c.name(), c.isEdge(),
                            TupleDomain.all(), OptionalLong.empty()))
                    .orElse(null); // null => table not found (Trino throws)
        }
        catch (ArangoDBException e) {
            if (isDatabaseNotFound(e)) {
                return null; // schema (database) does not exist => table not found
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to look up table '%s' in ArangoDB".formatted(tableName), e);
        }
    }

    private static boolean isDatabaseNotFound(ArangoDBException e) {
        return e.getErrorNum() != null && e.getErrorNum() == ERROR_DATABASE_NOT_FOUND;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        List<ColumnMetadata> columns = resolve(handle).stream()
                .map(c -> new ArangoColumnHandle(c.name(), c.type(), c.hidden(), List.of(c.name())).toColumnMetadata())
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ImmutableMap.Builder<String, ColumnHandle> out = ImmutableMap.builder();
        for (ArangoColumn c : resolve(handle)) {
            out.put(c.name(), new ArangoColumnHandle(c.name(), c.type(), c.hidden(), List.of(c.name())));
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
            List<CollectionInfo> collections;
            try {
                collections = client.listCollections(schema);
            }
            catch (ArangoDBException e) {
                if (isDatabaseNotFound(e)) {
                    continue; // schema (database) does not exist => zero tables in it
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "Failed to list tables in schema '%s' in ArangoDB".formatted(schema), e);
            }
            for (CollectionInfo c : collections) {
                if (!c.isSystem()) {
                    out.add(new SchemaTableName(schema, c.name()));
                }
            }
        }
        return out.build();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        TupleDomain<ColumnHandle> newDomain = constraint.getSummary();
        if (newDomain.isNone() || newDomain.isAll()) {
            return Optional.empty();
        }
        // If a LIMIT is already pushed onto this handle, a further filter push would render as
        // FILTER-before-LIMIT in the single-FOR AQL scan (AqlBuilder.buildScan), reordering the
        // semantics of `(... LIMIT n) WHERE p` (limit-then-filter) into filter-then-limit. Decline the
        // push and leave the filter residual (base-JDBC does the same). The common `WHERE p LIMIT n`
        // shape is unaffected -- Trino pushes the filter first, before any limit exists on the handle.
        if (handle.limit().isPresent()) {
            return Optional.empty();
        }

        Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
        ImmutableMap.Builder<ColumnHandle, Domain> pushedBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnHandle, Domain> residualBuilder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ArangoColumnHandle column = (ArangoColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            if (isPushable(column.type(), domain)) {
                pushedBuilder.put(column, domain);
                if (isPrefilterOnly(column.type(), domain)) {
                    residualBuilder.put(column, domain); // Trino re-checks the AQL prefilter's superset
                }
            }
            else {
                residualBuilder.put(column, domain);
            }
        }

        TupleDomain<ColumnHandle> pushed = TupleDomain.withColumnDomains(pushedBuilder.buildOrThrow());
        TupleDomain<ColumnHandle> residual = TupleDomain.withColumnDomains(residualBuilder.buildOrThrow());

        TupleDomain<ColumnHandle> newHandleConstraint = handle.constraint().intersect(pushed);
        if (newHandleConstraint.equals(handle.constraint())) {
            return Optional.empty(); // nothing new to push; avoid re-invoking applyFilter forever
        }

        ArangoTableHandle newHandle = handle.withConstraint(newHandleConstraint);
        return Optional.of(new ConstraintApplicationResult<>(newHandle, residual, constraint.getExpression(), false));
    }

    // Widened M2 pushdown. The read path (ArangoPageSource.appendValue) is type-exact, so a pushed
    // AQL predicate and Trino's residual re-check admit exactly the same values (the core invariant).
    // Equality/IN need no guard (AQL ==/IN are type-strict); numeric range is guarded in AqlBuilder.
    private boolean isPushable(Type type, Domain domain) {
        if (domain.isAll()) {
            return false;
        }
        // STRICT coercion: a pushed filter would exclude a type-mismatched row server-side and thus
        // suppress the ARANGODB_TYPE_CONVERSION_ERROR strict mode must raise. Keep results (and
        // errors) independent of pushdown by pushing nothing (spec §5).
        if (config.getTypeCoercion() == ArangoConfig.TypeCoercion.STRICT) {
            return false;
        }
        // IS NULL / IS NOT NULL and any null-allowing domain stay residual: AQL == null/!= null test
        // the raw stored value, which diverges from Trino's post-coercion null-ness on a type mismatch.
        if (domain.isNullAllowed()) {
            return false;
        }
        ValueSet values = domain.getValues();
        if (values.isAll()) {
            return false;
        }
        // Equality / IN: pushable for every scalar type (no guard needed).
        if (values.isDiscreteSet()) {
            return type.equals(BooleanType.BOOLEAN)
                    || type instanceof VarcharType
                    || type.equals(BigintType.BIGINT)
                    || type.equals(DoubleType.DOUBLE);
        }
        // Numeric range: BIGINT and DOUBLE (guarded by AqlBuilder). DOUBLE is fully enforced; BIGINT
        // is prefilter-only (see isPrefilterOnly / applyFilter). String range stays residual.
        return type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE);
    }

    // A pushable predicate whose AQL form admits a SUPERSET of what appendValue writes non-NULL, so
    // it must ALSO stay in Trino's residual for a post-read re-check. Only BIGINT range qualifies: its
    // bare IS_NUMBER guard (AqlBuilder) admits fractional values and integral values outside signed-64-bit
    // range, both of which appendValue reads as NULL -- the residual re-check drops them. We cannot tighten
    // the guard to integers-in-range: AQL FLOOR() returns a double and would false-miss a stored int64
    // > 2^53 (review finding C3). Everything else is fully enforced, never prefilter-only:
    //   - Equality/IN agrees for every type: ArangoDB compares by exact value and the BIGINT read path
    //     is exact (longValue(), no rounding); DOUBLE eq/IN promotes its operand with `+ 0.0`.
    //   - DOUBLE range agrees because AqlBuilder promotes the DOUBLE operand into double space with
    //     `+ 0.0`, matching appendValue's n.doubleValue() rounding; a bare comparison would diverge for
    //     a stored int64 > 2^53 (review finding C1).
    // (Review finding C2, option 2, covers the BIGINT-range superset handling.)
    private static boolean isPrefilterOnly(Type type, Domain domain) {
        return type.equals(BigintType.BIGINT) && !domain.getValues().isDiscreteSet();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle table, long limit) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }
        // Exact only for a single-split scan. With shard-parallelism enabled the table may fan out
        // (ArangoSplitManager), and each split applies LIMIT n independently (total <= n * splits), so
        // Trino must apply the final LIMIT -> report false. Disabled => always one split => exact => true.
        boolean limitGuaranteed = !config.isShardParallelismEnabled();
        return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), limitGuaranteed, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle table,
            List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
        ImmutableList.Builder<ConnectorExpression> newProjections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> newAssignments = ImmutableList.builder();
        Map<String, ArangoColumnHandle> deduped = new LinkedHashMap<>();
        boolean progress = false;

        for (ConnectorExpression projection : projections) {
            Optional<ArangoColumnHandle> resolved;
            if (projection instanceof Variable variable) {
                resolved = assignments.get(variable.getName()) instanceof ArangoColumnHandle existing
                        ? Optional.of(existing) : Optional.empty();
            }
            else {
                resolved = resolveDereference(projection, assignments);
                progress |= resolved.isPresent();
            }
            if (resolved.isEmpty()) {
                return Optional.empty(); // an expression we can't represent -- decline the whole call
            }
            ArangoColumnHandle column = resolved.get();
            ArangoColumnHandle priorAssignment = deduped.get(column.name());
            if (priorAssignment != null && !priorAssignment.path().equals(column.path())) {
                // A synthetic dereference name (baseName + "$" + fieldName) collided with a
                // distinct column's name -- can't safely represent both under one Assignment
                // name, so decline the whole call rather than silently dropping one. Fits
                // applyProjection's existing all-or-nothing contract (see the resolved.isEmpty()
                // decline above).
                return Optional.empty();
            }
            boolean isNewColumn = priorAssignment == null;
            ArangoColumnHandle canonical = deduped.computeIfAbsent(column.name(), key -> column);
            if (isNewColumn) {
                newAssignments.add(new Assignment(canonical.name(), canonical, canonical.type()));
            }
            newProjections.add(new Variable(canonical.name(), canonical.type()));
        }

        if (!progress) {
            return Optional.empty(); // pure passthrough of existing columns: nothing new to report
        }
        return Optional.of(new ProjectionApplicationResult<>(table, newProjections.build(), newAssignments.build(), false));
    }

    // Walks a (possibly chained) FieldDereference down to its root Variable, resolving each
    // field index against the actual RowType field names so the result is a genuine multi-
    // segment AQL document path (e.g. List.of("address", "city")). Declines (Optional.empty())
    // whenever the chain doesn't root in a known column, indexes into a non-ROW type, hits an
    // anonymous row field, or bottoms out at a still-structured (ROW/ARRAY/DECIMAL) leaf --
    // those stay Trino-evaluated, consistent with checkMaterializable's existing ARRAY/ROW/
    // DECIMAL rejection. Segments are never joined into a String, so a field name that itself
    // contains a literal "." is not ambiguous (see AqlBuilder.documentAccessor).
    private static Optional<ArangoColumnHandle> resolveDereference(
            ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        List<Integer> fieldIndexes = new ArrayList<>();
        ConnectorExpression current = expression;
        while (current instanceof FieldDereference deref) {
            fieldIndexes.add(0, deref.getField());
            current = deref.getTarget();
        }
        if (!(current instanceof Variable base) || fieldIndexes.isEmpty()) {
            return Optional.empty();
        }
        if (!(assignments.get(base.getName()) instanceof ArangoColumnHandle baseColumn)) {
            return Optional.empty();
        }

        Type currentType = baseColumn.type();
        List<String> path = new ArrayList<>(baseColumn.path());
        StringBuilder name = new StringBuilder(baseColumn.name());
        for (int fieldIndex : fieldIndexes) {
            if (!(currentType instanceof RowType rowType)) {
                return Optional.empty();
            }
            RowType.Field field = rowType.getFields().get(fieldIndex);
            if (field.getName().isEmpty()) {
                return Optional.empty();
            }
            String fieldName = field.getName().get();
            path.add(fieldName);
            name.append('$').append(fieldName);
            currentType = field.getType();
        }
        if (currentType instanceof RowType || currentType instanceof ArrayType || currentType instanceof DecimalType) {
            return Optional.empty();
        }
        return Optional.of(new ArangoColumnHandle(name.toString(), currentType, false, path));
    }

    private List<ArangoColumn> resolve(ArangoTableHandle handle) {
        return columnCache.computeIfAbsent(handle.schemaTableName(), key ->
                schemaResolver.resolveColumns(handle.schema(),
                        new CollectionInfo(handle.table(), handle.edge(), false)));
    }
}

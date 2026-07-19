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
    // M1: unbounded per-connector memoization so one SELECT samples a collection once;
    // spec §4.3 TTL cache lands in a later milestone.
    private final Map<SchemaTableName, List<ArangoColumn>> columnCache = new ConcurrentHashMap<>();

    @Inject
    public ArangoMetadata(ArangoClient client, SchemaResolver schemaResolver) {
        this.client = client;
        this.schemaResolver = schemaResolver;
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

        Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
        ImmutableMap.Builder<ColumnHandle, Domain> pushedBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnHandle, Domain> residualBuilder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ArangoColumnHandle column = (ArangoColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            if (isPushable(column.type(), domain)) {
                pushedBuilder.put(column, domain);
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

    // Per spec §6.1: equality/IN is safe to push unguarded for every type (AQL's `==`/`IN`
    // are type-strict, never coerce). Numeric range needs an IS_NUMBER guard (AqlBuilder adds
    // it) because AQL's cross-type total order would otherwise let a non-numeric value stored
    // under the same field silently satisfy a numeric `>`/`<`. String range is never pushed
    // (ICU collation vs Trino codepoint-order mismatch, spec §6.1). DECIMAL/ARRAY/ROW are
    // never pushed -- materialization itself is unsupported (ArangoPageSourceProvider
    // .checkMaterializable) and DECIMAL bind-var marshaling is untested. A domain that allows
    // both null AND a restricted value set ("x = 5 OR x IS NULL") is not modeled in M2 and
    // stays in the residual.
    private static boolean isPushable(Type type, Domain domain) {
        // IS NULL / IS NOT NULL are never pushed: AQL's `== null` / `!= null` test the raw
        // stored value, while ArangoPageSource.appendValue leniently coerces any type-mismatched
        // value to Trino NULL. A value outside SchemaResolver's sample (or one that didn't fit the
        // inferred type) would answer these predicates differently in AQL than in Trino, silently
        // dropping or including rows. Left residual, Trino re-applies them post-coercion.
        if (domain.isAll()) {
            return false;
        }
        if (domain.isNullAllowed()) {
            return false;
        }
        if (domain.getValues().isAll()) {
            return false;
        }
        if (type instanceof VarcharType || type.equals(BooleanType.BOOLEAN)) {
            return domain.getValues().isDiscreteSet();
        }
        return type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle table, long limit) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }
        // M2 is single-split always (ArangoSplitManager emits exactly one ArangoSplit per
        // table), so a pushed AQL LIMIT is always exactly satisfied: limitGuaranteed=true.
        return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), true, false));
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
            ArangoColumnHandle existing = deduped.get(column.name());
            if (existing != null && !existing.path().equals(column.path())) {
                // A synthetic dereference name (baseName + "$" + fieldName) collided with a
                // distinct column's name -- can't safely represent both under one Assignment
                // name, so decline the whole call rather than silently dropping one. Fits
                // applyProjection's existing all-or-nothing contract (see the resolved.isEmpty()
                // decline above).
                return Optional.empty();
            }
            boolean isNewColumn = existing == null;
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

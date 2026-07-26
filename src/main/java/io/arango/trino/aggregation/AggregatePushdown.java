package io.arango.trino.aggregation;

import io.arango.trino.ArangoConfig;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Decides what aggregation may be pushed, and declines everything else. Pure: no client, no
 * session, no I/O, so the entire decline matrix is unit-testable.
 *
 * <p>The bar is higher than for filters. {@code applyFilter} may push a prefilter and let Trino
 * re-check the residual; aggregation has no residual, because Trino replaces the aggregation node
 * and treats the connector's output as final. Anything claimed here must therefore be exactly right
 * -- a wrong aggregate is simply a wrong answer.
 */
public final class AggregatePushdown {
    private AggregatePushdown() {}

    // A throwaway accessor used only to ask ColumnGuard whether a type is guardable at all. The
    // real accessor is built from the column's path at render time (AqlBuilder).
    private static final String TYPE_PROBE_ACCESSOR = "x";

    public static Optional<ArangoAggregation> plan(
            ArangoConfig config,
            ArangoTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        if (!config.isAggregationPushdownEnabled()) {
            return Optional.empty();
        }
        // Strict coercion: a pushed aggregate silently absorbs the type mismatch that strict mode
        // exists to raise as ARANGODB_TYPE_CONVERSION_ERROR. Mirrors ArangoMetadata.isPushable.
        if (config.getTypeCoercion() == ArangoConfig.TypeCoercion.STRICT) {
            return Optional.empty();
        }
        // Trino may call the hook again on the handle it just returned; a second push would
        // aggregate an aggregate.
        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }
        // LIMIT n then GROUP BY is not GROUP BY then LIMIT n, and the single-FOR AQL body can
        // only express the latter (the same reason applyFilter declines on a limited handle).
        if (handle.limit().isPresent()) {
            return Optional.empty();
        }
        // A prefilter-only domain (BIGINT range) is enforced JOINTLY by the pushed AQL and Trino's
        // residual re-check. Aggregating over the AQL side alone would include rows -- fractional
        // or out-of-long-range values in the filter column -- that the residual drops, so the
        // aggregate would be silently wrong.
        //
        // This decline is load-bearing, not defensive. It is tempting to assume Trino would never
        // offer this shape, on the grounds that a residual leaves a FilterNode that
        // PushAggregationIntoTableScan will not match. Measured plans say otherwise: Trino fuses
        // the predicate into a ScanFilterProject and pushes the aggregate straight through it (see
        // ArangoConnectorAggregationTest.residualFilterInteractionIsPinned, where the fully
        // enforced DOUBLE-range case IS fully pushed down). Nothing but this check stands between
        // a BIGINT-range filter and a wrong count.
        if (hasPrefilterOnlyDomain(handle)) {
            return Optional.empty();
        }
        if (groupingSets.size() != 1) {
            return Optional.empty(); // no GROUPING SETS / CUBE / ROLLUP
        }

        List<ColumnHandle> groupingSet = groupingSets.get(0);
        if (aggregates.isEmpty() && groupingSet.isEmpty()) {
            return Optional.empty(); // global aggregation with nothing to aggregate
        }

        List<ArangoColumnHandle> groupingColumns = new ArrayList<>();
        for (ColumnHandle column : groupingSet) {
            if (!(column instanceof ArangoColumnHandle arangoColumn)
                    || ColumnGuard.predicate(arangoColumn.type(), TYPE_PROBE_ACCESSOR).isEmpty()) {
                return Optional.empty();
            }
            groupingColumns.add(arangoColumn);
        }

        Set<String> takenNames = new LinkedHashSet<>();
        for (ArangoColumnHandle column : groupingColumns) {
            takenNames.add(column.name());
        }

        List<AggregateSpec> specs = new ArrayList<>();
        for (int i = 0; i < aggregates.size(); i++) {
            Optional<AggregateSpec> spec =
                    specFor(aggregates.get(i), assignments, uniqueName(i, takenNames));
            if (spec.isEmpty()) {
                return Optional.empty(); // all-or-nothing, as base-JDBC does
            }
            takenNames.add(spec.get().outputName());
            specs.add(spec.get());
        }
        return Optional.of(new ArangoAggregation(groupingColumns, specs));
    }

    private static boolean hasPrefilterOnlyDomain(ArangoTableHandle handle) {
        return handle.constraint()
                .getDomains()
                .map(
                        domains ->
                                domains.entrySet().stream()
                                        .anyMatch(
                                                e ->
                                                        e.getKey() instanceof ArangoColumnHandle c
                                                                && isPrefilterOnly(
                                                                        c.type(), e.getValue())))
                .orElse(false);
    }

    // Kept in step with ArangoMetadata.isPrefilterOnly: BIGINT range is the only pushed shape
    // whose AQL form admits a superset of what the read path materializes.
    private static boolean isPrefilterOnly(Type type, Domain domain) {
        return type.equals(BigintType.BIGINT) && !domain.getValues().isDiscreteSet();
    }

    private static String uniqueName(int ordinal, Set<String> taken) {
        String candidate = "agg_" + ordinal;
        for (int suffix = 1; taken.contains(candidate); suffix++) {
            candidate = "agg_" + ordinal + "_" + suffix;
        }
        return candidate;
    }

    private static Optional<AggregateSpec> specFor(
            AggregateFunction function, Map<String, ColumnHandle> assignments, String outputName) {
        if (function.isDistinct()
                || function.getFilter().isPresent()
                || !function.getSortItems().isEmpty()) {
            return Optional.empty();
        }
        List<ConnectorExpression> arguments = function.getArguments();
        String name = function.getFunctionName();

        if (arguments.isEmpty()) {
            return "count".equals(name)
                    ? Optional.of(
                            new AggregateSpec(
                                    AggregateSpec.Kind.COUNT_STAR,
                                    Optional.empty(),
                                    outputName,
                                    function.getOutputType()))
                    : Optional.empty();
        }
        if (arguments.size() != 1
                || !(arguments.get(0) instanceof Variable variable)
                || !(assignments.get(variable.getName()) instanceof ArangoColumnHandle input)) {
            return Optional.empty();
        }

        Type type = input.type();
        // An explicit allowlist rather than a fall-through chain, so an unrecognized function
        // (approx_distinct, count_if, arbitrary, ...) declines by construction.
        AggregateSpec.Kind kind =
                switch (name) {
                    // count needs only a guard predicate: no ordering, no accumulation.
                    case "count" ->
                            ColumnGuard.predicate(type, TYPE_PROBE_ACCESSOR).isPresent()
                                    ? AggregateSpec.Kind.COUNT_COLUMN
                                    : null;
                    // min/max compare, so VARCHAR is out (server collation vs Trino codepoint)
                    // and BOOLEAN is out by scope decision.
                    case "min" -> isMinMaxable(type) ? AggregateSpec.Kind.MIN : null;
                    case "max" -> isMinMaxable(type) ? AggregateSpec.Kind.MAX : null;
                    // sum/avg accumulate, and AQL accumulates in double: BIGINT would lose
                    // precision past 2^53 and turn Trino's loud overflow into a silent one.
                    case "sum" -> type.equals(DoubleType.DOUBLE) ? AggregateSpec.Kind.SUM : null;
                    case "avg" -> type.equals(DoubleType.DOUBLE) ? AggregateSpec.Kind.AVG : null;
                    default -> null;
                };
        return kind == null
                ? Optional.empty()
                : Optional.of(
                        new AggregateSpec(
                                kind, Optional.of(input), outputName, function.getOutputType()));
    }

    private static boolean isMinMaxable(Type type) {
        return type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE);
    }
}

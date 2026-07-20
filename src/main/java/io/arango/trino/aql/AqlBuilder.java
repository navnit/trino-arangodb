package io.arango.trino.aql;

import io.airlift.slice.Slice;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AqlBuilder {
    public record AqlQuery(String aql, Map<String, Object> bindVars) {}

    public AqlQuery buildScan(ArangoTableHandle table, List<ArangoColumnHandle> columns) {
        Map<String, Object> bindVars = new LinkedHashMap<>();
        bindVars.put("@col", table.table());
        int[] counter = {0};

        StringBuilder aql = new StringBuilder("FOR d IN @@col");

        List<String> filters = new ArrayList<>();
        table.constraint().getDomains().ifPresent(domains -> {
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                ArangoColumnHandle column = (ArangoColumnHandle) entry.getKey();
                filters.add("(" + renderDomain(column, entry.getValue(), bindVars, counter) + ")");
            }
        });
        if (!filters.isEmpty()) {
            aql.append(" FILTER ").append(String.join(" AND ", filters));
        }

        table.limit().ifPresent(limit -> aql.append(" LIMIT ").append(limit));

        aql.append(" RETURN ").append(buildReturnClause(columns));
        return new AqlQuery(aql.toString(), bindVars);
    }

    // Only reachable for domain shapes ArangoMetadata.applyFilter already classified as
    // pushable (see ArangoMetadata.isPushable): equality/IN for BOOLEAN/VARCHAR/BIGINT/DOUBLE, and
    // numeric range for BIGINT/DOUBLE. IS NULL / IS NOT NULL are still always left residual and
    // never reach this method.
    //
    // DOUBLE promotion: a stored int64 in a DOUBLE column is read back rounded to double
    // (ArangoPageSource.appendValue does n.doubleValue()), but ArangoDB compares int64-vs-double by
    // exact mathematical value, not in double space -- so a bare `d.f <op> @v` diverges from the read
    // path for magnitudes > 2^53 (a stored 2^53+1 satisfies `> 2^53` in AQL yet reads back as 2^53,
    // and `== 2^54` misses a stored 2^54-1 that reads back as 2^54). Promoting the operand into double
    // space with `+ 0.0` makes AQL compare exactly the value the read path emits, keeping DOUBLE
    // pushdown fully enforced. The IS_NUMBER guard is required first because `+ 0.0` coerces
    // non-numbers ("abc" + 0.0 == 0.0); AQL's AND short-circuits, so the guard protects the arithmetic.
    // This promotion is DOUBLE-only on purpose: BIGINT reads exactly (longValue(), no rounding) against
    // long binds, so bare exact comparison is what agrees there -- promoting BIGINT would create the
    // mirror bug. Likewise the DOUBLE-equality IS_NUMBER guard must not leak onto BOOLEAN/VARCHAR/BIGINT,
    // whose equality is already type-exact in AQL.
    private static String renderDomain(ArangoColumnHandle column, Domain domain, Map<String, Object> bindVars, int[] counter) {
        String accessor = documentAccessor(column.path());
        boolean isDouble = column.type().equals(DoubleType.DOUBLE);
        String cmp = isDouble ? "(" + accessor + " + 0.0)" : accessor;
        ValueSet values = domain.getValues();
        if (values.isDiscreteSet()) {
            List<Object> discrete = values.getDiscreteSet();
            String clause;
            if (discrete.size() == 1) {
                String v = bindValue(bindVars, counter, toBindValue(discrete.get(0)));
                clause = cmp + " == @" + v;
            } else {
                List<Object> converted = discrete.stream().map(AqlBuilder::toBindValue).toList();
                String v = bindValue(bindVars, counter, converted);
                clause = cmp + " IN @" + v;
            }
            return isDouble ? "IS_NUMBER(" + accessor + ") AND " + clause : clause;
        }
        // Numeric range (ArangoMetadata.isPushable only admits this for BIGINT/DOUBLE). AQL's <,>
        // use a total cross-type ordering (null<bool<number<string), so d.f>@v would also match
        // non-numbers; guard with IS_NUMBER. Both BIGINT and DOUBLE use a bare IS_NUMBER guard:
        //   - We deliberately do NOT add an integrality guard for BIGINT. The obvious `d.f == FLOOR(d.f)`
        //     is broken: AQL FLOOR() returns a double, and ArangoDB compares int64-vs-double by exact
        //     value, so a stored int64 that isn't exactly double-representable (e.g. 2^53+1) fails the
        //     guard and is dropped server-side even though the read path reads it exactly via longValue()
        //     -- a silent false-miss (review finding C3). It is also unnecessary: BIGINT range is
        //     prefilter-only (isPrefilterOnly), so the guard only needs to admit a SUPERSET -- a fractional
        //     35.5 or an out-of-long-range integer passes IS_NUMBER, reads back NULL, and Trino's residual
        //     re-check excludes it.
        //   - DOUBLE promotes its operands with `+ 0.0` (via cmp above) so the comparison itself agrees
        //     with the read path; the IS_NUMBER guard still excludes non-numbers.
        String guard = "IS_NUMBER(" + accessor + ")";
        List<String> rangeClauses = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            List<String> bounds = new ArrayList<>();
            if (!range.isLowUnbounded()) {
                String op = range.isLowInclusive() ? " >= @" : " > @";
                bounds.add(cmp + op + bindValue(bindVars, counter, toBindValue(range.getLowBoundedValue())));
            }
            if (!range.isHighUnbounded()) {
                String op = range.isHighInclusive() ? " <= @" : " < @";
                bounds.add(cmp + op + bindValue(bindVars, counter, toBindValue(range.getHighBoundedValue())));
            }
            // isPushable guarantees a real range (not all, not discrete), so bounds is non-empty.
            rangeClauses.add(bounds.size() == 1 ? bounds.get(0) : "(" + String.join(" AND ", bounds) + ")");
        }
        String ranges = rangeClauses.size() == 1 ? rangeClauses.get(0) : "(" + String.join(" OR ", rangeClauses) + ")";
        return guard + " AND " + ranges;
    }

    // Trino represents VARCHAR predicate values as io.airlift.slice.Slice, not String; the
    // ArangoDB driver's bind-var marshaling has no notion of Slice, so it must be converted
    // before it can be handed to the driver. BIGINT/DOUBLE/BOOLEAN values are already the
    // right Java type (Long/Double/Boolean) and pass through unchanged.
    private static Object toBindValue(Object trinoValue) {
        return trinoValue instanceof Slice slice ? slice.toStringUtf8() : trinoValue;
    }

    private static String bindValue(Map<String, Object> bindVars, int[] counter, Object value) {
        String name = "v" + counter[0]++;
        bindVars.put(name, value);
        return name;
    }

    private static String buildReturnClause(List<ArangoColumnHandle> columns) {
        if (columns.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            ArangoColumnHandle column = columns.get(i);
            sb.append(quoteAqlString(column.name())).append(": ").append(documentAccessor(column.path()));
        }
        return sb.append("}").toString();
    }

    private static String documentAccessor(List<String> path) {
        StringBuilder sb = new StringBuilder("d");
        for (String segment : path) {
            sb.append('[').append(quoteAqlString(segment)).append(']');
        }
        return sb.toString();
    }

    private static String quoteAqlString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

package io.arango.trino.aql;

import io.airlift.slice.Slice;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;

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
    // pushable, so this method trusts its input rather than re-verifying types: equality/IN
    // (any type) or a genuine range (BIGINT/DOUBLE only). IS NULL / IS NOT NULL are never
    // pushed (see ArangoMetadata.isPushable) and so never reach this method.
    private static String renderDomain(ArangoColumnHandle column, Domain domain, Map<String, Object> bindVars, int[] counter) {
        String accessor = documentAccessor(column.path());
        ValueSet values = domain.getValues();
        if (values.isDiscreteSet()) {
            List<Object> discrete = values.getDiscreteSet();
            if (discrete.size() == 1) {
                String v = bindValue(bindVars, counter, toBindValue(discrete.get(0)));
                return accessor + " == @" + v;
            }
            List<Object> converted = discrete.stream().map(AqlBuilder::toBindValue).toList();
            String v = bindValue(bindVars, counter, converted);
            return accessor + " IN @" + v;
        }
        List<String> rangeClauses = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            rangeClauses.add(renderRange(accessor, range, bindVars, counter));
        }
        String combined = rangeClauses.size() == 1
                ? rangeClauses.get(0)
                : "(" + String.join(" OR ", rangeClauses) + ")";
        return "IS_NUMBER(" + accessor + ") AND " + combined;
    }

    private static String renderRange(String accessor, Range range, Map<String, Object> bindVars, int[] counter) {
        if (range.isSingleValue()) {
            String v = bindValue(bindVars, counter, toBindValue(range.getSingleValue()));
            return accessor + " == @" + v;
        }
        List<String> parts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            String v = bindValue(bindVars, counter, toBindValue(range.getLowBoundedValue()));
            parts.add(accessor + (range.isLowInclusive() ? " >= @" : " > @") + v);
        }
        if (!range.isHighUnbounded()) {
            String v = bindValue(bindVars, counter, toBindValue(range.getHighBoundedValue()));
            parts.add(accessor + (range.isHighInclusive() ? " <= @" : " < @") + v);
        }
        return parts.size() == 1 ? parts.get(0) : "(" + String.join(" AND ", parts) + ")";
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

    private static String documentAccessor(String path) {
        return "d[" + quoteAqlString(path) + "]";
    }

    private static String quoteAqlString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

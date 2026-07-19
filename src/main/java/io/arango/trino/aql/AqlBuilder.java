package io.arango.trino.aql;

import io.airlift.slice.Slice;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
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
    // pushable. After the M2 final-review narrowing, that is exactly a BOOLEAN discrete set
    // (equality or IN); VARCHAR/BIGINT/DOUBLE equality/IN, every range predicate, and IS NULL /
    // IS NOT NULL are all left residual (see ArangoMetadata.isPushable) and never reach this
    // method. The discrete-set rendering below stays generic -- single value -> `==`, multiple
    // -> `IN`, for any value type -- rather than hardcoding BOOLEAN. A non-discrete-set (range)
    // domain is unreachable here, so it is rejected loudly rather than silently mis-rendered.
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
        throw new IllegalStateException(
                "renderDomain only receives discrete-set domains from ArangoMetadata.isPushable; got " + domain);
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

package io.arango.trino.aql;

import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;

import java.util.List;
import java.util.Map;

public class AqlBuilder {
    public record AqlQuery(String aql, Map<String, Object> bindVars) {}

    public AqlQuery buildScan(ArangoTableHandle table, List<ArangoColumnHandle> columns) {
        Map<String, Object> bindVars = new java.util.LinkedHashMap<>();
        bindVars.put("@col", table.table());

        StringBuilder aql = new StringBuilder("FOR d IN @@col");
        aql.append(" RETURN ").append(buildReturnClause(columns));
        return new AqlQuery(aql.toString(), bindVars);
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

    // Bracket notation (not dot notation) so a path segment can never collide with AQL
    // identifier rules; ArangoDB null-propagates through bracket access on null/missing
    // values, so a missing intermediate object (e.g. "address" absent) yields null rather
    // than an AQL error, matching Trino's NULL-on-absent-field semantics.
    private static String documentAccessor(String path) {
        StringBuilder sb = new StringBuilder("d");
        for (String segment : path.split("\\.")) {
            sb.append('[').append(quoteAqlString(segment)).append(']');
        }
        return sb.toString();
    }

    private static String quoteAqlString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

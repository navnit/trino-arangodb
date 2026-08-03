package io.arango.trino.ptf;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The one place M6-B's safety invariant lives (spec §3): a passthrough query is admitted only if
 * every collection in its execution plan is accessed exactly {@code "read"}. This is an allowlist —
 * ArangoDB must declare a query's write collections up front to take locks, so a query cannot
 * mutate a collection this list does not carry. Anything unrecognized (absent type, new access
 * modes like "exclusive", a reshaped explain response) fails closed.
 */
public final class AqlReadOnlyGate {
    public enum Kind {
        NOT_READ_ONLY,
        SYSTEM_COLLECTION
    }

    public record Rejection(Kind kind, String reason) {}

    private AqlReadOnlyGate() {}

    public static Optional<Rejection> check(Map<String, Object> explainResponse) {
        if (explainResponse == null || !(explainResponse.get("plan") instanceof Map<?, ?> plan)) {
            return Optional.of(
                    new Rejection(Kind.NOT_READ_ONLY, "explain response carried no plan object"));
        }
        if (!(plan.get("collections") instanceof List<?> collections)) {
            return Optional.of(
                    new Rejection(Kind.NOT_READ_ONLY, "explain plan carried no collections list"));
        }
        for (Object entry : collections) {
            if (!(entry instanceof Map<?, ?> collection)) {
                return Optional.of(
                        new Rejection(
                                Kind.NOT_READ_ONLY, "unrecognized entry in plan collections"));
            }
            // A non-string name is only cosmetic here: the "read" type check below is still
            // exact for such an entry, so this fallback cannot open the gate — it only means
            // the _-prefix hardening (which needs a real name) does not apply to it.
            String name = collection.get("name") instanceof String s ? s : "<unnamed collection>";
            Object type = collection.get("type");
            if (!"read".equals(type)) {
                return Optional.of(
                        new Rejection(
                                Kind.NOT_READ_ONLY,
                                "collection '%s' is accessed '%s', not 'read'"
                                        .formatted(name, type)));
            }
            if (name.startsWith("_")) {
                // This connector hides system collections from listTables; the passthrough keeps
                // that convention. Hardening, not a guarantee — DOCUMENT("_users/x") resolves at
                // runtime and never appears in the plan (§3.3).
                return Optional.of(
                        new Rejection(
                                Kind.SYSTEM_COLLECTION, "system collection '%s'".formatted(name)));
            }
        }
        return Optional.empty();
    }
}

package io.arango.trino.ptf;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.ptf.AqlReadOnlyGate.Kind;
import io.arango.trino.ptf.AqlReadOnlyGate.Rejection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AqlReadOnlyGateTest {
    /**
     * Builds {"plan": {"collections": [{name,type}...]}} from (name, type) pairs; a null type puts
     * an entry with no "type" key at all (the absent case).
     */
    private static Map<String, Object> explain(String... nameTypePairs) {
        List<Map<String, Object>> collections = new ArrayList<>();
        for (int i = 0; i < nameTypePairs.length; i += 2) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("name", nameTypePairs[i]);
            if (nameTypePairs[i + 1] != null) {
                entry.put("type", nameTypePairs[i + 1]);
            }
            collections.add(entry);
        }
        return Map.of("plan", Map.of("collections", collections));
    }

    @Test
    void allReadAdmits() {
        assertThat(AqlReadOnlyGate.check(explain("users", "read", "follows", "read"))).isEmpty();
    }

    @Test
    void emptyCollectionsAdmits() {
        // RETURN 1..10 plans with no collections at all (§3 row 2)
        assertThat(AqlReadOnlyGate.check(explain())).isEmpty();
    }

    @Test
    void anyWriteRejectsNamingTheCollection() {
        Optional<Rejection> verdict =
                AqlReadOnlyGate.check(explain("users", "read", "follows", "write"));
        assertThat(verdict).isPresent();
        assertThat(verdict.get().kind()).isEqualTo(Kind.NOT_READ_ONLY);
        assertThat(verdict.get().reason()).contains("follows");
    }

    // Fail closed under novelty (§3): everything that is not exactly "read" rejects.
    @Test
    void absentTypeRejects() {
        assertThat(AqlReadOnlyGate.check(explain("users", null))).isPresent();
    }

    @Test
    void exclusiveTypeRejects() {
        assertThat(AqlReadOnlyGate.check(explain("users", "exclusive"))).isPresent();
    }

    @Test
    void unknownTypeRejects() {
        assertThat(AqlReadOnlyGate.check(explain("users", "readwrite"))).isPresent();
    }

    @Test
    void missingPlanRejects() {
        assertThat(AqlReadOnlyGate.check(Map.of("error", false))).isPresent();
    }

    @Test
    void missingCollectionsListRejects() {
        assertThat(AqlReadOnlyGate.check(Map.of("plan", Map.of("nodes", List.of())))).isPresent();
    }

    @Test
    void nonMapCollectionsEntryRejects() {
        assertThat(AqlReadOnlyGate.check(Map.of("plan", Map.of("collections", List.of("users")))))
                .isPresent();
    }

    @Test
    void systemCollectionRejectsAsItsOwnKind() {
        // read-typed but _-prefixed: the connector's own hiding convention (§3.3)
        Optional<Rejection> verdict = AqlReadOnlyGate.check(explain("_graphs", "read"));
        assertThat(verdict).isPresent();
        assertThat(verdict.get().kind()).isEqualTo(Kind.SYSTEM_COLLECTION);
        assertThat(verdict.get().reason()).contains("_graphs");
    }
}

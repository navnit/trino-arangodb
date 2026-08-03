package io.arango.trino.aql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.ptf.AqlReadOnlyGate;
import io.arango.trino.ptf.AqlReadOnlyGate.Kind;
import io.arango.trino.ptf.AqlReadOnlyGate.Rejection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Pins the explain-plan access-mode semantics AqlReadOnlyGate's soundness rests on (spec §3,
 * Appendix B). If an ArangoDB upgrade changes any row here, the gate's argument must be revisited
 * rather than silently lost — the analogue of AqlSemanticsAssumptionsTest for M5.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AqlPassthroughAssumptionsTest {
    private static final String DB = "gate_probe";
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "users");
        client.insertForTest(DB, "users", Map.of("_key", "a", "name", "ann"));
        client.insertForTest(DB, "users", Map.of("_key", "b", "name", "bob"));
        client.createEdgeCollectionForTest(DB, "follows");
        client.insertForTest(DB, "follows", Map.of("_from", "users/a", "_to", "users/b"));
        client.createGraphForTest(DB, "social", "follows", "users");
    }

    @AfterAll
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private Optional<Rejection> verdict(String aql) {
        return AqlReadOnlyGate.check(client.explainPlan(DB, aql));
    }

    private long userCount() {
        return client.countWithShardIds(DB, "users", List.of());
    }

    // ---- §3 table: reads admit ----

    @Test
    void plainReadAdmits() {
        assertThat(verdict("FOR d IN users RETURN d")).isEmpty();
    }

    @Test
    void noCollectionQueryAdmits() {
        assertThat(verdict("RETURN 1..10")).isEmpty();
    }

    @Test
    void anonymousTraversalAdmits() {
        assertThat(verdict("FOR v IN 1..1 OUTBOUND \"users/a\" follows RETURN v")).isEmpty();
    }

    @Test
    void namedGraphTraversalAdmits() {
        assertThat(verdict("FOR v IN 1..1 OUTBOUND \"users/a\" GRAPH \"social\" RETURN v"))
                .isEmpty();
    }

    @Test
    void insertKeywordInStringLiteralAdmits() {
        // exactly the false positive a keyword scan would produce (§3 row 5)
        assertThat(verdict("FOR d IN users FILTER d.name == \"INSERT INTO\" RETURN d")).isEmpty();
    }

    // ---- §3 table: every data-modification form rejects ----

    @Test
    void insertRejects() {
        assertRejectsAsWrite("INSERT {x: 1} INTO users", "users");
    }

    @Test
    void updateRejects() {
        assertRejectsAsWrite("FOR d IN users UPDATE d WITH {x: 1} IN users", "users");
    }

    @Test
    void removeRejects() {
        assertRejectsAsWrite("FOR d IN users REMOVE d IN users", "users");
    }

    @Test
    void replaceRejects() {
        assertRejectsAsWrite("FOR d IN users REPLACE d WITH {y: 2} IN users", "users");
    }

    @Test
    void upsertRejects() {
        assertRejectsAsWrite("UPSERT {_key: \"a\"} INSERT {n: 1} UPDATE {n: 2} IN users", "users");
    }

    @Test
    void subqueryInsertRejects() {
        assertRejectsAsWrite(
                "FOR d IN users LET x = (INSERT {q: 1} INTO users RETURN NEW) RETURN x", "users");
    }

    @Test
    void crossCollectionInsertRejectsTheWrittenCollection() {
        assertRejectsAsWrite("FOR d IN users INSERT {c: d.name} INTO follows", "follows");
    }

    private void assertRejectsAsWrite(String aql, String collection) {
        Optional<Rejection> v = verdict(aql);
        assertThat(v).isPresent();
        assertThat(v.get().kind()).isEqualTo(Kind.NOT_READ_ONLY);
        assertThat(v.get().reason()).contains(collection);
    }

    // ---- §3.2: a UDF escapes the gate but not the server's transaction registration ----

    @Test
    void udfWriteIsAdmittedByGateButBlockedByServer() {
        // The UDF body catches the server's refusal and returns it as a string — this matches
        // the spec's Appendix B probe, whose recorded RESULT was
        // ["BLOCKED: unregistered collection used in transaction: users [write]"], i.e. a
        // returned value, not a raised exception. Asserting on the returned string via the
        // Object-typed firstBatch avoids the Map.class deserialization confound entirely.
        client.registerAqlFunctionForTest(
                DB,
                "EVIL::WRITE",
                "function (x) { try { require(\"@arangodb\").db.users.save({x: x});"
                        + " return \"WROTE\"; } catch (e) { return \"BLOCKED: \" + String(e)"
                        + " + (e.errorMessage || \"\"); } }");
        // the gate admits: a UDF call declares no collections at all
        assertThat(verdict("RETURN EVIL::WRITE(\"pwned\")")).isEmpty();

        long before = userCount();
        // the server's lock-declaration refusal IS the closure argument the gate's soundness
        // rests on (§3.2); a "WROTE" result here means an ArangoDB upgrade relaxed it
        assertThat(String.valueOf(client.firstBatch(DB, "RETURN EVIL::WRITE(\"pwned\")", 1).get(0)))
                .contains("unregistered collection");
        // dynamic invocation forms too — the ones an AST denylist would have to enumerate
        assertThat(
                        String.valueOf(
                                client.firstBatch(DB, "RETURN CALL(\"EVIL::WRITE\", \"dyn\")", 1)
                                        .get(0)))
                .contains("unregistered collection");
        assertThat(
                        String.valueOf(
                                client.firstBatch(
                                                DB, "RETURN APPLY(\"EVIL::WRITE\", [\"dyn2\"])", 1)
                                        .get(0)))
                .contains("unregistered collection");
        // the real invariant: nothing was written
        assertThat(userCount()).isEqualTo(before);
    }

    // ---- §3.4: explain refuses a declared-but-unbound bind parameter ----

    @Test
    void unboundBindParameterRejectsAtExplain() {
        assertThatThrownBy(
                        () ->
                                client.explainPlan(
                                        DB, "FOR d IN users FILTER d.age > @minAge RETURN d"))
                .hasMessageContaining("bind parameter");
    }

    // ---- rows the §3 table lacked (§11): view read and SHORTEST_PATH ----

    @Test
    void arangoSearchViewReadAdmits() {
        client.createArangoSearchViewForTest(DB, "users_view", "users");
        assertThat(verdict("FOR d IN users_view SEARCH d.name == \"ann\" RETURN d")).isEmpty();
    }

    @Test
    void shortestPathAdmits() {
        assertThat(
                        verdict(
                                "FOR v IN OUTBOUND SHORTEST_PATH \"users/a\" TO \"users/b\" follows RETURN v"))
                .isEmpty();
    }
}

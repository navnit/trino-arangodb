package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end packaging smoke test. Mounts the *packaged* plugin directory
 * (target/trino-arangodb-&lt;version&gt;, passed as -Dplugin.dir by failsafe) into a real
 * trinodb/trino:476 container, points it at a real ArangoDB container over a shared Docker
 * network, and runs SQL over JDBC.
 *
 * <p>Unlike the in-JVM {@code DistributedQueryRunner} tests (which load the plugin on a flat
 * test classpath), this exercises real per-plugin classloader isolation of the packaged bundle
 * and actual plugin discovery/startup. The {@code *IT} suffix routes it to failsafe (verify
 * phase, after {@code package}), so it is excluded from {@code mvn test}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PackagingSmokeIT {

    private static final String ARANGO_ALIAS = "arangodb";
    private static final int ARANGO_PORT = 8529;
    private static final int TRINO_PORT = 8080;

    private Network network;
    private TestingArangoServer arango;
    private GenericContainer<?> trino;

    @BeforeAll
    void setup() throws Exception {
        String pluginDir = System.getProperty("plugin.dir");
        if (pluginDir == null || !Files.isDirectory(Path.of(pluginDir))) {
            throw new IllegalStateException(
                    "plugin.dir must point at the packaged plugin directory "
                            + "(target/trino-arangodb-<version>). Run via `mvn verify` "
                            + "(failsafe sets it). Got: " + pluginDir);
        }

        network = Network.newNetwork();

        // ArangoDB on the shared network under a stable alias; seeded via the host-mapped port.
        arango = new TestingArangoServer(network, ARANGO_ALIAS);
        try (ArangoClient seed = new ArangoClient(new ArangoConfig()
                .setHosts(arango.hostPort()).setUser("root").setPassword(arango.rootPassword()))) {
            seed.createDatabaseForTest("shop");
            seed.createDocumentCollectionForTest("shop", "users");
            seed.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
            seed.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
        }

        // Catalog the Trino container reads at startup. It reaches ArangoDB at the *internal*
        // network address (alias + container port), NOT the JVM-visible host-mapped port.
        String catalog = "connector.name=arangodb\n"
                + "arangodb.hosts=" + ARANGO_ALIAS + ":" + ARANGO_PORT + "\n"
                + "arangodb.user=root\n"
                + "arangodb.password=" + arango.rootPassword() + "\n";

        trino = new GenericContainer<>(DockerImageName.parse("trinodb/trino:476"))
                .withNetwork(network)
                .withExposedPorts(TRINO_PORT)
                .withFileSystemBind(pluginDir, "/usr/lib/trino/plugin/arangodb", BindMode.READ_ONLY)
                .withCopyToContainer(Transferable.of(catalog), "/etc/trino/catalog/arango.properties")
                // Trino answers HTTP while still starting and rejects queries during that window,
                // so wait for /v1/info to report the server is done starting.
                .waitingFor(Wait.forHttp("/v1/info").forPort(TRINO_PORT).forStatusCode(200)
                        .forResponsePredicate(body -> body.contains("\"starting\":false"))
                        .withStartupTimeout(Duration.ofMinutes(3)));
        trino.start();

        // "starting":false only means the HTTP server is up; the coordinator's node manager
        // registers the (self) active node, and the arangodb catalog's connector finishes
        // bootstrapping (Guice injector, ArangoClient connect), a moment later. A query issued
        // in that window transiently fails with "No nodes available to run query" (a plain
        // system.runtime.nodes probe isn't sufficient -- it can report an active node before the
        // catalog's own connector is schedulable). Poll with a real query against the catalog
        // under test until the cluster can actually execute it, separately from (and shorter
        // than) the startup wait above.
        awaitQueryable();
    }

    private void awaitQueryable() throws Exception {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try (Connection conn = connect();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM arango.shop.users")) {
                if (rs.next()) {
                    return;
                }
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(
                "Trino did not become queryable within 60s of startup"
                        + (lastFailure != null ? " (last failure: " + lastFailure + ")" : ""),
                lastFailure);
    }

    @AfterAll
    void teardown() {
        // Attempt every close even if an earlier one throws, so a failure stopping the Trino
        // container cannot leak the ArangoDB container or the shared network.
        try {
            if (trino != null) trino.stop();
        } finally {
            try {
                if (arango != null) arango.close();
            } finally {
                if (network != null) network.close();
            }
        }
    }

    private Connection connect() throws Exception {
        String url = "jdbc:trino://" + trino.getHost() + ":" + trino.getMappedPort(TRINO_PORT);
        Properties props = new Properties();
        props.setProperty("user", "test"); // no auth on the default image; any user is accepted
        return DriverManager.getConnection(url, props);
    }

    @Test
    void packagedPluginRegistersCatalog() throws Exception {
        try (Connection conn = connect();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW CATALOGS")) {
            List<String> catalogs = new ArrayList<>();
            while (rs.next()) {
                catalogs.add(rs.getString(1));
            }
            assertThat(catalogs).contains("arango");
        }
    }

    @Test
    void packagedPluginAnswersQuery() throws Exception {
        try (Connection conn = connect();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT name, age FROM arango.shop.users ORDER BY age")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("ada");
            assertThat(rs.getLong("age")).isEqualTo(36L);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("bob");
            assertThat(rs.getLong("age")).isEqualTo(41L);
            assertThat(rs.next()).isFalse();
        }
    }
}

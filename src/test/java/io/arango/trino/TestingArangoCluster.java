package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import java.io.File;
import java.time.Duration;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public final class TestingArangoCluster implements AutoCloseable {
    private final ComposeContainer compose;

    public TestingArangoCluster() {
        compose =
                new ComposeContainer(new File("src/test/resources/arangodb-cluster-compose.yml"))
                        .withExposedService(
                                "coordinator",
                                8529,
                                Wait.forHttp("/_api/version")
                                        .forStatusCode(200)
                                        // A cold ubuntu-latest runner (2 vCPU, image freshly
                                        // pulled) forms the
                                        // 3-node cluster (agency + one dbserver + coordinator) far
                                        // slower than a
                                        // warm local box, where it is ready in ~15-90s. These ITs
                                        // are @Tag("cluster")
                                        // and run only in the isolated, non-blocking cluster-its CI
                                        // job (surefire
                                        // skipped, so the runner is uncontended and boots exactly
                                        // one cluster).
                                        // Formation is CPU/coordination-bound, not memory-bound
                                        // (measured: the three
                                        // arangod containers peak at ~500MB total and are never
                                        // OOM-killed), so on the
                                        // 2-vCPU runner the wall-clock is what bites. The window
                                        // was 8 minutes through
                                        // the trino-476/JDK-24 line, but the JDK-25/trino-483
                                        // baseline made the forked
                                        // failsafe JVM heavier and consistently tipped the boot
                                        // past 8 minutes on CI
                                        // (the coordinator's /_api/version returned "read timed
                                        // out" for the full
                                        // window). 15 minutes absorbs the slower cold boot without
                                        // masking a genuine
                                        // hang; the job's own 30-minute ceiling still backstops a
                                        // true hang.
                                        .withStartupTimeout(Duration.ofMinutes(15)));
        try {
            compose.start();
            awaitClusterReady();
        } catch (RuntimeException e) {
            // Don't leak the compose containers if the cluster never comes up. Either a
            // start() timeout in the HTTP wait strategy or a write-readiness failure below
            // throws before this instance is assigned, so close() never runs. Left running,
            // the containers -- including Testcontainers' socat port ambassador -- sit until
            // the leaked-container safeguard hard-kills the JVM fork (turning one clean IT
            // failure into an unreadable "forked VM terminated without properly saying
            // goodbye" crash) and starve the next IT's cluster boot. start() must be inside
            // this try for stop() to cover the start()-timeout case, not just readiness.
            compose.stop();
            throw e;
        }
    }

    /**
     * The coordinator's plain-HTTP {@code /_api/version} answers, and even a metadata GET succeeds,
     * well before the cluster can actually service *writes*: creating a database is a coordinated
     * agency transaction, and issuing one immediately after {@link ComposeContainer}'s (GET-based)
     * wait strategy passes fails with a Vertx "Stream was closed" error -- the coordinator is up
     * but not yet wired to the agency/dbservers for write coordination. Retry an actual write (the
     * same call path {@code createDatabaseForTest} exercises) until it succeeds, so callers of
     * {@link #config()} always get a cluster that can serve writes, not just accept TCP connections
     * or answer reads.
     */
    private void awaitClusterReady() {
        long deadlineNanos = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try (ArangoClient probe = new ArangoClient(config())) {
                probe.createDatabaseForTest("cluster_ready_probe");
                return;
            } catch (Exception e) {
                lastFailure = e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting for cluster readiness", ie);
                }
            }
        }
        throw new IllegalStateException(
                "ArangoDB cluster did not become write-ready within 3 minutes of boot",
                lastFailure);
    }

    public ArangoConfig config() {
        String host = compose.getServiceHost("coordinator", 8529);
        int port = compose.getServicePort("coordinator", 8529);
        return new ArangoConfig().setHosts(host + ":" + port);
    }

    @Override
    public void close() {
        compose.stop();
    }
}

package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;

public final class TestingArangoCluster implements AutoCloseable {
    private final ComposeContainer compose;

    public TestingArangoCluster() {
        compose = new ComposeContainer(new File("src/test/resources/arangodb-cluster-compose.yml"))
                .withExposedService("coordinator", 8529,
                        Wait.forHttp("/_api/version").forStatusCode(200)
                                .withStartupTimeout(Duration.ofMinutes(3)));
        compose.start();
        try {
            awaitClusterReady();
        } catch (RuntimeException e) {
            // Don't leak the compose containers if the cluster never becomes write-ready:
            // this constructor throwing means the instance is never assigned, so close()
            // never runs, and the containers would otherwise sit until Ryuk reaps them
            // (or trip the test framework's leaked-container JVM-kill safeguard).
            compose.stop();
            throw e;
        }
    }

    /**
     * The coordinator's plain-HTTP {@code /_api/version} answers, and even a metadata GET
     * succeeds, well before the cluster can actually service *writes*: creating a database is
     * a coordinated agency transaction, and issuing one immediately after {@link ComposeContainer}'s
     * (GET-based) wait strategy passes fails with a Vertx "Stream was closed" error -- the
     * coordinator is up but not yet wired to the agency/dbservers for write coordination.
     * Retry an actual write (the same call path {@code createDatabaseForTest} exercises) until
     * it succeeds, so callers of {@link #config()} always get a cluster that can serve writes,
     * not just accept TCP connections or answer reads.
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
                    throw new IllegalStateException("Interrupted while waiting for cluster readiness", ie);
                }
            }
        }
        throw new IllegalStateException("ArangoDB cluster did not become write-ready within 3 minutes of boot", lastFailure);
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

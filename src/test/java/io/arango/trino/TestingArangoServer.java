package io.arango.trino;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;

public final class TestingArangoServer implements Closeable {
    private static final int PORT = 8529;
    private static final String ROOT_PASSWORD = "test";
    private final GenericContainer<?> container;

    public TestingArangoServer() {
        this(null, null);
    }

    /**
     * Starts ArangoDB joined to {@code network} under {@code alias}, so a container on the
     * same network reaches it at {@code alias:8529}. Pass {@code (null, null)} for a
     * host-only container (the no-arg constructor). The host-mapped port is still published
     * either way, so JVM-side seeding via {@link #hostPort()} works in both modes.
     */
    @SuppressWarnings("resource")
    public TestingArangoServer(Network network, String alias) {
        GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse("arangodb/arangodb:3.12"))
                .withExposedPorts(PORT)
                .withEnv("ARANGO_ROOT_PASSWORD", ROOT_PASSWORD)
                .waitingFor(Wait.forHttp("/_api/version").forStatusCode(200)
                        .forPort(PORT).withBasicCredentials("root", ROOT_PASSWORD));
        if (network != null) {
            c = c.withNetwork(network).withNetworkAliases(alias);
        }
        container = c;
        container.start();
    }

    public String host() { return container.getHost(); }
    public int port() { return container.getMappedPort(PORT); }
    public String hostPort() { return host() + ":" + port(); }
    public String rootPassword() { return ROOT_PASSWORD; }

    @Override
    public void close() { container.stop(); }
}

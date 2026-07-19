package io.arango.trino;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;

public final class TestingArangoServer implements Closeable {
    private static final int PORT = 8529;
    private static final String ROOT_PASSWORD = "test";
    private final GenericContainer<?> container;

    @SuppressWarnings("resource")
    public TestingArangoServer() {
        container = new GenericContainer<>(DockerImageName.parse("arangodb/arangodb:3.12"))
                .withExposedPorts(PORT)
                .withEnv("ARANGO_ROOT_PASSWORD", ROOT_PASSWORD)
                .waitingFor(Wait.forHttp("/_api/version").forStatusCode(200)
                        .forPort(PORT).withBasicCredentials("root", ROOT_PASSWORD));
        container.start();
    }

    public String host() { return container.getHost(); }
    public int port() { return container.getMappedPort(PORT); }
    public String hostPort() { return host() + ":" + port(); }
    public String rootPassword() { return ROOT_PASSWORD; }

    @Override
    public void close() { container.stop(); }
}

package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import java.io.File;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Boots the multi-node ArangoDB cluster (agency + one dbserver + coordinator) used by the
 * {@code @Tag("cluster")} ITs, via Testcontainers Compose.
 *
 * <p><b>What was measured.</b> Job wall-clock across CI runs is bimodal, with no middle: a
 * successful boot completes the *entire* job (checkout, JDK setup, image pull, Maven, cluster boot,
 * all tests) in 94-113s; a failed boot burns the *entire* {@code withStartupTimeout} window every
 * time (913s, 912.6s against the old 15-minute window) and never lands in between. That rules out
 * "the boot is slow" -- a slow-but-progressing boot would show intermediate durations. The boot
 * either completes in well under a minute or hangs indefinitely; the previous 8-then-15-minute
 * timeout escalation was chasing the wrong theory, since a longer window only makes a hang slower
 * to report and this class never retried, which is the one thing that actually helps. The root
 * cause of the hang itself (agency election that never converges? Docker networking on the runner?
 * CPU starvation?) is still unknown -- the per-service log consumers wired below exist to finally
 * capture the evidence needed to answer that.
 *
 * <p><b>The fix: retry with a FRESH container per attempt.</b> A deadlocked cluster formation does
 * not recover, so a retry that reused the same {@link ComposeContainer} would just hang again; each
 * attempt tears down its container on failure and the next attempt starts a new one. Per-attempt
 * budgets are tight because a real boot is fast: {@link #STARTUP_TIMEOUT} (4 minutes) and {@link
 * #READY_TIMEOUT} (90 seconds) are both a 4-8x margin over the measured successful case, not an
 * attempt to mask a genuine hang. Worst case is {@link #BOOT_ATTEMPTS} (3) x ~5.5 minutes =~ 16.5
 * minutes plus test time, comfortably inside the job's 30-minute ceiling -- versus the old
 * single-hang cost of 15 minutes for nothing.
 *
 * <p>Still true, unrelated to the above: one dbserver is enough because the shard-parallel ITs only
 * need a collection with more than one *shard*, and three shards live fine on a single PRIMARY -- a
 * second dbserver only doubled the boot footprint on the 2-vCPU runner. {@code
 * --cluster.system-replication-factor=1} on the coordinator is required for a single-dbserver
 * cluster: ArangoDB 3.12 creates its {@code _system} collections at replicationFactor 2 by default,
 * and with one dbserver that bootstrap step loops forever, so the coordinator never leaves
 * maintenance mode and {@code /_api/version} 503s until the wait times out.
 */
public final class TestingArangoCluster implements AutoCloseable {
    private static final String COMPOSE_FILE = "src/test/resources/arangodb-cluster-compose.yml";
    private static final List<String> SERVICES = List.of("agency", "dbserver1", "coordinator");
    private static final int MAX_LOG_LINES_PER_SERVICE = 200;

    private static final int BOOT_ATTEMPTS = 3;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(4);
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(90);

    private final ComposeContainer compose;

    public TestingArangoCluster() {
        this.compose = boot();
    }

    /**
     * Boots a healthy cluster, retrying with a fresh {@link ComposeContainer} on failure. {@link
     * #compose} is {@code final}, so this static helper returns the winning container for the
     * constructor to assign, rather than looping inside the constructor body.
     */
    private static ComposeContainer boot() {
        return bootWithRetries(
                BOOT_ATTEMPTS,
                TestingArangoCluster::attemptBoot,
                TestingArangoCluster::reportFailedAttempt);
    }

    /**
     * Pure retry policy: call {@code attempt} up to {@code attempts} times, returning the first
     * success. Every failure is reported to {@code onFailure} with its 1-based attempt number
     * before retrying. If every attempt fails, throws {@link IllegalStateException} naming the
     * attempt count, with the last failure as the cause. Deliberately container-agnostic (no
     * Docker, no ArangoDB) so it is unit-testable without a real boot.
     */
    static <T> T bootWithRetries(
            int attempts, Supplier<T> attempt, BiConsumer<Integer, RuntimeException> onFailure) {
        RuntimeException lastFailure = null;
        for (int attemptNumber = 1; attemptNumber <= attempts; attemptNumber++) {
            try {
                return attempt.get();
            } catch (RuntimeException e) {
                lastFailure = e;
                onFailure.accept(attemptNumber, e);
            }
        }
        throw new IllegalStateException(
                "ArangoDB cluster failed to boot after " + attempts + " attempts", lastFailure);
    }

    /**
     * One boot attempt against a brand-new {@link ComposeContainer}. Captures the last {@link
     * #MAX_LOG_LINES_PER_SERVICE} lines per service throughout the attempt (not just on failure --
     * the consumer is wired before {@code start()} and there is no way to know in advance which
     * attempt will fail) and, on failure, stops the doomed container and rethrows wrapped in a
     * {@link BootAttemptException} carrying those logs for the caller to report.
     */
    private static ComposeContainer attemptBoot() {
        Map<String, CappedLog> logs = new HashMap<>();
        for (String service : SERVICES) {
            logs.put(service, new CappedLog());
        }
        ComposeContainer candidate =
                new ComposeContainer(new File(COMPOSE_FILE))
                        .withExposedService(
                                "coordinator",
                                8529,
                                Wait.forHttp("/_api/version")
                                        .forStatusCode(200)
                                        .withStartupTimeout(STARTUP_TIMEOUT));
        for (String service : SERVICES) {
            CappedLog log = logs.get(service);
            candidate.withLogConsumer(service, frame -> log.append(frame.getUtf8String()));
        }
        try {
            // start() must be inside this try for stop() to cover the start()-timeout case, not
            // just the readiness-probe failure below. Left running, a leaked container --
            // including Testcontainers' socat port ambassador -- sits until the leaked-container
            // safeguard hard-kills the JVM fork (turning one clean IT failure into an unreadable
            // "forked VM terminated without properly saying goodbye" crash) and starves the next
            // attempt's boot.
            candidate.start();
            awaitClusterReady(candidate, READY_TIMEOUT);
            return candidate;
        } catch (RuntimeException e) {
            candidate.stop();
            Map<String, List<String>> snapshot = new HashMap<>();
            for (String service : SERVICES) {
                snapshot.put(service, logs.get(service).snapshot());
            }
            throw new BootAttemptException(e, snapshot);
        }
    }

    /**
     * Prints the captured per-service logs for a failed attempt to stderr, clearly delimited. A
     * successful boot never calls this -- silence on the happy path keeps every green run readable.
     */
    private static void reportFailedAttempt(int attemptNumber, RuntimeException failure) {
        Map<String, List<String>> serviceLogs =
                failure instanceof BootAttemptException bootAttemptException
                        ? bootAttemptException.serviceLogs()
                        : Map.of();
        System.err.println(
                "=== boot attempt "
                        + attemptNumber
                        + "/"
                        + BOOT_ATTEMPTS
                        + " failed: "
                        + failure.getMessage()
                        + " ===");
        for (String service : SERVICES) {
            List<String> lines = serviceLogs.getOrDefault(service, List.of());
            System.err.println(
                    "=== boot attempt "
                            + attemptNumber
                            + "/"
                            + BOOT_ATTEMPTS
                            + " failed — "
                            + service
                            + " log (last "
                            + MAX_LOG_LINES_PER_SERVICE
                            + " lines) ===");
            for (String line : lines) {
                System.err.println(line);
            }
        }
    }

    /**
     * Wraps a boot-attempt failure together with the per-service logs captured up to that point.
     */
    private static final class BootAttemptException extends RuntimeException {
        private final transient Map<String, List<String>> serviceLogs;

        BootAttemptException(RuntimeException cause, Map<String, List<String>> serviceLogs) {
            super(cause.getMessage(), cause);
            this.serviceLogs = serviceLogs;
        }

        Map<String, List<String>> serviceLogs() {
            return serviceLogs;
        }
    }

    /** A synchronized ring buffer of the last {@link #MAX_LOG_LINES_PER_SERVICE} log lines. */
    private static final class CappedLog {
        private final Deque<String> lines = new ArrayDeque<>();

        synchronized void append(String frameText) {
            for (String line : frameText.split("\\R")) {
                if (line.isEmpty()) {
                    continue;
                }
                lines.addLast(line);
                if (lines.size() > MAX_LOG_LINES_PER_SERVICE) {
                    lines.removeFirst();
                }
            }
        }

        synchronized List<String> snapshot() {
            return List.copyOf(lines);
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
     *
     * <p>Takes the candidate {@link ComposeContainer} explicitly (rather than reading the {@code
     * compose} field) so it can run against an attempt's container before that attempt has won and
     * been published to the instance.
     */
    private static void awaitClusterReady(ComposeContainer candidate, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try (ArangoClient probe = new ArangoClient(configFor(candidate))) {
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
                "ArangoDB cluster did not become write-ready within "
                        + timeout.toSeconds()
                        + "s of boot",
                lastFailure);
    }

    public ArangoConfig config() {
        return configFor(compose);
    }

    private static ArangoConfig configFor(ComposeContainer candidate) {
        String host = candidate.getServiceHost("coordinator", 8529);
        int port = candidate.getServicePort("coordinator", 8529);
        return new ArangoConfig().setHosts(host + ":" + port);
    }

    @Override
    public void close() {
        compose.stop();
    }
}

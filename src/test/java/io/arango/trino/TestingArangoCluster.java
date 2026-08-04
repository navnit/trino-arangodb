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
 * <p><b>What was measured, and the actual root cause.</b> Job wall-clock across CI runs was
 * bimodal, with no middle: a successful boot completes the *entire* job (checkout, JDK setup, image
 * pull, Maven, cluster boot, all tests) in 94-113s; a failed boot burned the *entire*
 * startup-timeout window every time and never landed in between -- ruling out "the boot is slow" (a
 * slow-but-progressing boot would show intermediate durations). The per-service log consumers wired
 * below were added to capture the evidence needed to find the actual cause, and did: it is a
 * genuine deadlock, not slowness. The CI capture identified one boot-order race directly -- the
 * coordinator started 1.4s before the agency finished leader election, so both it and dbserver1
 * waited forever on agency state that was never written -- and reproducing that fix locally
 * surfaced a second, unrelated one: a phantom throwaway-instance registration race in the ArangoDB
 * image's own {@code /entrypoint.sh}, never visible in the CI capture because the first race always
 * fired first and masked it. Both are now fixed in {@code arangodb-cluster-compose.yml} (see the
 * comment block at the top of that file). Neither deadlock times out on its own, which is why no
 * timeout value could ever have fixed this by itself.
 *
 * <p><b>Retry with a FRESH container per attempt stays as the safety net.</b> The compose-level fix
 * addresses the races that were actually observed, but does not guarantee no boot ever hangs for
 * some other reason, so the retry this class already had is kept: a deadlocked cluster formation
 * does not recover, so a retry that reused the same {@link ComposeContainer} would just hang again;
 * each attempt tears down its container on failure and the next attempt starts a new one.
 * Per-attempt budgets are tight because a real boot is fast: {@link #STARTUP_TIMEOUT} (2 minutes)
 * and {@link #READY_TIMEOUT} (90 seconds) are both a healthy margin over the measured successful
 * case (boots complete in well under a minute), not an attempt to mask a genuine hang. Worst case
 * is {@link #BOOT_ATTEMPTS} (3) x ~3.5 minutes =~ 10.5 minutes plus test time, comfortably inside
 * the job's 30-minute ceiling.
 *
 * <p><b>Failing fast on a recognized deadlock.</b> Even with the race fixed, a bad run should not
 * have to burn the full {@link #STARTUP_TIMEOUT} before retrying: dbserver1 repeating {@link
 * #DEADLOCK_SIGNATURE} is an unambiguous deadlock tell (a healthy boot converges instead of
 * repeating it -- measured at zero occurrences across multiple clean boots, see {@link
 * #DEADLOCK_THRESHOLD}'s Javadoc), so {@link #DeadlockDetector} watches for it on dbserver1's log
 * stream and interrupts the booting thread the moment the threshold is crossed, rather than waiting
 * out the timeout. The matcher is a plain substring, deliberately not clever: if a future ArangoDB
 * version changes the message, detection simply never triggers and this degrades to the ordinary
 * timeout path, which is still correct.
 *
 * <p>Still true, unrelated to the above: one dbserver is enough because the shard-parallel ITs only
 * need a collection with more than one *shard*, and three shards live fine on a single PRIMARY.
 * {@code --cluster.system-replication-factor=1} on the coordinator is required for a
 * single-dbserver cluster: ArangoDB 3.12 creates its {@code _system} collections at
 * replicationFactor 2 by default, and with one dbserver that bootstrap step loops forever, so the
 * coordinator never leaves maintenance mode and {@code /_api/version} 503s until the wait times
 * out.
 */
public final class TestingArangoCluster implements AutoCloseable {
    private static final String COMPOSE_FILE = "src/test/resources/arangodb-cluster-compose.yml";
    private static final List<String> SERVICES = List.of("agency", "dbserver1", "coordinator");
    private static final int MAX_LOG_LINES_PER_SERVICE = 200;

    /** The service whose log stream is watched for {@link #DEADLOCK_SIGNATURE}. */
    private static final String DEADLOCK_WATCHED_SERVICE = "dbserver1";

    /**
     * Substring logged repeatedly by dbserver1 when it can't find its own registration in the
     * agency -- the exact symptom captured from the deadlocked CI run this class was written to
     * survive. A plain substring match on purpose: safe to go stale (a future ArangoDB message
     * change just means detection never fires, degrading to the existing timeout), not worth making
     * clever. Package-private (not {@code private}) so {@code TestingArangoClusterRetryTest} can
     * assert it verbatim against a captured log line, rather than every test re-typing the string
     * and being unable to catch a typo in the real constant.
     */
    static final String DEADLOCK_SIGNATURE = "Plan/DBServers in agency is no object";

    /**
     * How many times {@link #DEADLOCK_SIGNATURE} may appear before an attempt is aborted early.
     * Measured, not guessed, as far as a healthy boot goes: across multiple clean boots against the
     * fixed compose file, the line was never observed (count 0 every time) -- dbserver1 only
     * reaches the code path that could log it after its agency handshake already succeeded, and a
     * healthy boot's handshake succeeds immediately. It also proved *not* reproducible on demand
     * outside the original race's exact timing -- every attempt to force a real ArangoDB cluster
     * into logging it (unreachable agency, an agency that never elects, agency-then-real-dbserver
     * with no coordinator) left dbserver1 blocked silently *before* that code path instead, never
     * emitting the line at all; see the fast-abort verification in the fix's report for what was
     * tried. So 8 (~10s of spinning, since the original CI capture showed dbserver1 repeating it
     * roughly once every 1.2s) is chosen as comfortably above the only number that could be
     * measured -- zero -- while still aborting a genuinely deadlocked attempt in seconds rather
     * than minutes; it is not calibrated against an observed nonzero maximum, because none exists.
     */
    private static final int DEADLOCK_THRESHOLD = 8;

    private static final int BOOT_ATTEMPTS = 3;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
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
     *
     * <p>{@link #DEADLOCK_WATCHED_SERVICE}'s log consumer also feeds a {@link DeadlockDetector};
     * the moment it latches, this method's own thread (captured up front, since the detector fires
     * from a Testcontainers log-streaming thread, not this one) is interrupted. Both {@link
     * ComposeContainer#start()}'s internal wait and {@link #awaitClusterReady} already treat
     * interruption as a failure (the latter explicitly, the former because the ducttape retry
     * helper it uses under the hood surfaces an interrupted {@code Future#get} as its normal
     * timeout exception), so this aborts the attempt within roughly one log line's worth of latency
     * instead of waiting out {@link #STARTUP_TIMEOUT}.
     *
     * <p>The log consumer keeps streaming for the container's whole lifetime, not just during this
     * method -- so the {@code finally} below disarms the detector on every exit path (success,
     * caught failure, or anything the {@code catch} doesn't cover); the {@code catch} separately
     * clears the interrupt flag before {@code stop()}, for the unrelated reason explained there.
     * Without disarming, a winning attempt's detector would keep accumulating against the
     * (still-running) container and could interrupt an unrelated later thread mid-test; a losing
     * attempt's detector could latch after this method has already thrown for an unrelated reason
     * and interrupt the *next* attempt, aborting it in seconds but under the wrong label.
     */
    private static ComposeContainer attemptBoot() {
        Map<String, CappedLog> logs = new HashMap<>();
        for (String service : SERVICES) {
            logs.put(service, new CappedLog());
        }
        Thread bootThread = Thread.currentThread();
        DeadlockDetector deadlockDetector = new DeadlockDetector();
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
            boolean watched = service.equals(DEADLOCK_WATCHED_SERVICE);
            candidate.withLogConsumer(
                    service,
                    frame -> {
                        String text = frame.getUtf8String();
                        log.append(text);
                        if (watched && deadlockDetector.record(text)) {
                            bootThread.interrupt();
                        }
                    });
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
            // Clear a lingering interrupt FIRST, before stop(): awaitClusterReady re-sets the flag
            // before throwing on its own interrupted sleep, and candidate.stop() runs a full
            // GenericContainer teardown that itself performs interruptible waits. Calling stop()
            // with the flag still set risks stop() throwing instead of tearing down cleanly, which
            // would skip the BootAttemptException below entirely and hand reportFailedAttempt a
            // bare exception -- a blank dump, the wrong reason label, and a leaked compose project.
            Thread.interrupted();
            candidate.stop();
            Map<String, List<String>> snapshot = new HashMap<>();
            for (String service : SERVICES) {
                snapshot.put(service, logs.get(service).snapshot());
            }
            throw new BootAttemptException(e, snapshot, deadlockDetector.triggered());
        } finally {
            // Disarm the detector and clear any lingering interrupt so neither can affect anything
            // after this method returns/throws: the log consumer keeps streaming for the
            // container's whole lifetime (not just this method), so a still-armed detector on a
            // *winning* attempt's container could later interrupt an unrelated thread mid-test, and
            // one on a *losing* attempt could latch just after this method threw for an unrelated
            // reason and interrupt the next attempt (which reuses this same thread) under the wrong
            // label. Redundant with the catch-block clear above on the exception path; belt and
            // braces for the success path and any path this catch doesn't cover.
            deadlockDetector.disarm();
            Thread.interrupted();
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
        String reason =
                failure instanceof BootAttemptException bootAttemptException
                                && bootAttemptException.deadlockSignatureDetected()
                        ? "deadlock signature detected"
                        : "timed out / failed";
        System.err.println(
                "=== boot attempt "
                        + attemptNumber
                        + "/"
                        + BOOT_ATTEMPTS
                        + " failed ("
                        + reason
                        + "): "
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
     * Wraps a boot-attempt failure together with the per-service logs captured up to that point,
     * and whether the failure was an early, deliberate abort ({@link DeadlockDetector} latched)
     * rather than an ordinary timeout -- surfaced so {@link #reportFailedAttempt} can label the
     * dump with the right cause.
     */
    private static final class BootAttemptException extends RuntimeException {
        private final transient Map<String, List<String>> serviceLogs;
        private final boolean deadlockSignatureDetected;

        BootAttemptException(
                RuntimeException cause,
                Map<String, List<String>> serviceLogs,
                boolean deadlockSignatureDetected) {
            super(cause.getMessage(), cause);
            this.serviceLogs = serviceLogs;
            this.deadlockSignatureDetected = deadlockSignatureDetected;
        }

        Map<String, List<String>> serviceLogs() {
            return serviceLogs;
        }

        boolean deadlockSignatureDetected() {
            return deadlockSignatureDetected;
        }
    }

    /**
     * Latches once {@link #DEADLOCK_SIGNATURE} has appeared {@link #DEADLOCK_THRESHOLD} or more
     * times across the frames fed to {@link #record}. Stateful and {@code synchronized} for the
     * same reason as {@link CappedLog}: {@link #record} runs on Testcontainers' log-streaming
     * threads, concurrently with each other and with whatever thread reads {@link #triggered()}.
     * {@link #record} returns {@code true} exactly once -- on the call that crosses the threshold
     * -- so callers can trigger a side effect (interrupting the boot thread) precisely once per
     * attempt.
     *
     * <p>{@code armed} and {@code triggered} are deliberately separate fields, not one field doing
     * double duty: {@code armed} is "should {@link #record} still do anything" (turned off once by
     * {@link #disarm()}), {@code triggered} is "did the signature actually cross the threshold"
     * (read by {@link #triggered()} for {@link BootAttemptException}'s reason label). Collapsing
     * them into a single flag -- as an earlier version of this class did, using {@code triggered}
     * for both -- would make the reason label's correctness depend on {@link #triggered()} being
     * read before {@link #disarm()} runs, an ordering that is easy to break by moving or
     * duplicating a {@code disarm()} call. With two fields, {@link #disarm()} cannot affect what
     * {@link #triggered()} reports, so no such ordering exists to break.
     */
    private static final class DeadlockDetector {
        private int count;
        private boolean armed = true;
        private boolean triggered;

        synchronized boolean record(String frameText) {
            if (!armed || triggered) {
                return false;
            }
            count += countMatchingLines(frameText, DEADLOCK_SIGNATURE);
            if (!deadlockThresholdReached(count, DEADLOCK_THRESHOLD)) {
                return false;
            }
            triggered = true;
            return true;
        }

        /**
         * Whether the signature actually crossed the threshold -- unaffected by {@link #disarm()}.
         */
        synchronized boolean triggered() {
            return triggered;
        }

        /**
         * Permanently disables further triggering (and further counting) without touching {@link
         * #triggered}. Called once {@code attemptBoot} is done with this detector (win or lose) so
         * frames that arrive afterward -- the log consumer outlives this method, streaming for as
         * long as the container itself runs -- can never fire {@link #record} again.
         */
        synchronized void disarm() {
            armed = false;
        }
    }

    /**
     * Pure line-splitting + substring count, extracted from {@link DeadlockDetector#record} so it
     * is unit-testable without any concurrency or container involved. Mirrors {@link
     * CappedLog#append}'s own line-splitting.
     */
    static int countMatchingLines(String frameText, String signature) {
        int matches = 0;
        for (String line : frameText.split("\\R")) {
            if (line.contains(signature)) {
                matches++;
            }
        }
        return matches;
    }

    /**
     * Pure threshold predicate, extracted from {@link DeadlockDetector#record} so the boundary
     * condition is unit-testable in isolation.
     */
    static boolean deadlockThresholdReached(int count, int threshold) {
        return count >= threshold;
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

package io.arango.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TestingArangoCluster#bootWithRetries}, the retry policy the cluster boot
 * uses to survive a hung {@code ComposeContainer} formation (see the boot-flake fix design: a
 * deadlocked cluster does not recover, so retrying means a FRESH container per attempt, never
 * reusing the failed one). This logic is pure -- no Docker, no containers -- so it runs in the
 * normal unit suite rather than needing a real cluster boot to exercise it.
 */
class TestingArangoClusterRetryTest {
    @Test
    void returnsFirstSuccessWithoutExtraAttempts() {
        List<Integer> attemptsMade = new ArrayList<>();
        Supplier<String> attempt =
                () -> {
                    attemptsMade.add(1);
                    return "booted";
                };
        String result = TestingArangoCluster.bootWithRetries(3, attempt, (n, e) -> fail());
        assertEquals("booted", result);
        assertEquals(1, attemptsMade.size());
    }

    @Test
    void retriesAfterAFailureAndReturnsALaterSuccess() {
        int[] callCount = {0};
        Supplier<String> attempt =
                () -> {
                    callCount[0]++;
                    if (callCount[0] == 1) {
                        throw new RuntimeException("first attempt hangs");
                    }
                    return "booted on attempt " + callCount[0];
                };
        List<Integer> failedAttempts = new ArrayList<>();
        String result =
                TestingArangoCluster.bootWithRetries(3, attempt, (n, e) -> failedAttempts.add(n));
        assertEquals("booted on attempt 2", result);
        assertEquals(2, callCount[0]);
        assertEquals(List.of(1), failedAttempts);
    }

    @Test
    void invokesTheFailureCallbackOncePerFailedAttemptWithTheRightAttemptNumber() {
        Supplier<String> attempt =
                () -> {
                    throw new RuntimeException("always hangs");
                };
        List<Integer> failedAttempts = new ArrayList<>();
        assertThrows(
                IllegalStateException.class,
                () ->
                        TestingArangoCluster.bootWithRetries(
                                3, attempt, (n, e) -> failedAttempts.add(n)));
        assertEquals(List.of(1, 2, 3), failedAttempts);
    }

    @Test
    void afterAllAttemptsFailThrowsWithTheLastFailureAsTheCause() {
        RuntimeException first = new RuntimeException("attempt 1 hangs");
        RuntimeException second = new RuntimeException("attempt 2 hangs");
        RuntimeException third = new RuntimeException("attempt 3 hangs");
        List<RuntimeException> failures = List.of(first, second, third);
        int[] callCount = {0};
        Supplier<String> attempt =
                () -> {
                    throw failures.get(callCount[0]++);
                };
        IllegalStateException thrown =
                assertThrows(
                        IllegalStateException.class,
                        () -> TestingArangoCluster.bootWithRetries(3, attempt, (n, e) -> {}));
        assertSame(third, thrown.getCause());
    }

    private static void fail() {
        throw new AssertionError("onFailure must not be invoked when the first attempt succeeds");
    }

    @Test
    void countMatchingLinesCountsOnlyLinesContainingTheSignature() {
        String frame =
                "some other line\n"
                        + "Plan/DBServers in agency is no object, but none. Agency not"
                        + " initialized?\n"
                        + "another unrelated line\n"
                        + "Plan/DBServers in agency is no object, but none. Agency not"
                        + " initialized?\n";
        assertEquals(2, TestingArangoCluster.countMatchingLines(frame, "Plan/DBServers"));
    }

    @Test
    void countMatchingLinesReturnsZeroWhenTheSignatureNeverAppears() {
        String frame = "ArangoDB is ready for business\nusing endpoint tcp://0.0.0.0:8530\n";
        assertEquals(0, TestingArangoCluster.countMatchingLines(frame, "Plan/DBServers"));
    }

    @Test
    void deadlockThresholdReachedIsFalseBelowTheThresholdAndTrueAtOrAboveIt() {
        assertFalse(TestingArangoCluster.deadlockThresholdReached(7, 8));
        assertTrue(TestingArangoCluster.deadlockThresholdReached(8, 8));
        assertTrue(TestingArangoCluster.deadlockThresholdReached(9, 8));
    }
}

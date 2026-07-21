package io.arango.trino.split;

import com.google.inject.Inject;
import io.arango.trino.client.ArangoClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ShardFanoutCapability {
    // Minimum ArangoDB version trusted for the internal shardIds option (master spec §0).
    static final String MIN_ARANGO_VERSION = "3.11";

    private enum Verdict { UNKNOWN, ENABLED, DISABLED }

    private final ArangoClient client;
    private final AtomicReference<Verdict> verdict = new AtomicReference<>(Verdict.UNKNOWN);

    @Inject
    public ShardFanoutCapability(ArangoClient client) {
        this.client = client;
    }

    /**
     * True only if the server is trusted (version pin) AND an active probe confirms the internal
     * shardIds option actually narrows results. Computed once on the first eligible collection and
     * cached for the connector's process lifetime (spec §8.2 — lazy-first-fan-out interpretation).
     */
    public boolean canFanOut(String database, String collection, List<List<String>> groups) {
        if (verdict.get() == Verdict.UNKNOWN) {
            Verdict computed = compute(database, collection, groups);
            if (computed != Verdict.UNKNOWN) {          // only a CONCLUSIVE verdict latches
                verdict.compareAndSet(Verdict.UNKNOWN, computed);
            }
            return computed == Verdict.ENABLED;         // inconclusive -> false this call, retried next
        }
        return verdict.get() == Verdict.ENABLED;
    }

    // Probes the ACTUAL groups about to be emitted (so multi-element shardIds arrays are exercised).
    // ENABLED / DISABLED are conclusive (cached); UNKNOWN is inconclusive (not cached, retried).
    private Verdict compute(String database, String collection, List<List<String>> groups) {
        try {
            if (!isVersionAtLeastMinimum(client.serverVersion())) {
                return Verdict.DISABLED;                 // conclusive: too old, will not improve
            }
            long full = client.countWithShardIds(database, collection, List.of());
            if (full == 0) {
                return Verdict.UNKNOWN;                   // inconclusive: empty collection, retry later
            }
            long sum = 0;
            boolean anyNarrower = false;
            for (List<String> group : groups) {
                long c = client.countWithShardIds(database, collection, group);
                sum += c;
                if (c < full) {
                    anyNarrower = true;
                }
            }
            if (sum != full) {
                // Inconclusive, NOT conclusive: `full` and the per-group counts are separate queries,
                // so a concurrent write can make sum != full from write-skew alone. A server that truly
                // ignores shardIds also lands here (sum = N×full) and still never fans out (UNKNOWN ->
                // single split). Either way we retry next time rather than latching DISABLED for the
                // whole process lifetime. Safe: UNKNOWN never enables fan-out, so never duplicates.
                return Verdict.UNKNOWN;
            }
            return anyNarrower ? Verdict.ENABLED : Verdict.UNKNOWN; // all-in-one-group -> can't confirm narrowing, retry
        }
        catch (RuntimeException e) {
            return Verdict.UNKNOWN;                       // inconclusive: transient, fall back this call, retry later
        }
    }

    /** Σ(per-group shard-scoped counts) == full count. Shared by the probe and the CI count-sum gate. */
    public static boolean sumMatchesFull(ArangoClient client, String database, String collection, List<List<String>> groups) {
        long full = client.countWithShardIds(database, collection, List.of());
        long sum = groups.stream()
                .mapToLong(g -> client.countWithShardIds(database, collection, g))
                .sum();
        return sum == full;
    }

    static boolean isVersionAtLeastMinimum(String actual) {
        return compareVersions(actual, MIN_ARANGO_VERSION) >= 0;
    }

    private static int compareVersions(String a, String b) {
        String[] pa = a.split("[.-]");
        String[] pb = b.split("[.-]");
        int len = Math.max(pa.length, pb.length);
        for (int i = 0; i < len; i++) {
            int va = i < pa.length ? leadingInt(pa[i]) : 0;
            int vb = i < pb.length ? leadingInt(pb[i]) : 0;
            if (va != vb) {
                return Integer.compare(va, vb);
            }
        }
        return 0;
    }

    private static int leadingInt(String s) {
        int end = 0;
        while (end < s.length() && Character.isDigit(s.charAt(end))) {
            end++;
        }
        return end == 0 ? 0 : Integer.parseInt(s.substring(0, end));
    }
}

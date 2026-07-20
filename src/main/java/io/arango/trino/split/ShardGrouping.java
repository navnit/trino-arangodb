package io.arango.trino.split;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class ShardGrouping {
    private ShardGrouping() {}

    /**
     * Partition shard IDs into balanced groups so the union of per-group scans equals the full scan.
     * <p>splits = min(ceil(N / shardsPerSplit), maxSplits). {@code maxSplits} is a HARD cap that can
     * force more than {@code shardsPerSplit} shards into a group when ceil(N/shardsPerSplit) exceeds it.
     * Every shard appears in exactly one group (the M3 correctness invariant).
     */
    public static List<List<String>> partition(List<String> shardIds, int shardsPerSplit, int maxSplits) {
        checkArgument(shardsPerSplit >= 1, "shardsPerSplit must be >= 1");
        checkArgument(maxSplits >= 1, "maxSplits must be >= 1");
        int n = shardIds.size();
        if (n == 0) {
            return List.of();
        }
        int groupsBySize = (n + shardsPerSplit - 1) / shardsPerSplit; // ceil(n / shardsPerSplit)
        int groups = Math.min(groupsBySize, maxSplits);
        int base = n / groups;
        int remainder = n % groups;
        List<List<String>> result = new ArrayList<>(groups);
        int idx = 0;
        for (int g = 0; g < groups; g++) {
            int size = base + (g < remainder ? 1 : 0); // first `remainder` groups get one extra
            result.add(List.copyOf(shardIds.subList(idx, idx + size)));
            idx += size;
        }
        return List.copyOf(result);
    }
}

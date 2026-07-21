package io.arango.trino.split;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardGroupingTest {
    private static List<String> shards(int n) {
        List<String> s = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            s.add("s" + i);
        }
        return s;
    }

    @Test
    void oneShardPerSplitByDefault() {
        assertEquals(List.of(List.of("s1"), List.of("s2"), List.of("s3")),
                ShardGrouping.partition(shards(3), 1, 32));
    }

    @Test
    void groupsBySizeWhenShardsPerSplitAboveOne() {
        // ceil(3/2) = 2 groups, balanced -> [2,1]
        assertEquals(List.of(List.of("s1", "s2"), List.of("s3")),
                ShardGrouping.partition(shards(3), 2, 32));
    }

    @Test
    void maxSplitsIsAHardCapThatForcesLargerGroups() {
        // N=10, S=1 would be 10 groups, but cap=3 wins -> 3 groups, sizes [4,3,3]
        List<List<String>> groups = ShardGrouping.partition(shards(10), 1, 3);
        assertEquals(3, groups.size());
        assertEquals(List.of(4, 3, 3), groups.stream().map(List::size).toList());
    }

    @Test
    void emptyInputYieldsNoGroups() {
        assertTrue(ShardGrouping.partition(List.of(), 1, 32).isEmpty());
    }

    @Test
    void rejectsNonPositiveArgs() {
        assertThrows(IllegalArgumentException.class, () -> ShardGrouping.partition(shards(3), 0, 32));
        assertThrows(IllegalArgumentException.class, () -> ShardGrouping.partition(shards(3), 1, 0));
    }

    @Test
    void invariantHoldsAcrossManyCombinations() {
        for (int n = 1; n <= 40; n++) {
            for (int s = 1; s <= 6; s++) {
                for (int m = 1; m <= 12; m++) {
                    List<String> input = shards(n);
                    List<List<String>> groups = ShardGrouping.partition(input, s, m);
                    int expected = Math.min((n + s - 1) / s, m);
                    assertEquals(expected, groups.size(), "split count for n=" + n + " s=" + s + " m=" + m);
                    // union == input, in order, no dupes:
                    List<String> flat = groups.stream().flatMap(List::stream).toList();
                    assertEquals(input, flat, "partition must cover every shard exactly once");
                    // balanced: sizes differ by at most 1
                    int min = groups.stream().mapToInt(List::size).min().orElse(0);
                    int max = groups.stream().mapToInt(List::size).max().orElse(0);
                    assertTrue(max - min <= 1, "groups must be balanced");
                }
            }
        }
    }
}

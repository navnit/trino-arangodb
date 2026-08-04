package io.arango.trino;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * Targets M6-A spec §2's guard chain: rows that decline (or answer without a count) must do so
 * before any client call — constructed with a null client so an accidental count throws NPE.
 */
class ArangoMetadataStatisticsTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VarcharType.VARCHAR, false, List.of("city"));

    private static ArangoTableHandle plainHandle() {
        return new ArangoTableHandle(
                "shop", "users", false, TupleDomain.all(), OptionalLong.empty(), Optional.empty());
    }

    private static ArangoAggregation globalAggregation() {
        // count(*): empty groupingColumns — the connector emits exactly one row (spec §2).
        return new ArangoAggregation(
                List.of(),
                List.of(
                        new AggregateSpec(
                                AggregateSpec.Kind.COUNT_STAR,
                                Optional.empty(),
                                "count",
                                BigintType.BIGINT)));
    }

    private static ArangoAggregation groupedAggregation() {
        // bare GROUP BY city (a pushed SELECT DISTINCT) — group cardinality unknowable.
        return new ArangoAggregation(List.of(CITY), List.of());
    }

    @Test
    void disabledFlagReturnsEmptyWithoutClientCall() {
        ArangoMetadata metadata =
                new ArangoMetadata(null, null, new ArangoConfig().setStatisticsEnabled(false));
        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());
        assertThat(stats.getRowCount().isUnknown())
                .as("kill switch must short-circuit before the client")
                .isTrue();
    }

    @Test
    void queryHandleReturnsEmpty() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoQueryHandle handle =
                new ArangoQueryHandle("shop", "FOR d IN users RETURN d", List.of());
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
    }

    @Test
    void globalAggregationReportsExactlyOneRowWithoutClientCall() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = plainHandle().withAggregation(globalAggregation());
        TableStatistics stats = metadata.getTableStatistics(null, handle);
        assertThat(stats.getRowCount().getValue()).isEqualTo(1.0);
    }

    @Test
    void groupedAggregationReturnsEmpty() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = plainHandle().withAggregation(groupedAggregation());
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
    }

    @Test
    void aggregationRowsWinOverLimit() {
        // Pins spec §2's guard ordering: a handle carrying both an aggregation and a limit takes
        // the aggregation rows (here: global -> 1), never the limit/count path.
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = plainHandle().withAggregation(globalAggregation()).withLimit(5);
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().getValue())
                .isEqualTo(1.0);
    }

    @Test
    void groupedAggregationWithPushedLimitReportsLimitWithoutClientCall() {
        // applyLimit reports limitGuaranteed=true for aggregated handles (single split, LIMIT
        // rendered after the COLLECT), so the pushed limit is an exact output cap (spec §2).
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle =
                plainHandle().withAggregation(groupedAggregation()).withLimit(10);
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().getValue())
                .isEqualTo(10.0);
    }

    @Test
    void disabledFlagWinsOverAggregation() {
        // Kill switch is the first row of the matrix: even the client-free Estimate.of(1) answer
        // is suppressed when statistics are disabled.
        ArangoMetadata metadata =
                new ArangoMetadata(null, null, new ArangoConfig().setStatisticsEnabled(false));
        ArangoTableHandle handle = plainHandle().withAggregation(globalAggregation());
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
    }
}

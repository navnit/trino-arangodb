package io.arango.trino;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;

import io.airlift.units.Duration;
import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
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

    /** Per-table counts + call counter; flaky mode throws once then succeeds. */
    private static class CountingArangoClient extends ArangoClient {
        final java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        final java.util.Map<String, Long> countsByCollection;
        boolean failNextCall;

        CountingArangoClient(java.util.Map<String, Long> countsByCollection) {
            super(new ArangoConfig());
            this.countsByCollection = countsByCollection;
        }

        @Override
        public long countDocuments(String database, String collection) {
            calls.incrementAndGet();
            if (failNextCall) {
                failNextCall = false;
                throw new IllegalStateException("simulated count failure");
            }
            return countsByCollection.get(collection);
        }
    }

    private static final class ManualTicker extends com.google.common.base.Ticker {
        long nanos;

        @Override
        public long read() {
            return nanos;
        }

        void advance(long amount, java.util.concurrent.TimeUnit unit) {
            nanos += unit.toNanos(amount);
        }
    }

    private static ArangoTableHandle handleFor(String collection) {
        return new ArangoTableHandle(
                "shop",
                collection,
                false,
                TupleDomain.all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    @Test
    void plainHandleSurfacesCount() {
        CountingArangoClient client = new CountingArangoClient(java.util.Map.of("users", 42L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        assertThat(metadata.getTableStatistics(null, handleFor("users")).getRowCount().getValue())
                .isEqualTo(42.0);
    }

    @Test
    void filteredHandleSurfacesSameBaseCount() {
        // Spec §1 recorded decision: filter presence does not change the number.
        CountingArangoClient client = new CountingArangoClient(java.util.Map.of("users", 42L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        ArangoTableHandle filtered =
                handleFor("users")
                        .withConstraint(
                                TupleDomain.withColumnDomains(
                                        java.util.Map.<ColumnHandle, Domain>of(
                                                CITY,
                                                Domain.singleValue(
                                                        VarcharType.VARCHAR,
                                                        utf8Slice("london")))));
        assertThat(metadata.getTableStatistics(null, filtered).getRowCount().getValue())
                .isEqualTo(42.0);
    }

    @Test
    void emptyCollectionIsZeroNotUnknown() {
        CountingArangoClient client = new CountingArangoClient(java.util.Map.of("empty_col", 0L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        TableStatistics stats = metadata.getTableStatistics(null, handleFor("empty_col"));
        assertThat(stats.getRowCount().isUnknown()).isFalse();
        assertThat(stats.getRowCount().getValue()).isEqualTo(0.0);
    }

    @Test
    void limitMinAppliedOnlyWhenSingleSplit() {
        CountingArangoClient client = new CountingArangoClient(java.util.Map.of("users", 42L));
        // Default config: shard parallelism on -> limit NOT applied (the engine's retained
        // LimitNode does the min; pre-applying would misstate the scan node, spec §2).
        ArangoMetadata fanOut = new ArangoMetadata(client, null, new ArangoConfig());
        assertThat(
                        fanOut.getTableStatistics(null, handleFor("users").withLimit(5))
                                .getRowCount()
                                .getValue())
                .isEqualTo(42.0);
        // Parallelism off -> pushed limit is exact (mirrors applyLimit's limitGuaranteed) -> min.
        ArangoMetadata singleSplit =
                new ArangoMetadata(
                        client, null, new ArangoConfig().setShardParallelismEnabled(false));
        assertThat(
                        singleSplit
                                .getTableStatistics(null, handleFor("users").withLimit(5))
                                .getRowCount()
                                .getValue())
                .isEqualTo(5.0);
        // ...and a limit above the count changes nothing.
        assertThat(
                        singleSplit
                                .getTableStatistics(null, handleFor("users").withLimit(100))
                                .getRowCount()
                                .getValue())
                .isEqualTo(42.0);
    }

    @Test
    void countIsCachedWithinTtlAndRefreshedAfter() {
        CountingArangoClient client = new CountingArangoClient(java.util.Map.of("users", 42L));
        ManualTicker ticker = new ManualTicker();
        ArangoMetadata metadata =
                new ArangoMetadata(
                        client,
                        null,
                        new ArangoConfig().setStatisticsCacheTtl(new Duration(1, TimeUnit.MINUTES)),
                        ticker);
        ArangoTableHandle handle = handleFor("users");

        metadata.getTableStatistics(null, handle);
        metadata.getTableStatistics(null, handle);
        assertThat(client.calls.get()).as("second call within TTL served from cache").isEqualTo(1);

        // Statistics TTL is 1m, schema TTL (default) is 5m. Advancing 2m expires the count
        // cache only if wired correctly to getStatisticsCacheTtl(); a schema-TTL mis-wire would
        // still serve from cache and fail the calls==2 assertion.
        ticker.advance(2, TimeUnit.MINUTES);
        metadata.getTableStatistics(null, handle);
        assertThat(client.calls.get()).as("expired entry re-counts").isEqualTo(2);
    }

    @Test
    void cacheKeysAreIsolatedPerTable() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L, "orders", 7L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        assertThat(metadata.getTableStatistics(null, handleFor("users")).getRowCount().getValue())
                .isEqualTo(42.0);
        assertThat(metadata.getTableStatistics(null, handleFor("orders")).getRowCount().getValue())
                .isEqualTo(7.0);
        assertThat(client.calls.get()).isEqualTo(2);
    }

    @Test
    void countFailureDegradesToEmptyAndIsNotNegativeCached() {
        CountingArangoClient client = new CountingArangoClient(java.util.Map.of("users", 42L));
        client.failNextCall = true;
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        ArangoTableHandle handle = handleFor("users");

        // Failure -> empty(), planning does not throw (spec §4 deviation).
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
        // No negative caching: the very next call retries and succeeds.
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().getValue())
                .isEqualTo(42.0);
        assertThat(client.calls.get()).isEqualTo(2);
    }
}

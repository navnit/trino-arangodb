package io.arango.trino;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Boots exactly one {@link TestingArangoCluster} and shares it across every IT annotated with
 * {@code @ExtendWith(SharedArangoClusterExtension.class)}, stopping it once at the end of the
 * whole test plan.
 *
 * <p><b>Why share.</b> Each {@link TestingArangoCluster} is a four-node ArangoDB stack (agency
 * + two dbservers + coordinator) booted via Testcontainers Compose. GitHub's 2-vCPU
 * {@code ubuntu-latest} runner can stand up the <em>first</em> such cluster in a run (~40s) but
 * not a <em>second</em> one later on: the second boot's coordinator never answers
 * {@code /_api/version} within even a five-minute wait (cumulative resource pressure from the
 * first cluster plus the earlier Testcontainers ITs). Booting once removes that fatal second
 * boot. The shard-parallel ITs are isolated by database ({@code shard_it} vs
 * {@code shard_correct_it}), so a single cluster serves both without cross-contamination.
 *
 * <p><b>Why a root-store {@link ExtensionContext.Store.CloseableResource}</b> rather than a
 * Ryuk-reaped "never stop" singleton or a JVM shutdown hook: a value put into the <em>root</em>
 * store is closed when the root context closes, at the end of the test plan -- which runs
 * <em>before</em> Trino's {@code ReportLeakedContainers} launcher-session check. A cluster left
 * running for Ryuk to reap would still be up at that check and trip the leaked-container
 * safeguard, which hard-kills the failsafe fork ("forked VM terminated without properly saying
 * goodbye") -- exactly the crash this whole change removes.
 */
public final class SharedArangoClusterExtension implements BeforeAllCallback {
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(SharedArangoClusterExtension.class);
    private static final String KEY = "cluster";

    private static volatile TestingArangoCluster cluster;

    @Override
    public synchronized void beforeAll(ExtensionContext context) {
        if (cluster == null) {
            TestingArangoCluster started = new TestingArangoCluster();
            // Register the stop on the ROOT store so JUnit closes it at end of the test plan
            // (before ReportLeakedContainers runs); publish the field only after a successful
            // boot so a boot failure leaves it null and does not register a no-op resource.
            context.getRoot().getStore(NAMESPACE)
                    .put(KEY, (ExtensionContext.Store.CloseableResource) started::close);
            cluster = started;
        }
    }

    /** The shared cluster. Non-null once any extended IT's {@code beforeAll} has run. */
    public static TestingArangoCluster cluster() {
        return cluster;
    }
}

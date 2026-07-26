package io.arango.trino.aggregation;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.Optional;

/**
 * The AQL rendering of {@link io.arango.trino.type.ValueMaterializer}'s coercion, and the single
 * place M5's exactness invariant lives: a pushed aggregate must compute over exactly the values the
 * read path would have emitted. {@link #predicate} admits precisely what the read path materializes
 * non-NULL; {@link #coerce} maps everything else to AQL {@code null}, which AQL's aggregates ignore
 * -- matching Trino's own NULL handling.
 *
 * <p>The bar is higher here than for filter pushdown. {@code applyFilter} may push a prefilter and
 * let Trino re-check the residual (that is how BIGINT range works); aggregation has no residual,
 * because Trino replaces the aggregation node and treats this output as final. Anything rendered
 * here must therefore be exact rather than a superset.
 */
public final class ColumnGuard {
    /**
     * Why the value expression depends on context, when the predicate does not:
     *
     * <ul>
     *   <li>{@code GROUPING_KEY} -- DOUBLE promotes with {@code + 0.0} and BIGINT normalizes signed
     *       zero, because COLLECT puts a stored {@code -0.0} in its own group (design §4/10, §4/18)
     *       while both read back as the same Trino value. Without this, one Trino group would be
     *       emitted as two final rows.
     *   <li>{@code SUM_AVG} -- DOUBLE keeps the {@code + 0.0} promotion, which is the M2 finding-C1
     *       argument unchanged: it makes AQL accumulate in the same double space the read path's
     *       {@code doubleValue()} produces.
     *   <li>{@code MIN_MAX} -- bare accessor. Promotion would turn a stored {@code -0.0} into
     *       {@code 0.0} and disagree with the unpushed plan, and it is unnecessary because double
     *       rounding is monotone: the bare extremum, rounded on read, equals the extremum of the
     *       rounded values (design §4/22).
     * </ul>
     */
    public enum Purpose {
        GROUPING_KEY,
        SUM_AVG,
        MIN_MAX
    }

    private ColumnGuard() {}

    /**
     * A boolean AQL expression true for exactly the values the read path materializes non-NULL, or
     * empty when the column's type supports no exact guard (ARRAY/ROW/DECIMAL).
     */
    public static Optional<String> predicate(Type type, String accessor) {
        if (type.equals(BooleanType.BOOLEAN)) {
            return Optional.of("IS_BOOL(" + accessor + ")");
        }
        if (type instanceof VarcharType) {
            return Optional.of("IS_STRING(" + accessor + ")");
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return Optional.of("IS_NUMBER(" + accessor + ")");
        }
        if (type.equals(BigintType.BIGINT)) {
            // ValueMaterializer.isIntegralInLongRange, transliterated: a number, within
            // [-2^63, 2^63), and integral. The integrality test cannot be a bare
            // `v == FLOOR(v)` -- FLOOR returns a double, so a stored int64 above 2^53 fails it
            // (review finding C3). Above 2^53 no double can carry a fractional part, so every
            // value there is integral and the FLOOR test is only needed below that threshold.
            // The range bound is exact because ArangoDB compares int64 against double by exact
            // mathematical value (the fact behind finding C1), not in double space.
            return Optional.of(
                    "IS_NUMBER(%1$s) AND %1$s >= -9223372036854775808 AND %1$s <"
                                    .formatted(accessor)
                            + " 9223372036854775808 AND (ABS(%1$s) >= 9007199254740992 OR %1$s =="
                                    .formatted(accessor)
                            + " FLOOR(%1$s))".formatted(accessor));
        }
        return Optional.empty();
    }

    /** The value expression to aggregate or group by, for a type {@link #predicate} admits. */
    public static String value(Type type, String accessor, Purpose purpose) {
        if (type.equals(DoubleType.DOUBLE) && purpose != Purpose.MIN_MAX) {
            return "(" + accessor + " + 0.0)";
        }
        if (type.equals(BigintType.BIGINT) && purpose == Purpose.GROUPING_KEY) {
            return "(" + accessor + " == 0 ? 0 : " + accessor + ")";
        }
        return accessor;
    }

    /** {@code ((predicate) ? value : null)}, or empty when the type supports no exact guard. */
    public static Optional<String> coerce(Type type, String accessor, Purpose purpose) {
        return predicate(type, accessor)
                .map(p -> "((" + p + ") ? " + value(type, accessor, purpose) + " : null)");
    }
}

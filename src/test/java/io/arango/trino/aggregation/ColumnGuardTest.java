package io.arango.trino.aggregation;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.ColumnGuard.Purpose;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ColumnGuardTest {
    private static final String A = "d[\"v\"]";

    @Test
    void predicatesMatchTheReadPathPerType() {
        assertThat(ColumnGuard.predicate(BOOLEAN, A)).contains("IS_BOOL(" + A + ")");
        assertThat(ColumnGuard.predicate(VARCHAR, A)).contains("IS_STRING(" + A + ")");
        assertThat(ColumnGuard.predicate(DOUBLE, A)).contains("IS_NUMBER(" + A + ")");
    }

    // The BIGINT predicate transliterates ValueMaterializer.isIntegralInLongRange: a number,
    // within [-2^63, 2^63), and integral. Integrality cannot use a bare FLOOR test (review
    // finding C3: FLOOR returns a double, so a stored int64 above 2^53 fails it) -- above 2^53
    // no double can carry a fractional part, so everything there is integral by construction.
    @Test
    void bigintPredicateGuardsRangeAndIntegralityWithoutABareFloorTest() {
        String p = ColumnGuard.predicate(BIGINT, A).orElseThrow();
        assertThat(p)
                .isEqualTo(
                        "IS_NUMBER(d[\"v\"]) AND d[\"v\"] >= -9223372036854775808 AND d[\"v\"] <"
                                + " 9223372036854775808 AND (ABS(d[\"v\"]) >= 9007199254740992 OR"
                                + " d[\"v\"] == FLOOR(d[\"v\"]))");
    }

    // Design §4/10: COLLECT separates -0.0 from 0.0. The DOUBLE grouping key's `+ 0.0` collapses
    // them, matching Trino's normalization. SUM/AVG keep it for the M2 finding-C1 reason.
    @Test
    void doubleGroupingKeyAndSumPromoteToDoubleSpace() {
        assertThat(ColumnGuard.value(DOUBLE, A, Purpose.GROUPING_KEY))
                .isEqualTo("(" + A + " + 0.0)");
        assertThat(ColumnGuard.value(DOUBLE, A, Purpose.SUM_AVG)).isEqualTo("(" + A + " + 0.0)");
    }

    // Review finding S1 / §4/22: `+ 0.0` would turn a stored -0.0 into 0.0, so min/max would
    // disagree with the unpushed plan. Rounding is monotone, so the promotion is unnecessary here.
    @Test
    void minMaxUsesTheBareAccessorSoSignedZeroSurvives() {
        assertThat(ColumnGuard.value(DOUBLE, A, Purpose.MIN_MAX)).isEqualTo(A);
        assertThat(ColumnGuard.value(BIGINT, A, Purpose.MIN_MAX)).isEqualTo(A);
    }

    // Review finding B1 / §4/18-19: a stored double -0.0 passes the BIGINT guard and reads back
    // as 0, but hash-COLLECT groups it separately -- two AQL groups, one Trino key, duplicate
    // output rows. Normalizing by exact numeric equality fixes it under both COLLECT methods.
    @Test
    void bigintGroupingKeyNormalizesSignedZero() {
        assertThat(ColumnGuard.value(BIGINT, A, Purpose.GROUPING_KEY))
                .isEqualTo("(" + A + " == 0 ? 0 : " + A + ")");
    }

    @Test
    void booleanAndVarcharValuesAreNeverRewritten() {
        for (Purpose purpose : Purpose.values()) {
            assertThat(ColumnGuard.value(BOOLEAN, A, purpose)).isEqualTo(A);
            assertThat(ColumnGuard.value(VARCHAR, A, purpose)).isEqualTo(A);
        }
    }

    @Test
    void coerceWrapsValueInTheGuardTernary() {
        assertThat(ColumnGuard.coerce(VARCHAR, A, Purpose.GROUPING_KEY))
                .contains("((IS_STRING(" + A + ")) ? " + A + " : null)");
        assertThat(ColumnGuard.coerce(DOUBLE, A, Purpose.SUM_AVG))
                .contains("((IS_NUMBER(" + A + ")) ? (" + A + " + 0.0) : null)");
    }

    @Test
    void structuredAndDecimalTypesDecline() {
        assertThat(ColumnGuard.predicate(new ArrayType(BIGINT), A)).isEmpty();
        assertThat(ColumnGuard.predicate(RowType.rowType(RowType.field("f", BIGINT)), A)).isEmpty();
        assertThat(ColumnGuard.predicate(DecimalType.createDecimalType(38, 0), A)).isEmpty();
        assertThat(ColumnGuard.coerce(new ArrayType(BIGINT), A, Purpose.MIN_MAX))
                .isEqualTo(Optional.empty());
    }
}

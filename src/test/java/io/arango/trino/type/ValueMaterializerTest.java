package io.arango.trino.type;

import static io.arango.trino.ArangoConfig.TypeCoercion.LENIENT;
import static io.arango.trino.ArangoConfig.TypeCoercion.STRICT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValueMaterializerTest {

    private static final RowType ADDRESS =
            RowType.rowType(RowType.field("city", VARCHAR), RowType.field("zip", BIGINT));

    private static final DecimalType DEC38 = DecimalType.createDecimalType(38, 0);
    private static final DecimalType DEC12_2 = DecimalType.createDecimalType(12, 2);
    private static final DecimalType DEC20_4 = DecimalType.createDecimalType(20, 4);

    // Writes one value through ValueMaterializer and returns the built single-position block.
    static Block materialize(Type type, Object value, ArangoConfig.TypeCoercion coercion) {
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        new ValueMaterializer(coercion).writeValue(builder, type, value, "col");
        return builder.build();
    }

    @Test
    void scalarParityWithM1AppendValue() {
        assertThat(BOOLEAN.getBoolean(materialize(BOOLEAN, true, LENIENT), 0)).isTrue();
        assertThat(BIGINT.getLong(materialize(BIGINT, 42L, LENIENT), 0)).isEqualTo(42L);
        assertThat(BIGINT.getLong(materialize(BIGINT, 42.0, LENIENT), 0))
                .isEqualTo(42L); // fraction-free double accepted
        assertThat(BIGINT.getLong(materialize(BIGINT, BigInteger.valueOf(7), LENIENT), 0))
                .isEqualTo(7L);
        assertThat(DOUBLE.getDouble(materialize(DOUBLE, 2.5, LENIENT), 0)).isEqualTo(2.5);
        assertThat(DOUBLE.getDouble(materialize(DOUBLE, 3L, LENIENT), 0))
                .isEqualTo(3.0); // any Number under DOUBLE
        assertThat(VARCHAR.getSlice(materialize(VARCHAR, "hi", LENIENT), 0).toStringUtf8())
                .isEqualTo("hi");
    }

    @Test
    void nullAppendsNullInBothModes() {
        assertThat(materialize(BIGINT, null, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(BIGINT, null, STRICT).isNull(0)).isTrue();
    }

    @Test
    void scalarMismatchIsNullUnderLenient() {
        assertThat(materialize(BIGINT, 42.5, LENIENT).isNull(0)).isTrue(); // genuine fraction
        assertThat(materialize(VARCHAR, 42L, LENIENT).isNull(0)).isTrue(); // number under VARCHAR
        assertThat(materialize(BOOLEAN, "true", LENIENT).isNull(0))
                .isTrue(); // string under BOOLEAN
    }

    @Test
    void scalarMismatchRaisesUnderStrictWithM1MessageShape() {
        assertThatThrownBy(() -> materialize(VARCHAR, 42L, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e -> {
                            assertThat(e.getErrorCode().getName())
                                    .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR");
                            // top-level mismatch keeps today's message shape: no path suffix
                            assertThat(e.getMessage()).startsWith("Column 'col' expected");
                        });
    }

    @Test
    void arrayOfBigintMaterializes() {
        ArrayType type = new ArrayType(BIGINT);
        Block block = materialize(type, List.of(1L, 2L, 3L), LENIENT);
        Block elements = type.getObject(block, 0);
        assertThat(elements.getPositionCount()).isEqualTo(3);
        assertThat(BIGINT.getLong(elements, 0)).isEqualTo(1L);
        assertThat(BIGINT.getLong(elements, 2)).isEqualTo(3L);
    }

    @Test
    void emptyArrayMaterializesEmpty() {
        ArrayType type = new ArrayType(VARCHAR);
        assertThat(type.getObject(materialize(type, List.of(), LENIENT), 0).getPositionCount())
                .isZero();
        assertThat(materialize(type, List.of(), LENIENT).isNull(0)).isFalse();
    }

    @Test
    void nestedArrayMaterializesRecursively() {
        ArrayType inner = new ArrayType(BIGINT);
        ArrayType type = new ArrayType(inner);
        Block outer =
                type.getObject(
                        materialize(type, List.of(List.of(1L), List.of(2L, 3L)), LENIENT), 0);
        assertThat(outer.getPositionCount()).isEqualTo(2);
        Block second = inner.getObject(outer, 1);
        assertThat(BIGINT.getLong(second, 1)).isEqualTo(3L);
    }

    @Test
    void arrayLeafMismatchNullsOnlyThatElementUnderLenient() {
        ArrayType type = new ArrayType(BIGINT);
        Block elements = type.getObject(materialize(type, List.of(1L, "oops", 3L), LENIENT), 0);
        assertThat(BIGINT.getLong(elements, 0)).isEqualTo(1L);
        assertThat(elements.isNull(1)).isTrue(); // leaf-level null (spec §3, user choice A)
        assertThat(BIGINT.getLong(elements, 2)).isEqualTo(3L);
    }

    @Test
    void storedNullElementIsNullInBothModesNeverAMismatch() {
        ArrayType type = new ArrayType(BIGINT);
        List<Object> withNull = Arrays.asList(1L, null, 3L);
        assertThat(type.getObject(materialize(type, withNull, LENIENT), 0).isNull(1)).isTrue();
        assertThat(type.getObject(materialize(type, withNull, STRICT), 0).isNull(1))
                .isTrue(); // no raise
    }

    @Test
    void scalarUnderArrayColumnIsStructuralMismatch() {
        ArrayType type = new ArrayType(BIGINT);
        assertThat(materialize(type, "not-a-list", LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(type, "not-a-list", STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e -> assertThat(e.getMessage()).startsWith("Column 'col' expected"));
    }

    @Test
    void strictNestedMismatchNamesThePath() {
        ArrayType type = new ArrayType(BIGINT);
        assertThatThrownBy(() -> materialize(type, List.of(1L, "oops"), STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e -> {
                            assertThat(e.getErrorCode().getName())
                                    .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR");
                            assertThat(e.getMessage()).contains("value at col[1]");
                        });
    }

    @Test
    void rowMaterializesFieldsInRowTypeOrder() {
        SqlRow row =
                ADDRESS.getObject(
                        materialize(ADDRESS, Map.of("zip", 10115L, "city", "berlin"), LENIENT), 0);
        assertThat(VARCHAR.getSlice(row.getRawFieldBlock(0), row.getRawIndex()).toStringUtf8())
                .isEqualTo("berlin");
        assertThat(BIGINT.getLong(row.getRawFieldBlock(1), row.getRawIndex())).isEqualTo(10115L);
    }

    @Test
    void absentRowFieldIsNullInBothModesNeverAMismatch() {
        Map<String, Object> cityOnly = Map.of("city", "berlin"); // no "zip" key at all
        SqlRow lenient = ADDRESS.getObject(materialize(ADDRESS, cityOnly, LENIENT), 0);
        assertThat(lenient.getRawFieldBlock(1).isNull(lenient.getRawIndex())).isTrue();
        SqlRow strict = ADDRESS.getObject(materialize(ADDRESS, cityOnly, STRICT), 0); // no raise
        assertThat(strict.getRawFieldBlock(1).isNull(strict.getRawIndex())).isTrue();
    }

    @Test
    void extraDocumentKeysAreIgnored() {
        SqlRow row =
                ADDRESS.getObject(
                        materialize(
                                ADDRESS,
                                Map.of("city", "berlin", "zip", 10115L, "unsampled", true),
                                LENIENT),
                        0);
        assertThat(row.getFieldCount()).isEqualTo(2);
    }

    @Test
    void listUnderRowColumnIsStructuralMismatch() {
        assertThat(materialize(ADDRESS, List.of("berlin"), LENIENT).isNull(0)).isTrue();
    }

    @Test
    void rowInArrayInRowMaterializes() {
        RowType leaf = RowType.rowType(RowType.field("v", BIGINT));
        RowType root = RowType.rowType(RowType.field("items", new ArrayType(leaf)));
        Block block = materialize(root, Map.of("items", List.of(Map.of("v", 7L))), LENIENT);
        SqlRow rootRow = root.getObject(block, 0);
        Block items =
                new ArrayType(leaf).getObject(rootRow.getRawFieldBlock(0), rootRow.getRawIndex());
        SqlRow leafRow = leaf.getObject(items, 0);
        assertThat(BIGINT.getLong(leafRow.getRawFieldBlock(0), leafRow.getRawIndex()))
                .isEqualTo(7L);
    }

    @Test
    void strictMismatchInsideArrayOfRowsNamesTheFullPath() {
        RowType leaf = RowType.rowType(RowType.field("b", BIGINT));
        ArrayType type = new ArrayType(leaf);
        List<Object> value = List.of(Map.of("b", 1L), Map.of("b", "oops"), Map.of("b", 3L));
        assertThatThrownBy(() -> materialize(type, value, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getMessage())
                                        .contains("value at col[1].b")); // spec §7 shape
    }

    @Test
    void decimalAcceptsAnyIntegralNumber() {
        assertThat((Int128) DEC38.getObject(materialize(DEC38, 42L, LENIENT), 0))
                .isEqualTo(Int128.valueOf(42));
        // uint64 max -- the value class that creates DECIMAL(38,0) columns in the first place
        BigInteger uint64Max = new BigInteger("18446744073709551615");
        assertThat((Int128) DEC38.getObject(materialize(DEC38, uint64Max, LENIENT), 0))
                .isEqualTo(Int128.valueOf(uint64Max));
        assertThat((Int128) DEC38.getObject(materialize(DEC38, 42.0, LENIENT), 0))
                .isEqualTo(Int128.valueOf(42));
    }

    @Test
    void integralDoubleBeyondLongRangeReadsBack() {
        // Spec review B1 mode B: 1e19 > 2^63 must NOT be rejected by BIGINT's long-range bound.
        assertThat((Int128) DEC38.getObject(materialize(DEC38, 1e19, LENIENT), 0))
                .isEqualTo(Int128.valueOf(new BigInteger("10000000000000000000")));
    }

    @Test
    void integralDoubleReadsBackExactBinaryValueNotShortestRepr() {
        // 2^63 is exactly representable as a double. new BigDecimal(d) yields its exact integer
        // (9223372036854775808); BigDecimal.valueOf(d) would yield the shortest round-trip repr
        // (9223372036854776000). This asserts the exact-binary path -- the "read exactly what's
        // stored" invariant -- so a regression back to valueOf(d) is caught.
        double twoPow63 = 0x1p63; // 9223372036854775808.0, exactly representable
        assertThat((Int128) DEC38.getObject(materialize(DEC38, twoPow63, LENIENT), 0))
                .isEqualTo(Int128.valueOf(new BigInteger("9223372036854775808")));
    }

    @Test
    void integralDoubleBeyondPrecisionIsCleanMismatchNotCrash() {
        // Spec review B1 mode A: 1e39 overflows DECIMAL(38,0); must be NULL, not
        // ArithmeticException.
        assertThat(materialize(DEC38, 1e39, LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC38, 1e39, STRICT))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    void fractionalAndOversizedValuesAreMismatches() {
        assertThat(materialize(DEC38, 42.5, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(DEC38, BigInteger.TEN.pow(39), LENIENT).isNull(0))
                .isTrue(); // >38 digits
        // Strings are the real decimal path -- ArangoDB has no decimal type (M6-C spec §5.1) --
        // so a numeric string now converts exactly instead of being a mismatch.
        assertThat((Int128) DEC38.getObject(materialize(DEC38, "42", LENIENT), 0))
                .isEqualTo(Int128.valueOf(42));
    }

    @Test
    void nonFiniteDoublesUnderDecimalAreMismatchesNotExceptions() {
        // The finiteness guard in unscaledExact must reject these before `new BigDecimal(d)`,
        // which throws an unchecked NumberFormatException on Infinity/NaN -- not the
        // ArithmeticException the setScale try/catch guards against.
        assertThat(materialize(DEC38, Double.POSITIVE_INFINITY, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(DEC38, Double.NEGATIVE_INFINITY, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(DEC38, Double.NaN, LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC38, Double.POSITIVE_INFINITY, STRICT))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    void nestedDecimalLeavesMaterializeThroughTheSameDispatch() {
        ArrayType arrayOfDec = new ArrayType(DEC38);
        Block elements =
                arrayOfDec.getObject(
                        materialize(
                                arrayOfDec,
                                List.of(1L, new BigInteger("18446744073709551615")),
                                LENIENT),
                        0);
        assertThat((Int128) DEC38.getObject(elements, 1))
                .isEqualTo(Int128.valueOf(new BigInteger("18446744073709551615")));

        RowType rowWithDec = RowType.rowType(RowType.field("x", DEC38));
        SqlRow row = rowWithDec.getObject(materialize(rowWithDec, Map.of("x", 5L), LENIENT), 0);
        assertThat((Int128) DEC38.getObject(row.getRawFieldBlock(0), row.getRawIndex()))
                .isEqualTo(Int128.valueOf(5));
    }

    @Test
    void shortDecimalStringConvertsExactly() {
        // DEC12_2 is a SHORT decimal (precision 12 <= 18) -- the new writeLong path.
        assertThat(DEC12_2.getLong(materialize(DEC12_2, "12.34", LENIENT), 0)).isEqualTo(1234L);
        assertThat(DEC12_2.getLong(materialize(DEC12_2, "1E+2", LENIENT), 0))
                .isEqualTo(10000L); // scientific notation accepted
        assertThat(DEC12_2.getLong(materialize(DEC12_2, "+12.34", LENIENT), 0))
                .isEqualTo(1234L); // leading + accepted
    }

    @Test
    void shortDecimalWhitespaceAndNonNumericStringsAreMismatches() {
        assertThat(materialize(DEC12_2, " 12.34", LENIENT).isNull(0))
                .isTrue(); // whitespace rejected by new BigDecimal
        assertThatThrownBy(() -> materialize(DEC12_2, " 12.34", STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));

        assertThat(materialize(DEC12_2, "abc", LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC12_2, "abc", STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    @Test
    void shortDecimalDoubleExactBinaryFitConverts() {
        assertThat(DEC12_2.getLong(materialize(DEC12_2, 0.25, LENIENT), 0))
                .isEqualTo(25L); // exact binary fit at s=2
    }

    @Test
    void shortDecimalIntegralDoubleConvertsAtNonZeroScale() {
        // Spec §5.1: an integral double matches at any scale s, not just s=0 -- 42.0 has no
        // fractional binary content to fail the exact-fit gate.
        assertThat(DEC12_2.getLong(materialize(DEC12_2, 42.0, LENIENT), 0)).isEqualTo(4200L);
    }

    @Test
    void hugeExponentDecimalStringIsCheapMismatchNotSlowRescale() {
        // Review finding: new BigDecimal(s) accepts an arbitrary exponent; without the magnitude
        // pre-gate in unscaledExact, setScale would materialize a ~hundreds-of-MB BigInteger
        // before Decimals.overflows could reject it ("1E+2000000".setScale(2) measured ~148ms /
        // 6.6Mbit; "1E+900000000" would be far larger). The pre-gate must make this a cheap
        // mismatch -- a plain isNull assertion would pass slowly even without the fix, so the
        // timeout is what actually pins the fast path.
        assertTimeoutPreemptively(
                Duration.ofSeconds(1),
                () -> assertThat(materialize(DEC12_2, "1E+900000000", LENIENT).isNull(0)).isTrue());
    }

    @Test
    void shortDecimalDoubleInexactBinaryFitIsMismatch() {
        // 12.34's exact binary value is 12.339999999999999857891452847979962825775146484375,
        // which does not fit scale 2 exactly -- decimal STRINGS are the intended encoding.
        assertThat(materialize(DEC12_2, 12.34, LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC12_2, 12.34, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    @Test
    void shortDecimalLongConvertsWithZeroFraction() {
        assertThat(DEC12_2.getLong(materialize(DEC12_2, 42L, LENIENT), 0))
                .isEqualTo(4200L); // 42.00
    }

    @Test
    void shortDecimalPrecisionOverflowIsMismatch() {
        // 12 digits + scale 2 = 14 unscaled digits > p=12.
        assertThat(materialize(DEC12_2, 123456789012L, LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC12_2, 123456789012L, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    @Test
    void booleanUnderDecimalColumnIsStructuralMismatch() {
        assertThat(materialize(DEC12_2, true, LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC12_2, true, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    @Test
    void longDecimalFractionalScaleStringConvertsExactly() {
        // DEC20_4 is a LONG decimal (precision 20 > 18) -- the pre-existing Int128 write path,
        // now reached with a non-zero scale.
        assertThat(
                        (Int128)
                                DEC20_4.getObject(
                                        materialize(DEC20_4, "12345678901234.5678", LENIENT), 0))
                .isEqualTo(Int128.valueOf(new BigInteger("123456789012345678")));
    }

    @Test
    void nullUnderShortDecimalIsNullInBothModes() {
        // Stored null is never a mismatch (spec §3) -- pins the same rule at the new short-decimal
        // leaf.
        assertThat(materialize(DEC12_2, null, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(DEC12_2, null, STRICT).isNull(0)).isTrue();
    }

    @Test
    void timestampMillisAcceptsIsoLocalDateTimeString() {
        long expectedMicros =
                LocalDateTime.of(2026, 8, 5, 12, 34, 56, 789_000_000).toEpochSecond(ZoneOffset.UTC)
                                * 1_000_000L
                        + 789_000L;
        assertThat(
                        TIMESTAMP_MILLIS.getLong(
                                materialize(TIMESTAMP_MILLIS, "2026-08-05T12:34:56.789", LENIENT),
                                0))
                .isEqualTo(expectedMicros);
    }

    @Test
    void timestampMillisAcceptsLowercaseTAndMissingSeconds() {
        // Parser surface, pinned (M6-C spec §5.1): ISO_LOCAL_DATE_TIME parses case-insensitively
        // and tolerates an omitted seconds field.
        long expectedMicros =
                LocalDateTime.of(2026, 8, 5, 12, 34).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        assertThat(
                        TIMESTAMP_MILLIS.getLong(
                                materialize(TIMESTAMP_MILLIS, "2026-08-05t12:34", LENIENT), 0))
                .isEqualTo(expectedMicros);
    }

    @Test
    void timestampMillisMismatchesUnderLenient() {
        assertThat(
                        materialize(TIMESTAMP_MILLIS, "2026-08-05 12:34:56", LENIENT)
                                .isNull(0)) // space separator
                .isTrue();
        assertThat(
                        materialize(TIMESTAMP_MILLIS, "2026-08-05T12:34:56.123456", LENIENT)
                                .isNull(0)) // finer than millis, never rounded
                .isTrue();
        assertThat(
                        materialize(TIMESTAMP_MILLIS, "2026-08-05T12:34:56+02:00", LENIENT)
                                .isNull(0)) // offset belongs to the tz type
                .isTrue();
        assertThat(
                        materialize(TIMESTAMP_MILLIS, "+999999999-12-31T23:59:59", LENIENT)
                                .isNull(0)) // epoch-micros long overflow, not an exception
                .isTrue();
    }

    @Test
    void timestampMillisMismatchesRaiseUnderStrict() {
        assertTimestampMillisMismatchRaises("2026-08-05 12:34:56"); // space separator
        assertTimestampMillisMismatchRaises(
                "2026-08-05T12:34:56.123456"); // finer than millis, never rounded
        assertTimestampMillisMismatchRaises(
                "2026-08-05T12:34:56+02:00"); // offset belongs to the tz type
        assertTimestampMillisMismatchRaises(
                "+999999999-12-31T23:59:59"); // epoch-micros long overflow, not an exception
    }

    private static void assertTimestampMillisMismatchRaises(Object value) {
        assertThatThrownBy(() -> materialize(TIMESTAMP_MILLIS, value, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    @Test
    void timestampMillisNeverMatchesANumber() {
        // Epoch-millis Number encoding is deliberately deferred (M6-C spec §5.1) -- numbers never
        // match the timestamp branch, string is the only accepted representation.
        assertThat(materialize(TIMESTAMP_MILLIS, 1722854096789L, LENIENT).isNull(0)).isTrue();
        assertTimestampMillisMismatchRaises(1722854096789L);
    }

    @Test
    void timestampWithTimeZoneAcceptsOffsetString() {
        String iso = "2026-08-05T12:34:56.789+05:30";
        long expectedMillis = OffsetDateTime.parse(iso).toInstant().toEpochMilli();
        Block block = materialize(TIMESTAMP_TZ_MILLIS, iso, LENIENT);
        long packed = TIMESTAMP_TZ_MILLIS.getLong(block, 0);
        assertThat(DateTimeEncoding.unpackMillisUtc(packed)).isEqualTo(expectedMillis);
        assertThat(DateTimeEncoding.unpackZoneKey(packed))
                .isEqualTo(TimeZoneKey.getTimeZoneKeyForOffset(330));
    }

    @Test
    void timestampWithTimeZoneAcceptsUtcZ() {
        String iso = "2026-08-05T12:34:56.789Z";
        long expectedMillis = OffsetDateTime.parse(iso).toInstant().toEpochMilli();
        Block block = materialize(TIMESTAMP_TZ_MILLIS, iso, LENIENT);
        long packed = TIMESTAMP_TZ_MILLIS.getLong(block, 0);
        assertThat(DateTimeEncoding.unpackMillisUtc(packed)).isEqualTo(expectedMillis);
        assertThat(DateTimeEncoding.unpackZoneKey(packed)).isEqualTo(TimeZoneKey.UTC_KEY);
    }

    @Test
    void timestampWithTimeZoneMismatchesUnderLenient() {
        assertThat(
                        materialize(TIMESTAMP_TZ_MILLIS, "2026-08-05T12:34:56.789", LENIENT)
                                .isNull(0)) // local string under tz type
                .isTrue();
        assertThat(
                        materialize(TIMESTAMP_TZ_MILLIS, "2026-08-05T12:34:56+05:30:15", LENIENT)
                                .isNull(0)) // sub-minute offset: TimeZoneKey would throw
                .isTrue();
        assertThat(
                        materialize(TIMESTAMP_TZ_MILLIS, "2026-08-05T12:34:56+16:00", LENIENT)
                                .isNull(0)) // parser allows +-18:00; Trino only +-14:00
                .isTrue();
        assertThat(
                        materialize(TIMESTAMP_TZ_MILLIS, "2026-08-05T12:34:56.1234+01:00", LENIENT)
                                .isNull(0)) // finer than millis
                .isTrue();
    }

    @Test
    void timestampWithTimeZoneMismatchesRaiseUnderStrict() {
        assertTimestampTzMismatchRaises("2026-08-05T12:34:56.789"); // local string under tz type
        assertTimestampTzMismatchRaises(
                "2026-08-05T12:34:56+05:30:15"); // sub-minute offset: TimeZoneKey would throw
        assertTimestampTzMismatchRaises(
                "2026-08-05T12:34:56+16:00"); // parser allows +-18:00; Trino only +-14:00
        assertTimestampTzMismatchRaises("2026-08-05T12:34:56.1234+01:00"); // finer than millis
    }

    private static void assertTimestampTzMismatchRaises(String value) {
        assertThatThrownBy(() -> materialize(TIMESTAMP_TZ_MILLIS, value, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode().getName())
                                        .isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    @Test
    void timestampWithTimeZonePackedMillisOverflowIsMismatchNotException() {
        // 52-bit packed-millis overflow via pack's IllegalArgumentException -- toEpochMilli itself
        // succeeds here (year 300000 fits in a signed long epoch-milli).
        assertThat(materialize(TIMESTAMP_TZ_MILLIS, "+300000-01-01T00:00:00Z", LENIENT).isNull(0))
                .isTrue();
        assertTimestampTzMismatchRaises("+300000-01-01T00:00:00Z");
    }

    @Test
    void timestampWithTimeZoneEpochMillisOverflowIsMismatchNotException() {
        // Epoch-millis ~3.15e19 > Long.MAX_VALUE: toEpochMilli's ArithmeticException arm,
        // exercised.
        assertThat(
                        materialize(TIMESTAMP_TZ_MILLIS, "+999999999-12-31T23:59:59Z", LENIENT)
                                .isNull(0))
                .isTrue();
        assertTimestampTzMismatchRaises("+999999999-12-31T23:59:59Z");
    }

    @Test
    void nullUnderBothTimestampTypesIsNullInBothModes() {
        // Stored null is never a mismatch (spec §3) -- pins the same rule at the new timestamp
        // leaves.
        assertThat(materialize(TIMESTAMP_MILLIS, null, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(TIMESTAMP_MILLIS, null, STRICT).isNull(0)).isTrue();
        assertThat(materialize(TIMESTAMP_TZ_MILLIS, null, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(TIMESTAMP_TZ_MILLIS, null, STRICT).isNull(0)).isTrue();
    }

    @Test
    void arrayOfTimestampMillisMismatchNullsOnlyThatElementUnderLenient() {
        ArrayType type = new ArrayType(TIMESTAMP_MILLIS);
        Block elements =
                type.getObject(
                        materialize(type, List.of("2026-08-05T12:00:00", "bad"), LENIENT), 0);
        assertThat(elements.isNull(0)).isFalse();
        assertThat(elements.isNull(1)).isTrue();
    }

    @Test
    void arrayOfTimestampMillisMismatchNamesThePathUnderStrict() {
        ArrayType type = new ArrayType(TIMESTAMP_MILLIS);
        List<Object> value = List.of("2026-08-05T12:00:00", "bad");
        assertThatThrownBy(() -> materialize(type, value, STRICT))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e -> assertThat(e.getMessage()).contains("value at col[1]"));
    }

    @Test
    void rowWithDecimalFieldMaterializesThroughTheNewLeafRecursion() {
        RowType amountRow = RowType.rowType(RowType.field("amount", DEC12_2));
        SqlRow row =
                amountRow.getObject(materialize(amountRow, Map.of("amount", "12.34"), LENIENT), 0);
        assertThat(DEC12_2.getLong(row.getRawFieldBlock(0), row.getRawIndex())).isEqualTo(1234L);
    }
}

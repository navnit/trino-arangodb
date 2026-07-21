package io.arango.trino.type;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.arango.trino.ArangoConfig.TypeCoercion.LENIENT;
import static io.arango.trino.ArangoConfig.TypeCoercion.STRICT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueMaterializerTest {

    private static final RowType ADDRESS = RowType.rowType(
            RowType.field("city", VARCHAR), RowType.field("zip", BIGINT));

    private static final DecimalType DEC38 = DecimalType.createDecimalType(38, 0);

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
        assertThat(BIGINT.getLong(materialize(BIGINT, 42.0, LENIENT), 0)).isEqualTo(42L); // fraction-free double accepted
        assertThat(BIGINT.getLong(materialize(BIGINT, BigInteger.valueOf(7), LENIENT), 0)).isEqualTo(7L);
        assertThat(DOUBLE.getDouble(materialize(DOUBLE, 2.5, LENIENT), 0)).isEqualTo(2.5);
        assertThat(DOUBLE.getDouble(materialize(DOUBLE, 3L, LENIENT), 0)).isEqualTo(3.0); // any Number under DOUBLE
        assertThat(VARCHAR.getSlice(materialize(VARCHAR, "hi", LENIENT), 0).toStringUtf8()).isEqualTo("hi");
    }

    @Test
    void nullAppendsNullInBothModes() {
        assertThat(materialize(BIGINT, null, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(BIGINT, null, STRICT).isNull(0)).isTrue();
    }

    @Test
    void scalarMismatchIsNullUnderLenient() {
        assertThat(materialize(BIGINT, 42.5, LENIENT).isNull(0)).isTrue();      // genuine fraction
        assertThat(materialize(VARCHAR, 42L, LENIENT).isNull(0)).isTrue();      // number under VARCHAR
        assertThat(materialize(BOOLEAN, "true", LENIENT).isNull(0)).isTrue();   // string under BOOLEAN
    }

    @Test
    void scalarMismatchRaisesUnderStrictWithM1MessageShape() {
        assertThatThrownBy(() -> materialize(VARCHAR, 42L, STRICT))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode().getName()).isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR");
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
        assertThat(type.getObject(materialize(type, List.of(), LENIENT), 0).getPositionCount()).isZero();
        assertThat(materialize(type, List.of(), LENIENT).isNull(0)).isFalse();
    }

    @Test
    void nestedArrayMaterializesRecursively() {
        ArrayType inner = new ArrayType(BIGINT);
        ArrayType type = new ArrayType(inner);
        Block outer = type.getObject(materialize(type, List.of(List.of(1L), List.of(2L, 3L)), LENIENT), 0);
        assertThat(outer.getPositionCount()).isEqualTo(2);
        Block second = inner.getObject(outer, 1);
        assertThat(BIGINT.getLong(second, 1)).isEqualTo(3L);
    }

    @Test
    void arrayLeafMismatchNullsOnlyThatElementUnderLenient() {
        ArrayType type = new ArrayType(BIGINT);
        Block elements = type.getObject(materialize(type, List.of(1L, "oops", 3L), LENIENT), 0);
        assertThat(BIGINT.getLong(elements, 0)).isEqualTo(1L);
        assertThat(elements.isNull(1)).isTrue();   // leaf-level null (spec §3, user choice A)
        assertThat(BIGINT.getLong(elements, 2)).isEqualTo(3L);
    }

    @Test
    void storedNullElementIsNullInBothModesNeverAMismatch() {
        ArrayType type = new ArrayType(BIGINT);
        List<Object> withNull = Arrays.asList(1L, null, 3L);
        assertThat(type.getObject(materialize(type, withNull, LENIENT), 0).isNull(1)).isTrue();
        assertThat(type.getObject(materialize(type, withNull, STRICT), 0).isNull(1)).isTrue(); // no raise
    }

    @Test
    void scalarUnderArrayColumnIsStructuralMismatch() {
        ArrayType type = new ArrayType(BIGINT);
        assertThat(materialize(type, "not-a-list", LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(type, "not-a-list", STRICT))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getMessage()).startsWith("Column 'col' expected"));
    }

    @Test
    void strictNestedMismatchNamesThePath() {
        ArrayType type = new ArrayType(BIGINT);
        assertThatThrownBy(() -> materialize(type, List.of(1L, "oops"), STRICT))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode().getName()).isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR");
                    assertThat(e.getMessage()).contains("value at col[1]");
                });
    }

    @Test
    void rowMaterializesFieldsInRowTypeOrder() {
        SqlRow row = ADDRESS.getObject(materialize(ADDRESS, Map.of("zip", 10115L, "city", "berlin"), LENIENT), 0);
        assertThat(VARCHAR.getSlice(row.getRawFieldBlock(0), row.getRawIndex()).toStringUtf8()).isEqualTo("berlin");
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
        SqlRow row = ADDRESS.getObject(materialize(ADDRESS,
                Map.of("city", "berlin", "zip", 10115L, "unsampled", true), LENIENT), 0);
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
        Block items = new ArrayType(leaf).getObject(rootRow.getRawFieldBlock(0), rootRow.getRawIndex());
        SqlRow leafRow = leaf.getObject(items, 0);
        assertThat(BIGINT.getLong(leafRow.getRawFieldBlock(0), leafRow.getRawIndex())).isEqualTo(7L);
    }

    @Test
    void strictMismatchInsideArrayOfRowsNamesTheFullPath() {
        RowType leaf = RowType.rowType(RowType.field("b", BIGINT));
        ArrayType type = new ArrayType(leaf);
        List<Object> value = List.of(Map.of("b", 1L), Map.of("b", "oops"), Map.of("b", 3L));
        assertThatThrownBy(() -> materialize(type, value, STRICT))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getMessage()).contains("value at col[1].b")); // spec §7 shape
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
        // Spec review B1 mode A: 1e39 overflows DECIMAL(38,0); must be NULL, not ArithmeticException.
        assertThat(materialize(DEC38, 1e39, LENIENT).isNull(0)).isTrue();
        assertThatThrownBy(() -> materialize(DEC38, 1e39, STRICT)).isInstanceOf(TrinoException.class);
    }

    @Test
    void fractionalAndOversizedValuesAreMismatches() {
        assertThat(materialize(DEC38, 42.5, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(DEC38, BigInteger.TEN.pow(39), LENIENT).isNull(0)).isTrue(); // >38 digits
        assertThat(materialize(DEC38, "42", LENIENT).isNull(0)).isTrue();                   // non-number
    }

    @Test
    void nestedDecimalLeavesMaterializeThroughTheSameDispatch() {
        ArrayType arrayOfDec = new ArrayType(DEC38);
        Block elements = arrayOfDec.getObject(
                materialize(arrayOfDec, List.of(1L, new BigInteger("18446744073709551615")), LENIENT), 0);
        assertThat((Int128) DEC38.getObject(elements, 1))
                .isEqualTo(Int128.valueOf(new BigInteger("18446744073709551615")));

        RowType rowWithDec = RowType.rowType(RowType.field("x", DEC38));
        SqlRow row = rowWithDec.getObject(materialize(rowWithDec, Map.of("x", 5L), LENIENT), 0);
        assertThat((Int128) DEC38.getObject(row.getRawFieldBlock(0), row.getRawIndex()))
                .isEqualTo(Int128.valueOf(5));
    }
}

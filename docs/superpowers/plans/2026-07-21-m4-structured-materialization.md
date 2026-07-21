# M4 — Structured-type value materialization: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ARRAY`/`ROW`/`DECIMAL(38,0)` column values readable (recursive, leaf-level-exact materialization) and delete the `NOT_SUPPORTED` guard, per the approved spec `docs/superpowers/specs/2026-07-21-m4-structured-materialization-design.md`.

**Architecture:** All value coercion moves out of `ArangoPageSource.appendValue` into a new single-purpose `io.arango.trino.type.ValueMaterializer` (TypeMapper's read-side dual). Scalar branches move verbatim; new recursive branches handle ARRAY (`ArrayBlockBuilder.buildEntry`), ROW (`RowBlockBuilder.buildEntry`, fields in RowType order, absent key → NULL), and DECIMAL(38,0) (integral numbers → `Int128`, gated by `Decimals.overflows(BigInteger, 38)`). Mismatches are handled at the exact depth they occur: lenient → `appendNull` for that leaf only; strict → `ARANGODB_TYPE_CONVERSION_ERROR` with a path (`a[2].b`). `checkMaterializable` is deleted.

**Tech Stack:** Java 24, `trino-spi` 476 (provided), `arangodb-java-driver` 7.13.0, JUnit 5 + AssertJ, Testcontainers (real ArangoDB — Docker must be running), Trino `DistributedQueryRunner` for e2e SQL.

## Global Constraints

- Maven needs `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` is not found. Build JDK is Java 24.
- Docker must be running locally: most tests boot a real ArangoDB container. No mocking framework anywhere — real containers or hand-written test doubles only.
- Do not touch `pom.xml` dependency pins (each has a comment explaining a real mediation bug).
- All SPI signatures used below were verified via javap against `trino-spi-476.jar` (spec Appendix A): `ArrayBlockBuilder.buildEntry(ArrayValueBuilder<E>)` with callback `build(BlockBuilder)`; `RowBlockBuilder.buildEntry(RowValueBuilder<E>)` with callback `build(List<BlockBuilder>)`; `Int128.valueOf(BigInteger)`; `Decimals.overflows(BigInteger, int)`; long-decimal write is `Type.writeObject(BlockBuilder, Object)` (no Int128-typed overload exists); `ArrayType.getObject(Block,int)` → `Block`; `RowType.getObject(Block,int)` → `SqlRow` (`getRawFieldBlock(i)`, `getRawIndex()`). Unit tests must read via `getObject`, **not** `getObjectValue` (which requires a `ConnectorSession`).
- Spec review finding **B1** is binding: the DECIMAL double branch must NOT reuse `isIntegralInLongRange`'s ±2⁶³ bound (would wrongly null `1e19`), and must gate on `Decimals.overflows` *before* `Int128.valueOf` (which throws past 128 bits — `1e39` must be a clean mismatch, not an `ArithmeticException`).
- Commit after every task; message style `feat:`/`test:`/`docs:` matching `git log`; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: `ValueMaterializer` — scalar branches (moved verbatim)

**Files:**
- Create: `src/main/java/io/arango/trino/type/ValueMaterializer.java`
- Test: `src/test/java/io/arango/trino/type/ValueMaterializerTest.java`

**Interfaces:**
- Consumes: `ArangoConfig.TypeCoercion` (existing enum: `LENIENT`/`STRICT`), `ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR`.
- Produces: `public ValueMaterializer(ArangoConfig.TypeCoercion coercion)`; `public void writeValue(BlockBuilder out, Type type, Object value, String columnName)`. Tasks 2–5 build on exactly this surface; Tasks 3–5 add branches to the private recursive `write(...)` this task creates.

- [ ] **Step 1: Write failing tests** — scalar-parity coverage mirroring today's `ArangoPageSource.appendValue` behavior, driven through the new class directly (no cursor, no container):

```java
package io.arango.trino.type;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static io.arango.trino.ArangoConfig.TypeCoercion.LENIENT;
import static io.arango.trino.ArangoConfig.TypeCoercion.STRICT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueMaterializerTest {

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
}
```

- [ ] **Step 2: Run tests, verify they fail to compile**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: compilation error — `ValueMaterializer` does not exist.

- [ ] **Step 3: Implement** — scalar logic moved verbatim from `ArangoPageSource.appendValue`/`isIntegralInLongRange`/`truncateForError` (see `ArangoPageSource.java:70-125`; do NOT delete from `ArangoPageSource` yet — that is Task 2):

```java
package io.arango.trino.type;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayDeque;
import java.util.Deque;

import static io.airlift.slice.Slices.utf8Slice;
import static io.arango.trino.ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * TypeMapper's read-side dual: TypeMapper maps runtime values to Trino types at schema inference;
 * this class maps (inferred Trino type, runtime value) to block writes at read. Coercion is
 * type-exact and handled at the exact depth a mismatch occurs (M4 spec §3): LENIENT writes NULL for
 * the offending leaf only, STRICT raises naming the column and, for nested leaves, the path.
 * Not thread-safe: one instance per ArangoPageSource (single-threaded per split).
 */
public class ValueMaterializer {
    private final ArangoConfig.TypeCoercion coercion;
    // Segments of the in-flight recursion ("[2]", ".b"); rendered only when STRICT raises.
    private final Deque<String> path = new ArrayDeque<>();

    public ValueMaterializer(ArangoConfig.TypeCoercion coercion) {
        this.coercion = requireNonNull(coercion, "coercion is null");
    }

    public void writeValue(BlockBuilder out, Type type, Object value, String columnName) {
        path.clear(); // a prior strict throw can leave stale segments behind
        write(out, type, value, columnName);
    }

    private void write(BlockBuilder out, Type type, Object value, String columnName) {
        if (value == null) {
            out.appendNull();
            return;
        }
        if (type.equals(BooleanType.BOOLEAN) && value instanceof Boolean b) {
            BooleanType.BOOLEAN.writeBoolean(out, b);
            return;
        }
        if (type.equals(BigintType.BIGINT) && isIntegralInLongRange(value)) {
            BigintType.BIGINT.writeLong(out, ((Number) value).longValue());
            return;
        }
        if (type.equals(DoubleType.DOUBLE) && value instanceof Number n) {
            DoubleType.DOUBLE.writeDouble(out, n.doubleValue());
            return;
        }
        if (type instanceof VarcharType && value instanceof String s) {
            type.writeSlice(out, utf8Slice(s));
            return;
        }
        mismatch(out, type, value, columnName);
    }

    private void mismatch(BlockBuilder out, Type type, Object value, String columnName) {
        if (coercion == ArangoConfig.TypeCoercion.STRICT) {
            throw new TrinoException(ARANGODB_TYPE_CONVERSION_ERROR, path.isEmpty()
                    ? "Column '%s' expected %s but a document held %s of type %s"
                            .formatted(columnName, type, truncateForError(value), value.getClass().getSimpleName())
                    : "Column '%s': value at %s%s expected %s but a document held %s of type %s"
                            .formatted(columnName, columnName, String.join("", path), type,
                                    truncateForError(value), value.getClass().getSimpleName()));
        }
        out.appendNull();
    }

    // Cap an offending value's rendering so a multi-megabyte stored string doesn't land verbatim in the error.
    private static String truncateForError(Object value) {
        String s = String.valueOf(value);
        return s.length() <= 100 ? s : s.substring(0, 100) + "... (" + s.length() + " chars)";
    }

    // A BIGINT column accepts an integer-valued number within signed 64-bit range. 42.0 is accepted
    // (reads as 42); 42.5 is a mismatch -- truncating it would disagree with a pushed FILTER.
    private static boolean isIntegralInLongRange(Object value) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return true;
        }
        if (value instanceof Double d) {
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof Float f) {
            double d = f;
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof java.math.BigInteger bi) {
            return bi.bitLength() < 64;
        }
        return false;
    }
}
```

- [ ] **Step 4: Run tests, verify pass**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/type/ValueMaterializer.java src/test/java/io/arango/trino/type/ValueMaterializerTest.java
git commit -m "feat: ValueMaterializer with M1 scalar coercion moved verbatim

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `ArangoPageSource` delegates to `ValueMaterializer`

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoPageSource.java`

**Interfaces:**
- Consumes: Task 1's `ValueMaterializer(TypeCoercion)` / `writeValue(BlockBuilder, Type, Object, String)`.
- Produces: `ArangoPageSource` public constructor unchanged (`ArangoCursor<Map>`, `List<ArangoColumnHandle>`, `ArangoConfig.TypeCoercion`) — nothing downstream changes.

- [ ] **Step 1: Rewire.** In `ArangoPageSource.java`:
  - Replace the field `private final ArangoConfig.TypeCoercion coercion;` with `private final ValueMaterializer materializer;` and in the constructor replace `this.coercion = requireNonNull(coercion, "coercion is null");` with `this.materializer = new ValueMaterializer(requireNonNull(coercion, "coercion is null"));`. Add `import io.arango.trino.type.ValueMaterializer;`.
  - In `getNextSourcePage()`'s column loop replace `appendValue(out, col, types.get(i), row.get(col.name()));` with `materializer.writeValue(out, types.get(i), row.get(col.name()), col.name());`.
  - Delete `appendValue`, `truncateForError`, `isIntegralInLongRange`, and the block comment above `appendValue` (lines 65–125), including the now-dead line-91 comment about `checkMaterializable`. Remove imports that become unused (`TrinoException`, `utf8Slice`, `ARANGODB_TYPE_CONVERSION_ERROR`, and the `io.trino.spi.type.*` star import if nothing else in the file uses it — `Type` is still needed for the `types` field).

- [ ] **Step 2: Run the read-path suites (parity check — behavior must be byte-identical)**
  Run: `mvn test -Dtest='ValueMaterializerTest,ArangoPageSourceProviderTest,ArangoConnectorQueryTest'`
  Expected: PASS (Docker required; the three `createPageSourceRejects*` tests still pass — the guard is untouched until Task 6).

- [ ] **Step 3: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoPageSource.java
git commit -m "refactor: ArangoPageSource delegates all coercion to ValueMaterializer

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: ARRAY branch (recursive) + strict path segments

**Files:**
- Modify: `src/main/java/io/arango/trino/type/ValueMaterializer.java`
- Test: `src/test/java/io/arango/trino/type/ValueMaterializerTest.java`

**Interfaces:**
- Produces: the `ARRAY` dispatch branch inside the private `write(...)`; the `[i]` path-segment convention Task 4/5 tests rely on.

- [ ] **Step 1: Write failing tests** (add to `ValueMaterializerTest`; new imports: `io.trino.spi.type.ArrayType`, `java.util.Arrays`, `java.util.List`):

```java
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
```

- [ ] **Step 2: Run, verify the new tests fail**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: the 7 new tests FAIL (array values currently fall through to `mismatch` → null / top-level-shaped error), the 4 Task-1 tests still pass.

- [ ] **Step 3: Implement.** In `write(...)`, insert between the `VarcharType` branch and the `mismatch(...)` call (new imports: `io.trino.spi.block.ArrayBlockBuilder`, `io.trino.spi.type.ArrayType`, `java.util.List`):

```java
        if (type instanceof ArrayType arrayType && value instanceof List<?> list) {
            ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
                int i = 0;
                for (Object element : list) {
                    path.addLast("[" + i + "]");
                    write(elementBuilder, arrayType.getElementType(), element, columnName);
                    path.removeLast();
                    i++;
                }
            });
            return;
        }
```

- [ ] **Step 4: Run tests, verify pass**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/type/ValueMaterializer.java src/test/java/io/arango/trino/type/ValueMaterializerTest.java
git commit -m "feat: recursive ARRAY materialization with leaf-level mismatch semantics

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: ROW branch

**Files:**
- Modify: `src/main/java/io/arango/trino/type/ValueMaterializer.java`
- Test: `src/test/java/io/arango/trino/type/ValueMaterializerTest.java`

**Interfaces:**
- Consumes: Task 3's path-segment convention.
- Produces: the `ROW` dispatch branch; `.field` path segments.

- [ ] **Step 1: Write failing tests** (new imports: `io.trino.spi.block.SqlRow`, `io.trino.spi.type.RowType`, `java.util.HashMap`, `java.util.Map`):

```java
    private static final RowType ADDRESS = RowType.rowType(
            RowType.field("city", VARCHAR), RowType.field("zip", BIGINT));

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
```

- [ ] **Step 2: Run, verify the new tests fail**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: 6 new tests FAIL; earlier ones pass.

- [ ] **Step 3: Implement.** Insert after the ARRAY branch (new imports: `io.trino.spi.block.RowBlockBuilder`, `io.trino.spi.type.RowType`, `java.util.Map`):

```java
        if (type instanceof RowType rowType && value instanceof Map<?, ?> map) {
            ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
                List<RowType.Field> fields = rowType.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = fields.get(i);
                    // Inference always names fields (RowType.field(name, type)) -- same
                    // assumption TypeMapper.mergeRows makes. An absent key reads as null in
                    // both modes: the union schema makes absence routine (spec decision 4).
                    String name = field.getName().orElseThrow();
                    path.addLast("." + name);
                    write(fieldBuilders.get(i), field.getType(), map.get(name), columnName);
                    path.removeLast();
                }
            });
            return;
        }
```

- [ ] **Step 4: Run tests, verify pass**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/type/ValueMaterializer.java src/test/java/io/arango/trino/type/ValueMaterializerTest.java
git commit -m "feat: recursive ROW materialization; absent fields null in both modes

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: DECIMAL(38,0) branch

**Files:**
- Modify: `src/main/java/io/arango/trino/type/ValueMaterializer.java`
- Test: `src/test/java/io/arango/trino/type/ValueMaterializerTest.java`

**Interfaces:**
- Produces: the `DECIMAL` dispatch branch, reachable both top-level and nested (full-dispatch recursion, spec §5).

- [ ] **Step 1: Write failing tests** (new imports: `io.trino.spi.type.DecimalType`, `io.trino.spi.type.Int128`):

```java
    private static final DecimalType DEC38 = DecimalType.createDecimalType(38, 0);

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
```

- [ ] **Step 2: Run, verify the new tests fail**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: 5 new tests FAIL; earlier ones pass.

- [ ] **Step 3: Implement.** Insert after the ROW branch (new imports: `io.trino.spi.type.DecimalType`, `io.trino.spi.type.Decimals`, `io.trino.spi.type.Int128`, `java.math.BigDecimal`, `java.math.BigInteger`; drop the fully-qualified `java.math.BigInteger` in `isIntegralInLongRange` for the plain name):

```java
        if (type instanceof DecimalType decimalType) {
            // TypeMapper only ever emits DECIMAL(38,0) -- always a long decimal (38 > 18), scale 0,
            // so the unscaled value IS the value and the write path is Int128 via writeObject.
            BigInteger unscaled = integralValueOf(value);
            // Overflow gate BEFORE Int128.valueOf, which throws past 128 bits (spec review B1):
            // an integral double >= 1e39 must be a mismatch (lenient NULL), not an ArithmeticException.
            if (unscaled != null && !Decimals.overflows(unscaled, decimalType.getPrecision())) {
                decimalType.writeObject(out, Int128.valueOf(unscaled));
                return;
            }
        }
        mismatch(out, type, value, columnName);
```

  (the existing trailing `mismatch(out, type, value, columnName);` is replaced by this block — the DECIMAL branch falls through into it.) Add the helper:

```java
    // The integral value of a number, or null if it isn't one. Deliberately NOT bounded to signed
    // 64-bit like isIntegralInLongRange: DECIMAL(38,0) exists precisely to hold uint64-range values
    // (spec review B1 -- reusing the long-range bound would wrongly null e.g. 1e19). Magnitude is
    // bounded by the caller's Decimals.overflows gate instead.
    private static BigInteger integralValueOf(Object value) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        if (value instanceof BigInteger bi) {
            return bi;
        }
        if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (!Double.isFinite(d) || d != Math.rint(d)) {
                return null;
            }
            return BigDecimal.valueOf(d).toBigIntegerExact();
        }
        return null;
    }
```

- [ ] **Step 4: Run tests, verify pass**
  Run: `mvn test -Dtest=ValueMaterializerTest`
  Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/type/ValueMaterializer.java src/test/java/io/arango/trino/type/ValueMaterializerTest.java
git commit -m "feat: DECIMAL(38,0) materialization; overflow-gated, not long-range-bounded

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Delete `checkMaterializable`; update provider tests + `ArangoMetadata` comment

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoPageSourceProvider.java`
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java` (comment only, lines 299–306)
- Test: `src/test/java/io/arango/trino/ArangoPageSourceProviderTest.java`

**Interfaces:**
- Consumes: Tasks 1–5 (all types now materialize, so removing the guard is safe).

- [ ] **Step 1: Replace the three rejection tests with a positive materialization test.** In `ArangoPageSourceProviderTest`:
  - Delete `createPageSourceRejectsArrayColumnLoudly`, `createPageSourceRejectsRowColumnLoudly`, `createPageSourceRejectsDecimalColumnLoudly` and the long "Human-requested fix: …" comment above them (lines 171–217). Remove the now-unused imports (`NOT_SUPPORTED`, `ArrayType`/`RowType`/`DecimalType` if unused elsewhere in the file — `ArrayType` gets re-used by the new test below).
  - Update the class Javadoc sentence "…and {@link ArangoPageSourceProvider}'s up-front rejection of ARRAY/ROW/DECIMAL-typed columns (scoped to the columns Trino actually projects)." to: "…and (since M4) structured-type materialization through {@link io.arango.trino.type.ValueMaterializer}."
  - Update the comment above `createPageSourceAllowsQueryThatDoesNotProjectUnsupportedColumn` and rename it to `createPageSourceProjectsOnlyRequestedColumns` — the behavior it proves (the projection is scoped to `columns`) is still real; the "unsupported" framing is gone. Keep its body as-is.
  - Add the positive end-to-end provider test:

```java
    // Since M4: structured columns materialize instead of being rejected up front.
    @Test
    void arrayColumnMaterializesThroughProvider() throws Exception {
        client.createDocumentCollectionForTest("shop", "tagged");
        client.insertForTest("shop", "tagged", mapOf("_key", "t1", "tags", List.of("red", "blue")));

        ArangoTableHandle handle = new ArangoTableHandle("shop", "tagged", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle col = new ArangoColumnHandle("tags", new ArrayType(VARCHAR), false, List.of("tags"));
        ArangoPageSourceProvider provider = new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig());
        ConnectorPageSource source = provider.createPageSource(null, null, new ArangoSplit(List.of()), handle, List.of(col), null);
        try {
            List<String> read = null;
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page == null) {
                    continue;
                }
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    Block elements = ((ArrayType) col.type()).getObject(page.getBlock(0), pos);
                    read = List.of(
                            VARCHAR.getSlice(elements, 0).toStringUtf8(),
                            VARCHAR.getSlice(elements, 1).toStringUtf8());
                }
            }
            assertThat(read).isEqualTo(List.of("red", "blue"));
        } finally {
            source.close();
        }
    }
```

  (new import: `io.trino.spi.block.Block`; `io.trino.spi.type.ArrayType` is already imported.)

- [ ] **Step 2: Delete the guard.** In `ArangoPageSourceProvider.java` remove the line `cols.forEach(ArangoPageSourceProvider::checkMaterializable);`, the whole `checkMaterializable` method with its comment block (lines 51–65), and the now-unused imports (`TrinoException`, `ArrayType`, `DecimalType`, `RowType`, `NOT_SUPPORTED` static import).

- [ ] **Step 3: Update the stale comment in `ArangoMetadata.java`** (lines 299–306). Replace the sentence fragment `or bottoms out at a still-structured (ROW/ARRAY/DECIMAL) leaf --
    // those stay Trino-evaluated, consistent with checkMaterializable's existing ARRAY/ROW/
    // DECIMAL rejection.` with `or bottoms out at a still-structured (ROW/ARRAY/DECIMAL) leaf --
    // those stay Trino-evaluated: a structured leaf isn't a pushdown target, and since M4 Trino
    // evaluates such dereferences over the materialized parent column (correct, just unoptimized).`

- [ ] **Step 4: Run the affected suites**
  Run: `mvn test -Dtest='ArangoPageSourceProviderTest,ArangoMetadataTest,ArangoConnectorPushdownTest'`
  Expected: PASS (dereference-pushdown behavior unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoPageSourceProvider.java src/main/java/io/arango/trino/ArangoMetadata.java src/test/java/io/arango/trino/ArangoPageSourceProviderTest.java
git commit -m "feat: remove checkMaterializable -- structured columns now materialize

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: End-to-end SQL coverage (`ArangoConnectorQueryTest`)

**Files:**
- Test: `src/test/java/io/arango/trino/ArangoConnectorQueryTest.java`

**Interfaces:**
- Consumes: everything prior; exercises the full stack (inference → pushdown → AQL → materialization) through `DistributedQueryRunner`.

- [ ] **Step 1: Extend the fixture.** In `setup()`, inside the existing `try (ArangoClient seed = …)` block, add (new imports: `java.math.BigInteger`, `java.util.Arrays`, `java.util.List`):

```java
            seed.createDocumentCollectionForTest("shop", "profiles");
            seed.insertForTest("shop", "profiles", Map.of(
                    "who", "ada",
                    "tags", List.of("pioneer", "math"),
                    "address", Map.of("city", "london", "zip", 1815L),
                    "big", new BigInteger("18446744073709551615"))); // uint64 -> DECIMAL(38,0)
            seed.insertForTest("shop", "profiles", Map.of(
                    "who", "bob",
                    "tags", List.of("ops", 5L),  // 5L under merged VARCHAR element -> leaf NULL
                    "address", Map.of("city", "berlin"), // absent zip -> NULL field
                    "big", 7L));                 // plain long under the DECIMAL column reads back
```

  And after the existing `createCatalog("arango", …)` call, add a strict-coercion catalog over the same server:

```java
        queryRunner.createCatalog("arango_strict", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword(),
                "arangodb.type-coercion", "STRICT"));
```

- [ ] **Step 2: Add the tests** (new import: `java.math.BigDecimal`; `MaterializedResult` rows sort with `ORDER BY who`):

```java
    @Test
    void arrayColumnMaterializesWithLeafNulls() {
        MaterializedResult r = queryRunner.execute(
                "SELECT who, tags FROM arango.shop.profiles ORDER BY who");
        assertThat(r.getRowCount()).isEqualTo(2);
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo(List.of("pioneer", "math"));
        // element 5L under the merged VARCHAR element type is a leaf mismatch -> NULL, not row loss
        assertThat(r.getMaterializedRows().get(1).getField(1)).isEqualTo(Arrays.asList("ops", null));
    }

    @Test
    void rowColumnMaterializesAndDereferenceStillPushes() {
        // whole-row select works post-M4 ...
        MaterializedResult whole = queryRunner.execute(
                "SELECT address FROM arango.shop.profiles WHERE who = 'bob'");
        assertThat(whole.getRowCount()).isEqualTo(1);
        assertThat(whole.getMaterializedRows().get(0).getField(0)).isNotNull();
        // ... and the pre-existing scalar dereference projection is unregressed
        MaterializedResult city = queryRunner.execute(
                "SELECT address.city FROM arango.shop.profiles ORDER BY who");
        assertThat(city.getMaterializedRows().get(0).getField(0)).isEqualTo("london");
        assertThat(city.getMaterializedRows().get(1).getField(0)).isEqualTo("berlin");
    }

    @Test
    void decimalColumnMaterializesUint64AndPlainLongs() {
        MaterializedResult r = queryRunner.execute(
                "SELECT who, big FROM arango.shop.profiles ORDER BY who");
        assertThat(r.getMaterializedRows().get(0).getField(1))
                .isEqualTo(new BigDecimal("18446744073709551615"));
        assertThat(r.getMaterializedRows().get(1).getField(1)).isEqualTo(new BigDecimal("7"));
    }

    @Test
    void strictModeRaisesOnNestedMismatchThroughSql() {
        // bob's tags hold 5L under the VARCHAR element type -> nested mismatch under strict
        assertThatThrownBy(() -> queryRunner.execute(
                "SELECT tags FROM arango_strict.shop.profiles"))
                .hasMessageContaining("ARANGODB_TYPE_CONVERSION_ERROR")
                .hasMessageContaining("value at tags[");
    }
```

  (add `import static org.assertj.core.api.Assertions.assertThatThrownBy;`. Note the strict-failure message assertions go through Trino's query-failure wrapping — if the error-code name doesn't surface in the wrapped message, assert `.hasMessageContaining("value at tags[")` only; the error identity is already unit-covered in `ValueMaterializerTest`.)

- [ ] **Step 3: Run**
  Run: `mvn test -Dtest=ArangoConnectorQueryTest`
  Expected: all tests PASS, including the three pre-existing ones.

- [ ] **Step 4: Full unit suite**
  Run: `mvn test`
  Expected: everything green.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/io/arango/trino/ArangoConnectorQueryTest.java
git commit -m "test: end-to-end SQL coverage for ARRAY/ROW/DECIMAL materialization

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Docs — README, CLAUDE.md, master-spec milestone renumber

**Files:**
- Modify: `README.md` (status blurb lines 10–13; Limitations lines 180–182)
- Modify: `CLAUDE.md` (read-path items 6 and 7; "What this is" milestone sentence)
- Modify: `docs/superpowers/specs/2026-07-18-arangodb-trino-connector-design.md` (§10 table, lines 336–343)

- [ ] **Step 1: README.** In the status blockquote, change "Milestones **M1** … **M3** ("shard-parallel splits") are complete. Writes (`INSERT`/`DELETE`) and value materialization for `ARRAY`/`ROW`/`DECIMAL` are out of scope so far" to "Milestones **M1**–**M4** are complete (**M4**: `ARRAY`/`ROW`/`DECIMAL` value materialization). Writes (`INSERT`/`DELETE`) are out of scope so far". In Limitations, delete the whole "**`ARRAY` / `ROW` / `DECIMAL` values are not materializable yet**" bullet (lines 180–182). If any other README section (search for `NOT_SUPPORTED` and "later milestone") repeats the old limitation, update it to describe the M4 behavior: structured values materialize recursively; under `lenient` a type-mismatched leaf reads as `NULL` (only that element/field), under `strict` it raises with a path.

- [ ] **Step 2: CLAUDE.md.** Three edits:
  - "What this is": change "Currently at milestone M3 ("shard-parallel splits")" to "Currently at milestone M4 ("structured-type materialization")" and append "; recursive ARRAY/ROW/DECIMAL value materialization (M4)" to the feature list sentence.
  - Read-path item 6: remove everything from "Before doing so, it calls `checkMaterializable`…" through "…doesn't select that column is unaffected." and replace with: "Structured columns (`ARRAY`/`ROW`/`DECIMAL`) materialize like any other since M4 — there is no up-front rejection anymore."
  - Read-path item 7: remove the sentence "The structured-type branch in `appendValue` is dead in practice because `checkMaterializable` already rejects those columns upstream; it remains only as a defensive fallback." and rewrite the item to state: coercion lives in `io.arango.trino.type.ValueMaterializer` (TypeMapper's read-side dual; `ArangoPageSource` is pure cursor/paging); materialization is recursive and leaf-level exact — a nested mismatch nulls only the offending element/field under `lenient` and raises `ARANGODB_TYPE_CONVERSION_ERROR` with a path (`col[2].b`) under `strict`; an absent ROW field or stored `null` at any depth is `NULL` in both modes, never a mismatch; `DECIMAL(38,0)` accepts any integral number gated by `Decimals.overflows(…, 38)` (deliberately NOT the ±2⁶³ BIGINT bound — uint64-range values like `1e19` must read back). Also update the package-layout list: `io.arango.trino.type` now holds "`TypeMapper`, `ValueMaterializer`, and the relocated `UnknownType`".
- [ ] **Step 3: Master spec §10.** Replace the `**M4**`/`**M5**`/`**M6**` rows with (renumber, inserting materialization as M4 — spec header note):

```markdown
| **M4** | Structured-type materialization *(inserted 2026-07-21; later rows renumbered — docs dated earlier reference the old numbers)* | Recursive ARRAY/ROW/DECIMAL(38,0) value materialization (`ValueMaterializer`); leaf-level mismatch semantics; `checkMaterializable` removed | `SELECT` of any inferred column returns correct values; leaf semantics proven under both coercion modes |
| **M5** | Aggregation pushdown | `applyAggregation` → **single-split** COUNT/SUM/MIN/MAX/AVG + GROUP BY | Aggregates correct vs reference; aggregated handle = 1 split |
| **M6** | Schema sources + `query()` + stats | override reader (`trino_schema`); validation-rule hint+merge; `ArangoQueryFunction` (AST read-only check, k-row schema, disable flag); `getTableStatistics` | Precedence honored; graph reachable via `query()`; row-count stats surfaced |
| **M7** | Hardening | TLS/auth, secrets, case-insensitive matching, cursor/failover resilience, best-effort dynamic filtering, `BaseConnectorTest` conformance, docs | Conformance green; deployment guide published |
```

- [ ] **Step 4: Verify docs mention nothing stale**
  Run: `grep -rn "checkMaterializable\|not materializable\|NOT_SUPPORTED" README.md CLAUDE.md`
  Expected: no hits describing the removed guard (hits inside historical `docs/superpowers/` files are fine and expected — they are dated snapshots).

- [ ] **Step 5: Full suite one last time**
  Run: `mvn test`
  Expected: green.

- [ ] **Step 6: Commit**

```bash
git add README.md CLAUDE.md docs/superpowers/specs/2026-07-18-arangodb-trino-connector-design.md
git commit -m "docs: M4 structured materialization -- README/CLAUDE.md, milestone renumber

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.EnumBlock;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Regression test for Aone #79476445:
 * DictionaryEnumColumnReader must keep offsets[] consistent for null rows,
 * otherwise the following non-null row's getString() returns concatenated
 * garbage (begin = offsets[i-1], which is stale/zero).
 * <p>
 * Scenarios encoded into one ORC file:
 * S1 first row null
 * S2 two consecutive nulls then a non-null row (mirrors Aone case id=248338,
 * positions 451/452 null -> 453 non-null)
 * S3 isolated nulls inside non-null runs
 * S4 last row null
 * S5 the rest are non-null (regression guard)
 */
public class EnumColumnDictionaryNullTest extends ColumnarStartAtTestBase {

    private static final String[] DICT = {"a", "b", "c"};
    private static final int ROW_COUNT = 3000;
    private static final int BATCH = 1000;

    private final Map<String, Integer> enumValues = new HashMap<>();
    private String[] expected;

    public EnumColumnDictionaryNullTest() throws IOException {
        super("dictionary_enum_null_test.orc", 1, 0);
    }

    @Before
    public void prepare() throws IOException {
        prepareExpected();
        prepareWriting();
        prepareReading();
        for (int i = 0; i < DICT.length; i++) {
            enumValues.put(DICT[i], i);
        }
    }

    private void prepareExpected() {
        expected = new String[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            expected[i] = DICT[i % DICT.length];
        }
        // S1: first row null
        expected[0] = null;
        // S2: two consecutive nulls followed by non-null
        expected[10] = null;
        expected[11] = null;
        expected[12] = "c";
        // S3: isolated null inside non-null runs, sprinkled every ~131 rows
        for (int i = 131; i < ROW_COUNT - 1; i += 131) {
            expected[i] = null;
        }
        // S4: last row null
        expected[ROW_COUNT - 1] = null;
    }

    private void prepareWriting() throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createString());

        try (Writer writer = OrcFile.createWriter(filePath,
            OrcFile.writerOptions(configuration)
                .fileSystem(fileSystem)
                .overwrite(true)
                .rowIndexStride(DEFAULT_INDEX_STRIDE)
                .setSchema(schema))) {

            VectorizedRowBatch batch = schema.createRowBatch(BATCH);
            BytesColumnVector col = (BytesColumnVector) batch.cols[0];

            for (int rowIndex = 0; rowIndex < ROW_COUNT; rowIndex++) {
                int slot = batch.size;
                if (expected[rowIndex] == null) {
                    col.isNull[slot] = true;
                    batch.cols[0].noNulls = false;
                } else {
                    col.setVal(slot, expected[rowIndex].getBytes());
                }
                batch.size += 1;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                    col = (BytesColumnVector) batch.cols[0];
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
    }

    @Test
    public void testNullRowsDoNotCorruptFollowingRows() throws IOException {
        DictionaryEnumColumnReader reader = new DictionaryEnumColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, encodings[columnId], indexStride, false
        );
        reader.open(true, rowGroupIncluded);

        for (int start = 0; start < ROW_COUNT; start += BATCH) {
            reader.startAt(0, start);
            EnumBlock block = new EnumBlock(BATCH, enumValues);
            reader.next(block, BATCH);

            for (int i = 0; i < BATCH; i++) {
                int globalPos = start + i;
                String want = expected[globalPos];
                boolean isNull = block.isNull(i);
                if (want == null) {
                    Assert.assertTrue(
                        "position " + globalPos + " expected null but was " + safeGet(block, i),
                        isNull);
                } else {
                    Assert.assertFalse(
                        "position " + globalPos + " expected \"" + want + "\" but was null",
                        isNull);
                    String got = block.getString(i);
                    Assert.assertEquals(
                        "position " + globalPos + " mismatch (prev="
                            + (globalPos > 0 ? String.valueOf(expected[globalPos - 1]) : "-")
                            + ", offsets around="
                            + dumpOffsets(block, i) + ")",
                        want, got);
                }
            }
        }
    }

    private static String safeGet(EnumBlock block, int i) {
        if (block.isNull(i)) {
            return "<null>";
        }
        try {
            return "\"" + block.getString(i) + "\"";
        } catch (RuntimeException e) {
            return "<error:" + e.getMessage() + ">";
        }
    }

    private static String dumpOffsets(EnumBlock block, int i) {
        int[] offsets = block.getOffsets();
        int from = Math.max(0, i - 2);
        int to = Math.min(offsets.length - 1, i + 2);
        StringBuilder sb = new StringBuilder("[");
        for (int k = from; k <= to; k++) {
            if (k > from) {
                sb.append(',');
            }
            sb.append(k).append(':').append(offsets[k]);
        }
        sb.append(']');
        return sb.toString();
    }
}

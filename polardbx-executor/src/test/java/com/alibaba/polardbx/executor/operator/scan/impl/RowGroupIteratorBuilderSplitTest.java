package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link RowGroupIteratorBuilder#splitSubRowGroupIterators} and
 * {@link RowGroupIteratorBuilder#buildSubRowGroupIterators} (lines 159-241).
 * No mocking — uses a real subclass that overrides the protected doCreateRowGroupIterator
 * to produce lightweight but real RowGroupIteratorImpl instances.
 */
public class RowGroupIteratorBuilderSplitTest {

    private static final int STRIPE_ID = 0;
    private static final Path FILE_PATH = new Path("/test/file.orc");
    private static final TypeDescription FILE_SCHEMA = TypeDescription.createStruct()
        .addField("col0", TypeDescription.createLong());
    private static final Configuration CONFIGURATION = new Configuration();

    private ExecutionContext executionContext;
    private RuntimeMetrics metrics;

    @Before
    public void setUp() {
        executionContext = new ExecutionContext();
        metrics = RuntimeMetrics.create("test");
    }

    /**
     * Creates a testable builder that produces real RowGroupIteratorImpl objects
     * without requiring heavy ORC dependencies (FileSystem, StripeInformation, etc.).
     */
    private TestableRowGroupIteratorBuilder createBuilder(boolean[] rowGroupIncluded) {
        int effectiveCount = 0;
        for (boolean included : rowGroupIncluded) {
            if (included) {
                effectiveCount++;
            }
        }
        return new TestableRowGroupIteratorBuilder(
            metrics, STRIPE_ID, 0, effectiveCount, rowGroupIncluded, executionContext);
    }

    /**
     * Helper: creates a real RowGroupIteratorImpl with the given startRowGroupId.
     */
    private RowGroupIteratorImpl createIteratorWithStartRowGroupId(int startRowGroupId,
                                                                   boolean[] rowGroupIncluded) {
        return new TestableRowGroupIteratorBuilder(
            metrics, STRIPE_ID, startRowGroupId, 0, rowGroupIncluded, executionContext)
            .build();
    }

    // ==================== buildSubRowGroupIterators ====================

    @Test
    public void testBuildSubRowGroupIterators_allIncluded_singleGroup() {
        // 5 row groups, all included, maxRowGroupCountPerTask=5 => 1 sub-iterator
        boolean[] included = {true, true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 5);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(0, result.get(0).getStartRowGroupId());
        Assert.assertEquals(5, result.get(0).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_allIncluded_multipleGroups() {
        // 6 row groups, all included, maxRowGroupCountPerTask=2 => 3 sub-iterators
        boolean[] included = {true, true, true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 2);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(0, result.get(0).getStartRowGroupId());
        Assert.assertEquals(2, result.get(0).getEffectiveGroupCount());
        Assert.assertEquals(2, result.get(1).getStartRowGroupId());
        Assert.assertEquals(2, result.get(1).getEffectiveGroupCount());
        Assert.assertEquals(4, result.get(2).getStartRowGroupId());
        Assert.assertEquals(2, result.get(2).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_partialIncluded() {
        // 6 row groups: [true, false, true, false, true, false], maxRowGroupCountPerTask=2
        boolean[] included = {true, false, true, false, true, false};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 2);

        // First task: starts at index 0, picks groups 0 and 2 (skipping 1), inner loop exits at index 3
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(0, result.get(0).getStartRowGroupId());
        Assert.assertEquals(2, result.get(0).getEffectiveGroupCount());
        // Second task: starts at index 3, picks group 4 (skipping 3,5), scans indices 3..5
        Assert.assertEquals(3, result.get(1).getStartRowGroupId());
        Assert.assertEquals(1, result.get(1).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_startFromMiddle() {
        // 8 row groups, all included, lastFinishedRowGroupId=3, maxRowGroupCountPerTask=2
        boolean[] included = {true, true, true, true, true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            3, 2);

        // Should start from index 4, creating 2 sub-iterators: [4,5] and [6,7]
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(4, result.get(0).getStartRowGroupId());
        Assert.assertEquals(2, result.get(0).getEffectiveGroupCount());
        Assert.assertEquals(6, result.get(1).getStartRowGroupId());
        Assert.assertEquals(2, result.get(1).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_unevenSplit() {
        // 5 row groups, all included, maxRowGroupCountPerTask=3 => 2 sub-iterators (3 + 2)
        boolean[] included = {true, true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 3);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals(0, result.get(0).getStartRowGroupId());
        Assert.assertEquals(3, result.get(0).getEffectiveGroupCount());
        Assert.assertEquals(3, result.get(1).getStartRowGroupId());
        Assert.assertEquals(2, result.get(1).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_singleRowGroup() {
        boolean[] included = {true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 1);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(0, result.get(0).getStartRowGroupId());
        Assert.assertEquals(1, result.get(0).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_noneIncluded() {
        // All false => tasks still created but with effectiveGroupCount=0
        boolean[] included = {false, false, false, false};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 2);

        // The loop scans all indices but never finds included groups
        // It creates sub-iterators with groupCountInTask=0
        Assert.assertFalse(result.isEmpty());
        for (RowGroupIteratorImpl iter : result) {
            Assert.assertEquals(0, iter.getEffectiveGroupCount());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildSubRowGroupIterators_invalidLastFinished() {
        boolean[] included = {true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);
        // lastFinishedRowGroupId + 1 >= rowGroupIncluded.length => should fail
        builder.buildSubRowGroupIterators(2, 1);
    }

    @Test
    public void testBuildSubRowGroupIterators_maxCountOne() {
        // Each included row group gets its own sub-iterator
        boolean[] included = {true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 1);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(0, result.get(0).getStartRowGroupId());
        Assert.assertEquals(1, result.get(0).getEffectiveGroupCount());
        Assert.assertEquals(1, result.get(1).getStartRowGroupId());
        Assert.assertEquals(1, result.get(1).getEffectiveGroupCount());
        Assert.assertEquals(2, result.get(2).getStartRowGroupId());
        Assert.assertEquals(1, result.get(2).getEffectiveGroupCount());
    }

    @Test
    public void testBuildSubRowGroupIterators_bitmapCorrectness() {
        // Verify the bitmap passed to doCreateRowGroupIterator is correct
        boolean[] included = {true, false, true, true, false};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> result = builder.buildSubRowGroupIterators(
            -1, 2);

        // First sub-iterator: indices 0..3, includes groups 0 and 2
        boolean[] bitmap0 = result.get(0).rgIncluded();
        Assert.assertEquals(5, bitmap0.length);
        Assert.assertTrue(bitmap0[0]);
        Assert.assertFalse(bitmap0[1]);
        Assert.assertTrue(bitmap0[2]);
        Assert.assertFalse(bitmap0[3]);
        Assert.assertFalse(bitmap0[4]);

        // Second sub-iterator: indices 4..4, includes group 3 (from the scan continuing at index 3)
        boolean[] bitmap1 = result.get(1).rgIncluded();
        Assert.assertEquals(5, bitmap1.length);
        Assert.assertTrue(bitmap1[3]);
    }

    // ==================== splitSubRowGroupIterators ====================

    @Test
    public void testSplitSubRowGroupIterators_replaceExisting() {
        boolean[] included = {true, true, true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        // Pre-populate list with 2 dummy iterators
        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        subList.add(createIteratorWithStartRowGroupId(0, included));
        subList.add(createIteratorWithStartRowGroupId(3, included));

        // Split from startSubIteratorIndex=0, lastFinishedRowGroupId=-1, maxRowGroupCountPerTask=2
        builder.splitSubRowGroupIterators(subList, 0, -1, 2);

        // Should produce 3 sub-iterators: [0,1], [2,3], [4,5]
        Assert.assertEquals(3, subList.size());
        Assert.assertEquals(0, subList.get(0).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(0).getEffectiveGroupCount());
        Assert.assertEquals(2, subList.get(1).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(1).getEffectiveGroupCount());
        Assert.assertEquals(4, subList.get(2).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(2).getEffectiveGroupCount());
    }

    @Test
    public void testSplitSubRowGroupIterators_appendBeyondListSize() {
        boolean[] included = {true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        // Pre-populate list with 1 iterator
        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        subList.add(createIteratorWithStartRowGroupId(0, included));

        // Split from startSubIteratorIndex=0, lastFinishedRowGroupId=-1, maxRowGroupCountPerTask=1
        builder.splitSubRowGroupIterators(subList, 0, -1, 1);

        // Should produce 4 sub-iterators: first one replaced, 3 appended
        Assert.assertEquals(4, subList.size());
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(i, subList.get(i).getStartRowGroupId());
            Assert.assertEquals(1, subList.get(i).getEffectiveGroupCount());
        }
    }

    @Test
    public void testSplitSubRowGroupIterators_startFromMiddleIndex() {
        boolean[] included = {true, true, true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        // Pre-populate list with 3 iterators
        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        subList.add(createIteratorWithStartRowGroupId(0, included));
        subList.add(createIteratorWithStartRowGroupId(2, included));
        subList.add(createIteratorWithStartRowGroupId(4, included));

        // Split from startSubIteratorIndex=1, lastFinishedRowGroupId=1, maxRowGroupCountPerTask=2
        // subList.get(1).getStartRowGroupId() = 2, lastFinishedRowGroupId + 1 = 2 >= 2 ✓
        builder.splitSubRowGroupIterators(subList, 1, 1, 2);

        // First element unchanged, rest replaced/appended starting from index 1
        Assert.assertEquals(0, subList.get(0).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(1).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(1).getEffectiveGroupCount());
        Assert.assertEquals(4, subList.get(2).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(2).getEffectiveGroupCount());
    }

    @Test
    public void testSplitSubRowGroupIterators_withGaps() {
        boolean[] included = {true, false, false, true, false, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        subList.add(createIteratorWithStartRowGroupId(0, included));

        builder.splitSubRowGroupIterators(subList, 0, -1, 2);

        // First task: scans indices 0..3, picks groups 0 and 3 (2 included), inner loop exits at index 4
        Assert.assertEquals(0, subList.get(0).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(0).getEffectiveGroupCount());
        // Second task: starts at index 4, picks group 5 (skipping 4), scans indices 4..5
        Assert.assertEquals(4, subList.get(1).getStartRowGroupId());
        Assert.assertEquals(1, subList.get(1).getEffectiveGroupCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitSubRowGroupIterators_invalidStartIndex() {
        boolean[] included = {true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        // startSubIteratorIndex >= subList.size() => should fail
        builder.splitSubRowGroupIterators(subList, 0, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitSubRowGroupIterators_invalidLastFinished() {
        boolean[] included = {true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        subList.add(createIteratorWithStartRowGroupId(3, included));

        // lastFinishedRowGroupId + 1 = 1, subRowGroupIteratorStartRowGroupId = 3
        // 1 < 3 => should fail
        builder.splitSubRowGroupIterators(subList, 0, 0, 1);
    }

    @Test
    public void testSplitSubRowGroupIterators_lastFinishedEqualsStart() {
        boolean[] included = {true, true, true, true};
        TestableRowGroupIteratorBuilder builder = createBuilder(included);

        List<RowGroupIteratorImpl> subList = new ArrayList<>();
        subList.add(createIteratorWithStartRowGroupId(2, included));

        // lastFinishedRowGroupId + 1 = 2 >= subRowGroupIteratorStartRowGroupId = 2 ✓
        // Starts scanning from index 2
        builder.splitSubRowGroupIterators(subList, 0, 1, 2);

        Assert.assertEquals(2, subList.get(0).getStartRowGroupId());
        Assert.assertEquals(2, subList.get(0).getEffectiveGroupCount());
    }

    // ==================== Inner testable subclass ====================

    /**
     * A subclass of RowGroupIteratorBuilder that overrides doCreateRowGroupIterator
     * to produce real RowGroupIteratorImpl instances without requiring heavy ORC dependencies.
     * All null-safe fields are passed as null; only the fields actually used by the tested
     * methods (rowGroupIncluded, startRowGroupId, effectiveGroupCount) are set properly.
     */
    private static class TestableRowGroupIteratorBuilder extends RowGroupIteratorBuilder {

        private final ExecutionContext executionContext;

        TestableRowGroupIteratorBuilder(
            RuntimeMetrics metrics,
            int stripeId,
            int startRowGroupId,
            int effectiveGroupCount,
            boolean[] rowGroupIncluded,
            ExecutionContext executionContext) {
            super(
                metrics,
                stripeId, startRowGroupId, effectiveGroupCount, rowGroupIncluded,
                null, // primaryKeyColIds
                null, // ioExecutor
                null, // fileSystem
                CONFIGURATION,
                FILE_PATH,
                0, // compressionSize
                CompressionKind.NONE,
                null, // preheatFileMeta
                null, // stripeInformation
                0L, // startRowOfStripe
                FILE_SCHEMA,
                OrcFile.WriterVersion.ORC_14,
                null, // encryption
                null, // encodings
                false, // ignoreNonUtf8BloomFilter
                0L, // maxBufferSize
                0, // maxDiskRangeChunkLimit
                0L, // maxMergeDistance
                1000, // chunkLimit
                null, // blockCacheManager
                null, // ossColumnTransformer
                executionContext,
                new boolean[] {true}, // columnIncluded
                1000, // indexStride
                false, // enableDecimal64
                null, // memoryAllocatorCtx
                null  // operatorStatistics
            );
            this.executionContext = executionContext;
        }

        @Override
        protected RowGroupIteratorImpl doCreateRowGroupIterator(
            int currentStartRowGroupId, int currentEffectiveGroupCount, boolean[] currentRowGroupBitmap) {
            return new RowGroupIteratorImpl(
                metrics,
                stripeId,
                currentStartRowGroupId,
                currentEffectiveGroupCount,
                currentRowGroupBitmap,
                null, // primaryKeyColIds
                null, // ioExecutor
                null, // fileSystem
                CONFIGURATION,
                filePath,
                0,
                CompressionKind.NONE,
                null, // preheatFileMeta
                null, // stripeInformation
                0L,
                fileSchema,
                OrcFile.WriterVersion.ORC_14,
                null, // encryption
                encodings,
                false,
                0L,
                0,
                0L,
                1000,
                blockCacheManager,
                ossColumnTransformer,
                executionContext,
                columnIncluded,
                1000,
                false,
                null, // memoryAllocatorCtx
                null  // operatorStatistics
            );
        }
    }
}

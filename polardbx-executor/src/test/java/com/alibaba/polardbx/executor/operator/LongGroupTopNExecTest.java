package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Test class for GroupTopNExec with Long data types
 */
public class LongGroupTopNExecTest extends BaseExecTest {

    private static final String TABLE_NAME = "MOCK_GROUP_TOP_TABLE";
    private static final String COLUMN_PREFIX = "MOCK_GROUP_TOP_COLUMN_";
    // for Rex
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Before
    public void setUp() {
        // Setup mock Parameters to avoid NullPointerException in getFetchValue()
        Map<Integer, ParameterContext> parameterMap = new HashMap<>();
        Parameters parameters = new Parameters(parameterMap);
        context.setParams(parameters);
    }

    // Mock GroupTop class for testing
    private static class MockGroupTop extends GroupTopN {
        private final RelCollation innerCollation;
        private final RexNode fetch;
        private final ImmutableBitSet groupSet;
        private final RelNode input;

        public MockGroupTop(RelCollation innerCollation, RexNode fetch, ImmutableBitSet groupSet, RelNode input) {
            super(mock(org.apache.calcite.plan.RelOptCluster.class), mock(org.apache.calcite.plan.RelTraitSet.class),
                input,
                mock(RelCollation.class), null, fetch, groupSet, false);
            this.innerCollation = innerCollation;
            this.fetch = fetch;
            this.groupSet = groupSet;
            this.input = input;
        }

        @Override
        public RelCollation getInnerCollation() {
            return innerCollation;
        }

        @Override
        public RexNode getFetch() {
            return fetch;
        }

        @Override
        public ImmutableBitSet getGroupSet() {
            return groupSet;
        }

        @Override
        public RelNode getInput() {
            return input;
        }
    }

    private RelCollation createCollation(RelFieldCollation.Direction direction, int... fieldIndices) {
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        for (int fieldIndex : fieldIndices) {
            fieldCollations.add(new RelFieldCollation(fieldIndex, direction));
        }
        return RelCollationImpl.of(fieldCollations);
    }

    private RelCollation createCollation(int... fieldIndices) {
        return createCollation(RelFieldCollation.Direction.ASCENDING, fieldIndices);
    }

    private RexNode createFetch(long fetchValue) {
        return REX_BUILDER.makeBigIntLiteral(fetchValue);
    }

    @Test
    public void testSimple() {
        // Create input data with group column (Integer) and long value column
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                LongBlock.of(100L, 200L, 300L, 400L, 500L, 600L)))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 2, 2, 3, 3),
                LongBlock.of(150L, 250L, 450L, 550L, 700L, 800L)))
            .build();

        // Create GroupTop operator
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by long value column descending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column (index 0)

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 from each group
        // Group 1: values 100L,150L,200L,250L,300L -> top 2 are 300L,250L
        // Group 2: values 400L,450L,500L,550L,600L -> top 2 are 600L,550L
        // Group 3: values 700L,800L -> top 2 are 800L,700L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2, 3, 3),
            LongBlock.of(300L, 250L, 600L, 550L, 800L, 700L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testNoGroup() {
        // Create input data with no grouping
        MockExec inputExec = MockExec.builder(DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.of(100L, 200L, 300L, 400L, 500L)))
            .withChunk(new Chunk(
                LongBlock.of(150L, 250L, 350L, 450L, 550L)))
            .build();

        // Create GroupTop operator with no grouping
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 0); // Sort by value column descending
        RexNode fetch = createFetch(3); // Take top 3 overall
        ImmutableBitSet groupSet = ImmutableBitSet.of(); // No grouping

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=3L to match createFetch(3)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 3L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 3 overall values: 550L, 500L, 450L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            LongBlock.of(550L, 500L, 450L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testOrderByAsc() {
        // Create input data with group column and long value column
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                LongBlock.of(300L, 100L, 200L, 600L, 400L, 500L)))
            .build();

        // Create GroupTop operator with ascending order
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 1); // Sort by long value ascending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 smallest from each group
        // Group 1: values 300L,100L,200L -> top 2 smallest are 100L,200L
        // Group 2: values 600L,400L,500L -> top 2 smallest are 400L,500L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            LongBlock.of(100L, 200L, 400L, 500L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testOrderByDesc() {
        // Create input data with group column and long value column
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                LongBlock.of(300L, 100L, 200L, 600L, 400L, 500L)))
            .build();

        // Create GroupTop operator with descending order
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by long value descending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 largest from each group
        // Group 1: values 300L,100L,200L -> top 2 largest are 300L,200L
        // Group 2: values 600L,400L,500L -> top 2 largest are 600L,500L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            LongBlock.of(300L, 200L, 600L, 500L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testMultipleSortColumns() {
        // Create input data with group column and two long value columns
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                LongBlock.of(100L, 100L, 200L, 300L, 300L, 400L),
                LongBlock.of(1000L, 2000L, 1500L, 1000L, 2000L, 1500L)))
            .build();

        // Create GroupTop operator - sort by second and third columns in descending order
        RelCollation innerCollation = createCollation(RelFieldCollation.Direction.DESCENDING, 1,
            2); // Sort by second and third columns descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.LongType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=1L to match createFetch(1)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 1L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 1 from each group based on multi-column sort
        // Group 1: (1,100L,1000L), (1,100L,2000L), (1,200L,1500L) -> top 1 is (1,200L,1500L) because 200L > 100L
        // Group 2: (2,300L,1000L), (2,300L,2000L), (2,400L,1500L) -> top 1 is (2,400L,1500L) because 400L > 300L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2),
            LongBlock.of(200L, 400L),
            LongBlock.of(1500L, 1500L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testEmptyInput() {
        // Create empty input data
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .build();

        // Create GroupTop operator
        RelCollation innerCollation = createCollation(1);
        RexNode fetch = createFetch(2);
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should be empty
        List<Chunk> expectedChunks = Collections.emptyList();
        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testWithNullValuesDesc() {
        // Create input data with null values in long column
        Long[] longValues = new Long[] {100L, null, 300L, 400L, null, 600L};

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                LongBlock.of(longValues)))
            .build();

        // Create GroupTop operator - nulls first, descending order
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - nulls should come first, then descending order
        // Group 1: null, 100L, 300L -> top 2 are null, 300L
        // Group 2: null, 400L, 600L -> top 2 are null, 600L
        Long[] expectedLongValues = new Long[] {300L, 100L, 600L, 400L};

        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            LongBlock.of(expectedLongValues)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testMultipleGroups() {
        // Create input data with multiple groups and long values
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4, 5),
                LongBlock.of(1000L, 2000L, 3000L, 4000L, 5000L)))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4, 5),
                LongBlock.of(1500L, 2500L, 3500L, 4500L, 5500L)))
            .build();

        // Create GroupTop operator
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 1); // Sort by long value ascending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=1L to match createFetch(1)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 1L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get minimum value from each group (ascending order)
        // Group 1: 1000L, 1500L -> min is 1000L
        // Group 2: 2000L, 2500L -> min is 2000L
        // Group 3: 3000L, 3500L -> min is 3000L
        // Group 4: 4000L, 4500L -> min is 4000L
        // Group 5: 5000L, 5500L -> min is 5000L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2, 3, 4, 5),
            LongBlock.of(1000L, 2000L, 3000L, 4000L, 5000L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testRehashLogic() {
        // Test rehash logic by using small estimateHashTableSize and creating enough groups to trigger rehash
        // With DEFAULT_LOAD_FACTOR = 0.75, estimateHashTableSize = 4 should result in:
        // n = HashCommon.arraySize(4, 0.75) ≈ 8, maxFill = HashCommon.maxFill(8, 0.75) ≈ 6
        // So creating 8+ groups should trigger rehash
        final int smallHashTableSize = 4;
        final int groupCount = 10; // This should trigger rehash since it exceeds maxFill
        final int itemsPerGroup = 2;

        // Create test data with multiple groups using Integer and Long columns
        List<Integer> groupKeys = new ArrayList<>();
        List<Long> longValues = new ArrayList<>();

        for (int group = 1; group <= groupCount; group++) {
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(group);
                longValues.add((long) (group * 1000 + item)); // Long values
            }
        }

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
                LongBlock.wrap(longValues.stream().mapToLong(i -> i).toArray())))
            .build();

        // Create GroupTop operator with small hash table size to force rehash
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by Long column descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec with small estimateHashTableSize to trigger rehash
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=1L to match createFetch(1)
        GroupTopNExec exec =
            new GroupTopNExec(inputDataTypes, groupTopN, smallHashTableSize, spillerFactory, 1L, context);

        // Run test - this should trigger rehash when enough groups are created
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 1 from each of the 10 groups = 10 total results
        List<Chunk> results = test.result();
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(groupCount, result.getPositionCount()); // 10 groups * 1 item = 10

        // Verify that we have results from all groups (proving rehash worked correctly)
        boolean[] groupFound = new boolean[groupCount + 1]; // 1-indexed
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            groupFound[groupId] = true;
        }

        for (int group = 1; group <= groupCount; group++) {
            Assert.assertTrue("Group " + group + " should be present in results after rehash", groupFound[group]);
        }

        // Verify the data correctness - each group should have its highest value
        // Group 1: (1001L), Group 2: (2001L), etc.
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            long expectedLongValue = groupId * 1000L + 1; // Highest value for each group

            Assert.assertEquals("Long value should be correct for group " + groupId,
                expectedLongValue, result.getBlock(1).getLong(i));
        }
    }

    @Test
    public void testHashCollisionAndLinearProbing() {
        // Test hash collision and linear probing logic with extremely small hash table to guarantee collisions
        // Using estimateHashTableSize = 2 will result in very small initial hash table (n ≈ 4, maxFill ≈ 3)
        // This guarantees hash collisions and tests the linear probing logic in findOrCreateGroupId and rehash
        final int verySmallHashTableSize = 2;
        final int groupCount = 15; // Many groups to ensure multiple collisions and multiple rehashes
        final int itemsPerGroup = 1;

        // Create test data with strategically chosen group keys to increase collision probability
        // Use powers of 2 and their neighbors which are more likely to collide in small hash tables
        // Also include some sequential numbers to test different hash patterns
        int[] strategicGroupKeys = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 3, 5, 9};

        List<Integer> groupKeys = new ArrayList<>();
        List<Long> longValues = new ArrayList<>();

        for (int i = 0; i < groupCount; i++) {
            int groupKey = strategicGroupKeys[i];
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(groupKey);
                longValues.add((long) (groupKey * 100 + item)); // Long values
            }
        }

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
                LongBlock.wrap(longValues.stream().mapToLong(i -> i).toArray())))
            .build();

        // Create GroupTop operator with very small hash table size to force collisions and multiple rehashes
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by Long column descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec with very small estimateHashTableSize to guarantee hash collisions and linear probing
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec =
            new GroupTopNExec(inputDataTypes, groupTopN, verySmallHashTableSize, spillerFactory, 1L, context);

        // Run test - this will definitely trigger hash collisions, linear probing, and multiple rehashes
        // The linear probing logic: while (k != NOT_EXISTS) { h = (h + 1) & mask; k = keys[h]; }
        // will be extensively exercised due to the small hash table size
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 1 from each of the 15 groups = 15 total results
        List<Chunk> results = test.result();
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(groupCount, result.getPositionCount()); // 15 groups * 1 item = 15

        // Verify that we have results from all groups (proving hash collision handling and linear probing worked correctly)
        boolean[] groupFound = new boolean[2049]; // Large enough to cover all strategic keys
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            groupFound[groupId] = true;
        }

        for (int i = 0; i < groupCount; i++) {
            int expectedGroupKey = strategicGroupKeys[i];
            Assert.assertTrue(
                "Group " + expectedGroupKey + " should be present in results after hash collision and linear probing",
                groupFound[expectedGroupKey]);
        }

        // Verify the data correctness - each group should have its correct value
        // This ensures that despite hash collisions and linear probing, the correct data is maintained
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            long expectedLongValue = groupId * 100L; // Value for each group

            Assert.assertEquals("Long value should be correct for group " + groupId + " after hash collision handling",
                expectedLongValue, result.getBlock(1).getLong(i));
        }
    }
}
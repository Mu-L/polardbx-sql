package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
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
 * Test class for GroupTopNExec with Integer data types
 */
public class IntGroupTopNExecTest extends BaseExecTest {

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
        // Create input data with group column and value column
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                IntegerBlock.of(10, 20, 30, 40, 50, 60)))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 2, 2, 3, 3),
                IntegerBlock.of(15, 25, 45, 55, 70, 80)))
            .build();

        // Create GroupTop operator
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by value column (index 1) descending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column (index 0)

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 from each group
        // Group 1: values 10,15,20,25,30 -> top 2 are 30,25
        // Group 2: values 40,45,50,55,60 -> top 2 are 60,55
        // Group 3: values 70,80 -> top 2 are 80,70
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2, 3, 3),
            IntegerBlock.of(30, 25, 60, 55, 80, 70)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testNoGroup() {
        // Create input data with no grouping
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(10, 20, 30, 40, 50)))
            .withChunk(new Chunk(
                IntegerBlock.of(15, 25, 35, 45, 55)))
            .build();

        // Create GroupTop operator with no grouping
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 0); // Sort by value column descending
        RexNode fetch = createFetch(3); // Take top 3 overall
        ImmutableBitSet groupSet = ImmutableBitSet.of(); // No grouping

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=3L to match createFetch(3)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 3L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 3 overall values: 55, 50, 45
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(55, 50, 45)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testOrderByAsc() {
        // Create input data with group column and value column
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                IntegerBlock.of(30, 10, 20, 60, 40, 50)))
            .build();

        // Create GroupTop operator with ascending order
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 1); // Sort by value column ascending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 smallest from each group
        // Group 1: values 30,10,20 -> top 2 smallest are 10,20
        // Group 2: values 60,40,50 -> top 2 smallest are 40,50
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            IntegerBlock.of(10, 20, 40, 50)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testOrderByDesc() {
        // Create input data with group column and value column
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                IntegerBlock.of(30, 10, 20, 60, 40, 50)))
            .build();

        // Create GroupTop operator with descending order
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by value column descending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 largest from each group
        // Group 1: values 30,10,20 -> top 2 largest are 30,20
        // Group 2: values 60,40,50 -> top 2 largest are 60,50
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            IntegerBlock.of(30, 20, 60, 50)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testMultipleSortColumns() {
        // Create input data with group column and two value columns
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                IntegerBlock.of(10, 10, 20, 30, 30, 40),
                IntegerBlock.of(100, 200, 150, 100, 200, 150)))
            .build();

        // Create GroupTop operator - sort by second and third columns in descending order
        RelCollation innerCollation = createCollation(RelFieldCollation.Direction.DESCENDING, 1,
            2); // Sort by second and third columns descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=1L to match createFetch(1)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 1L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 1 from each group based on multi-column sort
        // Group 1: (1,10,100), (1,10,200), (1,20,150) -> top 1 is (1,20,150) because 20 > 10
        // Group 2: (2,30,100), (2,30,200), (2,40,150) -> top 1 is (2,40,150) because 40 > 30
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2),
            IntegerBlock.of(20, 40),
            IntegerBlock.of(150, 150)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testEmptyInput() {
        // Create empty input data
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .build();

        // Create GroupTop operator
        RelCollation innerCollation = createCollation(1);
        RexNode fetch = createFetch(2);
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
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
    public void testWithNullValuesAsc() {
        // Create input data with null values in integer column
        Integer[] intValues = new Integer[] {100, null, 300, 400, null, 600};

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                IntegerBlock.of(intValues)))
            .build();

        // Create GroupTop operator - nulls first, descending order
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        Integer[] expectedIntValues = new Integer[] {null, 100, null, 400};

        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            IntegerBlock.of(expectedIntValues)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testWithNullValuesDesc() {
        // Create input data with null values in integer column
        Integer[] intValues = new Integer[] {100, null, 300, 400, null, 600};

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                IntegerBlock.of(intValues)))
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
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=2L to match createFetch(2)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - nulls should come first, then descending order
        Integer[] expectedIntValues = new Integer[] {300, 100, 600, 400};

        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            IntegerBlock.of(expectedIntValues)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testMultipleGroups() {
        // Create input data with multiple groups and integer values
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4, 5),
                IntegerBlock.of(100, 200, 300, 400, 500)))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4, 5),
                IntegerBlock.of(150, 250, 350, 450, 550)))
            .build();

        // Create GroupTop operator
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 1); // Sort by integer value ascending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=1L to match createFetch(1)
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 1L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get minimum value from each group (ascending order)
        // Group 1: 100, 150 -> min is 100
        // Group 2: 200, 250 -> min is 200
        // Group 3: 300, 350 -> min is 300
        // Group 4: 400, 450 -> min is 400
        // Group 5: 500, 550 -> min is 500
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2, 3, 4, 5),
            IntegerBlock.of(100, 200, 300, 400, 500)
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

        // Create test data with multiple groups using Integer columns
        List<Integer> groupKeys = new ArrayList<>();
        List<Integer> intValues = new ArrayList<>();

        for (int group = 1; group <= groupCount; group++) {
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(group);
                intValues.add(group * 100 + item); // Integer values
            }
        }

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
                IntegerBlock.wrap(intValues.stream().mapToInt(i -> i).toArray())))
            .build();

        // Create GroupTop operator with small hash table size to force rehash
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by Integer column descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec with small estimateHashTableSize to trigger rehash
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
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
        // Group 1: (101), Group 2: (201), etc.
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            int expectedIntValue = groupId * 100 + 1; // Highest value for each group

            Assert.assertEquals("Integer value should be correct for group " + groupId,
                expectedIntValue, result.getBlock(1).getInt(i));
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
        List<Integer> intValues = new ArrayList<>();

        for (int i = 0; i < groupCount; i++) {
            int groupKey = strategicGroupKeys[i];
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(groupKey);
                intValues.add(groupKey * 10 + item); // Integer values
            }
        }

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
                IntegerBlock.wrap(intValues.stream().mapToInt(i -> i).toArray())))
            .build();

        // Create GroupTop operator with very small hash table size to force collisions and multiple rehashes
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by Integer column descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTop(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec with very small estimateHashTableSize to guarantee hash collisions and linear probing
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.IntegerType);
        SpillerFactory spillerFactory = null;
        // Pass fetchValue=1L to match createFetch(1)
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
            int expectedIntValue = groupId * 10; // Value for each group

            Assert.assertEquals(
                "Integer value should be correct for group " + groupId + " after hash collision handling",
                expectedIntValue, result.getBlock(1).getInt(i));
        }
    }
}
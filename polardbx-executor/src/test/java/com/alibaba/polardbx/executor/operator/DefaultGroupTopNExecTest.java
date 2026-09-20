package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.chunk.StringBlockBuilder;
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
 * Test class for GroupTopNExec with mixed/default data types
 */
public class DefaultGroupTopNExecTest extends BaseExecTest {

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
    private static class MockGroupTopN extends GroupTopN {
        private final RelCollation innerCollation;
        private final RexNode fetch;
        private final ImmutableBitSet groupSet;
        private final RelNode input;

        public MockGroupTopN(RelCollation innerCollation, RexNode fetch, ImmutableBitSet groupSet, RelNode input) {
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

    private StringBlock createStringBlock(String... values) {
        StringBlockBuilder builder = new StringBlockBuilder(values.length, 10);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            } else {
                builder.writeString(value);
            }
        }
        return (StringBlock) builder.build();
    }

    private DecimalBlock createDecimalBlock(String... values) {
        DecimalBlockBuilder builder = new DecimalBlockBuilder(values.length, DataTypes.DecimalType);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            } else {
                builder.writeDecimal(Decimal.fromString(value));
            }
        }
        return (DecimalBlock) builder.build();
    }

    @Test
    public void testSimple() {
        // Create input data with mixed data types: Integer group, String name, Long value
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
                LongBlock.of(100L, 200L, 300L, 400L, 500L, 600L)))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 2, 2, 3, 3),
                createStringBlock("Grace", "Henry", "Ivy", "Jack", "Kate", "Leo"),
                LongBlock.of(150L, 250L, 450L, 550L, 700L, 800L)))
            .build();

        // Create GroupTop operator - sort by Long value column descending
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING,
                2); // Sort by Long value column (index 2) descending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column (index 0)

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 from each group by Long value
        // Group 1: (Alice,100L), (Grace,150L), (Bob,200L), (Henry,250L), (Charlie,300L) -> top 2 are (Charlie,300L), (Henry,250L)
        // Group 2: (David,400L), (Ivy,450L), (Eve,500L), (Jack,550L), (Frank,600L) -> top 2 are (Frank,600L), (Jack,550L)
        // Group 3: (Kate,700L), (Leo,800L) -> top 2 are (Leo,800L), (Kate,700L)
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2, 3, 3),
            createStringBlock("Charlie", "Henry", "Frank", "Jack", "Leo", "Kate"),
            LongBlock.of(300L, 250L, 600L, 550L, 800L, 700L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testNoGroup() {
        // Create input data with mixed types and no grouping
        MockExec inputExec = MockExec.builder(DataTypes.StringType, DataTypes.DecimalType)
            .withChunk(new Chunk(
                createStringBlock("Product1", "Product2", "Product3", "Product4", "Product5"),
                createDecimalBlock("10.50", "20.75", "30.25", "40.00", "50.50")))
            .withChunk(new Chunk(
                createStringBlock("Product6", "Product7", "Product8", "Product9", "Product10"),
                createDecimalBlock("15.25", "25.00", "35.75", "45.50", "55.25")))
            .build();

        // Create GroupTop operator with no grouping - sort by Decimal value descending
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by Decimal value column descending
        RexNode fetch = createFetch(3); // Take top 3 overall
        ImmutableBitSet groupSet = ImmutableBitSet.of(); // No grouping

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.StringType, DataTypes.DecimalType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 3L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 3 overall values by Decimal: Product10(55.25), Product5(50.50), Product9(45.50)
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            createStringBlock("Product10", "Product5", "Product9"),
            createDecimalBlock("55.25", "50.50", "45.50")
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testOrderByAsc() {
        // Create input data with mixed types
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
                createDecimalBlock("30.25", "10.50", "20.75", "60.75", "40.00", "50.50")))
            .build();

        // Create GroupTop operator with ascending order by Decimal value
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 2); // Sort by Decimal value ascending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 smallest from each group
        // Group 1: (Alice,30.25), (Bob,10.50), (Charlie,20.75) -> top 2 smallest are (Bob,10.50), (Charlie,20.75)
        // Group 2: (David,60.75), (Eve,40.00), (Frank,50.50) -> top 2 smallest are (Eve,40.00), (Frank,50.50)
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            createStringBlock("Bob", "Charlie", "Eve", "Frank"),
            createDecimalBlock("10.50", "20.75", "40.00", "50.50")
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testOrderByDesc() {
        // Create input data with mixed types
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
                createDecimalBlock("30.25", "10.50", "20.75", "60.75", "40.00", "50.50")))
            .build();

        // Create GroupTop operator with descending order by Decimal value
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal value descending
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 2 largest from each group
        // Group 1: (Alice,30.25), (Bob,10.50), (Charlie,20.75) -> top 2 largest are (Alice,30.25), (Charlie,20.75)
        // Group 2: (David,60.75), (Eve,40.00), (Frank,50.50) -> top 2 largest are (David,60.75), (Frank,50.50)
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            createStringBlock("Alice", "Charlie", "David", "Frank"),
            createDecimalBlock("30.25", "20.75", "60.75", "50.50")
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testMultipleSortColumns() {
        // Create input data with multiple columns for sorting
        MockExec inputExec =
            MockExec.builder(DataTypes.IntegerType, DataTypes.LongType, DataTypes.DecimalType, DataTypes.StringType)
                .withChunk(new Chunk(
                    IntegerBlock.of(1, 1, 1, 2, 2, 2),
                    LongBlock.of(100L, 100L, 200L, 300L, 300L, 400L),
                    createDecimalBlock("10.50", "20.75", "15.25", "10.50", "20.75", "15.25"),
                    createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank")))
                .build();

        // Create GroupTop operator - sort by Long and Decimal columns in descending order
        RelCollation innerCollation = createCollation(RelFieldCollation.Direction.DESCENDING, 1,
            2); // Sort by Long and Decimal columns descending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.LongType, DataTypes.DecimalType, DataTypes.StringType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 1L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get top 1 from each group based on multi-column sort
        // Group 1: (1,100L,10.50,Alice), (1,100L,20.75,Bob), (1,200L,15.25,Charlie) -> top 1 is (1,200L,15.25,Charlie) because 200L > 100L
        // Group 2: (2,300L,10.50,David), (2,300L,20.75,Eve), (2,400L,15.25,Frank) -> top 1 is (2,400L,15.25,Frank) because 400L > 300L
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2),
            LongBlock.of(200L, 400L),
            createDecimalBlock("15.25", "15.25"),
            createStringBlock("Charlie", "Frank")
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testEmptyInput() {
        // Create empty input data with mixed types
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType)
            .build();

        // Create GroupTop operator
        RelCollation innerCollation = createCollation(2);
        RexNode fetch = createFetch(2);
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should be empty
        List<Chunk> expectedChunks = Collections.emptyList();
        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testWithNullValues() {
        // Create input data with null values in mixed types
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 1, 2, 2, 2),
                createStringBlock("Alice", null, "Charlie", "David", null, "Frank"),
                createDecimalBlock("100.50", null, "300.25", "400.00", null, "600.75")))
            .build();

        // Create GroupTop operator - nulls first, descending order by Decimal value
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        RexNode fetch = createFetch(2); // Take top 2 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes =
            Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, DataTypes.DecimalType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 2L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - nulls should come first, then descending order
        // Group 1: (Alice,100.50), (null,null), (Charlie,300.25) -> top 2 are (null,null), (Charlie,300.25)
        // Group 2: (David,400.00), (null,null), (Frank,600.75) -> top 2 are (null,null), (Frank,600.75)
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            createStringBlock("Charlie", "Alice", "Frank", "David"),
            createDecimalBlock("300.25", "100.50", "600.75", "400.00")
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }

    @Test
    public void testMultipleGroups() {
        // Create input data with multiple groups and mixed data types
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.LongType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4, 5),
                createStringBlock("Group1A", "Group2A", "Group3A", "Group4A", "Group5A"),
                LongBlock.of(1000L, 2000L, 3000L, 4000L, 5000L)))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4, 5),
                createStringBlock("Group1B", "Group2B", "Group3B", "Group4B", "Group5B"),
                LongBlock.of(1500L, 2500L, 3500L, 4500L, 5500L)))
            .build();

        // Create GroupTop operator - sort by Long value ascending
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 2); // Sort by Long value ascending
        RexNode fetch = createFetch(1); // Take top 1 from each group
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column

        GroupTopN groupTopN = new MockGroupTopN(innerCollation, fetch, groupSet, null);

        // Create GroupTopNExec
        List<DataType> inputDataTypes = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType, DataTypes.LongType);
        SpillerFactory spillerFactory = null;
        GroupTopNExec exec = new GroupTopNExec(inputDataTypes, groupTopN, 1024, spillerFactory, 1L, context);

        // Run test
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        // Verify results - should get minimum Long value from each group (ascending order)
        // Group 1: (Group1A,1000L), (Group1B,1500L) -> min is (Group1A,1000L)
        // Group 2: (Group2A,2000L), (Group2B,2500L) -> min is (Group2A,2000L)
        // Group 3: (Group3A,3000L), (Group3B,3500L) -> min is (Group3A,3000L)
        // Group 4: (Group4A,4000L), (Group4B,4500L) -> min is (Group4A,4000L)
        // Group 5: (Group5A,5000L), (Group5B,5500L) -> min is (Group5A,5000L)
        List<Chunk> expectedChunks = Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2, 3, 4, 5),
            createStringBlock("Group1A", "Group2A", "Group3A", "Group4A", "Group5A"),
            LongBlock.of(1000L, 2000L, 3000L, 4000L, 5000L)
        ));

        assertExecResultByRow(test.result(), expectedChunks, false);
    }
}
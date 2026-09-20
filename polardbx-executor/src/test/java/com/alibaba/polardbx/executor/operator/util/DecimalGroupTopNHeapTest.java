package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.chunk.StringBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Test class for DecimalGroupTopNHeap with Decimal data types
 */
public class DecimalGroupTopNHeapTest {

    private ExecutionContext context;
    private Random random;
    private DecimalType decimalType = new DecimalType(30, 5);
    private static final int CHUNK_LIMIT = 1000;

    @Before
    public void setUp() {
        context = new ExecutionContext();
        context.setTraceId("mock_trace_id");
        context.setMemoryPool(
            MemoryManager.getInstance().createQueryMemoryPool(true, context.getTraceId(), context.getExtraCmds()));
        random = new Random();
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

    private DecimalBlock createDecimalBlock(String... values) {
        DecimalBlockBuilder builder = new DecimalBlockBuilder(values.length, decimalType);
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            } else {
                builder.writeDecimal(Decimal.fromString(value));
            }
        }
        return (DecimalBlock) builder.build();
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

    @Test
    public void testSimple() {
        // Create input data with Integer group column and Decimal value column
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
            createDecimalBlock("10.50", "20.75", "30.25", "40.00", "50.50", "60.75")
        );

        // Create DecimalGroupTopNHeap
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal column descending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 2 from each group by Decimal value
        // Group 1: Charlie(30.25), Bob(20.75)
        // Group 2: Frank(60.75), Eve(50.50)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        // Verify group 1 results
        Assert.assertEquals(1, result.getBlock(0).getInt(0)); // Group 1
        Assert.assertEquals("Charlie", result.getBlock(1).getString(0));
        Assert.assertEquals(Decimal.fromString("30.25"), result.getBlock(2).getDecimal(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1)); // Group 1
        Assert.assertEquals("Bob", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("20.75"), result.getBlock(2).getDecimal(1));

        // Verify group 2 results
        Assert.assertEquals(2, result.getBlock(0).getInt(2)); // Group 2
        Assert.assertEquals("Frank", result.getBlock(1).getString(2));
        Assert.assertEquals(Decimal.fromString("60.75"), result.getBlock(2).getDecimal(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3)); // Group 2
        Assert.assertEquals("Eve", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("50.50"), result.getBlock(2).getDecimal(3));

        heap.close();
    }

    @Test
    public void testNoGroup() {
        // Create input data with no grouping
        Chunk groupKeyChunk = new Chunk(); // Empty group key for no grouping
        Chunk inputChunk = new Chunk(
            createStringBlock("Product1", "Product2", "Product3", "Product4", "Product5"),
            createDecimalBlock("10.50", "20.75", "30.25", "40.00", "50.50")
        );

        // Create DecimalGroupTopNHeap with no grouping
        DataType[] groupKeyTypes = {}; // No group key types
        DataType[] inputTypes = {DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(); // No grouping
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1); // Sort by Decimal value descending
        long fetchValue = 3; // Take top 3 overall

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 3 overall values by Decimal: Product5(50.50), Product4(40.00), Product3(30.25)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(3, result.getPositionCount());

        Assert.assertEquals("Product5", result.getBlock(0).getString(0));
        Assert.assertEquals(Decimal.fromString("50.50"), result.getBlock(1).getDecimal(0));

        Assert.assertEquals("Product4", result.getBlock(0).getString(1));
        Assert.assertEquals(Decimal.fromString("40.00"), result.getBlock(1).getDecimal(1));

        Assert.assertEquals("Product3", result.getBlock(0).getString(2));
        Assert.assertEquals(Decimal.fromString("30.25"), result.getBlock(1).getDecimal(2));

        heap.close();
    }

    @Test
    public void testOrderByAsc() {
        // Create input data with Integer group column and Decimal value column
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
            createDecimalBlock("30.25", "10.50", "20.75", "60.75", "40.00", "50.50")
        );

        // Create DecimalGroupTopNHeap with ascending order
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 2); // Sort by Decimal value ascending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 2 smallest from each group
        // Group 1: Bob(10.50), Charlie(20.75)
        // Group 2: Eve(40.00), Frank(50.50)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        // Verify group 1 results (ascending order)
        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals("Bob", result.getBlock(1).getString(0));
        Assert.assertEquals(Decimal.fromString("10.50"), result.getBlock(2).getDecimal(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals("Charlie", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("20.75"), result.getBlock(2).getDecimal(1));

        // Verify group 2 results (ascending order)
        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals("Eve", result.getBlock(1).getString(2));
        Assert.assertEquals(Decimal.fromString("40.00"), result.getBlock(2).getDecimal(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals("Frank", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("50.50"), result.getBlock(2).getDecimal(3));

        heap.close();
    }

    @Test
    public void testOrderByDesc() {
        // Create input data with Integer group column and Decimal value column
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
            createDecimalBlock("30.25", "10.50", "20.75", "60.75", "40.00", "50.50")
        );

        // Create DecimalGroupTopNHeap with descending order
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal value descending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 2 largest from each group
        // Group 1: Alice(30.25), Charlie(20.75)
        // Group 2: David(60.75), Frank(50.50)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        // Verify group 1 results (descending order)
        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals("Alice", result.getBlock(1).getString(0));
        Assert.assertEquals(Decimal.fromString("30.25"), result.getBlock(2).getDecimal(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals("Charlie", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("20.75"), result.getBlock(2).getDecimal(1));

        // Verify group 2 results (descending order)
        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals("David", result.getBlock(1).getString(2));
        Assert.assertEquals(Decimal.fromString("60.75"), result.getBlock(2).getDecimal(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals("Frank", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("50.50"), result.getBlock(2).getDecimal(3));

        heap.close();
    }

    @Test
    public void testMultipleSortColumns() {
        // Create input data with multiple columns for sorting
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDecimalBlock("100.50", "100.50", "200.25", "300.75", "300.75", "400.00"),
            createDecimalBlock("10.50", "20.75", "15.25", "10.50", "20.75", "15.25"),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank")
        );

        // Create DecimalGroupTopNHeap - sort by first Decimal column descending, then by second Decimal column descending
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, decimalType, decimalType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by both Decimal columns descending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 1 from each group based on multi-column sort
        // Group 1: Charlie(200.25, 15.25) because 200.25 > 100.50
        // Group 2: Frank(400.00, 15.25) because 400.00 > 300.75
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(2, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Decimal.fromString("200.25"), result.getBlock(1).getDecimal(0));
        Assert.assertEquals(Decimal.fromString("15.25"), result.getBlock(2).getDecimal(0));
        Assert.assertEquals("Charlie", result.getBlock(3).getString(0));

        Assert.assertEquals(2, result.getBlock(0).getInt(1));
        Assert.assertEquals(Decimal.fromString("400.00"), result.getBlock(1).getDecimal(1));
        Assert.assertEquals(Decimal.fromString("15.25"), result.getBlock(2).getDecimal(1));
        Assert.assertEquals("Frank", result.getBlock(3).getString(1));

        heap.close();
    }

    @Test
    public void testEmptyInput() {
        // Create empty input data
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of());
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(),
            createStringBlock(),
            createDecimalBlock()
        );

        // Create DecimalGroupTopNHeap
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);
        RelCollation innerCollation = createCollation(2);
        long fetchValue = 2;

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should be empty
        Assert.assertEquals(0, results.size());

        heap.close();
    }

    @Test
    public void testWithNullValuesAsc() {
        // Create input data with null values in Decimal column
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
            createDecimalBlock("100.50", null, "300.25", "400.00", null, "600.75")
        );

        // Create DecimalGroupTopNHeap - nulls first, ascending order
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(2, RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - nulls should come first, then ascending order
        // Group 1: Bob(null), Alice(100.50)
        // Group 2: Eve(null), David(400.00)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals("Bob", result.getBlock(1).getString(0));
        Assert.assertTrue(result.getBlock(2).isNull(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals("Alice", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("100.50"), result.getBlock(2).getDecimal(1));

        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals("Eve", result.getBlock(1).getString(2));
        Assert.assertTrue(result.getBlock(2).isNull(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals("David", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("400.00"), result.getBlock(2).getDecimal(3));

        heap.close();
    }

    @Test
    public void testWithNullValuesDesc() {
        // Create input data with null values in Decimal column
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
            createDecimalBlock("100.50", null, "300.25", "400.00", null, "600.75")
        );

        // Create DecimalGroupTopNHeap - nulls last, descending order
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 2 largest from each group, nulls last
        // Group 1: Charlie(300.25), Alice(100.50)
        // Group 2: Frank(600.75), David(400.00)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals("Charlie", result.getBlock(1).getString(0));
        Assert.assertEquals(Decimal.fromString("300.25"), result.getBlock(2).getDecimal(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals("Alice", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("100.50"), result.getBlock(2).getDecimal(1));

        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals("Frank", result.getBlock(1).getString(2));
        Assert.assertEquals(Decimal.fromString("600.75"), result.getBlock(2).getDecimal(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals("David", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("400.00"), result.getBlock(2).getDecimal(3));

        heap.close();
    }

    @Test
    public void testMultipleGroups() {
        // Create input data with multiple groups and Decimal values
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 2, 3, 4, 5, 1, 2, 3, 4, 5));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 2, 3, 4, 5, 1, 2, 3, 4, 5),
            createStringBlock("Group1A", "Group2A", "Group3A", "Group4A", "Group5A",
                "Group1B", "Group2B", "Group3B", "Group4B", "Group5B"),
            createDecimalBlock("1000.50", "2000.75", "3000.25", "4000.00", "5000.50",
                "1500.25", "2500.50", "3500.75", "4500.25", "5500.00")
        );

        // Create DecimalGroupTopNHeap
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 2); // Sort by Decimal value ascending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get minimum Decimal value from each group (ascending order)
        // Group 1: Group1A(1000.50) < Group1B(1500.25)
        // Group 2: Group2A(2000.75) < Group2B(2500.50)
        // etc.
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(5, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals("Group1A", result.getBlock(1).getString(0));
        Assert.assertEquals(Decimal.fromString("1000.50"), result.getBlock(2).getDecimal(0));

        Assert.assertEquals(2, result.getBlock(0).getInt(1));
        Assert.assertEquals("Group2A", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("2000.75"), result.getBlock(2).getDecimal(1));

        Assert.assertEquals(3, result.getBlock(0).getInt(2));
        Assert.assertEquals("Group3A", result.getBlock(1).getString(2));
        Assert.assertEquals(Decimal.fromString("3000.25"), result.getBlock(2).getDecimal(2));

        Assert.assertEquals(4, result.getBlock(0).getInt(3));
        Assert.assertEquals("Group4A", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("4000.00"), result.getBlock(2).getDecimal(3));

        Assert.assertEquals(5, result.getBlock(0).getInt(4));
        Assert.assertEquals("Group5A", result.getBlock(1).getString(4));
        Assert.assertEquals(Decimal.fromString("5000.50"), result.getBlock(2).getDecimal(4));

        heap.close();
    }

    @Test
    public void testMultipleChunks() {
        // Test with multiple input chunks
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal value descending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add first chunk
        Chunk groupKeyChunk1 = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk1 = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank"),
            createDecimalBlock("100.50", "200.75", "300.25", "400.00", "500.50", "600.75")
        );
        heap.addChunk(groupKeyChunk1, inputChunk1);

        // Add second chunk
        Chunk groupKeyChunk2 = new Chunk(IntegerBlock.of(1, 1, 2, 2, 3, 3));
        Chunk inputChunk2 = new Chunk(
            IntegerBlock.of(1, 1, 2, 2, 3, 3),
            createStringBlock("Grace", "Henry", "Ivy", "Jack", "Kate", "Leo"),
            createDecimalBlock("150.25", "250.50", "450.75", "550.25", "700.00", "800.50")
        );
        heap.addChunk(groupKeyChunk2, inputChunk2);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 2 from each group across all chunks
        // Group 1: Charlie(300.25), Henry(250.50)
        // Group 2: Frank(600.75), Jack(550.25)
        // Group 3: Leo(800.50), Kate(700.00)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(6, result.getPositionCount());

        // Group 1 top 2
        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals("Charlie", result.getBlock(1).getString(0));
        Assert.assertEquals(Decimal.fromString("300.25"), result.getBlock(2).getDecimal(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals("Henry", result.getBlock(1).getString(1));
        Assert.assertEquals(Decimal.fromString("250.50"), result.getBlock(2).getDecimal(1));

        // Group 2 top 2
        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals("Frank", result.getBlock(1).getString(2));
        Assert.assertEquals(Decimal.fromString("600.75"), result.getBlock(2).getDecimal(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals("Jack", result.getBlock(1).getString(3));
        Assert.assertEquals(Decimal.fromString("550.25"), result.getBlock(2).getDecimal(3));

        // Group 3 top 2
        Assert.assertEquals(3, result.getBlock(0).getInt(4));
        Assert.assertEquals("Leo", result.getBlock(1).getString(4));
        Assert.assertEquals(Decimal.fromString("800.50"), result.getBlock(2).getDecimal(4));

        Assert.assertEquals(3, result.getBlock(0).getInt(5));
        Assert.assertEquals("Kate", result.getBlock(1).getString(5));
        Assert.assertEquals(Decimal.fromString("700.00"), result.getBlock(2).getDecimal(5));

        heap.close();
    }

    @Test
    public void testLargeDataSet() {
        // Test with larger dataset to verify heap behavior
        final int groupCount = 10;
        final int itemsPerGroup = 100;
        final int totalItems = groupCount * itemsPerGroup;

        // Generate test data
        int[] groupKeys = new int[totalItems];
        String[] decimalValues = new String[totalItems];
        String[] stringValues = new String[totalItems];

        for (int group = 1; group <= groupCount; group++) {
            for (int item = 0; item < itemsPerGroup; item++) {
                int index = (group - 1) * itemsPerGroup + item;
                groupKeys[index] = group;
                decimalValues[index] = String.format("%d.%02d", group * 100 + item, item % 100);
                stringValues[index] = "Item" + group + "_" + item;
            }
        }

        Chunk groupKeyChunk = new Chunk(IntegerBlock.wrap(groupKeys));
        Chunk inputChunk = new Chunk(
            IntegerBlock.wrap(groupKeys),
            createStringBlock(stringValues),
            createDecimalBlock(decimalValues)
        );

        // Create DecimalGroupTopNHeap
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal column descending
        long fetchValue = 5; // Take top 5 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add chunk
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 5 from each of the 10 groups = 50 total results
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(groupCount * fetchValue, result.getPositionCount()); // 10 groups * 5 items = 50

        // Verify that we have results from all groups
        boolean[] groupFound = new boolean[groupCount + 1]; // 1-indexed
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            groupFound[groupId] = true;
        }

        for (int group = 1; group <= groupCount; group++) {
            Assert.assertTrue("Group " + group + " should be present in results", groupFound[group]);
        }

        heap.close();
    }

    @Test
    public void testEstimateSize() {
        // Test estimateSize method
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);
        RelCollation innerCollation = createCollation(2);
        long fetchValue = 2;

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Initial size should be greater than 0
        long initialSize = heap.estimateSize();
        Assert.assertTrue("Initial size should be greater than 0", initialSize > 0);

        // Add some data
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            createStringBlock("Alice", "Bob", "Charlie", "David"),
            createDecimalBlock("10.50", "20.75", "30.25", "40.00")
        );
        heap.addChunk(groupKeyChunk, inputChunk);

        // Size should increase after adding data
        long sizeAfterAdd = heap.estimateSize();
        Assert.assertTrue("Size should increase after adding data", sizeAfterAdd > initialSize);

        heap.close();
    }

    /**
     * Generate random decimal value in specified max precision & max scale.
     * Borrowed from DecimalTopNHeapTest
     */
    private byte[] randomDecimal(int maxPrecision, int maxScale, boolean isSigned) {
        int precision = random.nextInt(maxPrecision) + 1;
        int scale = random.nextInt(Math.min(precision, maxScale + 1));
        if (precision == scale) {
            scale--;
        }

        boolean isNeg = isSigned && random.nextInt() % 2 == 0;
        String numberStr = "0123456789";

        byte[] res = new byte[(scale == 0 ? precision : precision + 1) + (isNeg ? 1 : 0)];
        int i = 0;
        if (isNeg) {
            res[i++] = '-';
        }
        res[i++] = (byte) numberStr.charAt(random.nextInt(9) + 1);
        for (; i < precision - scale + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) numberStr.charAt(random.nextInt(10));
        }
        if (scale == 0) {
            return res;
        }
        res[i++] = '.';
        for (; i < precision + 1 + (isNeg ? 1 : 0); i++) {
            res[i] = (byte) numberStr.charAt(random.nextInt(10));
        }
        return res;
    }

    /**
     * Generate random chunk for stress testing
     * Borrowed and adapted from DecimalTopNHeapTest
     */
    private Chunk generateRandomChunk(int positionCount, int maxGroups) {
        BlockBuilder intBlockBuilder = new LongBlockBuilder(positionCount);
        BlockBuilder stringBlockBuilder = new StringBlockBuilder(positionCount, 20);
        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(positionCount, decimalType);

        for (int i = 0; i < positionCount; i++) {
            // Random group ID
            intBlockBuilder.writeLong(random.nextInt(maxGroups) + 1);

            // Random string name
            stringBlockBuilder.writeString("Item" + i);

            // Random decimal value
            if (random.nextInt(10) == 1) {
                decimalBlockBuilder.appendNull();
            } else {
                byte[] decimalStr = randomDecimal(10, 2, true);
                Decimal d = Decimal.fromString(new String(decimalStr));
                decimalBlockBuilder.writeDecimal(d);
            }
        }

        return new Chunk(
            intBlockBuilder.build(),
            stringBlockBuilder.build(),
            decimalBlockBuilder.build()
        );
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

        // Create test data with multiple groups using Integer and Decimal columns
        List<Integer> groupKeys = new ArrayList<>();
        List<String> stringValues = new ArrayList<>();
        List<String> decimalValues = new ArrayList<>();

        for (int group = 1; group <= groupCount; group++) {
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(group);
                stringValues.add("Group" + group + "_Item" + item); // String column
                decimalValues.add(String.format("%d.%02d", group * 100 + item, item % 100)); // Decimal values
            }
        }

        Chunk groupKeyChunk = new Chunk(IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()));
        Chunk inputChunk = new Chunk(
            IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
            createStringBlock(stringValues.toArray(new String[0])),
            createDecimalBlock(decimalValues.toArray(new String[0]))
        );

        // Create DecimalGroupTopNHeap with small hash table size to force rehash
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal column descending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        // Use small estimateHashTableSize to trigger rehash
        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, smallHashTableSize, context,
            memoryAllocator, CHUNK_LIMIT);

        // Add chunk - this should trigger rehash when enough groups are created
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 1 from each of the 10 groups = 10 total results
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
        // Group 1: (101.01), Group 2: (201.01), etc.
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            String expectedStringValue = "Group" + groupId + "_Item1";
            String expectedDecimalValue = String.format("%d.%02d", groupId * 100 + 1, 1);

            Assert.assertEquals("String value should be correct for group " + groupId,
                expectedStringValue, result.getBlock(1).getString(i));
            Assert.assertEquals("Decimal value should be correct for group " + groupId,
                Decimal.fromString(expectedDecimalValue), result.getBlock(2).getDecimal(i));
        }

        heap.close();
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
        List<String> stringValues = new ArrayList<>();
        List<String> decimalValues = new ArrayList<>();

        for (int i = 0; i < groupCount; i++) {
            int groupKey = strategicGroupKeys[i];
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(groupKey);
                stringValues.add("Group" + groupKey + "_Item" + item); // String column
                decimalValues.add(String.format("%d.%02d", groupKey * 10 + item, item % 100)); // Decimal values
            }
        }

        Chunk groupKeyChunk = new Chunk(IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()));
        Chunk inputChunk = new Chunk(
            IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
            createStringBlock(stringValues.toArray(new String[0])),
            createDecimalBlock(decimalValues.toArray(new String[0]))
        );

        // Create DecimalGroupTopNHeap with very small hash table size to force collisions and multiple rehashes
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.StringType, decimalType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 2); // Sort by Decimal column descending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        // Use very small estimateHashTableSize to guarantee hash collisions and linear probing
        DecimalGroupTopNHeap heap = new DecimalGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, verySmallHashTableSize, context,
            memoryAllocator, CHUNK_LIMIT);

        // Add chunk - this will definitely trigger hash collisions, linear probing, and multiple rehashes
        // The linear probing logic: while (k != NOT_EXISTS) { h = (h + 1) & mask; k = keys[h]; }
        // will be extensively exercised due to the small hash table size
        heap.addChunk(groupKeyChunk, inputChunk);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 1 from each of the 15 groups = 15 total results
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
            String expectedStringValue = "Group" + groupId + "_Item0";
            String expectedDecimalValue = String.format("%d.%02d", groupId * 10, 0);

            Assert.assertEquals(
                "String value should be correct for group " + groupId + " after hash collision handling",
                expectedStringValue, result.getBlock(1).getString(i));
            Assert.assertEquals(
                "Decimal value should be correct for group " + groupId + " after hash collision handling",
                Decimal.fromString(expectedDecimalValue), result.getBlock(2).getDecimal(i));
        }

        heap.close();
    }
}
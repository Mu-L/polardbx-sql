package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.chunk.StringBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
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

import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Test class for DateGroupTopNHeap with Date data types and mixed column sorting
 * All tests use two-column sorting with different data types to ensure DateGroupTopNHeap is used
 */
public class DateGroupTopNHeapTest {

    private ExecutionContext context;
    private Random random;
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

    private DateBlock createDateBlock(String... dateStrings) {
        DateBlockBuilder builder = new DateBlockBuilder(dateStrings.length, DataTypes.DateType, context);
        for (String dateStr : dateStrings) {
            if (dateStr == null) {
                builder.appendNull();
            } else {
                // Convert date string to Date object
                Date date = Date.valueOf(dateStr);
                builder.writeDate(date);
            }
        }
        return (DateBlock) builder.build();
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
        // Create input data with Integer group column, Date and String value columns (two different types)
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock("2023-01-01", "2023-01-02", "2023-01-03", "2023-02-01", "2023-02-02", "2023-02-03"),
            // First sort column: Date
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank") // Second sort column: String
        );

        // Create DateGroupTopNHeap with two-column sorting (Date + String)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and String columns descending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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

        // Verify results - should get top 2 from each group by Date+String columns
        // Group 1: (2023-01-03, Charlie), (2023-01-02, Bob)
        // Group 2: (2023-02-03, Frank), (2023-02-02, Eve)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        // Verify group 1 results
        Assert.assertEquals(1, result.getBlock(0).getInt(0)); // Group 1
        Assert.assertEquals(Date.valueOf("2023-01-03"), result.getBlock(1).getDate(0)); // Date value
        Assert.assertEquals("Charlie", result.getBlock(2).getString(0)); // String value

        Assert.assertEquals(1, result.getBlock(0).getInt(1)); // Group 1
        Assert.assertEquals(Date.valueOf("2023-01-02"), result.getBlock(1).getDate(1)); // Date value
        Assert.assertEquals("Bob", result.getBlock(2).getString(1)); // String value

        // Verify group 2 results
        Assert.assertEquals(2, result.getBlock(0).getInt(2)); // Group 2
        Assert.assertEquals(Date.valueOf("2023-02-03"), result.getBlock(1).getDate(2)); // Date value
        Assert.assertEquals("Frank", result.getBlock(2).getString(2)); // String value

        Assert.assertEquals(2, result.getBlock(0).getInt(3)); // Group 2
        Assert.assertEquals(Date.valueOf("2023-02-02"), result.getBlock(1).getDate(3)); // Date value
        Assert.assertEquals("Eve", result.getBlock(2).getString(3)); // String value

        heap.close();
    }

    @Test
    public void testNoGroup() {
        // Create input data with no grouping, using Date and Long columns (two different types)
        Chunk groupKeyChunk = new Chunk(); // Empty group key for no grouping
        Chunk inputChunk = new Chunk(
            createDateBlock("2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"),
            // First sort column: Date
            LongBlock.wrap(new long[] {100L, 200L, 300L, 400L, 500L}) // Second sort column: Long
        );

        // Create DateGroupTopNHeap with no grouping, two-column sorting (Date + Long)
        DataType[] groupKeyTypes = {}; // No group key types
        DataType[] inputTypes = {DataTypes.DateType, DataTypes.LongType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(); // No grouping
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 0, 1); // Sort by Date and Long columns descending
        long fetchValue = 3; // Take top 3 overall

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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

        // Verify results - should get top 3 overall values by Date+Long: (2023-01-05, 500L), (2023-01-04, 400L), (2023-01-03, 300L)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(3, result.getPositionCount());

        Assert.assertEquals(Date.valueOf("2023-01-05"), result.getBlock(0).getDate(0));
        Assert.assertEquals(500L, result.getBlock(1).getLong(0));

        Assert.assertEquals(Date.valueOf("2023-01-04"), result.getBlock(0).getDate(1));
        Assert.assertEquals(400L, result.getBlock(1).getLong(1));

        Assert.assertEquals(Date.valueOf("2023-01-03"), result.getBlock(0).getDate(2));
        Assert.assertEquals(300L, result.getBlock(1).getLong(2));

        heap.close();
    }

    @Test
    public void testOrderByAsc() {
        // Create input data with Integer group column, Date and String value columns (two different types)
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock("2023-01-03", "2023-01-01", "2023-01-02", "2023-02-03", "2023-02-01", "2023-02-02"),
            // First sort column: Date
            createStringBlock("Charlie", "Alice", "Bob", "Frank", "David", "Eve") // Second sort column: String
        );

        // Create DateGroupTopNHeap with ascending order, two-column sorting (Date + String)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 1, 2); // Sort by Date and String columns ascending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
        // Group 1: (2023-01-01, Alice), (2023-01-02, Bob)
        // Group 2: (2023-02-01, David), (2023-02-02, Eve)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        // Verify group 1 results (ascending order)
        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Date.valueOf("2023-01-01"), result.getBlock(1).getDate(0));
        Assert.assertEquals("Alice", result.getBlock(2).getString(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-01-02"), result.getBlock(1).getDate(1));
        Assert.assertEquals("Bob", result.getBlock(2).getString(1));

        // Verify group 2 results (ascending order)
        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals(Date.valueOf("2023-02-01"), result.getBlock(1).getDate(2));
        Assert.assertEquals("David", result.getBlock(2).getString(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals(Date.valueOf("2023-02-02"), result.getBlock(1).getDate(3));
        Assert.assertEquals("Eve", result.getBlock(2).getString(3));

        heap.close();
    }

    @Test
    public void testOrderByDesc() {
        // Create input data with Integer group column, Date and Long value columns (two different types)
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock("2023-01-03", "2023-01-01", "2023-01-02", "2023-02-03", "2023-02-01", "2023-02-02"),
            // First sort column: Date
            LongBlock.wrap(new long[] {300L, 100L, 200L, 600L, 400L, 500L}) // Second sort column: Long
        );

        // Create DateGroupTopNHeap with descending order, two-column sorting (Date + Long)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.LongType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and Long columns descending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
        // Group 1: (2023-01-03, 300L), (2023-01-02, 200L)
        // Group 2: (2023-02-03, 600L), (2023-02-02, 500L)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        // Verify group 1 results (descending order)
        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Date.valueOf("2023-01-03"), result.getBlock(1).getDate(0));
        Assert.assertEquals(300L, result.getBlock(2).getLong(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-01-02"), result.getBlock(1).getDate(1));
        Assert.assertEquals(200L, result.getBlock(2).getLong(1));

        // Verify group 2 results (descending order)
        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals(Date.valueOf("2023-02-03"), result.getBlock(1).getDate(2));
        Assert.assertEquals(600L, result.getBlock(2).getLong(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals(Date.valueOf("2023-02-02"), result.getBlock(1).getDate(3));
        Assert.assertEquals(500L, result.getBlock(2).getLong(3));

        heap.close();
    }

    @Test
    public void testMultipleSortColumns() {
        // Create input data with Date and String columns for sorting (two different types)
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock("2023-01-02", "2023-01-02", "2023-01-01", "2023-02-02", "2023-02-02", "2023-02-01"),
            // First sort column: Date
            createStringBlock("B", "A", "C", "B", "A", "C") // Second sort column: String
        );

        // Create DateGroupTopNHeap - sort by Date and String columns in descending order
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and String columns descending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
        // Group 1: (2023-01-02, B) because 2023-01-02 > 2023-01-01, and among 2023-01-02s, B > A
        // Group 2: (2023-02-02, B) because 2023-02-02 > 2023-02-01, and among 2023-02-02s, B > A
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(2, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Date.valueOf("2023-01-02"), result.getBlock(1).getDate(0));
        Assert.assertEquals("B", result.getBlock(2).getString(0));

        Assert.assertEquals(2, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-02-02"), result.getBlock(1).getDate(1));
        Assert.assertEquals("B", result.getBlock(2).getString(1));

        heap.close();
    }

    @Test
    public void testEmptyInput() {
        // Create empty input data
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of());
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(),
            createDateBlock(),
            createStringBlock()
        );

        // Create DateGroupTopNHeap with two-column sorting (Date + String)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);
        RelCollation innerCollation = createCollation(1, 2); // Sort by Date and String columns
        long fetchValue = 2;

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
        // Create input data with null values in Date column
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));

        String[] dateValues = new String[] {"2023-01-01", null, "2023-01-03", "2023-02-01", null, "2023-02-03"};

        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock(dateValues), // First sort column: Date (with nulls)
            createStringBlock("Alice", "Bob", "Charlie", "David", "Eve", "Frank") // Second sort column: String
        );

        // Create DateGroupTopNHeap - nulls first, ascending order, two-column sorting (Date + String)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));
        fieldCollations.add(new RelFieldCollation(2, RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
        // Group 1: (null, Bob), (2023-01-01, Alice)
        // Group 2: (null, Eve), (2023-02-01, David)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertTrue(result.getBlock(1).isNull(0));
        Assert.assertEquals("Bob", result.getBlock(2).getString(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-01-01"), result.getBlock(1).getDate(1));
        Assert.assertEquals("Alice", result.getBlock(2).getString(1));

        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertTrue(result.getBlock(1).isNull(2));
        Assert.assertEquals("Eve", result.getBlock(2).getString(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals(Date.valueOf("2023-02-01"), result.getBlock(1).getDate(3));
        Assert.assertEquals("David", result.getBlock(2).getString(3));

        heap.close();
    }

    @Test
    public void testWithNullValuesDesc() {
        // Create input data with null values in Date column
        String[] dateValues = new String[] {"2023-01-01", null, "2023-01-03", "2023-02-01", null, "2023-02-03"};

        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock(dateValues), // First sort column: Date (with nulls)
            LongBlock.wrap(new long[] {100L, 200L, 300L, 400L, 500L, 600L}) // Second sort column: Long
        );

        // Create DateGroupTopNHeap - nulls last, descending order, two-column sorting (Date + Long)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.LongType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        fieldCollations.add(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
        fieldCollations.add(new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
        RelCollation innerCollation = RelCollationImpl.of(fieldCollations);
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
        // Group 1: (2023-01-03, 300L), (2023-01-01, 100L)
        // Group 2: (2023-02-03, 600L), (2023-02-01, 400L)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(4, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Date.valueOf("2023-01-03"), result.getBlock(1).getDate(0));
        Assert.assertEquals(300L, result.getBlock(2).getLong(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-01-01"), result.getBlock(1).getDate(1));
        Assert.assertEquals(100L, result.getBlock(2).getLong(1));

        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals(Date.valueOf("2023-02-03"), result.getBlock(1).getDate(2));
        Assert.assertEquals(600L, result.getBlock(2).getLong(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals(Date.valueOf("2023-02-01"), result.getBlock(1).getDate(3));
        Assert.assertEquals(400L, result.getBlock(2).getLong(3));

        heap.close();
    }

    @Test
    public void testMultipleGroups() {
        // Create input data with multiple groups, Date and String columns (two different types)
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 2, 3, 4, 5, 1, 2, 3, 4, 5));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 2, 3, 4, 5, 1, 2, 3, 4, 5),
            createDateBlock("2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01", "2023-05-01",
                "2023-01-15", "2023-02-15", "2023-03-15", "2023-04-15", "2023-05-15"), // First sort column: Date
            createStringBlock("Group1A", "Group2A", "Group3A", "Group4A", "Group5A",
                "Group1B", "Group2B", "Group3B", "Group4B", "Group5B") // Second sort column: String
        );

        // Create DateGroupTopNHeap with two-column sorting (Date + String)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.ASCENDING, 1, 2); // Sort by Date and String columns ascending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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

        // Verify results - should get minimum Date+String value from each group (ascending order)
        // Group 1: (2023-01-01, Group1A) < (2023-01-15, Group1B)
        // Group 2: (2023-02-01, Group2A) < (2023-02-15, Group2B)
        // etc.
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(5, result.getPositionCount());

        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Date.valueOf("2023-01-01"), result.getBlock(1).getDate(0));
        Assert.assertEquals("Group1A", result.getBlock(2).getString(0));

        Assert.assertEquals(2, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-02-01"), result.getBlock(1).getDate(1));
        Assert.assertEquals("Group2A", result.getBlock(2).getString(1));

        Assert.assertEquals(3, result.getBlock(0).getInt(2));
        Assert.assertEquals(Date.valueOf("2023-03-01"), result.getBlock(1).getDate(2));
        Assert.assertEquals("Group3A", result.getBlock(2).getString(2));

        Assert.assertEquals(4, result.getBlock(0).getInt(3));
        Assert.assertEquals(Date.valueOf("2023-04-01"), result.getBlock(1).getDate(3));
        Assert.assertEquals("Group4A", result.getBlock(2).getString(3));

        Assert.assertEquals(5, result.getBlock(0).getInt(4));
        Assert.assertEquals(Date.valueOf("2023-05-01"), result.getBlock(1).getDate(4));
        Assert.assertEquals("Group5A", result.getBlock(2).getString(4));

        heap.close();
    }

    @Test
    public void testMultipleChunks() {
        // Test with multiple input chunks, Date and Long columns (two different types)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.LongType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and Long columns descending
        long fetchValue = 2; // Take top 2 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Add first chunk
        Chunk groupKeyChunk1 = new Chunk(IntegerBlock.of(1, 1, 1, 2, 2, 2));
        Chunk inputChunk1 = new Chunk(
            IntegerBlock.of(1, 1, 1, 2, 2, 2),
            createDateBlock("2023-01-01", "2023-01-02", "2023-01-03", "2023-02-01", "2023-02-02", "2023-02-03"),
            // First sort column: Date
            LongBlock.wrap(new long[] {100L, 200L, 300L, 400L, 500L, 600L}) // Second sort column: Long
        );
        heap.addChunk(groupKeyChunk1, inputChunk1);

        // Add second chunk
        Chunk groupKeyChunk2 = new Chunk(IntegerBlock.of(1, 1, 2, 2, 3, 3));
        Chunk inputChunk2 = new Chunk(
            IntegerBlock.of(1, 1, 2, 2, 3, 3),
            createDateBlock("2023-01-15", "2023-01-25", "2023-02-15", "2023-02-25", "2023-03-01", "2023-03-15"),
            // First sort column: Date
            LongBlock.wrap(new long[] {150L, 250L, 450L, 550L, 700L, 800L}) // Second sort column: Long
        );
        heap.addChunk(groupKeyChunk2, inputChunk2);

        // Build results
        Iterator<Chunk> resultIterator = heap.buildChunks();
        List<Chunk> results = new ArrayList<>();
        while (resultIterator.hasNext()) {
            results.add(resultIterator.next());
        }

        // Verify results - should get top 2 from each group across all chunks
        // Group 1: (2023-01-25, 250L), (2023-01-15, 150L)
        // Group 2: (2023-02-25, 550L), (2023-02-15, 450L)
        // Group 3: (2023-03-15, 800L), (2023-03-01, 700L)
        Assert.assertEquals(1, results.size());
        Chunk result = results.get(0);
        Assert.assertEquals(6, result.getPositionCount());

        // Group 1 top 2
        Assert.assertEquals(1, result.getBlock(0).getInt(0));
        Assert.assertEquals(Date.valueOf("2023-01-25"), result.getBlock(1).getDate(0));
        Assert.assertEquals(250L, result.getBlock(2).getLong(0));

        Assert.assertEquals(1, result.getBlock(0).getInt(1));
        Assert.assertEquals(Date.valueOf("2023-01-15"), result.getBlock(1).getDate(1));
        Assert.assertEquals(150L, result.getBlock(2).getLong(1));

        // Group 2 top 2
        Assert.assertEquals(2, result.getBlock(0).getInt(2));
        Assert.assertEquals(Date.valueOf("2023-02-25"), result.getBlock(1).getDate(2));
        Assert.assertEquals(550L, result.getBlock(2).getLong(2));

        Assert.assertEquals(2, result.getBlock(0).getInt(3));
        Assert.assertEquals(Date.valueOf("2023-02-15"), result.getBlock(1).getDate(3));
        Assert.assertEquals(450L, result.getBlock(2).getLong(3));

        // Group 3 top 2
        Assert.assertEquals(3, result.getBlock(0).getInt(4));
        Assert.assertEquals(Date.valueOf("2023-03-15"), result.getBlock(1).getDate(4));
        Assert.assertEquals(800L, result.getBlock(2).getLong(4));

        Assert.assertEquals(3, result.getBlock(0).getInt(5));
        Assert.assertEquals(Date.valueOf("2023-03-01"), result.getBlock(1).getDate(5));
        Assert.assertEquals(700L, result.getBlock(2).getLong(5));

        heap.close();
    }

    @Test
    public void testEstimateSize() {
        // Test estimateSize method with Date and String columns (two different types)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0);
        RelCollation innerCollation = createCollation(1, 2); // Sort by Date and String columns
        long fetchValue = 2;

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, 1024, context, memoryAllocator,
            CHUNK_LIMIT);

        // Initial size should be greater than 0
        long initialSize = heap.estimateSize();
        Assert.assertTrue("Initial size should be greater than 0", initialSize > 0);

        // Add some data
        Chunk groupKeyChunk = new Chunk(IntegerBlock.of(1, 1, 2, 2));
        Chunk inputChunk = new Chunk(
            IntegerBlock.of(1, 1, 2, 2),
            createDateBlock("2023-01-01", "2023-01-02", "2023-02-01", "2023-02-02"),
            createStringBlock("Alice", "Bob", "Charlie", "David")
        );
        heap.addChunk(groupKeyChunk, inputChunk);

        // Size should increase after adding data
        long sizeAfterAdd = heap.estimateSize();
        Assert.assertTrue("Size should increase after adding data", sizeAfterAdd > initialSize);

        heap.close();
    }

    @Test
    public void testLargeDataSet() {
        // Test with larger dataset to verify heap behavior, Date and String columns (two different types)
        final int groupCount = 10;
        final int itemsPerGroup = 100;
        final int totalItems = groupCount * itemsPerGroup;

        // Generate test data
        int[] groupKeys = new int[totalItems];
        String[] dateValues = new String[totalItems];
        String[] stringValues = new String[totalItems];

        for (int group = 1; group <= groupCount; group++) {
            for (int item = 0; item < itemsPerGroup; item++) {
                int index = (group - 1) * itemsPerGroup + item;
                groupKeys[index] = group;
                dateValues[index] = String.format("2023-%02d-%02d", group, (item % 28) + 1);
                stringValues[index] = "Item" + group + "_" + item;
            }
        }

        Chunk groupKeyChunk = new Chunk(IntegerBlock.wrap(groupKeys));
        Chunk inputChunk = new Chunk(
            IntegerBlock.wrap(groupKeys),
            createDateBlock(dateValues), // First sort column: Date
            createStringBlock(stringValues) // Second sort column: String
        );

        // Create DateGroupTopNHeap with two-column sorting (Date + String)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.StringType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and String columns descending
        long fetchValue = 5; // Take top 5 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
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
    public void testRehashLogic() {
        // Test rehash logic by using small estimateHashTableSize and creating enough groups to trigger rehash
        // With DEFAULT_LOAD_FACTOR = 0.75, estimateHashTableSize = 4 should result in:
        // n = HashCommon.arraySize(4, 0.75) ≈ 8, maxFill = HashCommon.maxFill(8, 0.75) ≈ 6
        // So creating 8+ groups should trigger rehash
        final int smallHashTableSize = 4;
        final int groupCount = 10; // This should trigger rehash since it exceeds maxFill
        final int itemsPerGroup = 2;

        // Create test data with multiple groups using Integer and Date+Long columns (two different types)
        List<Integer> groupKeys = new ArrayList<>();
        List<String> dateValues = new ArrayList<>();
        List<Long> longValues = new ArrayList<>();

        for (int group = 1; group <= groupCount; group++) {
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(group);
                dateValues.add(String.format("2023-%02d-%02d", group, (item % 28) + 1)); // Date values
                longValues.add((long) (group * 1000 + item)); // Long values
            }
        }

        Chunk groupKeyChunk = new Chunk(IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()));
        Chunk inputChunk = new Chunk(
            IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
            createDateBlock(dateValues.toArray(new String[0])), // First sort column: Date
            LongBlock.wrap(longValues.stream().mapToLong(i -> i).toArray()) // Second sort column: Long
        );

        // Create DateGroupTopNHeap with small hash table size to force rehash, two-column sorting (Date + Long)
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.LongType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and Long columns descending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, smallHashTableSize, context,
            memoryAllocator,
            CHUNK_LIMIT);

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
        // Group 1: highest Date+Long should be (2023-01-02, 1001L), Group 2: (2023-02-02, 2001L), etc.
        for (int i = 0; i < result.getPositionCount(); i++) {
            int groupId = result.getBlock(0).getInt(i);
            String expectedDateValue = String.format("2023-%02d-02", groupId); // Highest date for each group
            long expectedLongValue = groupId * 1000L + 1; // Highest long value for each group

            Assert.assertEquals("Date value should be correct for group " + groupId,
                Date.valueOf(expectedDateValue), result.getBlock(1).getDate(i));
            Assert.assertEquals("Long value should be correct for group " + groupId,
                expectedLongValue, result.getBlock(2).getLong(i));
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
        List<String> dateValues = new ArrayList<>();
        List<Long> longValues = new ArrayList<>();

        for (int i = 0; i < groupCount; i++) {
            int groupKey = strategicGroupKeys[i];
            for (int item = 0; item < itemsPerGroup; item++) {
                groupKeys.add(groupKey);
                // Create date values based on group key to ensure uniqueness
                int month = (groupKey % 12) + 1;
                int day = ((groupKey / 12) % 28) + 1;
                dateValues.add(String.format("2023-%02d-%02d", month, day));
                longValues.add((long) (groupKey * 100 + item)); // Long values
            }
        }

        Chunk groupKeyChunk = new Chunk(IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()));
        Chunk inputChunk = new Chunk(
            IntegerBlock.wrap(groupKeys.stream().mapToInt(i -> i).toArray()),
            createDateBlock(dateValues.toArray(new String[0])), // First sort column: Date
            LongBlock.wrap(longValues.stream().mapToLong(i -> i).toArray()) // Second sort column: Long
        );

        // Create DateGroupTopNHeap with very small hash table size to guarantee hash collisions and linear probing
        DataType[] groupKeyTypes = {DataTypes.IntegerType};
        DataType[] inputTypes = {DataTypes.IntegerType, DataTypes.DateType, DataTypes.LongType};
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        RelCollation innerCollation =
            createCollation(RelFieldCollation.Direction.DESCENDING, 1, 2); // Sort by Date and Long columns descending
        long fetchValue = 1; // Take top 1 from each group

        MemoryPool memoryPool = MemoryPoolUtils.createOperatorTmpTablePool("group-top-n", context.getMemoryPool());
        OperatorMemoryAllocatorCtx memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);

        DateGroupTopNHeap heap = new DateGroupTopNHeap(
            groupKeyTypes, inputTypes, groupSet, innerCollation, fetchValue, verySmallHashTableSize, context,
            memoryAllocator,
            CHUNK_LIMIT);

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
            long expectedLongValue = groupId * 100L; // Value for each group

            // Verify date value based on group key
            int expectedMonth = (groupId % 12) + 1;
            int expectedDay = ((groupId / 12) % 28) + 1;
            String expectedDateValue = String.format("2023-%02d-%02d", expectedMonth, expectedDay);

            Assert.assertEquals("Date value should be correct for group " + groupId + " after hash collision handling",
                Date.valueOf(expectedDateValue), result.getBlock(1).getDate(i));
            Assert.assertEquals("Long value should be correct for group " + groupId + " after hash collision handling",
                expectedLongValue, result.getBlock(2).getLong(i));
        }

        heap.close();
    }
}
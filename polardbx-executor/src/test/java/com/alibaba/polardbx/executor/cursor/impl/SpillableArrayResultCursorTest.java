package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.executor.operator.spill.MemorySpillerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SpillableArrayResultCursorTest {

    private static final long LARGE_LIMIT = 256 * 1024 * 1024L;

    private SpillableArrayResultCursor createCursor(MemorySpillerFactory spillerFactory, long spillMemoryLimit) {
        SpillableArrayResultCursor cursor =
            new SpillableArrayResultCursor("test", spillerFactory, spillMemoryLimit);
        cursor.addColumn("file_name", DataTypes.StringType);
        cursor.addColumn("file_length", DataTypes.LongType);
        cursor.addColumn("file_type", DataTypes.IntegerType);
        cursor.initMeta();
        return cursor;
    }

    @Test
    public void testNoSpillWhenUnderLimit() {
        MemorySpillerFactory spillerFactory = new MemorySpillerFactory();
        SpillableArrayResultCursor cursor = createCursor(spillerFactory, LARGE_LIMIT);
        for (int i = 0; i < 10; i++) {
            cursor.addRow(new Object[] {"file_" + i, (long) i, i % 2});
        }
        cursor.completeWrite();

        Assert.assertFalse(cursor.isSpilled());
        Assert.assertEquals(0, spillerFactory.getSpillsCount());
        for (int i = 0; i < 10; i++) {
            Row row = cursor.next();
            Assert.assertNotNull(row);
            Assert.assertEquals("file_" + i, String.valueOf(row.getObject(0)));
            Assert.assertEquals(i, ((Number) row.getObject(1)).longValue());
            Assert.assertEquals(i % 2, ((Number) row.getObject(2)).intValue());
        }
        Assert.assertNull(cursor.next());
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testSpillWhenExceedLimit() {
        MemorySpillerFactory spillerFactory = new MemorySpillerFactory();
        // 阈值1字节，每次addRow都会触发spill
        SpillableArrayResultCursor cursor = createCursor(spillerFactory, 1);
        int rowCount = 2000;
        for (int i = 0; i < rowCount; i++) {
            cursor.addRow(new Object[] {"file_" + i, (long) i, 1});
        }
        cursor.completeWrite();

        Assert.assertTrue(cursor.isSpilled());
        Assert.assertTrue(spillerFactory.getSpillsCount() > 0);
        // 读取时保持写入顺序
        for (int i = 0; i < rowCount; i++) {
            Row row = cursor.next();
            Assert.assertNotNull("Row " + i + " should not be null", row);
            Assert.assertEquals("file_" + i, String.valueOf(row.getObject(0)));
            Assert.assertEquals(i, ((Number) row.getObject(1)).longValue());
        }
        Assert.assertNull(cursor.next());
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testSpillOnlyPartialRows() {
        MemorySpillerFactory spillerFactory = new MemorySpillerFactory();
        // 阈值较大，先缓存一部分行后手动降低触发点：用小阈值验证剩余行在completeWrite时全部spill
        SpillableArrayResultCursor cursor = createCursor(spillerFactory, 500);
        int rowCount = 100;
        for (int i = 0; i < rowCount; i++) {
            cursor.addRow(new Object[] {"file_" + i, (long) i, 0});
        }
        cursor.completeWrite();

        Assert.assertTrue(cursor.isSpilled());
        int count = 0;
        Row row;
        while ((row = cursor.next()) != null) {
            Assert.assertEquals("file_" + count, String.valueOf(row.getObject(0)));
            count++;
        }
        Assert.assertEquals(rowCount, count);
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testNullSpillerFactoryNeverSpills() {
        SpillableArrayResultCursor cursor = createCursor(null, 1);
        for (int i = 0; i < 100; i++) {
            cursor.addRow(new Object[] {"file_" + i, (long) i, 0});
        }
        cursor.completeWrite();

        Assert.assertFalse(cursor.isSpilled());
        int count = 0;
        while (cursor.next() != null) {
            count++;
        }
        Assert.assertEquals(100, count);
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testAddRowAfterCompleteWriteThrows() {
        SpillableArrayResultCursor cursor = createCursor(null, LARGE_LIMIT);
        cursor.addRow(new Object[] {"file_0", 0L, 0});
        cursor.completeWrite();
        try {
            cursor.addRow(new Object[] {"file_1", 1L, 0});
            Assert.fail("Should throw IllegalStateException after completeWrite");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("completed"));
        }
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testNextWithoutCompleteWrite() {
        SpillableArrayResultCursor cursor = createCursor(null, LARGE_LIMIT);
        cursor.addRow(new Object[] {"file_0", 0L, 0});
        // 未显式completeWrite，next()自动完成写入
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("file_0", String.valueOf(row.getObject(0)));
        Assert.assertNull(cursor.next());
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testCloseReleasesResources() {
        MemorySpillerFactory spillerFactory = new MemorySpillerFactory();
        SpillableArrayResultCursor cursor = createCursor(spillerFactory, 1);
        for (int i = 0; i < 10; i++) {
            cursor.addRow(new Object[] {"file_" + i, (long) i, 0});
        }
        cursor.completeWrite();
        Assert.assertTrue(cursor.isSpilled());

        List<Throwable> exceptions = cursor.close(new ArrayList<>());
        Assert.assertTrue(exceptions.isEmpty());
        // close后next返回null
        Assert.assertNull(cursor.next());
        // 重复close幂等
        Assert.assertTrue(cursor.close(new ArrayList<>()).isEmpty());
    }

    @Test
    public void testCloseWithNullExceptionList() {
        SpillableArrayResultCursor cursor = createCursor(null, LARGE_LIMIT);
        cursor.addRow(new Object[] {"file_0", 0L, 0});
        List<Throwable> exceptions = cursor.close(null);
        Assert.assertNotNull(exceptions);
        Assert.assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testMetaAndColumns() {
        SpillableArrayResultCursor cursor = createCursor(null, LARGE_LIMIT);
        Assert.assertEquals("test", cursor.getTableName());
        Assert.assertNotNull(cursor.getCursorMeta());
        Assert.assertEquals(3, cursor.getReturnColumns().size());
        Assert.assertEquals("FILE_NAME", cursor.getReturnColumns().get(0).getName());
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testEmptyCursor() {
        SpillableArrayResultCursor cursor = createCursor(new MemorySpillerFactory(), LARGE_LIMIT);
        cursor.completeWrite();
        Assert.assertNull(cursor.next());
        Assert.assertFalse(cursor.isSpilled());
        cursor.close(new ArrayList<>());
    }

    @Test
    public void testNullValueInRow() {
        SpillableArrayResultCursor cursor = createCursor(new MemorySpillerFactory(), 1);
        cursor.addRow(new Object[] {null, 1L, 0});
        cursor.completeWrite();
        Assert.assertTrue(cursor.isSpilled());
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertNull(row.getObject(0));
        cursor.close(new ArrayList<>());
    }
}

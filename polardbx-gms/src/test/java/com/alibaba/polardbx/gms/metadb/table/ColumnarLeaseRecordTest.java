package com.alibaba.polardbx.gms.metadb.table;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class ColumnarLeaseRecordTest {

    private ResultSet mockResultSet;

    @Before
    public void setUp() {
        // 初始化 mock 的 ResultSet
        mockResultSet = Mockito.mock(ResultSet.class);
    }

    @After
    public void tearDown() {
        mockResultSet = null;
    }

    /**
     * TC01: 正常情况 - ResultSet 包含合法数据
     */
    @Test
    public void testFill_WithValidData() throws SQLException {
        // 准备测试数据
        when(mockResultSet.getInt("id")).thenReturn(1);
        when(mockResultSet.getString("owner")).thenReturn("test_owner");
        when(mockResultSet.getLong("lease")).thenReturn(123456789L);
        when(mockResultSet.getLong("fencing_token")).thenReturn(987654321L);

        // 执行测试
        ColumnarLeaseRecord record = new ColumnarLeaseRecord();
        ColumnarLeaseRecord result = record.fill(mockResultSet);

        // 验证结果
        assertNotNull(result);
        assertEquals(record, result); // 验证返回的是 this
        assertEquals(1, result.id);
        assertEquals("test_owner", result.owner);
        assertEquals(123456789L, result.lease);
        assertEquals(987654321L, result.fencingToken);
    }

    /**
     * TC02: 异常情况 - ResultSet 缺少字段 "owner"
     */
    @Test(expected = SQLException.class)
    public void testFill_MissingField_Owner() throws SQLException {
        // 准备测试数据：故意让 getString("owner") 抛出异常
        when(mockResultSet.getInt("id")).thenReturn(1);
        when(mockResultSet.getString("owner")).thenThrow(new SQLException("Field not found"));

        // 执行测试（期望抛出异常）
        ColumnarLeaseRecord record = new ColumnarLeaseRecord();
        record.fill(mockResultSet);
    }

    /**
     * TC03: 异常情况 - 字段类型错误，例如 getInt("id") 实际上是字符串
     */
    @Test(expected = SQLException.class)
    public void testFill_FieldTypeError_Id() throws SQLException {
        // 准备测试数据：getString("id") 返回字符串，getInt 应该失败
        when(mockResultSet.getInt("id")).thenThrow(new SQLException("Invalid type for column id"));

        // 执行测试（期望抛出异常）
        ColumnarLeaseRecord record = new ColumnarLeaseRecord();
        record.fill(mockResultSet);
    }
}

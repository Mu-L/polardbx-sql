package com.alibaba.polardbx.gms.topology;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.mockito.Mockito.when;

/**
 * SubInstConfigRecord单元测试
 *
 * @author assistant
 */
public class SubInstConfigRecordTest {

    @Mock
    private ResultSet resultSet;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * 测试fill方法 - 正常场景
     */
    @Test
    public void testFill_Success() throws SQLException {
        // 准备测试数据
        long expectedId = 12345L;
        Timestamp expectedGmtCreated = new Timestamp(System.currentTimeMillis() - 10000);
        Timestamp expectedGmtModified = new Timestamp(System.currentTimeMillis());
        String expectedInstId = "polardbx-inst-001";
        String expectedSubInstId = "sub-inst-001";
        String expectedParamKey = "max_connections";
        String expectedParamVal = "1000";

        // Mock ResultSet行为
        when(resultSet.getLong("id")).thenReturn(expectedId);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(expectedGmtCreated);
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(expectedGmtModified);
        when(resultSet.getString("inst_id")).thenReturn(expectedInstId);
        when(resultSet.getString("sub_inst_id")).thenReturn(expectedSubInstId);
        when(resultSet.getString("param_key")).thenReturn(expectedParamKey);
        when(resultSet.getString("param_val")).thenReturn(expectedParamVal);

        // 执行测试
        SubInstConfigRecord record = new SubInstConfigRecord();
        SubInstConfigRecord result = record.fill(resultSet);

        // 验证结果
        Assert.assertNotNull(result);
        Assert.assertEquals(expectedId, result.id);
        Assert.assertEquals(expectedGmtCreated, result.gmtCreated);
        Assert.assertEquals(expectedGmtModified, result.gmtModified);
        Assert.assertEquals(expectedInstId, result.instId);
        Assert.assertEquals(expectedSubInstId, result.subInstId);
        Assert.assertEquals(expectedParamKey, result.paramKey);
        Assert.assertEquals(expectedParamVal, result.paramVal);
        Assert.assertSame(record, result); // 验证返回的是同一个对象
    }

    /**
     * 测试fill方法 - null值处理
     */
    @Test
    public void testFill_WithNullValues() throws SQLException {
        // Mock ResultSet行为 - 某些字段为null
        when(resultSet.getLong("id")).thenReturn(1L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(null);
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(null);
        when(resultSet.getString("inst_id")).thenReturn("inst-001");
        when(resultSet.getString("sub_inst_id")).thenReturn(null);
        when(resultSet.getString("param_key")).thenReturn("key1");
        when(resultSet.getString("param_val")).thenReturn(null);

        // 执行测试
        SubInstConfigRecord record = new SubInstConfigRecord();
        SubInstConfigRecord result = record.fill(resultSet);

        // 验证结果
        Assert.assertNotNull(result);
        Assert.assertEquals(1L, result.id);
        Assert.assertNull(result.gmtCreated);
        Assert.assertNull(result.gmtModified);
        Assert.assertEquals("inst-001", result.instId);
        Assert.assertNull(result.subInstId);
        Assert.assertEquals("key1", result.paramKey);
        Assert.assertNull(result.paramVal);
    }

    /**
     * 测试fill方法 - 空字符串处理
     */
    @Test
    public void testFill_WithEmptyStrings() throws SQLException {
        // Mock ResultSet行为 - 字符串字段为空
        when(resultSet.getLong("id")).thenReturn(999L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getString("inst_id")).thenReturn("");
        when(resultSet.getString("sub_inst_id")).thenReturn("");
        when(resultSet.getString("param_key")).thenReturn("");
        when(resultSet.getString("param_val")).thenReturn("");

        // 执行测试
        SubInstConfigRecord record = new SubInstConfigRecord();
        SubInstConfigRecord result = record.fill(resultSet);

        // 验证结果
        Assert.assertNotNull(result);
        Assert.assertEquals(999L, result.id);
        Assert.assertEquals("", result.instId);
        Assert.assertEquals("", result.subInstId);
        Assert.assertEquals("", result.paramKey);
        Assert.assertEquals("", result.paramVal);
    }

    /**
     * 测试fill方法 - 特殊字符处理
     */
    @Test
    public void testFill_WithSpecialCharacters() throws SQLException {
        // 准备包含特殊字符的测试数据
        String instIdWithSpecialChars = "inst-001_test.prod";
        String subInstIdWithSpecialChars = "sub_inst-001@region1";
        String paramKeyWithSpecialChars = "db.connection.pool.size";
        String paramValWithSpecialChars = "jdbc:mysql://localhost:3306/test?useSSL=true&charset=utf8mb4";

        // Mock ResultSet行为
        when(resultSet.getLong("id")).thenReturn(100L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getString("inst_id")).thenReturn(instIdWithSpecialChars);
        when(resultSet.getString("sub_inst_id")).thenReturn(subInstIdWithSpecialChars);
        when(resultSet.getString("param_key")).thenReturn(paramKeyWithSpecialChars);
        when(resultSet.getString("param_val")).thenReturn(paramValWithSpecialChars);

        // 执行测试
        SubInstConfigRecord record = new SubInstConfigRecord();
        SubInstConfigRecord result = record.fill(resultSet);

        // 验证结果
        Assert.assertNotNull(result);
        Assert.assertEquals(instIdWithSpecialChars, result.instId);
        Assert.assertEquals(subInstIdWithSpecialChars, result.subInstId);
        Assert.assertEquals(paramKeyWithSpecialChars, result.paramKey);
        Assert.assertEquals(paramValWithSpecialChars, result.paramVal);
    }

    /**
     * 测试fill方法 - 长字符串处理
     */
    @Test
    public void testFill_WithLongStrings() throws SQLException {
        // 准备长字符串测试数据
        String longParamVal = new String(new char[1000]).replace('\0', 'A');

        // Mock ResultSet行为
        when(resultSet.getLong("id")).thenReturn(200L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getString("inst_id")).thenReturn("inst-long-test");
        when(resultSet.getString("sub_inst_id")).thenReturn("sub-inst-long-test");
        when(resultSet.getString("param_key")).thenReturn("long_param_key");
        when(resultSet.getString("param_val")).thenReturn(longParamVal);

        // 执行测试
        SubInstConfigRecord record = new SubInstConfigRecord();
        SubInstConfigRecord result = record.fill(resultSet);

        // 验证结果
        Assert.assertNotNull(result);
        Assert.assertEquals(200L, result.id);
        Assert.assertEquals(longParamVal, result.paramVal);
        Assert.assertEquals(1000, result.paramVal.length());
    }

    /**
     * 测试fill方法 - 异常场景
     */
    @Test(expected = SQLException.class)
    public void testFill_SQLException() throws SQLException {
        // Mock ResultSet抛出异常
        when(resultSet.getLong("id")).thenThrow(new SQLException("Column not found"));

        // 执行测试 - 应该抛出SQLException
        SubInstConfigRecord record = new SubInstConfigRecord();
        record.fill(resultSet);
    }

    /**
     * 测试多次fill同一对象
     */
    @Test
    public void testFill_MultipleTimes() throws SQLException {
        SubInstConfigRecord record = new SubInstConfigRecord();

        // 第一次fill
        when(resultSet.getLong("id")).thenReturn(1L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(new Timestamp(100000L));
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(new Timestamp(100000L));
        when(resultSet.getString("inst_id")).thenReturn("inst-001");
        when(resultSet.getString("sub_inst_id")).thenReturn("sub-001");
        when(resultSet.getString("param_key")).thenReturn("key1");
        when(resultSet.getString("param_val")).thenReturn("val1");

        record.fill(resultSet);
        Assert.assertEquals("val1", record.paramVal);

        // 第二次fill - 使用不同的值
        when(resultSet.getLong("id")).thenReturn(2L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(new Timestamp(200000L));
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(new Timestamp(200000L));
        when(resultSet.getString("inst_id")).thenReturn("inst-002");
        when(resultSet.getString("sub_inst_id")).thenReturn("sub-002");
        when(resultSet.getString("param_key")).thenReturn("key2");
        when(resultSet.getString("param_val")).thenReturn("val2");

        record.fill(resultSet);

        // 验证第二次fill覆盖了第一次的值
        Assert.assertEquals(2L, record.id);
        Assert.assertEquals("inst-002", record.instId);
        Assert.assertEquals("sub-002", record.subInstId);
        Assert.assertEquals("key2", record.paramKey);
        Assert.assertEquals("val2", record.paramVal);
    }

    /**
     * 测试字段的默认值
     */
    @Test
    public void testDefaultValues() {
        SubInstConfigRecord record = new SubInstConfigRecord();

        // 验证新创建对象的默认值
        Assert.assertEquals(0L, record.id);
        Assert.assertNull(record.gmtCreated);
        Assert.assertNull(record.gmtModified);
        Assert.assertNull(record.instId);
        Assert.assertNull(record.subInstId);
        Assert.assertNull(record.paramKey);
        Assert.assertNull(record.paramVal);
    }

    /**
     * 测试中文字符处理
     */
    @Test
    public void testFill_WithChineseCharacters() throws SQLException {
        // 准备包含中文的测试数据
        String chineseParamKey = "数据库配置";
        String chineseParamVal = "最大连接数：1000，超时时间：30秒";

        // Mock ResultSet行为
        when(resultSet.getLong("id")).thenReturn(300L);
        when(resultSet.getTimestamp("gmt_created")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getTimestamp("gmt_modified")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(resultSet.getString("inst_id")).thenReturn("inst-cn");
        when(resultSet.getString("sub_inst_id")).thenReturn("sub-inst-cn");
        when(resultSet.getString("param_key")).thenReturn(chineseParamKey);
        when(resultSet.getString("param_val")).thenReturn(chineseParamVal);

        // 执行测试
        SubInstConfigRecord record = new SubInstConfigRecord();
        SubInstConfigRecord result = record.fill(resultSet);

        // 验证结果
        Assert.assertNotNull(result);
        Assert.assertEquals(chineseParamKey, result.paramKey);
        Assert.assertEquals(chineseParamVal, result.paramVal);
    }
}

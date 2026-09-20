package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.server.conn.InnerConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.common.cdc.CdcConstants.DDL_LOAD_STATUS_RUNNING;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DdlSqlAsyncLoaderTest {

    private DdlSqlAsyncLoader ddlSqlAsyncLoader;

    @Before
    public void setUp() {
        ddlSqlAsyncLoader = DdlSqlAsyncLoader.getInstance();
    }

    @Test
    public void testCheckIfRunningStrongly_withNullInstConfigRecord() throws SQLException {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.getGlobal(anyString()))
                .thenReturn(null);

            boolean result = DdlSqlAsyncLoader.checkIfRunningStrongly();
            assertFalse(result);
        }
    }

    @Test
    public void testCheckIfRunningStrongly_withNullParamVal() throws SQLException {
        InstConfigRecord record = new InstConfigRecord();
        record.paramVal = null;

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.getGlobal(anyString()))
                .thenReturn(record);

            boolean result = DdlSqlAsyncLoader.checkIfRunningStrongly();
            assertFalse(result);
        }
    }

    @Test
    public void testCheckIfRunningStrongly_withNonRunningStatus() throws SQLException {
        InstConfigRecord record = new InstConfigRecord();
        record.paramVal = "STOPPED";

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.getGlobal(anyString()))
                .thenReturn(record);

            boolean result = DdlSqlAsyncLoader.checkIfRunningStrongly();
            assertFalse(result);
        }
    }

    @Test
    public void testCheckIfRunningStrongly_withRunningStatus() throws SQLException {
        InstConfigRecord record = new InstConfigRecord();
        record.paramVal = DDL_LOAD_STATUS_RUNNING;

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.getGlobal(anyString()))
                .thenReturn(record);

            boolean result = DdlSqlAsyncLoader.checkIfRunningStrongly();
            assertTrue(result);
        }
    }

    @Test
    public void testLoadWithCheckPointZeroAndNotRunningStrongly()
        throws SQLException, InterruptedException, TimeoutException {
        // 设置 paramManager 并模拟 checkIfRunningStrongly 返回 false
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getInt(any())).thenReturn(10); // 模拟批处理大小

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<CdcTableUtil> cdcTableUtilMockedStatic = mockStatic(CdcTableUtil.class)) {

            // 模拟 checkIfRunningStrongly 返回 false
            try (MockedStatic<DdlSqlAsyncLoader> loaderMockedStatic = mockStatic(DdlSqlAsyncLoader.class)) {
                loaderMockedStatic.when(DdlSqlAsyncLoader::checkIfRunningStrongly)
                    .thenReturn(false);

                // 调用 load 方法
                int result = ddlSqlAsyncLoader.load(0L);

                // 验证结果
                assertEquals(0, result);
            }
        }
    }

    @Test
    public void testLoadWithValidCheckPoint() throws SQLException, InterruptedException, TimeoutException {
        // 创建 mock 数据
        List<CdcDdlRecord> mockRecords = new ArrayList<>();
        CdcDdlRecord record = new CdcDdlRecord(1, 1, "x", "x", "x", null, "x", "x", 0, "x");
        mockRecords.add(record);

        // 设置 paramManager
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getInt(any())).thenReturn(10); // 模拟批处理大小
        when(paramManagerMock.getBoolean(any())).thenReturn(false); // 默认不启用其他功能

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);
        DdlSqlAsyncLoader spyDdlSqlAsyncLoader = spy(ddlSqlAsyncLoader);

        doNothing().when(spyDdlSqlAsyncLoader).waitAlign(anyLong());
        doNothing().when(spyDdlSqlAsyncLoader).executeDdlSql(any());
        doNothing().when(spyDdlSqlAsyncLoader).tryInjectDuplicateTrouble(any());

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<CdcTableUtil> cdcTableUtilMockedStatic = mockStatic(CdcTableUtil.class);
            MockedConstruction<InnerConnection> mocked = mockConstruction(InnerConnection.class)) {

            CdcTableUtil mockCdcTableUtil = mock(CdcTableUtil.class);
            when(mockCdcTableUtil.queryDdlRecordLargeThanId(any(Connection.class), anyLong(), anyInt())).thenReturn(
                mockRecords);
            // 模拟 queryDdlRecordLargeThanId 返回记录
            cdcTableUtilMockedStatic.when(CdcTableUtil::getInstance).thenReturn(mockCdcTableUtil);

            // 模拟 upsertDdlLoadCheckPoint 不抛出异常
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.upsertDdlLoadCheckPoint(anyLong()))
                .thenAnswer(invocation -> null);

            // 调用 load 方法
            int result = spyDdlSqlAsyncLoader.load(100L);

            // 验证结果
            assertEquals(1, result);
        }
    }

    @Test
    public void testTryRewriteDdlSqlWithShadowModeEnable() {
        // 创建 mock 参数管理器
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(true);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_schema.test_table (id INT)",
            "", 1, ""
        );

        String sql = "CREATE TABLE test_schema.test_table (id INT)";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 schema 被替换并且 hints 被添加
        assertTrue(result.contains("test_schema_gdn_shadow"));
        assertTrue(result.startsWith(hints));
    }

    @Test
    public void testTryRewriteDdlSqlCreateDatabase() {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(false);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_DATABASE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE DATABASE test_db",
            "", 1, ""
        );

        String sql = "CREATE DATABASE test_db";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 dryrun 被设置为 true 并且 IF NOT EXISTS 被添加
        assertTrue(StringUtils.containsIgnoreCase(result, "DRYRUN = TRUE"));
        assertTrue(StringUtils.containsIgnoreCase(result, "IF NOT EXISTS"));
    }

    @Test
    public void testTryRewriteDdlSqlDropDatabase() {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(false);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "DROP_DATABASE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "DROP DATABASE test_db",
            "", 1, ""
        );

        String sql = "DROP DATABASE test_db";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 dryrun 被设置为 true 并且 IF EXISTS 被添加
        assertTrue(StringUtils.containsIgnoreCase(result, "DRYRUN = TRUE"));
        assertTrue(StringUtils.containsIgnoreCase(result, "IF EXISTS"));
    }

    @Test
    public void testTryRewriteDdlSqlCreateUser() {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(false);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_USER", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE USER 'test_user'@'localhost'",
            "", 1, ""
        );

        String sql = "CREATE USER 'test_user'@'localhost'";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 IF NOT EXISTS 被添加
        assertTrue(result.contains("IF NOT EXISTS"));
    }

    @Test
    public void testTryRewriteDdlSqlCreateRole() {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(false);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_ROLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE ROLE test_role",
            "", 1, ""
        );

        String sql = "CREATE ROLE test_role";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 IF NOT EXISTS 被添加
        assertTrue(StringUtils.containsIgnoreCase(result, "IF NOT EXISTS"));
    }

    @Test
    public void testTryRewriteDdlSqlWithoutHints() {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(false);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("CREATE_TABLE");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_table (id INT)",
            "", 1, ""
        );

        String sql = "CREATE TABLE test_table (id INT)";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 hints 没有被添加，因为 CREATE_TABLE 在 withoutHintsSqlKinds 列表中
        assertFalse(result.startsWith(hints));
        assertEquals(sql, result);
    }

    @Test
    public void testTryRewriteDdlSqlWithNormalDdl() {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)).thenReturn(false);
        when(paramManagerMock.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS)).thenReturn("");

        ddlSqlAsyncLoader.setParamManager(paramManagerMock);

        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_table (id INT)",
            "", 1, ""
        );

        String sql = "CREATE TABLE test_table (id INT)";
        String hints = "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_SQL_ID=1)*/";

        String result = ddlSqlAsyncLoader.tryRewriteDdlSql(record, sql, hints);

        // 验证 hints 被正常添加
        assertTrue(result.startsWith(hints));
    }

    @Test
    public void testIsDuplicateRevokeRoleWithMatchingConditions() {
        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "REVOKE_ROLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "REVOKE ROLE test_role FROM test_user",
            "", 1, ""
        );

        // 创建带有 ERR_ROLE_NOT_GRANTED 错误信息的 SQLException
        SQLException sqlException = new SQLException("Some error with ERR_ROLE_NOT_GRANTED in message");

        // 调用 isDuplicateRevokeRole 方法
        boolean result = ddlSqlAsyncLoader.isDuplicateRevokeRole(record, sqlException);

        // 验证结果为 true
        assertTrue(result);
    }

    @Test
    public void testIsDuplicateRevokeRoleWithNonRevokeRoleSqlKind() {
        // 创建测试用的 CdcDdlRecord，sqlKind 不是 REVOKE_ROLE
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_table (id INT)",
            "", 1, ""
        );

        // 创建带有 ERR_ROLE_NOT_GRANTED 错误信息的 SQLException
        SQLException sqlException = new SQLException("Some error with ERR_ROLE_NOT_GRANTED in message");

        // 调用 isDuplicateRevokeRole 方法
        boolean result = ddlSqlAsyncLoader.isDuplicateRevokeRole(record, sqlException);

        // 验证结果为 false，因为 sqlKind 不匹配
        assertFalse(result);
    }

    @Test
    public void testIsDuplicateRevokeRoleWithNonMatchingErrorMessage() {
        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "REVOKE_ROLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "REVOKE ROLE test_role FROM test_user",
            "", 1, ""
        );

        // 创建不包含 ERR_ROLE_NOT_GRANTED 错误信息的 SQLException
        SQLException sqlException = new SQLException("Some other error message");

        // 调用 isDuplicateRevokeRole 方法
        boolean result = ddlSqlAsyncLoader.isDuplicateRevokeRole(record, sqlException);

        // 验证结果为 false，因为错误信息不匹配
        assertFalse(result);
    }

    @Test
    public void testProcessExceptionWithDropDatabaseSkip() throws SQLException {
        ParamManager paramManagerMock = mock(ParamManager.class);
        when(paramManagerMock.getBoolean(ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META)).thenReturn(false);

        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_table (id INT)",
            "", 1, ""
        );

        // 创建 SQLException
        SQLException sqlException = new SQLException("Some SQL exception");

        // 创建 spy 对象来验证方法调用
        DdlSqlAsyncLoader spyDdlSqlAsyncLoader = spy(ddlSqlAsyncLoader);
        spyDdlSqlAsyncLoader.setParamManager(paramManagerMock);

        // 模拟 canSkipDdlBecauseOfDropDatabase 返回 true
        try (MockedStatic<CdcTableUtil> cdcTableUtilMockedStatic = mockStatic(CdcTableUtil.class)) {
            CdcTableUtil mockCdcTableUtil = mock(CdcTableUtil.class);
            cdcTableUtilMockedStatic.when(CdcTableUtil::getInstance).thenReturn(mockCdcTableUtil);
            when(mockCdcTableUtil.checkIfExistsDropDatabaseAfterId(anyLong(), anyString())).thenReturn(true);

            // 调用 processException 方法，应该不会抛出异常
            int result = spyDdlSqlAsyncLoader.processException(record, sqlException);

            // 验证方法正常返回，没有抛出异常
            Assert.assertEquals(1, result);
        }
    }

    @Test
    public void testProcessExceptionWithDuplicateDdlLoadMessage() throws SQLException {
        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_table (id INT)",
            "", 1, ""
        );

        // 创建包含 GDN_DUPLICATE_DDL_LOAD_MSG_PREFIX 的 SQLException
        SQLException sqlException =
            new SQLException("Some error with " + DdlConstants.GDN_DUPLICATE_DDL_LOAD_MSG_PREFIX + " in message");

        try (MockedStatic<CdcTableUtil> cdcTableUtilMockedStatic = mockStatic(CdcTableUtil.class)) {
            CdcTableUtil mockCdcTableUtil = mock(CdcTableUtil.class);
            cdcTableUtilMockedStatic.when(CdcTableUtil::getInstance).thenReturn(mockCdcTableUtil);
            when(mockCdcTableUtil.checkIfExistsDropDatabaseAfterId(anyLong(), anyString())).thenReturn(false);
            // 调用 processException 方法，应该不会抛出异常
            int result = ddlSqlAsyncLoader.processException(record, sqlException);

            // 如果没有抛出异常，则测试通过
            Assert.assertEquals(2, result);
        }
    }

    @Test
    public void testProcessExceptionWithDuplicateRevokeRole() throws SQLException {
        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "REVOKE_ROLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "REVOKE ROLE test_role FROM test_user",
            "", 1, ""
        );

        // 创建带有 ERR_ROLE_NOT_GRANTED 错误信息的 SQLException
        SQLException sqlException = new SQLException("Some error with ERR_ROLE_NOT_GRANTED in message");

        try (MockedStatic<CdcTableUtil> cdcTableUtilMockedStatic = mockStatic(CdcTableUtil.class)) {
            CdcTableUtil mockCdcTableUtil = mock(CdcTableUtil.class);
            cdcTableUtilMockedStatic.when(CdcTableUtil::getInstance).thenReturn(mockCdcTableUtil);
            when(mockCdcTableUtil.checkIfExistsDropDatabaseAfterId(anyLong(), anyString())).thenReturn(false);

            // 调用 processException 方法，应该不会抛出异常
            int result = ddlSqlAsyncLoader.processException(record, sqlException);

            // 如果没有抛出异常，则测试通过
            Assert.assertEquals(3, result);
        }
    }

    @Test
    public void testProcessExceptionWithOtherSQLException() {
        // 创建测试用的 CdcDdlRecord
        CdcDdlRecord record = new CdcDdlRecord(
            1L, 1L, "CREATE_TABLE", "test_schema", "test_table",
            new Timestamp(System.currentTimeMillis()), "CREATE TABLE test_table (id INT)",
            "", 1, ""
        );

        // 创建普通的 SQLException
        SQLException sqlException = new SQLException("Some other SQL exception");

        // 调用 processException 方法，应该抛出异常
        try (MockedStatic<CdcTableUtil> cdcTableUtilMockedStatic = mockStatic(CdcTableUtil.class)) {
            CdcTableUtil mockCdcTableUtil = mock(CdcTableUtil.class);
            cdcTableUtilMockedStatic.when(CdcTableUtil::getInstance).thenReturn(mockCdcTableUtil);
            when(mockCdcTableUtil.checkIfExistsDropDatabaseAfterId(anyLong(), anyString())).thenReturn(false);
            ddlSqlAsyncLoader.processException(record, sqlException);
            // 如果没有抛出异常，则测试失败
            fail("Expected SQLException to be thrown");
        } catch (SQLException e) {
            // 验证抛出的异常是我们预期的异常
            assertEquals("Some other SQL exception", e.getMessage());
        }
    }
}

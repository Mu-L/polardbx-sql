package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.CredentialUtil;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.apache.calcite.sql.SqlDescribeExternalCatalog;
import org.apache.calcite.sql.SqlShowCreateExternalCatalog;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Tests for privilege checks on SHOW CREATE EXTERNAL CATALOG and DESCRIBE EXTERNAL CATALOG.
 */
public class ExternalCatalogDALPrivilegeTest {

    private static final String CATALOG_NAME = "test_cat";

    private ExternalCatalogInfo mockCatalogInfo() {
        return new ExternalCatalogInfo(CATALOG_NAME, "mock", new HashMap<>(), null, "test");
    }

    private ExecutionContext mockExecutionContext(boolean isSuperUser) {
        ExecutionContext ec = mock(ExecutionContext.class);
        PrivilegeContext pc = mock(PrivilegeContext.class);
        PolarAccountInfo userInfo = mock(PolarAccountInfo.class);

        when(ec.getPrivilegeContext()).thenReturn(pc);
        when(pc.getPolarUserInfo()).thenReturn(userInfo);
        when(pc.getUser()).thenReturn("test_user");
        when(pc.getHost()).thenReturn("127.0.0.1");

        AccountType accountType = isSuperUser ? AccountType.GOD : AccountType.SSO;
        when(userInfo.getAccountType()).thenReturn(accountType);

        return ec;
    }

    // --- SHOW CREATE EXTERNAL CATALOG ---

    @Test
    public void testShowCreateAsSuperUserAllowed() {
        ExecutionContext ec = mockExecutionContext(true);
        LogicalShow logicalShow = mock(LogicalShow.class);
        SqlShowCreateExternalCatalog showNode = mock(SqlShowCreateExternalCatalog.class);
        when(logicalShow.getNativeSqlNode()).thenReturn(showNode);
        when(showNode.getCatalogName()).thenReturn(CATALOG_NAME);

        try (MockedStatic<ExternalCatalogManager> mockedEcm = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager ecm = mock(ExternalCatalogManager.class);
            mockedEcm.when(ExternalCatalogManager::getInstance).thenReturn(ecm);
            when(ecm.get(CATALOG_NAME)).thenReturn(mockCatalogInfo());

            LogicalShowCreateExternalCatalogHandler handler =
                new LogicalShowCreateExternalCatalogHandler(mock(IRepository.class));
            Cursor cursor = handler.handle(logicalShow, ec);
            Assert.assertNotNull(cursor);
        }
    }

    @Test
    public void testShowCreateEscapesStringLiterals() {
        ExecutionContext ec = mockExecutionContext(true);
        LogicalShow logicalShow = mock(LogicalShow.class);
        SqlShowCreateExternalCatalog showNode = mock(SqlShowCreateExternalCatalog.class);
        when(logicalShow.getNativeSqlNode()).thenReturn(showNode);
        when(showNode.getCatalogName()).thenReturn(CATALOG_NAME);

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("path'key", "oss://bucket/a'b");
        ExternalCatalogInfo info =
            new ExternalCatalogInfo(CATALOG_NAME, "mock", properties, "sec'ret", "owner's catalog");

        try (MockedStatic<ExternalCatalogManager> mockedEcm = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager ecm = mock(ExternalCatalogManager.class);
            mockedEcm.when(ExternalCatalogManager::getInstance).thenReturn(ecm);
            when(ecm.get(CATALOG_NAME)).thenReturn(info);

            LogicalShowCreateExternalCatalogHandler handler =
                new LogicalShowCreateExternalCatalogHandler(mock(IRepository.class));
            Cursor cursor = handler.handle(logicalShow, ec);
            String createStatement = cursor.next().getString(1);

            Assert.assertTrue(createStatement.contains("COMMENT 'owner''s catalog'"));
            Assert.assertTrue(createStatement.contains("'path''key'='oss://bucket/a''b'"));
            Assert.assertTrue(createStatement.contains("'secret'='sec''ret'"));
            Assert.assertFalse(createStatement.contains("owner's catalog"));
            Assert.assertFalse(createStatement.contains("oss://bucket/a'b"));
            Assert.assertFalse(createStatement.contains("sec'ret"));
        }
    }

    @Test
    public void testShowCreateOutputCanBeReparsed() {
        ExecutionContext ec = mockExecutionContext(true);
        LogicalShow logicalShow = mock(LogicalShow.class);
        SqlShowCreateExternalCatalog showNode = mock(SqlShowCreateExternalCatalog.class);
        when(logicalShow.getNativeSqlNode()).thenReturn(showNode);
        when(showNode.getCatalogName()).thenReturn(CATALOG_NAME);

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("path", "C:\\Users\\admin\\data");
        properties.put("note", "path\\to\\owner's file");
        properties.put("access_key_secret", "rawSecretValue");
        ExternalCatalogInfo info =
            new ExternalCatalogInfo(CATALOG_NAME, "mock", properties, "sec_ret", "owner's \\ catalog");

        try (MockedStatic<ExternalCatalogManager> mockedEcm = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager ecm = mock(ExternalCatalogManager.class);
            mockedEcm.when(ExternalCatalogManager::getInstance).thenReturn(ecm);
            when(ecm.get(CATALOG_NAME)).thenReturn(info);

            LogicalShowCreateExternalCatalogHandler handler =
                new LogicalShowCreateExternalCatalogHandler(mock(IRepository.class));
            String createStatement = handler.handle(logicalShow, ec).next().getString(1);

            Assert.assertFalse("credential must not be exposed: " + createStatement,
                createStatement.contains("rawSecretValue"));

            MySqlCreateExternalCatalogStatement parsed = (MySqlCreateExternalCatalogStatement)
                new MySqlStatementParser(createStatement).parseStatement();
            Assert.assertEquals(CATALOG_NAME, parsed.getName().getSimpleName());
            Assert.assertEquals("owner's \\ catalog", parsed.getComment());
            Assert.assertEquals("mock", parsed.getProperties().get("connector"));
            Assert.assertEquals("C:\\Users\\admin\\data", parsed.getProperties().get("path"));
            Assert.assertEquals("path\\to\\owner's file", parsed.getProperties().get("note"));
            Assert.assertEquals(CredentialUtil.mask("rawSecretValue"),
                parsed.getProperties().get("access_key_secret"));
            Assert.assertEquals("sec_ret", parsed.getProperties().get("secret"));
            Assert.assertEquals(Arrays.asList("connector", "path", "note", "access_key_secret", "secret"),
                new ArrayList<>(parsed.getProperties().keySet()));
        }
    }

    @Test
    public void testShowCreateAsNormalUserWithoutPrivDenied() {
        ExecutionContext ec = mockExecutionContext(false);
        LogicalShow logicalShow = mock(LogicalShow.class);
        SqlShowCreateExternalCatalog showNode = mock(SqlShowCreateExternalCatalog.class);
        when(logicalShow.getNativeSqlNode()).thenReturn(showNode);
        when(showNode.getCatalogName()).thenReturn(CATALOG_NAME);

        try (MockedStatic<PolarPrivManager> mockedPpm = mockStatic(PolarPrivManager.class)) {
            mockedPpm.when(() -> PolarPrivManager.hasAnyPrivOnCatalog(any(), eq(CATALOG_NAME)))
                .thenReturn(false);

            LogicalShowCreateExternalCatalogHandler handler =
                new LogicalShowCreateExternalCatalogHandler(mock(IRepository.class));
            try {
                handler.handle(logicalShow, ec);
                Assert.fail("Expected privilege check failure");
            } catch (TddlRuntimeException e) {
                String msg = e.getMessage();
                Assert.assertTrue("Message should contain 'does not have', got: " + msg,
                    msg.contains("does not have"));
                Assert.assertTrue("Message should contain catalog name, got: " + msg,
                    msg.contains(CATALOG_NAME));
                Assert.assertTrue("Message should contain user, got: " + msg,
                    msg.contains("test_user"));
                Assert.assertFalse("Message must not have unresolved placeholders, got: " + msg,
                    msg.contains("{1}") || msg.contains("{2}"));
            }
        }
    }

    // --- DESCRIBE EXTERNAL CATALOG ---

    @Test
    public void testDescribeAsSuperUserAllowed() {
        ExecutionContext ec = mockExecutionContext(true);
        LogicalShow logicalShow = mock(LogicalShow.class);
        SqlDescribeExternalCatalog showNode = mock(SqlDescribeExternalCatalog.class);
        when(logicalShow.getNativeSqlNode()).thenReturn(showNode);
        when(showNode.getCatalogName()).thenReturn(CATALOG_NAME);

        try (MockedStatic<ExternalCatalogManager> mockedEcm = mockStatic(ExternalCatalogManager.class)) {
            ExternalCatalogManager ecm = mock(ExternalCatalogManager.class);
            mockedEcm.when(ExternalCatalogManager::getInstance).thenReturn(ecm);
            when(ecm.get(CATALOG_NAME)).thenReturn(mockCatalogInfo());

            LogicalDescribeExternalCatalogHandler handler =
                new LogicalDescribeExternalCatalogHandler(mock(IRepository.class));
            Cursor cursor = handler.handle(logicalShow, ec);
            Assert.assertNotNull(cursor);
        }
    }

    @Test
    public void testDescribeAsNormalUserWithoutPrivDenied() {
        ExecutionContext ec = mockExecutionContext(false);
        LogicalShow logicalShow = mock(LogicalShow.class);
        SqlDescribeExternalCatalog showNode = mock(SqlDescribeExternalCatalog.class);
        when(logicalShow.getNativeSqlNode()).thenReturn(showNode);
        when(showNode.getCatalogName()).thenReturn(CATALOG_NAME);

        try (MockedStatic<PolarPrivManager> mockedPpm = mockStatic(PolarPrivManager.class)) {
            mockedPpm.when(() -> PolarPrivManager.hasAnyPrivOnCatalog(any(), eq(CATALOG_NAME)))
                .thenReturn(false);

            LogicalDescribeExternalCatalogHandler handler =
                new LogicalDescribeExternalCatalogHandler(mock(IRepository.class));
            try {
                handler.handle(logicalShow, ec);
                Assert.fail("Expected privilege check failure");
            } catch (TddlRuntimeException e) {
                String msg = e.getMessage();
                Assert.assertTrue("Message should contain 'does not have', got: " + msg,
                    msg.contains("does not have"));
                Assert.assertTrue("Message should contain catalog name, got: " + msg,
                    msg.contains(CATALOG_NAME));
                Assert.assertTrue("Message should contain user, got: " + msg,
                    msg.contains("test_user"));
                Assert.assertFalse("Message must not have unresolved placeholders, got: " + msg,
                    msg.contains("{1}") || msg.contains("{2}"));
            }
        }
    }
}

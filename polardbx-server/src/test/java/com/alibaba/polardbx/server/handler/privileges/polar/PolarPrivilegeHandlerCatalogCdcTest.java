package com.alibaba.polardbx.server.handler.privileges.polar;

import com.alibaba.polardbx.common.audit.ConnectionInfo;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Catalog privilege statements use three-part-name syntax that downstream instances may
 * not support, so they must skip CDC marking while normal privilege statements keep marking.
 */
public class PolarPrivilegeHandlerCatalogCdcTest {

    private ServerConnection mockConn;
    private PolarPrivManager mockPrivManager;
    private CdcManagerHelper mockCdcHelper;
    private PolarAccountInfo granter;

    private MockedStatic<PolarPrivManager> mockPrivManagerStatic;
    private MockedStatic<CdcManagerHelper> mockCdcHelperStatic;
    private MockedStatic<ConfigDataMode> mockConfigDataModeStatic;

    @Before
    public void setup() {
        mockConn = Mockito.mock(ServerConnection.class);
        Mockito.when(mockConn.getSchema()).thenReturn("test_db");
        ConnectionInfo mockConnectionInfo = Mockito.mock(ConnectionInfo.class);
        // Use the reserved root user so audit logging short-circuits without MetaDB.
        Mockito.when(mockConnectionInfo.getUser()).thenReturn(PolarPrivUtil.POLAR_ROOT);
        Mockito.when(mockConn.getConnectionInfo()).thenReturn(mockConnectionInfo);

        mockPrivManager = Mockito.mock(PolarPrivManager.class);
        Mockito.when(mockPrivManager.getExactUser(anyString(), anyString())).thenReturn(null);
        mockPrivManagerStatic = Mockito.mockStatic(PolarPrivManager.class);
        mockPrivManagerStatic.when(PolarPrivManager::getInstance).thenReturn(mockPrivManager);

        mockCdcHelper = Mockito.mock(CdcManagerHelper.class);
        mockCdcHelperStatic = Mockito.mockStatic(CdcManagerHelper.class);
        mockCdcHelperStatic.when(CdcManagerHelper::getInstance).thenReturn(mockCdcHelper);

        mockConfigDataModeStatic = Mockito.mockStatic(ConfigDataMode.class);
        mockConfigDataModeStatic.when(ConfigDataMode::isMasterMode).thenReturn(true);

        granter = new PolarAccountInfo(PolarAccount.newBuilder()
            .setUsername("granter")
            .setHost("%")
            .build());

        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", new HashMap<>(), null, null));
    }

    @After
    public void teardown() {
        if (mockPrivManagerStatic != null) {
            mockPrivManagerStatic.close();
        }
        if (mockCdcHelperStatic != null) {
            mockCdcHelperStatic.close();
        }
        if (mockConfigDataModeStatic != null) {
            mockConfigDataModeStatic.close();
        }
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    private void runGrant(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> stmts = parser.parseStatementList();
        SQLGrantStatement stmt = (SQLGrantStatement) stmts.get(0);
        PolarGrantPrivilegeHandler handler = new PolarGrantPrivilegeHandler(
            ByteString.from(sql), mockConn, granter, mockPrivManager, stmt);
        handler.handle(false);
    }

    private void runRevoke(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> stmts = parser.parseStatementList();
        SQLRevokeStatement stmt = (SQLRevokeStatement) stmts.get(0);
        PolarRevokePrivilegeHandler handler = new PolarRevokePrivilegeHandler(
            ByteString.from(sql), mockConn, granter, mockPrivManager, stmt);
        handler.handle(false);
    }

    private void verifyCdcMarked(int times) {
        Mockito.verify(mockCdcHelper, Mockito.times(times))
            .notifyDdlNew(anyString(), anyString(), anyString(), anyString(),
                any(), any(), any(), any(), any());
    }

    @Test
    public void testGrantOnCatalogDbSkipsCdcMark() {
        runGrant("grant select on hive.db1.* to 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testGrantOnCatalogTableSkipsCdcMark() {
        runGrant("grant select on hive.db1.t1 to 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testGrantOnNormalDbMarksCdc() {
        runGrant("grant select on db1.* to 'u1'@'%'");
        verifyCdcMarked(1);
    }

    @Test
    public void testRevokeOnCatalogDbSkipsCdcMark() {
        runRevoke("revoke select on hive.db1.* from 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testRevokeOnCatalogTableSkipsCdcMark() {
        runRevoke("revoke select on hive.db1.t1 from 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testRevokeOnNormalDbMarksCdc() {
        runRevoke("revoke select on db1.* from 'u1'@'%'");
        verifyCdcMarked(1);
    }

    @Test
    public void testGrantOnWildcardCatalogSkipsCdcMark() {
        runGrant("grant select on *.*.* to 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testGrantOnWildcardDbUnderCatalogSkipsCdcMark() {
        runGrant("grant select on hive.*.* to 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testRevokeOnWildcardCatalogSkipsCdcMark() {
        runRevoke("revoke select on *.*.* from 'u1'@'%'");
        verifyCdcMarked(0);
    }

    @Test
    public void testGrantOnMixedWildcardFailsWithoutCdcMark() {
        try {
            runGrant("grant select on hive.*.t1 to 'u1'@'%'");
            Assert.fail("Mixed wildcard pattern must be rejected");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Mixed wildcard pattern"));
        }
        verifyCdcMarked(0);
    }

    @Test
    public void testGrantOnCatalogDbWithGrantOptionSkipsCdcMark() {
        runGrant("grant select on hive.db1.* to 'u1'@'%' with grant option");
        verifyCdcMarked(0);
    }

    @Test
    public void testGrantOnCatalogTableWithGrantOptionSkipsCdcMark() {
        runGrant("grant select on hive.db1.t1 to 'u1'@'%' with grant option");
        verifyCdcMarked(0);
    }
}

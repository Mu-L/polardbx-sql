package com.alibaba.polardbx.server.handler.privileges.polar;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class PolarHandlerCommonThreePartTest {

    private ServerConnection mockConn;

    /**
     * getGrantees consults PolarPrivManager to reject role grantees, and its singleton
     * initialization needs a live MetaDB, which a unit test does not have.
     */
    private MockedStatic<PolarPrivManager> mockPrivManagerStatic;

    @Before
    public void setup() {
        mockConn = Mockito.mock(ServerConnection.class);
        Mockito.when(mockConn.getSchema()).thenReturn("test_db");

        PolarPrivManager mockPrivManager = Mockito.mock(PolarPrivManager.class);
        Mockito.when(mockPrivManager.getExactUser(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(null);
        mockPrivManagerStatic = Mockito.mockStatic(PolarPrivManager.class);
        mockPrivManagerStatic.when(PolarPrivManager::getInstance).thenReturn(mockPrivManager);

        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", new HashMap<>(), null, null));
    }

    @After
    public void teardown() {
        if (mockPrivManagerStatic != null) {
            mockPrivManagerStatic.close();
        }
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    private List<PolarAccountInfo> grant(String sql) throws Exception {
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statements = parser.parseStatementList();
        SQLGrantStatement stmt = (SQLGrantStatement) statements.get(0);
        return PolarHandlerCommon.getGrantees((SQLExprTableSource) stmt.getResource(),
            stmt.getUsers(), stmt.getPrivileges(), mockConn, true);
    }

    private List<PolarAccountInfo> revoke(String sql) throws Exception {
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statements = parser.parseStatementList();
        SQLRevokeStatement stmt = (SQLRevokeStatement) statements.get(0);
        return PolarHandlerCommon.getGrantees((SQLExprTableSource) stmt.getResource(),
            stmt.getUsers(), stmt.getPrivileges(), mockConn, false);
    }

    @Test
    public void testGrantOnUnknownCatalogIsRejected() {
        try {
            grant("grant select on no_such_catalog.warehouse.* to 'u'@'%'");
            fail("GRANT must reject a catalog that does not exist");
        } catch (Exception e) {
            assertEquals(true, e.getMessage() != null
                && e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testRevokeOnUnknownCatalogIsAccepted() throws Exception {
        // DROP CATALOG leaves db_priv/table_priv rows behind, so REVOKE must remain
        // usable on a dropped catalog or those grants can never be cleaned up via SQL.
        List<PolarAccountInfo> grantees =
            revoke("revoke select on no_such_catalog.warehouse.* from 'u'@'%'");
        assertEquals(1, grantees.size());
        assertNotNull(grantees.get(0).getFirstCatalogDbPriv());
        assertEquals("no_such_catalog", grantees.get(0).getFirstCatalogDbPriv().getCatalogName());
        assertEquals("warehouse", grantees.get(0).getFirstCatalogDbPriv().getDbName());
    }

    @Test
    public void testQuotedThreePartTableGrantIsNormalized() throws Exception {
        List<PolarAccountInfo> grantees =
            grant("grant select on hive.`Warehouse`.`Orders` to 'u'@'%'");
        assertEquals(1, grantees.size());
        assertNotNull(grantees.get(0).getFirstCatalogTbPriv());
        assertEquals("hive", grantees.get(0).getFirstCatalogTbPriv().getCatalogName());
        assertEquals("warehouse", grantees.get(0).getFirstCatalogTbPriv().getDbName());
        assertEquals("orders", grantees.get(0).getFirstCatalogTbPriv().getTbName());
    }

    @Test
    public void testQuotedCatalogGrantIsNormalized() throws Exception {
        List<PolarAccountInfo> grantees =
            grant("grant select on `hive`.`Warehouse`.* to 'u'@'%'");
        assertEquals(1, grantees.size());
        assertNotNull(grantees.get(0).getFirstCatalogDbPriv());
        assertEquals("hive", grantees.get(0).getFirstCatalogDbPriv().getCatalogName());
        assertEquals("warehouse", grantees.get(0).getFirstCatalogDbPriv().getDbName());
    }

    @Test
    public void testQuotedIllegalDbNameIsRejected() {
        try {
            grant("grant select on hive.`bad name`.* to 'u'@'%'");
            fail("A db name that cannot be stored safely must be rejected");
        } catch (Exception e) {
            assertEquals(true, e.getMessage() != null
                && e.getMessage().contains("External database name is invalid"));
        }
    }

    @Test
    public void testQuotedIllegalTableNameIsRejected() {
        // PolarTbPriv interpolates the table name into the table_priv insert, so the
        // character check must cover it as well as the db name.
        try {
            grant("grant select on hive.warehouse.`bad'name` to 'u'@'%'");
            fail("A table name that cannot be stored safely must be rejected");
        } catch (Exception e) {
            assertEquals(true, e.getMessage() != null
                && e.getMessage().contains("External table name is invalid"));
        }
    }

    @Test
    public void testGlobalWildcardWithConcreteTableIsRejected() {
        // A wildcard db can only carry a wildcard table: checkExternalPrivilege looks up
        // table privileges by concrete catalog/db, so '*.*.tbl' would be a dead record.
        assertMixedWildcardRejected("grant select on *.*.orders to 'u'@'%'");
    }

    @Test
    public void testCatalogWildcardDbWithConcreteTableIsRejected() {
        assertMixedWildcardRejected("grant select on hive.*.orders to 'u'@'%'");
    }

    @Test
    public void testWildcardCatalogWithConcreteDbIsRejected() {
        // The grammar already forbids a concrete db under a wildcard catalog, so this
        // never reaches the handler's mixed-wildcard check.
        try {
            grant("grant select on *.warehouse.* to 'u'@'%'");
            fail("A concrete db under a wildcard catalog must be rejected");
        } catch (Exception e) {
            assertEquals(ParserException.class, e.getClass());
        }
    }

    @Test
    public void testGlobalWildcardGrantIsAccepted() throws Exception {
        List<PolarAccountInfo> grantees = grant("grant select on *.*.* to 'u'@'%'");
        assertEquals(1, grantees.size());
        assertNotNull(grantees.get(0).getFirstCatalogDbPriv());
        assertEquals("*", grantees.get(0).getFirstCatalogDbPriv().getCatalogName());
        assertEquals("*", grantees.get(0).getFirstCatalogDbPriv().getDbName());
    }

    private void assertMixedWildcardRejected(String sql) {
        try {
            grant(sql);
            fail("Mixed wildcard pattern must be rejected: " + sql);
        } catch (Exception e) {
            assertEquals(true, e.getMessage() != null
                && e.getMessage().contains("Mixed wildcard pattern"));
        }
    }
}

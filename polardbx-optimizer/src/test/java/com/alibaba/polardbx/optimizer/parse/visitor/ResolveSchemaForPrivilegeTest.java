package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.external.connector.MockConnectorMetadata;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * UT for resolveSchemaForPrivilege encoding, native_query privilege schema,
 * and MockConnectorMetadata DML rejection.
 */
public class ResolveSchemaForPrivilegeTest {

    // === encodeSchemaName ===

    @Test
    public void testEncodeThreeSegmentSchema() {
        assertEquals("jdbc_test$$tt", ExternalNameValidator.encodeSchemaName("jdbc_test", "tt"));
    }

    @Test
    public void testEncodeCatalogWildcard() {
        String encoded = ExternalNameValidator.encodeSchemaName("jdbc_test", "*");
        assertEquals("jdbc_test$$*", encoded);
        // Note: isExternalSchema() also checks ExternalCatalogManager.isEmpty(),
        // which is true in UT env. Verify the $$ pattern directly.
        assertTrue(encoded.contains("$$"));
    }

    @Test
    public void testEncodeLowercaseNormalization() {
        String encoded = ExternalNameValidator.encodeSchemaName("JDBC_TEST", "TT");
        assertEquals("jdbc_test$$tt", encoded);
    }

    // === isExternalSchema ===

    @Test
    public void testInternalSchemaNotExternal() {
        assertFalse(ExternalNameValidator.isExternalSchema("mydb"));
        assertFalse(ExternalNameValidator.isExternalSchema(""));
    }

    @Test
    public void testNullSchemaNotExternal() {
        assertFalse(ExternalNameValidator.isExternalSchema(null));
    }

    // === splitSchemaName ===

    @Test
    public void testSplitSchemaName() {
        String[] parts = ExternalNameValidator.splitSchemaName("jdbc_test$$tt");
        assertEquals("jdbc_test", parts[0]);
        assertEquals("tt", parts[1]);
    }

    @Test
    public void testSplitWildcardSchema() {
        String[] parts = ExternalNameValidator.splitSchemaName("jdbc_test$$*");
        assertEquals("jdbc_test", parts[0]);
        assertEquals("*", parts[1]);
    }

    @Test
    public void testSplitInvalidReturnsNull() {
        assertNull(ExternalNameValidator.splitSchemaName("nodelimiter"));
    }

    // === ConnectorMetadata.inferReadOnlyQuerySchema DML rejection ===

    @Test
    public void testMockInferNativeQueryRejectsDML() {
        MockConnectorMetadata metadata = new MockConnectorMetadata(
            Collections.singletonMap("mock.native_query.columns", "v:int"));
        try {
            metadata.inferReadOnlyQuerySchema("DELETE FROM t0 WHERE id=1");
            fail("Should reject DML");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("DML/DDL"));
            assertTrue(e.getMessage().contains("DELETE"));
        }
    }

    @Test
    public void testJoinExternalTablesEncodePrivilegeSchema() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", Maps.newHashMap(), null, null));
        try {
            PrivilegeContext privilegeContext = new PrivilegeContext();
            ExecutionContext ec = new ExecutionContext("test_schema");
            ContextParameters context = new ContextParameters(false);
            context.setPrivilegeContext(privilegeContext);
            MySqlStatementParser parser = new MySqlStatementParser(
                "select * from hive.dwd.t1 a join hive.dwd.t2 b on a.id = b.id",
                SQLParserFeature.TDDLHint,
                SQLParserFeature.IgnoreNameQuotes);

            List<SQLStatement> statements = parser.parseStatementList();
            statements.get(0).accept(new FastSqlToCalciteNodeVisitor(context, ec));

            List<PrivilegeVerifyItem> items = privilegeContext.getPrivilegeVerifyItems();
            assertNotNull("JOIN should collect privilege verify items", items);
            assertEquals(2, items.size());
            assertEquals("hive$$dwd", items.get(0).getDb());
            assertEquals("t1", items.get(0).getTable());
            assertEquals("hive$$dwd", items.get(1).getDb());
            assertEquals("t2", items.get(1).getTable());
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }

    @Test
    public void testMockInferNativeQueryRejectsInsert() {
        MockConnectorMetadata metadata = new MockConnectorMetadata(
            Collections.singletonMap("mock.native_query.columns", "v:int"));
        try {
            metadata.inferReadOnlyQuerySchema("INSERT INTO t0 VALUES (1,2,3)");
            fail("Should reject DML");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("INSERT"));
        }
    }

    @Test
    public void testMockInferNativeQueryAllowsSelect() throws IOException {
        MockConnectorMetadata metadata = new MockConnectorMetadata(
            Collections.singletonMap("mock.native_query.columns", "v:int"));
        assertFalse(metadata.inferReadOnlyQuerySchema("SELECT 1 AS v").getColumns().isEmpty());
    }

    @Test
    public void testMockInferNativeQueryAllowsWith() throws IOException {
        MockConnectorMetadata metadata = new MockConnectorMetadata(
            Collections.singletonMap("mock.native_query.columns", "v:int"));
        assertFalse(
            metadata.inferReadOnlyQuerySchema("WITH cte AS (SELECT 1) SELECT * FROM cte").getColumns().isEmpty());
    }

    // ---- H1: quoted three-part names must reach the external privilege path ----

    private static List<PrivilegeVerifyItem> collectPrivilegeItems(String sql) {
        PrivilegeContext privilegeContext = new PrivilegeContext();
        ExecutionContext ec = new ExecutionContext("test_schema");
        ContextParameters context = new ContextParameters(false);
        context.setPrivilegeContext(privilegeContext);
        // IgnoreNameQuotes is deliberately NOT enabled: FastsqlUtils.DEFAULT_FEATURES
        // does not enable it either, so backticks really do reach the AST in production.
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statements = parser.parseStatementList();
        statements.get(0).accept(new FastSqlToCalciteNodeVisitor(context, ec));
        return privilegeContext.getPrivilegeVerifyItems();
    }

    @Test
    public void testQuotedThreePartNameEncodesPrivilegeSchema() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", Maps.newHashMap(), null, null));
        try {
            List<PrivilegeVerifyItem> items =
                collectPrivilegeItems("select * from `hive`.`dwd`.`t1`");
            assertNotNull(items);
            assertEquals(1, items.size());
            assertEquals("hive$$dwd", items.get(0).getDb());
            assertEquals("t1", items.get(0).getTable());
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }

    @Test
    public void testMixedCaseQuotedThreePartNameEncodesPrivilegeSchema() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", Maps.newHashMap(), null, null));
        try {
            List<PrivilegeVerifyItem> items =
                collectPrivilegeItems("select * from `HIVE`.`DWD`.`t1`");
            assertNotNull(items);
            assertEquals(1, items.size());
            assertEquals("hive$$dwd", items.get(0).getDb());
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }

    @Test
    public void testQuotedThreePartReplaceIsRejected() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", Maps.newHashMap(), null, null));
        try {
            collectPrivilegeItems("replace into `hive`.`dwd`.`t1` (id) values (1)");
            fail("REPLACE on an external table must be rejected instead of running as INSERT");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().toLowerCase().contains("replace"));
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }

    @Test
    public void testQuotedThreePartUpdateIsRejected() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("hive", "mock", Maps.newHashMap(), null, null));
        try {
            collectPrivilegeItems("update `hive`.`dwd`.`t1` set id = 1");
            fail("UPDATE on an external table must be rejected");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().toLowerCase().contains("external"));
        } finally {
            ExternalCatalogManager.getInstance().remove("hive");
        }
    }

    @Test
    public void testLocalQuotedTableNameKeepsExistingBehaviour() {
        // Only the external path normalizes the table name; the local path is left as it
        // was so that this change cannot alter existing local privilege decisions.
        List<PrivilegeVerifyItem> items = collectPrivilegeItems("select * from `mydb`.`t1`");
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals("`t1`", items.get(0).getTable());
    }
}

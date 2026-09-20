package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.MockConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.files.EphemeralFilesSchemaManager;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowConnectors;
import org.apache.calcite.sql.SqlShowExternalCatalogs;
import org.apache.calcite.sql.SqlShowSecrets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for FILES() visitor logic in FastSqlToCalciteNodeVisitor.
 * Covers key case-insensitivity, required parameter validation,
 * and ephemeral table name uniqueness.
 */
public class FilesVisitorTest {

    private ExecutionContext ec;
    private ContextParameters contextParameters;
    private boolean originalMockEnabled;
    private ConnectorDescriptor previousMockDescriptor;

    @Before
    public void setUp() {
        ec = new ExecutionContext();
        ec.setParams(new Parameters());
        contextParameters = new ContextParameters(false);

        // Always install the real mock connector: other tests may have registered a
        // different descriptor under the "mock" type (e.g. BasePlannerTest), which
        // would break native_query schema inference. Restore the previous state later.
        previousMockDescriptor = ConnectorRegistry.getInstance().getOrNull(MockConnectorDescriptor.TYPE);
        ConnectorRegistry.getInstance().register(new MockConnectorDescriptor());
        originalMockEnabled = DynamicConfig.getInstance().isEnableMockConnector();
        DynamicConfig.getInstance().loadValue(null, "ENABLE_MOCK_CONNECTOR", "true");
    }

    @After
    public void tearDown() {
        DynamicConfig.getInstance().loadValue(null, "ENABLE_MOCK_CONNECTOR",
            String.valueOf(originalMockEnabled));
        if (previousMockDescriptor != null) {
            ConnectorRegistry.getInstance().register(previousMockDescriptor);
        } else {
            ConnectorRegistry.getInstance().unregister(MockConnectorDescriptor.TYPE);
        }
    }

    private SQLStatement parse(String sql) {
        Map<Integer, ParameterContext> currentParameter = ec.getParams().getCurrentParameter();
        SqlParameterized result = SqlParameterizeUtils.parameterize(
            ByteString.from(sql), currentParameter, ec, false);
        return result.getAst();
    }

    @Test
    public void testMissingSecretThrows() {
        // No connector specified → defaults to 'files', triggers validation
        String sql = "SELECT * FROM FILES("
            + "'path'=>'/tmp/x.csv', 'format'=>'csv') f";
        SQLStatement stmt = parse(sql);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        try {
            stmt.accept(visitor);
            Assert.fail("Expected exception for missing 'secret'");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("secret"));
        }
    }

    @Test
    public void testMissingFormatThrows() {
        // No connector specified → defaults to 'files', triggers validation
        String sql = "SELECT * FROM FILES("
            + "'path'=>'/tmp/x.csv', 'secret'=>'s1') f";
        SQLStatement stmt = parse(sql);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        try {
            stmt.accept(visitor);
            Assert.fail("Expected exception for missing 'format'");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("format"));
        }
    }

    @Test
    public void testMockConnectorBypassesSecretAndFormat() {
        String sql = "SELECT * FROM FILES("
            + "'connector'=>'mock', "
            + "'mock.columns'=>'id:int,name:varchar') f";
        SQLStatement stmt = parse(sql);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        stmt.accept(visitor);

        // Should succeed — mock connector skips secret/format validation
        Assert.assertNotNull(visitor.getSqlNode());
    }

    @Test
    public void testCaseInsensitiveKeys() {
        String sql = "SELECT * FROM FILES("
            + "'CONNECTOR'=>'mock', "
            + "'MOCK.COLUMNS'=>'id:int,name:varchar') f";
        SQLStatement stmt = parse(sql);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        stmt.accept(visitor);

        Assert.assertNotNull(visitor.getSqlNode());
    }

    @Test
    public void testConnectorUpperCaseMockAccepted() {
        String sql = "SELECT * FROM FILES("
            + "'connector'=>'MOCK', "
            + "'mock.columns'=>'id:int,name:varchar') f";
        SQLStatement stmt = parse(sql);
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        stmt.accept(visitor);
        Assert.assertNotNull("Uppercase connector='MOCK' should be accepted", visitor.getSqlNode());
    }

    @Test
    public void testEphemeralTableNameSequential() {
        EphemeralFilesSchemaManager sm = new EphemeralFilesSchemaManager();
        Assert.assertEquals("__files_0", sm.nextTableName());
        Assert.assertEquals("__files_1", sm.nextTableName());
        Assert.assertEquals("__files_2", sm.nextTableName());
    }

    @Test
    public void testNativeQueryCatalogPropsDoNotOverwriteUserSql() {
        // Catalog properties containing a planted "native_query_sql" key must not
        // silently replace the user-supplied SQL at execution time.
        Map<String, String> catProps = new HashMap<>();
        catProps.put("mock.native_query.columns", "v:int");
        catProps.put(ExternalCatalogConstants.OPTION_NATIVE_QUERY_SQL, "SELECT 'injected' AS v");
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("evil_cat", "mock", catProps, null, null));
        try {
            String sql = "SELECT * FROM TABLE (evil_cat.native_query('SELECT 1 AS v')) nq";
            MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
            List<SQLStatement> stmts = parser.parseStatementList();
            FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
            stmts.get(0).accept(visitor);

            EphemeralFilesSchemaManager sm = ec.getOrCreateFilesSchemaManager();
            TableMeta table = sm.getTable("__files_0");
            Assert.assertNotNull("Ephemeral table should be created", table);
            Map<String, String> options = table.getExternalOptions();
            Assert.assertNotNull(options);
            Assert.assertEquals("User SQL must not be overwritten by catalog properties",
                "SELECT 1 AS v", options.get(ExternalCatalogConstants.OPTION_NATIVE_QUERY_SQL));
            Assert.assertEquals("mock", options.get(ExternalCatalogConstants.OPTION_CONNECTOR));
            Assert.assertEquals("evil_cat",
                options.get(ExternalCatalogConstants.OPTION_NATIVE_QUERY_CATALOG));
        } finally {
            ExternalCatalogManager.getInstance().remove("evil_cat");
        }
    }

    private Throwable visitExpectingFailure(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> stmts = parser.parseStatementList();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        try {
            stmts.get(0).accept(visitor);
        } catch (Throwable t) {
            return t;
        }
        return null;
    }

    private SqlNode visitOk(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> stmts = parser.parseStatementList();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, ec);
        stmts.get(0).accept(visitor);
        return visitor.getSqlNode();
    }

    private Throwable visitWithCatalogExpectingFailure(String catalogName, String sql) {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo(catalogName, "mock", new HashMap<>(), null, null));
        try {
            return visitExpectingFailure(sql);
        } finally {
            ExternalCatalogManager.getInstance().remove(catalogName);
        }
    }

    @Test
    public void testShowExternalCatalogsVisit() {
        Assert.assertTrue(visitOk("SHOW EXTERNAL CATALOGS") instanceof SqlShowExternalCatalogs);
        Assert.assertTrue(visitOk("SHOW EXTERNAL CATALOGS LIKE 'a%'") instanceof SqlShowExternalCatalogs);
    }

    @Test
    public void testShowSecretsVisit() {
        Assert.assertTrue(visitOk("SHOW SECRETS") instanceof SqlShowSecrets);
    }

    @Test
    public void testShowConnectorsVisit() {
        Assert.assertTrue(visitOk("SHOW CONNECTORS") instanceof SqlShowConnectors);
    }

    @Test
    public void testNativeQueryWithoutAlias() {
        Map<String, String> catProps = new HashMap<>();
        catProps.put("mock.native_query.columns", "v:int");
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("plain_cat", "mock", catProps, null, null));
        try {
            SqlNode node = visitOk("SELECT * FROM TABLE(plain_cat.native_query('SELECT 1 AS v'))");
            Assert.assertNotNull(node);
        } finally {
            ExternalCatalogManager.getInstance().remove("plain_cat");
        }
    }

    @Test
    public void testBareNativeQueryRejected() {
        // Without TABLE the generic expression parser drops the catalog qualifier, so the
        // form must be refused up front rather than failing later as an unknown function.
        Throwable t = visitWithCatalogExpectingFailure("some_cat",
            "SELECT * FROM some_cat.native_query('SELECT 1')");
        Assert.assertTrue("Bare native_query should be rejected",
            t instanceof TddlRuntimeException);
        Assert.assertTrue("Error must name the supported syntax, got: " + t.getMessage(),
            t.getMessage().contains("TABLE(some_cat.native_query('...'))"));
    }

    @Test
    public void testParenthesizedBareNativeQueryRejected() {
        Throwable t = visitWithCatalogExpectingFailure("some_cat",
            "SELECT * FROM (some_cat.native_query('SELECT 1')) nq");
        Assert.assertTrue("Parenthesized bare native_query should be rejected",
            t instanceof TddlRuntimeException);
        Assert.assertTrue("Error must name the supported syntax, got: " + t.getMessage(),
            t.getMessage().contains("TABLE(some_cat.native_query('...'))"));
    }

    @Test
    public void testBareNativeQueryRejectedWithQuotedCatalog() {
        Throwable t = visitWithCatalogExpectingFailure("my-cat",
            "SELECT * FROM `my-cat`.native_query('SELECT 1')");
        Assert.assertTrue(t instanceof TddlRuntimeException);
        Assert.assertTrue("Quotes must be stripped in the hint, got: " + t.getMessage(),
            t.getMessage().contains("TABLE(my-cat.native_query('...'))"));
    }

    @Test
    public void testBareNativeQueryOnUnknownCatalogNotRejected() {
        // A local function named native_query must not be reported as external catalog syntax.
        Throwable t = visitExpectingFailure("SELECT * FROM some_db.native_query('SELECT 1')");
        if (t != null) {
            Assert.assertFalse("Guard must only fire for existing catalogs, got: " + t.getMessage(),
                t.getMessage() != null && t.getMessage().contains("native_query requires"));
        }
    }

    @Test
    public void testUnrelatedQualifiedFunctionNotRejected() {
        Throwable t = visitExpectingFailure("SELECT * FROM some_db.some_func('x')");
        if (t != null) {
            Assert.assertFalse("Only native_query must be caught by the guard, got: " + t.getMessage(),
                t.getMessage() != null && t.getMessage().contains("native_query requires"));
        }
    }
}

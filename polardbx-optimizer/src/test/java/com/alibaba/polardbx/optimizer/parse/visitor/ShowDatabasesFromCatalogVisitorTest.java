package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowDatabasesFromCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class ShowDatabasesFromCatalogVisitorTest {

    private ExecutionContext executionContext;
    private ContextParameters contextParameters;

    @Before
    public void setup() {
        executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());
        contextParameters = new ContextParameters(false);
    }

    @After
    public void tearDown() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    private SQLStatement parse(String sql) {
        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();
        SqlParameterized result =
            SqlParameterizeUtils.parameterize(ByteString.from(sql), currentParameter, executionContext, false);
        return result.getAst();
    }

    @Test
    public void testShowDatabasesFromCatalog() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("iso_test_cat", "jdbc", Collections.emptyMap(), null, null));

        SQLStatement statement = parse("SHOW DATABASES FROM iso_test_cat");
        Assert.assertTrue(statement instanceof SQLShowDatabasesStatement);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        Assert.assertTrue(visitor.getSqlNode() instanceof SqlShowDatabasesFromCatalog);
        SqlShowDatabasesFromCatalog showNode = (SqlShowDatabasesFromCatalog) visitor.getSqlNode();
        Assert.assertEquals("iso_test_cat", showNode.getCatalogName());
    }

    @Test
    public void testShowDatabasesFromCatalogWithDotShouldFail() {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("iso_test_cat", "jdbc", Collections.emptyMap(), null, null));

        SQLStatement statement = parse("SHOW DATABASES FROM iso_test_cat.tt");
        Assert.assertTrue(statement instanceof SQLShowDatabasesStatement);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        try {
            statement.accept(visitor);
            Assert.fail("Expected TddlRuntimeException for SHOW DATABASES FROM catalog.db");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("SHOW DATABASES FROM only accepts a catalog name"));
        }
    }

    @Test
    public void testShowDatabasesWithoutCatalog() {
        SQLStatement statement = parse("SHOW DATABASES");
        Assert.assertTrue(statement instanceof SQLShowDatabasesStatement);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        Assert.assertTrue(visitor.getSqlNode() instanceof SqlShow);
    }

    @Test
    public void testShowDatabasesFromNonCatalogDbShouldFail() {
        SQLStatement statement = parse("SHOW DATABASES FROM internal_db");
        Assert.assertTrue(statement instanceof SQLShowDatabasesStatement);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        try {
            statement.accept(visitor);
            Assert.fail("Expected TddlRuntimeException for SHOW DATABASES FROM unknown catalog");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("unknown external catalog internal_db"));
        }
    }
}

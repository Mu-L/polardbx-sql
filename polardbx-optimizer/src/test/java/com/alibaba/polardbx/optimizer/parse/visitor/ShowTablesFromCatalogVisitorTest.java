package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.apache.calcite.sql.SqlShowTablesFromCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class ShowTablesFromCatalogVisitorTest {

    private ExecutionContext executionContext;
    private ContextParameters contextParameters;

    @Before
    public void setup() {
        executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());
        contextParameters = new ContextParameters(false);
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("show_tbl_cat", "jdbc", Collections.emptyMap(), null, null));
    }

    @After
    public void tearDown() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    private SqlShowTablesFromCatalog convert(String sql) {
        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();
        SqlParameterized parameterized =
            SqlParameterizeUtils.parameterize(ByteString.from(sql), currentParameter, executionContext, false);
        SQLStatement statement = parameterized.getAst();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);
        Assert.assertTrue(visitor.getSqlNode() instanceof SqlShowTablesFromCatalog);
        return (SqlShowTablesFromCatalog) visitor.getSqlNode();
    }

    @Test
    public void showTablesFromCatalogIsNotFull() {
        SqlShowTablesFromCatalog show = convert("SHOW TABLES FROM show_tbl_cat.remote_db");
        Assert.assertEquals("show_tbl_cat", show.getCatalogName());
        Assert.assertEquals("remote_db", show.getExternalDbName());
        Assert.assertFalse(show.isFull());
    }

    @Test
    public void showFullTablesFromCatalogKeepsFullFlag() {
        SqlShowTablesFromCatalog show = convert("SHOW FULL TABLES FROM show_tbl_cat.remote_db");
        Assert.assertEquals("show_tbl_cat", show.getCatalogName());
        Assert.assertEquals("remote_db", show.getExternalDbName());
        Assert.assertTrue(show.isFull());
    }

    @Test
    public void showFullTablesFromCatalogWithLikeKeepsFullFlag() {
        SqlShowTablesFromCatalog show = convert("SHOW FULL TABLES FROM show_tbl_cat.remote_db LIKE 'a%'");
        Assert.assertTrue(show.isFull());
        Assert.assertNotNull(show.like);
    }
}

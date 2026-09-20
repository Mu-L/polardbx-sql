package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlCreateExternalCatalog;
import org.apache.calcite.sql.SqlCreateSecret;
import org.apache.calcite.sql.SqlDescribeExternalCatalog;
import org.apache.calcite.sql.SqlDropExternalCatalog;
import org.apache.calcite.sql.SqlDropSecret;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRefreshExternalCatalog;
import org.apache.calcite.sql.SqlShowCreateExternalCatalog;
import org.apache.calcite.sql.SqlShowCreateSecret;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Backtick-quoted names must land in the SqlNode already unquoted, otherwise CREATE
 * stores `c1` while DROP looks for c1 and the metadata can never be removed.
 */
public class ExternalDdlNameNormalizeTest {

    private SqlNode convert(String sql) {
        // IgnoreNameQuotes is deliberately not enabled, matching FastsqlUtils.DEFAULT_FEATURES.
        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statements = parser.parseStatementList();
        ContextParameters context = new ContextParameters(false);
        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(context, new ExecutionContext("test_schema"));
        statements.get(0).accept(visitor);
        return visitor.getSqlNode();
    }

    @Test
    public void testCreateExternalCatalogQuotedName() {
        SqlCreateExternalCatalog node = (SqlCreateExternalCatalog)
            convert("create external catalog `C1` with ('connector'='mock')");
        assertEquals("c1", node.getCatalogName());
    }

    @Test
    public void testCreateExternalCatalogCommentUnquoted() {
        SqlCreateExternalCatalog node = (SqlCreateExternalCatalog)
            convert("create external catalog c1 with ('connector'='mock') comment 'my comment'");
        assertEquals("my comment", node.getComment());
    }

    @Test
    public void testDropExternalCatalogQuotedName() {
        SqlDropExternalCatalog node = (SqlDropExternalCatalog)
            convert("drop external catalog `C1`");
        assertEquals("c1", node.getCatalogName());
    }

    @Test
    public void testShowCreateExternalCatalogQuotedName() {
        SqlShowCreateExternalCatalog node = (SqlShowCreateExternalCatalog)
            convert("show create external catalog `C1`");
        assertEquals("c1", node.getCatalogName());
    }

    @Test
    public void testDescribeExternalCatalogQuotedName() {
        SqlDescribeExternalCatalog node = (SqlDescribeExternalCatalog)
            convert("describe external catalog `C1`");
        assertEquals("c1", node.getCatalogName());
    }

    @Test
    public void testCreateSecretQuotedName() {
        SqlCreateSecret node = (SqlCreateSecret)
            convert("create secret `S1` with ('type'='mock')");
        assertEquals("s1", node.getSecretName());
    }

    @Test
    public void testDropSecretQuotedName() {
        SqlDropSecret node = (SqlDropSecret) convert("drop secret `S1`");
        assertEquals("s1", node.getSecretName());
    }

    @Test
    public void testShowCreateSecretQuotedName() {
        SqlShowCreateSecret node = (SqlShowCreateSecret) convert("show create secret `S1`");
        assertEquals("s1", node.getSecretName());
    }

    @Test
    public void testRefreshExternalCatalogQuotedNames() {
        SqlRefreshExternalCatalog node = (SqlRefreshExternalCatalog)
            convert("refresh external table `C1`.`D1`.`T1`");
        assertEquals("c1", node.getCatalogName());
        assertEquals("D1", node.getExternalDbName());
        assertEquals("T1", node.getExternalTableName());
    }
}

package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.ColumnarWarmupControlStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.ColumnarWarmupStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWarmup;
import org.apache.calcite.sql.SqlWarmupControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class LogicalWarmupParserTest {

    ExecutionContext executionContext;
    ContextParameters contextParameters;

    @Before
    public void setup() {
        executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());

        contextParameters = new ContextParameters(false);
    }

    @Test
    public void testParse1() {
        ByteString sql = ByteString.from("warmup('*/1 * * * *') select * from test");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupStatement);

        ColumnarWarmupStatement columnarWarmupStatement = (ColumnarWarmupStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getCronExpression().equals("*/1 * * * *"));
        Assert.assertTrue(columnarWarmupStatement.getSelect().size() == 1);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmup);
        SqlWarmup sqlWarmup = (SqlWarmup) sqlNode;
        Assert.assertTrue(sqlWarmup.getCronExpression().equals("*/1 * * * *"));
        Assert.assertTrue(sqlWarmup.getSql().size() == 1);
    }

    @Test
    public void testParse2() {
        ByteString sql = ByteString.from("warmup('0 0 12 * * ?') {select * from test1} {select * from test2}");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupStatement);

        ColumnarWarmupStatement columnarWarmupStatement = (ColumnarWarmupStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getCronExpression().equals("0 0 12 * * ?"));
        Assert.assertTrue(columnarWarmupStatement.getSelect().size() == 2);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmup);
        SqlWarmup sqlWarmup = (SqlWarmup) sqlNode;
        Assert.assertTrue(sqlWarmup.getCronExpression().equals("0 0 12 * * ?"));
        Assert.assertTrue(sqlWarmup.getSql().size() == 2);
    }

    @Test
    public void testParse3() {
        ByteString sql = ByteString.from("warmup select * from test");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupStatement);

        ColumnarWarmupStatement columnarWarmupStatement = (ColumnarWarmupStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getCronExpression() == null);
        Assert.assertTrue(columnarWarmupStatement.getSelect().size() == 1);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmup);
        SqlWarmup sqlWarmup = (SqlWarmup) sqlNode;
        Assert.assertTrue(sqlWarmup.getCronExpression() == null);
        Assert.assertTrue(sqlWarmup.getSql().size() == 1);
    }

    @Test
    public void testParse4() {
        ByteString sql = ByteString.from("warmup {select * from test1} {select * from test2}");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupStatement);

        ColumnarWarmupStatement columnarWarmupStatement = (ColumnarWarmupStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getCronExpression() == null);
        Assert.assertTrue(columnarWarmupStatement.getSelect().size() == 2);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmup);
        SqlWarmup sqlWarmup = (SqlWarmup) sqlNode;
        Assert.assertTrue(sqlWarmup.getCronExpression() == null);
        Assert.assertTrue(sqlWarmup.getSql().size() == 2);
    }

    @Test
    public void testParse5() {
        ByteString sql = ByteString.from("warmup delete all");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupControlStatement);

        ColumnarWarmupControlStatement columnarWarmupStatement = (ColumnarWarmupControlStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getToken().equalsIgnoreCase("delete"));
        Assert.assertTrue(columnarWarmupStatement.isAll());

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmupControl);
        SqlWarmupControl sqlWarmup = (SqlWarmupControl) sqlNode;
        Assert.assertTrue(sqlWarmup.getControlType() == SqlWarmupControl.SqlWarmupControlType.DELETE);
        Assert.assertTrue(sqlWarmup.isAll());
    }

    @Test
    public void testParse6() {
        ByteString sql = ByteString.from("warmup delete 1");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupControlStatement);

        ColumnarWarmupControlStatement columnarWarmupStatement = (ColumnarWarmupControlStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getToken().equalsIgnoreCase("delete"));
        Assert.assertFalse(columnarWarmupStatement.isAll());
        Assert.assertTrue(columnarWarmupStatement.taskId() == 1);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmupControl);
        SqlWarmupControl sqlWarmup = (SqlWarmupControl) sqlNode;
        Assert.assertTrue(sqlWarmup.getControlType() == SqlWarmupControl.SqlWarmupControlType.DELETE);
        Assert.assertFalse(sqlWarmup.isAll());
        Assert.assertTrue(columnarWarmupStatement.taskId() == 1);
    }

    @Test
    public void testParse7() {
        ByteString sql =
            ByteString.from("warmup('0 0 12 * * ?') {/*+TDDL:cmd_extra()*/select * from test1} {select * from test2}");

        Map<Integer, ParameterContext> currentParameter = executionContext.getParams().getCurrentParameter();

        SqlParameterized result =
            SqlParameterizeUtils.parameterize(sql, currentParameter, executionContext, false);

        SQLStatement statement = result.getAst();
        Assert.assertTrue(statement instanceof ColumnarWarmupStatement);

        ColumnarWarmupStatement columnarWarmupStatement = (ColumnarWarmupStatement) statement;
        Assert.assertTrue(columnarWarmupStatement.getCronExpression().equals("0 0 12 * * ?"));
        Assert.assertTrue(columnarWarmupStatement.getSelect().size() == 2);

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(contextParameters, executionContext);
        statement.accept(visitor);

        SqlNode sqlNode = visitor.getSqlNode();
        Assert.assertTrue(sqlNode instanceof SqlWarmup);
        SqlWarmup sqlWarmup = (SqlWarmup) sqlNode;
        Assert.assertTrue(sqlWarmup.getCronExpression().equals("0 0 12 * * ?"));
        Assert.assertTrue(sqlWarmup.getSql().size() == 2);
    }
}
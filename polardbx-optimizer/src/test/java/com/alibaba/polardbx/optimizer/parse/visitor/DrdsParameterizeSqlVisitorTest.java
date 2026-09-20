package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsBaselineStatement;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.junit.Test;

import java.util.List;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;

/**
 * @author fangwu
 */
public class DrdsParameterizeSqlVisitorTest {

    @Test
    public void testPostSqlWithoutSemicolon() {
        String sql = "select * from t where a = 1;";
        String target = "SELECT *\n"
            + "FROM t\n"
            + "WHERE a = ?";
        SqlParameterized parameterize = SqlParameterizeUtils.parameterize(sql);
        System.out.println(parameterize.getSql());
        assertTrue(parameterize.getSql().equals(target));
    }

    @Test
    public void testAggSql() {
        String sql =
            "SELECT count( 1 ) FROM ( SELECT sum( amount ) AS amount, sum( usedAmount ) AS usedAmount FROM t) a";
        String target = "SELECT count(1) AS 'count(1)'\n"
            + "FROM (\n"
            + "\tSELECT sum(amount) AS amount, sum(usedAmount) AS usedAmount\n"
            + "\tFROM t\n"
            + ") a";
        SqlParameterized parameterize = SqlParameterizeUtils.parameterize(sql);
        System.out.println(parameterize.getSql());
        assertTrue(parameterize.getSql().equals(target));
    }

    @Test
    public void testBaselineFixAggSql() {
        String sql =
            "Baseline fix sql /*TDDL:a()*/ SELECT count( 1 ) FROM ( SELECT sum( amount ) AS amount, sum( usedAmount ) AS usedAmount FROM t) a";
        String target = "SELECT count(1) AS 'count(1)'\n"
            + "FROM (\n"
            + "\tSELECT sum(amount) AS amount, sum(usedAmount) AS usedAmount\n"
            + "\tFROM t\n"
            + ") a";
        SqlParameterized parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertTrue(parameterize.getAst() instanceof DrdsBaselineStatement);
        String targetSql = ((DrdsBaselineStatement) parameterize.getAst()).getTargetSql();
        String targetSqlParameterize = SqlParameterizeUtils.parameterize(targetSql).getSql();
        System.out.println(target);
        System.out.println(targetSqlParameterize);
        assertTrue(target.equals(targetSqlParameterize));
    }

    @Test
    public void testBaselineFixAggSqlWithExecutorMode() {
        String sql =
            "Baseline fix sql /*TDDL:EXECUTOR_MODE=MPP WORKLOAD_TYPE=AP*/ SELECT count( 1 ) FROM ( SELECT sum( amount ) AS amount, sum( usedAmount ) AS usedAmount FROM t) a";
        String target = "SELECT count(1) AS 'count(1)'\n"
            + "FROM (\n"
            + "\tSELECT sum(amount) AS amount, sum(usedAmount) AS usedAmount\n"
            + "\tFROM t\n"
            + ") a";
        SqlParameterized parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertTrue(parameterize.getAst() instanceof DrdsBaselineStatement);
        String targetSql = ((DrdsBaselineStatement) parameterize.getAst()).getTargetSql();
        String targetSqlParameterize = SqlParameterizeUtils.parameterize(targetSql).getSql();
        assertTrue(target.equals(targetSqlParameterize));

        assertTrue(parameterize.getStmt() instanceof DrdsBaselineStatement);

        DrdsBaselineStatement drdsBaselineStatement = (DrdsBaselineStatement) parameterize.getStmt();
        List<SQLCommentHint> inlineHint = drdsBaselineStatement.getInlineHint();

        assertTrue(inlineHint.size() == 1);
        assertTrue(inlineHint.get(0).getText().toLowerCase().contains("executor_mode=mpp"));
        assertTrue(inlineHint.get(0).getText().toLowerCase().contains("workload_type=ap"));
    }

    @Test
    public void testBaselineBindHintAggSql() {
        String sql =
            "Baseline hint bind /*TDDL:a()*/ SELECT count( 1 ) FROM ( SELECT sum( amount ) AS amount, sum( usedAmount ) AS usedAmount FROM t) a";
        String target = "SELECT count(1) AS 'count(1)'\n"
            + "FROM (\n"
            + "\tSELECT sum(amount) AS amount, sum(usedAmount) AS usedAmount\n"
            + "\tFROM t\n"
            + ") a";
        SqlParameterized parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertTrue(parameterize.getAst() instanceof DrdsBaselineStatement);
        String targetSql = ((DrdsBaselineStatement) parameterize.getAst()).getTargetSql();
        String targetSqlParameterize = SqlParameterizeUtils.parameterize(targetSql).getSql();
        System.out.println(target);
        System.out.println(targetSqlParameterize);
        assertTrue(target.equals(targetSqlParameterize));
    }
}

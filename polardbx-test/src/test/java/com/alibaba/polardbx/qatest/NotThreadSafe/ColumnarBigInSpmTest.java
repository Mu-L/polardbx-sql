package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class ColumnarBigInSpmTest extends DDLBaseNewDBTestCase {
    private static String TABLE_DEFINITION_FORMAT = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "\t`a` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`b` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`c` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\tprimary key(id)\n"
        + ") partition by key(id) partitions 8";

    private static String CREATE_COL_IDX = SKIP_WAIT_CCI_CREATION_HINT
        + "create clustered columnar index `%s` on %s(`%s`) partition by hash(`%s`) partitions 4";

    private static String PUB_COL_IDX =
        "/*+TDDL:CMD_EXTRA(ALTER_CCI_STATUS=true, ALTER_CCI_STATUS_BEFORE=CREATING, ALTER_CCI_STATUS_AFTER=PUBLIC)*/" +
            "ALTER TABLE `%s` alter index `%s` VISIBLE;";

    String ENABLE_COL_CACHE = "set global " + ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE + " = true";

    String tb1 = "bigIn";

    String colIdxA = "colIdx_a";

    String colA = "a";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void prepareTable() {
        JdbcUtil.dropTable(getTddlConnection1(), tb1);
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tb1));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(CREATE_COL_IDX, colIdxA, tb1, colA, colA));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(PUB_COL_IDX, tb1, colIdxA));
    }

    @After
    public void afterDDLBaseNewDBTestCase() {
        cleanDataBase();
    }

    @Test
    public void testExplain() throws SQLException, InterruptedException {
        long threshold = Long.parseLong(ConnectionParams.COL_IN_SEMIJOIN_THRESHOLD.getDefault());
        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < threshold; i++) {
            sb.append(", ").append(i);
        }
        String sql1 = String.format("select a.id from %s a, %s b where a.id in (%s)", tb1, tb1, sb);
        String exSql1 = "explain " + sql1;
        String sql2 = String.format("select a.id from %s a, %s b where a.id in (1)", tb1, tb1);
        String exSql2 = "explain " + sql2;
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeQuery(ENABLE_COL_CACHE, conn);
            Thread.sleep(3000L);
            JdbcUtil.executeQuery("clear plancache;", conn);
            JdbcUtil.executeQuery("set WORKLOAD_TYPE=AP", conn);
            JdbcUtil.executeQuery("set ENABLE_COLUMNAR_OPTIMIZER=true", conn);

            String explain = DataValidator.getExplainAllResult(exSql1, null, conn);
            Assert.assertTrue(explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(exSql2, null, conn);
            Assert.assertTrue(explain.contains("HitCache:false"));
            JdbcUtil.executeQuery(sql1, conn);
            JdbcUtil.executeQuery(sql2, conn);

            explain = DataValidator.getExplainAllResult(exSql1, null, conn);
            Assert.assertTrue(explain.contains("HitCache:true"));
            explain = DataValidator.getExplainAllResult(exSql2, null, conn);
            Assert.assertTrue(explain.contains("Source:SPM"));
        }
    }
}

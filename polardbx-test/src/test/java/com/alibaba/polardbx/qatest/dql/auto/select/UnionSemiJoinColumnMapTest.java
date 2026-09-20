package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * AONE-81725409: UNION ALL 分支中 row-value IN 子查询的 SemiJoin 转换列序号映射错误导致数据丢失。
 * <p>
 * 复现条件：UNION ALL 的一个分支包含 (col1, col2) IN (SELECT c1, c2 FROM ...) 形式的行值 IN 子查询，
 * 该分支的表是单表（可整体下推为一条 LogicalView 物理 SQL）。SemiJoin 下推/转置规则
 * （SemiJoinProjectTransposeRule / JoinSemiJoinTransposeRule）在生成下推 SQL 时未能正确重映射
 * row-value IN 左侧的列引用（operands），导致下推 SQL 出现虚构的等值条件，最终该分支返回错误的行数。
 * 单独执行该分支（不放入 UNION ALL）时结果正确，说明问题出在 UNION ALL 场景下的下推 SQL 生成阶段。
 */
public class UnionSemiJoinColumnMapTest extends AutoReadBaseTestCase {

    private static final String T_A = "union_semi_min_a";
    private static final String T_LZ = "union_semi_min_lz";
    private static final String T_ID = "union_semi_min_id";

    @Before
    public void setUp() {
        dropTables();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "CREATE TABLE " + T_A + " ("
            + "EXEC_ID varchar(64) NOT NULL, "
            + "EXEC_CODE varchar(200) NOT NULL, "
            + "PROPOSER_ID varchar(19) NOT NULL, "
            + "MKE_SUBTYPE_ID varchar(64) NOT NULL, "
            + "DEL_FLAG varchar(2) NOT NULL DEFAULT '0', "
            + "PRIMARY KEY (EXEC_ID)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(EXEC_ID) PARTITIONS 4");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "CREATE TABLE " + T_LZ + " ("
            + "EXEC_ID varchar(64) NOT NULL, "
            + "EXEC_CODE varchar(200) NOT NULL, "
            + "EXEC_DATE varchar(8) DEFAULT NULL, "
            + "MKE_SUBTYPE_ID varchar(64) NOT NULL, "
            + "PROPOSER_ID varchar(19) NOT NULL, "
            + "GROUPCUS_CODE varchar(30) NOT NULL, "
            + "PRIMARY KEY (EXEC_ID)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SINGLE");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "CREATE TABLE " + T_ID + " ("
            + "C_DATE varchar(8) NOT NULL, "
            + "CUSTOMER_CODE varchar(20) NOT NULL, "
            + "PRIMARY KEY (C_DATE, CUSTOMER_CODE)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SINGLE");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + T_A + " VALUES ('A001','A20260401','9999','1935234674490216448','0')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + T_LZ + " VALUES "
                + "('LZ001','LZ001CODE','20260423','1801441892639051776','1141166021500080959','207200275632'),"
                + "('LZ002','LZ002CODE','20260429','1801441892639051776','1141166021500080959','207200136101')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "INSERT INTO " + T_ID + " VALUES ('20260423','207200275632'),('20260429','207200136101')");
    }

    @After
    public void tearDown() {
        dropTables();
    }

    private void dropTables() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + T_A);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + T_LZ);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + T_ID);
    }

    /**
     * 分支单独执行：row-value IN 子查询走 SemiJoin，无 UNION ALL 干扰，应返回 2 行。
     */
    @Test
    public void testRowValueSemiJoinWithoutUnion() throws SQLException {
        String sql = "/*+TDDL:cmd_extra(ENABLE_SPM=false, PLAN_CACHE=false)*/"
            + "SELECT LZ.EXEC_ID, LZ.EXEC_CODE, LZ.PROPOSER_ID, LZ.MKE_SUBTYPE_ID"
            + " FROM " + T_LZ + " LZ"
            + " INNER JOIN ("
            + "   SELECT CUSTOMER_CODE, C_DATE FROM " + T_ID
            + "   WHERE (CUSTOMER_CODE, C_DATE) IN (SELECT DISTINCT GROUPCUS_CODE, EXEC_DATE FROM " + T_LZ + ")"
            + " ) CUS ON LZ.GROUPCUS_CODE = CUS.CUSTOMER_CODE AND LZ.EXEC_DATE = CUS.C_DATE"
            + " WHERE PROPOSER_ID = '1141166021500080959'";

        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        int rowCount = JdbcUtil.resultsSize(rs);
        Assert.assertEquals("row-value IN SemiJoin without UNION ALL should return 2 rows", 2, rowCount);
    }

    /**
     * 复现 Bug：将同一 row-value IN 子查询分支放入 UNION ALL 中，SemiJoin 下推该单表分支为一条物理 SQL 时
     * 列引用映射错误，导致下推 SQL 出现虚构等值条件（如 EXEC_CODE = EXEC_DATE），该分支本应仍返回 2 行，
     * 但当前代码下返回错误的行数（0 行）。
     */
    @Test
    public void testRowValueSemiJoinInsideUnionAll() throws SQLException {
        String sql = "/*+TDDL:cmd_extra(ENABLE_SPM=false, PLAN_CACHE=false)*/"
            + "SELECT * FROM ("
            + "  SELECT EXEC_ID, EXEC_CODE, PROPOSER_ID, MKE_SUBTYPE_ID"
            + "  FROM " + T_A
            + "  WHERE DEL_FLAG = '0' AND MKE_SUBTYPE_ID = '1935234674490216448'"
            + "  UNION ALL"
            + "  SELECT LZ.EXEC_ID, LZ.EXEC_CODE, LZ.PROPOSER_ID, LZ.MKE_SUBTYPE_ID"
            + "  FROM " + T_LZ + " LZ"
            + "  INNER JOIN ("
            + "    SELECT CUSTOMER_CODE, C_DATE FROM " + T_ID
            + "    WHERE (CUSTOMER_CODE, C_DATE) IN (SELECT DISTINCT GROUPCUS_CODE, EXEC_DATE FROM " + T_LZ + ")"
            + "  ) CUS ON LZ.GROUPCUS_CODE = CUS.CUSTOMER_CODE AND LZ.EXEC_DATE = CUS.C_DATE"
            + ") a WHERE PROPOSER_ID = '1141166021500080959'";

        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        int rowCount = JdbcUtil.resultsSize(rs);
        Assert.assertEquals(
            "row-value IN SemiJoin branch inside UNION ALL should still return 2 rows, not lose data",
            2, rowCount);
    }
}

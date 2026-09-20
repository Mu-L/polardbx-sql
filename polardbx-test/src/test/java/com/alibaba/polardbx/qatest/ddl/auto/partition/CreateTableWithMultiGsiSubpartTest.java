package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

/**
 * Test for bug: CREATE TABLE IF NOT EXISTS with multiple UNIQUE GLOBAL INDEX having RANGE subpartition
 * causes physical table not found error due to random partition placement mismatch.
 * <p>
 * Scenario: tb1_k3 and tb1_k4 are placed in the same implicit tablegroup,
 * but their physical placements are shuffled independently (PR #72067733),
 * causing tb1_k4's metadata to mismatch its physical location.
 *
 * @author luoyanxin
 */
public class CreateTableWithMultiGsiSubpartTest extends PartitionTestBase {

    private static final String TABLE_NAME = "tb1_multi_gsi_sp_test";
    private static final String DB_NAME = "CreateTableWithMultiGsiSubpartTest";

    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS `" + TABLE_NAME + "` (\n"
        + "  `col0` date,\n"
        + "  `col1` date DEFAULT NULL,\n"
        + "  `col2` date,\n"
        + "  `col3` date,\n"
        + "  `col4` datetime,\n"
        + "  `col5` datetime,\n"
        + "  `col6` datetime,\n"
        + "  `col7` datetime,\n"
        + "  `col8` datetime,\n"
        + "  PRIMARY KEY (`col0`),\n"
        + "  UNIQUE GLOBAL INDEX `tb1_k3` (`col6`, `col5`) PARTITION BY KEY (`col5`) PARTITIONS 4\n"
        + "  SUBPARTITION BY RANGE (DAYOFMONTH(`col6`)) (\n"
        + "    SUBPARTITION sp2 VALUES LESS THAN (2),\n"
        + "    SUBPARTITION sp3 VALUES LESS THAN (3),\n"
        + "    SUBPARTITION sp4 VALUES LESS THAN (4),\n"
        + "    SUBPARTITION sp5 VALUES LESS THAN (5),\n"
        + "    SUBPARTITION sp6 VALUES LESS THAN (6),\n"
        + "    SUBPARTITION sp7 VALUES LESS THAN (7)\n"
        + "  ),\n"
        + "  UNIQUE GLOBAL INDEX `tb1_k4` (`col6`, `col7`) COVERING (`col8`) PARTITION BY KEY (`col6`) PARTITIONS 4\n"
        + "  SUBPARTITION BY RANGE (DAYOFMONTH(`col7`)) (\n"
        + "    SUBPARTITION sp2 VALUES LESS THAN (2),\n"
        + "    SUBPARTITION sp3 VALUES LESS THAN (3),\n"
        + "    SUBPARTITION sp4 VALUES LESS THAN (4),\n"
        + "    SUBPARTITION sp5 VALUES LESS THAN (5),\n"
        + "    SUBPARTITION sp6 VALUES LESS THAN (6),\n"
        + "    SUBPARTITION sp7 VALUES LESS THAN (7)\n"
        + "  ),\n"
        + "  UNIQUE GLOBAL INDEX `tb1_k5` (`col1`, `col0`,`col2`) COVERING (`col8`) PARTITION BY KEY (`col1`, `col0`) PARTITIONS 4\n"
        + "SUBPARTITION BY RANGE (DAYOFMONTH(`col2`)) (\n"
        + "  SUBPARTITION sp2 VALUES LESS THAN (2),\n"
        + "  SUBPARTITION sp3 VALUES LESS THAN (3),\n"
        + "  SUBPARTITION sp4 VALUES LESS THAN (4)\n"
        + ")\n"
        + ")\n"
        + "PARTITION BY KEY (`col1`, `col0`) PARTITIONS 4\n"
        + "SUBPARTITION BY RANGE (DAYOFMONTH(`col2`)) (\n"
        + "  SUBPARTITION sp2 VALUES LESS THAN (2),\n"
        + "  SUBPARTITION sp3 VALUES LESS THAN (3),\n"
        + "  SUBPARTITION sp4 VALUES LESS THAN (4)\n"
        + ");";

    @Before
    public void setUp() {
        JdbcUtil.useDb(tddlConnection, "information_schema");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE IF EXISTS `" + DB_NAME + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "CREATE DATABASE IF NOT EXISTS `" + DB_NAME + "` mode=auto");

    }

    @After
    public void tearDown() {
        JdbcUtil.useDb(tddlConnection, "information_schema");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE IF EXISTS `" + DB_NAME + "`");
    }

    /**
     * Test that CREATE TABLE with two UNIQUE GLOBAL INDEX with RANGE subpartition succeeds,
     * and CHECK TABLE reports no errors.
     * <p>
     * Before the fix, tb1_k4's partition_group records had tg_id=-1 due to INSERT IGNORE
     * returning null when the implicit tablegroup already existed (created by tb1_k3),
     * causing physical table not found errors.
     */
    @Test
    public void testCreateTableWithMultiUniqueGsiAndRangeSubpartition() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        // Step 1: Create table with two UNIQUE GLOBAL INDEXes having RANGE subpartitions
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_SQL);
        JdbcUtil.executeUpdate(tddlConnection, "insert into " + TABLE_NAME
            + " values ('2026-02-02', '2026-02-02', '2026-02-02', '2026-02-02', '2026-02-02 01:01:01', '2026-02-02 01:01:01', '2026-02-02 01:01:01', '2026-02-02 01:01:01', '2026-02-02 01:01:01')");
        JdbcUtil.executeQuery("select * from " + TABLE_NAME, tddlConnection);
        JdbcUtil.executeQuery("select * from " + TABLE_NAME + " force index(tb1_k3)", tddlConnection);
        JdbcUtil.executeQuery("select * from " + TABLE_NAME + " force index(tb1_k4)", tddlConnection);
        JdbcUtil.executeQuery("select * from " + TABLE_NAME + " force index(tb1_k5)", tddlConnection);
        // Step 2: CHECK TABLE tb1 - should pass without errors (all status OK)
        String checkSql = "CHECK TABLE `" + TABLE_NAME + "`";
        ResultSet rs = JdbcUtil.executeQuery(checkSql, tddlConnection);
        List<List<Object>> checkResult = JdbcUtil.getAllResult(rs);

        Assert.assertFalse("CHECK TABLE returned no results", checkResult.isEmpty());

        for (List<Object> row : checkResult) {
            String msgType = row.get(2).toString();
            String msgText = row.get(3).toString();
            Assert.assertEquals(
                String.format("CHECK TABLE reported error: table=%s, op=%s, msg_type=%s, msg_text=%s",
                    row.get(0), row.get(1), msgType, msgText),
                "status", msgType.toLowerCase());
            Assert.assertEquals(
                String.format("CHECK TABLE status is not OK: table=%s, op=%s, msg_type=%s, msg_text=%s",
                    row.get(0), row.get(1), msgType, msgText),
                "ok", msgText.toLowerCase());
        }
    }
}

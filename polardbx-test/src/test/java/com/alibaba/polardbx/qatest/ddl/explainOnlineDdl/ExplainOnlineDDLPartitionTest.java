package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class ExplainOnlineDDLPartitionTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_ddl_range";
    String tableName2 = "explain_online_ddl_range2";

    String tableName3 = "explain_online_ddl_list";
    String tableName4 = "explain_online_ddl_list2";

    String dn1 = null;

    @Before
    public void beforeClass() {
        List<String> dnList = LocalityTestUtils.getDatanodes(tddlConnection);
        dn1 = dnList.get(0);

        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c int) PARTITION BY RANGE(a) "
                + "("
                + "PARTITION p1 VALUES LESS THAN(20),"
                + "PARTITION p2 VALUES LESS THAN(100),"
                + "PARTITION p3 VALUES LESS THAN(200)"
                + ")", tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        createTableSql =
            String.format("create table %s (a int, b varchar(10), c int) PARTITION BY RANGE(a) "
                + "("
                + "PARTITION p1 VALUES LESS THAN(20),"
                + "PARTITION p2 VALUES LESS THAN(100),"
                + "PARTITION p3 VALUES LESS THAN(200)"
                + ")", tableName2);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        createTableSql =
            String.format("create table %s (a int, b varchar(10), c int) PARTITION BY LIST(a) "
                + "("
                + "  PARTITION p1 VALUES IN(1, 2, 3, 4, 5, 6),"
                + "  PARTITION p2 VALUES IN(7,8,9),"
                + "  PARTITION p3 VALUES IN(default)"
                + ")", tableName3);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        createTableSql =
            String.format("create table %s (a int, b varchar(10), c int) PARTITION BY LIST(a) "
                + "("
                + "  PARTITION p1 VALUES IN(1, 2, 3, 4, 5, 6),"
                + "  PARTITION p2 VALUES IN(7,8,9),"
                + "  PARTITION p3 VALUES IN(default)"
                + ")", tableName4);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
    }

    @After
    public void afterClass() {
        String dropTableSql = String.format("drop table %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);

        dropTableSql = String.format("drop table %s", tableName2);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);

        dropTableSql = String.format("drop table %s", tableName3);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);

        dropTableSql = String.format("drop table %s", tableName4);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testMovePartition() {
        String sql =
            String.format("explain online_ddl alter table %s move partitions p1,p2,p3 to '%s'", tableName, dn1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testSplitPartition() {
        String sql =
            String.format(
                "explain online_ddl alter table %s split partition p1 into (PARTITION p4 VALUES LESS THAN(10),PARTITION p5 VALUES LESS THAN(20))",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testMergePartition() {
        String sql = String.format("explain online_ddl alter table %s merge partitions p2,p3 to p4", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testAddPartition() {
        String sql =
            String.format("explain online_ddl alter table %s add partition (PARTITION p4 VALUES LESS THAN(400))",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testDropPartition() {
        String sql = String.format("explain online_ddl alter table %s drop partition p2", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testReorganizePartition() {
        String sql =
            String.format(
                "explain online_ddl alter table %s reorganize partition p1 into (PARTITION p4 VALUES LESS THAN(10),PARTITION p5 VALUES LESS THAN(20))",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testTruncatePartition() {
        String sql = String.format("explain online_ddl alter table %s truncate partition p1", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testRenamePartition() {
        String sql = String.format("explain online_ddl alter table %s rename partition p1 to p4", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Ignore
    @Test
    public void testModifyPartition() {
        String sql =
            String.format("explain online_ddl alter table %s modify partition p1 drop values (6)", tableName3);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testRepartition() {
        String sql = String.format("explain online_ddl alter table %s partition by key(a)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testMovePartitionWithTableGroup() {
        String sql =
            String.format("explain online_ddl alter tablegroup by table %s move partitions p1,p2,p3 to '%s'", tableName,
                dn1);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testSplitPartitionWithTableGroup() {
        String sql =
            String.format(
                "explain online_ddl alter tablegroup by table %s split partition p1 into (PARTITION p4 VALUES LESS THAN(10),PARTITION p5 VALUES LESS THAN(20))",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testMergePartitionWithTableGroup() {
        String sql =
            String.format("explain online_ddl alter tablegroup by table %s merge partitions p2,p3 to p4", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testAddPartitionWithTableGroup() {
        String sql =
            String.format(
                "explain online_ddl alter tablegroup by table %s add partition (PARTITION p4 VALUES LESS THAN(400))",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testDropPartitionWithTableGroup() {
        String sql = String.format("explain online_ddl alter tablegroup by table %s drop partition p2", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testReorganizePartitionWithTableGroup() {
        String sql =
            String.format(
                "explain online_ddl alter tablegroup by table %s reorganize partition p1 into (PARTITION p4 VALUES LESS THAN(10),PARTITION p5 VALUES LESS THAN(20))",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testTruncatePartitionWithTableGroup() {
        String sql = String.format("explain online_ddl alter tablegroup by table %s truncate partition p1", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testRenamePartitionWithTableGroup() {
        String sql =
            String.format("explain online_ddl alter tablegroup by table %s rename partition p1 to p4", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Ignore
    @Test
    public void testModifyPartitionWithTableGroup() {
        String sql =
            String.format("explain online_ddl alter tablegroup by table %s modify partition p1 drop values (6)",
                tableName3);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
    }

    @Test
    public void testCreateTableGroup() {
        String sql = "explain online_ddl create tablegroup wumu_tg_123";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }
}

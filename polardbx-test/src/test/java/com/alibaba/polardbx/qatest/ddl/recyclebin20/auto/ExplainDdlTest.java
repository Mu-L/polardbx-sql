package com.alibaba.polardbx.qatest.ddl.recyclebin20.auto;

import com.alibaba.polardbx.qatest.ddl.recyclebin20.RecycleBinBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.calcite.sql.SqlKind;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.Matchers.is;

public class ExplainDdlTest extends RecycleBinBaseTest {

    public static String DB_NAME = "test_explain_auto_recyclebin";
    public static String tableName1 = "tb_explain1_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName2 = "tb_explain2_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName3 = "tb_explain3_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName4 = "tb_explain4_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName5 = "tb_explain5_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName6 = "tb_explain6_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName7 = "tb_explain7_" + RandomUtils.getStringBetween(4, 6);
    public static String tableGroupName = "tb_explain_group_" + RandomUtils.getStringBetween(4, 6);
    ;
    public static String createTgSql = "create tablegroup %s ";
    public static String createTableSql = "create table %s (id int, name varchar(10)) partition by key(id) partitions 3 tablegroup=%s";
    public static String createTableWithGsiSql = "create table %s (id int, name varchar(10), global index g1(id) partition by key(id) partitions 2) partition by key(id) partitions 4";
    public static String createRangeTableSql = "create table %s (id int, name varchar(10)) partition by range(id) (partition p1 values less than(10),partition p2 values less than(20))";
    public static String alterTableSetTablegroupSql = "alter table %s set tablegroup=''";


    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "polardbx");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            conn.createStatement().execute("use " + DB_NAME);
            conn.createStatement().execute(String.format(createTgSql, tableGroupName));
            conn.createStatement().execute(String.format(createTableSql, tableName1, tableGroupName));
            conn.createStatement().execute(String.format(createTableSql, tableName2, tableGroupName));
            conn.createStatement().execute(String.format(createTableWithGsiSql, tableName3));
            conn.createStatement().execute(String.format(createTableWithGsiSql, tableName4));
            conn.createStatement().execute(String.format(createTableSql, tableName5, tableGroupName));
            conn.createStatement().execute(String.format(createTableWithGsiSql, tableName6));
            conn.createStatement().execute(String.format(alterTableSetTablegroupSql, tableName5));
            conn.createStatement().execute(String.format(alterTableSetTablegroupSql, tableName6));
            conn.createStatement().execute(String.format(createRangeTableSql, tableName7));
        }
    }

    @Ignore
    public void testRepartTableWithGsi() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s partition by hash(id) partitions 20;", tableName6);
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }

    @Ignore
    public void testRepartTable() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s partition by hash(id) partitions 6;", tableName5);
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }

    @Test
    public void testDropTableWithGsi() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain drop table %s", tableName6);
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(2));
    }

    @Test
    public void testDropTable() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain drop table %s", tableName6);
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(2));
    }

    @Test
    public void testTruncateTableWithGsi() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain truncate table %s", tableName6);
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(2));
    }

    @Test
    public void testMovePartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        Map<String, String> partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        String sql = String.format("explain alter tablegroup %s move partitions %s to '%s' ", tableGroupName, "p1", partInstIdMap.get("p2"));
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(2));
    }

    @Test
    public void testMovePartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        Map<String, String> partInstIdMap = getPartInstIdMap(tableName5, tddlConnection);
        String sql = String.format("explain alter table %s move partitions %s to '%s' ", tableName5, "p1", partInstIdMap.get("p2"));
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }

    @Test
    public void testSplitPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter tablegroup %s split partition %s", tableGroupName, "p1");
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(2));
    }

    @Test
    public void testSplitPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s split partition %s", tableName5, "p1");
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }

    @Test
    public void testMergePartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter tablegroup %s merge partitions %s, %s to p100", tableGroupName, "p1", "p2");
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(2));
    }

    @Test
    public void testMergePartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s merge partitions %s, %s to p100", tableName5, "p1", "p2");
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }

    @Test
    public void testDropPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter tablegroup by table %s drop partition %s", tableName7, "p1");
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }

    @Test
    public void testDropPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s drop partition %s", tableName7, "p1");
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> taskInfos = new ArrayList<>();
        List<String> allTaskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            if (taskInfo.toLowerCase().indexOf(SqlKind.RENAME_TABLE.name().toLowerCase()) != -1) {
                taskInfos.add(taskInfo);
            }
            allTaskInfos.add(taskInfo);
        }
        Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(1));
    }
    public boolean usingNewPartDb() {
        return true;
    }
}

package com.alibaba.polardbx.qatest.ddl.recyclebin20.drds;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.recyclebin20.RecycleBinBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.calcite.sql.SqlKind;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.is;

public class ExplainDdlTest extends RecycleBinBaseTest {

    public static String DB_NAME = "test_explain_drds_recyclebin";
    public static String tableName1 = "tb_explain1_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName2 = "tb_explain2_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName3 = "tb_explain3_" + RandomUtils.getStringBetween(4, 6);
    public static String tableName4 = "tb_explain4_" + RandomUtils.getStringBetween(4, 6);

    public static String createTableSql = "create table %s (id int, name varchar(10)) dbpartition by hash(id)";
    public static String createTableWithGsiSql = "create table %s (id int, name varchar(10), global index %s(id) dbpartition by hash(id)) dbpartition by hash(id)";


    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "polardbx");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=drds");
            conn.createStatement().execute("use " + DB_NAME);
            conn.createStatement().execute(String.format(createTableSql, tableName1));
            conn.createStatement().execute(String.format(createTableSql, tableName2));
            conn.createStatement().execute(String.format(createTableWithGsiSql, tableName3, "g1"));
            conn.createStatement().execute(String.format(createTableWithGsiSql, tableName4, "g2"));
        }
    }

    @Test
    public void testRepartTableWithGsi() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2;", tableName3);
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
    public void testRepartTable() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain alter table %s dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2;", tableName1);
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
        String sql = String.format("explain drop table %s", tableName3);
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
        String sql = String.format("explain drop table %s", tableName1);
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
    public void testTruncateTableWithGsi() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        String sql = String.format("explain truncate table %s", tableName3);
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
    public void testMoveDatabase() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_PHY_RECYCLEBIN = true");
        Map<String, String> groupInstIdMap = getGroupInstIdMap(DB_NAME, tddlConnection);
        String group0 = String.format("%s_000000_GROUP", DB_NAME.toUpperCase());
        String group1 = String.format("%s_000001_GROUP", DB_NAME.toUpperCase());
        int i = 0;
        String ENABLE_CHANGESET_HINT = "/*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=%s)*/";
        do {
            String hint = String.format(ENABLE_CHANGESET_HINT, i == 0 ? true : false);
            String sql = String.format("explain move database %s %s to '%s'", hint, group1, groupInstIdMap.get(group0));
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
            Assert.assertThat(allTaskInfos.toString(), taskInfos.size(), is(6));
            i++;
        } while (i < 2);
    }

    public boolean usingNewPartDb() {
        return false;
    }
}

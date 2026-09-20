package com.alibaba.polardbx.qatest.ddl.recyclebin20.drds;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.recyclebin20.RecycleBinBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

@NotThreadSafe
@CdcIgnore(
    ignoreReason = "mysql对外键父表的truncate会报错")
public class CheckRecyclebinTaskTest extends RecycleBinBaseTest {

    public static String DB_NAME = "test_alter_table_drds_recyclebin";
    Boolean curSwitch = null;

    public CheckRecyclebinTaskTest(Boolean recyclebinSwitch) throws Exception {
        if (curSwitch == null || curSwitch != recyclebinSwitch) {
            // Use separate schema for each recyclebin switch value to avoid table group conflicts
            DB_NAME = "test_alter_table_drds_recyclebin_" + (recyclebinSwitch ? "on" : "off");
            prepare();
        }
        curSwitch = recyclebinSwitch;
    }

    public static void prepare() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "polardbx");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=drds");
            conn.createStatement().execute("use " + DB_NAME);
        }
    }

    @Parameterized.Parameters(name = "{index}:recyebin_enable={0}")
    public static List<Boolean[]> prepareData() {
        List<Boolean[]> recyclebinSwitch = new ArrayList<>();
        recyclebinSwitch.add(new Boolean[] {Boolean.FALSE});
        recyclebinSwitch.add(new Boolean[] {Boolean.TRUE});
        return recyclebinSwitch;
    }

    public boolean usingNewPartDb() {
        return false;
    }

    @Test
    public void testDropTableWoGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDropTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 0, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testDropTableWithFk() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropTableWithFk_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropTableWithFk_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, false, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropSingleTableWithFk() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropSingleTableWithFk_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropSingleTableWithFk_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, true, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropBroadcastTableWithFk() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropBroadcastTableWithFk_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropBroadcastTableWithFk_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, false, true);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropTableWith1Gsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDropTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 1, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testDropTableWith2Gsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDropTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testTruncateTableWithGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testTruncateTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, false);
        String truncateTableSql = String.format("%s truncate table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, truncateTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testTruncateForeignTableWithGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testTruncateForeignTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testTruncateForeignTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, false, false);
        String dropTableSql = String.format("%s truncate table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s truncate table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testMoveDatabase() throws SQLException {
        boolean shareStorageMode = isShareStorageMode();
        String scaleOutHint = shareStorageMode ? "/*+TDDL:CMD_EXTRA(SHARE_STORAGE_MODE=true)*/" : "";

        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName1 = "testMoveDatabase_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName1, 2, false);
        String tableName2 = "testMoveDatabase_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName2, 1, false);
        String tableName3 = "testMoveDatabase_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName3, 0, false);
        String tableName4 = "testMoveDatabase_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName4, 2, false);
        String parentTableName = "testMoveDatabasePFk1_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testMoveDatabaseSFk1_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, true, false);
        parentTableName = "testMoveDatabasePFk2_" + RandomUtils.getStringBetween(4, 6);
        childTableName = "testMoveDatabaseSFk2_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, false, false);
        parentTableName = "testMoveDatabasePFk3_" + RandomUtils.getStringBetween(4, 6);
        childTableName = "testMoveDatabaseSFk3_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentDrdsTable(tddlConnection, childTableName, parentTableName, 2, false, true);
        Map<String, String> groupInstIdMap = getGroupInstIdMap(DB_NAME, tddlConnection);
        String group0 = String.format("%s_000000_GROUP", DB_NAME.toUpperCase());
        String group1 = String.format("%s_000001_GROUP", DB_NAME.toUpperCase());
        String sql =
            String.format("move database %s %s to '%s' async=true", scaleOutHint, group1, groupInstIdMap.get(group0));
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheckIgnoreError(group1, curSwitch);
    }

    @Test
    public void testDrop1Gsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDrop1Gsi_" + RandomUtils.getStringBetween(4, 6);
        List<String> gsiNames = createTable(tddlConnection, tableName, 1, false);
        String dropTableSql = String.format("alter table %s drop index %s async=true", tableName, gsiNames.get(0));
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testRepartTableWithGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testRepartTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 3, false);
        String sql =
            String.format("alter table %s dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4 async=true",
                tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testRepartTable() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testRepartTable_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 0, false);
        String sql =
            String.format("alter table %s dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4 async=true",
                tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Before
    public void before() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = " + curSwitch);
    }

    @After
    public void after() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = " + true);
    }
}

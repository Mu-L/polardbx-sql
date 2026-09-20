package com.alibaba.polardbx.qatest.ddl.recyclebin20.auto;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.recyclebin20.RecycleBinBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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

    public static String DB_NAME = "test_alter_table_auto_recyclebin";

    static Boolean curSwitch = null;

    public CheckRecyclebinTaskTest(Boolean recyclebinSwitch) throws Exception {
        if (curSwitch == null || curSwitch != recyclebinSwitch) {
            // Use separate schema for each recyclebin switch value to avoid table group conflicts
            DB_NAME = "test_alter_table_auto_recyclebin_" + (recyclebinSwitch ? "on" : "off");
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
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
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
        return true;
    }

    @Test
    public void testDropTableWoGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDropTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 0, true);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testDropTableWithForeignKey() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropTableWithForeignKey_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropTableWithForeignKey_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, false, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropSingleTableWithForeignKey1() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropSingleTableWithForeignKey1_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropSingleTableWithForeignKey1_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, true, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropSingleTableWithForeignKey2() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropSingleTableWithForeignKey2_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropSingleTableWithForeignKey2_" + RandomUtils.getStringBetween(4, 6);
        createChildAndPartParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, true, false);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropBroadCastTableWithForeignKey() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropBroadCastTableWithForeignKey_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropBroadCastTableWithForeignKey_" + RandomUtils.getStringBetween(4, 6);
        createChildAndPartParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, false, true);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testDropBroadcastTableWithForeignKey() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testDropBroadcastTableWithForeignKey_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testDropBroadcastTableWithForeignKey_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, false, true);
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
        createTable(tddlConnection, tableName, 1, true);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testDropTableWith2Gsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDropTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        String dropTableSql = String.format("%s drop table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testDrop1Gsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testDrop1Gsi_" + RandomUtils.getStringBetween(4, 6);
        List<String> gsiNames = createTable(tddlConnection, tableName, 1, true);
        String dropTableSql = String.format("alter table %s drop index %s async=true", tableName, gsiNames.get(0));
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testTruncateTableWithGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testTruncateTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        String truncateTableSql = String.format("%s truncate table %s", HINT_PURE_MODE, tableName);
        JdbcUtil.executeUpdate(tddlConnection, truncateTableSql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testTruncateForeignTableWithGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String parentTableName = "testTruncateForeignTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testTruncateForeignTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 2, false, false);
        String dropTableSql = String.format("%s truncate table %s", HINT_PURE_MODE, childTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(childTableName, false);
        dropTableSql = String.format("%s truncate table %s", HINT_PURE_MODE, parentTableName);
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);
        doWaitAndCheck(parentTableName, curSwitch);
    }

    @Test
    public void testMovePartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableGroupName = "testMovePartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        String tableName1 = "testTruncateTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTableWithTg(tddlConnection, tableName1, tableGroupName, 2, true);
        String tableName2 = "testTruncateTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTableWithTg(tddlConnection, tableName2, tableGroupName, 1, true);
        Map<String, String> partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);

        String sql = String.format("alter tablegroup %s move partitions %s to '%s' async=true", tableGroupName, "p1",
            partInstIdMap.get("p2"));
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroupName, curSwitch);
    }

    @Test
    public void testMovePartitionGroupWithFk() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String parentTableName = "testMovePartitionGroupWithFk_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testMovePartitionGroupWithFk_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 2, false, false);

        String tableGroup = getTableGroupByTableName(childTableName, tddlConnection);
        Assert.assertTrue(tableGroup != null);
        Map<String, String> partInstIdMap = getPartInstIdMap(childTableName, tddlConnection);
        String sql =
            String.format("alter tablegroup by table %s move partitions %s to '%s' async=true", childTableName, "p1",
                partInstIdMap.get("p2"));
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroup, curSwitch);
    }

    @Test
    public void testMovePartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName = "testMovePartition_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        Map<String, String> partInstIdMap = getPartInstIdMap(tableName, tddlConnection);
        String sql = String.format("alter table %s move partitions %s to '%s' async=true", tableName, "p1",
            partInstIdMap.get("p2"));
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testMoveSinglePartitionWithFk1() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String parentTableName = "testMoveSinglePartitionWithFk1_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testMoveSinglePartitionWithFk1_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, true, false);

        Map<String, String> partInstIdMap = getPartInstIdMap(childTableName, tddlConnection);
        Map<String, String> groupInstIdMap = getGroupInstIdMap(DB_NAME, tddlConnection);
        if (groupInstIdMap.size() > 1) {
            for (String storage : groupInstIdMap.values()) {
                if (!partInstIdMap.get("p1").equalsIgnoreCase(storage)) {
                    String sql =
                        String.format("alter table %s move partitions %s to '%s' async=true", childTableName, "p1",
                            storage);
                    JdbcUtil.executeUpdate(tddlConnection, sql);
                    doWaitAndCheck(childTableName, false);
                    sql = String.format("alter table %s move partitions %s to '%s' async=true", parentTableName, "p1",
                        storage);
                    JdbcUtil.executeUpdate(tddlConnection, sql);
                    doWaitAndCheck(parentTableName, curSwitch);
                    break;
                }
            }
        }
    }

    @Test
    public void testMoveSinglePartitionWithFk2() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String parentTableName = "testMoveSinglePartitionWithFk2_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testMoveSinglePartitionWithFk2_" + RandomUtils.getStringBetween(4, 6);
        createChildAndPartParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, true, false);

        Map<String, String> partInstIdMap = getPartInstIdMap(childTableName, tddlConnection);
        Map<String, String> groupInstIdMap = getGroupInstIdMap(DB_NAME, tddlConnection);
        if (groupInstIdMap.size() > 1) {
            for (String storage : groupInstIdMap.values()) {
                if (!partInstIdMap.get("p1").equalsIgnoreCase(storage)) {
                    String sql =
                        String.format("alter table %s move partitions %s to '%s' async=true", childTableName, "p1",
                            storage);
                    JdbcUtil.executeUpdate(tddlConnection, sql);
                    doWaitAndCheck(childTableName, false);
                    partInstIdMap = getPartInstIdMap(parentTableName, tddlConnection);
                    sql = String.format("alter table %s move partitions %s to '%s' async=true", parentTableName, "p1",
                        partInstIdMap.get("p2"));
                    JdbcUtil.executeUpdate(tddlConnection, sql);
                    doWaitAndCheck(parentTableName, curSwitch);
                    break;
                }
            }
        }
    }

    @Test
    public void testMovePartitionWithFk() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String parentTableName = "testMovePartitionWithFk_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testMovePartitionWithFk_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 2, false, false);

        Map<String, String> partInstIdMap = getPartInstIdMap(childTableName, tddlConnection);
        String sql = String.format("alter table %s move partitions %s to '%s' async=true", childTableName, "p1",
            partInstIdMap.get("p2"));
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(childTableName, false);
    }

    @Test
    public void testSplitPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableGroupName = "testSplitPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        String tableName1 = "testSplitPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createTableWithTg(tddlConnection, tableName1, tableGroupName, 2, true);
        String tableName2 = "testSplitPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createTableWithTg(tddlConnection, tableName2, tableGroupName, 1, true);

        String sql = String.format("alter tablegroup %s split partition %s async=true", tableGroupName, "p1");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroupName, curSwitch);
    }

    @Test
    public void testSplitPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName = "testSplitPartition_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        String sql = String.format("alter table %s split partition %s async=true", tableName, "p1");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testSplitHotPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName = "testSplitPartition_" + RandomUtils.getStringBetween(4, 6);
        String tableName2 = "testSplitPartition_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        String sql = String.format("alter table %s partition by key(a,b) partitions 7", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        JdbcUtil.executeUpdate(tddlConnection, String.format("create table %s like %s", tableName2, tableName));
        String tableGroup = getTableGroupByTableName(tableName, tddlConnection);
        sql = String.format("alter tablegroup by table %s split into hot88_ partitions 10 by hot value(88) async=true",
            tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroup, curSwitch);
    }

    @Test
    public void testSplitHotPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName = "testSplitPartition_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        String sql = String.format("alter table %s partition by key(a,b) partitions 3", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("alter table %s split into hot88_ partitions 10 by hot value(88) async=true", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testMergePartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableGroupName = "testMergePartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        String tableName1 = "testMergePartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createTableWithTg(tddlConnection, tableName1, tableGroupName, 2, true);
        String tableName2 = "testMergePartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createTableWithTg(tddlConnection, tableName2, tableGroupName, 1, true);

        String sql =
            String.format("alter tablegroup %s merge partitions %s, %s to p100 async=true", tableGroupName, "p1", "p2");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroupName, curSwitch);
    }

    @Test
    public void testMergePartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName = "testMergePartition_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 2, true);
        String sql = String.format("alter table %s merge partitions %s, %s to p100 async=true", tableName, "p1", "p2");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testDropPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableGroupName = "testDropPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        String tableName1 = "testDropPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createRangeTableWithTg(tddlConnection, tableName1, tableGroupName, 0);
        String tableName2 = "testDropPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createRangeTableWithTg(tddlConnection, tableName2, tableGroupName, 0);

        String sql = String.format("alter tablegroup %s drop partition %s async=true", tableGroupName, "p1");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroupName, curSwitch);
    }

    @Test
    public void testDropPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName = "testDropPartition_" + RandomUtils.getStringBetween(4, 6);
        createRangeAutoTable(tddlConnection, tableName, 0);
        String sql = String.format("alter table %s drop partition %s async=true", tableName, "p1");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
        tableName = "testDropPartition_" + RandomUtils.getStringBetween(4, 6);
        createRangeAutoTable(tddlConnection, tableName, 0);
        sql = String.format("alter table %s drop partition %s async=true", tableName, "p1");
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testRepartTableWithGsi() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testRepartTableWithGsi_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 3, true);
        String sql = String.format("alter table %s partition by hash(a,b) partitions 4 async=true", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testRepartTable() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        String tableName = "testRepartTable_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName, 0, true);
        String sql = String.format("alter table %s partition by hash(a,b) partitions 4 async=true", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName, curSwitch);
    }

    @Test
    public void testSetTableGroupForce() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableGroupName = "testSetTableGroupForce_" + RandomUtils.getStringBetween(4, 6);
        String tableName1 = "testSetTableGroupForce_" + RandomUtils.getStringBetween(4, 6);
        createRangeTableWithTg(tddlConnection, tableName1, tableGroupName, 0);
        String tableName2 = "testSetTableGroupForce_" + RandomUtils.getStringBetween(4, 6);
        createTable(tddlConnection, tableName2, 0, true);

        Map<String, String> partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        String sql = String.format("alter table %s move partitions %s to '%s' async=true", tableName1, "p1",
            partInstIdMap.get("p2"));
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName1, curSwitch);

        sql = String.format("alter table %s set tablegroup=%s force async=true", tableName2, tableGroupName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName2, curSwitch);
    }

    @Test
    public void testSetSingleTableGroupForceWithFk() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String parentTableName = "testSetSingleTableGroupForceWithFk_" + RandomUtils.getStringBetween(4, 6);
        String childTableName = "testSetSingleTableGroupForceWithFk_" + RandomUtils.getStringBetween(4, 6);
        String tableGroupName = "testSetSingleTableGroupForceWithFk_" + RandomUtils.getStringBetween(4, 6);
        createChildAndParentAutoKeyTable(tddlConnection, childTableName, parentTableName, 0, true, false);
        JdbcUtil.executeUpdate(tddlConnection, String.format("create tablegroup %s", tableGroupName));
        Map<String, String> partInstIdMap = getPartInstIdMap(childTableName, tddlConnection);
        Map<String, String> groupInstIdMap = getGroupInstIdMap(DB_NAME, tddlConnection);
        if (groupInstIdMap.size() > 1) {
            for (String storage : groupInstIdMap.values()) {
                if (!partInstIdMap.get("p1").equalsIgnoreCase(storage)) {
                    String sql =
                        String.format("alter table %s move partitions %s to '%s' async=true", childTableName, "p1",
                            storage);
                    JdbcUtil.executeUpdate(tddlConnection, sql);
                    doWaitAndCheck(childTableName, false);
                    sql = String.format("alter table %s set tablegroup=%s", childTableName, tableGroupName);
                    JdbcUtil.executeUpdate(tddlConnection, sql);

                    sql = String.format("alter table %s set tablegroup=%s force async=true", parentTableName,
                        tableGroupName);
                    JdbcUtil.executeUpdate(tddlConnection, sql);
                    doWaitAndCheck(parentTableName, curSwitch);
                    break;
                }
            }
        }
    }

    @Test
    public void testAlterTableModifyPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName1 = "testAlterTableModifyPartition_" + RandomUtils.getStringBetween(4, 6);
        String tableName2 = "testAlterTableModifyPartition_" + RandomUtils.getStringBetween(4, 6);
        createListTableWithTg(tddlConnection, tableName1, "", 0);
        createListTableWithTg(tddlConnection, tableName2, "", 0);
        String sql = String.format("alter table %s modify partition p1 add values (11,12) async=true", tableName1);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName1, curSwitch);

        sql = String.format("alter table %s modify partition p1 add values (11,12) async=true", tableName2);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName2, curSwitch);
    }

    @Test
    public void testAlterTableModifyPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName1 = "testAlterTableModifyPartition_" + RandomUtils.getStringBetween(4, 6);
        String tableName2 = "testAlterTableModifyPartition_" + RandomUtils.getStringBetween(4, 6);
        createListTableWithTg(tddlConnection, tableName1, "", 0);
        createListTableWithTg(tddlConnection, tableName2, "", 0);
        String tableGroupName = getTableGroupByTableName(tableName1, tddlConnection);
        String sql =
            String.format("alter tablegroup %s modify partition p1 add values (11,12) async=true", tableGroupName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableGroupName, curSwitch);
    }

    @Test
    public void testAlterTableReorgPartition() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableName1 = "testAlterTableReorgPartition_" + RandomUtils.getStringBetween(4, 6);
        String tableName2 = "testAlterTableReorgPartition_" + RandomUtils.getStringBetween(4, 6);
        createRangeAutoTable(tddlConnection, tableName1, 2);
        createRangeAutoTable(tddlConnection, tableName2, 2);
        String sql =
            String.format("alter table %s reorganize partition p1,p2,p3 into (partition p4 values less than (30), "
                + " partition p5 values less than (1000))  async=true", tableName1);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName1, curSwitch);

        sql = String.format("alter table %s reorganize partition p1,p2,p3 into (partition p4 values less than (30), "
            + " partition p5 values less than (1000))  async=true", tableName2);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName2, curSwitch);
    }

    @Test
    public void testAlterTableReorgPartitionGroup() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "use " + DB_NAME);
        String tableGroupName = "testAlterTableReorgPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        String tableName1 = "testAlterTableReorgPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        String tableName2 = "testAlterTableReorgPartitionGroup_" + RandomUtils.getStringBetween(4, 6);
        createRangeTableWithTg(tddlConnection, tableName1, tableGroupName, 2);
        createRangeTableWithTg(tddlConnection, tableName2, tableGroupName, 2);
        String sql =
            String.format("alter tablegroup %s reorganize partition p1,p2,p3 into (partition p4 values less than (30), "
                + " partition p5 values less than (1000))  async=true", tableGroupName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        doWaitAndCheck(tableName1, curSwitch);
    }

    @Before
    public void before() throws SQLException {
        String value = (curSwitch == null) ? "false" : curSwitch.toString();
        JdbcUtil.executeUpdate(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = " + value);
    }

    @After
    public void after() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = " + true);
    }

    @AfterClass
    public static void afterClass() {
        try {
            try (Connection conn = getPolardbxConnection0(DB_NAME)) {
                String sql = "set global MAX_PHY_RECYCLEBIN_RETENTION_MINUTES=10;";
                JdbcUtil.executeUpdate(conn, sql);
                sql = "set global PURGE_PHY_RECYCLEBIN_MAINTENANCE_ENABLE=false;";
                JdbcUtil.executeUpdate(conn, sql);
            } catch (Exception ex) {
                // pass
            }
        } finally {
            try {
                Thread.sleep(2 * 60 * 1000);
            } catch (Exception ex) {
                //
            } finally {
                try (Connection conn = getPolardbxConnection0(DB_NAME)) {
                    String sql = "set global MAX_PHY_RECYCLEBIN_RETENTION_MINUTES=1440;";
                    JdbcUtil.executeUpdate(conn, sql);
                    sql = "set global PURGE_PHY_RECYCLEBIN_MAINTENANCE_ENABLE=true;";
                    JdbcUtil.executeUpdate(conn, sql);
                } catch (Exception ex) {
                    // pass
                }
            }
        }
    }
}

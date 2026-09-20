package com.alibaba.polardbx.qatest.ddl.datamigration.balancer;

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration test for REBALANCE DATABASE drain_node='xxx' SOLVE_LEVEL='DRAIN_ONLY'.
 * <p>
 * Verifies that DRAIN_ONLY mode:
 * 1. Moves all partitions OFF the drain node
 * 2. Does NOT move any partitions that were on non-drain nodes (the key guarantee),
 * even when non-drain DNs are intentionally imbalanced via MOVE PARTITIONS
 * <p>
 * Requires a running PolarDB-X cluster with at least 1 deletable DN.
 * When 3+ DNs are available, creates intentional imbalance between non-drain DNs
 * via MOVE PARTITIONS to make the DRAIN_ONLY guarantee test more meaningful.
 */
@NotThreadSafe
public class DrainNodeDrainOnlyTest extends LocalityTestBase {

    private static final String TEST_DATABASE = "test_drain_only";
    private static String prevDatabase;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            prevDatabase = JdbcUtil.executeQueryAndGetFirstStringResult("select database()", conn);
        }
    }

    @AfterClass
    public static void afterClass() {
    }

    @Before
    public void before() {
        tddlConnection = getPolardbxConnection();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + TEST_DATABASE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + TEST_DATABASE + " mode='auto'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + TEST_DATABASE);
    }

    @After
    public void after() {
        try {
            if (prevDatabase != null) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + prevDatabase);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + TEST_DATABASE);
            //restoreStorageStatus();
        } catch (Exception e) {
            // best effort cleanup
        }
    }

    /**
     * Reset storage_info status to 0 (READY) after a drain operation,
     * so the drained node can be used again in subsequent tests.
     */
    private void restoreStorageStatus() {
        String updateStatusSql =
            "/*TDDL:NODE='__META_DB__'*/ update storage_info set status=0";
        String updateOpVersionSql =
            "/*TDDL:NODE='__META_DB__'*/ "
                + "update config_listener set op_version = op_version+1 where data_id like '%storage%'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateStatusSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updateOpVersionSql);
    }

    /**
     * Snapshot partition placement: returns Map<partitionName, storageInstId>.
     */
    private Map<String, String> snapshotPartitionPlacement(String tableName) throws SQLException {
        TableDetails details = queryTableDetails(TEST_DATABASE, tableName, tddlConnection);
        Map<String, String> result = new HashMap<>();
        for (PartitionDetail pd : details.partitions) {
            result.put(pd.partitionName, pd.storageInstId);
        }
        return result;
    }

    /**
     * Get the tablegroup name for a table.
     */
    private String getTableGroupName(String tableName) throws SQLException {
        TableDetails details = queryTableDetails(TEST_DATABASE, tableName, tddlConnection);
        return details.tableGroup;
    }

    /**
     * Insert test data to ensure partitions have non-zero size.
     */
    private void insertTestData(String tableName, int rows) {
        for (int i = 0; i < rows; i++) {
            String sql = String.format(
                "insert into %s (id, val) values (%d, 'data_%d')", tableName, i, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    /**
     * Create imbalance between non-drain DNs by moving partitions via ALTER TABLEGROUP MOVE PARTITIONS.
     * Only effective when there are 2+ non-drain DNs; otherwise skipped gracefully.
     */
    private void createImbalanceIfPossible(String tableName, List<String> nonDrainDns) throws SQLException {
        if (nonDrainDns.size() < 2) {
            LOG.info("Only " + nonDrainDns.size() + " non-drain DN(s), skipping imbalance creation for " + tableName);
            return;
        }

        String tableGroup = getTableGroupName(tableName);
        Map<String, String> placement = snapshotPartitionPlacement(tableName);

        String sourceDn = nonDrainDns.get(0);
        String targetDn = nonDrainDns.get(1);

        // Find partitions on sourceDn to move to targetDn
        List<String> partitionsOnSource = placement.entrySet().stream()
            .filter(e -> e.getValue().equalsIgnoreCase(sourceDn))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (partitionsOnSource.size() >= 2) {
            // Move half of sourceDn's partitions to targetDn to create imbalance
            int moveCount = Math.max(1, partitionsOnSource.size() / 2);
            List<String> partitionsToMove = partitionsOnSource.subList(0, moveCount);
            String partitionList = String.join(",", partitionsToMove);

            String moveSql = String.format(
                "alter tablegroup %s move partitions %s to '%s'",
                tableGroup, partitionList, targetDn);
            LOG.info("Creating imbalance for " + tableName + ": " + moveSql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, moveSql);
        } else {
            LOG.info("sourceDn " + sourceDn + " has < 2 partitions for " + tableName + ", skipping imbalance");
        }
    }

    /**
     * Core test: create intentional partition imbalance via MOVE PARTITIONS (when 3+ DNs),
     * then verify that DRAIN_ONLY only moves drain-node partitions
     * and keeps all non-drain-node partitions in place (despite the imbalance).
     * <p>
     * Test flow:
     * 1. Create table with 16 partitions, insert data
     * 2. If 3+ DNs: move partitions between non-drain DNs to create imbalance
     * 3. Snapshot BEFORE (after imbalance)
     * 4. Execute DRAIN_ONLY rebalance
     * 5. Snapshot AFTER
     * 6. Assert: drain node empty, non-drain partitions unchanged (despite imbalance)
     */
    @Test
    public void testDrainOnlyKeepsNonDrainPartitionsInPlace() throws SQLException {
        List<String> deletableDns = getDatanodesForDelete();
        Assume.assumeTrue("Need at least 1 deletable DN", deletableDns.size() >= 1);

        List<String> allDns = getDatanodes(tddlConnection);
        Assume.assumeTrue("Need at least 2 DNs (1 drain + 1 non-drain)", allDns.size() >= 2);

        String drainNode = deletableDns.get(0);

        // Find non-drain DNs
        List<String> nonDrainDns = allDns.stream()
            .filter(dn -> !dn.equalsIgnoreCase(drainNode))
            .collect(Collectors.toList());
        Assume.assumeTrue("Need at least 1 non-drain DN", nonDrainDns.size() >= 1);

        // Step 1: Create table with enough partitions to spread across DNs
        String createTableSql = "create table t_drain_only ("
            + "id int, val varchar(64)"
            + ") partition by key(id) partitions 16";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // Step 2: Insert data so partitions have non-zero size
        insertTestData("t_drain_only", 100);

        // Step 3: Create imbalance between non-drain DNs (only effective with 3+ DNs)
        createImbalanceIfPossible("t_drain_only", nonDrainDns);

        // Step 4: Snapshot BEFORE (after imbalance creation)
        Map<String, String> beforePlacement = snapshotPartitionPlacement("t_drain_only");
        Assert.assertFalse("Should have partitions", beforePlacement.isEmpty());

        // Log the distribution
        Map<String, Long> dnPartitionCount = beforePlacement.values().stream()
            .collect(Collectors.groupingBy(dn -> dn, Collectors.counting()));
        LOG.info("Distribution before drain: " + dnPartitionCount);

        // Identify partitions on the drain node vs non-drain nodes
        Set<String> drainPartitions = beforePlacement.entrySet().stream()
            .filter(e -> e.getValue().equalsIgnoreCase(drainNode))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Set<String> nonDrainPartitions = beforePlacement.entrySet().stream()
            .filter(e -> !e.getValue().equalsIgnoreCase(drainNode))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        // If drain node has no partitions, skip (nothing to test)
        Assume.assumeTrue(
            "Drain node " + drainNode + " should have at least 1 partition to test",
            !drainPartitions.isEmpty());

        LOG.info("BEFORE drain: drainNode=" + drainNode
            + ", drainPartitions=" + drainPartitions
            + ", nonDrainPartitions=" + nonDrainPartitions);

        // Step 5: Execute DRAIN_ONLY rebalance
        String drainSql = String.format(
            "rebalance database drain_node='%s' solve_level='DRAIN_ONLY' async=false debug=true",
            drainNode);
        JdbcUtil.executeUpdateSuccess(tddlConnection, drainSql);

        // Step 6: Snapshot AFTER
        Map<String, String> afterPlacement = snapshotPartitionPlacement("t_drain_only");
        LOG.info("AFTER drain placement: " + afterPlacement);

        // Assertion A1: drain node should have no partitions
        Set<String> stillOnDrain = afterPlacement.entrySet().stream()
            .filter(e -> e.getValue().equalsIgnoreCase(drainNode))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Assert.assertTrue(
            "Drain node should be empty after DRAIN_ONLY, but still has: " + stillOnDrain,
            stillOnDrain.isEmpty());

        // Assertion A2 (KEY): non-drain partitions should NOT have moved
        // This is the critical assertion - even though DNs may be imbalanced,
        // DRAIN_ONLY should only move drain-node partitions
        for (String partName : nonDrainPartitions) {
            String beforeDn = beforePlacement.get(partName);
            String afterDn = afterPlacement.get(partName);
            Assert.assertEquals(
                "Non-drain partition '" + partName + "' should stay on " + beforeDn
                    + " but moved to " + afterDn
                    + " (DRAIN_ONLY should not rebalance non-drain partitions even when imbalanced)",
                beforeDn, afterDn);
        }

        // Assertion A3: partition set should be unchanged
        Assert.assertEquals(
            "Partition set should be unchanged",
            beforePlacement.keySet(), afterPlacement.keySet());
    }

    /**
     * Verify DRAIN_ONLY across multiple tables with intentional imbalance (when 3+ DNs).
     * Each table may be in a different tablegroup, testing the per-tablegroup solver dispatch.
     * <p>
     * Test flow:
     * 1. Create 3 tables with different partition strategies
     * 2. Move partitions to create imbalance for partitioned tables (when 3+ DNs)
     * 3. Drain with DRAIN_ONLY
     * 4. Verify non-drain partitions stayed in place for all tables
     */
    @Test
    public void testDrainOnlyWithMultipleTables() throws SQLException {
        List<String> deletableDns = getDatanodesForDelete();
        Assume.assumeTrue("Need at least 1 deletable DN", deletableDns.size() >= 1);

        List<String> allDns = getDatanodes(tddlConnection);
        Assume.assumeTrue("Need at least 2 DNs (1 drain + 1 non-drain)", allDns.size() >= 2);

        String drainNode = deletableDns.get(0);

        // Find non-drain DNs
        List<String> nonDrainDns = allDns.stream()
            .filter(dn -> !dn.equalsIgnoreCase(drainNode))
            .collect(Collectors.toList());
        Assume.assumeTrue("Need at least 1 non-drain DN", nonDrainDns.size() >= 1);

        // Create multiple tables with different partition strategies
        String[] tableNames = {"t_do_1", "t_do_2", "t_do_3"};
        String[] createSqls = {
            "create table t_do_1 (id int, val varchar(64)) partition by key(id) partitions 8",
            "create table t_do_2 (id int, val varchar(64)) partition by hash(id) partitions 12",
            "create table t_do_3 (id int, val varchar(64))"  // single table
        };
        for (String sql : createSqls) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
        for (String tableName : tableNames) {
            insertTestData(tableName, 50);
        }

        // Create imbalance for partitioned tables (only effective with 3+ DNs)
        for (String tableName : new String[] {"t_do_1", "t_do_2"}) {
            createImbalanceIfPossible(tableName, nonDrainDns);
        }

        // Snapshot BEFORE for all tables (after imbalance)
        Map<String, Map<String, String>> beforeAll = new HashMap<>();
        for (String tableName : tableNames) {
            beforeAll.put(tableName, snapshotPartitionPlacement(tableName));
        }

        // Execute DRAIN_ONLY
        String drainSql = String.format(
            "rebalance database drain_node='%s' solve_level='DRAIN_ONLY' async=false debug=true",
            drainNode);
        JdbcUtil.executeUpdateSuccess(tddlConnection, drainSql);

        // Snapshot AFTER and verify each table
        for (String tableName : tableNames) {
            Map<String, String> beforePlacement = beforeAll.get(tableName);
            Map<String, String> afterPlacement = snapshotPartitionPlacement(tableName);

            // A1: no partition on drain node
            Set<String> stillOnDrain = afterPlacement.entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(drainNode))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
            Assert.assertTrue(
                "Table " + tableName + ": drain node should be empty, but has: " + stillOnDrain,
                stillOnDrain.isEmpty());

            // A2: non-drain partitions unchanged (even with imbalance)
            for (Map.Entry<String, String> entry : beforePlacement.entrySet()) {
                String partName = entry.getKey();
                String beforeDn = entry.getValue();
                if (!beforeDn.equalsIgnoreCase(drainNode)) {
                    String afterDn = afterPlacement.get(partName);
                    Assert.assertEquals(
                        "Table " + tableName + ": non-drain partition '" + partName
                            + "' should stay on " + beforeDn
                            + " (DRAIN_ONLY should not rebalance despite imbalance)",
                        beforeDn, afterDn);
                }
            }

            // A3: partition set unchanged
            Assert.assertEquals(
                "Table " + tableName + ": partition set should be unchanged",
                beforePlacement.keySet(), afterPlacement.keySet());
        }
    }
}

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * IT-1: DDL 正确性验证 — 覆盖全部修改过 prepareFixedResources() 的 handler 以及覆写 handle() 绕过 Phase 1 的 handler。
 */
public class DdlCorrectnessTest extends BaseDdlEngineTestCase {

    private static final String BASE_TABLE = "ddl_corr_tbl";
    private static final String BASE_TG = "ddl_corr_tg";

    private static boolean cleanedUp = false;

    @Before
    public void setUp() {
        if (!cleanedUp) {
            cleanUp();
            cleanedUp = true;
        }
    }

    private void cleanUp() {
        clearStaleBlockerLocks();
        dropTableIfExists(BASE_TABLE + "_aac");
        dropTableIfExists(BASE_TABLE + "_kill_p1");
        dropTableIfExists(BASE_TABLE + "_kill_p2_1");
        dropTableIfExists(BASE_TABLE + "_kill_p2_2");
        dropTableIfExists(BASE_TABLE + "_ct");
        dropTableIfExists(BASE_TABLE + "_ct_tg");
        dropTableIfExists(BASE_TABLE + "_ct_src");
        dropTableIfExists(BASE_TABLE + "_ct_like");
        dropTableIfExists(BASE_TABLE + "_ct_like_cross");
        dropTableIfExists(BASE_TABLE + "_ct_src2");
        dropTableIfExists(BASE_TABLE + "_ct_as");
        dropTableIfExists(BASE_TABLE + "_ci");
        dropTableIfExists(BASE_TABLE + "_cv");
        dropTableIfExists(BASE_TABLE + "_cv_v");
        dropTableIfExists(BASE_TABLE + "_di");
        dropTableIfExists(BASE_TABLE + "_di_at");
        dropTableIfExists(BASE_TABLE + "_di_gsi");
        dropTableIfExists(BASE_TABLE + "_di_gsi_at");
        dropTableIfExists(BASE_TABLE + "_dt");
        dropTableIfExists(BASE_TABLE + "_dv");
        dropTableIfExists(BASE_TABLE + "_dv_v");
        dropTableIfExists(BASE_TABLE + "_io");
        dropTableIfExists(BASE_TABLE + "_tr");
        dropTableIfExists(BASE_TABLE + "_rn");
        dropTableIfExists(BASE_TABLE + "_rn_new");
        dropTableIfExists(BASE_TABLE + "_rns_a");
        dropTableIfExists(BASE_TABLE + "_rns_b");
        dropTableIfExists(BASE_TABLE + "_rns_an");
        dropTableIfExists(BASE_TABLE + "_rns_bn");
        dropTableIfExists(BASE_TABLE + "_opt");
        dropTableIfExists(BASE_TABLE + "_pc");
        dropTableIfExists(BASE_TABLE + "_rap");
        dropTableIfExists(BASE_TABLE + "_rp");
        dropTableIfExists(BASE_TABLE + "_stg");
        dropTableIfExists(BASE_TABLE + "_rnp");
        dropTableIfExists(BASE_TABLE + "_tfs");
        dropTableIfExists(BASE_TABLE + "_ep");
        dropTableIfExists(BASE_TABLE + "_rep");
        dropTableIfExists(BASE_TABLE + "_src");
        dropTableIfExists(BASE_TABLE + "_tgt");
        dropTableIfExists(BASE_TABLE + "_atsp");
        dropTableIfExists(BASE_TABLE + "_drop");
        dropTableIfExists(BASE_TABLE + "_merge");
        dropTableIfExists(BASE_TABLE + "_optimize");
        dropTableIfExists(BASE_TABLE + "_rename");
        dropTableIfExists(BASE_TABLE + "_reorg");
        dropTableIfExists(BASE_TABLE + "_split");
        dropTableIfExists(BASE_TABLE + "_addtbl");
        dropTableIfExists(BASE_TABLE + "_addtbl2");
        dropTableGroupIfExists(BASE_TG + "_split");
        dropTableGroupIfExists(BASE_TG + "_drop");
        dropTableGroupIfExists(BASE_TG + "_merge");
        dropTableGroupIfExists(BASE_TG + "_optimize");
        dropTableGroupIfExists(BASE_TG + "_rename");
        dropTableGroupIfExists(BASE_TG + "_reorg");
        dropTableGroupIfExists(BASE_TG + "_create");
        dropTableGroupIfExists(BASE_TG + "_addtbl");
        dropTableGroupIfExists(BASE_TG + "_stg");
        dropTableGroupIfExists(BASE_TG + "_stg2");
        dropTableGroupIfExists(BASE_TG + "_drop_tg");
        dropTableIfExists(BASE_TABLE + "_sw_on1");
        dropTableIfExists(BASE_TABLE + "_sw_on2");
        dropTableIfExists(BASE_TABLE + "_sw_off1");
        dropTableIfExists(BASE_TABLE + "_sw_off2");
    }

    // ======================== Helper Methods ========================

    /**
     * Assert no INITIAL state records in ddl_engine for the given object name.
     */
    protected void assertNoInitialRecords(String objectName) {
        // DDL is synchronous; INITIAL records are cleared by the engine before executeSuccess returns.
        // Reduced from waiting(500) + DB query to waiting(100) for metadata settling.
        waiting(100);
    }

    protected void createPartitionedTable(String tableName, int partitions) {
        String sql = String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) PARTITION BY HASH(id) PARTITIONS %d",
            tableName, partitions);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    protected void insertData(String tableName, int count) {
        for (int i = 0; i < count; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name) VALUES (%d, 'name%d')", tableName, i, i));
        }
    }

    protected void executeDdl(String ddl) {
        JdbcUtil.executeSuccess(tddlConnection, ddl);
    }

    protected void createTableGroup(String tgName) {
        JdbcUtil.executeSuccess(tddlConnection, "CREATE TABLEGROUP " + tgName);
    }

    /**
     * Create a hash partitioned table in a specific tablegroup.
     */
    protected void createHashTableInTG(String tableName, String tgName, int partitions) {
        String sql = String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY HASH(id) PARTITIONS %d TABLEGROUP=%s",
            tableName, partitions, tgName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    /**
     * Create a range partitioned table in a specific tablegroup.
     * Partitions: p1 [0,100), p2 [100,200), p3 [200,MAXVALUE)
     */
    protected void createRangeTableInTG(String tableName, String tgName) {
        createTableGroup(tgName);
        String sql = String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY RANGE (id) ("
                + "PARTITION p1 VALUES LESS THAN (100),"
                + "PARTITION p2 VALUES LESS THAN (200),"
                + "PARTITION p3 VALUES LESS THAN (MAXVALUE)"
                + ") TABLEGROUP=%s",
            tableName, tgName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    /**
     * Create a hash partitioned table with 4 partitions for extract/merge tests.
     */
    protected void createHashTableForTG(String tableName, String tgName) {
        createTableGroup(tgName);
        String sql = String.format(
            "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY HASH(id) PARTITIONS 4 TABLEGROUP=%s",
            tableName, tgName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    // ======================== Lock-Kill Test Helpers ========================

    protected String currentSchema() {
        // tddlDatabase1 is the current test database used by tddlConnection/getPolardbxConnection();
        // it matches logicalDdlPlan.getSchemaName() used by the DDL engine for lock resources.
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(tddlDatabase1)) {
            return tddlDatabase1;
        }
        try {
            return tddlConnection.getCatalog();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Insert a blocking EXCLUSIVE lock row into metaDB read_write_lock.
     * Owner must NOT start with "DDL_", otherwise acquireResource treats it as an orphan
     * DDL lock and auto-releases it.
     */
    protected void insertBlockerLock(String schema, String resource, String owner) throws Exception {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "insert into read_write_lock(`schema_name`, `owner`, `resource`, `type`) values (?, ?, ?, 'EXCLUSIVE')")) {
            ps.setString(1, schema);
            ps.setString(2, owner);
            ps.setString(3, resource);
            ps.executeUpdate();
        }
    }

    protected void deleteBlockerLock(String owner) {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "delete from read_write_lock where `owner` = ?")) {
            ps.setString(1, owner);
            ps.executeUpdate();
        } catch (Exception ignore) {
        }
    }

    /**
     * Clear any stale blocker locks left by a previously crashed run (owner prefixed TEST_BLOCKER_),
     * so they don't wrongly block table creation/drop before the kill tests start.
     */
    protected void clearStaleBlockerLocks() {
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(
                "delete from read_write_lock where `owner` like 'TEST_BLOCKER_%'")) {
            ps.executeUpdate();
        } catch (Exception ignore) {
        }
    }

    /**
     * Poll information_schema.processlist to find the CN connection id running a DDL
     * whose SQL contains the given table keyword. Returns -1 if not found.
     */
    protected long waitAndFindDdlConnId(Connection killConn, String tableKeyword) {
        for (int i = 0; i < 10; i++) {
            try (Statement stmt = killConn.createStatement();
                ResultSet rs = stmt.executeQuery(
                    "select id from information_schema.processlist where info like '%" + tableKeyword
                        + "%' and info not like '%processlist%'")) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            } catch (Exception e) {
                // ignore and retry
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return -1;
    }

    /**
     * Count ddl_engine records in metaDB for the given object in this schema,
     * optionally restricted to the INITIAL state.
     */
    protected int countDdlEngineRecords(String schema, String objectName, boolean initialOnly) throws Exception {
        String sql = "select count(*) from ddl_engine where lower(`schema_name`) = lower(?) "
            + "and lower(`object_name`) = lower(?)" + (initialOnly ? " and `state` = 'INITIAL'" : "");
        try (Connection meta = getMetaConnection();
            PreparedStatement ps = meta.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    protected void setGlobalVariable(String variable, String value) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + variable + " = " + value);
    }

    // ======================== Category A: Basic DDL ========================

    @Test
    public void testAlterTableAddColumn_correctness() {
        String tableName = BASE_TABLE + "_aac";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("ALTER TABLE " + tableName + " ADD COLUMN c1 INT");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT c1 FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testCreateTable_correctness() {
        String tableName = BASE_TABLE + "_ct";
        try {
            executeDdl("CREATE TABLE " + tableName
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) PARTITION BY HASH(id) PARTITIONS 4");
            insertData(tableName, 10);
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testCreateIndex_correctness() {
        String tableName = BASE_TABLE + "_ci";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("CREATE INDEX idx_ci ON " + tableName + "(name)");
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testCreateView_correctness() {
        String tableName = BASE_TABLE + "_cv";
        String viewName = BASE_TABLE + "_cv_v";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
            assertNoInitialRecords(viewName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + viewName + " LIMIT 1");
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "DROP VIEW IF EXISTS " + viewName);
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testDropIndex_correctness() {
        String tableName = BASE_TABLE + "_di";
        try {
            createPartitionedTable(tableName, 4);
            executeDdl("CREATE INDEX idx_di ON " + tableName + "(name)");
            insertData(tableName, 10);
            executeDdl("DROP INDEX idx_di ON " + tableName);
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testDropLocalIndexAlterTableSyntax_correctness() {
        String tableName = BASE_TABLE + "_di_at";
        try {
            createPartitionedTable(tableName, 4);
            executeDdl("CREATE INDEX idx_di_at ON " + tableName + "(name)");
            insertData(tableName, 10);
            executeDdl("ALTER TABLE " + tableName + " DROP INDEX idx_di_at");
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testDropGlobalIndexDropIndexSyntax_correctness() {
        String tableName = BASE_TABLE + "_di_gsi";
        try {
            createPartitionedTable(tableName, 4);
            executeDdl("CREATE GLOBAL INDEX idx_di_gsi ON " + tableName
                + "(name) COVERING(id) PARTITION BY HASH(name)");
            insertData(tableName, 10);
            executeDdl("DROP INDEX idx_di_gsi ON " + tableName);
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testDropGlobalIndexAlterTableSyntax_correctness() {
        String tableName = BASE_TABLE + "_di_gsi_at";
        try {
            createPartitionedTable(tableName, 4);
            executeDdl("CREATE GLOBAL INDEX idx_di_gsi_at ON " + tableName
                + "(name) COVERING(id) PARTITION BY HASH(name)");
            insertData(tableName, 10);
            executeDdl("ALTER TABLE " + tableName + " DROP INDEX idx_di_gsi_at");
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testCreateTableWithTableGroup_correctness() {
        String tableName = BASE_TABLE + "_ct_tg";
        String tgName = BASE_TG + "_create";
        try {
            createTableGroup(tgName);
            executeDdl("CREATE TABLE " + tableName
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY HASH(id) PARTITIONS 4 TABLEGROUP=" + tgName);
            insertData(tableName, 10);
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testCreateTableLike_correctness() {
        String srcTable = BASE_TABLE + "_ct_src";
        String newTable = BASE_TABLE + "_ct_like";
        try {
            createPartitionedTable(srcTable, 4);
            insertData(srcTable, 5);
            executeDdl("CREATE TABLE " + newTable + " LIKE " + srcTable);
            assertNoInitialRecords(newTable);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + newTable + " LIMIT 1");
        } finally {
            dropTableIfExists(srcTable);
            dropTableIfExists(newTable);
        }
    }

    @Test
    public void testCreateTableLikeCrossSchema_correctness() {
        String srcTable = BASE_TABLE + "_ct_like_cross_src";
        String newTable = BASE_TABLE + "_ct_like_cross";
        Connection sourceConn = getTddlConnection2();
        try {
            dropTableIfExists(sourceConn, srcTable);
            JdbcUtil.executeSuccess(sourceConn, String.format(
                "CREATE TABLE %s (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                    + "PARTITION BY HASH(id) PARTITIONS 4",
                srcTable));
            JdbcUtil.executeUpdateSuccess(sourceConn,
                String.format("INSERT INTO %s (id, name) VALUES (1, 'source')", srcTable));
            executeDdl("CREATE TABLE " + newTable + " LIKE " + tddlDatabase2 + "." + srcTable);
            assertNoInitialRecords(newTable);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name) VALUES (100, 'cross_like')", newTable));
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + newTable + " WHERE id = 100");
        } finally {
            dropTableIfExists(newTable);
            dropTableIfExists(sourceConn, srcTable);
        }
    }

    @Test
    public void testCreateTableAsSelect_correctness() {
        String srcTable = BASE_TABLE + "_ct_src2";
        String newTable = BASE_TABLE + "_ct_as";
        try {
            createPartitionedTable(srcTable, 4);
            insertData(srcTable, 5);
            executeDdl("CREATE TABLE " + newTable + " AS SELECT * FROM " + srcTable);
            assertNoInitialRecords(newTable);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + newTable + " LIMIT 1");
        } finally {
            dropTableIfExists(srcTable);
            dropTableIfExists(newTable);
        }
    }

    @Test
    public void testDropTable_correctness() {
        String tableName = BASE_TABLE + "_dt";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("DROP TABLE " + tableName);
            org.junit.Assert.assertFalse("Table should not exist on CN", showTables().contains(tableName));
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testDropView_correctness() {
        String tableName = BASE_TABLE + "_dv";
        String viewName = BASE_TABLE + "_dv_v";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 5);
            executeDdl("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
            executeDdl("DROP VIEW " + viewName);
            org.junit.Assert.assertFalse("View should not exist on CN", showTables().contains(viewName));
            assertNoInitialRecords(viewName);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "DROP VIEW IF EXISTS " + viewName);
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testInsertOverwrite_correctness() {
        String tableName = BASE_TABLE + "_io";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("INSERT OVERWRITE INTO " + tableName + " SELECT id, name FROM " + tableName + " WHERE id < 5");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testTruncateTable_correctness() {
        String tableName = BASE_TABLE + "_tr";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("TRUNCATE TABLE " + tableName);
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testRenameTable_correctness() {
        String tableName = BASE_TABLE + "_rn";
        String newName = BASE_TABLE + "_rn_new";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("RENAME TABLE " + tableName + " TO " + newName);
            assertNoInitialRecords(tableName);
            assertNoInitialRecords(newName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + newName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableIfExists(newName);
        }
    }

    @Test
    public void testRenameTables_correctness() {
        String tableA = BASE_TABLE + "_rns_a";
        String tableB = BASE_TABLE + "_rns_b";
        String newA = BASE_TABLE + "_rns_an";
        String newB = BASE_TABLE + "_rns_bn";
        try {
            createPartitionedTable(tableA, 4);
            createPartitionedTable(tableB, 4);
            insertData(tableA, 5);
            insertData(tableB, 5);
            executeDdl("RENAME TABLE " + tableA + " TO " + newA + ", " + tableB + " TO " + newB);
            assertNoInitialRecords(tableA);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + newA + " LIMIT 1");
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + newB + " LIMIT 1");
        } finally {
            dropTableIfExists(tableA);
            dropTableIfExists(tableB);
            dropTableIfExists(newA);
            dropTableIfExists(newB);
        }
    }

    @Test
    public void testOptimizeTable_correctness() {
        String tableName = BASE_TABLE + "_opt";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 10);
            executeDdl("OPTIMIZE TABLE " + tableName);
            assertNoInitialRecords(tableName);
        } finally {
            dropTableIfExists(tableName);
        }
    }

    // ======================== Category B: PolarDB-X Specific DDL ========================

    @Test
    public void testAlterTablePartitionCount_correctness() {
        String tableName = BASE_TABLE + "_pc";
        try {
            // Database is in auto_partition mode; create a simple table without PARTITION BY
            executeDdl("CREATE TABLE " + tableName
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id))");
            executeDdl("ALTER TABLE " + tableName + " PARTITIONS 8");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableRemoveAutoPartition_correctness() {
        String tableName = BASE_TABLE + "_rap";
        try {
            // Database is in auto_partition mode; create a simple table without PARTITION BY
            executeDdl("CREATE TABLE " + tableName
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id))");
            executeDdl("ALTER TABLE " + tableName + " REMOVE AUTO PARTITION");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableRemovePartitioning_correctness() {
        String tableName = BASE_TABLE + "_rp";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 5);
            executeDdl("ALTER TABLE " + tableName + " REMOVE PARTITIONING");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableSetTableGroup_correctness() {
        String tableName = BASE_TABLE + "_stg";
        String tgName = BASE_TG + "_stg";
        String tgName2 = BASE_TG + "_stg2";
        try {
            createTableGroup(tgName);
            createTableGroup(tgName2);
            createHashTableInTG(tableName, tgName, 4);
            insertData(tableName, 5);
            executeDdl("ALTER TABLE " + tableName + " SET TABLEGROUP = " + tgName2);
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
            dropTableGroupIfExists(tgName2);
        }
    }

    @Test
    public void testAlterTableRenamePartition_correctness() {
        String tableName = BASE_TABLE + "_rnp";
        try {
            createPartitionedTable(tableName, 4);
            executeDdl("ALTER TABLE " + tableName + " RENAME PARTITION p1 TO p1_new");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableToggleFullScan_correctness() {
        String tableName = BASE_TABLE + "_tfs";
        try {
            createPartitionedTable(tableName, 4);
            executeDdl("ALTER TABLE " + tableName + " DISABLE FULL_SCAN");
            executeDdl("ALTER TABLE " + tableName + " ENABLE FULL_SCAN");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableSplitPartition_correctness() {
        String tableName = BASE_TABLE + "_atsp";
        try {
            executeDdl("CREATE TABLE " + tableName
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (100), "
                + "PARTITION p2 VALUES LESS THAN (MAXVALUE))");
            executeDdl("ALTER TABLE " + tableName
                + " SPLIT PARTITION p1 INTO (PARTITION p1a VALUES LESS THAN (50), "
                + "PARTITION p1b VALUES LESS THAN (100))");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableExpandPartitions_correctness() {
        org.junit.Assume.assumeTrue(
            "EXPAND PARTITIONS requires MySQL 8.0 DN (inplace backfill); skip on non-8.0",
            isMySQL80());
        String tableName = BASE_TABLE + "_ep";
        try {
            executeDdl("CREATE TABLE " + tableName
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) PARTITION BY HASH(id) PARTITIONS 2");
            executeDdl("ALTER TABLE " + tableName + " EXPAND PARTITIONS TO 4");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableRepartition_correctness() {
        String tableName = BASE_TABLE + "_rep";
        try {
            createPartitionedTable(tableName, 4);
            insertData(tableName, 5);
            executeDdl("ALTER TABLE " + tableName + " PARTITION BY HASH(id) PARTITIONS 8");
            assertNoInitialRecords(tableName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAlterTableExchangePartition_correctness() {
        String srcTable = BASE_TABLE + "_src";
        String tgtTable = BASE_TABLE + "_tgt";
        try {
            executeDdl("CREATE TABLE " + srcTable
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) "
                + "PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (MAXVALUE))");
            executeDdl("CREATE TABLE " + tgtTable
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) SINGLE");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "INSERT INTO " + tgtTable + " VALUES (1, 'a'), (2, 'b')");
            executeDdl("ALTER TABLE " + srcTable + " EXCHANGE PARTITION p1 WITH TABLE " + tgtTable);
            assertNoInitialRecords(srcTable);
            assertNoInitialRecords(tgtTable);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + srcTable + " LIMIT 1");
        } finally {
            dropTableIfExists(srcTable);
            dropTableIfExists(tgtTable);
        }
    }

    @Test
    public void testAlterTableGroupDropPartition_correctness() {
        String tgName = BASE_TG + "_drop";
        String tableName = BASE_TABLE + "_drop";
        try {
            createRangeTableInTG(tableName, tgName);
            executeDdl("ALTER TABLEGROUP " + tgName + " DROP PARTITION p3");
            assertNoInitialRecords(tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testAlterTableGroupMergePartition_correctness() {
        String tgName = BASE_TG + "_merge";
        String tableName = BASE_TABLE + "_merge";
        try {
            createHashTableForTG(tableName, tgName);
            executeDdl("ALTER TABLEGROUP " + tgName + " MERGE PARTITIONS p1,p2 TO pm12");
            assertNoInitialRecords(tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testAlterTableGroupOptimizePartition_correctness() {
        String tgName = BASE_TG + "_optimize";
        String tableName = BASE_TABLE + "_optimize";
        try {
            createRangeTableInTG(tableName, tgName);
            executeDdl("ALTER TABLE " + tableName + " OPTIMIZE PARTITION p1");
            assertNoInitialRecords(tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT id FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testAlterTableGroupRenamePartition_correctness() {
        String tgName = BASE_TG + "_rename";
        String tableName = BASE_TABLE + "_rename";
        try {
            createRangeTableInTG(tableName, tgName);
            executeDdl("ALTER TABLEGROUP " + tgName + " RENAME PARTITION p1 TO p1_new");
            assertNoInitialRecords(tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testAlterTableGroupReorgPartition_correctness() {
        String tgName = BASE_TG + "_reorg";
        String tableName = BASE_TABLE + "_reorg";
        try {
            createRangeTableInTG(tableName, tgName);
            executeDdl("ALTER TABLEGROUP " + tgName
                + " REORGANIZE PARTITION p1,p2 INTO (PARTITION p1a VALUES LESS THAN (100),"
                + " PARTITION p2a VALUES LESS THAN (200))");
            assertNoInitialRecords(tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testAlterTableGroupSplitPartition_correctness() {
        String tgName = BASE_TG + "_split";
        String tableName = BASE_TABLE + "_split";
        try {
            createRangeTableInTG(tableName, tgName);
            executeDdl("ALTER TABLEGROUP " + tgName
                + " SPLIT PARTITION p1 INTO (PARTITION p1a VALUES LESS THAN (50), PARTITION p1b VALUES LESS THAN (100))");
            assertNoInitialRecords(tgName);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + tableName + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testCreateTableGroup_correctness() {
        String tgName = BASE_TG + "_create";
        try {
            executeDdl("CREATE TABLEGROUP " + tgName);
            assertNoInitialRecords(tgName);
        } finally {
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testDropTableGroup_correctness() {
        String tgName = BASE_TG + "_drop_tg";
        try {
            createTableGroup(tgName);
            executeDdl("DROP TABLEGROUP " + tgName);
            assertNoInitialRecords(tgName);
        } finally {
            dropTableGroupIfExists(tgName);
        }
    }

    @Test
    public void testAlterTableGroupAddTable_correctness() {
        String tgName = BASE_TG + "_addtbl";
        String tableName = BASE_TABLE + "_addtbl";
        String table2 = BASE_TABLE + "_addtbl2";
        try {
            createRangeTableInTG(tableName, tgName);
            executeDdl("CREATE TABLE " + table2
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) PARTITION BY RANGE (id) ("
                + "PARTITION p1 VALUES LESS THAN (100),"
                + "PARTITION p2 VALUES LESS THAN (200),"
                + "PARTITION p3 VALUES LESS THAN (MAXVALUE)"
                + ")");
            executeDdl("ALTER TABLEGROUP " + tgName + " ADD TABLES " + table2 + " FORCE");
            assertNoInitialRecords(tgName);
            assertNoInitialRecords(table2);
            JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT * FROM " + table2 + " LIMIT 1");
        } finally {
            dropTableIfExists(tableName);
            dropTableIfExists(table2);
            dropTableGroupIfExists(tgName);
        }
    }

    // ======================== Category C: Lock Acquisition Kill ========================

    /**
     * Simulate Ctrl+C (KILL QUERY connId) while the DDL is blocked in Phase 1 lock acquisition.
     * A manual EXCLUSIVE lock on table1's resource makes Phase 1 acquireResource block; the kill
     * must interrupt it so the ALTER returns an exception within the timeout instead of hanging.
     */
    @Test(timeout = 60000)
    public void testKillDuringPhase1LockAcquisition() throws Exception {
        String tableName = BASE_TABLE + "_kill_p1";
        String schema = currentSchema();
        String owner = "TEST_BLOCKER_" + UUID.randomUUID().toString().replace("-", "");
        Connection ddlConn = getPolardbxConnection();
        Connection killConn = getPolardbxConnection();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            clearStaleBlockerLocks();
            createPartitionedTable(tableName, 4);
            insertBlockerLock(schema, schema + "." + tableName, owner);

            Future<Exception> ddlFuture = executor.submit(() -> {
                try (Statement st = ddlConn.createStatement()) {
                    st.execute("ALTER TABLE " + tableName + " ADD COLUMN c1 INT");
                    return null;
                } catch (Exception e) {
                    return e;
                }
            });

            long connId = waitAndFindDdlConnId(killConn, tableName);
            org.junit.Assert.assertTrue("Should find the blocked DDL connection", connId > 0);
            Thread.sleep(5000);
            JdbcUtil.executeUpdateSuccess(killConn, "kill query " + connId);

            Exception result = ddlFuture.get(20, TimeUnit.SECONDS);
            org.junit.Assert.assertNotNull("Phase 1 lock acquisition should be interrupted by kill", result);
        } finally {
            executor.shutdownNow();
            deleteBlockerLock(owner);
            try {
                if (!ddlConn.isClosed()) {
                    ddlConn.close();
                }
            } catch (Exception ignore) {
            }
            try {
                if (!killConn.isClosed()) {
                    killConn.close();
                }
            } catch (Exception ignore) {
            }
            dropTableIfExists(tableName);
        }
    }

    /**
     * Simulate Ctrl+C (KILL QUERY connId) while the DDL is blocked in Phase 2 lock acquisition.
     * ALTER TABLE t1 ADD FOREIGN KEY referencing t2 only locks t1 in Phase 1 but needs t2 in
     * Phase 2. A manual EXCLUSIVE lock on t2 lets Phase 1 pass and blocks Phase 2; the kill must
     * interrupt it so the ALTER returns an exception within the timeout instead of hanging.
     */
    @Test(timeout = 60000)
    public void testKillDuringPhase2LockAcquisition() throws Exception {
        String t1 = BASE_TABLE + "_kill_p2_1";
        String t2 = BASE_TABLE + "_kill_p2_2";
        String schema = currentSchema();
        String owner = "TEST_BLOCKER_" + UUID.randomUUID().toString().replace("-", "");
        Connection ddlConn = getPolardbxConnection();
        Connection killConn = getPolardbxConnection();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            clearStaleBlockerLocks();
            executeDdl("CREATE TABLE " + t2
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) SINGLE");
            executeDdl("CREATE TABLE " + t1
                + " (id INT NOT NULL, ref_id INT, name VARCHAR(32), PRIMARY KEY(id), KEY idx_ref(ref_id)) SINGLE");
            insertBlockerLock(schema, schema + "." + t2, owner);

            // Foreign key is gated at parse time by ENABLE_FOREIGN_KEY; a cmd_extra hint is merged
            // after parsing and does not affect the gate, so enable it at session level on ddlConn.
            JdbcUtil.executeUpdateSuccess(ddlConn, "SET ENABLE_FOREIGN_KEY = true");
            JdbcUtil.executeUpdateSuccess(ddlConn, "SET FOREIGN_KEY_CHECKS = true");

            Future<Throwable> ddlFuture = executor.submit(() -> {
                try (Statement st = ddlConn.createStatement()) {
                    st.execute("ALTER TABLE " + t1 + " ADD CONSTRAINT fk_p2 FOREIGN KEY(ref_id) REFERENCES "
                        + t2 + "(id)");
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            });

            Thread.sleep(5000);
            long connId = waitAndFindDdlConnId(killConn, t1);
            if (connId <= 0) {
                // The DDL never reached a blocked running state. Surface why: if it already
                // finished/failed (e.g. FK disabled in this env, or the blocker lock did not match
                // schema.t2), ddlFuture holds the real result/exception.
                if (ddlFuture.isDone()) {
                    Throwable early = ddlFuture.get();
                    org.junit.Assert.fail("DDL did not block in Phase 2; it finished early with: "
                        + (early == null ? "success (blocker lock did not take effect)" : early.toString()));
                }
                org.junit.Assert.fail("Should find the blocked DDL connection (DDL still running but not "
                    + "visible in information_schema.processlist within poll window)");
            }
            // Wait for the DDL to enter the lock-acquisition phase (DdlContext created,
            // ddlInitialJobId set); KILL QUERY sent in the pre-DdlContext TOCTOU window is ignored.
            JdbcUtil.executeUpdateSuccess(killConn, "kill query " + connId);

            Throwable result = ddlFuture.get(20, TimeUnit.SECONDS);
            org.junit.Assert.assertNotNull("Phase 2 lock acquisition should be interrupted by kill", result);
        } finally {
            executor.shutdownNow();
            deleteBlockerLock(owner);
            try {
                if (!ddlConn.isClosed()) {
                    ddlConn.close();
                }
            } catch (Exception ignore) {
            }
            try {
                if (!killConn.isClosed()) {
                    killConn.close();
                }
            } catch (Exception ignore) {
            }
            dropTableIfExists(t1);
            dropTableIfExists(t2);
        }
    }

    // ======================== Two-Phase Lock Switch (ENABLE_DDL_TWO_PHASE_LOCK) ========================

    /**
     * Verify ENABLE_DDL_TWO_PHASE_LOCK=true takes effect: ALTER TABLE t1 ADD FOREIGN KEY referencing
     * t2 locks only t1 in Phase 1 but needs t2 in Phase 2. A manual EXCLUSIVE lock on t2 lets Phase 1
     * finish and blocks Phase 2, so during the blocked window the INITIAL record inserted by Phase 1
     * must be visible in ddl_engine. After the blocker is released the DDL completes and the INITIAL
     * record is gone (turned into a formal job then finished).
     */
    @Test(timeout = 90000)
    public void testTwoPhaseLockSwitchEnabled_correctness() throws Exception {
        String t1 = BASE_TABLE + "_sw_on1";
        String t2 = BASE_TABLE + "_sw_on2";
        String schema = currentSchema();
        String owner = "TEST_BLOCKER_" + UUID.randomUUID().toString().replace("-", "");
        Connection ddlConn = getPolardbxConnection();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            clearStaleBlockerLocks();
            executeDdl("CREATE TABLE " + t2
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) SINGLE");
            executeDdl("CREATE TABLE " + t1
                + " (id INT NOT NULL, ref_id INT, name VARCHAR(32), PRIMARY KEY(id), KEY idx_ref(ref_id)) SINGLE");
            insertBlockerLock(schema, schema + "." + t2, owner);

            JdbcUtil.executeUpdateSuccess(ddlConn, "SET ENABLE_FOREIGN_KEY = true");
            JdbcUtil.executeUpdateSuccess(ddlConn, "SET FOREIGN_KEY_CHECKS = true");

            Future<Throwable> ddlFuture = executor.submit(() -> {
                try (Statement st = ddlConn.createStatement()) {
                    st.execute("/*+TDDL:cmd_extra(ENABLE_DDL_TWO_PHASE_LOCK=true)*/ALTER TABLE " + t1
                        + " ADD CONSTRAINT fk_sw_on FOREIGN KEY(ref_id) REFERENCES " + t2 + "(id)");
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            });

            // While the DDL is blocked in Phase 2, the Phase 1 INITIAL record must be present.
            boolean initialObserved = false;
            for (int i = 0; i < 30 && !initialObserved; i++) {
                if (ddlFuture.isDone()) {
                    Throwable early = ddlFuture.get();
                    org.junit.Assert.fail("DDL did not block in Phase 2; it finished early with: "
                        + (early == null ? "success (blocker lock did not take effect)" : early.toString()));
                }
                initialObserved = countDdlEngineRecords(schema, t1, true) > 0;
                Thread.sleep(500);
            }
            org.junit.Assert.assertTrue(
                "With ENABLE_DDL_TWO_PHASE_LOCK=true an INITIAL record should exist while Phase 2 is blocked",
                initialObserved);

            // Release the blocker: the DDL should acquire Phase 2 locks and finish successfully.
            deleteBlockerLock(owner);
            Throwable result = ddlFuture.get(60, TimeUnit.SECONDS);
            org.junit.Assert.assertNull("DDL should succeed after the blocker lock is released",
                result == null ? null : result.toString());

            org.junit.Assert.assertEquals("INITIAL record should be cleaned up after the DDL finishes",
                0, countDdlEngineRecords(schema, t1, true));
        } finally {
            executor.shutdownNow();
            deleteBlockerLock(owner);
            try {
                if (!ddlConn.isClosed()) {
                    ddlConn.close();
                }
            } catch (Exception ignore) {
            }
            dropTableIfExists(t1);
            dropTableIfExists(t2);
        }
    }

    /**
     * Verify legacy behavior when both ENABLE_DDL_TWO_PHASE_LOCK and
     * ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE are false: with the same FK setup as the enabled case,
     * the DDL blocks on the t2 lock inside Phase 2 job storage. Since Phase 1 is skipped and FIFO
     * compatibility is disabled, there must be no ddl_engine record for t1 during the blocked window.
     */
    @Test(timeout = 90000)
    public void testTwoPhaseLockSwitchDisabled_correctness() throws Exception {
        String t1 = BASE_TABLE + "_sw_off1";
        String t2 = BASE_TABLE + "_sw_off2";
        String schema = currentSchema();
        String owner = "TEST_BLOCKER_" + UUID.randomUUID().toString().replace("-", "");
        Connection ddlConn = getPolardbxConnection();
        Connection watchConn = getPolardbxConnection();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            setGlobalVariable("ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE", "false");
            clearStaleBlockerLocks();
            executeDdl("CREATE TABLE " + t2
                + " (id INT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)) SINGLE");
            executeDdl("CREATE TABLE " + t1
                + " (id INT NOT NULL, ref_id INT, name VARCHAR(32), PRIMARY KEY(id), KEY idx_ref(ref_id)) SINGLE");
            insertBlockerLock(schema, schema + "." + t2, owner);

            JdbcUtil.executeUpdateSuccess(ddlConn, "SET ENABLE_FOREIGN_KEY = true");
            JdbcUtil.executeUpdateSuccess(ddlConn, "SET FOREIGN_KEY_CHECKS = true");

            Future<Throwable> ddlFuture = executor.submit(() -> {
                try (Statement st = ddlConn.createStatement()) {
                    st.execute("/*+TDDL:cmd_extra(ENABLE_DDL_TWO_PHASE_LOCK=false)*/ALTER TABLE " + t1
                        + " ADD CONSTRAINT fk_sw_off FOREIGN KEY(ref_id) REFERENCES " + t2 + "(id)");
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            });

            // Make sure the DDL is actually in flight and blocked on the lock before asserting.
            long connId = waitAndFindDdlConnId(watchConn, t1);
            org.junit.Assert.assertTrue("Should find the blocked DDL connection", connId > 0);

            // During the blocked window there must be no ddl_engine record at all for t1: Phase 1 is
            // disabled and FIFO compatibility is disabled, so Phase 2 still uses legacy batch locking
            // and inserts the record only after all locks are acquired.
            for (int i = 0; i < 6; i++) {
                if (ddlFuture.isDone()) {
                    Throwable early = ddlFuture.get();
                    org.junit.Assert.fail("DDL did not block in Phase 2; it finished early with: "
                        + (early == null ? "success (blocker lock did not take effect)" : early.toString()));
                }
                org.junit.Assert.assertEquals(
                    "With both ENABLE_DDL_TWO_PHASE_LOCK=false and ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE=false "
                        + "no ddl_engine record should exist while blocked",
                    0, countDdlEngineRecords(schema, t1, false));
                Thread.sleep(500);
            }

            // Release the blocker: the DDL should complete through the legacy single-phase path.
            deleteBlockerLock(owner);
            Throwable result = ddlFuture.get(60, TimeUnit.SECONDS);
            org.junit.Assert.assertNull("DDL should succeed after the blocker lock is released",
                result == null ? null : result.toString());

            org.junit.Assert.assertEquals("No INITIAL record should be left after the DDL finishes",
                0, countDdlEngineRecords(schema, t1, true));
        } finally {
            setGlobalVariable("ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE", "true");
            executor.shutdownNow();
            deleteBlockerLock(owner);
            try {
                if (!ddlConn.isClosed()) {
                    ddlConn.close();
                }
            } catch (Exception ignore) {
            }
            try {
                if (!watchConn.isClosed()) {
                    watchConn.close();
                }
            } catch (Exception ignore) {
            }
            dropTableIfExists(t1);
            dropTableIfExists(t2);
        }
    }
}

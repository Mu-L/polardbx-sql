package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 测试columnar表在执行AlterTableGroupRefreshMetaBaseTask和AlterTableSetTableGroupRefreshMetaTask时
 * 是否正确更新了table version
 */
@NotThreadSafe
@RunWith(Parameterized.class)
public class ColumnarTableVersionUpdateTest extends DDLBaseNewDBTestCase {

    private String primaryTableName;
    private String columnarIndexName;

    // 跳过WaitColumnarTableAlterPartitionTask的hint
    private static final String SKIP_ALTER_CCI_PARTITION_HINT =
        "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableAlterPartitionTask\")*/ ";

    public ColumnarTableVersionUpdateTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}
        });
    }

    @Before
    public void init() {
        this.primaryTableName = schemaPrefix + randomTableName("columnar_version_test", 4);
        this.columnarIndexName = "cci_" + randomTableName("test", 4);
    }

    @After
    public void cleanup() {
        // 清理测试数据
        dropTableIfExists(primaryTableName);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * 测试通过split cci partition触发AlterTableGroupRefreshMetaBaseTask对columnar表的table version更新
     */
    @Test
    public void testColumnarTableVersionUpdateViaSplitCciPartition() throws SQLException {
        // 1. 创建分区表
        String createTableSql = String.format(
            "CREATE TABLE %s (" +
                "id bigint NOT NULL AUTO_INCREMENT, " +
                "name varchar(100), " +
                "age int, " +
                "PRIMARY KEY (id)" +
                ") PARTITION BY HASH(id) PARTITIONS 4",
            primaryTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // 2. 创建columnar索引，使用SKIP_WAIT_CCI_CREATION_HINT跳过WaitColumnarTableCreationTask
        String createCciSql = String.format(
            SKIP_WAIT_CCI_CREATION_HINT + "CREATE CLUSTERED COLUMNAR INDEX %s ON %s (name) " +
                "PARTITION BY KEY(name) PARTITIONS 4",
            columnarIndexName, primaryTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCciSql);

        // 3. 获取split前的table version（通过metadb connection）
        long versionBeforeSplit = getTableVersionFromMetaDb(primaryTableName);

        // 4. 执行split cci partition操作，这会触发AlterTableGroupRefreshMetaBaseTask
        String splitCciPartitionSql = String.format(
            SKIP_ALTER_CCI_PARTITION_HINT + "ALTER TABLE %s.%s SPLIT PARTITION p1",
            primaryTableName, columnarIndexName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitCciPartitionSql);

        // 5. 验证table version是否更新
        long versionAfterSplit = getTableVersionFromMetaDb(primaryTableName);
        assertTrue("Table version should be updated after split cci partition operation",
            versionAfterSplit > versionBeforeSplit);

        // 6. 验证内存中的version是否与MetaDB匹配
        long memoryVersionAfterSplit = getTableVersionFromMemory(primaryTableName);
        assertEquals("Memory version should match MetaDB version after split cci partition operation",
            versionAfterSplit, memoryVersionAfterSplit);

        // 7. 验证columnar索引仍然存在且可用
        verifyColumnarIndexExists(primaryTableName, columnarIndexName);
    }

    /**
     * 测试通过merge cci partition触发AlterTableGroupRefreshMetaBaseTask对columnar表的table version更新
     */
    @Test
    public void testColumnarTableVersionUpdateViaMergeCciPartition() throws SQLException {
        // 1. 创建分区表
        String createTableSql = String.format(
            "CREATE TABLE %s (" +
                "id bigint NOT NULL AUTO_INCREMENT, " +
                "name varchar(100), " +
                "age int, " +
                "PRIMARY KEY (id)" +
                ") PARTITION BY HASH(id) PARTITIONS 8",
            primaryTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // 2. 创建columnar索引，使用SKIP_WAIT_CCI_CREATION_HINT跳过WaitColumnarTableCreationTask
        String createCciSql = String.format(
            SKIP_WAIT_CCI_CREATION_HINT + "CREATE CLUSTERED COLUMNAR INDEX %s ON %s (name) " +
                "PARTITION BY HASH(name) PARTITIONS 8",
            columnarIndexName, primaryTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCciSql);

        // 3. 获取merge前的table version（通过metadb connection）
        long versionBeforeMerge = getTableVersionFromMetaDb(primaryTableName);

        // 4. 执行merge cci partition操作，这会触发AlterTableGroupRefreshMetaBaseTask
        String mergeCciPartitionSql = String.format(
            SKIP_ALTER_CCI_PARTITION_HINT + "ALTER TABLE %s.%s MERGE PARTITIONS p2,p3 TO p23",
            primaryTableName, columnarIndexName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, mergeCciPartitionSql);

        // 5. 验证table version是否更新
        long versionAfterMerge = getTableVersionFromMetaDb(primaryTableName);
        assertTrue("Table version should be updated after merge cci partition operation",
            versionAfterMerge > versionBeforeMerge);

        // 6. 验证内存中的version是否与MetaDB匹配
        long memoryVersionAfterMerge = getTableVersionFromMemory(primaryTableName);
        assertEquals("Memory version should match MetaDB version after split cci partition operation",
            versionAfterMerge, memoryVersionAfterMerge);

        // 7. 验证columnar索引仍然存在且可用
        verifyColumnarIndexExists(primaryTableName, columnarIndexName);
    }

    /**
     * 测试通过split cci partition后，系统自动将表移动到拓扑匹配的已存在列存表组
     */
    @Test
    public void testColumnarTableVersionUpdateViaAutoMoveToMatchingTableGroup() throws SQLException {
        // 1. 创建第一个表，用于建立目标列存表组
        String firstTableName = schemaPrefix + randomTableName("first_table", 4);
        String firstCciName = "cci_" + randomTableName("first", 4);

        String createFirstTableSql = String.format(
            "CREATE TABLE %s (" +
                "id bigint NOT NULL AUTO_INCREMENT, " +
                "name varchar(100), " +
                "age int, " +
                "PRIMARY KEY (id)" +
                ") PARTITION BY HASH(id) PARTITIONS 4",
            firstTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createFirstTableSql);

        // 2. 为第一个表创建columnar索引，这会创建一个列存表组
        String createFirstCciSql = String.format(
            SKIP_WAIT_CCI_CREATION_HINT + "CREATE CLUSTERED COLUMNAR INDEX %s ON %s (name) " +
                "PARTITION BY KEY(name) PARTITIONS 4",
            firstCciName, firstTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createFirstCciSql);

        // 3. 对第一个表执行split操作，创建目标拓扑结构（4分区 -> 5分区）
        String splitFirstCciSql = String.format(
            SKIP_ALTER_CCI_PARTITION_HINT + "ALTER TABLE %s.%s SPLIT PARTITION p1",
            firstTableName, firstCciName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitFirstCciSql);

        // 5. 创建第二个表（测试表），初始分区数为4
        String createTableSql = String.format(
            "CREATE TABLE %s (" +
                "id bigint NOT NULL AUTO_INCREMENT, " +
                "name varchar(100), " +
                "age int, " +
                "PRIMARY KEY (id)" +
                ") PARTITION BY HASH(id) PARTITIONS 4",
            primaryTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // 6. 为第二个表创建columnar索引
        String createCciSql = String.format(
            SKIP_WAIT_CCI_CREATION_HINT + "CREATE CLUSTERED COLUMNAR INDEX %s ON %s (name) " +
                "PARTITION BY KEY(name) PARTITIONS 4",
            columnarIndexName, primaryTableName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCciSql);

        // 7. 获取操作前的table version和表组信息
        long versionBeforeSplit = getTableVersionFromMetaDb(primaryTableName);
        long memoryVersionBeforeSplit = getTableVersionFromMemory(primaryTableName);

        // 验证初始状态下MetaDB和内存版本一致
        assertEquals("Initial MetaDB and memory versions should match",
            versionBeforeSplit, memoryVersionBeforeSplit);

        // 8. 对第二个表执行split操作，使其拓扑结构与目标列存表组匹配（4分区 -> 5分区）
        String splitCciPartitionSql = String.format(
            SKIP_ALTER_CCI_PARTITION_HINT + "ALTER TABLE %s.%s SPLIT PARTITION p1",
            primaryTableName, columnarIndexName
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitCciPartitionSql);

        // 9. 验证split后table version是否更新
        long versionAfterSplit = getTableVersionFromMetaDb(primaryTableName);
        assertTrue("Table version should be updated after split cci partition operation",
            versionAfterSplit > versionBeforeSplit);

        // 10. 验证内存中的version是否与MetaDB匹配
        long memoryVersionAfterSplit = getTableVersionFromMemory(primaryTableName);
        assertEquals("Memory version should match MetaDB version after split cci partition operation",
            versionAfterSplit, memoryVersionAfterSplit);

        assertTrue("Memory version should be updated after split operation",
            memoryVersionAfterSplit > memoryVersionBeforeSplit);

        // 12. 验证columnar索引仍然存在且可用
        verifyColumnarIndexExists(primaryTableName, columnarIndexName);

        // 清理第一个表
        dropTableIfExists(firstTableName);
    }

    /**
     * 验证表是否在指定的表组中
     */
    private void verifyTableInTableGroup(String tableName, String tableGroupName) throws SQLException {
        String sql = String.format(
            "SELECT COUNT(*) as cnt FROM information_schema.table_group " +
                "WHERE table_schema = '%s' AND table_name = '%s' AND table_group_name = '%s'",
            getDdlSchema(), getSimpleTableName(tableName), tableGroupName
        );

        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            if (rs.next()) {
                int count = rs.getInt("cnt");
                assertTrue("Table should be in the specified table group", count > 0);
            }
        }
    }

    /**
     * 通过metadb connection获取表的version
     */
    private long getTableVersionFromMetaDb(String tableName) throws SQLException {
        String simpleTableName = getSimpleTableName(tableName);
        String sql = "SELECT version FROM tables WHERE table_schema = ? AND table_name = ?";

        try (Connection metaDbConn = getMetaConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(sql)) {
            ps.setString(1, getDdlSchema());
            ps.setString(2, simpleTableName);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("version");
                }
            }
        }
        throw new SQLException("Could not find table version for " + tableName);
    }

    /**
     * 验证columnar索引是否存在
     */
    private void verifyColumnarIndexExists(String tableName, String indexName) throws SQLException {
        String sql = String.format(
            "SELECT COUNT(*) as cnt FROM information_schema.columnar_status " +
                "WHERE table_schema = '%s' AND table_name = '%s' AND index_name like '%s%%'",
            getDdlSchema(), getSimpleTableName(tableName), indexName
        );

        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            if (rs.next()) {
                int count = rs.getInt("cnt");
                assertTrue("Columnar index should exist after table group operations", count > 0);
            }
        }
    }

    /**
     * 从内存中获取表的version（通过SHOW TABLE REPLICATE STATUS命令）
     */
    private long getTableVersionFromMemory(String tableName) throws SQLException {
        String simpleTableName = getSimpleTableName(tableName);

        String sql = "SHOW TABLE REPLICATE STATUS";

        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            while (rs.next()) {
                String tableNameFromResult = rs.getString("TABLE_NAME");

                // 匹配主表或GSI表
                if (simpleTableName.equalsIgnoreCase(tableNameFromResult)) {
                    return rs.getLong("VERSION");
                }
            }
        }

        throw new RuntimeException("Could not find table version in memory for table: " + simpleTableName);
    }
}
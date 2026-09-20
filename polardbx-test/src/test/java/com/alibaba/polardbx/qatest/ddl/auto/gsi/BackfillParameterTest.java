package com.alibaba.polardbx.qatest.ddl.auto.gsi;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;

/**
 * Test for auto parameter adjustor feature
 * Tests BackfillParameterManager with different PERF_DDL_MODE settings
 */
@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BackfillParameterTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public String schemaName = "backfill_param_test_db";
    public String originalTableName = "backfill_param_test_boost";

    public void prepareTableIfNotExists(Connection tddlConnection, String schemaName, String tableName, int rows)
        throws Exception {
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        prepareTableIfNotExists(tddlConnection, schemaName, tableName, createTableStmt, rows);
    }

    public void prepareTableIfNotExists(Connection tddlConnection, String schemaName, String tableName,
                                        String createTableStmt, int rows)
        throws Exception {
        JdbcUtil.executeUpdate(tddlConnection, "create database if not exists " + schemaName + " mode = auto");

        int partNum = 3;
        int eachPartRows = rows / partNum;
        JdbcUtil.executeUpdate(tddlConnection, "use " + schemaName);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, "show tables"));
        Boolean tableExists = results.stream().anyMatch(o -> o.get(0).toString().equalsIgnoreCase(tableName));
        if (!tableExists) {
            prepareData(tddlConnection, schemaName, tableName, eachPartRows, createTableStmt,
                partNum, DataManipulateUtil.TABLE_TYPE.PARTITION_TABLE);
        }

        String analyzeTableSql = String.format("analyze table %s", tableName);
        JdbcUtil.executeUpdate(tddlConnection, analyzeTableSql);
    }

    /**
     * Test GSI creation with boost mode enabled
     * Verifies that backfill parameters are automatically adjusted based on table size
     */
    @Test
    public void test01CreateGsiWithBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        // Enable boost mode for performance DDL
        String setBoostModeSql = "set PERF_DDL_MODE = 'boost'";
        JdbcUtil.executeUpdate(tddlConnection, setBoostModeSql);

        long expectedBatchFileSize = 1024 * 512;
        // Set batch file size to a specific value to test auto adjustment
        String setBatchFileSizeSql = "set BATCH_FILE_SIZE = " + expectedBatchFileSize; // 512KB
        JdbcUtil.executeUpdate(tddlConnection, setBatchFileSizeSql);

        String setMaxSplitPhysicalSize = "set PHYSICAL_TABLE_START_SPLIT_SIZE = " + 1000;
        JdbcUtil.executeUpdate(tddlConnection, setMaxSplitPhysicalSize);

        String setMaxBatchFileSizeSpeedSql = "set MAX_BATCH_FILE_SIZE_SPEED = 1024 * 1024 * 10"; // 10MB
        JdbcUtil.executeUpdate(tddlConnection, setMaxBatchFileSizeSpeedSql);

        String setThreadPoolSize = "set global FASTCHECKER_THREAD_POOL_SIZE = 3";
        JdbcUtil.executeUpdate(tddlConnection, setThreadPoolSize);

        // test add clustered index
        String createGsiSql = String.format(
            "create clustered index gsi_backfill_test on %s(b) partition by hash(b) partitions 16",
            originalTableName);
        List<String> explainResult = getExplainDdlResult(tddlConnection, createGsiSql);
        checkBackfillParameter(explainResult, 3L, 4096L, 10240L);
        JdbcUtil.executeUpdate(tddlConnection, createGsiSql);
        // Cleanup
        JdbcUtil.executeUpdate(tddlConnection, String.format("drop index gsi_backfill_test on %s", originalTableName));
    }

    @Test
    public void test02RepartitionWithBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        long expectedBatchFileSize = 1024 * 256;
        // Set batch file size to a specific value to test auto adjustment
        String setBatchFileSizeSql = "set BATCH_FILE_SIZE = " + expectedBatchFileSize; // 256KB
        JdbcUtil.executeUpdate(tddlConnection, setBatchFileSizeSql);

        String setMaxSplitPhysicalSize = "set PHYSICAL_TABLE_START_SPLIT_SIZE = " + 1000;
        JdbcUtil.executeUpdate(tddlConnection, setMaxSplitPhysicalSize);

        String setMaxBatchFileSizeSpeedSql = "set MAX_BATCH_FILE_SIZE_SPEED = 1024 * 1024 * 20"; // 20MB
        JdbcUtil.executeUpdate(tddlConnection, setMaxBatchFileSizeSpeedSql);

        // test repartition
        String repartitionSql = String.format(
            "alter table %s partition by key(a) partitions 6 perf_mode = 'boost'",
            originalTableName);
        List<String> explainResult2 = getExplainDdlResult(tddlConnection, repartitionSql);
        checkBackfillParameter(explainResult2, 3L, 2048L, 6000L);
        JdbcUtil.executeUpdate(tddlConnection, repartitionSql);
    }

    @Test
    public void test03ModifyPartitionKeyWithBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        long expectedBatchFileSize = 1024 * 1024;
        // Set batch file size to a specific value to test auto adjustment
        String setBatchFileSizeSql = "set BATCH_FILE_SIZE = " + expectedBatchFileSize; // 1MB
        JdbcUtil.executeUpdate(tddlConnection, setBatchFileSizeSql);

        String setMaxSplitPhysicalSize = "set PHYSICAL_TABLE_START_SPLIT_SIZE = " + 1000;
        JdbcUtil.executeUpdate(tddlConnection, setMaxSplitPhysicalSize);

        String setMaxBatchFileSizeSpeedSql = "set MAX_BATCH_FILE_SIZE_SPEED = 1024 * 1024 * 20"; // 20MB
        JdbcUtil.executeUpdate(tddlConnection, setMaxBatchFileSizeSpeedSql);

        // test modify partition key
        String modifySql = String.format(
            "alter table %s modify column a bigint perf_mode = 'boost'",
            originalTableName);
        List<String> explainResult3 = getExplainDdlResult(tddlConnection, modifySql);
        checkBackfillParameter(explainResult3, 6L, 10000L, 20240L);
        JdbcUtil.executeUpdate(tddlConnection, modifySql);
    }

    @Test
    public void test04ModifyPartitionKeyWithOutBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        // test modify partition key
        String modifySql = String.format(
            "alter table %s modify column a int",
            originalTableName);
        List<String> explainResult3 = getExplainDdlResult(tddlConnection, modifySql);
        checkBackfillParameter(explainResult3, 1L, 1024L, 1024L);
        JdbcUtil.executeUpdate(tddlConnection, modifySql);
    }

    @Test
    public void test05RepartitionWithOutBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        // test repartition
        String repartitionSql = String.format(
            "alter table %s partition by key(a) partitions 3",
            originalTableName);
        List<String> explainResult2 = getExplainDdlResult(tddlConnection, repartitionSql);
        checkBackfillParameter(explainResult2, 1L, 1024L, 1024L);
        JdbcUtil.executeUpdate(tddlConnection, repartitionSql);
    }

    @Test
    public void test06CreateGsiWithOutBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        // test add clustered index
        String createGsiSql = String.format(
            "create clustered index gsi_backfill_test on %s(b) partition by hash(b) partitions 16",
            originalTableName);
        List<String> explainResult = getExplainDdlResult(tddlConnection, createGsiSql);
        checkBackfillParameter(explainResult, 1L, 1024L, 1024L);
        JdbcUtil.executeUpdate(tddlConnection, createGsiSql);
        // Cleanup
        JdbcUtil.executeUpdate(tddlConnection, String.format("drop index gsi_backfill_test on %s", originalTableName));
    }

    @Test
    public void test07OmcWithBoostMode() throws Exception {
        // Prepare a large table with 1M rows
        prepareTableIfNotExists(tddlConnection, schemaName, originalTableName, 1000000);

        // Enable boost mode for performance DDL
        String setBoostModeSql = "set PERF_DDL_MODE = 'boost'";
        JdbcUtil.executeUpdate(tddlConnection, setBoostModeSql);

        // test modify partition key
        String modifySql = String.format(
            "alter table %s modify column b bigint, algorithm=omc",
            originalTableName);
        List<String> explainResult3 = getExplainDdlResult(tddlConnection, modifySql);
        if (isMySQL80()) {
            checkBackfillParameter(explainResult3, 0L, 0L, 0L);
        } else {
            checkBackfillParameter(explainResult3, 1L, 0L, 0L);
        }

        JdbcUtil.executeUpdate(tddlConnection, modifySql);
    }

    @Test
    public void test08CleanUp() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + originalTableName);
        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + schemaName);
    }

    List<String> getExplainDdlResult(Connection connection, String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(connection, "explain " + sql);
        List<List<String>> explainResult = JdbcUtil.getStringResult(rs, false);
        return explainResult.stream().map(l -> l.get(0)).collect(Collectors.toList());
    }

    void checkBackfillParameter(List<String> explainResult, Long expectedPartitions,
                                Long minBatchSize, Long maxBatchSize) throws SQLException {
        int logicalBackfillCount = 0;

        for (String line : explainResult) {
            // Count LOGICAL_BACKFILL occurrences
            if (line.contains("LOGICAL_BACKFILL(")) {
                logicalBackfillCount++;
            }

            // Check parameters in PARAMETER lines
            if (line.contains("PARAMETER[")) {
                // Extract BATCH_SIZE
                String batchSizePattern = "BATCH_SIZE=(\\d+)";
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(batchSizePattern);
                java.util.regex.Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    long actualBatchSize = Long.parseLong(matcher.group(1));
                    if (actualBatchSize < minBatchSize) {
                        throw new SQLException(
                            String.format("BATCH_SIZE mismatch: minBatchSize %d, but got %d",
                                minBatchSize, actualBatchSize));
                    }
                    if (actualBatchSize > maxBatchSize) {
                        throw new SQLException(
                            String.format("BATCH_SIZE mismatch: maxBatchSize %d, but got %d",
                                maxBatchSize, actualBatchSize));
                    }
                }
            }
        }

        // Check total LOGICAL_BACKFILL count
        if (logicalBackfillCount != expectedPartitions) {
            throw new SQLException(
                String.format("LOGICAL_BACKFILL count mismatch: expected %d partitions, but got %d",
                    expectedPartitions, logicalBackfillCount));
        }
    }
}

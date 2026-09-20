package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 *
 */

public class AlterTableAddPartitionTest extends PartitionAutoLoadSqlTestBase {

    public AlterTableAddPartitionTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        List<AutoLoadSqlTestCaseParams> paramsList = getParameters(AlterTableAddPartitionTest.class, 3, true);
        for (AutoLoadSqlTestCaseParams params : paramsList) {
            params.dropDbAfterCheck = false;
        }
        return paramsList;
    }

    @Test
    @CdcIgnore(ignoreReason = "ignore duplicate primary key")
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        runOneTestCaseInner(this.params);
        if (!this.params.tcName.startsWith("test_rand_placement")) {
            return;
        }
        List<String> storageIds = getStorageInstIds(this.params.testDbName);
        if (storageIds.size() < 2) {
            return;
        }
        boolean randPlacementOn = this.params.tcName.toLowerCase().contains("test_rand_placement_on");
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, params.testDbName);
            List<String> tables = JdbcUtil.showTables(connection);
            for (String table : tables) {
                if (!table.contains("_tp")) {
                    continue;
                }
                Map<String, Set<String>> storageAndPartitions = new HashMap<>();
                Map<String, Set<String>> PartitionAndSubPartitions = new HashMap<>();
                getTableTopologyInfo(connection, table, storageAndPartitions, PartitionAndSubPartitions);
                for (Map.Entry<String, Set<String>> partEntry : PartitionAndSubPartitions.entrySet()) {
                    for (Map.Entry<String, Set<String>> storageEntry : storageAndPartitions.entrySet()) {
                        if (storageEntry.getValue().containsAll(partEntry.getValue())) {
                            if (randPlacementOn) {
                                assertTrue(String.format("table %s partition %s storage %s", table, partEntry.getKey(),
                                    storageEntry.getKey()), false);
                            }
                        }
                    }
                }
            }
        } finally {
            JdbcUtil.dropDatabase(tddlConnection, params.testDbName);
        }
    }

    public void getTableTopologyInfo(Connection conn, String tbName, Map<String, Set<String>> storageAndPartitions,
                                     Map<String, Set<String>> PartitionAndSubPartitions) {
        String sql = "show topology " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            while (rs.next()) {
                String partitionName = rs.getString("PARTITION_NAME");
                String subpartitionName = rs.getString("SUBPARTITION_NAME");
                String storageId = rs.getString("DN_ID");
                if (StringUtils.isEmpty(subpartitionName)) {
                    storageAndPartitions.computeIfAbsent(storageId, k -> new HashSet<>()).add(partitionName);
                } else {
                    storageAndPartitions.computeIfAbsent(storageId, k -> new HashSet<>()).add(subpartitionName);
                }
                PartitionAndSubPartitions.computeIfAbsent(partitionName, k -> new HashSet<>()).add(subpartitionName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(rs);
        }
    }
}

package com.alibaba.polardbx.qatest.ddl.recyclebin20;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameUselessPhyTableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RenameUselessTmpGsiPhyTableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class RecycleBinBaseTest extends DDLBaseNewDBTestCase {

    private static final String createTableStmt =
        "create table %s (a int, b varchar(20), c char(20), d datetime, e int ,f text %s) %s %s";
    private static final String autoPrimaryPartitionDefinition1 = "partition by key(a) partitions 3";
    private static final String autoPrimaryPartitionRangeDefinition1 =
        "partition by range(a) (partition p1 values less than (10),partition p2 values less than (100),partition p3 values less than (1000))";
    private static final String autoPrimaryPartitionDefinition2 = "partition by key(e) partitions 3";
    private static final String singleTable = "single";
    private static final String broadcastTable = "broadcast";
    private static final String autoPrimaryPartitionRangeDefinition2 =
        "partition by range(e) (partition p1 values less than (10),partition p2 values less than (100),partition p3 values less than (1000))";
    private static final String autoPrimaryPartitionListDefinition1 =
        "partition by list(e) (partition p1 values in (10),partition p2 values in (100),partition p3 values in (1000))";
    private static final String drdsPrimaryPartitionDefinition1 =
        "dbpartition by hash(a) tbpartition by hash(a) tbpartitions 3";
    private static final String drdsPrimaryPartitionDefinition2 =
        "dbpartition by hash(e) tbpartition by hash(e) tbpartitions 3";
    private static final String foreignKeyDefinition = ", foreign key (e) references %s(e)";
    protected static final String HINT_PURE_MODE = "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=TRUE)*/";
    protected static final String HINT_ENABLE_FK = "/*+TDDL:CMD_EXTRA(ENABLE_FOREIGN_KEY=TRUE)*/";
    private static final String checkTableExistence =
        "/*+TDDL:NODE(%s)*/ select 1 from `__pxc_phy_recycle_bin__`.`%s` where 1=0";
    private static long sleepTime = 500;// 500ms
    public static int ddlTimeout = 15 * 60;// 15 minutes
    private static final int DDL_MAX_RETRIES = 3;
    private static final long DDL_RETRY_DELAY_MS = 1000;

    /**
     * Execute DDL with retry on transient errors like ERR_TABLE_GROUP_NOT_EXISTS.
     * Concurrent DDL operations may delete a table group between prepare and execute phases,
     * causing transient failures that can be resolved by retrying.
     */
    protected void executeUpdateWithRetry(Connection conn, String sql) {
        for (int attempt = 0; attempt <= DDL_MAX_RETRIES; attempt++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                return;
            } catch (SQLException e) {
                if (attempt < DDL_MAX_RETRIES && isRetriableDdlError(e)) {
                    try {
                        Thread.sleep(DDL_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        Assert.fail("Interrupted during DDL retry for: " + sql);
                    }
                } else {
                    Assert.fail("DDL execution failed after " + (attempt + 1) + " attempts: " + sql + "\n" +
                        e.getMessage());
                }
            }
        }
    }

    private boolean isRetriableDdlError(SQLException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("ERR_TABLE_GROUP_NOT_EXISTS") || msg.contains("table group") &&
            msg.contains("not exist"));
    }

    public List<String> createTable(Connection conn, String tableName, int numOfGsi, boolean autoMode) {
        if (autoMode) {
            return createAutoTable(conn, tableName, numOfGsi);
        } else {
            return createDrdsTable(conn, tableName, numOfGsi);
        }
    }

    public List<String> createTableWithTg(Connection conn, String tableName, String tableGroup, int numOfGsi,
                                          boolean autoMode) {
        if (autoMode) {
            return createAutoTableWithTg(conn, tableName, tableGroup, numOfGsi, true);
        } else {
            Assert.fail("not support drds mode");
        }
        return null;
    }

    private List<String> createAutoTable(Connection conn, String tableName, int numOfGsi) {
        return createAutoTableWithTg(conn, tableName, null, numOfGsi, true);
    }

    public List<String> createRangeTableWithTg(Connection conn, String tableName, String tableGroup, int numOfGsi) {
        return createAutoTableWithTg(conn, tableName, tableGroup, numOfGsi, false);
    }

    public List<String> createRangeAutoTable(Connection conn, String tableName, int numOfGsi) {
        return createAutoTableWithTg(conn, tableName, null, numOfGsi, false);
    }

    public List<String> createListTableWithTg(Connection conn, String tableName, String tableGroup, int numOfGsi) {
        return createAutoListTableWithTg(conn, tableName, tableGroup, numOfGsi);
    }

    private List<String> createAutoTableWithTg(Connection conn, String tableName, String tableGroup, int numOfGsi,
                                               boolean isKeyPartition) {
        String gsiInfo = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi; i++) {
            gsiNames.add("gid_" + i);
            gsiInfo += String.format(", global index gid_%d(e) %s", i,
                isKeyPartition ? autoPrimaryPartitionDefinition2 : autoPrimaryPartitionRangeDefinition2);
        }
        String tgInfo = "";
        if (StringUtils.isNotEmpty(tableGroup)) {
            executeUpdateWithRetry(conn, String.format("create tablegroup if not exists %s", tableGroup));
            tgInfo = "tablegroup=" + tableGroup;
        }
        String createTableSql = String.format(createTableStmt, tableName, gsiInfo,
            isKeyPartition ? autoPrimaryPartitionDefinition1 : autoPrimaryPartitionRangeDefinition1, tgInfo);
        executeUpdateWithRetry(conn, createTableSql);
        return gsiNames;
    }

    private List<String> createAutoListTableWithTg(Connection conn, String tableName, String tableGroup, int numOfGsi) {
        String gsiInfo = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi; i++) {
            gsiNames.add("gid_" + i);
            gsiInfo += String.format(", global index gid_%d(e) %s", i,
                autoPrimaryPartitionListDefinition1);
        }
        String tgInfo = "";
        if (StringUtils.isNotEmpty(tableGroup)) {
            executeUpdateWithRetry(conn, String.format("create tablegroup if not exists %s", tableGroup));
            tgInfo = "tablegroup=" + tableGroup;
        }
        String createTableSql = String.format(createTableStmt, tableName, gsiInfo,
            autoPrimaryPartitionListDefinition1, tgInfo);
        executeUpdateWithRetry(conn, createTableSql);
        return gsiNames;
    }

    public void createChildAndParentAutoKeyTable(Connection conn, String childTableName, String parentTableName,
                                                 int numOfGsi, boolean isSingle, boolean isBroadcast) {

        String gsiInfo = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi && !isSingle && !isBroadcast; i++) {
            gsiNames.add("gid_" + i);
            gsiInfo += String.format(", global index gid_%d(e) %s", i, autoPrimaryPartitionDefinition2);
        }
        String part = isSingle ? singleTable : (isBroadcast ? broadcastTable : autoPrimaryPartitionDefinition1);
        String createTableSql = String.format(createTableStmt, parentTableName, ", index idx1(e)" + gsiInfo,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
        String fkInfo = String.format(foreignKeyDefinition, parentTableName);
        JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_FOREIGN_KEY = true");
        createTableSql = String.format(createTableStmt, childTableName, fkInfo + gsiInfo,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
    }

    public void createChildAndPartParentAutoKeyTable(Connection conn, String childTableName, String parentTableName,
                                                     int numOfGsi, boolean isSingle, boolean isBroadcast) {

        String gsiInfo = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi && !isSingle && !isBroadcast; i++) {
            gsiNames.add("gid_" + i);
            gsiInfo += String.format(", global index gid_%d(e) %s", i, autoPrimaryPartitionDefinition2);
        }
        String part = autoPrimaryPartitionDefinition1;
        String createTableSql = String.format(createTableStmt, parentTableName, ", index idx1(e)" + gsiInfo,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
        String fkInfo = String.format(foreignKeyDefinition, parentTableName);
        JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_FOREIGN_KEY = true");
        part = isSingle ? singleTable : (isBroadcast ? broadcastTable : autoPrimaryPartitionDefinition1);
        createTableSql = String.format(createTableStmt, childTableName, fkInfo + gsiInfo,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
    }

    public void createChildAndParentAutoListTable(Connection conn, String childTableName, String parentTableName,
                                                  int numOfGsi, boolean isSingle, boolean isBroadcast) {

        String gsiInfo = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi && !isSingle && !isBroadcast; i++) {
            gsiNames.add("gid_" + i);
            gsiInfo += String.format(", global index gid_%d(e) %s", i, autoPrimaryPartitionListDefinition1);
        }
        String part = isSingle ? singleTable : (isBroadcast ? broadcastTable : autoPrimaryPartitionListDefinition1);
        String createTableSql = String.format(createTableStmt, parentTableName, ", index idx1(e)" + gsiInfo,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
        String fkInfo = String.format(foreignKeyDefinition, parentTableName);
        JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_FOREIGN_KEY = true");
        createTableSql = String.format(createTableStmt, childTableName, fkInfo + gsiInfo,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
    }

    public void createChildAndParentDrdsTable(Connection conn, String childTableName, String parentTableName,
                                              int numOfGsi, boolean isSingle, boolean isBroadcast) {

        String gsiInfo1 = "";
        String gsiInfo2 = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi && !isSingle && !isBroadcast; i++) {
            String gsiName1 = buildGsiName(parentTableName, i);
            gsiInfo1 += String.format(", global index %s(e) %s", gsiName1, drdsPrimaryPartitionDefinition2);
            String gsiName2 = buildGsiName(childTableName, i);
            gsiInfo2 += String.format(", global index %s(e) %s", gsiName2, drdsPrimaryPartitionDefinition2);
        }
        String part = isSingle ? singleTable : (isBroadcast ? broadcastTable : drdsPrimaryPartitionDefinition1);
        String createTableSql = String.format(createTableStmt, parentTableName, ", index idx1(e)" + gsiInfo1,
            part, "");
        executeUpdateWithRetry(conn, createTableSql);
        String fkInfo = String.format(foreignKeyDefinition, parentTableName);
        JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_FOREIGN_KEY = true");
        createTableSql = String.format(createTableStmt, childTableName, fkInfo + gsiInfo2,
            isSingle ? singleTable : drdsPrimaryPartitionDefinition1, "");
        executeUpdateWithRetry(conn, createTableSql);
    }

    private List<String> createDrdsTable(Connection conn, String tableName, int numOfGsi) {
        String gsiInfo = "";
        List<String> gsiNames = new ArrayList<>();
        for (int i = 0; i < numOfGsi; i++) {
            String gsiName = buildGsiName(tableName, i);
            gsiNames.add(gsiName);
            gsiInfo += String.format(", global index %s(e) %s", gsiName, drdsPrimaryPartitionDefinition2);
        }
        String tgInfo = "";
        String createTableSql =
            String.format(createTableStmt, tableName, gsiInfo, drdsPrimaryPartitionDefinition1, tgInfo);
        executeUpdateWithRetry(conn, createTableSql);
        return gsiNames;
    }

    private static String buildGsiName(String tableName, int index) {
        // GSI names are schema-wide and case-insensitive. Include the owning table name instead of a short random
        // suffix so GSIs created for different test tables cannot collide.
        return String.format("gid_%d_%s", index, tableName);
    }

    protected Map<String, String> getGroupInstIdMap(String schemaName, Connection conn) {
        Map<String, String> groupInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        String sql = String.format("show ds where db='%s'", schemaName);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            while (rs.next()) {
                String groupName = rs.getString("GROUP");
                String dnId = rs.getString("STORAGE_INST_ID");
                groupInstIdMap.put(groupName, dnId);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        return groupInstIdMap;
    }

    protected Map<String, String> getPartInstIdMap(String logicalTable, Connection conn) {
        Map<String, String> partInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        String sql = String.format("show topology from %s", logicalTable);
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        try {
            while (rs.next()) {
                String partName = rs.getString("PARTITION_NAME");
                String dnId = rs.getString("DN_ID");
                partInstIdMap.put(partName, dnId);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        return partInstIdMap;
    }

    protected JobInfo fetchCurrentJobUntilAppears(String tableName)
        throws SQLException {
        JobInfo job;
        long startTime = System.currentTimeMillis();
        do {
            job = fetchCurrentJob(tableName);
        } while ((job == null ||
            DdlState.QUEUED.name().equalsIgnoreCase(job.parentJob.state))
            && System.currentTimeMillis() - startTime < 5 * 1000);

        return job;
    }

    protected boolean waitDdlDone(JobInfo jobInfo, int waitSeconds)
        throws SQLException {
        long startTime = System.currentTimeMillis();
        do {
            try (Connection conn = getMetaConnection()) {
                DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
                ddlEngineAccessor.setConnection(conn);
                DdlEngineRecord jobRecord = ddlEngineAccessor.query(jobInfo.parentJob.jobId);
                if (jobRecord == null || DdlState.TERMINATED.contains(DdlState.valueOf(jobRecord.state))) {
                    return true;
                }
            } catch (Exception ex) {
                Assert.fail(ex.getMessage());
                throw new SQLException(ex);
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                // pass
            }
        } while (System.currentTimeMillis() - startTime < waitSeconds * 1000);
        Assert.fail("ddl execution timeout:" + jobInfo.parentJob.jobId);
        return false;
    }

    protected List<DdlTask> queryRenameUselessTask(JobInfo jobInfo)
        throws SQLException {
        RenameUselessPhyTableDdlTask.class.getName();
        RenameUselessTmpGsiPhyTableDdlTask.class.getName();
        try (Connection conn = getMetaConnection()) {
            DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
            ddlEngineTaskAccessor.setConnection(conn);

            List<DdlEngineTaskRecord> ddlEngineTaskRecords =
                ddlEngineTaskAccessor.queryArchiveTaskInfoByJobIdName(jobInfo.parentJob.jobId,
                    "RenameUselessPhyTableDdlTask", false);
            if (GeneralUtil.isEmpty(ddlEngineTaskRecords)) {
                ddlEngineTaskRecords = ddlEngineTaskAccessor.queryArchiveTaskInfoByJobIdName(jobInfo.parentJob.jobId,
                    "RenameUselessPhyTableDdlTask", true);
            }
            if (GeneralUtil.isEmpty(ddlEngineTaskRecords)) {
                ddlEngineTaskRecords = ddlEngineTaskAccessor.queryArchiveTaskInfoByJobIdName(jobInfo.parentJob.jobId,
                    "RenameUselessTmpGsiPhyTableDdlTask", false);
            }
            if (GeneralUtil.isEmpty(ddlEngineTaskRecords)) {
                ddlEngineTaskRecords = ddlEngineTaskAccessor.queryArchiveTaskInfoByJobIdName(jobInfo.parentJob.jobId,
                    "RenameUselessTmpGsiPhyTableDdlTask", true);
            }
            if (GeneralUtil.isNotEmpty(ddlEngineTaskRecords)) {
                return TaskHelper.fromDdlEngineTaskRecord(ddlEngineTaskRecords);
            } else {
                return null;
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
            throw new SQLException(ex);
        }
    }

    protected void checkRecyclebinTables(DdlTask ddlTask, Connection conn, Set<String> ignoreErr) {
        if (ddlTask.getState() != DdlTaskState.SUCCESS) {
            return;
        }
        //key:groupKey,value:<oldTableName,newTableName> caseinsensitive map
        TreeMap<String, HashMap<String, String>> srcTableTopology;
        if (ddlTask instanceof RenameUselessPhyTableDdlTask) {
            RenameUselessPhyTableDdlTask renameUselessPhyTableDdlTask = (RenameUselessPhyTableDdlTask) ddlTask;
            srcTableTopology = renameUselessPhyTableDdlTask.getSrcTableTopology();
        } else {
            RenameUselessTmpGsiPhyTableDdlTask renameUselessPhyTableDdlTask =
                (RenameUselessTmpGsiPhyTableDdlTask) ddlTask;
            srcTableTopology = renameUselessPhyTableDdlTask.getSrcTableTopology();
        }
        for (Map.Entry<String, HashMap<String, String>> entry : srcTableTopology.entrySet()) {
            for (Map.Entry<String, String> item : entry.getValue().entrySet()) {
                String sql = String.format(checkTableExistence, entry.getKey(), item.getValue());
                if (GeneralUtil.isEmpty(ignoreErr)) {
                    JdbcUtil.executeUpdateSuccess(conn, sql);
                } else {
                    JdbcUtil.executeUpdateSuccessIgnoreErr(conn, sql, ignoreErr);
                }
            }
        }

    }

    protected void doWaitAndCheck(String tableName, boolean checkRecyclebin) throws SQLException {
        JobInfo jobInfo = fetchCurrentJobUntilAppears(tableName);
        if (jobInfo != null) {
            waitDdlDone(jobInfo, ddlTimeout);
            List<DdlTask> ddlTasks = queryRenameUselessTask(jobInfo);
            if (checkRecyclebin) {
                for (DdlTask ddlTask : GeneralUtil.emptyIfNull(ddlTasks)) {
                    checkRecyclebinTables(ddlTask, tddlConnection, null);
                }
            }
        }
    }

    protected void doWaitAndCheckIgnoreError(String tableName, boolean checkRecyclebin) throws SQLException {
        JobInfo jobInfo = fetchCurrentJobUntilAppears(tableName);
        if (jobInfo != null) {
            Set<String> ignoreErr = new HashSet<>();
            ignoreErr.add("DataSource is null");
            waitDdlDone(jobInfo, ddlTimeout);
            List<DdlTask> ddlTasks = queryRenameUselessTask(jobInfo);
            if (checkRecyclebin) {
                for (DdlTask ddlTask : GeneralUtil.emptyIfNull(ddlTasks)) {
                    checkRecyclebinTables(ddlTask, tddlConnection, ignoreErr);
                }
            }
        }
    }

    protected boolean isShareStorageMode() {
        Set<String> ipPort = new TreeSet<>(String::compareToIgnoreCase);
        try (Connection conn = getMetaConnection()) {
            StorageInfoAccessor accessor = new StorageInfoAccessor();
            accessor.setConnection(conn);
            List<StorageInfoRecord> storageInfos = accessor.getAliveStorageInfos();
            for (int i = 0; i < storageInfos.size(); i++) {
                StorageInfoRecord storageInfo = storageInfos.get(i);
                ipPort.add(storageInfo.getIp() + ":" + storageInfo.getHostPort());
            }
            return ipPort.size() == 1;
        } catch (SQLException ex) {
            Assert.fail(ex.getMessage());
        }
        return false;
    }

    protected String getTableGroupByTableName(String tableName, Connection connection) throws SQLException {
        String sql = String.format("show full tables like '%s'", tableName);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql)) {
            while (rs.next()) {
                String tableGroup = rs.getString("Table_group");
                if (tableGroup != null) {
                    rs.close();
                    return tableGroup;
                }
            }
        }
        return null;
    }
}

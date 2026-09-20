package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowStats;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalShowStatsHandler extends HandlerCommon {

    private static final Class<?> showStatsSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showStatsSyncActionClass = Class.forName("com.alibaba.polardbx.server.response.ShowStatsSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public LogicalShowStatsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowStats showStats = (SqlShowStats) show.getNativeSqlNode();

        ISyncAction showStatsAction;

        if (showStatsSyncActionClass == null) {
            throw new NotSupportException();
        }
        try {
            showStatsAction = (ISyncAction) showStatsSyncActionClass.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        int ddlJobCount = 0;

        ArrayResultCursor cursor = new ArrayResultCursor("STATS");
        buildColumn(cursor, showStats.isFull(), showStats.isList());
        cursor.initMeta();

        if (showStats.isList()) {
            SyncManagerHelper.syncIgnoreExceptions(showStatsAction, executionContext.getSchemaName(), SyncScope.ALL,
                new ISyncResultHandler() {

                    @Override
                    public void handle(List<Pair<GmsNode, List<Map<String, Object>>>> results) {
                        for (Pair<GmsNode, List<Map<String, Object>>> nodeRows : results) {
                            buildDetail(cursor, nodeRows.getKey(), nodeRows.getValue(), 0, showStats.isFull());
                        }
                    }
                });
        } else {
            List<List<Map<String, Object>>> results =
                SyncManagerHelper.syncIgnoreExceptions(showStatsAction, executionContext.getSchemaName(),
                    SyncScope.ALL);

            buildTotal(cursor, results, ddlJobCount, showStats.isFull());
        }
        return cursor;
    }

    private static void buildColumn(ArrayResultCursor cursor, boolean isFull, boolean isList) {
        if (isList) {
            cursor.addColumn("NODE", DataTypes.VarcharType);
        }

        if (isFull) {
            cursor.addColumn("QPS", DataTypes.DoubleType);
            cursor.addColumn("RDS_QPS", DataTypes.DoubleType);
            cursor.addColumn("SLOW_QPS", DataTypes.DoubleType);
            cursor.addColumn("PHYSICAL_SLOW_QPS", DataTypes.DoubleType);
            cursor.addColumn("ERROR_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("VIOLATION_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("MERGE_QUERY_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("ACTIVE_CONNECTIONS", DataTypes.LongType);
            cursor.addColumn("CONNECTION_CREATE_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("RT(ms)", DataTypes.DoubleType);
            cursor.addColumn("RDS_RT(ms)", DataTypes.DoubleType);
            cursor.addColumn("NET_IN(KB/S)", DataTypes.DoubleType);
            cursor.addColumn("NET_OUT(KB/S)", DataTypes.DoubleType);
            cursor.addColumn("THREAD_RUNNING", DataTypes.LongType);
            cursor.addColumn("HINT_USED_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("HINT_USED_COUNT", DataTypes.LongType);
            cursor.addColumn("AGGREGATE_QUERY_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("AGGREGATE_QUERY_COUNT", DataTypes.LongType);
            cursor.addColumn("TEMP_TABLE_CREATE_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("TEMP_TABLE_CREATE_COUNT", DataTypes.LongType);
            cursor.addColumn("MULTI_DB_JOIN_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("MULTI_DB_JOIN_COUNT", DataTypes.LongType);

            cursor.addColumn("CPU", DataTypes.StringType);
            cursor.addColumn("FREEMEM", DataTypes.StringType);
            cursor.addColumn("FULLGCCOUNT", DataTypes.LongType);
            cursor.addColumn("FULLGCTIME", DataTypes.LongType);

            cursor.addColumn("TRANS_COUNT_XA", DataTypes.LongType);
            cursor.addColumn("TRANS_COUNT_2PC", DataTypes.LongType);
            cursor.addColumn("TRANS_COUNT_TSO", DataTypes.LongType);

            cursor.addColumn("DDL_JOB_COUNT", DataTypes.IntegerType);

            cursor.addColumn("BACKFILL_ROWS", DataTypes.IntegerType);
            cursor.addColumn("CHANGE_SET_DELETE_ROWS", DataTypes.IntegerType);
            cursor.addColumn("CHANGE_SET_REPLACE_ROWS", DataTypes.IntegerType);
            cursor.addColumn("CHECKED_ROWS", DataTypes.IntegerType);
        } else {
            cursor.addColumn("QPS", DataTypes.DoubleType);
            cursor.addColumn("RDS_QPS", DataTypes.DoubleType);
            cursor.addColumn("SLOW_QPS", DataTypes.DoubleType);
            cursor.addColumn("PHYSICAL_SLOW_QPS", DataTypes.DoubleType);
            cursor.addColumn("ERROR_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("MERGE_QUERY_PER_SECOND", DataTypes.DoubleType);
            cursor.addColumn("ACTIVE_CONNECTIONS", DataTypes.LongType);
            cursor.addColumn("RT(ms)", DataTypes.DoubleType);
            cursor.addColumn("RDS_RT(ms)", DataTypes.DoubleType);
            cursor.addColumn("NET_IN(KB/S)", DataTypes.DoubleType);
            cursor.addColumn("NET_OUT(KB/S)", DataTypes.DoubleType);
            cursor.addColumn("THREAD_RUNNING", DataTypes.LongType);

            cursor.addColumn("DDL_JOB_COUNT", DataTypes.IntegerType);

            cursor.addColumn("BACKFILL_ROWS", DataTypes.LongType);
            cursor.addColumn("CHANGE_SET_DELETE_ROWS", DataTypes.IntegerType);
            cursor.addColumn("CHANGE_SET_REPLACE_ROWS", DataTypes.IntegerType);
            cursor.addColumn("CHECKED_ROWS", DataTypes.LongType);
        }
    }

    private static void buildDetail(ArrayResultCursor cursor, GmsNode node, List<Map<String, Object>> nodeRows,
                                    int ddlJobCount, boolean isFull) {
        if (nodeRows == null) {
            return;
        }

        String nodeId = (node != null) ? node.getHostPort() : "localhost";

        Map<String, Object> currentRow = nodeRows.get(0);
        Map<String, Object> historyRow = nodeRows.get(1);

        long currentRecordTime = DataTypes.LongType.convertFrom(currentRow.get("RECORDTIME"));
        long historyRecordTime = DataTypes.LongType.convertFrom(historyRow.get("RECORDTIME"));

        double timePeriod = (((currentRecordTime - historyRecordTime) / 1000D) == 0 ? 1 :
            ((currentRecordTime - historyRecordTime) / 1000D));

        double qps = (DataTypes.LongType.convertFrom(currentRow.get("REQUEST")) - DataTypes.LongType
            .convertFrom(historyRow.get("REQUEST")))
            / timePeriod;

        double mergeQPS = (DataTypes.LongType.convertFrom(currentRow.get("MULTIDBCOUNT")) - DataTypes.LongType
            .convertFrom(historyRow.get("MULTIDBCOUNT")))
            / timePeriod;

        double rdsQps = (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALREQUEST")) - DataTypes.LongType
            .convertFrom(historyRow.get("PHYSICALREQUEST")))
            / timePeriod;

        long totalRequest = (DataTypes.LongType.convertFrom(currentRow.get("REQUEST")) - DataTypes.LongType
            .convertFrom(historyRow.get("REQUEST")));
        long totalTimeCost = (DataTypes.LongType.convertFrom(currentRow.get("TIMECOST")) - DataTypes.LongType
            .convertFrom(historyRow.get("TIMECOST")));

        long totalPhysicalRequest =
            (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALREQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("PHYSICALREQUEST")));
        long totalPhysicalTimeCost =
            (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALTIMECOST")) - DataTypes.LongType
                .convertFrom(historyRow.get("PHYSICALTIMECOST")));

        double errorPerSecond = (DataTypes.LongType.convertFrom(currentRow.get("ERRORCOUNT")) - DataTypes.LongType
            .convertFrom(historyRow.get("ERRORCOUNT")))
            / timePeriod;

        double connectionCreatePerSecond =
            (DataTypes.LongType.convertFrom(currentRow.get("CONNECTIONCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("CONNECTIONCOUNT")))
                / timePeriod;
        long activeConnection = DataTypes.LongType.convertFrom(currentRow.get("ACTIVECONNECTION"));

        long totalHintQuery = DataTypes.LongType.convertFrom(currentRow.get("HINTCOUNT"));
        double hintQPS = (DataTypes.LongType.convertFrom(currentRow.get("HINTCOUNT")) - DataTypes.LongType
            .convertFrom(historyRow.get("HINTCOUNT")))
            / timePeriod;

        double tempTableCreatePerSecond =
            (DataTypes.LongType.convertFrom(currentRow.get("TEMPTABLECOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("TEMPTABLECOUNT")))
                / timePeriod;
        long totalTempTableQuery = DataTypes.LongType.convertFrom(currentRow.get("TEMPTABLECOUNT"));

        double multiDbJoinQPS =
            (DataTypes.LongType.convertFrom(currentRow.get("JOINMULTIDBCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("JOINMULTIDBCOUNT")))
                / timePeriod;

        double slowQPS = (DataTypes.LongType.convertFrom(currentRow.get("SLOWREQUEST")) - DataTypes.LongType
            .convertFrom(historyRow.get("SLOWREQUEST")))
            / timePeriod;
        double physicalSlowQPS =
            (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALSLOWREQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("PHYSICALSLOWREQUEST")))
                / timePeriod;
        long totalMultiDbJoinQuery = DataTypes.LongType.convertFrom(currentRow.get("JOINMULTIDBCOUNT"));

        double multiDbAggregateQPS =
            (DataTypes.LongType.convertFrom(currentRow.get("AGGREGATEMULTIDBCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("AGGREGATEMULTIDBCOUNT")))
                / timePeriod;
        long totalMultiDbAggregateQuery = DataTypes.LongType.convertFrom(currentRow.get("AGGREGATEMULTIDBCOUNT"));

        double netIn = (DataTypes.LongType.convertFrom(currentRow.get("NETIN")) - DataTypes.LongType
            .convertFrom(historyRow.get("NETIN")))
            / timePeriod;

        double netOut = (DataTypes.LongType.convertFrom(currentRow.get("NETOUT")) - DataTypes.LongType
            .convertFrom(historyRow.get("NETOUT")))
            / timePeriod;

        double violationPerSecond =
            (DataTypes.LongType.convertFrom(currentRow.get("integrityConstraintViolationErrorCount".toUpperCase()))
                - DataTypes.LongType
                .convertFrom(historyRow.get("integrityConstraintViolationErrorCount".toUpperCase())))
                / timePeriod;

        long threadRunning = DataTypes.LongType.convertFrom(currentRow.get("THREADRUNNING"));

        double cpu = DataTypes.DoubleType.convertFrom(currentRow.get("CPU"));
        double mem = DataTypes.DoubleType.convertFrom(currentRow.get("FREEMEM"));

        long fullgcCount = DataTypes.LongType.convertFrom(currentRow.get("FULLGCCOUNT"));
        long fullgcTime = DataTypes.LongType.convertFrom(currentRow.get("FULLGCTIME"));

        long transCountXA = DataTypes.LongType.convertFrom(currentRow.get("transCountXA"));
        long transCountBestEffort = DataTypes.LongType.convertFrom(currentRow.get("transCountBestEffort"));
        long transCountTSO = DataTypes.LongType.convertFrom(currentRow.get("transCountTSO"));

        long backfillRows = DataTypes.LongType.convertFrom(currentRow.get("backfillRows"));
        long changeSetDeleteRows = DataTypes.LongType.convertFrom(currentRow.get("changeSetDeleteRows"));
        long changeSetReplaceRows = DataTypes.LongType.convertFrom(currentRow.get("changeSetReplaceRows"));
        long checkedRows = DataTypes.LongType.convertFrom(currentRow.get("checkedRows"));

        cpu = cpu * 100D;
        mem = mem * 100D;

        double rt = (totalRequest == 0 ? 0 : (totalTimeCost / (double) totalRequest)) / 1000;
        double physicalRt =
            (totalPhysicalRequest == 0 ? 0 : (totalPhysicalTimeCost / (double) totalPhysicalRequest)) / 1000;

        if (isFull) {
            cursor.addRow(new Object[] {
                nodeId,
                new DecimalFormat("0.00").format(qps),
                new DecimalFormat("0.00").format(rdsQps), new DecimalFormat("0.00").format(slowQPS),
                new DecimalFormat("0.00").format(physicalSlowQPS),
                new DecimalFormat("0.00").format(errorPerSecond),
                new DecimalFormat("0.00").format(violationPerSecond), new DecimalFormat("0.00").format(mergeQPS),
                activeConnection, new DecimalFormat("0.00").format(connectionCreatePerSecond),
                new DecimalFormat("0.00").format(rt), new DecimalFormat("0.00").format(physicalRt),
                new DecimalFormat("0.00").format(netIn / 1000), new DecimalFormat("0.00").format(netOut / 1000),
                threadRunning, new DecimalFormat("0.00").format(hintQPS), totalHintQuery,
                new DecimalFormat("0.00").format(multiDbAggregateQPS), totalMultiDbAggregateQuery,
                new DecimalFormat("0.00").format(tempTableCreatePerSecond), totalTempTableQuery,
                new DecimalFormat("0.00").format(multiDbJoinQPS), totalMultiDbJoinQuery,
                new DecimalFormat("0.00").format(cpu) + "%", new DecimalFormat("0.00").format(mem) + "%",
                fullgcCount, fullgcTime, transCountXA, transCountBestEffort, transCountTSO, ddlJobCount,
                backfillRows, changeSetDeleteRows, changeSetReplaceRows, checkedRows});
        } else {
            cursor.addRow(new Object[] {
                nodeId,
                new DecimalFormat("0.00").format(qps),
                new DecimalFormat("0.00").format(rdsQps), new DecimalFormat("0.00").format(slowQPS),
                new DecimalFormat("0.00").format(physicalSlowQPS),
                new DecimalFormat("0.00").format(errorPerSecond), new DecimalFormat("0.00").format(mergeQPS),
                activeConnection, new DecimalFormat("0.00").format(rt),
                new DecimalFormat("0.00").format(physicalRt), new DecimalFormat("0.00").format(netIn / 1000),
                new DecimalFormat("0.00").format(netOut / 1000), threadRunning, ddlJobCount,
                backfillRows, changeSetDeleteRows, changeSetReplaceRows, checkedRows});
        }
    }

    private static void buildTotal(ArrayResultCursor cursor, List<List<Map<String, Object>>> results, int ddlJobCount,
                                   boolean isFull) {

        double qps = 0;
        double rdsQps = 0;
        double errorPerSecond = 0;

        double connectionCreatePerSecond = 0;

        long activeConnection = 0;
        long totalRequest = 0;
        long totalTimeCost = 0;

        long totalPhysicalRequest = 0;
        long totalPhysicalTimeCost = 0;

        double hintQPS = 0;
        long totalHintQuery = 0;

        double netIn = 0;
        double netOut = 0;

        double tempTableCreatePerSecond = 0;
        long totalTempTableQuery = 0;

        double multiDbJoinQPS = 0;
        long totalMultiDbJoinQuery = 0;

        double multiDbAggregateQPS = 0;
        long totalMultiDbAggregateQuery = 0;

        double violationPerSecond = 0;
        double mergeQPS = 0;

        double slowQPS = 0;
        double physicalSlowQPS = 0;

        long threadRunning = 0;

        double cpu = 0D;
        double mem = 0D;

        long fullgcCount = 0L;
        long fullgcTime = 0L;

        long size = 0L;

        long transCountXA = 0;
        long transCountBestEffort = 0;
        long transCountTSO = 0;

        long backfillRows = 0;
        long changeSetDeleteRows = 0;
        long changeSetReplaceRows = 0;
        long checkedRows = 0;

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            size++;

            Map<String, Object> currentRow = nodeRows.get(0);
            Map<String, Object> historyRow = nodeRows.get(1);
            long currentRecordTime = DataTypes.LongType.convertFrom(currentRow.get("RECORDTIME"));
            long historyRecordTime = DataTypes.LongType.convertFrom(historyRow.get("RECORDTIME"));

            double timePeriod = (((currentRecordTime - historyRecordTime) / 1000D) == 0 ? 1 :
                ((currentRecordTime - historyRecordTime) / 1000D));

            qps += (DataTypes.LongType.convertFrom(currentRow.get("REQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("REQUEST")))
                / timePeriod;

            mergeQPS += (DataTypes.LongType.convertFrom(currentRow.get("MULTIDBCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("MULTIDBCOUNT")))
                / timePeriod;

            rdsQps += (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALREQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("PHYSICALREQUEST")))
                / timePeriod;

            totalRequest += (DataTypes.LongType.convertFrom(currentRow.get("REQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("REQUEST")));
            totalTimeCost += (DataTypes.LongType.convertFrom(currentRow.get("TIMECOST")) - DataTypes.LongType
                .convertFrom(historyRow.get("TIMECOST")));

            totalPhysicalRequest +=
                (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALREQUEST")) - DataTypes.LongType
                    .convertFrom(historyRow.get("PHYSICALREQUEST")));
            totalPhysicalTimeCost +=
                (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALTIMECOST")) - DataTypes.LongType
                    .convertFrom(historyRow.get("PHYSICALTIMECOST")));

            errorPerSecond += (DataTypes.LongType.convertFrom(currentRow.get("ERRORCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("ERRORCOUNT")))
                / timePeriod;

            connectionCreatePerSecond +=
                (DataTypes.LongType.convertFrom(currentRow.get("CONNECTIONCOUNT")) - DataTypes.LongType
                    .convertFrom(historyRow.get("CONNECTIONCOUNT")))
                    / timePeriod;
            activeConnection += DataTypes.LongType.convertFrom(currentRow.get("ACTIVECONNECTION"));

            totalHintQuery += DataTypes.LongType.convertFrom(currentRow.get("HINTCOUNT"));
            hintQPS += (DataTypes.LongType.convertFrom(currentRow.get("HINTCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("HINTCOUNT")))
                / timePeriod;

            tempTableCreatePerSecond +=
                (DataTypes.LongType.convertFrom(currentRow.get("TEMPTABLECOUNT")) - DataTypes.LongType
                    .convertFrom(historyRow.get("TEMPTABLECOUNT")))
                    / timePeriod;
            totalTempTableQuery += DataTypes.LongType.convertFrom(currentRow.get("TEMPTABLECOUNT"));

            multiDbJoinQPS += (DataTypes.LongType.convertFrom(currentRow.get("JOINMULTIDBCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("JOINMULTIDBCOUNT")))
                / timePeriod;

            slowQPS += (DataTypes.LongType.convertFrom(currentRow.get("SLOWREQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("SLOWREQUEST")))
                / timePeriod;
            physicalSlowQPS +=
                (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALSLOWREQUEST")) - DataTypes.LongType
                    .convertFrom(historyRow.get("PHYSICALSLOWREQUEST")))
                    / timePeriod;
            totalMultiDbJoinQuery += DataTypes.LongType.convertFrom(currentRow.get("JOINMULTIDBCOUNT"));

            multiDbAggregateQPS +=
                (DataTypes.LongType.convertFrom(currentRow.get("AGGREGATEMULTIDBCOUNT")) - DataTypes.LongType
                    .convertFrom(historyRow.get("AGGREGATEMULTIDBCOUNT")))
                    / timePeriod;
            totalMultiDbAggregateQuery += DataTypes.LongType.convertFrom(currentRow.get("AGGREGATEMULTIDBCOUNT"));

            netIn += (DataTypes.LongType.convertFrom(currentRow.get("NETIN")) - DataTypes.LongType
                .convertFrom(historyRow.get("NETIN")))
                / timePeriod;

            netOut += (DataTypes.LongType.convertFrom(currentRow.get("NETOUT")) - DataTypes.LongType
                .convertFrom(historyRow.get("NETOUT")))
                / timePeriod;

            violationPerSecond +=
                (DataTypes.LongType.convertFrom(currentRow.get("integrityConstraintViolationErrorCount".toUpperCase()))
                    - DataTypes.LongType
                    .convertFrom(historyRow.get("integrityConstraintViolationErrorCount".toUpperCase())))
                    / timePeriod;

            threadRunning += DataTypes.LongType.convertFrom(currentRow.get("THREADRUNNING"));

            cpu += DataTypes.DoubleType.convertFrom(currentRow.get("CPU"));

            mem += DataTypes.DoubleType.convertFrom(currentRow.get("FREEMEM"));

            fullgcCount += DataTypes.LongType.convertFrom(currentRow.get("FULLGCCOUNT"));

            fullgcTime += DataTypes.LongType.convertFrom(currentRow.get("FULLGCTIME"));

            transCountXA += DataTypes.LongType.convertFrom(currentRow.get("transCountXA"));
            transCountBestEffort += DataTypes.LongType.convertFrom(currentRow.get("transCountBestEffort"));
            transCountTSO += DataTypes.LongType.convertFrom(currentRow.get("transCountTSO"));

            backfillRows += DataTypes.LongType.convertFrom(currentRow.get("backfillRows"));
            changeSetDeleteRows += DataTypes.LongType.convertFrom(currentRow.get("changeSetDeleteRows"));
            changeSetReplaceRows += DataTypes.LongType.convertFrom(currentRow.get("changeSetReplaceRows"));
            checkedRows += DataTypes.LongType.convertFrom(currentRow.get("checkedRows"));
        }

        cpu = cpu / size * 100D;
        mem = (mem / size) * 100D;

        double rt = (totalRequest == 0 ? 0 : (totalTimeCost / (double) totalRequest)) / 1000;
        double physicalRt =
            (totalPhysicalRequest == 0 ? 0 : (totalPhysicalTimeCost / (double) totalPhysicalRequest)) / 1000;

        if (isFull) {
            cursor.addRow(new Object[] {
                new DecimalFormat("0.00").format(qps),
                new DecimalFormat("0.00").format(rdsQps), new DecimalFormat("0.00").format(slowQPS),
                new DecimalFormat("0.00").format(physicalSlowQPS),
                new DecimalFormat("0.00").format(errorPerSecond),
                new DecimalFormat("0.00").format(violationPerSecond), new DecimalFormat("0.00").format(mergeQPS),
                activeConnection, new DecimalFormat("0.00").format(connectionCreatePerSecond),
                new DecimalFormat("0.00").format(rt), new DecimalFormat("0.00").format(physicalRt),
                new DecimalFormat("0.00").format(netIn / 1000), new DecimalFormat("0.00").format(netOut / 1000),
                threadRunning, new DecimalFormat("0.00").format(hintQPS), totalHintQuery,
                new DecimalFormat("0.00").format(multiDbAggregateQPS), totalMultiDbAggregateQuery,
                new DecimalFormat("0.00").format(tempTableCreatePerSecond), totalTempTableQuery,
                new DecimalFormat("0.00").format(multiDbJoinQPS), totalMultiDbJoinQuery,
                new DecimalFormat("0.00").format(cpu) + "%", new DecimalFormat("0.00").format(mem) + "%",
                fullgcCount, fullgcTime, transCountXA, transCountBestEffort, transCountTSO, ddlJobCount,
                backfillRows, changeSetDeleteRows, changeSetReplaceRows, checkedRows});
        } else {
            cursor.addRow(new Object[] {
                new DecimalFormat("0.00").format(qps),
                new DecimalFormat("0.00").format(rdsQps), new DecimalFormat("0.00").format(slowQPS),
                new DecimalFormat("0.00").format(physicalSlowQPS),
                new DecimalFormat("0.00").format(errorPerSecond), new DecimalFormat("0.00").format(mergeQPS),
                activeConnection, new DecimalFormat("0.00").format(rt),
                new DecimalFormat("0.00").format(physicalRt), new DecimalFormat("0.00").format(netIn / 1000),
                new DecimalFormat("0.00").format(netOut / 1000), threadRunning, ddlJobCount,
                backfillRows, changeSetDeleteRows, changeSetReplaceRows, checkedRows});
        }
    }
}

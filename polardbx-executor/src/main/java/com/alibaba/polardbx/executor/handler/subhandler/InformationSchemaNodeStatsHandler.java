package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaNodeStats;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * Created by in355hz.
 *
 * @author in355hz
 */
public class InformationSchemaNodeStatsHandler extends BaseVirtualViewSubClassHandler {

    private static final Class<?> showStatsSyncActionClass;

    static {
        try {
            showStatsSyncActionClass = Class.forName("com.alibaba.polardbx.server.response.ShowStatsSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaNodeStatsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaNodeStats;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        ISyncAction showStatsAction;

        try {
            showStatsAction = (ISyncAction) showStatsSyncActionClass.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        SyncManagerHelper.syncIgnoreExceptions(showStatsAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL,
            new ISyncResultHandler() {

                @Override
                public void handle(List<Pair<GmsNode, List<Map<String, Object>>>> results) {
                    for (Pair<GmsNode, List<Map<String, Object>>> nodeRows : results) {
                        buildNodeStats(cursor, nodeRows.getKey(), nodeRows.getValue());
                    }
                }
            });
        return cursor;
    }

    private static void buildNodeStats(ArrayResultCursor cursor, GmsNode node, List<Map<String, Object>> nodeStats) {
        if (nodeStats != null) {
            Map<String, Object> currentRow = nodeStats.get(0);
            Map<String, Object> historyRow = nodeStats.get(1);

            long currentRecordTime = DataTypes.LongType.convertFrom(currentRow.get("RECORDTIME"));
            long historyRecordTime = DataTypes.LongType.convertFrom(historyRow.get("RECORDTIME"));

            double timePeriod = (((currentRecordTime - historyRecordTime) / 1000D) == 0 ? 1 :
                ((currentRecordTime - historyRecordTime) / 1000D));

            double cpu = DataTypes.DoubleType.convertFrom(currentRow.get("CPU"));
            double mem = DataTypes.DoubleType.convertFrom(currentRow.get("FREEMEM"));

            cpu = cpu * 100D;
            mem = mem * 100D;

            long fullGC = DataTypes.LongType.convertFrom(currentRow.get("FULLGCCOUNT"));
            long fullGCTime = DataTypes.LongType.convertFrom(currentRow.get("FULLGCTIME"));

            double netIn = (DataTypes.LongType.convertFrom(currentRow.get("NETIN")) - DataTypes.LongType
                .convertFrom(historyRow.get("NETIN")))
                / timePeriod;

            double netOut = (DataTypes.LongType.convertFrom(currentRow.get("NETOUT")) - DataTypes.LongType
                .convertFrom(historyRow.get("NETOUT")))
                / timePeriod;

            long totalRequest = (DataTypes.LongType.convertFrom(currentRow.get("REQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("REQUEST")));
            long totalTimeCost = (DataTypes.LongType.convertFrom(currentRow.get("TIMECOST")) - DataTypes.LongType
                .convertFrom(historyRow.get("TIMECOST")));

            double qps = totalRequest / timePeriod;
            double rt = (totalRequest == 0 ? 0 : (totalTimeCost / (double) totalRequest)) / 1000;

            long totalPhysicalRequest =
                (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALREQUEST")) - DataTypes.LongType
                    .convertFrom(historyRow.get("PHYSICALREQUEST")));
            long totalPhysicalTimeCost =
                (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALTIMECOST")) - DataTypes.LongType
                    .convertFrom(historyRow.get("PHYSICALTIMECOST")));

            double physicalQPS = totalPhysicalRequest / timePeriod;
            double physicalRt =
                (totalPhysicalRequest == 0 ? 0 : (totalPhysicalTimeCost / (double) totalPhysicalRequest)) / 1000;

            double slowQPS = (DataTypes.LongType.convertFrom(currentRow.get("SLOWREQUEST")) - DataTypes.LongType
                .convertFrom(historyRow.get("SLOWREQUEST")))
                / timePeriod;

            double physicalSlowQPS =
                (DataTypes.LongType.convertFrom(currentRow.get("PHYSICALSLOWREQUEST")) - DataTypes.LongType
                    .convertFrom(historyRow.get("PHYSICALSLOWREQUEST")))
                    / timePeriod;

            long activeConnection = DataTypes.LongType.convertFrom(currentRow.get("ACTIVECONNECTION"));
            long threadRunning = DataTypes.LongType.convertFrom(currentRow.get("THREADRUNNING"));
            long transXA = DataTypes.LongType.convertFrom(currentRow.get("transCountXA"));
            long transTSO = DataTypes.LongType.convertFrom(currentRow.get("transCountTSO"));
            long transBestEffort = DataTypes.LongType.convertFrom(currentRow.get("transCountBestEffort"));
            long transTotal = transXA + transTSO + transBestEffort;

            double errorPerSecond = (DataTypes.LongType.convertFrom(currentRow.get("ERRORCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("ERRORCOUNT")))
                / timePeriod;

            double violationPerSecond =
                (DataTypes.LongType.convertFrom(currentRow.get("integrityConstraintViolationErrorCount".toUpperCase()))
                    - DataTypes.LongType
                    .convertFrom(historyRow.get("integrityConstraintViolationErrorCount".toUpperCase())))
                    / timePeriod;

            double mergeQPS = (DataTypes.LongType.convertFrom(currentRow.get("MULTIDBCOUNT")) - DataTypes.LongType
                .convertFrom(historyRow.get("MULTIDBCOUNT")))
                / timePeriod;

            double connectionCreatePerSecond =
                (DataTypes.LongType.convertFrom(currentRow.get("CONNECTIONCOUNT")) - DataTypes.LongType
                    .convertFrom(historyRow.get("CONNECTIONCOUNT")))
                    / timePeriod;

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

            long totalMultiDbJoinQuery = DataTypes.LongType.convertFrom(currentRow.get("JOINMULTIDBCOUNT"));

            double multiDbAggregateQPS =
                (DataTypes.LongType.convertFrom(currentRow.get("AGGREGATEMULTIDBCOUNT")) - DataTypes.LongType
                    .convertFrom(historyRow.get("AGGREGATEMULTIDBCOUNT")))
                    / timePeriod;

            long totalMultiDbAggregateQuery = DataTypes.LongType.convertFrom(currentRow.get("AGGREGATEMULTIDBCOUNT"));

            if (node != null) {
                cursor.addRow(new Object[] {
                    node.uniqueId,
                    node.host,
                    node.serverPort,
                    convertReadableStatus(node.status),
                    node.instId,
                    convertReadableInstType(node.instType),
                    node.cpuCore,
                    new DecimalFormat("0.00").format(cpu) + "%", new DecimalFormat("0.00").format(mem) + "%",
                    fullGC, fullGCTime,
                    new DecimalFormat("0.00").format(netIn / 1000), new DecimalFormat("0.00").format(netOut / 1000),
                    new DecimalFormat("0.00").format(qps),
                    new DecimalFormat("0.00").format(physicalQPS),
                    new DecimalFormat("0.00").format(slowQPS),
                    new DecimalFormat("0.00").format(physicalSlowQPS),
                    new DecimalFormat("0.00").format(rt), new DecimalFormat("0.00").format(physicalRt),
                    activeConnection, threadRunning,
                    transTotal, transXA, transTSO,
                    new DecimalFormat("0.00").format(errorPerSecond),
                    new DecimalFormat("0.00").format(violationPerSecond),
                    new DecimalFormat("0.00").format(mergeQPS),
                    new DecimalFormat("0.00").format(connectionCreatePerSecond),
                    new DecimalFormat("0.00").format(hintQPS), totalHintQuery,
                    new DecimalFormat("0.00").format(multiDbJoinQPS), totalMultiDbJoinQuery,
                    new DecimalFormat("0.00").format(multiDbAggregateQPS), totalMultiDbAggregateQuery,
                    new DecimalFormat("0.00").format(tempTableCreatePerSecond), totalTempTableQuery});
            } else {
                /**
                 * Workaround for null node.
                 */
                cursor.addRow(new Object[] {
                    0, "localhost", -1,
                    convertReadableStatus(0), "",
                    convertReadableInstType(0), -1,
                    new DecimalFormat("0.00").format(cpu) + "%", new DecimalFormat("0.00").format(mem) + "%",
                    fullGC, fullGCTime,
                    new DecimalFormat("0.00").format(netIn / 1000), new DecimalFormat("0.00").format(netOut / 1000),
                    new DecimalFormat("0.00").format(qps),
                    new DecimalFormat("0.00").format(physicalQPS),
                    new DecimalFormat("0.00").format(slowQPS),
                    new DecimalFormat("0.00").format(physicalSlowQPS),
                    new DecimalFormat("0.00").format(rt), new DecimalFormat("0.00").format(physicalRt),
                    activeConnection, threadRunning,
                    transTotal, transXA, transTSO,
                    new DecimalFormat("0.00").format(errorPerSecond),
                    new DecimalFormat("0.00").format(violationPerSecond),
                    new DecimalFormat("0.00").format(mergeQPS),
                    new DecimalFormat("0.00").format(connectionCreatePerSecond),
                    new DecimalFormat("0.00").format(hintQPS), totalHintQuery,
                    new DecimalFormat("0.00").format(multiDbJoinQPS), totalMultiDbJoinQuery,
                    new DecimalFormat("0.00").format(multiDbAggregateQPS), totalMultiDbAggregateQuery,
                    new DecimalFormat("0.00").format(tempTableCreatePerSecond), totalTempTableQuery});
            }
        }
    }

    private static String convertReadableInstType(int instType) {
        switch (instType) {
        case 0: // ServerInfoRecord.INST_TYPE_MASTER
            return "MASTER";
        case 1: // ServerInfoRecord.INST_TYPE_ROW_SLAVE
            return "ROW";
        case 2: // ServerInfoRecord.INST_TYPE_HTAP_SLAVE
            return "HTAP";
        case 3: // ServerInfoRecord.INST_TYPE_STANDBY
            return "STANDBY";
        case 4: // ServerInfoRecord.INST_TYPE_COLUMNAR_SLAVE
            return "COLUMNAR";
        default:
            return "NA";
        }
    }

    private static String convertReadableStatus(int status) {
        switch (status) {
        case 0: // ServerInfoRecord.SERVER_STATUS_READY
            return "READY";
        case 1: // ServerInfoRecord.SERVER_STATUS_NOT_READY
            return "NOT_READY";
        case 2: // ServerInfoRecord.SERVER_STATUS_REMOVED:
            return "REMOVED";
        default:
            return "NA";
        }
    }
}

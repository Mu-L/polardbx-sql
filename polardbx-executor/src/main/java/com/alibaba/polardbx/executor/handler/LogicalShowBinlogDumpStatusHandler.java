package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinlogDumpStatus;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.entity.ContentType;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class LogicalShowBinlogDumpStatusHandler extends HandlerCommon {
    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    /**
     * CN 本地监控指标列名（追加到 CDC daemon 返回的列之后）
     */
    private static final String[] CN_METRICS_COLUMNS = {
        "Recent_Avg_Fetch_Wait_Ms",
        "Recent_Avg_Process_Ms",
        "Recent_Avg_Write_Ms",
        "Idle_Ratio",
        "Fetch_Wait_Ratio",
        "Process_Ratio",
        "Write_Ratio"
    };

    private static volatile Class<?> showBinlogDumpMetricsSyncActionClass;
    private static volatile boolean syncActionClassResolved = false;

    private static Class<?> getSyncActionClass() {
        if (!syncActionClassResolved) {
            synchronized (LogicalShowBinlogDumpStatusHandler.class) {
                if (!syncActionClassResolved) {
                    try {
                        showBinlogDumpMetricsSyncActionClass =
                            Class.forName("com.alibaba.polardbx.server.response.ShowBinlogDumpMetricsSyncAction");
                    } catch (ClassNotFoundException e) {
                        cdcLogger.warn("ShowBinlogDumpMetricsSyncAction not found, CN metrics will be unavailable");
                        showBinlogDumpMetricsSyncActionClass = null;
                    }
                    syncActionClassResolved = true;
                }
            }
        }
        return showBinlogDumpMetricsSyncActionClass;
    }

    public LogicalShowBinlogDumpStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        SqlShowBinlogDumpStatus sqlShowBinlogDumpStatus =
            (SqlShowBinlogDumpStatus) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlShowBinlogDumpStatus.getWith();
        String groupName = with == null ? "" : RelUtils.lastStringValue(with);
        Map<String, String> params = new HashMap<>(1);
        String daemonEndpoint;
        if (StringUtils.isEmpty(groupName)) {
            daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget();
        } else {
            daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget(groupName);
        }
        params.put("instId", InstIdUtil.getInstId());
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/dumper/showBinlogDumpStatus",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(params), 4000);
        } catch (Throwable e) {
            cdcLogger.error("show dump status error!", e);
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, e, e.getMessage());
        }

        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            cdcLogger.warn("show slave status failed! code:" + httpResult.getCode() + ", msg:" + httpResult.getMsg());
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, httpResult.getMsg());
        }

        String jsonResponse = (String) httpResult.getData();
        List<LinkedHashMap<String, String>> responses = JSON.parseObject(jsonResponse,
            new TypeReference<List<LinkedHashMap<String, String>>>() {
            });

        // 通过 SyncAction 从当前实例的所有 CN 节点获取 dump 连接性能指标
        Map<String, double[]> metricsByTraceId = gatherMetricsFromAllCn(executionContext);

        ArrayResultCursor result = new ArrayResultCursor("SHOW BINLOG DUMP STATUS");
        if (CollectionUtils.isEmpty(responses)) {
            result.addColumn("Process_Id", DataTypes.StringType, false);
            result.addColumn("Trace_Id", DataTypes.StringType, false);
            result.addColumn("Dumper_Address", DataTypes.StringType, false);
            result.addColumn("Client_Ip", DataTypes.StringType, false);
            result.addColumn("Client_Port", DataTypes.StringType, false);
            result.addColumn("Filename", DataTypes.StringType, false);
            result.addColumn("Position", DataTypes.StringType, false);
            result.addColumn("Delay", DataTypes.StringType, false);
            result.addColumn("Bps", DataTypes.StringType, false);
            result.addColumn("Last_Sync_Timestamp", DataTypes.StringType, false);
            result.addColumn("Alive_Second", DataTypes.StringType, false);
            for (String col : CN_METRICS_COLUMNS) {
                result.addColumn(col, DataTypes.StringType, false);
            }

            result.initMeta();
            return result;
        }

        // 添加 CDC daemon 返回的列
        for (Map.Entry<String, String> entry : responses.get(0).entrySet()) {
            result.addColumn(entry.getKey(), DataTypes.StringType, false);
        }
        // 追加 CN 本地监控列
        for (String col : CN_METRICS_COLUMNS) {
            result.addColumn(col, DataTypes.StringType, false);
        }

        result.initMeta();

        int cdcColumnCount = responses.get(0).size();
        for (LinkedHashMap<String, String> response : responses) {
            Object[] values = new Object[result.getReturnColumns().size()];
            result.addRow(values);

            // 填充 CDC daemon 返回的数据
            int i = 0;
            for (Map.Entry<String, String> entry : response.entrySet()) {
                values[i++] = entry.getValue() != null ? entry.getValue() : "";
            }

            // 通过 Trace_Id 匹配 metrics 并填充
            String traceId = response.get("Trace_Id");
            double[] metrics = traceId != null ? metricsByTraceId.get(traceId) : null;
            if (metrics != null) {
                values[cdcColumnCount] = String.format("%.3f", metrics[0]);
                values[cdcColumnCount + 1] = String.format("%.3f", metrics[1]);
                values[cdcColumnCount + 2] = String.format("%.3f", metrics[2]);
                values[cdcColumnCount + 3] = String.format("%.1f%%", metrics[3] * 100);
                values[cdcColumnCount + 4] = String.format("%.1f%%", metrics[4] * 100);
                values[cdcColumnCount + 5] = String.format("%.1f%%", metrics[5] * 100);
                values[cdcColumnCount + 6] = String.format("%.1f%%", metrics[6] * 100);
            } else {
                for (int j = 0; j < CN_METRICS_COLUMNS.length; j++) {
                    values[cdcColumnCount + j] = "";
                }
            }
        }

        return result;
    }

    /**
     * 通过 SyncAction 从当前实例的所有 CN 节点聚合 BinlogDumpMetrics。
     * 返回 Map: traceId -> double[7] {avgFetchWait, avgProcess, avgWrite, idleRatio, fetchWaitRatio, processRatio, writeRatio}
     */
    Map<String, double[]> gatherMetricsFromAllCn(ExecutionContext executionContext) {
        Map<String, double[]> metricsByTraceId = new HashMap<>();
        Class<?> syncActionClass = getSyncActionClass();
        if (syncActionClass == null) {
            return metricsByTraceId;
        }
        try {
            ISyncAction syncAction = (ISyncAction) syncActionClass
                .getConstructor().newInstance();
            List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
                syncAction, executionContext.getSchemaName(), SyncScope.CURRENT_ONLY);
            return processMetricsResults(results);
        } catch (Exception e) {
            cdcLogger.warn("Failed to gather binlog dump metrics from CN nodes", e);
        }
        return metricsByTraceId;
    }

    /**
     * 处理 SyncAction 返回的原始结果，提取 traceId 到 metrics 数组的映射。
     */
    static Map<String, double[]> processMetricsResults(List<List<Map<String, Object>>> results) {
        Map<String, double[]> metricsByTraceId = new HashMap<>();
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                String traceId = DataTypes.StringType.convertFrom(row.get("Trace_Id"));
                if (traceId == null) {
                    continue;
                }
                double[] values = new double[7];
                values[0] = convertToDouble(row.get("Recent_Avg_Fetch_Wait_Ms"));
                values[1] = convertToDouble(row.get("Recent_Avg_Process_Ms"));
                values[2] = convertToDouble(row.get("Recent_Avg_Write_Ms"));
                values[3] = convertToDouble(row.get("Idle_Ratio"));
                values[4] = convertToDouble(row.get("Fetch_Wait_Ratio"));
                values[5] = convertToDouble(row.get("Process_Ratio"));
                values[6] = convertToDouble(row.get("Write_Ratio"));
                metricsByTraceId.put(traceId, values);
            }
        }
        return metricsByTraceId;
    }

    private static double convertToDouble(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}

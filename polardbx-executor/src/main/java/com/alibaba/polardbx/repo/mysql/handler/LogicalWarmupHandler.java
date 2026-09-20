package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.config.SqlEngineAlert;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupAccessor;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalWarmup;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlWarmup;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalWarmupHandler extends HandlerCommon {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");

    public LogicalWarmupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        Preconditions.checkArgument(logicalPlan instanceof LogicalWarmup);
        SqlWarmup sqlWarmup = ((LogicalWarmup) logicalPlan).getSqlWarmup();

        ArrayResultCursor result = new ArrayResultCursor("warmup");
        result.addColumn("START_TIME", new DateTimeType(6));
        result.addColumn("FINISH_TIME", new DateTimeType(6));
        result.addColumn("TIME_COST", DataTypes.LongType);
        result.addColumn("IO_MESSAGE", DataTypes.VarcharType);

        if (TStringUtil.isEmpty(sqlWarmup.getCronExpression())) {

            // without cron-expression, execute the warmup sql at once.
            List<String> sqlList = sqlWarmup.getSql();

            for (int i = 0; i < sqlList.size(); i++) {
                ExecutionContext newContext = executionContext.copy();
                IdGenerator traceIdGen = TrxIdGenerator.getInstance().getIdGenerator();
                String traceId = Long.toHexString(traceIdGen.nextId());
                newContext.setTraceId(traceId);

                if (newContext.getHintCmds() != null) {
                    newContext.getHintCmds().clear();
                }
                Object[] objects = executeAtOnce(newContext, sqlWarmup, i);
                result.addRow(objects);
            }
        } else {
            recordWarmupTask(executionContext, sqlWarmup);
        }

        return result;
    }

    private void recordWarmupTask(ExecutionContext executionContext, SqlWarmup sqlWarmup) {
        List<String> sqlList = sqlWarmup.getSql();
        List<String> hintList = sqlWarmup.getHint();
        Preconditions.checkArgument(sqlList.size() == hintList.size());

        // add warmup tasks into meta db
        String executionSql;
        if (sqlList.size() == 1) {
            String sql = sqlList.get(0);
            String hint = hintList.get(0);
            executionSql = hint == null ? sql : (hint + " " + sql);
            executionSql = executionSql.replaceAll("\\r|\\n", " ");

            validateSql(executionContext, executionSql);
        } else {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < sqlList.size(); i++) {
                String sql = sqlList.get(i);
                String hint = hintList.get(i);
                builder.append('{');
                String singleSql = (hint == null || hint.isEmpty()) ? sql : (hint + " " + sql);

                validateSql(executionContext, singleSql);

                singleSql = singleSql.replaceAll("\\r|\\n", " ");
                builder.append(singleSql);
                builder.append('}');
            }
            executionSql = builder.toString();
        }

        String cronExpression = sqlWarmup.getCronExpression();
        String schemaName = executionContext.getSchemaName();
        String instanceId = InstIdUtil.getInstId();

        // construct record
        ColumnarWarmupRecord columnarWarmupRecord = new ColumnarWarmupRecord();
        columnarWarmupRecord.setSchemaName(schemaName);
        columnarWarmupRecord.setInstanceId(instanceId);
        columnarWarmupRecord.setCronExpression(cronExpression);
        columnarWarmupRecord.setSqlDef(executionSql);
        columnarWarmupRecord.setStatus(0);

        ColumnarWarmupAccessor accessor = new ColumnarWarmupAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            accessor.setConnection(connection);
            accessor.insert(columnarWarmupRecord);

            // print event log
            LOGGER.info(MessageFormat.format(
                "Add new warmup task, schemaName: {0}, instanceId: {1}, cronExpression: {2}, sqlDef: {3}",
                schemaName, instanceId, cronExpression, executionSql));

        } catch (SQLException e) {
            // print event log
            LOGGER.error(MessageFormat.format(
                "Fail to add warmup task, schemaName: {0}, instanceId: {1}, cronExpression: {2}, sqlDef: {3}",
                schemaName, instanceId, cronExpression, executionSql));

            throw GeneralUtil.nestedException(e);
        }
    }

    private void validateSql(ExecutionContext executionContext, String executionSql) {
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.init();

        // validate sql.
        Planner planner = Planner.getInstance();
        planner.plan(executionSql, executionContext);
    }

    @NotNull
    private static Object[] executeAtOnce(ExecutionContext executionContext, SqlWarmup sqlWarmup, int sqlIndex) {

        List<String> sqlList = sqlWarmup.getSql();
        List<String> hintList = sqlWarmup.getHint();
        Preconditions.checkArgument(sqlList.size() == hintList.size());

        executionContext.setWarmup(true);
        String sql = sqlList.get(sqlIndex);
        String hint = hintList.get(sqlIndex);
        String executionSql = hint == null ? sql : (hint + " " + sql);

        PlanExecutor planExecutor = PlanExecutor.create();
        planExecutor.init();

        Planner planner = Planner.getInstance();
        ExecutionPlan executionPlan = planner.plan(executionSql, executionContext);

        // for statistics collection.
        executionContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.setSqlExplainLevel(SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        if (executionContext.getHintCmds() == null) {
            executionContext.putAllHintCmds(new HashMap<>());
        }
        executionContext.getHintCmds()
            .put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.OPERATOR.metricLevel);
        executionContext.getExtraCmds()
            .put(ConnectionProperties.ENABLE_SPM, false);
        executionContext.getExtraCmds()
            .put(ConnectionProperties.EXECUTOR_MODE, ExecutorMode.MPP);
        executionContext.getExtraCmds()
            .put(ConnectionProperties.WORKLOAD_TYPE, WorkloadType.AP);
        executionContext.setWorkloadType(WorkloadType.AP);
        executionContext.setExecuteMode(ExecutorMode.MPP);

        RuntimeStatistics statistics = RuntimeStatHelper.buildRuntimeStat(executionContext);
        executionContext.setRuntimeStatistics(statistics);
        statistics.setPlanTree(executionPlan.getPlan());

        // get start time.
        ZonedDateTime zonedDateTime;
        if (executionContext.getTimeZone() != null) {
            ZoneId zoneId = executionContext.getTimeZone().getZoneId();
            zonedDateTime = ZonedDateTime.now(zoneId);
        } else {
            zonedDateTime = ZonedDateTime.now();
        }
        MysqlDateTime startTime = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);
        OriginalTimestamp startTimeStr = new OriginalTimestamp(startTime);

        // Execute the plan.
        StringBuilder builder = new StringBuilder();
        ResultCursor cursor = null;
        List<Throwable> exceptionList = new ArrayList<>();
        Parameters parameters = executionContext.cloneParamsOrNull();

        try {
            cursor = PlanExecutor.execute(executionPlan, executionContext);
            executionContext.setParams(parameters);

            while (cursor.next() != null) {
                // discard rows.
            }
        } finally {
            if (cursor != null) {
                cursor.close(exceptionList);
                if (!exceptionList.isEmpty()) {
                    builder.append("Exception: ").append(exceptionList).append(". ");

                    // event log: warmup task execution error.
                    EventLogger.log(EventType.COLUMNAR_WARMUP, sqlWarmup + ", " + builder);
                    SqlEngineAlert.getInstance().putColumnarWarmUp(sqlWarmup.toString());
                }
            }
        }

        // collect statistics.
        long totalIOBytes = 0L;
        Map<RelNode, RuntimeStatisticsSketch> runtimeStatistic = statistics.toMppSketch();
        for (Map.Entry<RelNode, RuntimeStatisticsSketch> entry : runtimeStatistic.entrySet()) {
            RuntimeStatisticsSketch sketch = entry.getValue();
            RelNode relNode = entry.getKey();
            if (sketch.getIoBytesCount() > 0) {
                totalIOBytes += sketch.getIoBytesCount();

                builder.append(relNode.getClass().getSimpleName())
                    .append(": ").append(sketch.getIoBytesCount()).append(" bytes. ");
            }
        }
        builder.append(" Total: ").append(totalIOBytes).append(" bytes.");
        executionContext.getCalcitePlanOptimizerTrace().ifPresent(
            x -> x.getOptimizerTracer().setRuntimeStatistics(runtimeStatistic));

        // get finish time.
        if (executionContext.getTimeZone() != null) {
            ZoneId zoneId = executionContext.getTimeZone().getZoneId();
            zonedDateTime = ZonedDateTime.now(zoneId);
        } else {
            zonedDateTime = ZonedDateTime.now();
        }
        MysqlDateTime endTime = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);
        OriginalTimestamp endTimeStr = new OriginalTimestamp(endTime);

        MysqlDateTime diff = MySQLTimeCalculator.calTimeDiff(endTime, startTime, false);
        long timeCost;
        if (diff == null) {
            timeCost = -1;
        } else {
            long sec = diff.getHour() * 3600L + diff.getMinute() * 60 + diff.getSecond();
            long millis = sec * 1000L + diff.getSecondPart() / 1_000_000L;
            timeCost = millis;
        }

        Object[] objects = new Object[] {startTimeStr, endTimeStr, timeCost, builder.toString()};
        return objects;
    }
}

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.async.GroupTaskExecutor;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.util.StatisticFullProcessUtils;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.statistic.CollectStatisticProgress;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCollectStatistic;
import org.apache.calcite.sql.SqlIdentifier;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.*;

/**
 * @author pangzhaoxing
 */
public class CollectStatisticHandler extends HandlerCommon {

    private static final Logger logger = LoggerUtil.statisticsLogger;

    public static final String PARALLEL_STATISTIC_EXECUTOR_NAME = "ParallelStatisticExecutor";

    public CollectStatisticHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        SqlCollectStatistic sqlCollectStatistic = (SqlCollectStatistic) ((LogicalDal) logicalPlan).getNativeSqlNode();

        boolean allSchema = sqlCollectStatistic.getSchemas() == null;
        //逻辑表并行
        int statisticParallelism = executionContext.getParamManager().getInt(ConnectionParams.STATISTIC_PARALLELISM);
        boolean enableCollectHll = executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COLLECT_HLL)
            && InstConfUtil.getBool(ConnectionParams.ENABLE_HLL);
        int dnHllParallelStatistic =
            executionContext.getParamManager().getInt(ConnectionParams.DN_HLL_STATISTIC_PARALLELISM);

        long startTime = System.nanoTime();
        ModuleLogInfo.getInstance().logRecord(
            Module.STATISTICS,
            PROCESS_START,
            new String[] {
                "CollectStatisticHandler : " + sqlCollectStatistic.toString(),
                "traceId : " + executionContext.getTraceId() + ", allSchema : " + allSchema
                    + ", statisticParallelism : " + statisticParallelism
                    + ", enableCollectHll : " + enableCollectHll
                    + ", dnHllParallelStatistic : " + dnHllParallelStatistic
            },
            NORMAL);

        ExecutorService parallelStatisticExecutor = null;
        //collect table names
        List<Pair<String, String>> tableNames = new ArrayList<>();

        CollectStatisticProgress collectStatisticProgress = new CollectStatisticProgress(executionContext,
            sqlCollectStatistic.toString(), tableNames, statisticParallelism, enableCollectHll, dnHllParallelStatistic,
            LocalDateTime.now(), Thread.currentThread());
        CollectStatisticProgress.appendCollectStatisticProgress(collectStatisticProgress);
        try {
            //1. preparing
            prepareCollectTables(sqlCollectStatistic, tableNames);

            //逻辑表并行
            if (statisticParallelism > 1) {
                parallelStatisticExecutor = Executors.newFixedThreadPool(statisticParallelism,
                    new NamedThreadFactory("connection-" + executionContext.getConnId() + "-ParallelStatisticExecutor",
                        true));
                executionContext.setParallelStatisticExecutor(parallelStatisticExecutor);
            }

            //dn hll 并行
            Set<String> dnIds = getAllStorageInstId();
            if (enableCollectHll && dnHllParallelStatistic > 0) {
                Map<String, Integer> map = new HashMap<>();
                for (String dnId : dnIds) {
                    map.put(dnId, dnHllParallelStatistic);
                }
                GroupTaskExecutor hllTaskExecutor =
                    new GroupTaskExecutor("connection-" + executionContext.getConnId() + "-statistic-hll", map);
                executionContext.setHllExecutor(hllTaskExecutor);
            }

            //2. collecting
            List<Future<Boolean>> futures = Collections.synchronizedList(new ArrayList<>());
            collectStatisticProgress.setFutures(futures);
            for (int i = 0; i < tableNames.size(); i++) {
                String schema = tableNames.get(i).getKey();
                String table = tableNames.get(i).getValue();
                futures.add(StatisticFullProcessUtils.collectStatisticConcurrent(schema, table, enableCollectHll, executionContext,
                    parallelStatisticExecutor));
            }

            ArrayResultCursor result = new ArrayResultCursor("collect_static_result");
            result.addColumn("schema", null, DataTypes.StringType);
            result.addColumn("table", null, DataTypes.StringType);
            result.addColumn("enable_collect_hll", null, DataTypes.StringType);
            result.addColumn("collect_static", null, DataTypes.StringType);

            for (int i = 0; i < tableNames.size(); i++) {
                String schema = tableNames.get(i).getKey();
                String table = tableNames.get(i).getValue();
                try {
                    result.addRow(new Object[] {
                        schema, table, enableCollectHll ? "true" : "false",
                        futures.get(i).get() ? "success" : "failed"});
                } catch (ExecutionException e) {
                    logger.error("collect statistic one table failed : " + schema + "." + table);
                    result.addRow(new Object[] {schema, table, enableCollectHll ? "true" : "false", "failed"});
                } catch (InterruptedException e) {
                    //collect statistic sql is interrupted， finally code(parallelStatisticExecutor.shutdownNow) will interrupt all logical table collect task;
                    logger.error("collect statistic is interrupted : " + sqlCollectStatistic);
                    throw new TddlNestableRuntimeException(e);
                }
            }

            long endTime = System.nanoTime();
            ModuleLogInfo.getInstance().logRecord(
                Module.STATISTICS,
                PROCESS_END,
                new String[] {
                    "CollectStatisticHandler : " + sqlCollectStatistic.toString(),
                    "traceId : " + executionContext.getTraceId() + " , time : " + (endTime - startTime) / 1000000000
                        + "s "
                },
                NORMAL);

            return result;
        } catch (Exception e) {
            ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, INTERRUPTED,
                new String[] {"CollectStatisticHandler : " + sqlCollectStatistic.toString(), e.getMessage()}, WARNING);
            throw e;
        } finally {
            if (shutdownAllStatisticExecutor(executionContext)) {
                CollectStatisticProgress.removeCollectStatisticProgress(executionContext.getConnId());
            } else {
                logger.warn(
                    "shutdown all statistic executor failed, use sql (cancel collect statistic connection_id) to try again");
            }
        }
    }

    private void prepareCollectTables(SqlCollectStatistic sqlCollectStatistic, List<Pair<String, String>> tableNames) {
        if (sqlCollectStatistic.getSchemas() == null) {
            for (String schema : DbInfoManager.getInstance().getDbList()) {
                if (SystemDbHelper.isDBBuildIn(schema)) {
                    continue;
                }

                Set<String> logicalTableSet = StatisticManager.getInstance().getTableNamesCollected(schema);
                for (TableMeta tableMeta : OptimizerContext.getContext(schema).getLatestSchemaManager()
                    .getAllUserTables()) {
                    logicalTableSet.add(tableMeta.getTableName().toLowerCase());
                }
                for (String logicalTableName : logicalTableSet) {
                    tableNames.add(new Pair<>(schema, logicalTableName));
                }
            }
        } else {
            for (SqlIdentifier sqlIdentifier : sqlCollectStatistic.getSchemas()) {
                if (sqlIdentifier.names.size() == 1) {
                    String schema = sqlIdentifier.getSimple();
                    if (SystemDbHelper.isDBBuildIn(schema)) {
                        continue;
                    }

                    Set<String> logicalTableSet = StatisticManager.getInstance().getTableNamesCollected(schema);
                    for (TableMeta tableMeta : OptimizerContext.getContext(schema).getLatestSchemaManager()
                        .getAllUserTables()) {
                        logicalTableSet.add(tableMeta.getTableName().toLowerCase());
                    }
                    for (String logicalTableName : logicalTableSet) {
                        tableNames.add(new Pair<>(schema, logicalTableName));
                    }
                } else {
                    tableNames.add(new Pair<>(sqlIdentifier.names.get(0), sqlIdentifier.names.get(1)));
                }
            }
        }
    }

    public static Set<String> getAllStorageInstId() {
        Set<String> dnIds = StorageHaManager.getInstance().getMasterStorageList()
            .stream()
            .filter(s -> !s.isMetaDb())
            .map(StorageInstHaContext::getStorageInstId)
            .collect(Collectors.toSet());
        return dnIds;
    }

    public static boolean shutdownAllStatisticExecutor(ExecutionContext ec) {
        boolean shutDownAllExecutor = true;
        if (ec.getParallelStatisticExecutor() != null) {
            shutDownAllExecutor =
                shutDownAllExecutor && AsyncUtils.shutdownNowAndAwaitTermination(ec.getParallelStatisticExecutor(), 30,
                    TimeUnit.SECONDS);
        }
        if (ec.getHllExecutor() != null) {
            shutDownAllExecutor = shutDownAllExecutor && ec.getHllExecutor().destroy();
        }
        return shutDownAllExecutor;
    }

}

/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelXPlanOptimizer;
import com.alibaba.polardbx.optimizer.core.planner.rule.Xplan.XPlanCalcRule;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalReplace;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.core.rel.TableId;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.htaprouting.HtapTrace;
import com.alibaba.polardbx.optimizer.index.AdviceResult;
import com.alibaba.polardbx.optimizer.index.Configuration;
import com.alibaba.polardbx.optimizer.index.IndexAdvisor;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.PlanShardInfo;
import com.alibaba.polardbx.optimizer.statis.XplanStat;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ExplainUtils;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.statistics.ExplainStatisticsHandler;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.calcite.util.trace.OptimizerPhase;
import org.apache.calcite.util.trace.PlanOptimizerTracer;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor.isAllDnUseXDataSource;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainAdvisor;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainAnalyzeExecute;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainDdlDag;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainExecute;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainJsonExecute;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainJsonPlan;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainKeyword;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainLogicalView;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOnlineDdl;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizerDetail;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainPipeline;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainRouting;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSchedule;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSharding;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSimple;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSnapshot;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainStatistics;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainTreeExecute;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainVec;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isPhysicalFragment;
import static com.alibaba.polardbx.optimizer.utils.RelUtils.disableMpp;

public class ExplainExecutorUtil {

    private static final int MAX_EXPLAIN_VEC_SUB_QUERY_STACK_SIZE = 16;

    public static ResultCursor explain(ExecutionPlan executionPlan, ExecutionContext executionContext,
                                       ExplainResult explain) {
        if (executionContext.getMemoryPool() == null) {
            executionContext.setMemoryPool(MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "ExplainCommandMemoryPool" + executionContext.getTraceId(), MemorySetting.UNLIMITED_SIZE,
                MemoryType.QUERY));
        }
        if (isExplainOptimizer(explain)) {
            return ExplainExecutorUtil.handleExplainWithStage(executionContext, executionPlan);
        } else if (isExplainSharding(explain)) {
            return ExplainExecutorUtil.handleExplainSharding(executionContext, executionPlan);
        } else if (isExplainSimple(explain)) {
            return ExplainExecutorUtil.handleExplainSimple(executionContext, executionPlan);
        } else if (isExplainJsonPlan(explain)) {
            return handleExplainJsonPlan(executionPlan, executionContext);
        } else if (isExplainExecute(explain)) {
            //mpp 模式现在还不支持 explain execute
            disableMpp(executionContext);
            return ExplainExecutorUtil.handleExplainExecute(executionPlan, executionContext);
        } else if (isExplainAnalyzeExecute(explain) || isExplainTreeExecute(explain) || isExplainJsonExecute(explain)) {
            if (!InstanceVersion.isMYSQL80()) {
                throw new NotSupportException("only support for dn 8.x");
            }
            disableMpp(executionContext);
            return ExplainExecutorUtil.handleExplainAnalyzeExecute(executionPlan, executionContext);
        } else if (isPhysicalFragment(explain)) {
            return ExplainExecutorUtil.handleExplainPhysical(executionPlan, executionContext, false);
        } else if (isExplainSchedule(explain)) {
            return ExplainExecutorUtil.handleExplainPhysical(executionPlan, executionContext, true);
        } else if (isExplainLogicalView(explain)) {
            PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
            boolean oldExplainLogicalView =
                plannerContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_LOGICALVIEW);
            plannerContext.getExtraCmds().put(ConnectionProperties.EXPLAIN_LOGICALVIEW, true);
            try {
                return ExplainExecutorUtil.handleExplain(executionContext, executionPlan, explain.explainMode);
            } finally {
                plannerContext.getExtraCmds().put(ConnectionProperties.EXPLAIN_LOGICALVIEW, oldExplainLogicalView);
            }
        } else if (isExplainAdvisor(explain)) {
            if (executionPlan.getPlan() instanceof BaseDdlOperation) {
                return handleDdl(executionContext, executionPlan);
            } else {
                return ExplainExecutorUtil.handleExplainAdvisor(executionContext, executionPlan);
            }
        } else if (isExplainRouting(explain)) {
            return handleExplainRouting(executionContext, executionPlan);
        } else if (isExplainStatistics(explain)) {
            return ExplainStatisticsHandler.handleExplainStatistics(executionContext, executionPlan);
        } else if (isExplainVec(explain)) {
            return ExplainExecutorUtil.handleExplainVec(executionContext, executionPlan, explain.explainMode);
        } else if (isExplainPipeline(explain)) {
            return ExplainExecutorUtil.handleExplainPipeline(executionContext, executionPlan);
        } else if (isExplainSnapshot(explain)) {
            return ExplainExecutorUtil.handleExplainSnapshot(executionContext, executionPlan);
        } else if (isExplainOnlineDdl(explain)) {
            if (executionPlan.getPlan() instanceof BaseDdlOperation) {
                return handleDdl(executionContext, executionPlan);
            } else {
                throw new NotSupportException("explain online_ddl non_ddl ");
            }
        } else if (isExplainDdlDag(explain)) {
            if (executionPlan.getPlan() instanceof BaseDdlOperation) {
                return handleDdl(executionContext, executionPlan);
            } else {
                throw new NotSupportException("explain ddl_dag non_ddl ");
            }
        } else if (executionPlan.getPlan() instanceof BaseDdlOperation) {
            return handleDdl(executionContext, executionPlan);
        } else if (isExplainKeyword(explain)) {
            return handleExplainKeyword(executionContext);
        } else {
            Object plan = (Object) executionPlan;
            return ExplainExecutorUtil.handleExplain(executionContext, executionPlan, explain.explainMode);
        }
    }

    static ResultCursor handleExplainPhysical(ExecutionPlan executionPlan, ExecutionContext executionContext,
                                              boolean withSchedule) {
        ExecutorHelper.selectExecutorMode(
            executionPlan.getPlan(), executionContext, true);
        if (executionContext.getExecuteMode() == ExecutorMode.MPP) {
            executionContext.getExtraCmds().put(ConnectionProperties.MERGE_UNION, false);
            return ExplainExecutorUtil.handleExplainMppPhysicalPlan(executionContext, executionPlan, withSchedule);
        } else if (executionContext.getExecuteMode() == ExecutorMode.AP_LOCAL
            || executionContext.getExecuteMode() == ExecutorMode.TP_LOCAL) {
            return ExplainExecutorUtil.handleExplainLocalPhysicalPlan(
                executionContext, executionPlan, executionContext.getExecuteMode());
        } else {
            PropUtil.ExplainOutputFormat outputFormat =
                (PropUtil.ExplainOutputFormat) executionContext.getParamManager()
                    .getEnum(ConnectionParams.EXPLAIN_OUTPUT_FORMAT);
            if (executionContext.getExecuteMode() == ExecutorMode.CURSOR
                && executionPlan.getAst().isA(SqlKind.DML)
                && outputFormat != PropUtil.ExplainOutputFormat.LEGACY) {
                return ExplainExecutorUtil.handleExplainLocalPhysicalPlan(
                    executionContext, executionPlan, executionContext.getExecuteMode());
            }
            return ExplainExecutorUtil.handleExplain(
                executionContext, executionPlan, ExplainResult.ExplainMode.LOGIC);
        }
    }

    private static ResultCursor handleExplainAdvisor(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        List<AdviceResult> adviceResultList = new ArrayList<>();
        IndexAdvisor indexAdvisor = new IndexAdvisor(executionPlan, executionContext);

        // check statistics
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CHECK_STATISTICS_EXPIRE)) {
            AdviceResult checkStatistics = indexAdvisor.checkStatistics();
            if (checkStatistics != null) {
                return ExplainExecutorUtil.handleExplainAdvisorResult(ImmutableList.of(checkStatistics));
            }
        }

        // if advise type defined
        String adviseTypeString = executionContext.getParamManager().getString(ConnectionParams.ADVISE_TYPE);
        if (adviseTypeString != null) {
            IndexAdvisor.AdviseType adviseType = null;
            if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.LOCAL_INDEX.toString())) {
                adviseType = IndexAdvisor.AdviseType.LOCAL_INDEX;
            } else if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.GLOBAL_INDEX.toString())) {
                adviseType = IndexAdvisor.AdviseType.GLOBAL_INDEX;
            } else if (adviseTypeString
                .equalsIgnoreCase(IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX.toString())) {
                adviseType = IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX;
            } else if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.BROADCAST.toString())) {
                adviseType = IndexAdvisor.AdviseType.BROADCAST;
            } else if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.COLUMNAR_INDEX.toString())) {
                adviseType = IndexAdvisor.AdviseType.COLUMNAR_INDEX;
            } else if (adviseTypeString.equalsIgnoreCase("ALL")) {
                AdviceResult localIndexAdviceResult = indexAdvisor.advise(IndexAdvisor.AdviseType.LOCAL_INDEX);
                AdviceResult gsiAdviceResult = indexAdvisor.advise(IndexAdvisor.AdviseType.GLOBAL_INDEX);
                AdviceResult coveringGsiAdviceResult =
                    indexAdvisor.advise(IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX);
                AdviceResult broadcastAdviceResult = indexAdvisor.advise(IndexAdvisor.AdviseType.BROADCAST);

                adviceResultList.add(localIndexAdviceResult);
                adviceResultList.add(gsiAdviceResult);
                adviceResultList.add(coveringGsiAdviceResult);
                adviceResultList.add(broadcastAdviceResult);

                return ExplainExecutorUtil.handleExplainAdvisorResult(adviceResultList);
            }
            if (adviseType != null) {
                AdviceResult adviceResult = indexAdvisor.advise(adviseType);
                adviceResultList.add(adviceResult);
                return ExplainExecutorUtil.handleExplainAdvisorResult(adviceResultList);
            }
        }

        return ExplainExecutorUtil.handleExplainAdvisorResult(adviceResultList);
    }

    private static ResultCursor handleExplainAdvisorResult(List<AdviceResult> adviceResultList) {
        ArrayResultCursor result = new ArrayResultCursor("AdviceResult");

        result.addColumn("IMPROVE_VALUE", DataTypes.StringType);
        result.addColumn("IMPROVE_CPU", DataTypes.StringType);
        result.addColumn("IMPROVE_MEM", DataTypes.StringType);
        result.addColumn("IMPROVE_IO", DataTypes.StringType);
        result.addColumn("IMPROVE_NET", DataTypes.StringType);
        result.addColumn("BEFORE_VALUE", DataTypes.DoubleType);
        result.addColumn("BEFORE_CPU", DataTypes.DoubleType);
        result.addColumn("BEFORE_MEM", DataTypes.DoubleType);
        result.addColumn("BEFORE_IO", DataTypes.DoubleType);
        result.addColumn("BEFORE_NET", DataTypes.DoubleType);
        result.addColumn("AFTER_VALUE", DataTypes.DoubleType);
        result.addColumn("AFTER_CPU", DataTypes.DoubleType);
        result.addColumn("AFTER_MEM", DataTypes.DoubleType);
        result.addColumn("AFTER_IO", DataTypes.DoubleType);
        result.addColumn("AFTER_NET", DataTypes.DoubleType);
        result.addColumn("ADVISE_INDEX", DataTypes.StringType);
        result.addColumn("NEW_PLAN", DataTypes.StringType);
        result.addColumn("INFO", DataTypes.StringType);
        result.initMeta();

        boolean hasRow = false;

        for (AdviceResult adviceResult : adviceResultList) {
            if (adviceResult != null && adviceResult.getConfiguration() != null
                && adviceResult.getAfterPlanForDisplay() != null) {
                Configuration configuration = adviceResult.getConfiguration();
                DrdsRelOptCostImpl beforeCost = (DrdsRelOptCostImpl) adviceResult.getBeforeCost();
                DrdsRelOptCostImpl afterCost = (DrdsRelOptCostImpl) configuration.getAfterCost();
                result.addRow(new Object[] {
                    afterCost.getValue() != 0 ?
                        toPercent((beforeCost.getValue() - afterCost.getValue()) / afterCost.getValue()) : "INF",
                    afterCost.getCpu() != 0 ?
                        toPercent((beforeCost.getCpu() - afterCost.getCpu()) / afterCost.getCpu()) : "INF",
                    afterCost.getMemory() != 0 ?
                        toPercent((beforeCost.getMemory() - afterCost.getMemory()) / afterCost.getMemory()) : "INF",
                    afterCost.getIo() != 0 ? toPercent((beforeCost.getIo() - afterCost.getIo()) / afterCost.getIo()) :
                        "INF",
                    afterCost.getNet() != 0 ?
                        toPercent((beforeCost.getNet() - afterCost.getNet()) / afterCost.getNet()) : "INF",
                    round(beforeCost.getValue(), 1),
                    round(beforeCost.getCpu(), 1),
                    round(beforeCost.getMemory(), 1),
                    round(beforeCost.getIo(), 1),
                    round(beforeCost.getNet(), 1),
                    round(afterCost.getValue(), 1),
                    round(afterCost.getCpu(), 1),
                    round(afterCost.getMemory(), 1),
                    round(afterCost.getIo(), 1),
                    round(afterCost.getNet(), 1),
                    configuration.broadcastSql() +
                        (configuration.getCandidateIndexSet().size() > 0 ?
                            String.join(";\n",
                                configuration.getCandidateIndexSet().stream()
                                    .map(candidateIndex -> candidateIndex.getSql()).collect(Collectors.toList()))
                                + ";" : ""),
                    adviceResult.getAfterPlanForDisplay(),
                    adviceResult.getInfo()
                });
                hasRow = true;
            }
        }
        if (!hasRow && !CollectionUtils.isEmpty(adviceResultList)) {
            DrdsRelOptCostImpl beforeCost = (DrdsRelOptCostImpl) adviceResultList.get(0).getBeforeCost();
            result.addRow(new Object[] {
                null,
                null,
                null,
                null,
                null,
                beforeCost == null ? 1 : round(beforeCost.getValue(), 1),
                beforeCost == null ? 1 : round(beforeCost.getCpu(), 1),
                beforeCost == null ? 1 : round(beforeCost.getMemory(), 1),
                beforeCost == null ? 1 : round(beforeCost.getIo(), 1),
                beforeCost == null ? 1 : round(beforeCost.getNet(), 1),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                adviceResultList.get(0).getInfo()
            });
        }
        if (CollectionUtils.isEmpty(adviceResultList)) {
            result.addRow(new Object[] {
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                "not supported"});
        }
        return result;
    }

    public static double round(double value, int scale) {
        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(scale, BigDecimal.ROUND_FLOOR);
        double d = bd.doubleValue();
        return d;
    }

    private static String toPercent(double s) {
        DecimalFormat fmt = new DecimalFormat("##0.0%");
        return fmt.format(s);
    }

    private static ResultCursor handleExplainJsonPlan(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        String logicalPlanString = PlanManagerUtil.relNodeToJson(executionPlan.getPlan());

        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Plan", DataTypes.StringType);
        result.initMeta();

        result.addRow(new Object[] {StringUtils.normalizeSpace(logicalPlanString).replace("\\", "\\\\")});

        result.addRow(new Object[] {""});
        AtomicInteger max = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();

        handleSubquerySimpleExplain(executionContext, executionPlan.getPlan(), result, max, current);
        return result;
    }

    private static ResultCursor handleDdl(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        ExecutionContext copyExecutionContext = executionContext.copy();
//        copyExecutionContext.setExplain(null);
        ExecutionPlan copyExectionPlan = executionPlan.copy(executionPlan.getPlan());
        String sourceSql = executionContext.getOriginSql();
        List<SQLStatement> statementList = FastsqlUtils.parseSql(sourceSql);
        MySqlExplainStatement mySqlExplainStatement = (MySqlExplainStatement) statementList.get(0);
        mySqlExplainStatement.setShowExplain(false);
        String sql = SQLUtils.toSQLString(mySqlExplainStatement, DbType.mysql, new SQLUtils.FormatOption(true, false));
        copyExecutionContext.setOriginSql(sql);
        copyExectionPlan.setExplain(false);
        return PlanExecutor.execute(copyExectionPlan, copyExecutionContext);
    }

    private static ResultCursor handleExplainWithStage(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        Preconditions.checkArgument(executionContext.getCalcitePlanOptimizerTrace().isPresent());
        final Function<RexNode, Object> evalFunc = RexUtils.getEvalFunc(executionContext);
        try {
            executionContext.getCalcitePlanOptimizerTrace().ifPresent(x -> {
                x.addPhaseSnapshot(OptimizerPhase.END, executionPlan.getPlan(),
                    PlannerContext.getPlannerContext(executionContext, evalFunc));
            });
            return buildExplainOptimizerCursor(executionContext, executionPlan);
        } finally {
            executionContext.getCalcitePlanOptimizerTrace().ifPresent(CalcitePlanOptimizerTrace::clean);
        }
    }

    private static ResultCursor buildExplainOptimizerCursor(ExecutionContext executionContext,
                                                            ExecutionPlan executionPlan) {
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }

        PlanOptimizerTracer optimizerTracer = executionContext.getCalcitePlanOptimizerTrace()
            .get().getOptimizerTracer();
        List<PlanOptimizerTracer.PhaseSnapshot> phaseSnapshots = optimizerTracer.getPhaseSnapshots();
        boolean showRules = isExplainOptimizerDetail(executionContext.getExplain());

        ArrayResultCursor result = new ArrayResultCursor("Optimizer Trace");
        result.addColumn("Optimizer Trace", DataTypes.StringType);
        result.initMeta();

        final String DIVIDER_MAJOR = "════════════════════════════════════════════════════════";
        final String DIVIDER_MINOR = "────────────────────────────────────────────────────────";

        // ── Summary: phase name + duration + progress-bar percentage ──────────
        long totalDurationMs = 0;
        for (PlanOptimizerTracer.PhaseSnapshot snapshot : phaseSnapshots) {
            if (!snapshot.isSkipped() && snapshot.getParent() == null) {
                totalDurationMs += snapshot.getDurationMs();
            }
        }

        result.addRow(new Object[] {"Phase Summary:"});
        for (PlanOptimizerTracer.PhaseSnapshot snapshot : phaseSnapshots) {
            String indent = snapshot.getIndent();
            String seq = snapshot.getSequenceNumber() != null ? snapshot.getSequenceNumber() + " " : "";
            String name = snapshot.getPhase().getDisplayName();
            if (snapshot.isSkipped()) {
                result.addRow(new Object[] {indent + "  " + seq + name + "  [SKIPPED]"});
            } else {
                long dur = snapshot.getDurationMs();
                int pct = totalDurationMs > 0 ? (int) (dur * 100 / totalDurationMs) : 0;
                String bar = buildProgressBar(pct, 20);
                result.addRow(new Object[] {
                    indent + "  " + seq + name + "  " + dur + " ms  " + bar + " " + pct + "%"});
            }
        }
        result.addRow(new Object[] {DIVIDER_MAJOR});

        for (PlanOptimizerTracer.PhaseSnapshot snapshot : phaseSnapshots) {
            String indent = snapshot.getIndent();
            String seq = snapshot.getSequenceNumber() != null ? snapshot.getSequenceNumber() + " " : "";
            String stageLabel = indent + seq + snapshot.getPhase().getDisplayName();

            result.addRow(new Object[] {DIVIDER_MAJOR});
            if (snapshot.isSkipped()) {
                result.addRow(new Object[] {stageLabel + "  [SKIPPED]"});
            } else {
                result.addRow(new Object[] {stageLabel + "  (" + snapshot.getDurationMs() + " ms)"});

                // rule invocation counts: only when EXPLAIN OPTIMIZER DETAIL
                if (showRules) {
                    Map<String, Integer> ruleCounts = snapshot.getRuleCounts();
                    if (!ruleCounts.isEmpty()) {
                        result.addRow(new Object[] {DIVIDER_MINOR});
                        result.addRow(new Object[] {"Rule counts:"});
                        List<Map.Entry<String, Integer>> sorted = new ArrayList<>(ruleCounts.entrySet());
                        sorted.sort((a, b) -> b.getValue().compareTo(a.getValue()));
                        for (Map.Entry<String, Integer> entry : sorted) {
                            result.addRow(new Object[] {
                                "  " + entry.getKey() + ": " + entry.getValue()});
                        }
                    }
                }

                // rule-level detail: only when EXPLAIN OPTIMIZER DETAIL
                // Each RuleSnapshot holds the plan BEFORE that rule fired.
                // Output: plan-before-rule[i], then rule[i] name (the rule transforms the plan above it).
                // PhaseSnapshot.planDisplay is the plan AFTER all rules finished (output at the end).
                if (showRules) {
                    List<PlanOptimizerTracer.RuleSnapshot> phaseRules = snapshot.getRuleSnapshots();
                    if (!phaseRules.isEmpty()) {
                        result.addRow(new Object[] {DIVIDER_MINOR});
                        result.addRow(new Object[] {"Rules applied:"});
                        for (int i = 0; i < phaseRules.size(); i++) {
                            PlanOptimizerTracer.RuleSnapshot rs = phaseRules.get(i);
                            if (rs.getPlanDisplay() != null) {
                                for (String line : StringUtils.split(rs.getPlanDisplay(), "\r\n")) {
                                    result.addRow(new Object[] {"  " + line});
                                }
                            }
                            result.addRow(new Object[] {"  => [" + (i + 1) + "] " + rs.getRuleName()});
                        }
                    }
                }

                if (snapshot.getPlanDisplay() != null) {
                    result.addRow(new Object[] {DIVIDER_MINOR});
                    for (String line : StringUtils.split(snapshot.getPlanDisplay(), "\r\n")) {
                        result.addRow(new Object[] {line});
                    }
                }
            }
        }

        return result;
    }

    private static ResultCursor handleExplainSharding(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        final ArrayResultCursor result = new ArrayResultCursor("Sharding Table");
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        result.addColumn("Logical_Table", DataTypes.StringType);
        result.addColumn("Sharding", DataTypes.StringType);
        result.addColumn("Shard_Count", DataTypes.StringType);
        result.addColumn("Broadcast", DataTypes.StringType);
        result.addColumn("Condition", DataTypes.StringType);
        result.initMeta();

        final RelNode plan = executionPlan.getPlan();
        final String schemaName = executionContext.getSchemaName();
        final Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        final ExtractionResult er = ConditionExtractor.predicateFrom(plan).extract();

        final Map<String, Map<String, Comparative>> allComps = new HashMap<>();
        final List<Pair<String, String>> logicalTables = new ArrayList<>();

        if (plan instanceof DirectMultiDBTableOperation) {
            List<TableId> tableIds = ((DirectMultiDBTableOperation) plan).getLogicalTables();
            List<TableId> physicalTableNames = ((DirectMultiDBTableOperation) plan).getPhysicalTableNames();
            for (int i = 0; i < tableIds.size(); i++) {
                result.addRow(new Object[] {
                    tableIds.get(i).getTableName(), physicalTableNames.get(i).getTableName(), 1, "false", ""});
            }
            return result;
        }
        if (plan instanceof DirectTableOperation) {
            ((DirectTableOperation) plan).getLogicalTableNames()
                .forEach(t -> logicalTables.add(Pair.of(schemaName, t)));
        } else {
            er.allCondition(
                allComps, executionContext,
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_DRDS_REX_ROUTE));
            er.getLogicalTables().forEach(t -> logicalTables.add(RelUtils.getQualifiedTableName(t)));
        }

        PlanShardInfo planShardInfo = ConditionExtractor.predicateFrom(plan).extract().allShardInfo(
            executionContext, executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_DRDS_REX_ROUTE));

        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());
        calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, executionContext);

        for (Pair<String, String> qn : logicalTables) {
            final OptimizerContext context = OptimizerContext.getContext(qn.left);
            final PartitionInfoManager partitionInfoManager = context.getPartitionInfoManager();
            final TddlRuleManager or = context.getRuleManager();
            final Partitioner partitioner = context.getPartitioner();
            final String tableName = qn.getValue();
            final Map<String, Comparative> comps = allComps.get(tableName);
            final Map<String, Set<String>> tableMap = new LinkedHashMap<>();
            final Set<String> groupSet = new HashSet<>();
            int shardCount = 0;
            boolean isPhyTableGet = true;
            List<TargetDB> tdbs = new ArrayList<>();
            if (!partitionInfoManager.isNewPartDbTable(tableName)) {
                calcParams.remove(CalcParamsAttribute.DB_SHARD_KEY_SET);
                calcParams.remove(CalcParamsAttribute.TB_SHARD_KEY_SET);
                try {
                    tdbs = or
                        .shard(tableName, true, true, comps, params, calcParams, executionContext);
                } catch (Exception e) {
                    isPhyTableGet = false;
                }
            } else {
                PartitionPruneStep partitionPruneStep =
                    planShardInfo.getRelShardInfo(qn.left, qn.right).getPartPruneStepInfo();
                PartPrunedResult
                    partPrunedResult = PartitionPruner.doPruningByStepInfo(partitionPruneStep, executionContext);
                tdbs = PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(partPrunedResult);
            }

            for (TargetDB targetDB : tdbs) {
                groupSet.add(targetDB.getDbIndex());
                if (!tableMap.containsKey(tableName)) {
                    tableMap.put(tableName, new HashSet<>());
                }
                shardCount += targetDB.getTableNames().size();
                tableMap.get(tableName).addAll(targetDB.getTableNames());
            }

            final String phyTableString =
                isPhyTableGet ? ExplainUtils.compressPhyTableString(tableMap, groupSet) : tableName;
            final TableRule tableRule = or.getTableRule(tableName);

            StringBuilder condition = new StringBuilder();
            Map<String, DataType> tmpDataTypeMap = null;
            if (!MapUtils.isEmpty(comps)) {
                SchemaManager schemaManager =
                    OptimizerContext.getContext(executionContext.getSchemaName()).getLatestSchemaManager();
                tmpDataTypeMap = PlannerUtils.buildDataType(ImmutableList.copyOf(comps.keySet()),
                    schemaManager.getTable(tableName));

                for (Map.Entry<String, Comparative> entry : comps.entrySet()) {
                    final String column = entry.getKey();
                    final Comparative comparative = partitioner.getComparativeByFetcher(
                        tableRule,
                        comps,
                        column,
                        params,
                        tmpDataTypeMap,
                        calcParams);

                    final String conditionString = null == comparative ? "" : ExplainUtils.comparativeToString(column,
                        comparative,
                        null);
                    if (TStringUtil.isNotBlank(conditionString)) {
                        if (condition.length() > 0) {
                            condition.append(", ");
                        }
                        condition.append(conditionString);
                    }
                } // end of for
            }

            boolean isBroacast = or.isBroadCast(tableName);
            if (isBroacast) {
                shardCount = 1;
            }
            result.addRow(new Object[] {
                tableName, phyTableString, shardCount, String.valueOf(isBroacast),
                condition.toString()});
        } // end of for

        return result;
    }

    private static ResultCursor handleExplainSimple(ExecutionContext executionContext, ExecutionPlan executionPlan) {

        String logicalPlanString = RelOptUtil.dumpPlan("",
            executionPlan.getPlan(),
            SqlExplainFormat.TEXT,
            SqlExplainLevel.NO_ATTRIBUTES);
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.initMeta();

        for (String row : StringUtils.split(logicalPlanString, "\r\n")) {
            result.addRow(new Object[] {row});
        }

        result.addRow(new Object[] {""});
        AtomicInteger max = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();

        handleSubquerySimpleExplain(executionContext, executionPlan.getPlan(), result, max, current);
        return result;
    }

    private static void handleSubquerySimpleExplain(ExecutionContext executionContext, RelNode logicalPlan,
                                                    ArrayResultCursor result, AtomicInteger max,
                                                    AtomicInteger current) {
        if (current.incrementAndGet() > max.get()) {
            max.set(current.get());
        }

        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(logicalPlan)) {
            if (rexDynamicParam.getIndex() == -2) {
                result.addRow(new Object[] {">> individual scalar subquery"});
            } else {
                result.addRow(new Object[] {">> individual correlate subquery"});
            }
            String subLogicalPlanString = RelOptUtil.dumpPlan("",
                rexDynamicParam.getRel(),
                SqlExplainFormat.TEXT,
                SqlExplainLevel.NO_ATTRIBUTES);
            for (String row : StringUtils.split(subLogicalPlanString, "\r\n")) {
                result.addRow(new Object[] {row});
            }
            result.addRow(new Object[] {""});
            handleSubqueryExplain(executionContext, rexDynamicParam.getRel(), result, max, current);
        }
        current.decrementAndGet();
    }

    private static void handleSubqueryExplain(ExecutionContext executionContext, RelNode logicalPlan,
                                              ArrayResultCursor result, AtomicInteger max, AtomicInteger current) {
        if (current.incrementAndGet() > max.get()) {
            max.set(current.get());
        }

        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(logicalPlan)) {
            if (rexDynamicParam.getIndex() == -2) {
                result.addRow(new Object[] {">> individual scalar subquery : " + rexDynamicParam.getRel().hashCode()});
            } else {
                result.addRow(new Object[] {
                    ">> individual correlate subquery : "
                        + rexDynamicParam.getRel().hashCode()});
            }
            String subLogicalPlanString = RelUtils.toString(rexDynamicParam.getRel(), executionContext.getParams()
                .getCurrentParameter(), null, executionContext);
            for (String row : StringUtils.split(subLogicalPlanString, "\r\n")) {
                result.addRow(new Object[] {row});
            }
            result.addRow(new Object[] {""});
            handleSubqueryExplain(executionContext, rexDynamicParam.getRel(), result, max, current);
        }
        current.decrementAndGet();
    }

    private static ResultCursor handleExplainRouting(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        ArrayResultCursor result = new ArrayResultCursor("RoutingInfo");
        result.addColumn("Routing Type", DataTypes.StringType);
        result.addColumn("Candidate Optimizer Types", DataTypes.StringType);
        result.addColumn("Workload Type", DataTypes.StringType);
        result.addColumn("Optimizer Type", DataTypes.StringType);
        result.addColumn("Plan Type", DataTypes.StringType);
        result.addColumn("Detail trace", DataTypes.StringType);
        result.initMeta();
        if (executionContext.getHtapTrace().isPresent()) {
            HtapTrace trace = executionContext.getHtapTrace().get();
            result.addRow(new Object[] {
                trace.printRoutType(),
                trace.printCandidateOptimizerTypes(),
                trace.printWorkLoadType(),
                trace.printOptimizerType(),
                trace.printPlanType(),
                trace.printDetails()});
        }
        return result;
    }

    private static ResultCursor handleExplainVec(ExecutionContext executionContext, ExecutionPlan executionPlan,
                                                 ExplainResult.ExplainMode mode) {
        SqlExplainLevel explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        executionContext.getCalcitePlanOptimizerTrace().ifPresent(x -> x.setSqlExplainLevel(explainLevel));

        Map<Integer, ParameterContext> parameters = executionContext.getParams().getCurrentParameter();
        Function<RexNode, Object> evalFunc = RexUtils.getEvalFunc(executionContext);

        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.addColumn("Extra info", DataTypes.StringType);
        result.initMeta();

        Function<RelNode, String> extraInfoBuilder = relNode -> {
            if (relNode == null) {
                return "";
            } else {
                StringBuilder builder = new StringBuilder();
                builder
                    .append(relNode.getClass().getSimpleName())
                    .append(":")
                    .append(relNode.getId());
                if (relNode instanceof Project) {
                    return buildExtraInfoForProject(builder, executionContext, (Project) relNode);
                } else if (relNode instanceof OSSTableScan) {
                    return buildExtraInfoForOSSTableScan(builder, executionContext, (OSSTableScan) relNode);
                } else {
                    return builder.toString();
                }
            }
        };

        List<Object[]> res = RelUtils.toStringWithExtraInfo(
            executionPlan.getPlan(), parameters, evalFunc, executionContext,
            extraInfoBuilder);

        res.forEach(result::addRow);

        handleSubQueryExplainVec(executionContext, executionPlan.getPlan(), result, extraInfoBuilder, 0);

        return result;
    }

    private static void handleSubQueryExplainVec(ExecutionContext executionContext, RelNode logicalPlan,
                                                 ArrayResultCursor result,
                                                 Function<RelNode, String> extraInfoBuilder, int level) {
        // prevent from stack overflow.
        if (level > MAX_EXPLAIN_VEC_SUB_QUERY_STACK_SIZE) {
            return;
        }

        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(logicalPlan)) {
            if (rexDynamicParam.getIndex() == -2) {
                result.addRow(new Object[] {">> individual scalar subquery : " + rexDynamicParam.getRel().hashCode()});
            } else {
                result.addRow(new Object[] {
                    ">> individual correlate subquery : "
                        + rexDynamicParam.getRel().hashCode()});
            }

            List<Object[]> res = RelUtils.toStringWithExtraInfo(
                rexDynamicParam.getRel(),
                executionContext.getParams().getCurrentParameter(),
                null,
                executionContext,
                extraInfoBuilder);

            res.forEach(result::addRow);

            // split row
            result.addRow(new Object[] {"", ""});

            // next sub-query
            handleSubQueryExplainVec(executionContext, rexDynamicParam.getRel(), result, extraInfoBuilder, level);
        }
    }

    private static String buildExtraInfoForProject(StringBuilder treeBuilder, ExecutionContext executionContext,
                                                   Project relNode) {
        Project project = relNode;

        RelNode input = project.getInput();
        List<DataType<?>> inputTypes = getInputDataType(input);

        treeBuilder.append('\n');

        for (RexNode rexNode : project.getProjects()) {
            // binding to vectorized expressions.
            VectorizedExpression vectorizedExpression = VectorizedExpressionBuilder
                .buildVectorizedExpression(inputTypes, rexNode, executionContext).getKey();

            String digest = VectorizedExpressionUtils.digest(vectorizedExpression);
            treeBuilder
                .append(rexNode.toString())
                .append('\n')
                .append(digest);
        }

        return treeBuilder.toString();
    }

    private static String buildExtraInfoForOSSTableScan(StringBuilder treeBuilder, ExecutionContext context,
                                                        OSSTableScan ossTableScan) {
        OrcTableScan orcTableScan = ossTableScan.getOrcNode();
        treeBuilder.append('\n');
        if (!orcTableScan.getFilters().isEmpty()) {
            RexNode rexNode = orcTableScan.getFilters().get(0);
            List<DataType<?>> inputTypes = orcTableScan.getInProjectsDataType();
            // binding vec expression
            RexNode root = VectorizedExpressionBuilder.rewriteRoot(rexNode, true);
            InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
            root = root.accept(inputRefTypeChecker);
            Rex2VectorizedExpressionVisitor converter =
                new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
            VectorizedExpression vectorizedExpression = root.accept(converter);
            String digest = VectorizedExpressionUtils.digest(vectorizedExpression);
            treeBuilder
                .append(rexNode.toString())
                .append('\n')
                .append(digest);
        }
        return treeBuilder.toString();
    }

    private static List<DataType<?>> getInputDataType(RelNode input) {
        // get input types from rel data type
        return input.getRowType()
            .getFieldList()
            .stream()
            .map(f -> new Field(f.getType()).getDataType())
            .map(d -> (DataType<?>) d)
            .collect(Collectors.toList());
    }

    protected static ResultCursor handleExplainDdl(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        if (executionPlan.getPlan() instanceof BaseDdlOperation) {
            final BaseDdlOperation plan = (BaseDdlOperation) executionPlan.getPlan();
            ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
            result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
            result.addColumn("DDL Sql", DataTypes.StringType);
            result.addColumn("Logical_Table", DataTypes.StringType);
            result.addColumn("Sharding", DataTypes.StringType);
            result.addColumn("Count", DataTypes.IntegerType);
            result.initMeta();
            result.addRow(new Object[] {
                "DDL:ReturnType:" + plan.getRowType() + ",HitCache:" + executionPlan.isHitCache(),
                executionPlan.getAst().toString(), plan.getTableName(),
                "", -1});
            return result;
        } else {
            return handleExplain(executionContext, executionPlan, ExplainResult.ExplainMode.DETAIL);
        }

    }

    private static ResultCursor handleExplainKeyword(ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("KeyWord");
        result.addColumn("keywords_list", DataTypes.StringType);
        result.initMeta();
        SqlParameterized sqlParameterized = executionContext.getSqlParameterized();
        if (sqlParameterized != null) {
            String sql = sqlParameterized.getSql();
            MySqlLexer lexer = new MySqlLexer(ByteString.from(sql));
            List<String> keywords = Lists.newArrayList();
            do {
                lexer.nextToken();
                String word =
                    lexer.subString(lexer.getStartPos(), lexer.pos() - lexer.getStartPos()).toLowerCase().trim();
                if (StringUtils.isEmpty(word)) {
                    continue;
                }
                word = SQLUtils.normalizeNoTrim(word);
                keywords.add("'" + word + "'");
            } while (lexer.token() != Token.EOF);
            result.addRow(new Object[] {String.join(", ", keywords)});
        }
        return result;
    }

    private static ResultCursor handleExplainPipeline(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        // To collect the runtime driver stats that detected by StageInfo.
        Map<String, List<Object[]>> driverStatistics = new HashMap<>();
        executionContext.setDriverStatistics(driverStatistics);

        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("trace_id", DataTypes.StringType);
        result.addColumn("stage-pipeline", DataTypes.StringType);
        result.addColumn("node_id", DataTypes.StringType);
        result.addColumn("driver_id", DataTypes.StringType);
        result.addColumn("running_cost", DataTypes.StringType);
        result.addColumn("pending_cost", DataTypes.StringType);
        result.addColumn("blocked_cost", DataTypes.StringType);
        result.addColumn("open_cost", DataTypes.StringType);
        result.addColumn("total_cost", DataTypes.StringType);
        result.addColumn("running_count", DataTypes.StringType);
        result.addColumn("pending_count", DataTypes.StringType);
        result.addColumn("blocked_count", DataTypes.StringType);
        result.addColumn("split_stats", DataTypes.StringType);
        result.addColumn("read_bytes", DataTypes.StringType);
        result.addColumn("input_rows", DataTypes.StringType);
        result.addColumn("output_rows", DataTypes.StringType);
        result.addColumn("block_reason", DataTypes.StringType);
        result.initMeta();

        ExecutorHelper.selectExecutorMode(executionPlan.getPlan(), executionContext, true);
        if (executionContext.getExecuteMode() == ExecutorMode.MPP
            || executionContext.getExecuteMode() == ExecutorMode.AP_LOCAL) {
            // The statement of EXPLAIN PIPELINE is only for MPP mode.
            executePlanForExplainAnalyze(executionPlan, executionContext);

            // Sorting all driver information according to their unique id.
            List<Object[]> allDriverInfo = new ArrayList<>();
            driverStatistics.values().forEach(allDriverInfo::addAll);
            Collections.sort(allDriverInfo, (o1, o2) -> {
                    int comparison;
                    if ((comparison = String.CASE_INSENSITIVE_ORDER.compare((String) o1[1], (String) o2[1])) != 0) {
                        return comparison;
                    } else if ((comparison = Long.valueOf((String) o1[2]).compareTo(Long.valueOf((String) o2[2]))) != 0) {
                        return comparison;
                    } else {
                        return Long.valueOf((String) o1[3]).compareTo(Long.valueOf((String) o2[3]));
                    }
                }

            );

            for (Object[] driverInfo : allDriverInfo) {
                result.addRow(driverInfo);
            }
        }

        return result;
    }

    private static ResultCursor handleExplainSnapshot(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        ArrayResultCursor result = new ArrayResultCursor("ColumnarSnapshot");
        result.addColumn("INDEX_NAME", DataTypes.StringType);
        result.addColumn("SNAPSHOT_TSO", DataTypes.LongType);
        result.addColumn("SNAPSHOT_INFO", DataTypes.StringType);
        result.initMeta();

        final RelNode plan = executionPlan.getPlan();
        final String schemaName = executionContext.getSchemaName();
        final Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        LogicalViewFinder finder = new LogicalViewFinder();
        plan.accept(finder);

        Long tsoFromGms = null;
        Long tsoFromTrans = null;
        if (executionContext.getTransaction() != null &&
            executionContext.getTransaction() instanceof IColumnarTransaction) {
            long tso = ((IColumnarTransaction) executionContext.getTransaction()).getSnapshotSeq();
            if (tso > 0) {
                tsoFromTrans = tso;
            }
        }
        for (LogicalView lv : finder.getResult()) {
            StringBuilder snapshotInfoBuilder = new StringBuilder();
            boolean isFirstElement = true;
            if (!(lv instanceof OSSTableScan)) {
                continue;
            }

            OSSTableScan ossTableScan = (OSSTableScan) lv;
            Long tso = ossTableScan.getFlashbackQueryTso(executionContext);
            if (ossTableScan.isColumnarIndex()) {
                if (tso == null) {
                    if (tsoFromTrans != null) {
                        tso = tsoFromTrans;
                    } else {
                        if (tsoFromGms == null) {
                            tsoFromGms = ColumnarManager.getInstance().latestTso();
                        }
                        tso = tsoFromGms;
                    }
                }
            }

            List<RelNode> inputs = ExecUtils.getInputs(ossTableScan, executionContext, false);
            for (RelNode input : inputs) {
                List<OssSplit> splits = OssSplit.getFileConcurrencySplit(ossTableScan, input, executionContext, tso);

                if (ossTableScan.isColumnarIndex()) {
                    for (OssSplit split : splits) {
                        if (!CollectionUtils.isEmpty(split.getDesignatedFile())) {
                            // ORC files
                            String fileName = split.getDesignatedFile().get(0);
                            if (!isFirstElement) {
                                snapshotInfoBuilder.append(",");
                            }
                            snapshotInfoBuilder.append(fileName);
                            isFirstElement = false;
                        } else {
                            // CSV files
                            Optional<List<String>> csvFiles =
                                split.getDeltaReadOption().getAllCsvFiles().values().stream().findFirst();
                            if (!csvFiles.isPresent() || csvFiles.get().isEmpty()) {
                                continue;
                            }
                            String fileName = csvFiles.get().get(0);
                            if (!isFirstElement) {
                                snapshotInfoBuilder.append(",");
                            }
                            snapshotInfoBuilder.append(fileName);
                            isFirstElement = false;
                        }
                    }
                } else {
                    for (OssSplit split : splits) {
                        List<String> orcFileNames = split.getPrunedOrcFiles(ossTableScan, executionContext);
                        for (String orcFileName : orcFileNames) {
                            if (!isFirstElement) {
                                snapshotInfoBuilder.append(",");
                            }
                            snapshotInfoBuilder.append(orcFileName);
                            isFirstElement = false;
                        }
                    }
                }
                // TODO(siyun): delete bitmap
            }
            result.addRow(new Object[] {ossTableScan.getTableNames().get(0), tso, snapshotInfoBuilder.toString()});
        }

        return result;
    }

    static ResultCursor handleExplain(ExecutionContext executionContext, ExecutionPlan executionPlan,
                                      ExplainResult.ExplainMode mode) {
        SqlExplainLevel explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        if (mode.isCost() || mode.isAnalyze()) {
            // set parameters for precise cost
            PlannerContext.getPlannerContext(executionPlan.getPlan()).setParams(executionContext.getParams());
            explainLevel = SqlExplainLevel.ALL_ATTRIBUTES;
        }
        SqlExplainLevel finalExplainLevel = explainLevel;
        executionContext.getCalcitePlanOptimizerTrace().ifPresent(x -> x.setSqlExplainLevel(finalExplainLevel));

        RuntimeStatistics statistics = (RuntimeStatistics) executionContext.getRuntimeStatistics();
        if (mode.isAnalyze()) {
            executionContext.getExtraCmds()
                .put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.OPERATOR.metricLevel);
            if (statistics == null) {
                statistics = RuntimeStatHelper.buildRuntimeStat(executionContext);
                executionContext.setRuntimeStatistics(statistics);
            }
            statistics.setPlanTree(executionPlan.getPlan());
            Map<RelNode, RuntimeStatisticsSketch> runtimeStatistic;
            ExecutorHelper.selectExecutorMode(
                executionPlan.getPlan(), executionContext, true);
            if (executionContext.getExecuteMode() == ExecutorMode.MPP) {
                if (executionContext.getHintCmds() == null) {
                    executionContext.putAllHintCmds(new HashMap<>());
                }
                executionContext.getHintCmds()
                    .put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.OPERATOR.metricLevel);
                executePlanForExplainAnalyze(executionPlan, executionContext);
                runtimeStatistic = statistics.toMppSketch();
            } else {
                executePlanForExplainAnalyze(executionPlan, executionContext);
                runtimeStatistic = statistics.toSketch();
            }
            executionContext.getCalcitePlanOptimizerTrace().ifPresent(
                x -> x.getOptimizerTracer().setRuntimeStatistics(runtimeStatistic));
        }

        PropUtil.ExplainOutputFormat outputFormat = (PropUtil.ExplainOutputFormat) executionContext.getParamManager()
            .getEnum(ConnectionParams.EXPLAIN_OUTPUT_FORMAT);

        Map<Integer, ParameterContext> parameters = executionContext.getParams().getCurrentParameter();
        Function<RexNode, Object> evalFunc = RexUtils.getEvalFunc(executionContext);

        // 对replace
        // 在executionContext中填入IsAllDnUseXDataSource和SupportsReturningAll
        // 便于后续在explainTermsForDisplay中判断是否通过returning流程
        if (executionPlan.getPlan() instanceof LogicalReplace) {
            LogicalReplace replace = (LogicalReplace) executionPlan.getPlan();
            final ExecutorContext executorContext = ExecutorContext.getContext(replace.getSchemaName());
            final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
            executionContext.setCheckIsAllDnUseXDataSource(isAllDnUseXDataSource(topologyHandler));
            executionContext.setCheckSupportsReturningAll(
                executorContext.getStorageInfoManager().supportsReturningAll());
        }

        // 对relocate
        // 在executionContext中填入IsAllDnUseXDataSource和SupportsReturningAll
        // 便于后续在explainTermsForDisplay中判断是否通过returning流程
        if (executionPlan.getPlan() instanceof LogicalRelocate) {
            LogicalRelocate relocate = (LogicalRelocate) executionPlan.getPlan();
            final ExecutorContext executorContext = ExecutorContext.getContext(relocate.getSchemaName());
            final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
            executionContext.setCheckIsAllDnUseXDataSource(isAllDnUseXDataSource(topologyHandler));
            executionContext.setCheckSupportsReturningAll(
                executorContext.getStorageInfoManager().supportsReturningAll());
        }

        String output;
        if (outputFormat == PropUtil.ExplainOutputFormat.JSON) {
            output = RelUtils.toJsonString(executionPlan.getPlan(), parameters, evalFunc, executionContext);
        } else {
            output = RelUtils.toString(executionPlan.getPlan(), parameters, evalFunc, executionContext);
        }

        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.initMeta();

        for (String row : StringUtils.split(output, "\r\n")) {
            result.addRow(new Object[] {row});
        }

        handleForeignKeySubPlans(executionPlan, executionContext, outputFormat, parameters, evalFunc, result);

        result.addRow(new Object[] {"HitCache:" + executionPlan.isHitCache()});
        result.addRow(new Object[] {"Source:" + executionContext.getPlanSource()});
        if (mode.isCost()) {
            result.addRow(new Object[] {
                "WorkloadType: " +
                    executionContext.getWorkloadType()});
        }
        BaselineInfo baselineInfo = PlannerContext.getPlannerContext(executionPlan.getPlan()).getBaselineInfo();
        PlanInfo planInfo = PlannerContext.getPlannerContext(executionPlan.getPlan()).getPlanInfo();
        if (baselineInfo != null) {
            result.addRow(new Object[] {"BaselineInfo Id: " + baselineInfo.getId()});
            if (baselineInfo.isRebuildAtLoad()) {
                result.addRow(new Object[] {"baseline is rebuildAtLoad :" + baselineInfo.isRebuildAtLoad()});
                result.addRow(new Object[] {"baseline use post planner :" + baselineInfo.isUsePostPlanner()});
                result.addRow(new Object[] {"baseline hint :" + baselineInfo.getHint()});
            } else {
                if (planInfo != null) {
                    result.addRow(new Object[] {"PlanInfo Id: " + planInfo.getId()});
                }
            }
        }

        if (mode.isBaseLine() && baselineInfo != null && planInfo != null) {
            extractBaselineInfo(result, baselineInfo, planInfo);
        }
        AtomicInteger max = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())
                && executionPlan.getAst().getKind() != SqlKind.CREATE_DATABASE
                && executionPlan.getAst().getKind() != SqlKind.DROP_DATABASE
                && executionPlan.getAst().getKind() != SqlKind.CREATE_VIEW
                && executionPlan.getAst().getKind() != SqlKind.DROP_VIEW
                && executionPlan.getAst().getKind() != SqlKind.DROP_MATERIALIZED_VIEW
                && executionPlan.getAst().getKind() != SqlKind.CREATE_MATERIALIZED_VIEW) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        handleSubqueryExplain(executionContext, executionPlan.getPlan(), result, max, current);
        StringBuilder judgement = new StringBuilder();
        if (max.get() - PlannerContext.getPlannerContext(executionPlan.getPlan()).getCacheNodes().size() > 2) {
            judgement.append("extremely slow for multi nested subqueries");
        }

        if (statistics != null) {
            // Make sure to release the reference to operators
            statistics.clear();
        }

        // show cache
        if (PlannerContext.getPlannerContext(executionPlan.getPlan()).getCacheNodes().size() > 0) {
            result.addRow(new Object[] {"cache node:"});

            for (RelNode relNode : PlannerContext.getPlannerContext(executionPlan.getPlan()).getCacheNodes()) {
                result.addRow(new Object[] {relNode.getDigest()});
            }
        }

        if (judgement.length() > 0) {
            result.addRow(new Object[] {judgement});
        }

        //show sql template id
        String sqlTid = "NULL";
        PlanCache.CacheKey cacheKey = executionContext.getFinalPlan().getCacheKey();
        if (cacheKey != null) {
            sqlTid = cacheKey.getTemplateId();
        }
        result.addRow(new Object[] {"TemplateId: " + sqlTid});

        // show statistic trace
        if (mode == ExplainResult.ExplainMode.COST_TRACE &&
            PlannerContext.getPlannerContext(executionPlan.getPlan()).isNeedStatisticTrace()) {
            result.addRow(
                new Object[] {
                    PlannerContext.getPlannerContext(executionPlan.getPlan()).formatAndClearStatisticTrace()});
        }
        return result;
    }

    /**
     * Extracts baseline information and adds it to the result set.
     *
     * @param result The result set object
     * @param baselineInfo The baseline info object
     * @param planInfo The plan info object
     */
    protected static void extractBaselineInfo(ArrayResultCursor result, BaselineInfo baselineInfo, PlanInfo planInfo) {
        // Add baseline info ID
        result.addRow(new Object[] {"BaselineInfo Id: " + baselineInfo.getId()});

        // Add table hash code
        result.addRow(new Object[] {"BaselineInfo TablesHashCode: " + planInfo.getTablesHashCode()});

        // Add table set information
        result.addRow(new Object[] {"BaselineInfo TableSet: " + baselineInfo.getTableSet()});

        // If accepted plans list is not empty, add relevant information
        if (!baselineInfo.getAcceptedPlans().isEmpty()) {
            result.addRow(new Object[] {
                "BaselineInfo acceptedPlan: " +
                    baselineInfo.getAcceptedPlans().values().stream()
                        .map(x -> String.valueOf(x.getId()))
                        .collect(Collectors.joining(","))
            });
        }

        // If unaccepted plans list is not empty, add relevant information
        if (!baselineInfo.getUnacceptedPlans().isEmpty()) {
            result.addRow(new Object[] {
                "BaselineInfo unacceptedPlan: " +
                    baselineInfo.getUnacceptedPlans().values().stream()
                        .map(x -> String.valueOf(x.getId()))
                        .collect(Collectors.joining(","))
            });
        }

        // Add fixed status information
        result.addRow(new Object[] {"PlanInfo fixed: " + planInfo.isFixed()});

        // Add whether accepted information
        result.addRow(new Object[] {"PlanInfo accepted: " + planInfo.isAccepted()});

        // Add fix hint information
        result.addRow(new Object[] {"PlanInfo fix hint: " + planInfo.getFixHint()});

        // Add fix arguments information
        result.addRow(new Object[] {"PlanInfo fix args: " + planInfo.getHintArgs().toString()});

        // Add fix expression information
        result.addRow(new Object[] {"PlanInfo fix expr: " + planInfo.getExpr()});

        // Add estimated execution time information
        result.addRow(
            new Object[] {String.format("PlanInfo estimateExecutionTime: %.3fs", planInfo.getEstimateExecutionTime())});

        // Add last execute time information
        long lastExecuteTime = planInfo.getLastExecuteTime() == null ? -1 : planInfo.getLastExecuteTime();
        result.addRow(new Object[] {"PlanInfo lastExecuteTime: " + new Timestamp(lastExecuteTime * 1000)});

        // Add create time information
        result.addRow(new Object[] {"PlanInfo createTime: " + new Timestamp(planInfo.getCreateTime() * 1000)});

        // Add choose count information
        result.addRow(new Object[] {"PlanInfo chooseCount: " + planInfo.getChooseCount()});

        // Add trace ID information
        result.addRow(new Object[] {"PlanInfo traceId: " + planInfo.getTraceId()});

        // Add origin information
        result.addRow(new Object[] {"PlanInfo origin: " + planInfo.getOrigin()});

        // Add version information
        result.addRow(new Object[] {"PlanInfo version: " + planInfo.getVersion()});
    }

    private static void executePlanForExplainAnalyze(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        /*
            Explain analyze 复用同一个executionContext, 参数可能会被污染
            已知情况: limit的物理sql cache需要保存计算好的limit到Params中
         */
        Parameters parameters = executionContext.cloneParamsOrNull();
        ResultCursor cursor = PlanExecutor.execByExecPlanNodeByOne(executionPlan, executionContext);
        executionContext.setParams(parameters);
        ArrayList<Throwable> exceptions = new ArrayList<>();
        try {
            Row result;
            while ((result = cursor.next()) != null) {
                // do nothing
            }
        } finally {
            cursor.close(exceptions);
        }

        if (!exceptions.isEmpty()) {
            throw GeneralUtil.nestedException(exceptions.get(0));
        }
    }

    /**
     * 展示MPP的物理执行计划
     */
    static ResultCursor handleExplainMppPhysicalPlan(ExecutionContext executionContext,
                                                     ExecutionPlan executionPlan,
                                                     boolean withSchedule) {
        ArrayResultCursor result = null;
        try {
            result = new ArrayResultCursor("PhysicalPlan");
            result.addColumn("Plan", DataTypes.StringType);
            result.initMeta();
            Session session = new Session(executionContext.getTraceId(), executionContext);
            String mppPlanString = PlanUtils.textPlan(executionContext, session, executionPlan.getPlan(), withSchedule);
            for (String row : StringUtils.split(mppPlanString, "\r\n")) {
                result.addRow(new Object[] {row});
            }
        } finally {
            executionContext.getMemoryPool().destroy();
        }

        return result;
    }

    static ResultCursor handleExplainLocalPhysicalPlan(ExecutionContext executionContext,
                                                       ExecutionPlan executionPlan,
                                                       ExecutorMode type) {
        ArrayResultCursor result = new ArrayResultCursor("PhysicalPlan");
        result.addColumn("Plan", DataTypes.StringType);
        result.initMeta();
        String mppPlanString = PlanUtils.textLocalPlan(executionContext, executionPlan.getPlan(), type);
        for (String row : StringUtils.split(mppPlanString, "\r\n")) {
            result.addRow(new Object[] {row});
        }
        return result;
    }

    private static ResultCursor handleExplainAnalyzeExecute(ExecutionPlan executionPlan,
                                                            ExecutionContext executionContext) {
        if (executionPlan.getAst().getKind() != SqlKind.SELECT) {
            throw new NotSupportException("explain analyze execute only support select ");
        }

        List<LogicalView> views = Lists.newArrayList();
        RelShuttle logicalViewGetter = new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                if (scan instanceof LogicalView) {
                    views.add((LogicalView) scan);
                }
                return scan;
            }
        };

        executionPlan.getPlan().accept(logicalViewGetter);
        boolean metaInit = false;
        ArrayResultCursor result = new ArrayResultCursor("PhysicalPlan");
        if (executionPlan.getPlan() instanceof BaseTableOperation) {
            ResultCursor rc = PlanExecutor.execByExecPlanNodeByOne(executionPlan, executionContext);
            try {
                rc.setCursorMeta(result.getMeta());
                Row row = rc.next();
                if (!metaInit) {
                    initOriginMeta(rc, row, result);
                    metaInit = true;
                }
                while (row != null) {
                    result.addRow(row.getValues().toArray());
                    row = rc.next();
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                rc.close(Lists.newArrayList());
            }
        }

        for (LogicalView lv : views) {
            ExecutionPlan lp = new ExecutionPlan(executionPlan.getAst(), lv, null);
            ResultCursor rc = PlanExecutor.execByExecPlanNodeByOne(lp, executionContext);

            try {
                rc.setCursorMeta(result.getMeta());
                Row row = rc.next();
                if (!metaInit) {
                    initOriginMeta(rc, row, result);
                    metaInit = true;
                }
                while (row != null) {
                    result.addRow(row.getValues().toArray());
                    row = rc.next();
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                rc.close(Lists.newArrayList());
            }
        }
        return result;

    }

    private static ResultCursor handleExplainExecute(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        if (executionPlan.getAst() instanceof SqlInsert || executionPlan.getAst() instanceof SqlCreateTable) {
            throw new NotSupportException("explain execute insert or ddl ");
        }

        List<LogicalView> views = Lists.newArrayList();
        RelShuttle logicalViewGetter = new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                if (scan instanceof LogicalView) {
                    views.add((LogicalView) scan);
                }
                return scan;
            }
        };

        executionPlan.getPlan().accept(logicalViewGetter);

        boolean cursorMode = ExecutorMode.CURSOR.equals(
            ExecutorHelper.getExecutorMode(executionPlan.getPlan(), executionContext, false));
        // build meta info for physical plan
        boolean metaInit = false;
        ArrayResultCursor result = new ArrayResultCursor("PhysicalPlan");
        if (executionPlan.getPlan() instanceof BaseTableOperation) {
            ResultCursor rc = PlanExecutor.execByExecPlanNodeByOne(executionPlan, executionContext);
            try {
                boolean xplanBuilt = false;
                rc.setCursorMeta(result.getMeta());
                Row row = rc.next();
                if (!StringUtils.isEmpty(XplanStat.getXplanIndex(executionContext.getXplanStat()))) {
                    if (explainExecuteXPlan(
                        ((BaseTableOperation) executionPlan.getPlan()).getOriginPlan(),
                        metaInit,
                        result, executionContext)) {
                        metaInit = true;
                        xplanBuilt = true;
                        while (rc.next() != null) {
                            // consume all result
                        }
                    }
                }
                if (!xplanBuilt) {
                    initOriginMeta(rc, row, result);
                    metaInit = true;
                    while (row != null) {
                        result.addRow(row.getValues().toArray());
                        row = rc.next();
                    }
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                rc.close(Lists.newArrayList());
            }
        }

        for (LogicalView lv : views) {
            ExecutionPlan lp = new ExecutionPlan(executionPlan.getAst(), lv, null);
            ResultCursor rc = PlanExecutor.execByExecPlanNodeByOne(lp, executionContext);

            try {
                rc.setCursorMeta(result.getMeta());
                Row row = rc.next();
                boolean xplanBuilt = false;
                if (cursorMode && !StringUtils.isEmpty(XplanStat.getXplanIndex(executionContext.getXplanStat()))) {
                    if (explainExecuteXPlan(lv.getPushedRelNode(), metaInit,
                        result, executionContext)) {
                        metaInit = true;
                        xplanBuilt = true;
                        while (rc.next() != null) {
                            // consume all result
                        }
                    }
                }
                if (!xplanBuilt) {
                    if (!metaInit) {
                        initOriginMeta(rc, row, result);
                        metaInit = true;
                    }
                    while (row != null) {
                        result.addRow(row.getValues().toArray());
                        row = rc.next();
                    }
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                rc.close(Lists.newArrayList());
            }
        }
        return result;
    }

    /**
     * used in RelUtils#displayPhysicalPlan
     */
    private static String getExplainExecuteResultForDisplay(RelNode relNode, ExecutionContext executionContext) {
        if (relNode == null || executionContext == null) {
            return null;
        }
        ExecutionPlan executionPlan = null;
        if (relNode instanceof LogicalView) {
            executionPlan = new ExecutionPlan(((LogicalView) relNode).getNativeSqlNode(), relNode, null);
        } else if (relNode instanceof BaseTableOperation) {
            SqlNode ast = ((BaseTableOperation) relNode).getNativeSqlNode();
            if (ast == null || !ast.isA(SqlKind.QUERY)) {
                return null;
            }
            executionPlan = new ExecutionPlan(((BaseTableOperation) relNode).getNativeSqlNode(), relNode, null);
        }

        if (executionPlan == null || executionPlan.getAst() == null) {
            return null;
        }

        boolean cursorMode = ExecutorMode.CURSOR.equals(
            ExecutorHelper.getExecutorMode(executionPlan.getPlan(), executionContext, false));
        if (!cursorMode) {
            throw new NotSupportException("explain execute must be cursor mode");
        }

        boolean originalMergeUnionValue = executionContext.getParamManager().getBoolean(ConnectionParams.MERGE_UNION);
        executionContext.getParamManager().getProps().put(ConnectionProperties.MERGE_UNION, "false");
        try {
            ResultCursor rc = handleExplainExecute(executionPlan, executionContext);
            CursorMeta cursorMeta = rc.getCursorMeta();

            int selectTypeIndex = -1;
            int tableIndex = -1;
            int typeIndex = -1;
            //int possibleKeyIndex = -1;
            int keyIndex = -1;
            int rowIndex = -1;
            int filteredIndex = -1;
            int extraIndex = -1;
            for (int i = 0; i < cursorMeta.getColumns().size(); i++) {

                switch (cursorMeta.getColumns().get(i).getName().toLowerCase()) {
                case "select_type":
                    selectTypeIndex = i;
                    break;
                case "table":
                    tableIndex = i;
                    break;
                case "type":
                    typeIndex = i;
                    break;
                case "key":
                    keyIndex = i;
                    break;
                case "rows":
                    rowIndex = i;
                    break;
                case "filtered":
                    filteredIndex = i;
                    break;
                case "extra":
                    extraIndex = i;
                    break;
                }
            }

            Row explainExecuteRow = rc.next();
            StringJoiner physicalPlanSj = new StringJoiner(",");
            while (explainExecuteRow != null) {
                Object[] explainExecuteRowArray = explainExecuteRow.getValues().toArray();
                StringJoiner stringJoiner = new StringJoiner(",");
                stringJoiner.add("table:" + (tableIndex < 0 ? null : explainExecuteRowArray[tableIndex]));
                stringJoiner.add(
                    "selectType:" + (selectTypeIndex < 0 ? null : explainExecuteRowArray[selectTypeIndex]));
                stringJoiner.add("type:" + (typeIndex < 0 ? null : explainExecuteRowArray[typeIndex]));
                //stringJoiner.add("possibleKey:" + (possibleKeyIndex < 0 ? null : explainExecuteRowArray[possibleKeyIndex]));
                stringJoiner.add("key:" + (keyIndex < 0 ? null : explainExecuteRowArray[keyIndex]));
                stringJoiner.add("rows:" + (rowIndex < 0 ? null : explainExecuteRowArray[rowIndex]));
                stringJoiner.add("filtered:" + (filteredIndex < 0 ? null : explainExecuteRowArray[filteredIndex]));
                if (extraIndex >= 0 && explainExecuteRowArray[extraIndex] != null
                    && explainExecuteRowArray[extraIndex].toString().length() > 0) {
                    stringJoiner.add("extra:" + explainExecuteRowArray[extraIndex]);
                }
                physicalPlanSj.add("{" + stringJoiner.toString() + "}");
                explainExecuteRow = rc.next();
            }

            return "[" + physicalPlanSj.toString() + "]";
        } finally {
            executionContext.getParamManager().getProps()
                .put(ConnectionProperties.MERGE_UNION, String.valueOf(originalMergeUnionValue));
        }

    }

    /**
     * mock the result of explain of mysql
     *
     * @param plan the plan to be optimized
     * @param metaInit whether the meta is initied
     * @param result the result cursor
     */
    private static boolean explainExecuteXPlan(RelNode plan, boolean metaInit,
                                               ArrayResultCursor result, ExecutionContext executionContext) {
        if (plan == null) {
            return false;
        }
        SqlConverter sqlConverter = SqlConverter.getInstance(
            PlannerContext.getPlannerContext(plan).getSchemaName(), executionContext);
        RelOptCluster cluster = sqlConverter.createRelOptCluster();
        RelOptSchema relOptSchema = sqlConverter.getCatalog();
        String serialPlan = PlanManagerUtil.relNodeToJson(plan);
        plan = PlanManagerUtil.jsonToRelNode(serialPlan, cluster, relOptSchema);
        PlannerContext.getPlannerContext(plan).setParams(executionContext.getParams());
        RelXPlanOptimizer.XplanExplainExecuteVisitor indexFinder =
            new RelXPlanOptimizer.XplanExplainExecuteVisitor(executionContext);
        indexFinder.go(RelXPlanOptimizer.optimizeFilter(plan));
        XPlanCalcRule.IndexInfo indexInfo = indexFinder.getIndexInfo();
        if (!indexInfo.isFound()) {
            return false;
        }
        // build meta
        if (!metaInit) {
            String tableName = "explain";
            String[] columns = new String[] {
                "id",
                "select_type",
                "table",
                "partitions",
                "type",
                "possible_keys",
                "key",
                "key_len",
                "ref",
                "rows",
                "filtered",
                "Extra"};
            TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
            for (String columnName : columns) {
                RelDataType dataType = factory.createSqlType(SqlTypeName.VARCHAR);
                Field col = new Field(tableName, columnName, dataType);
                ColumnMeta iColumnMeta = new ColumnMeta(tableName, columnName, null, col);
                result.addColumn(iColumnMeta);
            }
            result.initMeta();
        }
        // add result
        String index = indexInfo.getIndex();
        if (StringUtils.isEmpty(index)) {
            index = "Primary";
        }
        String type = "Primary".equalsIgnoreCase(index) ? "const" : "ref";

        double filtered = indexInfo.getFinalRowCount() / indexInfo.getRowCount() * 100;
        result.addRow(new Object[] {
            1,
            "SIMPLE",
            indexInfo.getTableName(),
            null,
            type,
            String.join(",", indexInfo.getCandidateIndexes()),
            index,
            8,
            null,
            indexInfo.getRowCount(),
            String.format("%.2f", Math.min(filtered, 100D)),
            "Using XPlan" + (indexInfo.isUsingWhere() ? ", Using where" : ""),
        });
        plan.getCluster().invalidateMetadataQuery();
        return true;
    }

    private static void initOriginMeta(ResultCursor rc, Row row, ArrayResultCursor result)
        throws SQLException, UnsupportedEncodingException {
        List<ColumnMeta> metas = Lists.newArrayList();
        String tableName = "explain";
        for (int i = 1; i <= row.getColNum(); i++) {
            final String columnName;
            if (row instanceof XRowSet) {
                final XResult xResult = ((XRowSet) row).getResult();
                columnName = xResult.getMetaData().get(i - 1).getName().toString(XSession
                    .toJavaEncoding(xResult.getSession().getResultMetaEncodingMySQL()));
            } else if (row instanceof ResultSetRow) {
                columnName = ((ResultSetRow) row).getOriginMeta().getColumnName(i);
            } else {
                columnName = row.getParentCursorMeta().getColumnMeta(i - 1).getName();
            }
            TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
            RelDataType dataType = factory.createSqlType(SqlTypeName.VARCHAR);
            Field col = new Field(tableName, columnName, dataType);
            ColumnMeta iColumnMeta = new ColumnMeta(tableName, columnName, null, col);
            metas.add(iColumnMeta);
            result.addColumn(iColumnMeta);
        }
        CursorMeta cursorMeta = CursorMeta.build(metas);
        row.setCursorMeta(cursorMeta);
        result.initMeta();
        rc.setCursorMeta(cursorMeta);
    }

    public static List<ColumnMeta> getRowMetas(Row row, String tableName)
        throws SQLException, UnsupportedEncodingException {
        List<ColumnMeta> metas = Lists.newArrayList();
        for (int i = 1; i <= row.getColNum(); i++) {
            final String columnName;
            if (row instanceof XRowSet) {
                final XResult xResult = ((XRowSet) row).getResult();
                columnName = xResult.getMetaData().get(i - 1).getName().toString(XSession
                    .toJavaEncoding(xResult.getSession().getResultMetaEncodingMySQL()));
            } else if (row instanceof ResultSetRow) {
                columnName = ((ResultSetRow) row).getOriginMeta().getColumnName(i);
            } else {
                columnName = row.getParentCursorMeta().getColumnMeta(i - 1).getName();
            }
            TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
            RelDataType dataType = factory.createSqlType(SqlTypeName.VARCHAR);
            Field col = new Field(tableName, columnName, dataType);
            ColumnMeta iColumnMeta = new ColumnMeta(tableName, columnName, null, col);
            metas.add(iColumnMeta);
        }
        return metas;
    }

    private static void handleForeignKeySubPlans(ExecutionPlan executionPlan, ExecutionContext executionContext,
                                                 PropUtil.ExplainOutputFormat outputFormat,
                                                 Map<Integer, ParameterContext> parameters,
                                                 Function<RexNode, Object> evalFunc, ArrayResultCursor result) {
        // Foreign Key Cascade Plan
        Map<Integer, List<Pair<String, String>>> fkOutputs = new TreeMap<>();
        if (executionPlan.getPlan() instanceof LogicalRelocate || executionPlan.getPlan() instanceof LogicalModify) {
            TableModify modify = (TableModify) executionPlan.getPlan();
            if (!modify.getFkPlans().isEmpty()) {
                modify.getFkPlans().forEach((schema, secondLayerMap) ->
                    secondLayerMap.forEach((table, thirdLayerMap) ->
                        thirdLayerMap.forEach((constraint, value) -> {
                                String fkOutput;
                                if (outputFormat == PropUtil.ExplainOutputFormat.JSON) {
                                    fkOutput = RelUtils.toJsonString(value.right, parameters, evalFunc, executionContext);
                                } else {
                                    fkOutput = RelUtils.toString(value.right, parameters, evalFunc, executionContext);
                                }
                                List<Pair<String, String>> fk =
                                    fkOutputs.computeIfAbsent(value.left, x -> new ArrayList<>());
                                fk.add(new Pair<>(schema + "." + table + "." + constraint, fkOutput));
                            }
                        )
                    )
                );
            }
        }
        for (Map.Entry<Integer, List<Pair<String, String>>> entry : fkOutputs.entrySet()) {
            int depth = entry.getKey();
            String chevrons = IntStream.range(0, depth).mapToObj(i -> ">>").collect(Collectors.joining());
            String spaces = IntStream.range(0, depth).mapToObj(i -> "  ").collect(Collectors.joining());
            for (Pair<String, String> fkOutput : entry.getValue()) {
                result.addRow(new Object[] {chevrons + " Foreign Key: " + fkOutput.left});
                for (String row : StringUtils.split(fkOutput.right, "\r\n")) {
                    result.addRow(new Object[] {spaces + row});
                }
            }
        }
    }

    private static String buildProgressBar(int pct, int width) {
        int filled = Math.max(0, Math.min(width, pct * width / 100));
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < width; i++) {
            sb.append(i < filled ? '█' : '░');
        }
        sb.append("]");
        return sb.toString();
    }
}

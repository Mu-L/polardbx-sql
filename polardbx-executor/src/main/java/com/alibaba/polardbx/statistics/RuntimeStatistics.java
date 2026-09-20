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

package com.alibaba.polardbx.statistics;

import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.CpuCollector;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.FirstThenOtherCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.cursor.impl.LogicalViewResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.MyPhysicalCursor;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.operator.AbstractExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStatItem;
import com.alibaba.polardbx.optimizer.core.profiler.memory.MemoryEstimation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.statis.MemoryStatisticsGroup;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.optimizer.statis.TaskMemoryStatisticsGroup;
import com.alibaba.polardbx.optimizer.utils.SimplePlanVisitor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.apache.calcite.util.trace.RuntimeStatisticsSketchExt;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime Statistics of stat all cpu time & mem for a sql
 *
 * @author chenghui.lch
 */
public class RuntimeStatistics extends RuntimeStat implements CpuCollector {

    private static final Logger logger = LoggerFactory.getLogger(RuntimeStatistics.class);

    public static final long NOT_SUPPORT_VALUE = -1;

    private Metrics storedMetrics;

    private RelNode planTree;
    private final String traceId;
    private final Map<Integer, OperatorStatisticsGroup> relationToStatistics =
        new ConcurrentHashMap<>();
    private final Map<String, MemoryStatisticsGroup> memoryToStatistics = new ConcurrentHashMap<>();
    private final Map<Integer, RelNode> relationIdToNode = new HashMap<>();
    private final WeakHashMap<Integer, RuntimeStatisticsSketch> mppOperatorStats =
        new WeakHashMap<>();
    private QuerySpillSpaceMonitor querySpillSpaceMonitor;
    private MemoryPool holdMemoryPool;
    private CpuStat sqlWholeStageCpuStat;
    private MemoryEstimation sqlWholeStageMemEstimation;
    private boolean finishExecution = false;
    private SqlType sqlType = null;
    private String schemaName;

    // Metrics for sql.log
    private AtomicLong sqlLogCpuTime = new AtomicLong(0L);
    private AtomicLong totalPhySqlCount = new AtomicLong(0L);
    private AtomicLong totalPhyAffectedRows = new AtomicLong(0L);
    private AtomicLong totalPhyFetchRows = new AtomicLong(0L);
    private AtomicLong totalPhySqlTimecost = new AtomicLong(0L);
    private AtomicLong totalPhyConnTimecost = new AtomicLong(0L);
    private AtomicLong totalFetchTSOTimecost = new AtomicLong(0L);
    private AtomicLong totalFetchSequenceTimecost = new AtomicLong(0L);
    private String trxType = null;
    private AtomicLong commitPrepareTimecost = new AtomicLong(0L);
    private AtomicLong commitTsoTimecost = new AtomicLong(0L);
    private AtomicLong commitLoggerTimecost = new AtomicLong(0L);
    private AtomicLong commitCommitTimecost = new AtomicLong(0L);
    private AtomicLong columnarSnapshotTimecost = new AtomicLong(0L);
    private AtomicLong spillCnt = new AtomicLong(0L);

    // Per-SQL external column latency breakdown (set from ExecutionContext before toMetrics)
    private volatile com.alibaba.polardbx.common.columnar.ExternalColumnStatistics extColStats;

    /**
     * When the whole plan is a logicalView only, it is call simpleLvPlan
     */
    private boolean isSimpleLvPlan = false;

    /**
     * flag that if the sql is running with using cpu profile
     */
    private boolean runningWithCpuProfile = false;

    /**
     * Flag if the plan is from PostPlanner.allAtOneTable where plan will be
     * convert to one PhyOperation
     */
    private boolean isFromAllAtOnePhyTable = false;

    private Set<TaskId> taskTraceSet = new HashSet<>();

    /**
     * Query-Level memory usage info for MPP mode.
     */
    private Map<String, Long> maxMemoryUsageForEachNode = new HashMap<>();

    /**
     * Records the number of external network communications and their durations for the version storage.
     */
    private Map<TaskId, VersionStorageStatistics> versionStorageStatisticsMap = new HashMap<>();

    private Map<TaskId, ColumnarScanMetrics> columnarScanMetricsMap = new HashMap<>();

    public RuntimeStatistics(String traceId, ExecutionContext executionContext) {
        this.traceId = traceId;
        if (executionContext != null) {
            this.schemaName = executionContext.getSchemaName();
            this.holdMemoryPool = executionContext.getMemoryPool();
            this.querySpillSpaceMonitor = executionContext.getQuerySpillSpaceMonitor();
        }
        this.sqlWholeStageCpuStat = new CpuStat();
        this.sqlWholeStageMemEstimation = new MemoryEstimation();
    }

    /**
     * register the runtime stats of executor to the relation
     */
    public void register(RelNode relation, Executor executor) {

        try {
            OperatorStatisticsGroup sg = relationToStatistics.get(relation.getRelatedId());
            if (sg == null) {
                sg = new OperatorStatisticsGroup(this);
                sg.targetRel = relation;
                if (!relationIdToNode.containsKey(relation.getRelatedId())) {
                    relationIdToNode.put(relation.getRelatedId(), relation);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "register:" + relation.getRelatedId() + ":" + relation.getClass()
                            .getSimpleName());
                }
                relationToStatistics.put(relation.getRelatedId(), sg);
            }

            boolean isLv = relation instanceof LogicalView;
            sg.add(((AbstractExecutor) executor).getStatistics());
            ((AbstractExecutor) executor).setTargetPlanStatGroup(sg);
            if (isLv) {
                sg.totalCount.addAndGet(1);
            }
        } catch (Throwable e) {
            logger.warn(
                "register cpu stat of executor failed for " + relation.getRelTypeName(), e);
        }
    }

    /**
     * register the stats of cursor to the relation
     */
    public void register(RelNode relation, Cursor cursor) {

        try {
            OperatorStatisticsGroup sg = null;
            if (relation instanceof BaseTableOperation) {
                RelNode parentOfBaseTableOperation = ((BaseTableOperation) relation).getParent();
                if (isFromAllAtOnePhyTable) {
                    sg = relationToStatistics.get(relation.getRelatedId());
                    assert sg != null;
                    registerCursorToGroup(cursor, sg);
                } else {
                    register(parentOfBaseTableOperation, cursor);
                }
            } else {
                sg = relationToStatistics.get(relation.getRelatedId());
                if (sg == null) {
                    sg = new OperatorStatisticsGroup(this);
                    sg.targetRel = relation;
                    //FIXME mpp也会走到这个流程里，BKA的gather
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            this + " register3:" + relation.getRelatedId() + ":"
                                + relation.getClass().getSimpleName()
                                + "," + cursor.getClass().getSimpleName());
                    }
                    if (!relationIdToNode.containsKey(relation.getRelatedId())) {
                        relationIdToNode.put(relation.getRelatedId(), relation);
                    }
                    relationToStatistics.put(relation.getRelatedId(), sg);
                }
                registerCursorToGroup(cursor, sg);
            }
        } catch (Throwable e) {
            logger.warn(
                "register cpu stat of cursor failed for " + relation.getRelTypeName(), e);
        }
    }

    public static void registerAsyncTaskTimeCost(Cursor targetCursor, long asyncTaskTimeCostNano) {
        if (asyncTaskTimeCostNano > 0) {
            OperatorStatisticsGroup sg = ((AbstractCursor) targetCursor).getTargetPlanStatGroup();
            if (sg != null) {
                sg.selfAsyncTaskTimeCost.addAndGet(asyncTaskTimeCostNano);
            }
        }
    }

    /**
     * <pre>
     *
     * register the async cpu time key :
     * relation val :
     * the async task cpu time
     * e.g
     *  Join
     *      Union
     *          LV1
     *          LV2
     *      LV3(no-async)
     * if lv1 & lv2 are executing by multi async tasks
     * that consume the time cost C1 & C2, then the async task cpu time cost of LV1 is C1;
     * the async task cpu time cost of LV2 is C2;
     * the async task cpu time cost of Union is C1 + C2,
     * because C1 & C2 are also contributed to Union;
     * If the async task cpu time cost of LV3 is 0,
     * then the async task cpu time cost of Join is C1 + C2,
     * because C1 & C2 are also contributed to Join;
     *
     * </pre>
     */
    public static void registerAsyncTaskCpuTime(Cursor targetCursor, long asyncTaskTimeNano) {

        if (asyncTaskTimeNano > 0) {
            OperatorStatisticsGroup sg = ((AbstractCursor) targetCursor).getTargetPlanStatGroup();
            if (sg != null) {
                sg.selfAsyncTaskCpuTime.addAndGet(asyncTaskTimeNano);
            }
        }
    }

    public static void registerAsyncTaskCpuTime(Executor targetExec, long asyncTaskTimeNano) {
        if (asyncTaskTimeNano > 0) {
            OperatorStatisticsGroup sg = ((AbstractExecutor) targetExec).getTargetPlanStatGroup();
            if (sg != null) {
                sg.selfAsyncTaskCpuTime.addAndGet(asyncTaskTimeNano);
            }
        }

    }

    public static void addSelfAsyncTaskCpuTimeToParent(OperatorStatisticsGroup sg) {
        if (sg == null) {
            return;
        }
        long allAsyncCpuTime = sg.selfAsyncTaskCpuTime.get();
        if (allAsyncCpuTime == 0) {
            return;
        }
        RuntimeStatistics runtimeStat = sg.runtimeStat;
        assert runtimeStat != null;
        OperatorStatisticsGroup tmSg = sg;
        RelNode parent = tmSg.parentRel;
        // avoid dead loop
        if (parent == tmSg.targetRel) {
            return;
        }
        while (parent != null) {
            tmSg = runtimeStat.relationToStatistics.get(parent.getRelatedId());
            if (tmSg != null) {
                tmSg.childrenAsyncTaskCpuTime.addAndGet(allAsyncCpuTime);
                parent = tmSg.parentRel;
            } else {
                break;
            }
        }
    }

    public static void registerWaitLockCpuTime(OperatorStatisticsGroup targetPlanStatGroup,
                                               long waitLockDuration) {
        if (targetPlanStatGroup != null) {
            targetPlanStatGroup.waitLockDuration.addAndGet(waitLockDuration);
        }
    }

    protected void registerCursorToGroup(Cursor cursor, OperatorStatisticsGroup sg) {
        /**
         * when the cursor is AdaptiveParallelCursor, it is no need to
         * register its runtime stats to its relation, because its input
         * cursors will be registered during their init process
         */
        initTargetPlanStatGroup(sg, cursor);
    }

    private void initTargetPlanStatGroup(OperatorStatisticsGroup sg, Cursor c) {
        ((AbstractCursor) c).setTargetPlanStatGroup(sg);
        if (c instanceof MyPhysicalCursor || c instanceof GroupConcurrentUnionCursor
            || c instanceof FirstThenOtherCursor) {
            return;
        }
        sg.add(((AbstractCursor) c).getStatistics());
        if (sg.targetRel instanceof LogicalView) {
            sg.totalCount.addAndGet(1);
        } else if (sg.targetRel instanceof PhyTableOperation && isFromAllAtOnePhyTable) {
            sg.totalCount.addAndGet(1);
        }
        if (c instanceof LogicalViewResultCursor) {
            registerCursorToGroup(((LogicalViewResultCursor) c).getCursor(), sg);
        }
        return;
    }

    @Override
    public RelNode getPlanTree() {
        return planTree;
    }

    @Override
    public void setPlanTree(RelNode planTree) {
        this.planTree = planTree;
        initOperatorStats();
    }

    protected void initOperatorStats() {
        SimplePlanVisitor simplePlanVisitor = new SimplePlanVisitor();
        simplePlanVisitor.visit(this.planTree);
        this.isSimpleLvPlan =
            simplePlanVisitor.isSamplePlan() && !(this.planTree instanceof Gather);
        if (this.planTree instanceof PhyTableOperation
            || this.planTree instanceof SingleTableOperation) {
            isFromAllAtOnePhyTable = true;
            this.isSimpleLvPlan = true;
        }

        Stack<RelNode> relNodeStack = new Stack<>();
        relNodeStack.push(planTree);
        while (!relNodeStack.isEmpty()) {
            RelNode node = relNodeStack.pop();
            relationIdToNode.put(node.getRelatedId(), node);
            for (RelNode child : node.getInputs()) {
                relNodeStack.push(child);
            }
        }

        buildStatisticsGroup(null, this.planTree);
    }

    protected void buildStatisticsGroup(RelNode parent, RelNode targetRel) {

        boolean isLvOrLm = false;
        if (targetRel instanceof BaseTableOperation && !isFromAllAtOnePhyTable) {
            targetRel = ((BaseTableOperation) targetRel).getParent();
        }
        if (targetRel instanceof LogicalView || targetRel instanceof LogicalInsert) {
            isLvOrLm = true;
        }
        OperatorStatisticsGroup osg = new OperatorStatisticsGroup(this);
        osg.parentRel = parent;
        osg.targetRel = targetRel;
        if (!relationIdToNode.containsKey(targetRel.getRelatedId())) {
            relationIdToNode.put(targetRel.getRelatedId(), targetRel);
        }
        relationToStatistics.put(targetRel.getRelatedId(), osg);
        if (logger.isDebugEnabled()) {
            logger.debug("register2:" + targetRel.getRelatedId() + ":" + targetRel.getClass()
                .getSimpleName());
        }
        if (!isLvOrLm && !isFromAllAtOnePhyTable) {
            List<RelNode> inputRelList = targetRel.getInputs();
            for (int i = 0; i < inputRelList.size(); i++) {
                RelNode input = inputRelList.get(i);
                buildStatisticsGroup(targetRel, input);
            }
        }
    }

    public void clear() {
        relationToStatistics.clear();
        planTree = null;
        totalPhySqlCount.set(0);
        totalPhyAffectedRows.set(0);
        totalPhyFetchRows.set(0);
        totalPhySqlTimecost.set(0);
        sqlLogCpuTime.set(0);
        totalPhyConnTimecost.set(0);
        totalFetchTSOTimecost.set(0);
        totalFetchSequenceTimecost.set(0);
        trxType = null;
        commitPrepareTimecost.set(0);
        commitTsoTimecost.set(0);
        commitLoggerTimecost.set(0);
        commitCommitTimecost.set(0);
        columnarSnapshotTimecost.set(0);
    }

    public Map<Integer, OperatorStatisticsGroup> getRelationToStatistics() {
        return relationToStatistics;
    }

    public Map<String, MemoryStatisticsGroup> getMemoryToStatistics() {
        return memoryToStatistics;
    }

    public Map<RelNode, RuntimeStatisticsSketch> toSketch() {
        Map<RelNode, RuntimeStatisticsSketch> results = new IdentityHashMap<>();
        for (Map.Entry<Integer, OperatorStatisticsGroup> entry : relationToStatistics.entrySet()) {
            final OperatorStatisticsGroup stats = entry.getValue();
            RelNode node = relationIdToNode.get(entry.getKey());
            if (node != null) {
                results.put(node, stats.toSketch());
            }
        }
        return results;
    }

    public Map<RelNode, RuntimeStatisticsSketch> toMppSketch() {
        final Map<RelNode, RuntimeStatisticsSketch> results = new IdentityHashMap<>();
        if (mppOperatorStats != null && mppOperatorStats.size() > 0) {
            for (Map.Entry<Integer, RuntimeStatisticsSketch> operatorStats : mppOperatorStats
                .entrySet()) {
                if (relationIdToNode.containsKey(operatorStats.getKey())) {
                    results.put(relationIdToNode.get(operatorStats.getKey()),
                        operatorStats.getValue());
                }
            }
        }
        return results;
    }

    public synchronized void collectMppStatistics(TaskStatus taskStatus, ExecutionContext context) {
        if (!taskTraceSet.contains(taskStatus.getTaskId())) {
            taskTraceSet.add(taskStatus.getTaskId());
            Map<Integer, RuntimeStatistics.OperatorStatisticsGroup> taskRuntimeStats =
                taskStatus.getRuntimeStatistics();
            if (taskRuntimeStats != null) {
                for (Map.Entry<Integer, RuntimeStatistics.OperatorStatisticsGroup>
                    entry : taskRuntimeStats.entrySet()) {
                    RuntimeStatistics.OperatorStatisticsGroup serverPointStatisticsGroup =
                        getRelationToStatistics().get(entry.getKey());
                    if (serverPointStatisticsGroup == null) {
                        //FIXME Gather不会被统计到
                        if (logger.isDebugEnabled()) {
                            logger.debug("error serverPointStatisticsGroup:" + entry.getKey());
                        }
                        continue;
                    }
                    RuntimeStatistics.OperatorStatisticsGroup taskOperatorStat = entry.getValue();
                    serverPointStatisticsGroup.hasInputOperator = taskOperatorStat.hasInputOperator;
                    serverPointStatisticsGroup.statistics.addAll(taskOperatorStat.statistics);

                    serverPointStatisticsGroup.createAndInitJdbcStmtDuration.addAndGet(
                        taskOperatorStat.createAndInitJdbcStmtDuration.get());
                    serverPointStatisticsGroup.prepareStmtEnvDuration.addAndGet(
                        taskOperatorStat.prepareStmtEnvDuration.get());
                    serverPointStatisticsGroup.createConnDuration.addAndGet(
                        taskOperatorStat.createConnDuration.get());
                    serverPointStatisticsGroup.initConnDuration.addAndGet(
                        taskOperatorStat.initConnDuration.get());
                    serverPointStatisticsGroup.waitConnDuration.addAndGet(
                        taskOperatorStat.waitConnDuration.get());
                    serverPointStatisticsGroup.createAndInitJdbcStmtDuration.addAndGet(
                        taskOperatorStat.createAndInitJdbcStmtDuration.get());
                    serverPointStatisticsGroup.execJdbcStmtDuration.addAndGet(
                        taskOperatorStat.execJdbcStmtDuration.get());
                    serverPointStatisticsGroup.fetchJdbcResultSetDuration.addAndGet(
                        taskOperatorStat.fetchJdbcResultSetDuration.get());
                    serverPointStatisticsGroup.closeAndClearJdbcEnv.addAndGet(
                        taskOperatorStat.closeAndClearJdbcEnv.get());
                    //FIXME 需要确认下
                    addPhySqlTimecost(taskOperatorStat.execJdbcStmtDuration.get());
                }
            }
            if (taskStatus.getMemoryStatistics() != null) {
                TaskMemoryStatisticsGroup memoryStatistics = taskStatus.getMemoryStatistics();
                MemoryStatisticsGroup nodeStatisticsGroup =
                    getMemoryToStatistics().get(taskStatus.getSelf().getNodeServer().toString());
                if (nodeStatisticsGroup == null) {
                    nodeStatisticsGroup =
                        new MemoryStatisticsGroup(memoryStatistics.getQueryMemoryUsage(),
                            memoryStatistics.getQueryMaxMemoryUsage(), 0);
                    nodeStatisticsGroup.setMemoryStatistics(new TreeMap<>());
                    getMemoryToStatistics().put(taskStatus.getSelf().getNodeServer().toString(),
                        nodeStatisticsGroup);
                }
                if (memoryStatistics.getQueryMaxMemoryUsage()
                    > nodeStatisticsGroup.getMaxMemoryUsage()) {
                    nodeStatisticsGroup.setMaxMemoryUsage(
                        memoryStatistics.getQueryMaxMemoryUsage());
                }
                nodeStatisticsGroup.getMemoryStatistics()
                    .put("Task@" + taskStatus.getTaskId().toString(), memoryStatistics);
            }
            if (context.getTracer() != null && taskStatus.getSqlTracer() != null) {
                context.getTracer().getOperations().addAll(taskStatus.getSqlTracer());
            }
        }

    }

    public Map<RelNode, RuntimeStatisticsSketch> toSketchExt() {
        Map<RelNode, RuntimeStatisticsSketch> results = new IdentityHashMap<>();
        for (Map.Entry<Integer, OperatorStatisticsGroup> entry : relationToStatistics.entrySet()) {
            final OperatorStatisticsGroup stats = entry.getValue();
            RuntimeStatisticsSketch sketchExt = stats.toSketchExt();
            results.put(relationIdToNode.get(entry.getKey()), sketchExt);
        }
        return results;
    }

    public Metrics toMetrics() {

        /*
         * How to measure CPU time via our statistics? We define the CPU time as
         * the processDuration consumed by all the server or worker threads,
         * excluding any waiting (blocking and IO) time, so we sum up all the
         * time cost of each thread and subtract the blocking time caused by
         * Parallel Gather and IO time of LogicalView
         */
        Metrics metrics = new Metrics();

        // ==============================
        // the total count from all logicalView
        long fetchedRows = 0;

        // the total shard count of all the lv of tables in sql
        long phySqlCount = 1;

        // ============ mem =============
        // the percent(PCT) of Max memory usage during this query
        double queryMemPct = 0;

        // the max memory usage during the query
        long queryMem = 0;

        // the memory usage of a plan after sharding
        long planShardMem = 0;

        // the memory usage sum of operator temp table
        long planTmpTbMem = 0;

        // ============ log cpu =============
        // all time cost for a logic sql, including logic part and physical part
        long totalTc = 0;

        // logCpu = sqlToPlanTc + planExecTc
        long logCpuTc = 0;

        // the time cost of converting a sql to a final plan
        long sqlToPlanTc = 0;

        // the time cost sum of executing all operators of the plan build by
        // drds
        long execPlanTc = 0;

        // ============= phy cpu ============
        // the time cost sum of executing all phy sql and reading all result set
        // of phy sql, so
        // phyCpuhTc = execSqlTc + fetchRsTc
        long phyCpuTc = 0;

        // the response time sum of executing all phy sql of a plan in mysql
        long execSqlTc = 0;

        // the time cost sum of fetching all result set from mysql of the plan
        long fetchRsTc = 0;

        // the time cost sum of closing all result set from mysql of the plan
        long closeRsTc = 0;

        // =========================
        // the time cost sum of all create & wait & init phy jdbc conn for a
        // plan
        long phyConnTc = 0;

        // the time cost sum of fetching TSO
        long fetchTSOTc = 0;

        // the time cost sum of fetching Sequence
        long fetchSeqTc = 0;

        // trx type
        String trxType = null;

        // the time cost of commit prepare phase
        long commitPrepareTc = 0;

        // the time cost of commit get tso phase
        long commitTsoTc = 0;

        // the time cost of commit logger phase
        long commitLoggerTc = 0;

        // the time cost of commit commit phase
        long commitCommitTc = 0;

        // the time cost of generate columnar snapshot in SplitManager
        long columnarSnapshotTc = 0;

        long affectedPhyRows = 0;

        boolean isQuery = SqlTypeUtils.isSelectSqlType(sqlType);

        boolean memBlockedFlag = false;

        long spillCnt = 0;

        CpuStat cpuStatOfCurrQuery = sqlWholeStageCpuStat;
        List<CpuStatItem> cpuStatItems = cpuStatOfCurrQuery.getStatItems();
        for (int i = 0; i < cpuStatItems.size(); ++i) {
            CpuStatItem item = cpuStatItems.get(i);
            if (item != null) {
                sqlToPlanTc += item.timeCostNano;
            }
        }

        fetchRsTc = NOT_SUPPORT_VALUE;
        if (planTree != null && isQuery) {
            fetchRsTc = 0;
            for (Map.Entry<Integer, OperatorStatisticsGroup>
                entry : relationToStatistics.entrySet()) {
                final AbstractRelNode relation =
                    (AbstractRelNode) relationIdToNode.get(entry.getKey());
                OperatorStatisticsGroup operatorStatisticsGroup = entry.getValue();
                if (relation instanceof LogicalView || relation instanceof LogicalInsert) {

                    // the time cost of fetchRsTc include the fetching rs time
                    // cost and close rs time cost
                    fetchRsTc += operatorStatisticsGroup.fetchJdbcResultSetDuration.get()
                        + operatorStatisticsGroup.closeAndClearJdbcEnv.get();
                }
            }
        }

        // ====== Rows Count ========
        affectedPhyRows = this.totalPhyAffectedRows.get();
        fetchedRows += this.totalPhyFetchRows.get();
        phySqlCount = this.totalPhySqlCount.get();

        // ====== Cpu ========
        logCpuTc = this.sqlLogCpuTime.get();
        execSqlTc = this.totalPhySqlTimecost.get();
        fetchTSOTc = this.totalFetchTSOTimecost.get();
        fetchSeqTc = this.totalFetchSequenceTimecost.get();
        if (this.trxType != null) {
            trxType = this.trxType;
        } else {
            trxType = "UNKNOWN";
        }
        commitPrepareTc = this.commitPrepareTimecost.get();
        commitTsoTc = this.commitTsoTimecost.get();
        commitLoggerTc = this.commitLoggerTimecost.get();
        commitCommitTc = this.commitCommitTimecost.get();
        columnarSnapshotTc = this.columnarSnapshotTimecost.get();
        execPlanTc = logCpuTc - sqlToPlanTc;
        if (fetchRsTc > NOT_SUPPORT_VALUE) {
            phyCpuTc = execSqlTc + fetchRsTc;
        } else {
            phyCpuTc = execSqlTc;
        }
        totalTc = logCpuTc + phyCpuTc;
        phyConnTc = this.totalPhyConnTimecost.get();

        // ====== Memory ========
        queryMem = getSqlMemoryMaxUsage();
        queryMemPct = getSqlMemoryMaxUsagePct();
        MemoryPool queryPool = getMemoryPool();
        if (queryPool != null && queryPool instanceof QueryMemoryPool) {
            memBlockedFlag = ((QueryMemoryPool) queryPool).isBlockFlag();
            queryMem = queryPool.getMaxMemoryUsage();
            long globalMemLimit = MemoryManager.getInstance().getGlobalMemoryPool().getMaxLimit();
            BigDecimal queryMemPctVal = new BigDecimal(queryMem * 100 / globalMemLimit);
            queryMemPct = queryMemPctVal.setScale(
                4, BigDecimal.ROUND_HALF_UP).doubleValue();

            MemoryPool planMemoryPool = ((QueryMemoryPool) queryPool).getPlanMemPool();
            if (planMemoryPool != null) {
                planShardMem += planMemoryPool.getMaxMemoryUsage();
            }
            planTmpTbMem += (queryPool.getMaxMemoryUsage() - planMemoryPool.getMaxMemoryUsage());
        }

        if (querySpillSpaceMonitor != null) {
            spillCnt = querySpillSpaceMonitor.getSpillCnt();
        }

        // ====== disable some metrics for non-query sql ========
        if (!isQuery) {
            // not support metrics for non-query sql
            queryMemPct = NOT_SUPPORT_VALUE;
            queryMem = NOT_SUPPORT_VALUE;
            planShardMem = NOT_SUPPORT_VALUE;
            planTmpTbMem = NOT_SUPPORT_VALUE;
            fetchRsTc = NOT_SUPPORT_VALUE;
        }

        metrics.fetchedRows = fetchedRows;
        metrics.affectedPhyRows = affectedPhyRows;
        metrics.phySqlCount = phySqlCount;

        metrics.totalTc = totalTc;
        metrics.logCpuTc = logCpuTc;
        metrics.sqlToPlanTc = sqlToPlanTc;
        metrics.execPlanTc = execPlanTc;

        metrics.phyCpuTc = phyCpuTc;
        metrics.execSqlTc = execSqlTc;
        metrics.fetchRsTc = fetchRsTc;
        metrics.phyConnTc = phyConnTc;
        metrics.fetchTSOTc = fetchTSOTc;
        metrics.fetchSeqTc = fetchSeqTc;
        metrics.trxType = trxType;
        metrics.commitPrepareTc = commitPrepareTc;
        metrics.commitTsoTc = commitTsoTc;
        metrics.commitLoggerTc = commitLoggerTc;
        metrics.commitCommitTc = commitCommitTc;
        metrics.columnarSnapshotTc = columnarSnapshotTc;

        metrics.queryMemPct = queryMemPct;
        metrics.queryMem = queryMem;
        metrics.planShardMem = planShardMem;
        metrics.planTmpTbMem = planTmpTbMem;

        metrics.spillCnt = spillCnt;
        if (extColStats != null && !extColStats.isEmpty()) {
            metrics.extColStatsStr = extColStats.toPrintable();
        }
        if (memBlockedFlag) {
            metrics.memBlockedFlag = 1;
        }
        return metrics;
    }

    @Override
    public MemoryEstimation getMemoryEstimation() {
        return this.sqlWholeStageMemEstimation;
    }

    @Override
    public CpuStat getCpuStat() {
        return this.sqlWholeStageCpuStat;
    }

    @Override
    public MemoryPool getMemoryPool() {
        return holdMemoryPool;
    }

    @Override
    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    public long getSqlCpuTime() {
        return sqlLogCpuTime.get();
    }

    public long getSqlMemoryMaxUsage() {
        MemoryPool memoryPool = getMemoryPool();
        if (memoryPool == null) {
            return 0;
        }
        return memoryPool.getMaxMemoryUsage();
    }

    public void updateSqlMemoryMaxUsageInfo(Map<String, Long> maxQueryMemoryUsage) {
        if (maxQueryMemoryUsage == null) {
            return;
        }
        for (Map.Entry<String, Long> entry : maxQueryMemoryUsage.entrySet()) {
            final String hostPort = entry.getKey();
            final Long memoryUsage = entry.getValue();
            maxMemoryUsageForEachNode.compute(hostPort, (k, v) -> {
                if (v == null) {
                    return memoryUsage;
                }
                return Math.max(memoryUsage, v);
            });
        }
    }

    public Map<String, Long> getMaxMemoryUsageForEachNode() {
        return maxMemoryUsageForEachNode;
    }

    public String getMppMaxMemoryUsageInfo() {
        if (maxMemoryUsageForEachNode == null || maxMemoryUsageForEachNode.isEmpty()) {
            return "0";
        }

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0;
        int count = 0;

        for (Long value : maxMemoryUsageForEachNode.values()) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            count++;
        }

        long average = count != 0 ? (sum / count) : 0;

        return String.format("cnt-%s/max-%s/min-%s/avg-%s/sum-%s",
            count,
            toMemorySizeString(max),
            toMemorySizeString(min),
            toMemorySizeString(average),
            toMemorySizeString(sum)
        );
    }

    private static String toMemorySizeString(long bytes) {
        if (bytes < 1024) {
            return String.format("%.1fB", bytes * 1.0d);
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1fKB", bytes / 1024.0d);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1fMB", bytes / 1024.0d / 1024);
        } else {
            return String.format("%.1fGB", bytes / 1024.0d / 1024 / 1024);
        }
    }

    public void updateVersionStorageStatistics(Map<TaskId, VersionStorageStatistics> versionStorageStatistics) {
        // Overwrite with the latest VersionStorageStatistics
        versionStorageStatisticsMap.putAll(versionStorageStatistics);
    }

    public String getVersionStorageStatisticsInfo() {
        VersionStorageStatistics result = VersionStorageStatistics.from(versionStorageStatisticsMap.values());
        return result.toPrintable();
    }

    public void updateColumnarScanMetrics(Map<TaskId, ColumnarScanMetrics> columnarScanMetrics) {
        columnarScanMetricsMap.putAll(columnarScanMetrics);
    }

    public String getColumnarScanMetricsInfo() {
        ColumnarScanMetrics result = ColumnarScanMetrics.from(columnarScanMetricsMap.values());
        return result.toPrintable();
    }

    public double getSqlMemoryMaxUsagePct() {

        MemoryPool memoryPool = getMemoryPool();
        if (memoryPool == null) {
            return -1;
        }

        long maxMemUsage = memoryPool.getMaxMemoryUsage();
        long globalMemLimit = MemoryManager.getInstance().getGlobalMemoryPool().getMaxLimit();
        BigDecimal queryMemPctVal = new BigDecimal(maxMemUsage * 100 / globalMemLimit);
        double queryMemPct = queryMemPctVal.setScale(
            4, BigDecimal.ROUND_HALF_UP).doubleValue();
        return queryMemPct;

    }

    public static class OperatorStatisticsGroup {

        @JsonIgnore
        public RuntimeStatistics runtimeStat;
        @JsonIgnore
        public RelNode parentRel;
        @JsonIgnore
        public RelNode targetRel;

        /**
         * operator stats for parallel query
         */
        @JsonProperty
        public Set<OperatorStatistics> statistics = new HashSet<>();

        @JsonProperty
        public boolean hasInputOperator = true;

        /**
         * the finish count of ChunkExecutor for parallel query.
         * <p>
         * <pre>
         *     For LogicalView in parallel query, its will have multi CursorExec that is running at
         *     the same time, only when the last CursorExec has been finished fetching result set
         * and the operator statistics of LV can be completed computing.
         *
         * <pre/>
         */
        @JsonProperty
        public AtomicLong finishCount = new AtomicLong(0);

        /**
         * The total count of LogicalViewResultCursor for LV
         */
        @JsonProperty
        public AtomicLong totalCount = new AtomicLong(0);

        /**
         * Duration of async task cpu, included the async task cpu from children
         * (only stat the cpu time of async cpu time of thread ), its cpu stat
         * use ThreadMXBean
         */
        @JsonProperty
        public AtomicLong childrenAsyncTaskCpuTime = new AtomicLong(0);

        /**
         * Duration of async task cpu from self (only stat the cpu time of async
         * cpu time of thread ), its cpu stat use ThreadMXBean.getCurrentThreadCpuTime()
         */
        @JsonProperty
        public AtomicLong selfAsyncTaskCpuTime = new AtomicLong(0);

        // ==================
        /**
         * The total time cost of async from self (include io time/lock/time/
         * are included) included), its time cost stat use System.nano
         */
        @JsonProperty
        public AtomicLong selfAsyncTaskTimeCost = new AtomicLong(0);

        /**
         * The total time cost of all LogicalViewResultCursor to process all
         * physical result set (exec & fetch)
         */
        @JsonProperty
        public AtomicLong processLvTimeCost = new AtomicLong(0);

        /**
         * The total time cost of wait & lock time & init time
         */
        @JsonProperty
        public AtomicLong waitLockDuration = new AtomicLong(0);

        /**
         * Duration sum of create connections for all phy table operation of one
         * logicalView or logicalModify
         */
        @JsonProperty
        public AtomicLong createConnDuration = new AtomicLong(0);

        /**
         * Duration sum of wait to get connection for all phy table operation of
         * one logicalView or logicalModify
         */
        @JsonProperty
        public AtomicLong waitConnDuration = new AtomicLong(0);

        /**
         * Duration sum of wait to init connection for all phy table operation
         * of one logicalView or logicalModify
         */
        @JsonProperty
        public AtomicLong initConnDuration = new AtomicLong(0);

        // ========= time cost for execution and waiting for first row =========

        /**
         * Duration sum of prepare the env for phy sql query, e.g. build phy sql
         * & get conn & set jdbc params, and so on
         */
        @JsonProperty
        public AtomicLong prepareStmtEnvDuration = new AtomicLong(0);

        /**
         * Duration sum of init jdbc conn env and create stmt for all phy table
         * operations of one logicalView or logicalModify
         */
        @JsonProperty
        public AtomicLong createAndInitJdbcStmtDuration = new AtomicLong(0);

        /**
         * Duration sum of exec jdbc stmt for all phy table operation of one
         * logicalView or one logicalModify
         */
        @JsonProperty
        public AtomicLong execJdbcStmtDuration = new AtomicLong(0);

        // ========= time cost for fetching the the whole result set =========

        /**
         * Duration sum of fetch all JdbcResultSet for all phy table operation
         * of one logicalView or one logicalModify
         */
        @JsonProperty
        public AtomicLong fetchJdbcResultSetDuration = new AtomicLong(0);

        /**
         * Duration sum of clear and close physical jdbc stmt & rs
         */
        @JsonProperty
        public AtomicLong closeAndClearJdbcEnv = new AtomicLong(0);

        /**
         * The row count sum of all physical jdbc result set
         */
        @JsonProperty
        public AtomicLong phyResultSetRowCount = new AtomicLong(0);

        @JsonProperty
        public AtomicLong ioBytesCount = new AtomicLong(0);

        /**
         * The parallelism for the fetching result set of logical view
         */
        @JsonProperty
        public int fetchJdbcResultSetParallelism = 0;

        public OperatorStatisticsGroup(RuntimeStatistics runtimeStat) {
            this.runtimeStat = runtimeStat;
        }

        @JsonCreator
        public OperatorStatisticsGroup(
            @JsonProperty("statistics") Set<OperatorStatistics> statistics,
            @JsonProperty("hasInputOperator") boolean hasInputOperator,
            @JsonProperty("finishCount") long finishCount,
            @JsonProperty("totalCount") long totalCount,
            @JsonProperty("childrenAsyncTaskCpuTime") long childrenAsyncTaskCpuTime,
            @JsonProperty("selfAsyncTaskCpuTime") long selfAsyncTaskCpuTime,
            @JsonProperty("selfAsyncTaskTimeCost") long selfAsyncTaskTimeCost,
            @JsonProperty("processLvTimeCost") long processLvTimeCost,
            @JsonProperty("waitLockDuration") long waitLockDuration,
            @JsonProperty("createConnDuration") long createConnDuration,
            @JsonProperty("waitConnDuration") long waitConnDuration,
            @JsonProperty("initConnDuration") long initConnDuration,
            @JsonProperty("prepareStmtEnvDuration") long prepareStmtEnvDuration,
            @JsonProperty("createAndInitJdbcStmtDuration")
            long createAndInitJdbcStmtDuration,
            @JsonProperty("execJdbcStmtDuration") long execJdbcStmtDuration,
            @JsonProperty("fetchJdbcResultSetDuration") long fetchJdbcResultSetDuration,
            @JsonProperty("closeAndClearJdbcEnv") long closeAndClearJdbcEnv,
            @JsonProperty("phyResultSetRowCount") long phyResultSetRowCount,
            @JsonProperty("ioBytesCount") long ioBytesCount,
            @JsonProperty("fetchJdbcResultSetParallelism")
            int fetchJdbcResultSetParallelism) {
            this.statistics = statistics;
            this.hasInputOperator = hasInputOperator;
            this.finishCount.set(finishCount);
            this.totalCount.set(totalCount);
            this.childrenAsyncTaskCpuTime.set(childrenAsyncTaskCpuTime);
            this.selfAsyncTaskCpuTime.set(selfAsyncTaskCpuTime);
            this.selfAsyncTaskTimeCost.set(selfAsyncTaskTimeCost);
            this.processLvTimeCost.set(processLvTimeCost);
            this.waitLockDuration.set(waitLockDuration);
            this.createConnDuration.set(createConnDuration);
            this.waitConnDuration.set(waitConnDuration);
            this.initConnDuration.set(initConnDuration);
            this.prepareStmtEnvDuration.set(prepareStmtEnvDuration);
            this.createAndInitJdbcStmtDuration.set(createAndInitJdbcStmtDuration);
            this.execJdbcStmtDuration.set(execJdbcStmtDuration);
            this.fetchJdbcResultSetDuration.set(fetchJdbcResultSetDuration);
            this.closeAndClearJdbcEnv.set(closeAndClearJdbcEnv);
            this.phyResultSetRowCount.set(phyResultSetRowCount);
            this.ioBytesCount.set(ioBytesCount);
            this.fetchJdbcResultSetParallelism = fetchJdbcResultSetParallelism;
        }

        synchronized void add(OperatorStatistics stats) {
            if (!statistics.contains(stats)) {
                statistics.add(stats);
            }
        }

        synchronized RuntimeStatisticsSketch toSketch() {
            long startupDuration = 0;
            long duration = 0;
            long rowCount = 0;
            long ioBytesCount = 0;
            long runtimeFilteredCount = 0;
            long memory = 0;
            int spillCnt = 0;
            long workerDuration = 0;
            long outputBytes = 0;
            for (OperatorStatistics statistic : statistics) {
                startupDuration += statistic.getStartupDuration();
                duration += statistic.getProcessDuration();
                rowCount += statistic.getRowCount();
                ioBytesCount += statistic.getIOReadBytes();
                runtimeFilteredCount += statistic.getRuntimeFilteredCount();
                memory += statistic.getMemory();
                workerDuration += statistic.getWorkerDuration();
                spillCnt += statistic.getSpillCnt();
            }
            double durationSeconds = (double) duration / 1e9;
            double startupDurationSeconds = (double) startupDuration / 1e9;
            double workerDurationSeconds = (double) workerDuration / 1e9;

            int n = statistics.size();
            if (targetRel instanceof LogicalView && !(targetRel instanceof OSSTableScan)) {
                n = this.fetchJdbcResultSetParallelism;
            }

            return new RuntimeStatisticsSketch(durationSeconds, startupDurationSeconds,
                workerDurationSeconds, rowCount, ioBytesCount, runtimeFilteredCount,
                outputBytes, memory, n, spillCnt);
        }

        synchronized RuntimeStatisticsSketch toSketchExt() {

            int n = statistics.size();
            long startupDuration = 0;
            long duration = 0;
            long closeDuration = 0;
            long rowCount = 0;
            long ioBytesCount = 0;
            long runtimeFilteredCount = 0;
            long memory = 0;
            int spillCnt = 0;
            long outputBytes = 0;

            long createConnDurationSum = 0;
            long waitConnDurationSum = 0;
            long initConnDurationSum = 0;
            long totalGetConnDurationSum = 0;
            long createAndInitJdbcStmtDurationSum = 0;
            long execJdbcStmtDurationSum = 0;
            long fetchJdbcResultSetDurationSum = 0;
            long closeJdbcResultSetDurationSum = 0;
            long childrenAsyncCpuTimeSum = this.childrenAsyncTaskCpuTime.get();
            long selfAsyncCpuTimeSum = this.selfAsyncTaskCpuTime.get();
            long statisticCount = statistics.size();

            RuntimeStatisticsSketchExt rsse = null;
            for (OperatorStatistics os : statistics) {
                startupDuration += os.getStartupDuration();
                duration += os.getProcessDuration();
                closeDuration += os.getCloseDuration();
                rowCount += os.getRowCount();
                ioBytesCount += os.getIOReadBytes();
                runtimeFilteredCount += os.getRuntimeFilteredCount();
                memory += os.getMemory();
                spillCnt += os.getSpillCnt();
            }

            createConnDurationSum = this.createConnDuration.get();
            waitConnDurationSum = this.waitConnDuration.get();
            initConnDurationSum = this.initConnDuration.get();
            totalGetConnDurationSum =
                initConnDurationSum + waitConnDurationSum + createConnDurationSum;
            createAndInitJdbcStmtDurationSum = this.createAndInitJdbcStmtDuration.get();
            execJdbcStmtDurationSum = this.execJdbcStmtDuration.get();
            fetchJdbcResultSetDurationSum = this.fetchJdbcResultSetDuration.get();
            closeJdbcResultSetDurationSum = this.closeAndClearJdbcEnv.get();

            if (this.runtimeStat.isSimpleLvPlan) {
                long sqlToPlanTc = 0;
                CpuStat cpuStat = this.runtimeStat.getSqlWholeStageCpuStat();
                List<CpuStatItem> cpuStatItems = cpuStat.getStatItems();
                for (int i = 0; i < cpuStatItems.size(); ++i) {
                    CpuStatItem item = cpuStatItems.get(i);
                    if (item != null) {
                        sqlToPlanTc += item.timeCostNano;
                    }
                }
                duration = this.runtimeStat.sqlLogCpuTime.get() - sqlToPlanTc;
                rowCount = this.phyResultSetRowCount.get();
                ioBytesCount = this.ioBytesCount.get();
            }
            rsse = new RuntimeStatisticsSketchExt(
                startupDuration, duration, closeDuration, 0,
                rowCount, ioBytesCount, runtimeFilteredCount, outputBytes,
                memory, n, hasInputOperator, spillCnt);

            rsse.setCreateConnDurationNanoSum(createConnDurationSum);
            rsse.setWaitConnDurationNanoSum(waitConnDurationSum);
            rsse.setInitConnDurationNanoSum(initConnDurationSum);
            rsse.setTotalGetConnDurationNanoSum(totalGetConnDurationSum);

            rsse.setCreateAndInitJdbcStmtDurationNanoSum(createAndInitJdbcStmtDurationSum);
            rsse.setExecJdbcStmtDurationNanoSum(execJdbcStmtDurationSum);
            rsse.setFetchJdbcResultSetDurationNanoSum(fetchJdbcResultSetDurationSum);
            rsse.setCloseJdbcResultSetDurationNanoSum(closeJdbcResultSetDurationSum);
            rsse.setSubOperatorStatCount(statisticCount);
            rsse.setChildrenAsyncTaskDuration(childrenAsyncCpuTimeSum);
            rsse.setSelfAsyncTaskDuration(selfAsyncCpuTimeSum);
            return rsse;
        }
    }

    public static class Metrics {

        // Connection Id
        public long connectionId;

        // How much rows was processed in total
        public long affectedPhyRows;

        // How much rows was fetched in total
        public long fetchedRows;

        // the total phy sql count of a logical sql, included all lv of tables
        public long phySqlCount;

        // =========================
        // the percent(PCT) of the max memory usage during this query
        public double queryMemPct;

        // the max memory usage during the query
        public long queryMem;

        // the max memory usage of a plan after sharding during the query
        public long planShardMem;

        // the max memory usage sum of operator temp table during the query
        public long planTmpTbMem;

        // the total time cost of finishing sql
        public long totalTc;

        // =========================
        // logCpu = sqlToPlanTc + execPlanTc
        public long logCpuTc;

        // the time cost of converting a sql to a final plan
        public long sqlToPlanTc;

        // the time cost sum of executing all operators of the plan build by
        // drds
        public long execPlanTc;

        // =========================
        // the time cost sum of executing all phy sql of a plan in mysql and
        // reading all its resultset,
        // phyCpuTc = execSqlTc + fetchRsTc
        public long phyCpuTc;

        // the timecost sum of executing all phy sql of a plan in mysql
        public long execSqlTc;

        // the timecost sum of fetching all result set from mysql of the plan
        public long fetchRsTc;

        // =========================
        // the time cost sum of all create & wait & init phy jdbc conn for a
        // plan
        public long phyConnTc;

        // the time cost sum of fetching tso
        public long fetchTSOTc;

        // the time cost sum of fetching sequence
        public long fetchSeqTc;

        // the transaction type
        public String trxType;

        // the time cost sum of commit prepare
        public long commitPrepareTc;

        // the time cost sum of commit logger
        public long commitLoggerTc;

        // the time cost sum of commit tso
        public long commitTsoTc;

        // the time cost sum of commit commit
        public long commitCommitTc;

        // the time cost sum of fetching auto commit
        public long fetchAutoCommitTc;

        // the time cost of generate columnar snapshot in SplitManager
        public long columnarSnapshotTc;

        // the sql template id of planCache key
        public String sqlTemplateId = "-";

        public int memBlockedFlag = 0;

        public long spillCnt;

        // External column latency breakdown (null if no ext col involved)
        public String extColStatsStr;
    }

    public CpuStat getSqlWholeStageCpuStat() {
        return sqlWholeStageCpuStat;
    }

    public MemoryEstimation getSqlWholeStageMemEstimation() {
        return sqlWholeStageMemEstimation;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean isFinishExecution() {
        return finishExecution;
    }

    public void setFinishExecution(boolean finishExecution) {
        this.finishExecution = finishExecution;
    }

    @Override
    public void collectThreadCpu(long threadCpuTimeCost) {
        this.sqlLogCpuTime.addAndGet(threadCpuTimeCost);
    }

    public void collectMppStats(int relationId, RuntimeStatisticsSketch mppOperatorStat) {
        this.mppOperatorStats.put(relationId, mppOperatorStat);
    }

    @Override
    public void addPhyConnTimecost(long totalPhyConnTimecost) {
        this.totalPhyConnTimecost.addAndGet(totalPhyConnTimecost);
    }

    @Override
    public void addFetchTSOTimecost(long totalFetchTSOTimecost) {
        this.totalFetchTSOTimecost.addAndGet(totalFetchTSOTimecost);
    }

    @Override
    public void addFetchSequenceTimecost(long totalFetchSequenceTimecost) {
        this.totalFetchSequenceTimecost.addAndGet(totalFetchSequenceTimecost);
    }

    @Override
    public void setTrxType(String trxType) {
        this.trxType = trxType;
    }

    public String getTrxType() {
        return trxType;
    }

    @Override
    public void addCommitPrepareTimecost(long commitPrepareTimecost) {
        this.commitPrepareTimecost.addAndGet(commitPrepareTimecost);
    }

    public long getCommitPrepareTimecost() {
        return this.commitPrepareTimecost.get();
    }

    @Override
    public void addCommitLoggerTimecost(long commitLoggerTimecost) {
        this.commitLoggerTimecost.addAndGet(commitLoggerTimecost);
    }

    public long getCommitLoggerTimecost() {
        return this.commitLoggerTimecost.get();
    }

    @Override
    public void addCommitTsoTimecost(long commitTsoTimecost) {
        this.commitTsoTimecost.addAndGet(commitTsoTimecost);
    }

    public long getCommitTsoTimecost() {
        return this.commitTsoTimecost.get();
    }

    @Override
    public void addCommitCommitTimecost(long commitCommitTimecost) {
        this.commitCommitTimecost.addAndGet(commitCommitTimecost);
    }

    public long getCommitCommitTimecost() {
        return this.commitCommitTimecost.get();
    }

    @Override
    public void addColumnarSnapshotTimecost(long columnarSnapshotTimecost) {
        this.columnarSnapshotTimecost.addAndGet(columnarSnapshotTimecost);
    }

    @Override
    public void addPhySqlCount(long shardCount) {
        totalPhySqlCount.addAndGet(shardCount);
    }

    @Override
    public void addPhySqlTimecost(long phySqlTc) {
        totalPhySqlTimecost.addAndGet(phySqlTc);
    }

    @Override
    public void addPhyFetchRows(long phyRsTc) {
        totalPhyFetchRows.addAndGet(phyRsTc);
    }

    @Override
    public void addPhyAffectedRows(long phyAffectiveRow) {
        totalPhyAffectedRows.addAndGet(phyAffectiveRow);
    }

    public String getTraceId() {
        return traceId;
    }

    @Override
    public boolean isRunningWithCpuProfile() {
        return runningWithCpuProfile;
    }

    @Override
    public void setRunningWithCpuProfile(boolean runningWithCpuProfile) {
        this.runningWithCpuProfile = runningWithCpuProfile;
    }

    public boolean isFromAllAtOnePhyTable() {
        return isFromAllAtOnePhyTable;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    @Override
    public void holdMemoryPool() {
    }

    public Metrics getStoredMetrics() {
        return storedMetrics;
    }

    public void setStoredMetrics(Metrics storedMetrics) {
        this.storedMetrics = storedMetrics;
    }

    public void setExtColStats(com.alibaba.polardbx.common.columnar.ExternalColumnStatistics extColStats) {
        this.extColStats = extColStats;
    }
}

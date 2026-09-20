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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Column;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.Table;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.htaprouting.OptimizerType;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class IndexAdvisor {

    protected final Logger logger = LoggerFactory.getLogger(IndexAdvisor.class);

    private ExecutionPlan executionPlan;

    private ExecutionContext executionContext;

    private PlannerContext plannerContext;

    private RelMetadataQuery mq;

    private Map<String, Map<String, Configuration>> configurationMap;

    private CoverableColumnSet coverableColumnSet;

    private PartitionRuleSet partitionRuleSet;

    private Map<String, Collection<HumanReadableRule>> partitionPolicyMap;

    private AdviseType adviseType;

    private Map<String, Map<String, Pair<OSSTableScan, RelOptCost>>> ossTableScanCostMap;

    RelOptCost finalCost;
    RelNode finalPlan;

    Set<CandidateIndex> finalCandidateSet;

    public enum AdviseType {
        LOCAL_INDEX,
        GLOBAL_INDEX,
        GLOBAL_COVERING_INDEX,
        BROADCAST,
        COLUMNAR_INDEX
    }

    public static final String ANALYZE_FORMAT = "`%s`.`%s`";

    public IndexAdvisor(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        this.executionPlan = executionPlan;
        this.executionContext = executionContext;
        this.plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
        this.mq = executionPlan.getPlan().getCluster().getMetadataQuery();
        this.configurationMap = new HashMap<>();
    }

    /**
     * check whether any table in the query should refresh statistics
     *
     * @return an 'analyze table' advise if there are some tables should refresh statistics
     */
    public AdviceResult checkStatistics() {
        SqlNode ast = executionPlan.getAst();
        if (ast == null) {
            return null;
        }
        if (!(ast.getKind().belongsTo(SqlKind.QUERY) || ast.getKind().belongsTo(SqlKind.DML))) {
            return null;
        }
        // init
        executionPlan.getPlan().getCluster().invalidateMetadataQuery();
        this.mq = executionPlan.getPlan().getCluster().getMetadataQuery();
        this.configurationMap = new HashMap<>();

        // collect tables
        RelNode unOptimizedPlan = executionContext.getUnOptimizedPlan();
        RelNode logicalPlan = Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
        TableScanFinder tableScanFinder = new TableScanFinder();
        logicalPlan.accept(tableScanFinder);
        List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();

        Set<String> tables = Sets.newTreeSet(String::compareToIgnoreCase);
        for (Pair<String, TableScan> pair : tableScans) {
            String schemaName = pair.getKey().toLowerCase();
            String tableName = CBOUtil.getTableMeta(pair.getValue().getTable()).getTableName().toLowerCase();
            String fullTable = String.format(ANALYZE_FORMAT, schemaName, tableName);
            if (StatisticManager.expired(schemaName, tableName)) {
                tables.add(fullTable);
            }
        }
        if (!tables.isEmpty()) {
            StringBuilder infoBuilder = new StringBuilder();
            infoBuilder.append("Statistics of tables are expired! You can try to analyze tables to refresh ");
            infoBuilder.append("statistics during the low business period by `ANALYZE TABLE ");
            infoBuilder.append(String.join(",", tables));
            infoBuilder.append("`");
            AdviceResult adviceResult = new AdviceResult(null, null);
            adviceResult.setInfo(infoBuilder.toString());
            return adviceResult;
        }
        return null;
    }

    public AdviceResult advise(AdviseType adviseType) {
        this.adviseType = adviseType;

        SqlNode ast = executionPlan.getAst();
        if (ast == null) {
            return null;
        }
        if (!(ast.getKind().belongsTo(SqlKind.QUERY) || ast.getKind().belongsTo(SqlKind.DML))) {
            return null;
        }

        // init
        executionPlan.getPlan().getCluster().invalidateMetadataQuery();
        this.mq = executionPlan.getPlan().getCluster().getMetadataQuery();
        this.configurationMap = new HashMap<>();

        RelNode unOptimizedPlan = executionContext.getUnOptimizedPlan();
        RelNode logicalPlan = Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
        RelNode physicalPlan =
            Planner.getInstance().optimizeByPlanEnumerator(unOptimizedPlan, logicalPlan, plannerContext);
        RelOptCost originalCost = mq.getCumulativeCost(physicalPlan);

        AdviceResult adviceResult = new AdviceResult(originalCost, executionPlan.getPlan());

        //prepare broadcast tables
        if (adviseType == AdviseType.BROADCAST) {
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("origin rowcount ").append(mq.getRowCount(physicalPlan)).append("\n");
                sb.append(RelUtils
                        .toString(physicalPlan, executionContext.getParams().getCurrentParameter(),
                            RexUtils.getEvalFunc(executionContext), executionContext))
                    .append("original cost").append(originalCost.toString());
                logger.debug(sb.toString());
            }
            BroadcastContext initContext = initBroadcast(unOptimizedPlan, originalCost);
            if (initContext != null) {
                for (Map.Entry<String, Set<String>> entry : initContext.broadcastTables.entrySet()) {
                    if (entry.getValue().size() > 0) {
                        executionContext.getSchemaManager(entry.getKey())
                            .getTddlRuleManager().getTddlRule().getVersionedTableNames().clear();
                    }
                }

                RelNode newLogicalPlan = Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
                RelNode finalPlan =
                    Planner.getInstance().optimizeByPlanEnumerator(unOptimizedPlan, newLogicalPlan, plannerContext);
                RelOptCost finalCost = mq.getCumulativeCost(finalPlan);
                Configuration finalConfiguration =
                    new Configuration(new HashSet<>(), finalCost, initContext.broadcastTables);

                if (lessThan(finalCost, originalCost)) {
                    adviceResult.setConfiguration(finalConfiguration);
                    adviceResult.setAfterPlan(finalPlan, "\n" + RelUtils
                        .toString(finalPlan, executionContext.getParams().getCurrentParameter(),
                            RexUtils.getEvalFunc(executionContext), executionContext));
                    endWhatIf(initContext);
                    adviceResult.setInfo(adviseType.name());
                    return adviceResult;
                }
                endWhatIf(initContext);
            }
        } else {
            // find indexable column
            IndexableColumnSet indexableColumnSet = null;
            if (adviseType == AdviseType.COLUMNAR_INDEX) {
                //行存rbo有join下推，所以使用列存rbo的结果分析indexableColumn
                RelNode columnRbo = Planner.getInstance()
                    .optimizeByColumnarRBO(unOptimizedPlan, executionContext.getParamManager(), plannerContext, false);
                indexableColumnSet = analyzeIndexableColumn(columnRbo, adviseType);
            } else {
                indexableColumnSet = analyzeIndexableColumn(logicalPlan, adviseType);
            }

            // find coverable column
            coverableColumnSet = analyzeCoverableColumn(logicalPlan, adviseType);

            // find partition rule
            partitionRuleSet = analyzePartitionRule(indexableColumnSet, adviseType);

            partitionPolicyMap = analyzePartitionPolicyMap(partitionRuleSet, adviseType);

            // find candidate index
            Map<String, Map<String, Set<CandidateIndex>>> candidateMap =
                buildCandidateIndex(indexableColumnSet, adviseType);

            //对于列存索引推荐，其cost的基准不应该来自行存执行计划（目前行列计划的代价不可比较），所以构建默认CCI，作为列存索引推荐的基准
            Set<CandidateIndex> initCCICandidateSet = null;
            RelNode initCCIPlan = null;
            if (adviseType == AdviseType.COLUMNAR_INDEX) {
                //生成默认CCI下的Cost基准
                initCCICandidateSet = new HashSet<>();
                addDefaultCci(initCCICandidateSet);
                //强制进入列存优化器
                plannerContext.getParamManager().getProps()
                    .put(ConnectionProperties.OPTIMIZER_TYPE, OptimizerType.COLUMNAR.name());
                //打开列存索引代价估算
                plannerContext.getParamManager().getProps().put(ConnectionProperties.ENABLE_COLUMNAR_SCAN_COST, "true");
                WhatIfContext whatIfContext = beginWhatIf(unOptimizedPlan, initCCICandidateSet);
                initCCIPlan =
                    Planner.getInstance().optimizeByPlanEnumerator(unOptimizedPlan, logicalPlan, plannerContext);
                originalCost = mq.getCumulativeCost(initCCIPlan);

                LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
                initCCIPlan.accept(logicalViewFinder);
                ossTableScanCostMap = new HashMap<>();
                for (LogicalView logicalView : logicalViewFinder.getResult()) {
                    OSSTableScan ossTableScan = (OSSTableScan) logicalView;
                    ossTableScanCostMap.putIfAbsent(ossTableScan.getSchemaName(), new HashMap<>());
                    String tbName = executionContext.getSchemaManager(ossTableScan.getSchemaName())
                        .getTable(ossTableScan.getTableNames().get(0)).getGsiTableMetaBean().gsiMetaBean.tableName;
                    ossTableScanCostMap.get(ossTableScan.getSchemaName())
                        .put(tbName, Pair.of(ossTableScan, mq.getCumulativeCost(ossTableScan)));
                }

                endWhatIf(whatIfContext);
                adviceResult = new AdviceResult(originalCost, initCCIPlan);
            }

            // enumerate configuration
            this.finalCandidateSet = enumerateConfiguration(candidateMap, logicalPlan, originalCost);

            if (adviseType == AdviseType.COLUMNAR_INDEX) {
                //因为cci的枚举是根据indexColumn生成的，如果有些表没有indexColumn，则enumerateConfiguration就没有为其生成cci，但是目前要求列存查询每张表都必须有cci
                finalCandidateSet = addDefaultCci(finalCandidateSet);
                WhatIfContext whatIfContext = beginWhatIf(unOptimizedPlan, finalCandidateSet);
                this.finalPlan =
                    Planner.getInstance().optimizeByPlanEnumerator(unOptimizedPlan, logicalPlan, plannerContext);
                this.finalCost = mq.getCumulativeCost(finalPlan);
                endWhatIf(whatIfContext);

                //枚举分区键
                enumeratePartColumn(new ArrayList<>(finalCandidateSet), indexableColumnSet, logicalPlan,
                    executionContext.getParamManager().getBoolean(ConnectionParams.CCI_ADVISOR_FAST_ENUMERATION),
                    executionContext.getParamManager().getBoolean(ConnectionParams.CCI_ADVISOR_PREFER_PARTITION_WISE),
                    finalCost);

            } else {
                WhatIfContext whatIfContext = beginWhatIf(logicalPlan, finalCandidateSet);
                finalPlan =
                    Planner.getInstance().optimizeByPlanEnumerator(unOptimizedPlan, logicalPlan, plannerContext);
                finalCost = mq.getCumulativeCost(finalPlan);
                endWhatIf(whatIfContext);
            }

            if (lessThan(finalCost, originalCost) || (adviseType == AdviseType.COLUMNAR_INDEX && looseLessThan(
                finalCost, originalCost))) {
                Configuration finalConfiguration = new Configuration(finalCandidateSet, finalCost);
                adviceResult.setConfiguration(finalConfiguration);
                WhatIfContext ctx = beginWhatIf(finalPlan, finalCandidateSet);
                adviceResult.setAfterPlan(finalPlan, "\n" + RelUtils
                    .toString(finalPlan, executionContext.getParams().getCurrentParameter(),
                        RexUtils.getEvalFunc(executionContext), executionContext));
                endWhatIf(ctx);
                adviceResult.setInfo(adviseType.name());
                return adviceResult;
            }

            //对于列存索引推荐，默认CCI可能也是最佳的索引结构
            if (adviseType == AdviseType.COLUMNAR_INDEX) {
                finalCandidateSet = initCCICandidateSet;
                finalPlan = initCCIPlan;
                finalCost = originalCost;
                Configuration finalConfiguration = new Configuration(finalCandidateSet, finalCost);
                adviceResult.setConfiguration(finalConfiguration);
                WhatIfContext ctx = beginWhatIf(finalPlan, finalCandidateSet);
                adviceResult.setAfterPlan(finalPlan, "\n" + RelUtils
                    .toString(finalPlan, executionContext.getParams().getCurrentParameter(),
                        RexUtils.getEvalFunc(executionContext), executionContext));
                endWhatIf(ctx);
                adviceResult.setInfo(adviseType.name());
                return adviceResult;
            }
        }

        Set<Pair<String, String>> tableSet = PlanManagerUtil.getTableSetFromAst(executionPlan.getAst());
        String defaultSchemaName = executionContext.getSchemaName();
        StringBuilder infoBuilder = new StringBuilder();
        infoBuilder.append("Advisor can't not find any useful index. You can try to analyze tables to refresh ");
        infoBuilder.append("statistics during the low business period by ");
        infoBuilder.append("`ANALYZE TABLE ");
        infoBuilder.append(tableSet.stream().map(pair -> {
            String schemaName = pair.getKey() == null ? defaultSchemaName : pair.getKey();
            String tableName = pair.getValue();
            return String.format(ANALYZE_FORMAT, schemaName, tableName);
        }).collect(Collectors.joining(",")));
        infoBuilder.append("`");
        adviceResult.setInfo(infoBuilder.toString());
        return adviceResult;
    }

    private IndexableColumnSet analyzeIndexableColumn(RelNode logicalPlan, AdviseType adviseType) {
        IndexableColumnRelFinder indexableColumnRelFinder = new IndexableColumnRelFinder(mq, adviseType);
        indexableColumnRelFinder.go(logicalPlan);
        IndexableColumnSet indexableColumnSet = indexableColumnRelFinder.getIndexableColumnSet();
        return indexableColumnSet;
    }

    private CoverableColumnSet analyzeCoverableColumn(RelNode logicalPlan, AdviseType adviseType) {
        if (adviseType == AdviseType.COLUMNAR_INDEX) {
            return null;
        }
        CoverableColumnRelFinder coverableColumnRelFinder = new CoverableColumnRelFinder(mq);
        coverableColumnRelFinder.go(logicalPlan);
        CoverableColumnSet coverableColumnSet = coverableColumnRelFinder.getCoverableColumnSet();
        return coverableColumnSet;
    }

    private PartitionRuleSet analyzePartitionRule(IndexableColumnSet indexableColumnSet, AdviseType adviseType) {
        PartitionRuleSet partitionRuleSet = new PartitionRuleSet();
        for (String schemaName : indexableColumnSet.m.keySet()) {
            for (String tableName : indexableColumnSet.m.get(schemaName).keySet()) {

                PartitionInfoManager partitionInfo =
                    executionContext.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager();
                if (partitionInfo.isNewPartDbTable(tableName)) {
                    HumanReadableRule humanReadableRule =
                        HumanReadableRule.getHumanReadableRule(partitionInfo.getPartitionInfo(tableName));
                    partitionRuleSet.addPartitionRule(schemaName, tableName, humanReadableRule);
                } else {
                    TableRule tableRule =
                        executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableRule(tableName);
                    HumanReadableRule humanReadableRule = HumanReadableRule.getHumanReadableRule(tableRule);
                    partitionRuleSet.addPartitionRule(schemaName, tableName, humanReadableRule);
                    // gsi
                    List<String> gsiList = GlobalIndexMeta
                        .getPublishedIndexNames(tableName, schemaName, executionContext);
                    for (String gsi : gsiList) {
                        tableRule =
                            executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableRule(gsi);
                        humanReadableRule = HumanReadableRule.getHumanReadableRule(tableRule);
                        partitionRuleSet.addPartitionRule(schemaName, tableName, humanReadableRule);
                    }
                }
            }
        }
        return partitionRuleSet;
    }

    private Map<String, Collection<HumanReadableRule>> analyzePartitionPolicyMap(PartitionRuleSet partitionRuleSet,
                                                                                 AdviseType adviseType) {
        Map<String, Collection<HumanReadableRule>> result = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (String schemaName : partitionRuleSet.m.keySet()) {
            result.put(schemaName.toLowerCase(), partitionRuleSet.getPartitionPolicys(schemaName));
        }
        return result;
    }

    private Map<String, Map<String, Set<CandidateIndex>>> buildCandidateIndex(IndexableColumnSet indexableColumnSet,
                                                                              AdviseType adviseType) {
        int MAX_MULTI_COLUMN_INDEX_SIZE = 2;
        if (adviseType == AdviseType.COLUMNAR_INDEX) {
            MAX_MULTI_COLUMN_INDEX_SIZE = 1;
        }
        // First, consider every single column index
        Set<CandidateIndex> singleColumnCandidateIndexSet = getSingleColumnCandidateIndexSet(indexableColumnSet);
        Set<CandidateIndex> candidateIndexLevelOne = new HashSet<>();
        List<Set<CandidateIndex>> candidateIndexLevel = new ArrayList<>();

        for (CandidateIndex singleColumnCandidateIndex : singleColumnCandidateIndexSet) {
            selectCandidateIndex(singleColumnCandidateIndex, candidateIndexLevelOne);
        }

        // Second, consider multi column index
        candidateIndexLevel.add(candidateIndexLevelOne);
        int currentLevel = 2;
        while (currentLevel <= MAX_MULTI_COLUMN_INDEX_SIZE) {
            Set<CandidateIndex> currentLevelCandidateIndexSet = new HashSet<>();
            int lastLevelidx = currentLevel - 2;
            Set<CandidateIndex> lastLevelCandidateIndexSet = candidateIndexLevel.get(lastLevelidx);
            // currentLevel = lastLevel + levelOne
            for (CandidateIndex lastLevelCandidateIndex : lastLevelCandidateIndexSet) {
                Set<String> hasLevelUp = new HashSet<>();
                for (CandidateIndex levelOneCandidateIndex : candidateIndexLevelOne) {
                    if (lastLevelCandidateIndex.isContainGsiPartColOfUnsupportedDataType()
                        || levelOneCandidateIndex.isContainGsiPartColOfUnsupportedDataType()) {
                        continue;
                    }
                    if (!hasLevelUp.add(levelOneCandidateIndex.getColumnNames().get(0))) {
                        continue;
                    }
                    Set<CandidateIndex> levelUpIndexSet = levelUp(lastLevelCandidateIndex, levelOneCandidateIndex);
                    if (levelUpIndexSet == null) {
                        continue;
                    }
                    for (CandidateIndex currentLevelCandidateIndex : levelUpIndexSet) {
                        selectCandidateIndex(currentLevelCandidateIndex, currentLevelCandidateIndexSet);
                    }
                }

            }
            candidateIndexLevel.add(currentLevelCandidateIndexSet);
            currentLevel++;
        }

        // schema -> table -> candidateIndex
        Map<String, Map<String, Set<CandidateIndex>>> candidateMap = new HashMap<>();

        for (Set<CandidateIndex> candidateIndexSet : candidateIndexLevel) {
            for (CandidateIndex candidateIndex : candidateIndexSet) {
                String schemaName = candidateIndex.getSchemaName().toLowerCase();
                String tableName = candidateIndex.getTableName().toLowerCase();
                Map<String, Set<CandidateIndex>> tableMap = candidateMap.get(schemaName);
                if (tableMap == null) {
                    tableMap = new HashMap<>();
                    candidateMap.put(schemaName, tableMap);
                }
                Set<CandidateIndex> notExistsCandidateSet = tableMap.get(tableName);
                if (notExistsCandidateSet == null) {
                    notExistsCandidateSet = new HashSet<>();
                    tableMap.put(tableName, notExistsCandidateSet);
                }
                if (!candidateIndex.alreadyExist()) {
                    notExistsCandidateSet.add(candidateIndex);
                }
            }
        }

        return candidateMap;
    }

    private Set<CandidateIndex> enumerateConfiguration
        (Map<String, Map<String, Set<CandidateIndex>>> candidateMap,
         RelNode logicalPlan, RelOptCost originalCost) {

        // configuration enumeration - greedy search
        // schema.table.configuration independent property
        // we enumerate every schema.table best configuration and then add them together
        // best configuration with max-cost-improve and min-candidateIndex-num
        int MAX_INDEX_PER_CONFIGURATION = 2;
        if (adviseType == AdviseType.GLOBAL_COVERING_INDEX || adviseType == AdviseType.GLOBAL_INDEX
            || adviseType == AdviseType.COLUMNAR_INDEX) {
            MAX_INDEX_PER_CONFIGURATION = 1;
        }

        for (int combination = 1; combination <= MAX_INDEX_PER_CONFIGURATION; combination++) {
            for (Map<String, Set<CandidateIndex>> tableMap : candidateMap.values()) {
                for (Set<CandidateIndex> notExistsCandidateSet : tableMap.values()) {
                    int combinationSize = Math.min(notExistsCandidateSet.size(), combination);
                    Set<Set<CandidateIndex>> combinationSet =
                        Sets.combinations(notExistsCandidateSet, combinationSize);
                    for (Set<CandidateIndex> candidateIndexSet : combinationSet) {
                        if (adviseType == AdviseType.COLUMNAR_INDEX) {
                            tryConfigurationForCCIByScanCost(logicalPlan, originalCost, candidateIndexSet);
                        } else {
                            tryConfiguration(logicalPlan, originalCost, candidateIndexSet);
                        }
                    }
                }
            }
        }

        // In GSI case, we suppose only one Table GSI is the most significant index.
        // (To avoid table partition policy change each other for join, which is high cost for maintaining GSI)
        // so finalCandidateSet should only contain one table Configuration for GSI case.
        Set<CandidateIndex> finalCandidateSet = new HashSet<>();
        if (adviseType == AdviseType.GLOBAL_COVERING_INDEX || adviseType == AdviseType.GLOBAL_INDEX) {
            Configuration bestTableConfiguration = null;
            for (Map<String, Configuration> tableConfiguration : configurationMap.values()) {
                for (Configuration configuration : tableConfiguration.values()) {
                    if (bestTableConfiguration == null) {
                        bestTableConfiguration = configuration;
                    } else if (configuration.getAfterCost().isLt(bestTableConfiguration.getAfterCost())) {
                        bestTableConfiguration = configuration;
                    }
                }
            }
            if (bestTableConfiguration != null) {
                finalCandidateSet.addAll(bestTableConfiguration.getCandidateIndexSet());
            }
        } else {
            for (Map<String, Configuration> tableConfiguration : configurationMap.values()) {
                for (Configuration configuration : tableConfiguration.values()) {
                    finalCandidateSet.addAll(configuration.getCandidateIndexSet());
                }
            }
        }

        return finalCandidateSet;
    }

    private boolean tryConfigurationForCCIByScanCost(RelNode logicalPlan, RelOptCost originalCost,
                                                     Set<CandidateIndex> candidateIndexSet) {
        CandidateIndex candidateIndex = candidateIndexSet.iterator().next();
        Pair<OSSTableScan, RelOptCost> ossPair =
            ossTableScanCostMap.get(candidateIndex.getSchemaName()).get(candidateIndex.getTableName());
        WhatIfContext whatIfContext =
            beginWhatIf(plannerContext.getExecutionContext().getUnOptimizedPlan(), candidateIndexSet);
        OSSTableScan oldScan = ossPair.getKey();
        final RelOptSchema catalog = RelUtils.buildCatalogReader(candidateIndex.getSchemaName(), executionContext);
        final RelOptTable indexTable =
            catalog.getTableForMember(ImmutableList.of(candidateIndex.getSchemaName(), candidateIndex.getIndexName()));
        LogicalTableScan logicalTableScan =
            LogicalTableScan.create(oldScan.getCluster(), indexTable, oldScan.getHints(), null, null, null, null);
        OSSTableScan newScan = new OSSTableScan(logicalTableScan, oldScan.getLockMode());
        newScan =
            (OSSTableScan) newScan.copy(newScan.getTraitSet(), oldScan.getPushedRelNode().accept(new RelShuttleImpl() {
                @Override
                public RelNode visit(TableScan scan) {
                    return logicalTableScan;
                }
            }));

        RelOptCost newCost = mq.getCumulativeCost(newScan);
        endWhatIf(whatIfContext);
        if (newCost.isLe(ossPair.getValue())) {
            ossTableScanCostMap.get(candidateIndex.getSchemaName())
                .put(candidateIndex.getTableName(), Pair.of(ossPair.getKey(), newCost));
            Configuration configuration = new Configuration(candidateIndexSet, newCost);
            String schemaName = candidateIndexSet.iterator().next().getSchemaName().toLowerCase();
            String tableName = candidateIndexSet.iterator().next().getTableName().toLowerCase();
            Map<String, Configuration> tableConfigurationMap = configurationMap.get(schemaName);
            if (tableConfigurationMap == null) {
                tableConfigurationMap = new HashMap<>();
                configurationMap.put(schemaName, tableConfigurationMap);
            }
            tableConfigurationMap.put(tableName, configuration);
            return true;
        }
        return false;
    }

    /**
     * 一个表增加列存索引
     */
    private boolean tryConfigurationForCCIByOptimizer(RelNode logicalPlan, RelOptCost originalCost,
                                                      Set<CandidateIndex> candidateIndexSet) {

        //补充默认CCI
        Set<CandidateIndex> allCci = new HashSet<>(candidateIndexSet);
        allCci = addDefaultCci(allCci);

        WhatIfContext whatIfContext = beginWhatIf(plannerContext.getExecutionContext().getUnOptimizedPlan(), allCci);

        //impossible to happen
        if (!CBOUtil.allTablesHaveColumnarIndex(plannerContext.getExecutionContext().getUnOptimizedPlan(),
            plannerContext.getExecutionContext())) {
            throw new RuntimeException("not all table has columnar");
        }

        RelNode physicalPlan = Planner.getInstance()
            .optimizeByPlanEnumerator(plannerContext.getExecutionContext().getUnOptimizedPlan(), logicalPlan,
                plannerContext);

        RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
        endWhatIf(whatIfContext);
        // make the plan cost lower!
        if (newCost.isLe(originalCost)) {
            Configuration configuration = new Configuration(candidateIndexSet, newCost);
            String schemaName = candidateIndexSet.iterator().next().getSchemaName().toLowerCase();
            String tableName = candidateIndexSet.iterator().next().getTableName().toLowerCase();
            Map<String, Configuration> tableConfigurationMap = configurationMap.get(schemaName);
            if (tableConfigurationMap == null) {
                tableConfigurationMap = new HashMap<>();
                configurationMap.put(schemaName, tableConfigurationMap);
            }
            Configuration oldConfiguration = tableConfigurationMap.get(tableName);
            if (oldConfiguration == null || configuration.getAfterCost().isLt(oldConfiguration.getAfterCost())) {
                tableConfigurationMap.put(tableName, configuration);
            } else if (configuration.getAfterCost().equals(oldConfiguration.getAfterCost())) {
                //选取索引列少的索引
                if (configuration.getCandidateIndexSet().iterator().next().getColumnNames().size()
                    < oldConfiguration.getCandidateIndexSet().iterator().next().getColumnNames().size()) {
                    tableConfigurationMap.put(tableName, configuration);
                }
            }
            return true;
        }
        return false;
    }

    private Set<CandidateIndex> addDefaultCci(Set<CandidateIndex> candidateIndexSet) {
        Set<Pair<String, String>> tableSet = executionPlan.getTableSet();
        for (Pair<String, String> tablePair : tableSet) {
            if (tablePair.getKey() == null) {
                tablePair = Pair.of(executionContext.getSchemaName(), tablePair.getValue());
            }
            boolean addDefaultCci = true;
            for (CandidateIndex candidateIndex : candidateIndexSet) {
                if (candidateIndex.getSchemaName().equalsIgnoreCase(tablePair.getKey()) && candidateIndex.getTableName()
                    .equalsIgnoreCase(tablePair.getValue())) {
                    addDefaultCci = false;
                    break;
                }
            }
            if (addDefaultCci) {
                TableMeta tableMeta = OptimizerContext.getContext(tablePair.getKey()).getLatestSchemaManager()
                    .getTable(tablePair.getValue());
                List<String> defaultCciSortColumns =
                    tableMeta.getPrimaryIndex().getKeyColumns().stream().map(ColumnMeta::getName)
                        .collect(Collectors.toList());
                if (defaultCciSortColumns.size() > 1) {
                    defaultCciSortColumns = Collections.singletonList(defaultCciSortColumns.get(0));
                }
                List<String> defaultCciPartColumns = tableMeta.getPartitionInfo().getPartitionColumns();
                if (defaultCciPartColumns == null || defaultCciPartColumns.isEmpty()) {
                    defaultCciPartColumns = Collections.singletonList(defaultCciSortColumns.get(0));
                } else {
                    defaultCciPartColumns = Collections.singletonList(defaultCciPartColumns.get(0));
                }
                CandidateIndex candidateIndex = new CandidateIndex(tablePair.getKey(), tablePair.getValue(),
                    defaultCciSortColumns, defaultCciPartColumns, true, true,
                    CoverableColumnSet.getCCICoverableColumns(tablePair.getKey(), tablePair.getValue(),
                        defaultCciSortColumns));
                candidateIndexSet.add(candidateIndex);
            }
        }
        return candidateIndexSet;
    }

    private void enumeratePartColumn(List<CandidateIndex> candidateIndexList, IndexableColumnSet indexableColumnSet,
                                     RelNode logicalPlan, boolean fast, boolean preferPartitionWise,
                                     RelOptCost nowCost) {
        if (!fast) {
            enumerateAllPartColumn(candidateIndexList, indexableColumnSet, logicalPlan, 0);
            return;
        }
        fastEnumeratePartColumn(candidateIndexList, indexableColumnSet, logicalPlan, preferPartitionWise, nowCost);
    }

    /**
     * 1. 根据join关系对所有的表进行分区，具有join关系的所有表为一个JoinTableGroup <=> Set<Table>
     * 2. 根据join关系，对所有的join列进行分区，具有等值关系的join列为一个JoinColumnGroup <=> Set<Column>
     * 3. 基于JoinTableGroup，对所有的JoinColumnGroup按照所属的表再分组，很明显一个JoinTableGroup =》 List<JoinColumnGroup>
     * 4. 对于每一个JoinTableGroup，尝试将其对应的List<JoinColumnGroup>进行merge操作，即JoinColumnGroup之间没有表重叠的情况时，合并成一个JoinColumnGroup
     * 5. JoinColumnGroup包含的列越多，说明join分区对齐的程度越大，优先选择join分区对齐度高的JoinColumnGroup
     * 6. 对于每一个JoinTableGroup, 基于其对应的JoinColumnGroup，选择cci的分区键进行优化器计算代价，代价最小的JoinColumnGroup确定cci的分区键
     * 7. 每个JoinTableGroup的分区键枚举是互不影响的，所以最后聚合所有JoinTableGroup的分区键枚举结果
     */
    private void fastEnumeratePartColumn(List<CandidateIndex> candidateIndexList, IndexableColumnSet indexableColumnSet,
                                         RelNode logicalPlan, boolean preferPartitionWise,
                                         RelOptCost nowCost) {
        Map<Table, Integer> tableIndexMap = new HashMap<>();
        for (int i = 0; i < candidateIndexList.size(); i++) {
            CandidateIndex candidateIndex = candidateIndexList.get(i);
            String schemaName = candidateIndex.getSchemaName();
            String tableName = candidateIndex.getTableName();
            tableIndexMap.put(Table.of(schemaName, tableName), i);
        }
        //1.
        List<Set<Table>> joinTableGroupList = indexableColumnSet.joinTbs.getAllGroup();
        //2.
        List<Set<Column>> allJoinColGroupList = indexableColumnSet.joinCols.getAllGroup();

        //3.
        List<List<Set<Column>>> JoinColumnGroupListForAllTable = new ArrayList<>(joinTableGroupList.size());
        for (int i = 0; i < joinTableGroupList.size(); i++) {
            Set<Table> joinTableGroup = joinTableGroupList.get(i);
            JoinColumnGroupListForAllTable.add(new ArrayList<>());
            loop:
            for (Set<Column> joinColGroup : allJoinColGroupList) {
                for (Column joinCol : joinColGroup) {
                    if (joinTableGroup.contains(joinCol.getTable())) {
                        JoinColumnGroupListForAllTable.get(i).add(joinColGroup);
                        continue loop;
                    }
                }
            }
        }

        //4. 对不重叠的join列组合进行合并，最大化join分区对齐
        for (int i = 0; i < JoinColumnGroupListForAllTable.size(); i++) {
            List<Set<Column>> joinColumnGroupList = JoinColumnGroupListForAllTable.get(i);
            List<Set<Column>> newJoinColumnGroupList = new ArrayList<>();
            mergeJoinColumnGroup(joinColumnGroupList, newJoinColumnGroupList, new HashSet<>(), new HashSet<>(), 0);
            //5.
            newJoinColumnGroupList.sort((o1, o2) -> Integer.compare(o2.size(), o1.size()));
            int maxSize = 5;
            int endIndex = 2;
            for (; endIndex < newJoinColumnGroupList.size() && endIndex < maxSize; endIndex++) {
                if (newJoinColumnGroupList.get(endIndex).size() + newJoinColumnGroupList.get(endIndex - 1).size()
                    <= newJoinColumnGroupList.get(endIndex - 2).size()) {
                    break;
                }
            }
            JoinColumnGroupListForAllTable.set(i, endIndex > newJoinColumnGroupList.size() ? newJoinColumnGroupList :
                newJoinColumnGroupList.subList(0, endIndex));
        }

        //6. 对每一个join表组，独立的进行join列分区对齐枚举
        for (int i = 0; i < joinTableGroupList.size(); i++) {
            Set<Table> joinTableGroup = joinTableGroupList.get(i);
            if (joinTableGroup.size() == 1) {
                continue;
            }
            List<Set<Column>> joinColumnGroupList = JoinColumnGroupListForAllTable.get(i);
            //如果优先考虑PartitionWise，则不与当前的final cost进行比较，这样一定会选择join分区对齐的方式
            RelOptCost bestJoinTableGroupCost = preferPartitionWise ? null : nowCost;
            Set<Column> bestJoinColumnGroup = null;
            List<CandidateIndex> bestJoinCandidateIndices = null;
            for (Set<Column> joinColumnGroup : joinColumnGroupList) {
                List<CandidateIndex> joinCandidateIndexList = new ArrayList<>(candidateIndexList);
                for (Column joinCol : joinColumnGroup) {
                    int index = tableIndexMap.get(joinCol.getTable());
                    CandidateIndex oldCandidateIndex = joinCandidateIndexList.get(index);
                    if (oldCandidateIndex.getPartColumns().get(0).equalsIgnoreCase(joinCol.getColumnName())) {
                        continue;
                    }
                    CandidateIndex newCandidateIndex = new CandidateIndex(
                        oldCandidateIndex.getSchemaName(), oldCandidateIndex.getTableName(),
                        oldCandidateIndex.getColumnNames(), Arrays.asList(joinCol.getColumnName()), true, true,
                        oldCandidateIndex.getCoveringColumns()
                    );
                    joinCandidateIndexList.set(index, newCandidateIndex);
                }

                Set<CandidateIndex> joinCandidateIndexSet = new HashSet<>(joinCandidateIndexList);
                WhatIfContext whatIfContext =
                    beginWhatIf(plannerContext.getExecutionContext().getUnOptimizedPlan(), joinCandidateIndexSet);
                RelNode physicalPlan = Planner.getInstance()
                    .optimizeByPlanEnumerator(plannerContext.getExecutionContext().getUnOptimizedPlan(), logicalPlan,
                        plannerContext);
                RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
                endWhatIf(whatIfContext);
                //更新candidateIndexList
                if (bestJoinTableGroupCost == null || looseLessThan(newCost, bestJoinTableGroupCost)) {
                    bestJoinTableGroupCost = newCost;
                    bestJoinColumnGroup = joinColumnGroup;
                    bestJoinCandidateIndices = joinCandidateIndexList;
                }
            }

            if (bestJoinColumnGroup != null) {
                for (Column col : bestJoinColumnGroup) {
                    int index = tableIndexMap.get(col.getTable());
                    candidateIndexList.set(index, bestJoinCandidateIndices.get(index));
                }
            }
        }

        //7. final check
        Set<CandidateIndex> candidateIndexSet = new HashSet<>(candidateIndexList);
        tryReplaceFinalCandidateSet(plannerContext.getExecutionContext().getUnOptimizedPlan(), logicalPlan,
            candidateIndexSet, preferPartitionWise);
    }

    private void mergeJoinColumnGroup(List<Set<Column>> joinColumnGroupList,
                                      List<Set<Column>> newJoinColumnGroupList,
                                      Set<Table> tmpJoinTables,
                                      Set<Column> tmpJoinCols,
                                      int i) {
        if (i >= joinColumnGroupList.size()) {
            if (tmpJoinCols.size() > 0) {
                newJoinColumnGroupList.add(new HashSet<>(tmpJoinCols));
            }
            return;
        }

        mergeJoinColumnGroup(joinColumnGroupList, newJoinColumnGroupList, tmpJoinTables, tmpJoinCols, i + 1);

        Set<Column> joinCols = joinColumnGroupList.get(i);
        for (Column joinCol : joinCols) {
            if (tmpJoinTables.contains(joinCol.getTable())) {
                return;
            }
        }
        tmpJoinCols.addAll(joinCols);
        for (Column joinCol : joinCols) {
            tmpJoinTables.add(joinCol.getTable());
        }
        mergeJoinColumnGroup(joinColumnGroupList, newJoinColumnGroupList, tmpJoinTables, tmpJoinCols, i + 1);
        tmpJoinCols.removeAll(joinCols);
        for (Column joinCol : joinCols) {
            tmpJoinTables.remove(joinCol.getTable());
        }

    }

    private void enumerateAllPartColumn(List<CandidateIndex> candidateIndexList, IndexableColumnSet indexableColumnSet,
                                        RelNode logicalPlan, int i) {
        if (i >= candidateIndexList.size()) {
            Set<CandidateIndex> candidateIndexSet = new HashSet<>(candidateIndexList);
            tryReplaceFinalCandidateSet(plannerContext.getExecutionContext().getUnOptimizedPlan(), logicalPlan,
                candidateIndexSet, false);
            return;
        }

        CandidateIndex oldCandidateIndex = candidateIndexList.get(i);
        if (indexableColumnSet.partColumns.containsKey(oldCandidateIndex.getSchemaName())
            && indexableColumnSet.partColumns.get(oldCandidateIndex.getSchemaName())
            .containsKey(oldCandidateIndex.getTableName())) {
            for (String partCol : indexableColumnSet.partColumns.get(oldCandidateIndex.getSchemaName())
                .get(oldCandidateIndex.getTableName())) {
                CandidateIndex newCandidateIndex = new CandidateIndex(
                    oldCandidateIndex.getSchemaName(), oldCandidateIndex.getTableName(),
                    oldCandidateIndex.getColumnNames(), Arrays.asList(partCol), true, true,
                    oldCandidateIndex.getCoveringColumns()
                );
                candidateIndexList.set(i, newCandidateIndex);
                enumerateAllPartColumn(candidateIndexList, indexableColumnSet, logicalPlan, i + 1);
            }
        } else {
            enumerateAllPartColumn(candidateIndexList, indexableColumnSet, logicalPlan, i + 1);
        }
        candidateIndexList.set(i, oldCandidateIndex);
    }

    /**
     * all candidateIndex in candidateIndexSet must be with the same schema.table
     */
    private boolean tryConfiguration(RelNode logicalPlan, RelOptCost originalCost,
                                     Set<CandidateIndex> candidateIndexSet) {

        if (isConfigurationRedundant(candidateIndexSet)) {
            return false;
        }

        WhatIfContext whatIfContext = beginWhatIf(logicalPlan, candidateIndexSet);
        RelNode physicalPlan = Planner.getInstance().optimizeByPlanEnumerator(logicalPlan, logicalPlan, plannerContext);
        RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
        endWhatIf(whatIfContext);
        // make the plan cost lower!
        if (lessThan(newCost, originalCost)) {
            Configuration configuration = new Configuration(candidateIndexSet, newCost);
            String schemaName = candidateIndexSet.iterator().next().getSchemaName().toLowerCase();
            String tableName = candidateIndexSet.iterator().next().getTableName().toLowerCase();
            Map<String, Configuration> tableConfigurationMap = configurationMap.get(schemaName);
            if (tableConfigurationMap == null) {
                tableConfigurationMap = new HashMap<>();
                configurationMap.put(schemaName, tableConfigurationMap);
            }
            Configuration oldConfiguration = tableConfigurationMap.get(tableName);
            if (oldConfiguration == null) {
                tableConfigurationMap.put(tableName, configuration);
            } else if (configuration.getAfterCost().isLt(oldConfiguration.getAfterCost().multiplyBy(1.01))
                && oldConfiguration.getAfterCost().isLt(configuration.getAfterCost().multiplyBy(1.01))) {
                // 1% range, we choose small candidateIndexSet size and small index length
                if (configuration.getCandidateIndexSet().size() < oldConfiguration.getCandidateIndexSet().size()) {
                    tableConfigurationMap.put(tableName, configuration);
                } else if (configuration.getCandidateIndexSet().size()
                    == oldConfiguration.getCandidateIndexSet().size()
                    && configuration.totalIndexLength() < oldConfiguration.totalIndexLength()) {
                    tableConfigurationMap.put(tableName, configuration);
                }
            } else if (configuration.getAfterCost().isLt(oldConfiguration.getAfterCost().multiplyBy(0.99))) {
                tableConfigurationMap.put(tableName, configuration);
            }
            return true;
        }
        return false;
    }

    private void tryReplaceFinalCandidateSet(RelNode originInput, RelNode input, Set<CandidateIndex> candidateIndexSet,
                                             boolean force) {
        WhatIfContext whatIfContext =
            beginWhatIf(plannerContext.getExecutionContext().getUnOptimizedPlan(), candidateIndexSet);
        RelNode physicalPlan = Planner.getInstance()
            .optimizeByPlanEnumerator(plannerContext.getExecutionContext().getUnOptimizedPlan(), input,
                plannerContext);
        RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
        endWhatIf(whatIfContext);
        if (force || looseLessThan(newCost, finalCost)) {
            finalCost = newCost;
            finalPlan = physicalPlan;
            finalCandidateSet = candidateIndexSet;
        }
    }

    class WhatIfContext {

        public Map<String, SchemaManager> oldSchemaManagers;
        public List<Pair<String, TableScan>> tableScans;
        public Map<TableScan, TableMeta> oldMap;

        public WhatIfContext(
            Map<String, SchemaManager> oldSchemaManagers,
            List<Pair<String, TableScan>> tableScans,
            Map<TableScan, TableMeta> oldMap) {
            this.oldSchemaManagers = oldSchemaManagers;
            this.tableScans = tableScans;
            this.oldMap = oldMap;
        }
    }

    class BroadcastContext extends WhatIfContext {
        private Map<String, Set<String>> broadcastTables;

        public BroadcastContext(
            Map<String, SchemaManager> oldSchemaManagers,
            List<Pair<String, TableScan>> tableScans,
            Map<TableScan, TableMeta> oldMap,
            Map<String, Set<String>> broadcastTables) {
            super(oldSchemaManagers, tableScans, oldMap);
            this.broadcastTables = broadcastTables;
        }

    }

    class BroadcastInfo {
        TableRule rule;
        TableRule oldRule;
        PartitionInfoManager.PartInfoCtx infoCtx;

        BroadcastInfo(TableRule rule, TableRule oldRule) {
            this.rule = rule;
            this.oldRule = oldRule;
            infoCtx = null;
        }

        BroadcastInfo(PartitionInfoManager.PartInfoCtx infoCtx) {
            this.rule = null;
            this.oldRule = null;
            this.infoCtx = infoCtx;
        }

        void rollback(SchemaManager schemaManager, SchemaManager oldSchemaManager,
                      String tableName, TableMeta tableMeta) {
            if (infoCtx != null) {
                PartitionInfoManager.PartInfoCtx ctx = oldSchemaManager.
                    getTddlRuleManager().getPartitionInfoManager().getPartInfoCtx(tableName);
                schemaManager.getTddlRuleManager().getPartitionInfoManager().putPartInfoCtx(
                    tableName, ctx);
                tableMeta.setPartitionInfo(ctx.getPartInfo());
            } else {
                // rollback old table
                if (schemaManager.getTddlRuleManager().isBroadCast(tableName)) {
                    ((WhatIfTddlRuleManager) (schemaManager.getTddlRuleManager())).addTableRule(tableName,
                        oldRule);
                }
            }
        }

        void prepare(SchemaManager schemaManager, String tableName, TableMeta tableMeta) {
            if (infoCtx != null) {
                PartitionInfoManager partitionInfoManager = schemaManager.getTddlRuleManager()
                    .getPartitionInfoManager();
                partitionInfoManager.putPartInfoCtx(tableName, infoCtx);
                tableMeta.setPartitionInfo(infoCtx.getPartInfo());
            } else {
                ((WhatIfTddlRuleManager) (schemaManager.getTddlRuleManager())).addTableRule(tableName, rule);
            }
        }
    }

    /**
     * find all candidate broadcast tables which are small enough
     *
     * @param schemaManager read schemaManager
     * @param tables tables to be test
     * @return tables small enough
     */
    public Set<String> candidateBroadCastTable(SchemaManager schemaManager, Set<String> tables) {
        Set<String> candidateBroadCastTables = new TreeSet<>();
        for (String table : tables) {
            if (schemaManager.getTable(table).getRowCount(null) < executionContext.getParamManager().getInt(
                ConnectionParams.INDEX_ADVISOR_BROADCAST_THRESHOLD)) {
                candidateBroadCastTables.add(table);
            }
        }
        return candidateBroadCastTables;
    }

    public BroadcastInfo prepareInfo(String schemaName, String tableName,
                                     SchemaManager schemaManager,
                                     SchemaManager oldSchemaManager,
                                     TableMeta whatIfTableMeta,
                                     TableMeta oldTableMeta) {
        BroadcastInfo broadcastInfo = null;
        if (oldTableMeta.getRowCount(null) < executionContext.getParamManager().getInt(
            ConnectionParams.INDEX_ADVISOR_BROADCAST_THRESHOLD)) {
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {

                PartitionInfo partitionInfo = PartitionInfoBuilder
                    .buildPartitionInfoByPartDefAst(schemaName, tableName, null, false, null,
                        null, null,
                        new ArrayList<>(whatIfTableMeta.getPrimaryKey()),
                        whatIfTableMeta.getAllColumns(),
                        PartitionTableType.BROADCAST_TABLE,
                        executionContext);

                partitionInfo.setStatus(TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);

                PartitionInfoManager partitionInfoManager = schemaManager.getTddlRuleManager()
                    .getPartitionInfoManager();
                PartitionInfoManager.PartInfoCtx partInfoCtx =
                    new PartitionInfoManager.PartInfoCtx(partitionInfoManager,
                        tableName,
                        partitionInfo.getTableGroupId(),
                        partitionInfo);
                broadcastInfo = new BroadcastInfo(partInfoCtx);
            } else {

                // make sure old table was not broadcast
                if (!schemaManager.getTddlRuleManager().isBroadCast(tableName)) {
                    TableRule tableRule =
                        TableRuleBuilder.buildBroadcastTableRule(tableName, whatIfTableMeta);
                    broadcastInfo = new BroadcastInfo(tableRule,
                        oldSchemaManager.getTddlRuleManager().getTableRule(tableName));
                }
            }
        }
        return broadcastInfo;
    }

    private BroadcastContext initBroadcast(RelNode unOptimizedPlan, RelOptCost originalCost) {
        TableScanFinder tableScanFinder = new TableScanFinder();
        unOptimizedPlan.accept(tableScanFinder);
        Map<String, Map<String, List<TableScan>>> tableScanClass = tableScanFinder.getMappedResult(
            executionContext.getSchemaName());

        Map<String, Set<String>> candidateBroadCastTables = new TreeMap<>();
        Set<String> schemaNames = tableScanClass.keySet();

        Map<String, SchemaManager> oldSchemaManagers = executionContext.getSchemaManagers();
        Map<String, SchemaManager> whatIfSchemaManagers = new ConcurrentHashMap<>(oldSchemaManagers);
        executionContext.setSchemaManagers(whatIfSchemaManagers);
        for (String schemaName : schemaNames) {
            SchemaManager oldSchemaManager = oldSchemaManagers.get(schemaName);
            // build candidate table meta
            Set<String> tables = candidateBroadCastTable(oldSchemaManager, tableScanClass.get(schemaName).keySet());

            candidateBroadCastTables.put(schemaName, tables);

            WhatIfSchemaManager whatIfSchemaManager =
                new WhatIfSchemaManager(oldSchemaManager, new HashSet<>(), tables, executionContext);
            whatIfSchemaManager.init();
            executionContext.getSchemaManagers().put(schemaName, whatIfSchemaManager);
        }

        Map<String, Map<String, BroadcastInfo>> broadcastTableInfo = new HashMap<>(schemaNames.size());
        for (String schemaName : schemaNames) {
            broadcastTableInfo.put(schemaName, new HashMap<>());
        }

        boolean anyBroadcast = false;
        TableScanCounter tsc = new TableScanCounter();
        unOptimizedPlan.accept(tsc);
        int oldTableScanCount = tsc.getCount();
        // try all schemas
        for (Map.Entry<String, Map<String, List<TableScan>>> pair : tableScanClass.entrySet()) {
            String schemaName = pair.getKey();

            //try all tables
            for (Map.Entry<String, List<TableScan>> entry : pair.getValue().entrySet()) {
                String tableName = entry.getKey();
                if (!candidateBroadCastTables.get(schemaName).contains(tableName)) {
                    continue;
                }
                List<TableScan> tableScans = entry.getValue();
                BroadcastInfo broadcastInfo = null;

                //substitute all tableScan of the same table
                for (TableScan tableScan : tableScans) {
                    RelOptTable relOptTable = tableScan.getTable();
                    TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                    SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
                    TableMeta whatIfTableMeta = schemaManager.getTable(oldTableMeta.getTableName());
                    //don't broadcast gsi
                    if (whatIfTableMeta.isGsi()) {
                        continue;
                    }
                    // make sure the table is small enough to be broadcast
                    if (broadcastInfo == null) {
                        broadcastInfo =
                            prepareInfo(schemaName, tableName, schemaManager,
                                oldSchemaManagers.get(schemaName), whatIfTableMeta, oldTableMeta);
                    }
                    if (broadcastInfo != null) {
                        ((RelOptTableImpl) relOptTable).setImplTable(whatIfTableMeta);
                        broadcastInfo.prepare(schemaManager, tableName, whatIfTableMeta);
                    }
                }

                //check the result of making the table broadcast
                if (broadcastInfo != null) {
                    //clear old meta
                    whatIfSchemaManagers.get(schemaName).getTddlRuleManager().
                        getTddlRule().getVersionedTableNames().clear();

                    RelNode logicalPlan =
                        Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
                    RelNode physicalPlan =
                        Planner.getInstance().optimizeByPlanEnumerator(unOptimizedPlan, logicalPlan, plannerContext);
                    RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
                    if (logger.isDebugEnabled()) {
                        StringBuilder sb = new StringBuilder("\n");
                        sb.append("whatif rowcount ").append(mq.getRowCount(physicalPlan))
                            .append("\n");
                        sb.append(tableName).append("\n").
                            append(
                                RelUtils.toString(physicalPlan, executionContext.getParams().getCurrentParameter(),
                                    RexUtils.getEvalFunc(executionContext), executionContext))
                            .append(originalCost.toString()).append("\n").append(newCost.toString());
                    }

                    // smaller cost
                    if (lessThan(newCost, originalCost)) {
                        tsc = new TableScanCounter();
                        physicalPlan.accept(tsc);
                        // more tables are pushed down
                        if (tsc.getCount() < oldTableScanCount) {
                            anyBroadcast = true;
                            //record the useful table
                            if (!broadcastTableInfo.containsKey(schemaName)) {
                                broadcastTableInfo.put(schemaName, new HashMap<>());
                            }
                            broadcastTableInfo.get(schemaName).put(tableName, broadcastInfo);
                        }
                    }

                    // rollback
                    for (TableScan tableScan : tableScans) {
                        RelOptTable relOptTable = tableScan.getTable();
                        TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
                        TableMeta whatIfTableMeta = schemaManager.getTable(tableName);
                        if (whatIfTableMeta.isGsi()) {
                            continue;
                        }

                        ((RelOptTableImpl) relOptTable).setImplTable(oldTableMeta);
                        broadcastInfo.rollback(schemaManager, oldSchemaManagers.get(schemaName),
                            tableName, whatIfTableMeta);
                    }
                }

            }
        }

        //prepare executionContext with new broadcast
        Map<TableScan, TableMeta> oldMap = new HashMap<>();
        for (Pair<String, TableScan> pair : tableScanFinder.getResult()) {
            String schemaName = pair.getKey();
            TableScan tableScan = pair.getValue();
            RelOptTable relOptTable = tableScan.getTable();
            if ((relOptTable instanceof RelOptTableImpl)) {
                TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                oldMap.put(tableScan, oldTableMeta);
                TableMeta whatIfTableMeta =
                    whatIfSchemaManagers.get(schemaName).getTable(oldTableMeta.getTableName());
                ((RelOptTableImpl) relOptTable).setImplTable(whatIfTableMeta);

                String tableName = whatIfTableMeta.getTableName();
                if (broadcastTableInfo.get(schemaName).containsKey(tableName)) {
                    broadcastTableInfo.get(schemaName).get(tableName).prepare(
                        executionContext.getSchemaManager(schemaName), tableName, whatIfTableMeta);
                }
            }
        }
        if (!anyBroadcast) {
            return null;
        }

        Map<String, Set<String>> broadcastTables = new HashMap<>();
        for (Map.Entry<String, Map<String, BroadcastInfo>> entry : broadcastTableInfo.entrySet()) {
            broadcastTables.put(entry.getKey(), entry.getValue().keySet());
        }
        return new BroadcastContext(oldSchemaManagers, tableScanFinder.getResult(), oldMap, broadcastTables);
    }

    private WhatIfContext beginWhatIf(RelNode logicalPlan, Set<CandidateIndex> candidateIndexSet) {
        Set<String> schemaNames = candidateIndexSet.stream()
            .map(x -> x.getSchemaName().toLowerCase())
            .collect(Collectors.toSet());

        Map<String, SchemaManager> oldSchemaManagers = executionContext.getSchemaManagers();
        Map<String, SchemaManager> whatIfSchemaManagers = new ConcurrentHashMap<>(oldSchemaManagers);
        executionContext.setSchemaManagers(whatIfSchemaManagers);
        for (String schemaName : schemaNames) {
            SchemaManager oldSchemaManager = executionContext.getSchemaManager(schemaName);
            SchemaManager whatIfSchemaManager =
                new WhatIfSchemaManager(oldSchemaManager, candidateIndexSet, executionContext);
            whatIfSchemaManager.init();
            executionContext.getSchemaManagers().put(schemaName, whatIfSchemaManager);
        }

        Map<TableScan, TableMeta> oldMap = new HashMap<>();
        TableScanFinder tableScanFinder = new TableScanFinder();
        logicalPlan.accept(tableScanFinder);
        List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();
        for (Pair<String, TableScan> pair : tableScans) {
            String schemaName = pair.getKey();
            TableScan tableScan = pair.getValue();
            RelOptTable relOptTable = tableScan.getTable();
            if ((relOptTable instanceof RelOptTableImpl)) {
                TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                oldMap.put(tableScan, oldTableMeta);
                TableMeta whatIfTableMeta =
                    executionContext.getSchemaManager(schemaName).getTable(oldTableMeta.getTableName());
                ((RelOptTableImpl) relOptTable).setImplTable(whatIfTableMeta);
            }
        }
        return new WhatIfContext(oldSchemaManagers, tableScans, oldMap);
    }

    private void endWhatIf(WhatIfContext whatIfContext) {
        // replace back
        executionContext.setSchemaManagers(whatIfContext.oldSchemaManagers);
        for (Pair<String, TableScan> pair : whatIfContext.tableScans) {
            TableScan tableScan = pair.getValue();
            RelOptTable relOptTable = tableScan.getTable();
            if ((relOptTable instanceof RelOptTableImpl)) {
                ((RelOptTableImpl) relOptTable).setImplTable(whatIfContext.oldMap.get(tableScan));
            }
        }
    }

    private boolean isConfigurationRedundant(Set<CandidateIndex> candidateIndexSet) {
        // some index a in candidateIndexSet is cover by some index b in candidateIndexSet
        for (CandidateIndex indexA : candidateIndexSet) {
            for (CandidateIndex indexB : candidateIndexSet) {
                if (indexA == indexB) {
                    continue;
                }
                if (indexA.getSchemaName().equalsIgnoreCase(indexB.getSchemaName())
                    && indexA.getTableName().equalsIgnoreCase(indexB.getTableName())
                    && indexA.getColumnNames().size() < indexB.getColumnNames().size()) {
                    boolean redundant = true;
                    for (int i = 0; i < indexA.getColumnNames().size(); i++) {
                        if (!indexA.getColumnNames().get(i).equalsIgnoreCase(indexB.getColumnNames().get(i))) {
                            redundant = false;
                        }
                    }
                    if (redundant == true) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void selectCandidateIndex(CandidateIndex currentLevelCandidateIndex,
                                      Set<CandidateIndex> currentLevelCandidateIndexSet) {

        HumanReadableRule rule = partitionRuleSet.getRule(
            currentLevelCandidateIndex.getSchemaName(), currentLevelCandidateIndex.getTableName());

        // Do not consider GSI for single or broadcast table
        if ((rule.isSingle() || rule.isBroadcast()) && currentLevelCandidateIndex.isGsi()) {
            return;
        }

        // ignore the gsi of unsupported type for partTbl
        if (currentLevelCandidateIndex.getTableMeta().getPartitionInfo() != null
            && currentLevelCandidateIndex.isContainGsiPartColOfUnsupportedDataType()) {
            return;
        }

        //列存索引直接加入
        if (currentLevelCandidateIndex.isCci()) {
            currentLevelCandidateIndexSet.add(currentLevelCandidateIndex);
            return;
        }

        // consider cardinality
        if (currentLevelCandidateIndex.isHighCardinality()
            && (currentLevelCandidateIndex.isNotCoverPrimaryUniqueKey() || currentLevelCandidateIndex.isGsi())
            && !currentLevelCandidateIndex.getTableMeta().isGsi()
            && !currentLevelCandidateIndex.notSupportPartitionGsi()) {
            currentLevelCandidateIndexSet.add(currentLevelCandidateIndex);
        }
    }

    private Set<CandidateIndex> levelUp(CandidateIndex a, CandidateIndex b) {
        if (!a.getSchemaName().equalsIgnoreCase(b.getSchemaName())) {
            return null;
        }
        if (!a.getTableName().equalsIgnoreCase(b.getTableName())) {
            return null;
        }

        Set<String> columnSet = new HashSet<>();
        columnSet.addAll(
            a.getColumnNames().stream().map(columnName -> columnName.toLowerCase()).collect(Collectors.toList()));
        columnSet.addAll(
            b.getColumnNames().stream().map(columnName -> columnName.toLowerCase()).collect(Collectors.toList()));

        if (columnSet.size() != a.getColumnNames().size() + b.getColumnNames().size()) {
            return null;
        }

        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.addAll(a.getColumnNames());
        newColumnNames.addAll(b.getColumnNames());

        String schemaName = a.getSchemaName();
        String tableName = a.getTableName();

        Set<CandidateIndex> candidateIndexSet = new HashSet<>();
        CandidateIndex candidateIndex;

        Collection<HumanReadableRule> rules = partitionPolicyMap.get(schemaName);

        switch (adviseType) {
        case LOCAL_INDEX:
            candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, false, null);
            candidateIndexSet.add(candidateIndex);
            break;
        case GLOBAL_INDEX:
            if (a.hasChangePartitionPolicy() || rules.isEmpty()) {
                candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true, null);
                candidateIndex.changePartitionPolicy(a.getHumanReadableRule());
                candidateIndexSet.add(candidateIndex);
            } else {
                boolean successOnce = false;
                for (HumanReadableRule rule : rules) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true, null);
                    boolean success = candidateIndex.changePartitionPolicy(rule);
                    if (success) {
                        candidateIndexSet.add(candidateIndex);
                    }
                    successOnce = successOnce || success;
                }
                if (!successOnce) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true, null);
                    candidateIndexSet.add(candidateIndex);
                }
            }
            break;
        case GLOBAL_COVERING_INDEX:
            if (a.hasChangePartitionPolicy() || rules.isEmpty()) {
                candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true,
                    coverableColumnSet.getCoverableColumns(schemaName, tableName));
                candidateIndex.changePartitionPolicy(a.getHumanReadableRule());
                candidateIndexSet.add(candidateIndex);
            } else {
                boolean successOnce = false;
                for (HumanReadableRule rule : rules) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true,
                        coverableColumnSet.getCoverableColumns(schemaName, tableName));
                    boolean success = candidateIndex.changePartitionPolicy(rule);
                    if (success) {
                        candidateIndexSet.add(candidateIndex);
                    }
                    successOnce = successOnce || success;
                }
                if (!successOnce) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true,
                        coverableColumnSet.getCoverableColumns(schemaName, tableName));
                    candidateIndexSet.add(candidateIndex);
                }
            }
            break;
        case COLUMNAR_INDEX:
            //直接使用第一个候选索引的分区键，减少枚举情况
            List<String> newPartCols = new ArrayList<>(a.getPartColumns());
            candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, newPartCols, true, true,
                CoverableColumnSet.getCCICoverableColumns(schemaName, tableName, newColumnNames));
            candidateIndexSet.add(candidateIndex);
            break;
        default:
            throw new AssertionError("unKnown type");
        }

        return candidateIndexSet;
    }

    public Set<CandidateIndex> getSingleColumnCandidateIndexSet(IndexableColumnSet indexableColumnSet) {
        Set<CandidateIndex> candidateIndexSet = new HashSet<>();
        for (String schemaName : indexableColumnSet.m.keySet()) {
            for (String tableName : indexableColumnSet.m.get(schemaName).keySet()) {
                TableMeta tableMeta =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
                for (String columnName : indexableColumnSet.m.get(schemaName).get(tableName)) {
                    List<String> index = new ArrayList<>();
                    index.add(columnName);

                    Collection<HumanReadableRule> rules = partitionPolicyMap.get(schemaName);

                    CandidateIndex candidateIndex;

                    switch (adviseType) {
                    case LOCAL_INDEX:
                        candidateIndex = new CandidateIndex(schemaName, tableName, index, false, null);
                        candidateIndexSet.add(candidateIndex);
                        break;
                    case GLOBAL_INDEX: {
                        boolean allIdxColDataTypeSupportPart =
                            checkIfAllIndexColDataTypeSupportPartition(schemaName, tableName, index);
                        if (rules.isEmpty()) {
                            candidateIndex = new CandidateIndex(schemaName, tableName, index, true, null);
                            candidateIndex.setContainGsiColOfUnsupportedDataType(!allIdxColDataTypeSupportPart);
                            candidateIndexSet.add(candidateIndex);
                        } else {
                            boolean successOnce = false;
                            for (HumanReadableRule rule : rules) {
                                boolean isGsi = allIdxColDataTypeSupportPart;
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, isGsi, null);
                                candidateIndex.setContainGsiColOfUnsupportedDataType(!allIdxColDataTypeSupportPart);
                                boolean success = candidateIndex.changePartitionPolicy(rule);
                                if (success) {
                                    candidateIndexSet.add(candidateIndex);
                                }
                                successOnce = successOnce || success;
                            }
                            if (!successOnce) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, true, null);
                                candidateIndex.setContainGsiColOfUnsupportedDataType(!allIdxColDataTypeSupportPart);
                                candidateIndexSet.add(candidateIndex);
                            }
                        }
                        break;
                    }
                    case GLOBAL_COVERING_INDEX: {
                        boolean allIdxColDataTypeSupportPart =
                            checkIfAllIndexColDataTypeSupportPartition(schemaName, tableName, index);
                        boolean isGsi = allIdxColDataTypeSupportPart;
                        if (rules.isEmpty()) {
                            candidateIndex = new CandidateIndex(schemaName, tableName, index, isGsi,
                                coverableColumnSet.getCoverableColumns(schemaName, tableName));
                            candidateIndex.setContainGsiColOfUnsupportedDataType(!allIdxColDataTypeSupportPart);
                            candidateIndexSet.add(candidateIndex);
                        } else {
                            boolean successOnce = false;
                            for (HumanReadableRule rule : rules) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, isGsi,
                                    coverableColumnSet.getCoverableColumns(schemaName, tableName));
                                candidateIndex.setContainGsiColOfUnsupportedDataType(!allIdxColDataTypeSupportPart);
                                boolean success = candidateIndex.changePartitionPolicy(rule);
                                if (success) {
                                    candidateIndexSet.add(candidateIndex);
                                }
                                successOnce = successOnce || success;
                            }
                            if (!successOnce) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, isGsi,
                                    coverableColumnSet.getCoverableColumns(schemaName, tableName));
                                candidateIndex.setContainGsiColOfUnsupportedDataType(!allIdxColDataTypeSupportPart);
                                candidateIndexSet.add(candidateIndex);
                            }
                        }
                        break;
                    }
                    case COLUMNAR_INDEX: {
                        //目前只支持单列分区
                        for (String partCol : indexableColumnSet.m.get(schemaName).get(tableName)) {
                            List<String> partCols = new ArrayList<>();
                            partCols.add(partCol);
                            if (checkIfAllIndexColDataTypeSupportPartition(schemaName, tableName, partCols)) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, partCols, true, true,
                                    CoverableColumnSet.getCCICoverableColumns(schemaName, tableName, index));
                                candidateIndexSet.add(candidateIndex);
                            }
                        }
                        break;
                    }
                    default:
                        throw new AssertionError("unKnown type");
                    }
                }
            }
        }
        return candidateIndexSet;
    }

    private boolean lessThan(RelOptCost cost1, RelOptCost cost2) {
        return cost1.isLt(cost2) && (cost1.getIo() < 0.95 * cost2.getIo()
            || cost1.getNet() < 0.95 * cost2.getNet());
    }

    private boolean looseLessThan(RelOptCost cost1, RelOptCost cost2) {
        return cost1.isLt(cost2);
    }

    private boolean checkIfAllIndexColDataTypeSupportPartition(String dbName, String tbName, List<String> idxCols) {
        TableMeta tbMata = OptimizerContext.getContext(dbName).getLatestSchemaManager().getTable(tbName);
        List<ColumnMeta> idxColMetas = new ArrayList<>();
        for (int i = 0; i < idxCols.size(); i++) {
            String idxColName = idxCols.get(i);
            ColumnMeta cm = tbMata.getColumnIgnoreCase(idxColName);
            idxColMetas.add(cm);
        }
        for (int i = 0; i < idxColMetas.size(); i++) {
            ColumnMeta cm = idxColMetas.get(i);
            if (dataTypeNotSupportPartitionType(cm.getDataType())) {
                return false;
            }
        }
        return true;
    }

    private boolean dataTypeNotSupportPartitionType(DataType dataType) {
        return DataTypeUtil.equalsSemantically(DataTypes.YearType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.FloatType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.DoubleType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.DecimalType, dataType);
    }
}

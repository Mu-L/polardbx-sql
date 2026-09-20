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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategyResult;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsertIgnore;
import com.alibaba.polardbx.optimizer.core.rel.LogicalReplace;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.UkCheckEntry;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.RowWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.WriterFactory;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.ReplaceRexWithParamHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.MappingBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexHandlerCallFactory.ReplaceRexWithParamHandlerCallBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexHandlerChain;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexHandlerChainFactory;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexHandlerFactory;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertWriter;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.mapping.Mappings;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.core.TddlOperatorTable.NEXTVAL;
import static com.alibaba.polardbx.optimizer.utils.BuildPlanUtils.buildUpdateColumnList;

/**
 * @author chenmo.cm
 */
public class OptimizeLogicalInsertRule extends RelOptRule {

    public static final OptimizeLogicalInsertRule INSTANCE = new OptimizeLogicalInsertRule(
        operand(LogicalInsert.class, any()));

    public OptimizeLogicalInsertRule(RelOptRuleOperand operand) {
        super(operand, OptimizeLogicalInsertRule.class.getName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalInsert insert = call.rel(0);
        final PlannerContext plannerContext = PlannerContext.getPlannerContext(call);
        final boolean gsiConcurrentWrite =
            plannerContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        final boolean withHint = insert.hasHint() || Optional.ofNullable(plannerContext.getExecutionContext())
            .map(ExecutionContext::isOriginSqlPushdownOrRoute).orElse(false);
        final boolean optimized = null != insert.getPrimaryInsertWriter() || insert instanceof LogicalInsertIgnore;
        return !optimized && !withHint && gsiConcurrentWrite;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ExecutionContext ec = PlannerContext.getPlannerContext(call).getExecutionContext();

        LogicalInsert origin = call.rel(0);
        final PlannerContext context = call.getPlanner().getContext().unwrap(PlannerContext.class);

        final ExecutionStrategyResult executionStrategyRs =
            ExecutionStrategy.determineExecutionStrategy(origin, context);
        ExecutionStrategy executionStrategy = executionStrategyRs.execStrategy;

        origin.setPushablePrimaryKeyCheck(executionStrategyRs.pushablePrimaryKeyCheck);
        origin.setPushableForeignConstraintCheck(executionStrategyRs.pushableForeignConstraintCheck);

        // For pure columnar table, REPLACE/UPSERT/INSERT IGNORE are not supported by default
        // because Blackhole engine has no unique keys, duplicate handling is meaningless.
        // This check must be before the execution strategy switch, because pure columnar tables
        // with CCI may use DETERMINISTIC_PUSHDOWN strategy, which would bypass a check inside LOGICAL.
        if ((origin.isReplace() || origin.isUpsert() || origin.isInsertIgnore())
            && isPureColumnarTable(origin, ec)) {
            boolean forbid = ec.getParamManager()
                .getBoolean(ConnectionParams.FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR);
            if (forbid) {
                String op = origin.isReplace() ? "REPLACE" :
                    origin.isUpsert() ? "INSERT ... ON DUPLICATE KEY UPDATE" : "INSERT IGNORE";
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    op + " on pure columnar table '" + origin.getLogicalTableName() + "'");
            }
            // Degrade to plain INSERT
            if (origin.isReplace()) {
                // REPLACE uses immutable Operation field, must create new LogicalInsert with INSERT operation
                origin = new LogicalInsert(
                    origin.getCluster(),
                    origin.getTraitSet(),
                    origin.getTable(),
                    origin.getCatalogReader(),
                    origin.getInput(),
                    TableModify.Operation.INSERT,
                    origin.isFlattened(),
                    origin.getInsertRowType(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    origin.getBatchSize(),
                    origin.getAppendedColumnIndex(),
                    origin.getHints(),
                    origin.getTableInfo(),
                    ImmutableList.of());
                origin.setPushablePrimaryKeyCheck(executionStrategyRs.pushablePrimaryKeyCheck);
                origin.setPushableForeignConstraintCheck(executionStrategyRs.pushableForeignConstraintCheck);
            } else {
                // INSERT IGNORE / UPSERT use mutable fields, just clear them
                origin.setKeywords(ImmutableList.of());
                origin.setDuplicateKeyUpdateList(ImmutableList.of());
            }
        }

        RelNode updated = origin;
        switch (executionStrategy) {
        case PUSHDOWN:
            updated = handlePushdown(origin, false, ec);
            break;
        case DETERMINISTIC_PUSHDOWN:
            updated = handlePushdown(origin, true, ec);
            break;
        case LOGICAL:
            if (origin.isReplace()) {
                origin.setUkContainsAllSkAndGsiContainsAllUk(executionStrategyRs.ukContainsAllSkAndGsiContainsAllUk);
                updated = handleReplace(origin, context, executionStrategyRs, ec);
            } else if (origin.isUpsert()) {
                updated = handleUpsert(origin, context, ec);
            } else if (origin.isInsertIgnore()) {
                updated = handleInsertIgnore(origin, context, executionStrategyRs, ec);
            } else {
                updated = handleInsert(origin, context, executionStrategyRs, ec);
            }
            break;
        default:
            throw new IllegalStateException("Unexpected value: " + executionStrategy);
        }

        if (updated != origin) {
            call.transformTo(updated);
        }
    }

    /**
     * Build LogicalInsert with writer for INSERT/INSERT IGNORE/REPLACE/UPSERT with PUSHDOWN execute strategy
     * Replace call parameter and sequence
     *
     * @param origin Origin LogicalInsert
     * @return LogicalInsert
     */
    private LogicalInsert handlePushdown(LogicalInsert origin, boolean deterministicPushdown, ExecutionContext ec) {
        final String schema = origin.getSchemaName();
        final String logicalTableName = origin.getLogicalTableName();
        final RelOptTable primaryTable = origin.getTable();

        final RelDataType sourceRowType = origin.getInsertRowType();
        final List<Integer> valuePermute = Mappings.identityMapping(sourceRowType.getFieldCount());

        // Replace call parameter and sequence
        final LogicalInsert newInsert = replaceExpAndSeqWithParam(origin, false, deterministicPushdown, ec);
        newInsert.setUkContainsAllSkAndGsiContainsAllUk(origin.isUkContainsAllSkAndGsiContainsAllUk());

        final TableMeta primaryTableMeta = ec.getSchemaManager(schema).getTable(logicalTableName);
        if (newInsert.isUpsert() && ExternalizedDmlRewriter.needsHandling(primaryTableMeta)) {
            // Keep the ordinary InsertWriter/physical UPSERT route. The externalized extension only compiles
            // safe conflict assignments into explicit parameter bindings that the existing handler materializes
            // after RexCallParam expansion; unsafe expressions never select this strategy.
            ExternalizedDmlRewriter.prepareExternalizedUpsertPushdown(
                newInsert, primaryTableMeta, getMaxAllocatedParamIndex(newInsert));
        }
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);
        final List<String> primaryColumnNames = sourceRowType.getFieldNames();
        final List<List<Integer>> gsiColumnMappings = initGsiColumnMapping(primaryColumnNames, gsiMetas);

        // For table with gsi, only simple insert can be pushdown
        if (!gsiMetas.isEmpty() && !deterministicPushdown && !origin.isSimpleInsert()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support PUSHDOWN strategy for INSERT IGNORE, REPLACE and UPSERT on table with gsi");
        }

        if (!gsiMetas.isEmpty() && newInsert.withDuplicateKeyUpdate()) {
            final List<String> updateColumnList = buildUpdateColumnList(newInsert);

            final Set<String> gsiCol = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            gsiMetas.stream().flatMap(tm -> tm.getAllColumns().stream()).map(ColumnMeta::getName).forEach(gsiCol::add);
            final boolean updateGsi = updateColumnList.stream().anyMatch(gsiCol::contains);

            if (updateGsi) {
                final String strategy = deterministicPushdown ? "DETERMINISTIC_PUSHDOWN" : "PUSHDOWN";
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Do not support " + strategy + " strategy for UPSERT which update gsi");
            }
        }

        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;
        final boolean isBroadCastOrReplicas = oc.getRuleManager().isBroadCastOrReplicas(logicalTableName);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(logicalTableName);
        final boolean isReplace = origin.isReplace();
        final boolean isValueSource = !origin.isSourceSelect();

        final List<RexNode> newDuplicatedUpdateList = newInsert.isSourceSelect() ?
            LogicalInsert.buildDuplicateKeyUpdateList(newInsert, new AtomicInteger(valuePermute.size()), null, null) :
            newInsert.getDuplicateKeyUpdateList();

        // Build writers
        final InsertWriter writer = WriterFactory
            .createInsertOrReplaceWriter(newInsert, primaryTable, sourceRowType, valuePermute, primaryTableMeta,
                newInsert.getKeywords(), newDuplicatedUpdateList, isReplace, isBroadCastOrReplicas, isSingleTable,
                isValueSource,
                ec);

        // Push insert ignore for upsert on table with gsi and not update any column in gsi
        final List<String> gsiKeywords = new ArrayList<>(newInsert.getKeywords());
        if (gsiMetas.size() > 0 && GeneralUtil.isNotEmpty(newDuplicatedUpdateList)) {
            if (!newInsert.withIgnore()) {
                gsiKeywords.add("IGNORE");
            }
        }

        // Writer for gsi
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        IntStream.range(0, gsiMetas.size()).forEach(i -> {
            final TableMeta gsiMeta = gsiMetas.get(i);
            final RelOptTable gsiTable = catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));
            final List<Integer> gsiValuePermute = gsiColumnMappings.get(i);
            final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
            gsiInsertWriters.add(WriterFactory
                .createInsertOrReplaceWriter(newInsert, gsiTable, sourceRowType, gsiValuePermute, gsiMeta, gsiKeywords,
                    null, isReplace, isGsiBroadcast, isGsiSingle, isValueSource, ec));
        });

        // Do not need share lock for INSERT SELECT
        newInsert.setPrimaryInsertWriter(writer);
        newInsert.setGsiInsertWriters(gsiInsertWriters);
        newInsert.initAutoIncrementColumn();
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(newInsert, ec, primaryTableMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryTableMeta, newInsert, ec);

        //insert select判断
        if (newInsert.isSourceSelect()) {
            return handleInsertSelect(newInsert, ec);
        }
        return newInsert;
    }

    private boolean isPureColumnarTable(LogicalInsert insert, ExecutionContext ec) {
        final String schema = insert.getSchemaName();
        final String tableName = insert.getLogicalTableName();
        final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(tableName);
        return tableMeta != null && Engine.isPureColumnar(tableMeta.getEngine());
    }

    private LogicalInsert handleInsert(LogicalInsert insert, PlannerContext context,
                                       ExecutionStrategyResult executionStrategyRs, ExecutionContext ec) {
        return handlePushdown(insert, true, ec);
    }

    private LogicalInsert handleInsertIgnore(LogicalInsert insert, PlannerContext context,
                                             ExecutionStrategyResult strategyResult, ExecutionContext ec) {

        final String schema = insert.getSchemaName();
        final String targetTable = insert.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;

        final boolean isBroadCastOrReplicas = oc.getRuleManager().isBroadCastOrReplicas(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);
        final RelOptTable primaryTable = insert.getTable();
        final List<String> primaryColumnNames = insert.getInsertRowType().getFieldNames();
        final TableMeta primaryMeta = ec.getSchemaManager(schema).getTable(targetTable);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final boolean withGsi = GeneralUtil.isNotEmpty(gsiMetas);

        final RelDataType originSourceRowType = insert.getInsertRowType();
        final List<Integer> originSourceValuePermute = Mappings.identityMapping(originSourceRowType.getFieldCount());
        final boolean isOriginValueSource = !insert.isSourceSelect();

        // Build writers
        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);
        final List<List<Integer>> gsiColumnMappings = initGsiColumnMapping(primaryColumnNames, gsiMetas);
        final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(primaryMeta);
        final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryMeta);

        boolean useStrategyByHintParams = strategyResult.useStrategyByHintParams;
        boolean canPushDuplicateIgnoreScaleOutCheck = strategyResult.canPushDuplicateIgnoreScaleOutCheck;
        boolean pushDuplicateCheckByHintParams = strategyResult.pushDuplicateCheckByHintParams;

        // Replace call parameter and sequence
        final LogicalInsert newInsert = replaceExpAndSeqWithParam(insert, true, false, ec);
        newInsert.setUkContainsAllSkAndGsiContainsAllUk(insert.isUkContainsAllSkAndGsiContainsAllUk());

        // Writer for primary table
        final List<Integer> primaryValuePermute =
            IntStream.range(0, primaryColumnNames.size()).boxed().collect(Collectors.toList());
        final InsertWriter primaryInsertWriter =

            WriterFactory
                .createInsertOrReplaceWriter(newInsert, primaryTable, newInsert.getInsertRowType(), primaryValuePermute,
                    primaryMeta,
                    null, null, false, isBroadCastOrReplicas, isSingleTable, !newInsert.isSourceSelect(), ec);
        // Delete writer for inserted duplicated row
        DistinctWriter primaryDeleteWriter = null;
        if (isBroadCastOrReplicas) {
            primaryDeleteWriter = WriterFactory.createBroadcastDeleteWriter(newInsert, primaryTable, 0, ec);
        } else if (isSingleTable) {
            primaryDeleteWriter = WriterFactory.createSingleDeleteWriter(newInsert, primaryTable, 0, ec);
        } else {
            primaryDeleteWriter = WriterFactory.createDeleteWriter(newInsert, primaryTable, 0, ec);
        }

        // Writer for gsi
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        final List<InsertWriter> gsiInsertIgnoreWriters = new ArrayList<>();
        final List<DistinctWriter> gsiDeleteWriters = new ArrayList<>();
        IntStream.range(0, gsiMetas.size()).forEach(i -> {
            final TableMeta gsiMeta = gsiMetas.get(i);
            final RelOptTable gsiTable =
                catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));

            final List<Integer> valuePermute = gsiColumnMappings.get(i);
            final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
            gsiInsertWriters.add(WriterFactory
                .createInsertOrReplaceWriter(newInsert, gsiTable, newInsert.getInsertRowType(), valuePermute, gsiMeta,
                    null,
                    null, false, isGsiBroadcast, isGsiSingle, !newInsert.isSourceSelect(), ec));

            gsiInsertIgnoreWriters.add(WriterFactory
                .createInsertOrReplaceWriter(newInsert, gsiTable, newInsert.getInsertRowType(), valuePermute, gsiMeta,
                    ImmutableList.of("IGNORE"),
                    null, false, isGsiBroadcast, isGsiSingle, !newInsert.isSourceSelect(), ec));

            if (isGsiBroadcast) {
                gsiDeleteWriters.add(WriterFactory.createBroadcastDeleteWriter(newInsert, gsiTable, 0, ec));
            } else if (isGsiSingle) {
                gsiDeleteWriters.add(WriterFactory.createSingleDeleteWriter(newInsert, gsiTable, 0, ec));
            } else {
                gsiDeleteWriters.add(WriterFactory.createDeleteWriter(newInsert, gsiTable, 0, ec));
            }
        });

        // Writer for Scaleout pushdown when NOT hit ScaleOut Group Build writers
        InsertWriter scaleoutPushdownWriter = null;
        if (!withGsi && !isBroadCastOrReplicas && !isSingleTable && scaleOutCanWrite) {
            // Only do this optimization for non-gsi table not-broadcast table
            // and its status is on scale-out writable
            if (!useStrategyByHintParams && (pushDuplicateCheckByHintParams || canPushDuplicateIgnoreScaleOutCheck)) {
                scaleoutPushdownWriter = WriterFactory
                    .createInsertOrReplaceWriter(newInsert, primaryTable, originSourceRowType, originSourceValuePermute,
                        primaryMeta,
                        newInsert.getKeywords(), null, false, isBroadCastOrReplicas, isSingleTable, isOriginValueSource,
                        ec);
            }
        }

        // Do not need share lock for INSERT SELECT
        newInsert.setPrimaryInsertWriter(primaryInsertWriter);
        newInsert.setPrimaryDeleteWriter(primaryDeleteWriter);
        newInsert.setGsiInsertWriters(gsiInsertWriters);
        newInsert.setGsiInsertIgnoreWriters(gsiInsertIgnoreWriters);
        newInsert.setGsiDeleteWriters(gsiDeleteWriters);
        newInsert.setPushDownInsertWriter(scaleoutPushdownWriter);
        newInsert.initAutoIncrementColumn();

        final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        ukSet.addAll(GlobalIndexMeta.getUniqueKeyColumnList(targetTable, schema, true, ec));
        final List<String> selectListForDuplicateCheck =
            primaryTable.getRowType().getFieldNames().stream().filter(ukSet::contains).collect(Collectors.toList());
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        final LogicalInsertIgnore insertIgnore = new LogicalInsertIgnore(newInsert, selectListForDuplicateCheck);
        insertIgnore.setTargetTableIsWritable(scaleOutCanWrite);
        insertIgnore.setTargetTableIsReadyToPublish(scaleOutReadyToPublish);
        insertIgnore.setSourceTablesIsReadyToPublish(false);
        insertIgnore.setPushDownInsertWriter(scaleoutPushdownWriter);
        initDuplicateCheckPlan(insertIgnore, schema, ec);
        insertIgnore.getColumnMetaMap().putAll(getColumnMetaMap(primaryMeta, insertIgnore.getUkGroupByTable()));
        insertIgnore.setUsePartFieldChecker(isNewPartDb && allColumnsSupportPartField(insertIgnore.getColumnMetaMap()));
        insertIgnore.setUkContainGeneratedColumn(
            ukContainGeneratedColumn(primaryMeta, insertIgnore.getColumnMetaMap().keySet()));
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(insertIgnore, ec, primaryMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryMeta, insertIgnore, ec);

        return insertIgnore;
    }

    private LogicalInsert handleReplace(LogicalInsert replace, PlannerContext context,
                                        ExecutionStrategyResult strategyResult, ExecutionContext ec) {

        final String schema = replace.getSchemaName();
        final String targetTable = replace.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;

        final boolean isBroadCastOrReplicas = oc.getRuleManager().isBroadCastOrReplicas(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);
        final RelOptTable primaryTable = replace.getTable();
        final List<String> primaryColumnNames = replace.getInsertRowType().getFieldNames();
        final TableMeta primaryMeta = ec.getSchemaManager(schema).getTable(targetTable);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final boolean withGsi = GeneralUtil.isNotEmpty(gsiMetas);
        final boolean hasJsonColumn = primaryMeta.getAllColumns().stream()
            .anyMatch(cm -> DataTypeUtil.equalsSemantically(cm.getDataType(), DataTypes.JsonType));

        final RelDataType originSourceRowType = replace.getInsertRowType();
        final List<Integer> originSourceValuePermute = Mappings.identityMapping(originSourceRowType.getFieldCount());
        final boolean isOriginValueSource = !replace.isSourceSelect();

        // Build writers
        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);
        final List<List<Integer>> gsiColumnMappings = initGsiColumnMapping(primaryColumnNames, gsiMetas);

        final List<Integer> primaryValuePermute =
            IntStream.range(0, primaryColumnNames.size()).boxed().collect(Collectors.toList());
        final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(primaryMeta);
        final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryMeta);

        boolean useStrategyByHintParams = strategyResult.useStrategyByHintParams;
        boolean canPushDuplicateIgnoreScaleOutCheck = strategyResult.canPushDuplicateIgnoreScaleOutCheck;
        boolean pushDuplicateCheckByHintParams = strategyResult.pushDuplicateCheckByHintParams;

        final LogicalInsert newInsert = replaceExpAndSeqWithParam(replace, true, false, ec);

        newInsert.setUkContainsAllSkAndGsiContainsAllUk(replace.isUkContainsAllSkAndGsiContainsAllUk());
        // Writer for primary table
        InsertWriter primaryInsertWriter = null;
        ReplaceRelocateWriter primaryReplaceRelocateWriter = null;

        final LogicalInsertIgnore base = new LogicalInsertIgnore(newInsert, primaryColumnNames);
        final boolean containsAllUk = base.containsAllUk(targetTable);

        // SELECT --> deduplicate --> REPLACE or DELETE + INSERT
        primaryReplaceRelocateWriter =
            WriterFactory.createReplaceRelocateWriter(newInsert, primaryTable, primaryValuePermute, primaryMeta,
                containsAllUk, false, isBroadCastOrReplicas, isSingleTable, ec);

        // Writer for gsi
        final List<ReplaceRelocateWriter> gsiReplaceRelocateWriters = new ArrayList<>();
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        IntStream.range(0, gsiMetas.size()).forEach(i -> {
            final TableMeta gsiMeta = gsiMetas.get(i);
            final RelOptTable gsiTable =
                catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));

            final LogicalInsertIgnore tmpBase = new LogicalInsertIgnore(newInsert, primaryColumnNames);
            final boolean gsiContainsAllUk = tmpBase.containsAllUk(gsiMeta.getTableName());

            final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
            gsiReplaceRelocateWriters.add(
                WriterFactory.createReplaceRelocateWriter(newInsert, gsiTable, gsiColumnMappings.get(i), gsiMeta,
                    gsiContainsAllUk, true, isGsiBroadcast, isGsiSingle, ec));
        });

        // Writer for Scaleout pushdown when NOT hit ScaleOut Group Build writers
        InsertWriter scaleoutPushdownWriter = null;
        if (!withGsi && !isBroadCastOrReplicas && !isSingleTable && scaleOutCanWrite) {
            // Only do this optimization for non-gsi table not-broadcast table
            // and its status is on scale-out writable
            if (!useStrategyByHintParams && (pushDuplicateCheckByHintParams || canPushDuplicateIgnoreScaleOutCheck)) {
                // 1. Make sure that connProps has no HINT about DML_EXECUTION_STRATEGY
                // 2. Only optimise for the situation that DML_PUSH_DUPLICATE_CHECK=true or canPushDuplicateCheck=true
                scaleoutPushdownWriter = WriterFactory
                    .createInsertOrReplaceWriter(newInsert, primaryTable, originSourceRowType, originSourceValuePermute,
                        primaryMeta,
                        newInsert.getKeywords(), null, true, isBroadCastOrReplicas, isSingleTable, isOriginValueSource,
                        ec);
            }
        }
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        // not optimize by returning for replace select
        // not optimize by returning for DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN = true
        // not optimize by returning for no gsi
        boolean canUseReturning = ec.getParamManager().getBoolean(ConnectionParams.DML_USE_RETURNING) &&
            ec.getParamManager().getBoolean(ConnectionParams.OPTIMIZE_REPLACE_BY_RETURNING) &&
            !ec.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN)
            && !gsiMetas.isEmpty()
            && !replace.isSourceSelect()
            && !ExternalizedDmlRewriter.isReturningForbidden(primaryMeta);

        DistinctWriter primaryDeleteWriter = null;
        List<DistinctWriter> gsiDeleteWriters = new ArrayList<>();
        if (canUseReturning) {
            // for returning need to construct replace writer and delete writer for fix
            // replace writer for primary
            primaryInsertWriter =
                WriterFactory.createInsertOrReplaceWriter(newInsert, primaryTable, originSourceRowType,
                    originSourceValuePermute, primaryMeta, newInsert.getKeywords(), null, true, isBroadCastOrReplicas,
                    isSingleTable, isOriginValueSource, ec);

            // replace writer for gsi
            IntStream.range(0, gsiMetas.size()).forEach(i -> {
                final TableMeta gsiMeta = gsiMetas.get(i);
                final RelOptTable gsiTable =
                    catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));
                final List<Integer> gsiValuePermute = gsiColumnMappings.get(i);
                final boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
                final boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);
                gsiInsertWriters.add(WriterFactory
                    .createInsertOrReplaceWriter(newInsert, gsiTable, originSourceRowType, gsiValuePermute, gsiMeta,
                        newInsert.getKeywords(),
                        null, true, isGsiBroadcast, isGsiSingle, isOriginValueSource, ec));
            });

            // delete writer for primary
            primaryDeleteWriter = WriterFactory.createDeleteWriter(newInsert, primaryTable, 0, ec);

            // delete writer for gsi
            IntStream.range(0, gsiMetas.size()).forEach(i -> {
                final TableMeta gsiMeta = gsiMetas.get(i);
                final RelOptTable gsiTable =
                    catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));
                gsiDeleteWriters.add(WriterFactory.createDeleteWriter(newInsert, gsiTable, 0, ec));
            });
        }

        final LogicalReplace newReplace =
            new LogicalReplace(newInsert, primaryInsertWriter, primaryReplaceRelocateWriter, gsiInsertWriters,
                gsiReplaceRelocateWriters, primaryDeleteWriter, gsiDeleteWriters, null, primaryColumnNames,
                hasJsonColumn, canUseReturning);
        newReplace.setTargetTableIsWritable(scaleOutCanWrite);
        newReplace.setTargetTableIsReadyToPublish(scaleOutReadyToPublish);
        newReplace.setSourceTablesIsReadyToPublish(false);
        newReplace.setPushDownInsertWriter(scaleoutPushdownWriter);
        initDuplicateCheckPlan(newReplace, schema, ec);
        newReplace.getColumnMetaMap().putAll(getColumnMetaMap(primaryMeta, newReplace.getUkGroupByTable()));
        newReplace.setUsePartFieldChecker(isNewPartDb && allColumnsSupportPartField(newReplace.getColumnMetaMap()));
        newReplace.setUkContainGeneratedColumn(
            ukContainGeneratedColumn(primaryMeta, newReplace.getColumnMetaMap().keySet()));

        // TODO need exclusive lock for REPLACE SELECT ?
        newReplace.initAutoIncrementColumn();
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(newReplace, ec, primaryMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryMeta, newReplace, ec);

        return newReplace;

    }

    private RelNode handleUpsert(LogicalInsert upsert, PlannerContext context, ExecutionContext ec) {

        final String schema = upsert.getSchemaName();
        final String targetTable = upsert.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;
        final TddlRuleManager rule = oc.getRuleManager();

        final boolean isBroadCastOrReplicas = oc.getRuleManager().isBroadCastOrReplicas(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);

        final List<List<String>> uniqueKeys = GlobalIndexMeta.getUniqueKeys(targetTable, schema, true, tm -> true, ec);
        final boolean withoutPkAndUk = uniqueKeys.isEmpty() || uniqueKeys.get(0).isEmpty();

        if (withoutPkAndUk) {
            // Without pk and uk, upsert can be pushdown.
            // Assuming than table with scale out running or gsi must have primary key
            return handlePushdown(upsert, isBroadCastOrReplicas, ec);
        }

        if (upsert.withIgnore()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Do not support insert ignore...on duplicate key update");
        }

        final RelOptTable primaryTable = upsert.getTable();
        final TableMeta primaryMeta = ec.getSchemaManager(schema).getTable(targetTable);
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final List<String> primaryTargetColumns = upsert.getInsertRowType().getFieldNames();
        final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(primaryMeta);
        final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryMeta);
        final boolean hasJsonColumn = primaryMeta.getAllColumns().stream()
            .anyMatch(cm -> DataTypeUtil.equalsSemantically(cm.getDataType(), DataTypes.JsonType));

        // Mapping from VALUES of target INSERT to VALUES of source INSERT
        final List<Integer> primaryValuePermute =
            IntStream.range(0, primaryTargetColumns.size()).boxed().collect(Collectors.toList());
        final List<List<Integer>> gsiValuePermutes = initGsiColumnMapping(primaryTargetColumns, gsiMetas);

        // Get columns to be update
        final List<String> updateColumnList = BuildPlanUtils.buildUpdateColumnList(upsert);

        if (ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK)
            && !upsert.isPushablePrimaryKeyCheck()) {
            // TODO(qihua): support upsert Pk check
            // Upsert updates primary key, and we are not able to check since primary key does not contain all sharding
            // key
            Set<String> updateColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            updateColumnSet.addAll(updateColumnList);
            if (primaryMeta.getPrimaryKey().stream().anyMatch(cm -> updateColumnSet.contains(cm.getName()))) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Can not check primary key for upsert");
            }
        }

        // Build column mapping for columns to be update
        final Map<Integer, List<Integer>> primaryUpdateColumnMappings = new HashMap<>();
        final Map<Integer, List<List<Integer>>> gsiUpdateColumnMappings = new HashMap<>();
        BuildPlanUtils.buildColumnMappings(updateColumnList,
            updateColumnList.stream().map(c -> 0).collect(Collectors.toList()),
            ImmutableMap.of(0, gsiMetas), primaryUpdateColumnMappings, gsiUpdateColumnMappings);

        final boolean allGsiPublished = GlobalIndexMeta.isAllGsiPublished(gsiMetas, context);
        AtomicBoolean modifyPartitionKey = new AtomicBoolean();
        final boolean modifyUniqueKey = isModifyUniqueKey(updateColumnList, targetTable, schema, ec);

        final LogicalInsert newInsert = replaceExpAndSeqWithParam(upsert, true, false, ec);

        // Record terminal replacements and migration (contentIdx, addrIdx) pairs in the final
        // "after" row index space. Migration addr slots are appended columns, so a newly generated
        // BlobRef must not make the logical identical-row check report a content change.
        final List<RowWriteBinding> externalizedConflictWriteBindings =
            compileExternalizedConflictWriteBindings(schema, targetTable, primaryTargetColumns, primaryMeta,
                updateColumnList);
        for (RowWriteBinding binding : externalizedConflictWriteBindings) {
            if (binding.getAction() == ExternalizedWriteBinding.Action.MIGRATION_APPEND) {
                newInsert.getAppendedColumnIndex().add(binding.getTargetRowIndex());
            }
        }

        // Build writer for primary table
        InsertWriter primaryInsertWriter = null;
        UpsertWriter primaryUpsertWriter = null;
        UpsertRelocateWriter primaryRelocateWriter = null;
        final List<String> selectListForDuplicateCheck = new ArrayList<>();
        final List<Integer> beforeUpdateMapping = new ArrayList<>();
        final AtomicBoolean withColumnRefInDuplicateKeyUpdate = new AtomicBoolean(false);

        final MappingBuilder mappingBuilder = MappingBuilder.create(primaryTargetColumns);
        // Get all columns of target table
        selectListForDuplicateCheck.addAll(mappingBuilder.getTarget());
        // Map each ON DUPLICATE KEY UPDATE target ordinal into the complete before/after row selected for duplicate
        // checking. For example, target columns [id, body, note] and UPDATE [note, body] produce [2, 1]. The handler
        // uses this shared row space both to apply final logical SET values and to locate externalized write bindings.
        // This is the existing UPSERT mapping; externalized tables only add bindings that refer to its slots.
        beforeUpdateMapping.addAll(mappingBuilder.source(updateColumnList).getMapping());
        // Check ON DUPLICATE KEY UPDATE part reference before value of duplicate row
        withColumnRefInDuplicateKeyUpdate.set(
            newInsert.getDuplicateKeyUpdateList().stream().map(rex -> ((RexCall) rex).getOperands().get(1))
                .anyMatch(call -> BeforeInputFinder.analyze(call).withInputRef));

        // Check modify partition key of primary
        final Set<String> partitionKeySet = rule.getSharedColumns(targetTable).stream()
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
        boolean primaryDoRelocate = updateColumnList.stream().anyMatch(partitionKeySet::contains);
        boolean inputInValueColumnOrder = !checkInputInTableOrder(upsert, primaryMeta);

        if (!primaryDoRelocate && (scaleOutCanWrite || !allGsiPublished)) {
            final Set<String> pkName = GlobalIndexMeta.getPrimaryKeys(primaryMeta).stream().map(String::toLowerCase)
                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            primaryDoRelocate |= updateColumnList.stream().map(String::toLowerCase).anyMatch(pkName::contains);
        }
        if (!primaryDoRelocate) {
            // SELECT --> deduplicate --> INSERT or UPDATE
            primaryUpsertWriter = WriterFactory.createUpsertWriter(newInsert, primaryTable, updateColumnList,
                primaryUpdateColumnMappings.get(0), primaryValuePermute, selectListForDuplicateCheck,
                false, primaryMeta, false, isBroadCastOrReplicas, isSingleTable, ec);
        } else {
            // SELECT --> deduplicate --> INSERT(values) or UPDATE or DELETE + INSERT(selected)
            primaryRelocateWriter =
                WriterFactory.createUpsertRelocateWriter(newInsert, primaryTable, updateColumnList,
                    primaryUpdateColumnMappings.get(0), primaryValuePermute, selectListForDuplicateCheck,
                    updateColumnList, primaryMeta, false, isBroadCastOrReplicas, isSingleTable, ec);
        }
        modifyPartitionKey.set(primaryDoRelocate);

        AtomicBoolean primaryModifySk = new AtomicBoolean();
        primaryModifySk.set(primaryDoRelocate);

        final RelOptSchema catalog = RelUtils.buildCatalogReader(schema, ec);

        // Writer for gsi
        final List<UpsertWriter> gsiUpsertWriters = new ArrayList<>();
        final List<RelocateWriter> gsiRelocateWriters = new ArrayList<>();
        final List<InsertWriter> gsiInsertWriters = new ArrayList<>();
        Ord.zip(gsiMetas).forEach(o -> {
            final Integer gsiIndex = o.getKey();
            final TableMeta gsiMeta = o.getValue();

            // Get update columns for gsi
            final List<Integer> gsiUpdateColumnMapping = gsiUpdateColumnMappings.get(0).get(gsiIndex);
            final boolean withoutUpdate = gsiUpdateColumnMapping.isEmpty();
            final List<String> gsiUpdateColumns = withoutUpdate ? new ArrayList<>() :
                Mappings.permute(updateColumnList, Mappings.source(gsiUpdateColumnMapping, updateColumnList.size()));

            // Check modify partition key of gsi
            final Set<String> gsiPartitionKeySet = rule.getSharedColumns(gsiMeta.getTableName()).stream()
                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            boolean doRelocate = gsiUpdateColumns.stream().anyMatch(gsiPartitionKeySet::contains);

            final boolean isPublished = GlobalIndexMeta.isPublished(context.getExecutionContext(), gsiMeta);
            final boolean isGsiTableCanScaleOutWrite = ComplexTaskPlanUtils.canWrite(gsiMeta);
            if (!doRelocate && (scaleOutCanWrite || !isPublished || isGsiTableCanScaleOutWrite)) {
                final Set<String> pkName = GlobalIndexMeta.getPrimaryKeys(gsiMeta).stream()
                    .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
                doRelocate |= gsiUpdateColumns.stream().anyMatch(pkName::contains);
            }

            if (GlobalIndexMeta.canDelete(ec, gsiMeta) && !GlobalIndexMeta.canWrite(ec, gsiMeta)) {
                // DELETE_ONLY, need delete writer
                doRelocate = true;
            }

            boolean forceRelocate = primaryModifySk.get() && GlobalIndexMeta.isBackFillStatus(ec, gsiMeta);

            modifyPartitionKey.set(modifyPartitionKey.get() || doRelocate);

            final RelOptTable gsiTable = catalog.getTableForMember(ImmutableList.of(schema, gsiMeta.getTableName()));
            final List<Integer> gsiValuePermute = gsiValuePermutes.get(gsiIndex);

            boolean isGsiBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
            boolean isGsiSingle = TableTopologyUtil.isSingle(gsiMeta);

            if (!doRelocate && !forceRelocate) {
                // SELECT --> deduplicate --> INSERT or UPDATE
                gsiUpsertWriters.add(WriterFactory
                    .createUpsertWriter(newInsert, gsiTable, gsiUpdateColumns, gsiUpdateColumnMapping, gsiValuePermute,
                        selectListForDuplicateCheck, withoutUpdate, gsiMeta, true, isGsiBroadcast, isGsiSingle, ec));
            } else {
                // SELECT --> deduplicate --> INSERT(values) or UPDATE or DELETE + INSERT(selected)
                gsiRelocateWriters.add(WriterFactory.createUpsertRelocateWriter(newInsert, gsiTable, gsiUpdateColumns,
                    gsiUpdateColumnMapping, gsiValuePermute, selectListForDuplicateCheck, updateColumnList, gsiMeta,
                    true, isGsiBroadcast, isGsiSingle, ec));
            }
        });
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        final LogicalUpsert result =
            new LogicalUpsert(newInsert, primaryInsertWriter, primaryUpsertWriter, primaryRelocateWriter,
                gsiInsertWriters, gsiUpsertWriters, gsiRelocateWriters, selectListForDuplicateCheck,
                beforeUpdateMapping, selectListForDuplicateCheck.size(), modifyPartitionKey.get(), modifyUniqueKey,
                withColumnRefInDuplicateKeyUpdate.get(), hasJsonColumn, inputInValueColumnOrder);
        result.setSourceTablesIsReadyToPublish(false);
        result.setTargetTableIsWritable(scaleOutCanWrite);
        result.setTargetTableIsReadyToPublish(scaleOutReadyToPublish);
        initDuplicateCheckPlan(result, schema, ec);
        result.getColumnMetaMap().putAll(getColumnMetaMap(primaryMeta, result.getUkGroupByTable()));
        result.setUsePartFieldChecker(isNewPartDb && allColumnsSupportPartField(result.getColumnMetaMap()));
        result.setUkContainGeneratedColumn(ukContainGeneratedColumn(primaryMeta, result.getColumnMetaMap().keySet()));
        // TODO need exclusive lock for UPSERT SELECT ?
        result.initAutoIncrementColumn();
        GeneratedColumnUtil.buildGeneratedColumnInfoForInsert(result, ec, primaryMeta);

        //build DefaultExprColumns
        DefaultExprUtil.buildDefaultExprColumns(primaryMeta, result, ec);

        return result;
    }

    static List<RowWriteBinding> compileExternalizedConflictWriteBindings(String schemaName, String tableName,
                                                                          List<String> targetColumns,
                                                                          TableMeta tableMeta,
                                                                          List<String> updateColumns) {
        return ExternalizedDmlRewriter.compileUpsertConflictWriteBindings(schemaName, tableName, targetColumns,
            tableMeta, updateColumns);
    }

    private LogicalInsert handleInsertSelect(LogicalInsert insert, ExecutionContext ec) {
        //不是LogicalInsert类（有些继承LogicalInsert，执行方式不同),或者不是insert select 直接返回
        if (!insert.getClass().isAssignableFrom(LogicalInsert.class) || !insert.isSourceSelect()) {
            return insert;
        }
        final boolean hasIndex = GlobalIndexMeta.hasGsi(
            insert.getLogicalTableName(), insert.getSchemaName(), ec);
        final boolean gsiConcurrentWrite =
            ec.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(insert.getSchemaName()))
            .getRuleManager();

        final boolean isBroadCastOrReplicas = or.isBroadCastOrReplicas(insert.getLogicalTableName());
        final boolean canGsiConcurrentWrite = !hasIndex || gsiConcurrentWrite;
        //能否多线程执行Insert,即将select的数据切割，可并行执行doExecute：
        // 1.广播表策略FIRST_THEN_CONCURRENT,不用多线程
        // 2.是简单的Insert,有些特殊的sql语句，继承LogicalInsert：LogicalInsertIgnore、LogicalReplace、LogicalUpsert等doExecute不同
        // 3.如果有gsi，gsi支持并行写
        // 4.不支持含有生成列或正在进行 OMC 的表

        String schemaName = insert.getSchemaName();
        String tableName = insert.getLogicalTableName();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        final boolean isColumnMultiWriting = TableColumnUtils.isModifying(schemaName, tableName, ec);
        final boolean containGeneratedColumn = tableMeta.hasLogicalGeneratedColumn();

        final boolean requiresWriteRewrite = ExternalizedDmlRewriter.needsHandling(tableMeta);
        final boolean canMultiInsert = !isBroadCastOrReplicas && canGsiConcurrentWrite && !isColumnMultiWriting
            && !containGeneratedColumn && !requiresWriteRewrite;
        boolean insertSelectByMpp = ec.getParamManager().getBoolean(ConnectionParams.INSERT_SELECT_MPP);
        //用户通过hint指定MPP运行
        if (insertSelectByMpp) {
            // Externalized columns require CN-side blob_addr transform which MPP bypasses;
            // silently fall through to non-MPP path.
            if (!requiresWriteRewrite) {
                if (!canMultiInsert) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INSERT_SELECT,
                        "This InsertSelect SQL isn't supported use MPP.");
                }
                insert.setInsertSelectMode(LogicalInsert.InsertSelectMode.MPP);
                return insert;
            }
        }
        //默认开启:多线程执行insert,可hint关闭
        boolean insertSelectByMulti = ec.getParamManager().getBoolean(ConnectionParams.MODIFY_SELECT_MULTI);
        if (insertSelectByMulti && canMultiInsert) {
            insert.setInsertSelectMode(LogicalInsert.InsertSelectMode.MULTI);
        }
        return insert;
    }

    private static Map<String, ColumnMeta> getColumnMetaMap(TableMeta tableMeta,
                                                            Map<String, List<List<String>>> ukGroupByTable) {
        Map<String, ColumnMeta> columnMetaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ukGroupByTable.values().forEach(key -> key.forEach(
            cols -> cols.forEach(col -> columnMetaMap.put(col, tableMeta.getColumnIgnoreCase(col)))));
        return columnMetaMap;
    }

    private static boolean ukContainGeneratedColumn(TableMeta tableMeta, Set<String> ukColumns) {
        return tableMeta.getGeneratedColumnNames().stream().anyMatch(ukColumns::contains);
    }

    private static boolean allColumnsSupportPartField(Map<String, ColumnMeta> columnMetaMap) {
        return columnMetaMap.values().stream().allMatch(cm -> {
            try {
                if (cm.getDataType() instanceof BinaryType) {
                    return false;
                }
                PartitionField partitionField = PartitionFieldBuilder.createField(cm.getDataType());
            } catch (Throwable ex) {
                return false;
            }
            return true;
        });
    }

    protected Map<String, List<List<String>>> groupUkByTable(LogicalInsertIgnore insertIgnore,
                                                             ExecutionContext executionContext) {
        // Map uk to table
        Map<String, List<List<String>>> tableUkMap = new HashMap<>();
        final String schemaName = insertIgnore.getSchemaName();
        final String primaryTableName = insertIgnore.getLogicalTableName();

        // Get plan for finding duplicate values
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        final SchemaManager sm = executionContext.getSchemaManager(schemaName);
        assert oc != null;
        final TableMeta baseTableMeta = sm.getTable(primaryTableName);

        // Get all uk constraints from WRITABLE tables
        // [[columnName(upper case)]]
        List<List<String>> uniqueKeys = new ArrayList<>(new HashSet<>(
            GlobalIndexMeta.getUniqueKeys(primaryTableName, schemaName, true,
                tm -> GlobalIndexMeta.canWrite(executionContext, tm), executionContext)));

        // Only lookup primary table, could be
        // 1. Set by hint
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_USING_GSI)) {
            tableUkMap.put(primaryTableName, uniqueKeys);
            return tableUkMap;
        }

        final GsiMetaManager.GsiTableMetaBean gsiTableMeta = baseTableMeta.getGsiTableMetaBean();
        List<String> writableIndexTables = new ArrayList<>();
        // Get all PUBLIC / WRITE_ONLY gsi
        if (null != gsiTableMeta && GeneralUtil.isNotEmpty(gsiTableMeta.indexMap)) {
            gsiTableMeta.indexMap.entrySet().stream().filter(e -> GlobalIndexMeta.canWrite(executionContext,
                    sm.getTable(e.getValue().indexName)))
                .forEach(e -> writableIndexTables.add(e.getKey().toUpperCase()));
        }

        // Get all tables' local uk, include PUBLIC / WRITE_ONLY gsi
        // tableName -> [indexName -> [columnName(upper case)]]
        Map<String, Map<String, Set<String>>> writableTableUkMap =
            insertIgnore.getTableUkMap()
                .entrySet()
                .stream()
                .filter(e -> writableIndexTables.contains(e.getKey().toUpperCase())
                    || primaryTableName.equalsIgnoreCase(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Map uk to tables, must be exact match for uk
        // i -> [tableName]
        Map<Integer, List<String>> ukAllTableMap = new HashMap<>();
        for (int i = 0; i < uniqueKeys.size(); i++) {
            List<String> uniqueKey = uniqueKeys.get(i);
            for (Map.Entry<String, Map<String, Set<String>>> e : writableTableUkMap.entrySet()) {
                String currentTableName = e.getKey().toUpperCase();
                Map<String, Set<String>> currentUniqueKeys = e.getValue();
                // At least match one uk in table
                if (currentUniqueKeys.values().stream().anyMatch(
                    currentUniqueKey -> currentUniqueKey.size() == uniqueKey.size()
                        && currentUniqueKey.containsAll(uniqueKey))) {
                    final List<String> ukAllTables = ukAllTableMap.computeIfAbsent(i, k -> new ArrayList<>());
                    if (TStringUtil.equalsIgnoreCase(currentTableName, primaryTableName)) {
                        // Add primary table to first position
                        ukAllTables.add(0, currentTableName);
                    } else {
                        ukAllTables.add(currentTableName);
                    }
                }
            }
        }

        Set<String> primaryKey = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (baseTableMeta.getPrimaryIndex() != null) {
            primaryKey.addAll(
                baseTableMeta.getPrimaryIndex().getKeyColumns().stream().map(cm -> cm.getName().toUpperCase())
                    .collect(Collectors.toList()));
        }

        final boolean isGetDupForPkFromPrimaryOnly =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY);

        // Best table for all uk is the table contains all uk columns
        // and every uk contains all partition columns of this table
        final Map<String, List<List<String>>> bestTableUkMap = findBestTableForAllUk(ukAllTableMap,
            uniqueKeys,
            primaryKey,
            schemaName,
            primaryTableName,
            isGetDupForPkFromPrimaryOnly,
            executionContext);

        if (bestTableUkMap.isEmpty()) {
            for (Map.Entry<Integer, List<String>> e : ukAllTableMap.entrySet()) {
                final List<String> tableNames = e.getValue();
                final Set<String> uniqueKey = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                uniqueKey.addAll(uniqueKeys.get(e.getKey()));

                String ukTargetTable = null;
                if (isGetDupForPkFromPrimaryOnly
                    && uniqueKey.containsAll(primaryKey) && primaryKey.containsAll(uniqueKey)) {
                    // PK must be searched on primary table
                    ukTargetTable = primaryTableName;
                } else {
                    ukTargetTable = getUkTargetTable(schemaName,
                        primaryTableName,
                        uniqueKey,
                        tableNames,
                        executionContext);
                }

                tableUkMap.computeIfAbsent(ukTargetTable.toUpperCase(), k -> new ArrayList<>())
                    .add(uniqueKeys.get(e.getKey()));
            }
        } else {
            tableUkMap.putAll(bestTableUkMap);
        }

        return tableUkMap;
    }

    private void initDuplicateCheckPlan(LogicalInsertIgnore insert, String schemaName,
                                        ExecutionContext executionContext) {
        final Map<String, List<List<String>>> ukGroupByTable = groupUkByTable(insert, executionContext);
        final Map<String, List<String>> localIndexNames =
            getLocalIndexName(ukGroupByTable, schemaName, executionContext);

        insert.getUkGroupByTable().putAll(ukGroupByTable);
        insert.getLocalIndexPhyName().putAll(localIndexNames);
        insert.getCandidateUkChecks().putAll(
            buildPartitionLocalUniqueCandidate(insert, ukGroupByTable, localIndexNames, executionContext));
    }

    protected Map<String, List<UkCheckEntry>> buildPartitionLocalUniqueCandidate(
        LogicalInsertIgnore insert,
        Map<String, List<List<String>>> legacyUkGroupByTable,
        Map<String, List<String>> legacyLocalIndexNames,
        ExecutionContext executionContext) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_USING_GSI)) {
            return ImmutableMap.of();
        }

        final String schemaName = insert.getSchemaName();
        final String primaryTableName = insert.getLogicalTableName();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        final TableMeta primaryTableMeta = schemaManager.getTable(primaryTableName);
        final TddlRuleManager ruleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        final Map<String, List<UkCheckEntry>> candidate = new LinkedHashMap<>();

        legacyUkGroupByTable.forEach((tableName, uniqueKeys) -> {
            final List<String> indexNames = legacyLocalIndexNames.get(tableName);
            final List<UkCheckEntry> entries = new ArrayList<>(uniqueKeys.size());
            for (int i = 0; i < uniqueKeys.size(); i++) {
                final String indexName = indexNames == null || i >= indexNames.size() ? null : indexNames.get(i);
                entries.add(new UkCheckEntry(uniqueKeys.get(i), indexName, false));
            }
            candidate.put(tableName, entries);
        });

        final List<List<String>> primaryLegacyUks = legacyUkGroupByTable.entrySet().stream()
            .filter(entry -> TStringUtil.equalsIgnoreCase(entry.getKey(), primaryTableName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(ImmutableList.of());
        final Set<String> primaryKey = getColumnSet(GlobalIndexMeta.getPrimaryKeys(primaryTableMeta));
        final List<TableMeta> writableGsiMetas =
            GlobalIndexMeta.getIndex(primaryTableName, schemaName, executionContext).stream()
                .filter(tableMeta -> !tableMeta.isColumnar())
                .filter(tableMeta -> GlobalIndexMeta.canWrite(executionContext, tableMeta))
                .collect(Collectors.toList());

        final List<List<String>> partitionLocalKeys = new ArrayList<>(primaryLegacyUks);
        legacyUkGroupByTable.values().stream()
            .flatMap(List::stream)
            .filter(uniqueKey -> sameColumns(getColumnSet(uniqueKey), primaryKey))
            .findFirst()
            .filter(uniqueKey -> partitionLocalKeys.stream()
                .noneMatch(existing -> sameColumns(getColumnSet(existing), primaryKey)))
            .ifPresent(partitionLocalKeys::add);

        boolean optimized = false;
        for (List<String> uniqueKey : partitionLocalKeys) {
            final Set<String> uniqueKeySet = getColumnSet(uniqueKey);
            final boolean primaryKeyCheck = sameColumns(uniqueKeySet, primaryKey);
            if (isLegacyCheckPartitionPruned(legacyUkGroupByTable, uniqueKeySet, ruleManager)
                || matchesWritableUgsi(uniqueKeySet, writableGsiMetas)) {
                continue;
            }

            final IndexMeta primaryLocalIndex = primaryKeyCheck
                ? getPlainPrimaryIndex(primaryTableMeta, uniqueKeySet)
                : getPlainLocalUniqueIndex(primaryTableMeta, uniqueKeySet);
            if (primaryLocalIndex == null) {
                continue;
            }

            final Map<String, String> partitionLocalScopes =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            partitionLocalScopes.put(primaryTableName, primaryLocalIndex.getPhysicalIndexName());

            boolean allScopesStable = true;
            for (TableMeta gsiMeta : writableGsiMetas) {
                final List<IndexMeta> matchingLocalIndexes =
                    getMatchingLocalUniqueIndexes(gsiMeta, uniqueKeySet, primaryKeyCheck);
                if (matchingLocalIndexes.isEmpty()) {
                    continue;
                }
                if (!GlobalIndexMeta.isPublished(executionContext, gsiMeta)
                    || matchingLocalIndexes.stream().anyMatch(indexMeta -> !isPlainFullLengthIndex(indexMeta))) {
                    allScopesStable = false;
                    break;
                }
                final IndexMeta localIndex = matchingLocalIndexes.get(0);
                partitionLocalScopes.put(gsiMeta.getTableName(), localIndex.getPhysicalIndexName());
            }
            if (!allScopesStable) {
                continue;
            }

            applyPartitionLocalScopes(candidate, uniqueKey, uniqueKeySet, partitionLocalScopes);
            optimized = true;
        }

        candidate.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        return optimized ? candidate : ImmutableMap.of();
    }

    private static boolean isLegacyCheckPartitionPruned(Map<String, List<List<String>>> legacyUkGroupByTable,
                                                        Set<String> uniqueKey, TddlRuleManager ruleManager) {
        return legacyUkGroupByTable.entrySet().stream()
            .filter(entry -> entry.getValue().stream()
                .anyMatch(columns -> sameColumns(getColumnSet(columns), uniqueKey)))
            .anyMatch(entry -> isTablePartitionByColumns(entry.getKey(), uniqueKey, ruleManager));
    }

    private static boolean matchesWritableUgsi(Set<String> uniqueKey, List<TableMeta> writableGsiMetas) {
        for (TableMeta gsiMeta : writableGsiMetas) {
            if (gsiMeta.getGsiTableMetaBean() == null
                || gsiMeta.getGsiTableMetaBean().gsiMetaBean == null) {
                return true;
            }
            final GsiMetaManager.GsiIndexMetaBean gsiMetaBean =
                gsiMeta.getGsiTableMetaBean().gsiMetaBean;
            if (gsiMetaBean.nonUnique) {
                continue;
            }
            if (gsiMetaBean.indexColumns == null) {
                return true;
            }
            final Set<String> indexColumns = getColumnSet(gsiMetaBean.indexColumns.stream()
                .map(column -> column.columnName)
                .collect(Collectors.toList()));
            if (sameColumns(uniqueKey, indexColumns)) {
                return true;
            }
        }
        return false;
    }

    private static IndexMeta getPlainLocalUniqueIndex(TableMeta tableMeta, Set<String> uniqueKey) {
        final List<IndexMeta> matchingIndexes = getMatchingLocalUniqueIndexes(tableMeta, uniqueKey, false);
        if (matchingIndexes.isEmpty()
            || matchingIndexes.stream().anyMatch(indexMeta -> !isPlainFullLengthIndex(indexMeta))) {
            return null;
        }
        return matchingIndexes.get(0);
    }

    private static IndexMeta getPlainPrimaryIndex(TableMeta tableMeta, Set<String> primaryKey) {
        final IndexMeta primaryIndex = tableMeta.getPrimaryIndex();
        if (primaryIndex == null
            || !sameColumns(primaryKey, getColumnSet(primaryIndex.getKeyColumns().stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList())))) {
            return null;
        }
        final List<IndexMeta> matchingIndexes = getMatchingLocalUniqueIndexes(tableMeta, primaryKey, true);
        if (matchingIndexes.isEmpty()
            || matchingIndexes.stream().anyMatch(indexMeta -> !isPlainFullLengthIndex(indexMeta))) {
            return null;
        }
        return primaryIndex;
    }

    private static List<IndexMeta> getMatchingLocalUniqueIndexes(TableMeta tableMeta, Set<String> uniqueKey,
                                                                 boolean includingPrimaryIndex) {
        return tableMeta.getUniqueIndexes(includingPrimaryIndex).stream()
            .filter(indexMeta -> sameColumns(uniqueKey, getColumnSet(indexMeta.getKeyColumns().stream()
                .map(ColumnMeta::getName)
                .collect(Collectors.toList()))))
            .collect(Collectors.toList());
    }

    private static boolean isPlainFullLengthIndex(IndexMeta indexMeta) {
        return !indexMeta.isFunctionIndex()
            && indexMeta.getKeyColumnsExt().stream()
            .allMatch(indexColumn -> indexColumn.hasColumn() && indexColumn.getSubPart() == 0);
    }

    private static void applyPartitionLocalScopes(Map<String, List<UkCheckEntry>> candidate, List<String> uniqueKey,
                                                  Set<String> uniqueKeySet,
                                                  Map<String, String> partitionLocalScopes) {
        final Set<String> existingScopes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        candidate.forEach((tableName, entries) -> {
            final String localIndexName = partitionLocalScopes.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(tableName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
            for (int i = entries.size() - 1; i >= 0; i--) {
                if (!sameColumns(uniqueKeySet, getColumnSet(entries.get(i).getUkColumns()))) {
                    continue;
                }
                if (localIndexName == null) {
                    entries.remove(i);
                } else {
                    entries.set(i, new UkCheckEntry(uniqueKey, localIndexName, true));
                    existingScopes.add(tableName);
                }
            }
        });

        partitionLocalScopes.forEach((tableName, indexName) -> {
            if (!existingScopes.contains(tableName)) {
                candidate.computeIfAbsent(tableName, ignored -> new ArrayList<>())
                    .add(new UkCheckEntry(uniqueKey, indexName, true));
            }
        });
    }

    private static Set<String> getColumnSet(List<String> columns) {
        final Set<String> columnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        columnSet.addAll(columns);
        return columnSet;
    }

    private static boolean sameColumns(Set<String> left, Set<String> right) {
        return left.size() == right.size() && left.containsAll(right);
    }

    /**
     * if there are multiple tables contains the same uk, find the best table for all uk.
     * <p>
     * if primary key is the only uk, choose table with priority as below:
     * <pre>
     *     1. primary table partition by pk
     *     2. published gsi partition by pk
     *     3. primary table not partition by pk
     * </pre>
     * <p>
     * if exists uk other than pk, choose best table for uk first, with priority as below:
     * <pre>
     *     1. published gsi with partition key included in all uk columns
     * </pre>
     * <p>
     * if best table for uk exists, choose the best table for pk, with priority as below:
     * <pre>
     *     1. best table for uk is also partitioned by pk
     *     2. primary table partition by pk
     *     3. published gsi partitioned by pk
     *     4. primary table not partition by pk
     * </pre>
     *
     * @param ukAllTableMap map[uk index, tables contains this uk]
     * @param uniqueKeys map[uk index, columns of this uk]
     */
    private static @NotNull Map<String, List<List<String>>> findBestTableForAllUk(
        Map<Integer, List<String>> ukAllTableMap,
        List<List<String>> uniqueKeys,
        Set<String> pkSet,
        String schemaName,
        String primaryTableName,
        boolean isGetDupForPkFromPrimaryOnly,
        ExecutionContext ec) {
        final Map<String, List<List<String>>> bestTableUkMap = new HashMap<>();

        final SchemaManager sm = ec.getSchemaManager(schemaName);

        final Set<String> tablesContainsPk = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Optional<List<String>> bestTableCandidates = ukAllTableMap
            .entrySet()
            .stream()
            .filter(e -> {
                final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                ukSet.addAll(uniqueKeys.get(e.getKey()));
                final boolean isPk = ukSet.containsAll(pkSet) && pkSet.containsAll(ukSet);
                if (isPk) {
                    tablesContainsPk.addAll(e.getValue());
                }
                // skip primary key candidates
                return !isPk;
            })
            .map(e -> e.getValue()
                .stream()
                // exclude unpublished gsi
                .filter(tableName -> isPrimaryOrPublishedGsi(primaryTableName, tableName, ec, sm))
                .collect(Collectors.toList()))
            // exclude empty table candidates list
            .filter(candidates -> !candidates.isEmpty())
            .findFirst();

        final boolean pkOnly = !bestTableCandidates.isPresent();

        final TddlRuleManager rm = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();

        // PK only
        if (pkOnly) {
            // Choose primary table by default
            String tableForPk = primaryTableName;

            if (!isGetDupForPkFromPrimaryOnly) {
                final Set<String> tablesPartitionByPk = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                tablesContainsPk.stream()
                    .filter(pkCandidate -> isTablePartitionByColumns(pkCandidate, pkSet, rm)
                        && isPrimaryOrPublishedGsi(primaryTableName, pkCandidate, ec, sm))
                    .forEach(tablesPartitionByPk::add);
                // Choose primary table if primary table partition by pk
                // or choose first published gsi table partition by pk
                if (!tablesPartitionByPk.contains(tableForPk) && !tablesPartitionByPk.isEmpty()) {
                    tableForPk = tablesPartitionByPk.iterator().next();
                }
            }

            bestTableUkMap.computeIfAbsent(
                    tableForPk.toUpperCase(),
                    k -> new ArrayList<>())
                .add(ImmutableList.copyOf(pkSet));
        } else {
            // UK exists
            for (String bestTable : bestTableCandidates.get()) {
                boolean bestForAllUk = true;
                final List<List<String>> ukList = new ArrayList<>();

                for (Map.Entry<Integer, List<String>> e : ukAllTableMap.entrySet()) {
                    final Set<String> ukTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    ukTableNames.addAll(e.getValue());
                    final Set<String> ukColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    ukColumnSet.addAll(uniqueKeys.get(e.getKey()));

                    if (ukColumnSet.containsAll(pkSet) && pkSet.containsAll(ukColumnSet)) {
                        // Check candidate best for pk later
                        continue;
                    }

                    if (!ukTableNames.contains(bestTable)
                        || !isTablePartitionByColumns(bestTable, ukColumnSet, rm)) {
                        bestForAllUk = false;
                        break;
                    }

                    ukList.add(uniqueKeys.get(e.getKey()));
                }

                if (bestForAllUk) {
                    bestTableUkMap.put(bestTable.toUpperCase(), ukList);

                    // Choose primary table by default
                    String tableForPk = primaryTableName;

                    if (!isGetDupForPkFromPrimaryOnly) {
                        final Set<String> tablesPartitionByPk = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                        tablesContainsPk.stream()
                            .filter(pkCandidate -> isTablePartitionByColumns(pkCandidate, pkSet, rm)
                                && isPrimaryOrPublishedGsi(primaryTableName, pkCandidate, ec, sm))
                            .forEach(tablesPartitionByPk::add);
                        // Choose best table for all uk if table is also partition by pk
                        // or choose primary table if primary table partition by pk
                        // or choose first published gsi partition by pk
                        if (tablesPartitionByPk.contains(bestTable)) {
                            tableForPk = bestTable;
                        } else if (!tablesPartitionByPk.contains(tableForPk) && !tablesPartitionByPk.isEmpty()) {
                            tableForPk = tablesPartitionByPk.iterator().next();
                        }
                    }

                    bestTableUkMap.computeIfAbsent(
                            tableForPk.toUpperCase(),
                            k -> new ArrayList<>())
                        .add(ImmutableList.copyOf(pkSet));

                    break;
                }
            }
        }
        return bestTableUkMap;
    }

    private static boolean isPrimaryOrPublishedGsi(String primaryTableName, String tableName, ExecutionContext ec,
                                                   SchemaManager sm) {
        return tableName.equalsIgnoreCase(primaryTableName)
            || GlobalIndexMeta.isPublished(ec, sm.getTable(tableName));
    }

    private Map<String, List<String>> getLocalIndexName(Map<String, List<List<String>>> tableUkMap, String schemaName,
                                                        ExecutionContext executionContext) {
        // Get local index name so that we can use FORCE INDEX later
        Map<String, List<String>> localIndexName = new HashMap<>();
        for (Map.Entry<String, List<List<String>>> entry : tableUkMap.entrySet()) {
            String tableName = entry.getKey();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            List<IndexMeta> indexMetas = tableMeta.getUniqueIndexes(true);
            for (List<String> uniqueKey : entry.getValue()) {
                // phyIndexName could be null since user may choose to select all uk from primary table, which may not
                // contains corresponding local uk
                String phyIndexName = null;
                for (IndexMeta indexMeta : indexMetas) {
                    Set<String> indexColumns = indexMeta.getKeyColumns().stream().map(cm -> cm.getName().toUpperCase())
                        .collect(Collectors.toCollection(HashSet::new));
                    if (indexColumns.size() == uniqueKey.size() && indexColumns.containsAll(uniqueKey)) {
                        phyIndexName = indexMeta.getPhysicalIndexName();
                        break;
                    }
                }
                localIndexName.computeIfAbsent(tableName, k -> new ArrayList<>()).add(phyIndexName);
            }
        }
        return localIndexName;
    }

    private String getUkTargetTable(String schemaName, String primaryTableName, Set<String> ukColumnSet,
                                    List<String> tableNames, ExecutionContext executionContext) {
        final TddlRuleManager rm = OptimizerContext.getContext(schemaName).getRuleManager();
        final SchemaManager sm = executionContext.getSchemaManager(schemaName);

        final Set<String> tablesPartitionedByUk = tableNames
            .stream()
            .filter(tableName -> isTablePartitionByColumns(tableName, ukColumnSet, rm))
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
        final Set<String> primaryTableAndPublicGsiNames = tableNames
            .stream()
            .filter(tableName -> isPrimaryOrPublishedGsi(primaryTableName, tableName, executionContext, sm))
            .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

        // If primary table is partitioned by uk
        if (tablesPartitionedByUk.contains(primaryTableName)) {
            return primaryTableName;
        }

        // Try to use table whose sharding key is included in this uk first to avoid full table scan, should
        // improve small batch performance
        for (String tableName : primaryTableAndPublicGsiNames) {
            if (tablesPartitionedByUk.contains(tableName)) {
                return tableName;
            }
        }

        // If no table partitioned by uk, check primary table contains uk first
        if (primaryTableAndPublicGsiNames.contains(primaryTableName)) {
            return primaryTableName;
        }

        for (String tableName : primaryTableAndPublicGsiNames) {
            return tableName;
        }

        // Only WRITE_ONLY GSI contains this UK
        for (String tableName : tableNames) {
            if (tablesPartitionedByUk.contains(tableName)) {
                return tableName;
            }
        }

        for (String tableName : tableNames) {
            return tableName;
        }

        // One of the UK can not find corresponding tables, which should be impossible
        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
            "can not find corresponding gsi for uk " + String.join(",", ukColumnSet));
    }

    private static @NotNull Boolean isTablePartitionByColumns(String tableName, Set<String> ukColumnSet,
                                                              TddlRuleManager rm) {
        return Optional.ofNullable(rm.getSharedColumns(tableName))
            .map(partitionKeys ->
                // skip single/broadcast table
                !partitionKeys.isEmpty()
                    && ukColumnSet.containsAll(partitionKeys))
            .orElse(false);
    }

    private LogicalInsert processOnDuplicateKeyUpdateForLogicalExecute(LogicalInsert upsert,
                                                                       AtomicInteger maxParamIndex,
                                                                       ExecutionContext ec) {
        final ReplaceRexWithParamHandlerCallBuilder handlerCallBuilder =
            new ReplaceRexWithParamHandlerCallBuilder(upsert, maxParamIndex, ec);

        final RexBuilder rexBuilder = upsert.getCluster().getRexBuilder();
        final List<RexNode> duplicateKeyUpdateList = upsert.getDuplicateKeyUpdateList().stream()
            .map(duplicateKeyUpdateItem -> {
                final RexCall rexCall = (RexCall) duplicateKeyUpdateItem;
                final RexInputRef columnRef = (RexInputRef) rexCall.getOperands().get(0);
                final int columnIndex = columnRef.getIndex();
                final RexNode rex = rexCall.getOperands().get(1);

                /**
                 * <pre>
                 * Check type and compute functions.
                 * If the function is a sharding key, compute it.
                 * If the function can not be pushed down ( like LAST_INSERT_ID() ), compute it.
                 * If the function can be pushed down, check its operands, which may be functions can't be pushed down.
                 * If the function is not deterministic, compute it.
                 * If a parent node need to be computed, its child node must also be computed.
                 * If a child node is cloned, its parent node must also be cloned.
                 * If the target column is column with implicit default on null, wrap function with IF(ISNULL(rexNode), implicitDefault, rexNode).
                 * </pre>
                 */
                final ReplaceRexWithParamHandlerCall call = handlerCallBuilder.buildLogicalDynamicValues(columnIndex,
                    rex,
                    true,
                    true);

                final RexHandlerChain<ReplaceRexWithParamHandlerCall> handlerChain =
                    RexHandlerChainFactory.create(call);

                final RexNode value = handlerChain.process(call).getResult();

                return rexBuilder.makeCall(rexCall.op, columnRef, value);
            }).collect(Collectors.toList());

        final LogicalInsert result = new LogicalInsert(upsert.getCluster(),
            upsert.getTraitSet(),
            upsert.getTable(),
            upsert.getCatalogReader(),
            upsert.getInput(),
            upsert.getOperation(),
            upsert.isFlattened(),
            upsert.getInsertRowType(),
            upsert.getKeywords(),
            duplicateKeyUpdateList,
            upsert.getBatchSize(),
            upsert.getAppendedColumnIndex(),
            upsert.getHints(),
            upsert.getTableInfo(),
            ImmutableList.copyOf(handlerCallBuilder.getDynamicImplicitDefaultParamMap().values()));
        result.setAutoIncParamIndex(upsert.getAutoIncParamIndex());

        return result;
    }

    private LogicalInsert processOnDuplicateKeyUpdateForNondeterministic(LogicalInsert upsert,
                                                                         AtomicInteger maxParamIndex,
                                                                         ExecutionContext ec) {
        final ReplaceRexWithParamHandlerCallBuilder handlerCallBuilder =
            new ReplaceRexWithParamHandlerCallBuilder(upsert, maxParamIndex, ec, true);

        // For batch upsert, we need to compute after-value for each row,
        // but we have only one ON DUPLICATE KEY UPDATE clause for each physical INSERT,
        // so that we need to compute the ON DUPLICATE KEY UPDATE clause on CN.
        final RexUtils.ColumnRefFinder columnRefFinder = new RexUtils.ColumnRefFinder(false);
        final RexBuilder rexBuilder = upsert.getCluster().getRexBuilder();
        final List<RexNode> duplicateKeyUpdateList = upsert.getDuplicateKeyUpdateList().stream()
            .map(duplicateKeyUpdateItem -> {
                final RexCall rexCall = (RexCall) duplicateKeyUpdateItem;
                final RexInputRef columnRef = (RexInputRef) rexCall.getOperands().get(0);
                final int columnIndex = columnRef.getIndex();
                final RexNode rex = rexCall.getOperands().get(1);

                /**
                 * <pre>
                 * Check type and compute functions.
                 * If the function is a sharding key, compute it.
                 * If the function can not be pushed down ( like LAST_INSERT_ID() ), compute it.
                 * If the function can be pushed down, check its operands, which may be functions can't be pushed down.
                 * If the function is not deterministic, compute it.
                 * If a parent node need to be computed, its child node must also be computed.
                 * If a child node is cloned, its parent node must also be cloned.
                 * If the target column is column with dynamic implicit default on null, wrap function with IF_NULL(rexNode, implicitDefault).
                 * </pre>
                 */
                final ReplaceRexWithParamHandlerCall call = handlerCallBuilder.buildOnDuplicateKeyUpdate(columnIndex,
                    rex,
                    rexBuilder,
                    false,
                    true,
                    columnRefFinder);

                final RexHandlerChain<ReplaceRexWithParamHandlerCall> chain =
                    handlerCallBuilder.withImplicitDefault(columnIndex) ?
                        RexHandlerChainFactory.create(call)
                        // For DeterministicPushDown, replace RexCall only
                        : RexHandlerChain.create(RexHandlerFactory.REPLACE_REX_CALL_WITH_PARAM_HANDLER);

                final RexNode value = chain.process(call).getResult();
                return rexBuilder.makeCall(rexCall.op, columnRef, value);
            }).collect(Collectors.toList());

        final LogicalInsert result = new LogicalInsert(upsert.getCluster(),
            upsert.getTraitSet(),
            upsert.getTable(),
            upsert.getCatalogReader(),
            upsert.getInput(),
            upsert.getOperation(),
            upsert.isFlattened(),
            upsert.getInsertRowType(),
            upsert.getKeywords(),
            duplicateKeyUpdateList,
            upsert.getBatchSize(),
            upsert.getAppendedColumnIndex(),
            upsert.getHints(),
            upsert.getTableInfo(),
            ImmutableList.copyOf(handlerCallBuilder.getDynamicImplicitDefaultParamMap().values()));
        result.setAutoIncParamIndex(upsert.getAutoIncParamIndex());
        return result;
    }

    private static boolean isModifyUniqueKey(List<String> updateColumnList, String logicalTableName, String schema,
                                             ExecutionContext ec) {
        final Set<String> ukColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        ukColumnSet.addAll(GlobalIndexMeta.getUniqueKeyColumnList(logicalTableName, schema, true, ec));
        return updateColumnList.stream().anyMatch(ukColumnSet::contains);
    }

    boolean checkInputInTableOrder(LogicalInsert logicalInsert, TableMeta tableMeta) {
        // Here all columns should be appended in tddlSqlToRelConverter
        List<String> tableColumns =
            tableMeta.getPhysicalColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        List<String> inputColumns = logicalInsert.getInsertRowType().getFieldNames();

        Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < tableColumns.size(); i++) {
            columnIndexMap.put(tableColumns.get(i), i);
        }
        int prev = -1;
        for (String column : inputColumns) {
            int current = columnIndexMap.get(column);
            if (current < prev) {
                return false;
            }
            prev = current;
        }
        return true;
    }

    /**
     * Get column that referenced by ON DUPLICATE KEY UPDATE for before value
     */
    private static class BeforeInputFinder extends RelOptUtil.InputFinder {
        public boolean withInputRef = false;

        public static BeforeInputFinder analyze(RexNode node) {
            final BeforeInputFinder inputFinder = new BeforeInputFinder();
            node.accept(inputFinder);
            return inputFinder;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            withInputRef = true;
            return super.visitInputRef(inputRef);
        }

        @Override
        public Void visitCall(RexCall call) {
            final SqlOperator op = call.getOperator();
            if ("VALUES".equalsIgnoreCase(op.getName())) {
                // Values(c1) is referencing after value of c1
                return null;
            }
            return super.visitCall(call);
        }
    }

    private static List<List<Integer>> initGsiColumnMapping(List<String> primaryColumnNames, List<TableMeta> gsiMetas) {
        final List<List<Integer>> gsiColumnMappings = new ArrayList<>();
        IntStream.range(0, primaryColumnNames.size()).forEach(i -> {
            final String column = primaryColumnNames.get(i);

            Ord.zip(gsiMetas).forEach(o -> {
                final TableMeta gsi = o.e;
                final int gsiIndex = o.i;
                if (gsiColumnMappings.size() <= gsiIndex) {
                    gsiColumnMappings.add(new ArrayList<>());
                }

                if (gsi.containsColumn(column)) {
                    gsiColumnMappings.get(gsiIndex).add(i);
                }
            });
        });
        return gsiColumnMappings;
    }

    /**
     * Replace rexCall in LogicalDynamicValues with RexCallParam
     * New parameter will be append to the end of current parameter row
     *
     * @param insert Insert plan
     * @param maxParamIndex Max parameter index of current parameter row
     * @return Insert plan with new LogicalDynamicValues
     */
    private LogicalInsert processRexCall(LogicalInsert insert, AtomicInteger maxParamIndex, ExecutionContext ec) {
        final ReplaceRexWithParamHandlerCallBuilder handlerCallBuilder =
            new ReplaceRexWithParamHandlerCallBuilder(insert, maxParamIndex, ec);

        final LogicalDynamicValues input = RelUtils.getRelInput(insert);
        final AtomicBoolean withRexCallParam = new AtomicBoolean(false);
        final ImmutableList.Builder<ImmutableList<RexNode>> tuplesBuilder = ImmutableList.builder();
        input.tuples.forEach(tuple -> {
            final ImmutableList.Builder<RexNode> tupleBuilder = ImmutableList.builder();
            Ord.zip(tuple).forEach(o -> {
                final int columnIndex = o.getKey();
                final RexNode rex = o.getValue();

                /**
                 * <pre>
                 * Check type and compute functions.
                 * If the function is a sharding key, compute it.
                 * If the function can not be pushed down ( like LAST_INSERT_ID() ), compute it.
                 * If the function can be pushed down, check its operands, which may be functions can't be pushed down.
                 * If the function is not deterministic, compute it.
                 * If a parent node need to be computed, its child node must also be computed.
                 * If a child node is cloned, its parent node must also be cloned.
                 * If the target column is column with implicit default on null, wrap function with IF(ISNULL(rexNode), implicitDefault, rexNode).
                 * </pre>
                 */
                final ReplaceRexWithParamHandlerCall call =
                    handlerCallBuilder.buildLogicalDynamicValues(columnIndex, rex);

                final RexHandlerChain<ReplaceRexWithParamHandlerCall> handlerChain =
                    RexHandlerChainFactory.create(call);

                final RexNode replaced = handlerChain.process(call).getResult();

                if (rex != replaced && !withRexCallParam.get()) {
                    withRexCallParam.set(true);
                }

                tupleBuilder.add(replaced);
            });

            tuplesBuilder.add(tupleBuilder.build());
        });

        LogicalInsert newInsert = insert;
        if (withRexCallParam.get()) {
            newInsert = new LogicalInsert(insert.getCluster(),
                insert.getTraitSet(),
                insert.getTable(),
                insert.getCatalogReader(),
                LogicalDynamicValues.createDrdsValues(input.getCluster(), input.getTraitSet(), input.getRowType(),
                    tuplesBuilder.build()),
                insert.getOperation(),
                insert.isFlattened(),
                insert.getInsertRowType(),
                insert.getKeywords(),
                insert.getDuplicateKeyUpdateList(),
                insert.getBatchSize(),
                insert.getAppendedColumnIndex(),
                insert.getHints(),
                insert.getTableInfo(),
                ImmutableList.copyOf(handlerCallBuilder.getDynamicImplicitDefaultParamMap().values()));
        }
        return newInsert;
    }

    /**
     * 1. Field for sequence is Literal: replace it with a dynamicParam.
     * 2. Field for sequence is DynamicParam: record its param index.
     *
     * @param logicalInsert original logicalInsert
     * @param maxParamIndex max param index of current current insert
     * @return the new logicalInsert
     */
    private LogicalInsert processAutoInc(LogicalInsert logicalInsert, AtomicInteger maxParamIndex,
                                         ExecutionContext ec) {
        final String tableName = logicalInsert.getLogicalTableName();
        final String schemaName = logicalInsert.getSchemaName();
        final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        final Set<String> autoIncColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final LogicalDynamicValues values = RelUtils.getRelInput(logicalInsert);
        final List<RelDataTypeField> fields = values.getRowType().getFieldList();

        autoIncColumns.addAll(tableMeta.getAutoIncrementColumns());

        // It has been checked that autoIncNode will only be dynamicParam or literal
        if (autoIncColumns.isEmpty()) {
            return logicalInsert;
        }

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        if (or.isTableInSingleDb(tableName) || or.isBroadCastOrReplicas(tableName)) {
            if (!SequenceManagerProxy.getInstance().isUsingSequence(schemaName, tableName)) {
                // It's using MySQL auto increment rule
                return logicalInsert;
            }
        }

        final Set<Integer> autoIncColumnIndex = new HashSet<>();
        Ord.zip(fields).stream().filter(o -> autoIncColumns.contains(o.getValue().getName()))
            .forEach(o -> autoIncColumnIndex.add(o.getKey()));

        final ImmutableList<ImmutableList<RexNode>> newTuples =
            replaceSequenceCallWithParam(logicalInsert, values.getTuples(), maxParamIndex, autoIncColumnIndex);

        final LogicalInsert result = new LogicalInsert(logicalInsert.getCluster(),
            logicalInsert.getTraitSet(),
            logicalInsert.getTable(),
            logicalInsert.getCatalogReader(),
            LogicalDynamicValues.createDrdsValues(
                values.getCluster(), values.getTraitSet(), values.getRowType(), newTuples),
            logicalInsert.getOperation(),
            logicalInsert.isFlattened(),
            logicalInsert.getInsertRowType(),
            logicalInsert.getKeywords(),
            logicalInsert.getDuplicateKeyUpdateList(),
            logicalInsert.getBatchSize(),
            logicalInsert.getAppendedColumnIndex(),
            logicalInsert.getHints(),
            logicalInsert.getTableInfo(),
            logicalInsert.getDynamicImplicitDefaultParams());

        return result;
    }

    public static ImmutableList<ImmutableList<RexNode>> replaceSequenceCallWithParam(LogicalInsert logicalInsert,
                                                                                     ImmutableList<ImmutableList<RexNode>> tuples,
                                                                                     AtomicInteger nextParamIndex,
                                                                                     Set<Integer> autoIncColumnIndex) {
        final String tableName = logicalInsert.getLogicalTableName();
        final List<RelDataTypeField> fields = logicalInsert.getInsertRowType().getFieldList();
        final RexBuilder rexBuilder = logicalInsert.getCluster().getRexBuilder();

        final ImmutableList.Builder<ImmutableList<RexNode>> tuplesBuilder = ImmutableList.builder();
        for (ImmutableList<RexNode> tuple : tuples) {
            final ImmutableList.Builder<RexNode> tupleBuilder = ImmutableList.builder();

            for (Ord<RexNode> o1 : Ord.zip(tuple)) {
                final Integer columnIndex = o1.getKey();
                final RelDataType fieldType = fields.get(columnIndex).getType();

                RexNode value = o1.getValue();

                final boolean isRexCallParam = value instanceof RexCallParam;
                final boolean isSeqCall = RexUtils.isSeqCall(value);
                final boolean isSeqCallInRexCall =
                    isRexCallParam && RexUtils.isSeqCall(((RexCallParam) value).getRexCall());

                if (isSeqCallInRexCall && autoIncColumnIndex.contains(columnIndex)) {
                    final RexCallParam callParam = (RexCallParam) value;
                    final RexCall seqCall = (RexCall) callParam.getRexCall();
                    final RexLiteral seqNameLiteral = (RexLiteral) seqCall.getOperands().get(0);
                    final String seqName = seqNameLiteral.getValueAs(String.class);

                    if ((ISequenceManager.AUTO_SEQ_PREFIX + tableName).equalsIgnoreCase(seqName)) {
                        final RexSequenceParam sequenceParam =
                            new RexSequenceParam(fieldType, callParam.getIndex(), seqCall);

                        value = new RexCallParam(callParam.getType(), callParam.getIndex(), rexBuilder.constantNull());
                        ((RexCallParam) value).setSequenceCall(sequenceParam);

                        tupleBuilder.add(value);
                        continue;
                    }
                }

                // Evaluate RexCall then replace dynamic parameter for implicit sequence call
                if (isRexCallParam && autoIncColumnIndex.contains(columnIndex)) {
                    final RexNode seqName = rexBuilder.makeLiteral(ISequenceManager.AUTO_SEQ_PREFIX + tableName);
                    final RexNode falseLiteral = rexBuilder.makeLiteral(false);
                    final RexNode sequenceCall = rexBuilder.makeCall(NEXTVAL, ImmutableList.of(seqName, falseLiteral));

                    final RexCallParam callParam = (RexCallParam) value;
                    callParam.setSequenceCall(new RexSequenceParam(fieldType, callParam.getIndex(), sequenceCall));

                    tupleBuilder.add(value);
                    continue;
                }

                // DO NOT replace NULL or RexLiteral with dynamic parameter for implicit sequence call,
                // because implicit sequence call has affect on result of last_insert_id

                // Replace seq.NextVal with dynamic parameter for explicit sequence call
                if (isSeqCall) {
                    value = new RexSequenceParam(fieldType, nextParamIndex.incrementAndGet(), value);
                }

                tupleBuilder.add(value);
            }

            tuplesBuilder.add(tupleBuilder.build());
        }

        return tuplesBuilder.build();
    }

    /**
     * Replace RexCall and Sequence with RexDynamicParam
     *
     * @param logicalExecute Logical execute means SELECT all rows might be affected first, then execute INSERT/UPDATE/DELETE with condition of primary key
     */
    private LogicalInsert replaceExpAndSeqWithParam(LogicalInsert origin, boolean logicalExecute,
                                                    boolean replaceNondeterministicOnly, ExecutionContext ec) {
        final boolean sourceSelect = origin.isSourceSelect();

        LogicalInsert result = (LogicalInsert) origin.copy(origin.getTraitSet(), origin.getInputs());
        final AtomicInteger maxParamIndex = getMaxParamIndex(origin);

        if (!sourceSelect) {
            // Replace expression with parameter
            result = processRexCall(origin, maxParamIndex, ec);

            // Add parameter for auto increment column which is not included in origin sql
            result = processAutoInc(result, maxParamIndex, ec);
        }

        // Add parameter for ON DUPLICATE KEY UPDATE list
        if (origin.isUpsert()) {
            if (logicalExecute) {
                result = processOnDuplicateKeyUpdateForLogicalExecute(result, maxParamIndex, ec);
            } else if (replaceNondeterministicOnly) {
                // For broadcast table, here is a little bit tricky
                result = processOnDuplicateKeyUpdateForNondeterministic(result, maxParamIndex, ec);
            }
        }

        boolean rebuildMultiValues = !sourceSelect && (logicalExecute || replaceNondeterministicOnly);
        if (rebuildMultiValues) {
            final LogicalDynamicValues oldInput = RelUtils.getRelInput(result);
            rebuildMultiValues &= oldInput.getTuples().size() > 1;
        }

        LogicalInsert newInsertOrReplace = result;
        if (rebuildMultiValues) {
            /**
             * <pre>
             * Build a new LogicalInsert with new LogicalDynamicValues, also save the old LogicalDynamicValues for later
             * compute the Parameters in RexUtils.calculateAndUpdateAllRexCallParams().
             * the new LogicalDynamicValues only has one tuple, we will execute the LogicalInsert in batch mode, for the batch
             * parameters(for each tuple), we will compute them in the execution time.
             * for the case:
             * insert into t1(a,b) values(1,now()),(2+3,'2010-10-12 12:12:12'),(5, null)
             * --> change to -->
             * insert into t1(a,b) values(?,?), and save the old LogicalDynamicValues[(?,?),(?,?),(?, ?)]
             * </pre>
             */
            final String schemaName = origin.getSchemaName();
            final String tableName = origin.getLogicalTableName();
            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

            final List<Integer> autoIncParamIndex = new ArrayList<>();
            final LogicalDynamicValues oldInput = RelUtils.getRelInput(result);
            final LogicalDynamicValues newInput = Optional.of(oldInput.getTuples()).filter(i -> i.size() > 1).map(
                    i -> LogicalDynamicValues
                        .createDrdsValues(oldInput.getCluster(), oldInput.getTraitSet(), oldInput.getRowType(),
                            buildNewTupleForLogicalDynamicValue(oldInput, autoIncParamIndex, ec, tableMeta)))
                .orElse(oldInput);
            final int batchSize =
                (result.getBatchSize() == 0 && oldInput.getTuples().size() > 1) ? oldInput.getTuples().size() :
                    result.getBatchSize();
            newInsertOrReplace =
                new LogicalInsert(result.getCluster(), result.getTraitSet(), result.getTable(),
                    result.getCatalogReader(), newInput, result.getOperation(),
                    result.isFlattened(), result.getInsertRowType(), result.getKeywords(),
                    result.getDuplicateKeyUpdateList(), batchSize/*set the batch size*/,
                    result.getAppendedColumnIndex(), result.getHints(),
                    result.getTableInfo(), result.getPrimaryInsertWriter(),
                    result.getGsiInsertWriters(), autoIncParamIndex, null, null, result.getEvalRowColMetas(),
                    result.getGenColRexNodes(), result.getInputToEvalFieldsMapping(), result.getDefaultExprColMetas(),
                    result.getDefaultExprColRexNodes(), result.getDefaultExprEvalFieldsMapping(),
                    result.isPushablePrimaryKeyCheck(), result.isPushableForeignConstraintCheck(),
                    result.isModifyForeignKey(), result.isUkContainsAllSkAndGsiContainsAllUk(),
                    result.isCanSkipPkCheck(),
                    new ArrayList<>(), null);
            /**
             * 2、update the index of RexDynamicParam in onDuplicatedUpdate list recursively
             * how to update them? firstly, find out the minimum RexDynamicPara and compute the offset
             * between the maximum RexDynamicParam of the new param(the last column of the last row),
             * then update the index of RexDynamicParam in onDuplicatedUpdate list recursively by plus the offset
             */
            if (GeneralUtil.isNotEmpty(newInsertOrReplace.getDuplicateKeyUpdateList())) {
                final List<RexNode> onDuplicatedUpdate = newInsertOrReplace.getDuplicateKeyUpdateList();
                RexUtils.FindMinDynamicParam findMinDynamicParam = new RexUtils.FindMinDynamicParam();
                onDuplicatedUpdate.forEach(o -> ((RexCall) o).getOperands().get(1).accept(findMinDynamicParam));

                final AtomicInteger mapParamIndex = new AtomicInteger(0);
                final List<RexDynamicParam> params = newInput.tuples.stream().flatMap(Collection::stream)
                    .flatMap(p -> RexUtils.ParamFinder.getParams(p).stream()).collect(Collectors.toList());
                params.stream().map(RexDynamicParam::getIndex).max(Integer::compareTo).ifPresent(mapParamIndex::set);

                int minPara = findMinDynamicParam.getMinDynamicParam();
                if (minPara != Integer.MAX_VALUE) {
                    int tupleSize = result.getBatchSize() == 0 ? oldInput.getTuples().size() : result.getBatchSize();
                    int offset = (mapParamIndex.get() + 1) * tupleSize - minPara;
                    RexUtils.ReplaceDynamicParam replaceDynamicParam = new RexUtils.ReplaceDynamicParam(offset);
                    //no action need for offset = 0
                    if (offset != 0) {
                        final RexBuilder rexBuilder = newInsertOrReplace.getCluster().getRexBuilder();
                        final List<RexNode> newOnDuplicatedUpdate = new ArrayList<>(onDuplicatedUpdate.size());
                        onDuplicatedUpdate
                            .forEach(o -> {
                                RexNode rexNode = ((RexCall) o).getOperands().get(1).accept(replaceDynamicParam);
                                List<RexNode> operands = new ArrayList<>(2);
                                operands.add(((RexCall) o).getOperands().get(0));
                                operands.add(rexNode);
                                RexNode rexCall = rexBuilder.makeCall(((RexCall) o).getOperator(), operands);
                                newOnDuplicatedUpdate.add(rexCall);
                            });
                        newInsertOrReplace.setDuplicateKeyUpdateList(newOnDuplicatedUpdate);
                    }
                }
            }
            newInsertOrReplace.setUnOptimizedLogicalDynamicValues(oldInput);
            newInsertOrReplace.setUnOptimizedDuplicateKeyUpdateList(result.getDuplicateKeyUpdateList());
            newInsertOrReplace.setUnoptimizedDynamicImplicitDefaultParams(result.getDynamicImplicitDefaultParams());
        }
        return RelUtils.removeHepRelVertex(newInsertOrReplace);
    }

    private static AtomicInteger getMaxParamIndex(LogicalInsert insertOrReplace) {
        final boolean sourceSelect = insertOrReplace.isSourceSelect();

        final AtomicInteger maxParamIndex = new AtomicInteger(-1);
        if (sourceSelect) {
            // Get max parameter index
            if (insertOrReplace.isUpsert()) {
                final List<RexDynamicParam> params = Optional.ofNullable(insertOrReplace.getDuplicateKeyUpdateList())
                    .map(dl -> dl.stream().flatMap(p -> RexUtils.ParamFinder.getParams(p).stream())
                        .collect(Collectors.toList())).orElseGet(ArrayList::new);

                params.stream().map(RexDynamicParam::getIndex).max(Integer::compareTo).ifPresent(maxParamIndex::set);
            } else {
                maxParamIndex.set(insertOrReplace.getInsertRowType().getFieldCount());
            }
        } else {
            final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);
            final List<RexDynamicParam> params = input.tuples.stream().flatMap(Collection::stream)
                .flatMap(p -> RexUtils.ParamFinder.getParams(p).stream()).collect(Collectors.toList());

            params.addAll(Optional.ofNullable(insertOrReplace.getDuplicateKeyUpdateList()).map(
                    dl -> dl.stream().flatMap(p -> RexUtils.ParamFinder.getParams(p).stream()).collect(Collectors.toList()))
                .orElseGet(ArrayList::new));

            params.stream().map(RexDynamicParam::getIndex).max(Integer::compareTo).ifPresent(maxParamIndex::set);
        }
        return maxParamIndex;
    }

    /**
     * Return the highest parameter index that is already owned by the current plan, including the index of a
     * {@link RexCallParam} itself.
     *
     * <p>{@link RexUtils.ParamFinder} intentionally unwraps a RexCallParam and reports only the dynamic parameters
     * referenced by its expression. That behavior is correct for expression dependency analysis, but not for
     * allocating another execution parameter: SQL2Rel's synthetic MCE addr NULL has already become a RexCallParam,
     * and reusing its index would alias the incoming addr slot with the UPSERT conflict addr slot.
     */
    private static AtomicInteger getMaxAllocatedParamIndex(LogicalInsert insertOrReplace) {
        final List<RexNode> allocatedParams = new ArrayList<>();
        if (!insertOrReplace.isSourceSelect()) {
            final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);
            input.tuples.forEach(allocatedParams::addAll);
        }
        Optional.ofNullable(insertOrReplace.getDuplicateKeyUpdateList()).ifPresent(allocatedParams::addAll);
        return new AtomicInteger(maxAllocatedParamIndex(allocatedParams));
    }

    static int maxAllocatedParamIndex(List<? extends RexNode> rexNodes) {
        return rexNodes.stream()
            .flatMap(rex -> RexUtils.getDynamicParams(rex).stream())
            .map(RexDynamicParam::getIndex)
            .max(Integer::compareTo)
            .orElse(-1);
    }

    private ImmutableList<ImmutableList<RexNode>> buildNewTupleForLogicalDynamicValue(LogicalDynamicValues sourceRel,
                                                                                      List<Integer> autoIncParamIndex,
                                                                                      ExecutionContext ec,
                                                                                      TableMeta tableMeta) {
        final ImmutableList.Builder<ImmutableList<RexNode>> tuplesBuilder = ImmutableList.builder();
        final RexBuilder rexBuilder = sourceRel.getCluster().getRexBuilder();

        final AtomicInteger sequenceParamIndex = new AtomicInteger(0);
        sequenceParamIndex.addAndGet(
            sourceRel.getTuples().get(0).stream()
                .filter(r -> !(r instanceof RexSequenceParam || r instanceof RexLiteral))
                .mapToInt(r -> 1).sum());

        final AtomicInteger rexIndex = new AtomicInteger(0);
        ImmutableList<RexNode> tuple = sourceRel.tuples.get(0);
        final ImmutableList.Builder<RexNode> tupleBuilder = ImmutableList.builder();
        Ord.zip(tuple).forEach(o -> {
            final RexNode rex = o.getValue();
            if (rex instanceof RexSequenceParam) {
                final RexSequenceParam seqCall = (RexSequenceParam) rex;
                final int seqParamIndex = sequenceParamIndex.getAndIncrement();
                tupleBuilder.add(new RexSequenceParam(rex.getType(), seqParamIndex, seqCall.getSequenceCall()));
                if (null != autoIncParamIndex) {
                    autoIncParamIndex.add(seqParamIndex);
                }
            } else {
                tupleBuilder.add(rexBuilder.makeDynamicParam(rex.getType(), rexIndex.getAndIncrement()));
            }
        });

        tuplesBuilder.add(tupleBuilder.build());
        return tuplesBuilder.build();
    }
}

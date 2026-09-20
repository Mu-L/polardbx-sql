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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadCastReplaceScaleOutWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * REPLACE on sharding table
 *
 * @author chenmo.cm
 */
public class LogicalReplace extends LogicalInsertIgnore {

    private final ReplaceRelocateWriter primaryRelocateWriter;
    private final List<ReplaceRelocateWriter> gsiRelocateWriters;
    private final BroadCastReplaceScaleOutWriter broadCastReplaceScaleOutWriter;

    private final boolean hasJsonColumn;
    private final boolean canUseReturning;

    public LogicalReplace(LogicalInsert insert,
                          InsertWriter primaryInsertWriter,
                          ReplaceRelocateWriter primaryRelocateWriter,
                          List<InsertWriter> gsiInsertWriters,
                          List<ReplaceRelocateWriter> gsiRelocateWriters,
                          DistinctWriter primaryDeleteWriter,
                          List<DistinctWriter> gsiDeleteWriters,
                          BroadCastReplaceScaleOutWriter broadCastReplaceScaleOutWriter,
                          List<String> selectListForDuplicateCheck,
                          boolean hasJsonColumn, boolean canUseReturning
    ) {
        super(insert.getCluster(),
            insert.getTraitSet(),
            insert.getTable(),
            insert.getCatalogReader(),
            insert.getInput(),
            Operation.REPLACE,
            insert.isFlattened(),
            insert.getInsertRowType(),
            insert.getKeywords(),
            ImmutableList.of(),
            insert.getBatchSize(),
            insert.getAppendedColumnIndex(),
            insert.getHints(),
            insert.getTableInfo(),
            insert.getPrimaryInsertWriter(),
            insert.getGsiInsertWriters(),
            insert.getAutoIncParamIndex(),
            selectListForDuplicateCheck,
            initColumnMeta(insert),
            initTableColumnMeta(insert),
            insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList(),
            insert.getPushDownInsertWriter(),
            insert.getGsiInsertIgnoreWriters(),
            insert.getPrimaryDeleteWriter(),
            insert.getGsiDeleteWriters(),
            insert.getEvalRowColMetas(),
            insert.getGenColRexNodes(),
            insert.getInputToEvalFieldsMapping(),
            insert.getDefaultExprColMetas(),
            insert.getDefaultExprColRexNodes(),
            insert.getDefaultExprEvalFieldsMapping(),
            insert.isPushablePrimaryKeyCheck(),
            insert.isPushableForeignConstraintCheck(),
            insert.isModifyForeignKey(),
            insert.isUkContainsAllSkAndGsiContainsAllUk(),
            insert.isCanSkipPkCheck(),
            insert.getDynamicImplicitDefaultParams(),
            insert.getUnoptimizedDynamicImplicitDefaultParams()
        );
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.canUseReturning = canUseReturning;
        this.primaryInsertWriter = primaryInsertWriter;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.gsiInsertWriters = gsiInsertWriters;
        this.primaryDeleteWriter = primaryDeleteWriter;
        this.gsiDeleteWriters = gsiDeleteWriters;
        this.broadCastReplaceScaleOutWriter = broadCastReplaceScaleOutWriter;
        this.hasJsonColumn = hasJsonColumn;
    }

    protected LogicalReplace(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                             Prepare.CatalogReader catalogReader, RelNode input, Operation operation, boolean flattened,
                             RelDataType insertRowType, List<String> keywords, List<RexNode> duplicateKeyUpdateList,
                             int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfo,
                             InsertWriter primaryInsertWriter, List<InsertWriter> gsiInsertWriters,
                             List<Integer> autoIncParamIndex, List<List<String>> ukColumnNamesList,
                             List<List<Integer>> beforeUkMapping, List<List<Integer>> afterUkMapping,
                             List<Integer> afterUgsiUkMapping, List<Integer> selectInsertRowMapping,
                             List<String> pkColumnNames, List<Integer> beforePkMapping, List<Integer> afterPkMapping,
                             Set<String> allUkSet, Map<String, Map<String, Set<String>>> tableUkMap,
                             Map<String, List<List<String>>> ukGroupByTable,
                             Map<String, List<String>> localIndexPhyName, List<ColumnMeta> rowColumnMetas,
                             List<ColumnMeta> tableColumnMetas, List<String> selectListForDuplicateCheck,
                             ReplaceRelocateWriter primaryRelocateWriter,
                             List<ReplaceRelocateWriter> gsiRelocateWriters,
                             BroadCastReplaceScaleOutWriter broadCastReplaceScaleOutWriter,
                             boolean targetTableIsWritable, boolean targetTableIsReadyToPublish,
                             boolean sourceTablesIsReadyToPublish, LogicalDynamicValues logicalDynamicValues,
                             List<RexNode> unOpitimizedDuplicateKeyUpdateList, InsertWriter pushDownInsertWriter,
                             List<InsertWriter> gsiInsertIgnoreWriter, DistinctWriter primaryDeleteWriter,
                             List<DistinctWriter> gsiDeleteWriters, boolean usePartFieldChecker, boolean hasJsonColumn,
                             Map<String, ColumnMeta> columnMetaMap, boolean ukContainGeneratedColumn,
                             List<ColumnMeta> evalRowColMetas, List<RexNode> genColRexNodes,
                             List<Integer> inputToEvalFieldsMapping, List<ColumnMeta> defaultExprColMetas,
                             List<RexNode> defaultExprColRexNodes, List<Integer> defaultExprEvalFieldsMapping,
                             boolean pushablePrimaryKeyCheck, boolean pushableForeignConstraintCheck,
                             boolean modifyForeignKey, boolean ukContainsAllSkAndGsiContainsAllUk,
                             boolean canSkipPkCheck,
                             List<RexCallParam> dynamicImplicitDefaultParams,
                             List<RexCallParam> unoptimizedDynamicImplicitDefaultParams, boolean canUseReturning) {
        super(cluster, traitSet, table, catalogReader, input, operation, flattened, insertRowType, keywords,
            duplicateKeyUpdateList, batchSize, appendedColumnIndex, hints, tableInfo, primaryInsertWriter,
            gsiInsertWriters, autoIncParamIndex, ukColumnNamesList, beforeUkMapping, afterUkMapping, afterUgsiUkMapping,
            selectInsertRowMapping, pkColumnNames, beforePkMapping, afterPkMapping, allUkSet, tableUkMap,
            ukGroupByTable, localIndexPhyName, rowColumnMetas, tableColumnMetas, selectListForDuplicateCheck,
            targetTableIsWritable, targetTableIsReadyToPublish, sourceTablesIsReadyToPublish, logicalDynamicValues,
            unOpitimizedDuplicateKeyUpdateList, pushDownInsertWriter, gsiInsertIgnoreWriter, primaryDeleteWriter,
            gsiDeleteWriters, usePartFieldChecker, columnMetaMap, ukContainGeneratedColumn, evalRowColMetas,
            genColRexNodes, inputToEvalFieldsMapping, defaultExprColMetas, defaultExprColRexNodes,
            defaultExprEvalFieldsMapping, pushablePrimaryKeyCheck, pushableForeignConstraintCheck, modifyForeignKey,
            ukContainsAllSkAndGsiContainsAllUk, canSkipPkCheck, dynamicImplicitDefaultParams,
            unoptimizedDynamicImplicitDefaultParams);
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.broadCastReplaceScaleOutWriter = broadCastReplaceScaleOutWriter;
        this.hasJsonColumn = hasJsonColumn;
        this.canUseReturning = canUseReturning;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        final LogicalReplace newLogicalReplace = new LogicalReplace(getCluster(),
            traitSet,
            table,
            catalogReader,
            sole(inputs),
            getOperation(),
            isFlattened(),
            getInsertRowType(),
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getPrimaryInsertWriter(),
            getGsiInsertWriters(),
            getAutoIncParamIndex(),
            getUkColumnNamesList(),
            getBeforeUkMapping(),
            getAfterUkMapping(),
            getAfterUgsiUkIndex(),
            getSelectInsertColumnMapping(),
            getPkColumnNames(),
            getBeforePkMapping(),
            getAfterPkMapping(),
            getAllUkSet(),
            getTableUkMap(),
            getUkGroupByTable(),
            getLocalIndexPhyName(),
            getRowColumnMetaList(),
            getTableColumnMetaList(),
            getSelectListForDuplicateCheck(),
            getPrimaryRelocateWriter(),
            getGsiRelocateWriters(),
            getBroadCastReplaceScaleOutWriter(),
            isTargetTableIsWritable(),
            isTargetTableIsReadyToPublish(),
            isSourceTablesIsReadyToPublish(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getPushDownInsertWriter(),
            getGsiInsertIgnoreWriters(),
            getPrimaryDeleteWriter(),
            getGsiDeleteWriters(),
            isUsePartFieldChecker(),
            isHasJsonColumn(),
            getColumnMetaMap(),
            isUkContainGeneratedColumn(),
            getEvalRowColMetas(),
            getGenColRexNodes(),
            getInputToEvalFieldsMapping(),
            getDefaultExprColMetas(),
            getDefaultExprColRexNodes(),
            getDefaultExprEvalFieldsMapping(),
            isPushablePrimaryKeyCheck(),
            isPushableForeignConstraintCheck(),
            isModifyForeignKey(),
            isUkContainsAllSkAndGsiContainsAllUk(),
            isCanSkipPkCheck(),
            getDynamicImplicitDefaultParams(),
            getUnoptimizedDynamicImplicitDefaultParams(),
            canUseReturning);
        newLogicalReplace.getCandidateUkChecks().putAll(getCandidateUkChecks());
        return newLogicalReplace;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs, List<RexCallParam> dynamicImplicitDefaultParams) {
        final LogicalReplace newLogicalReplace = new LogicalReplace(getCluster(),
            traitSet,
            table,
            catalogReader,
            sole(inputs),
            getOperation(),
            isFlattened(),
            getInsertRowType(),
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getPrimaryInsertWriter(),
            getGsiInsertWriters(),
            getAutoIncParamIndex(),
            getUkColumnNamesList(),
            getBeforeUkMapping(),
            getAfterUkMapping(),
            getAfterUgsiUkIndex(),
            getSelectInsertColumnMapping(),
            getPkColumnNames(),
            getBeforePkMapping(),
            getAfterPkMapping(),
            getAllUkSet(),
            getTableUkMap(),
            getUkGroupByTable(),
            getLocalIndexPhyName(),
            getRowColumnMetaList(),
            getTableColumnMetaList(),
            getSelectListForDuplicateCheck(),
            getPrimaryRelocateWriter(),
            getGsiRelocateWriters(),
            getBroadCastReplaceScaleOutWriter(),
            isTargetTableIsWritable(),
            isTargetTableIsReadyToPublish(),
            isSourceTablesIsReadyToPublish(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getPushDownInsertWriter(),
            getGsiInsertIgnoreWriters(),
            getPrimaryDeleteWriter(),
            getGsiDeleteWriters(),
            isUsePartFieldChecker(),
            isHasJsonColumn(),
            getColumnMetaMap(),
            isUkContainGeneratedColumn(),
            getEvalRowColMetas(),
            getGenColRexNodes(),
            getInputToEvalFieldsMapping(),
            getDefaultExprColMetas(),
            getDefaultExprColRexNodes(),
            getDefaultExprEvalFieldsMapping(),
            isPushablePrimaryKeyCheck(),
            isPushableForeignConstraintCheck(),
            isModifyForeignKey(),
            isUkContainsAllSkAndGsiContainsAllUk(),
            isCanSkipPkCheck(),
            dynamicImplicitDefaultParams,
            getUnoptimizedDynamicImplicitDefaultParams(),
            canUseReturning);
        newLogicalReplace.getCandidateUkChecks().putAll(getCandidateUkChecks());
        return newLogicalReplace;
    }

    public ReplaceRelocateWriter getPrimaryRelocateWriter() {
        return primaryRelocateWriter;
    }

    public List<ReplaceRelocateWriter> getGsiRelocateWriters() {
        return gsiRelocateWriters;
    }

    public BroadCastReplaceScaleOutWriter getBroadCastReplaceScaleOutWriter() {
        return broadCastReplaceScaleOutWriter;
    }

    @Override
    public <R extends LogicalInsert> List<RelNode> getPhyPlanForDisplay(ExecutionContext executionContext,
                                                                        R replace) {
        final InsertWriter primaryWriter = replace.getPrimaryInsertWriter();
        final LogicalInsert insert = primaryWriter.getInsert();
        final LogicalInsert copied = new LogicalInsert(insert.getCluster(), insert.getTraitSet(), insert.getTable(),
            insert.getCatalogReader(), insert.getInput(), Operation.REPLACE, insert.isFlattened(),
            insert.getInsertRowType(), insert.getKeywords(), insert.getDuplicateKeyUpdateList(),
            insert.getBatchSize(), insert.getAppendedColumnIndex(), insert.getHints(), insert.getTableInfo(), null,
            new ArrayList<>(), insert.getAutoIncParamIndex(), insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList(), insert.getEvalRowColMetas(), insert.getGenColRexNodes(),
            insert.getInputToEvalFieldsMapping(), insert.getDefaultExprColMetas(), insert.getDefaultExprColRexNodes(),
            insert.getDefaultExprEvalFieldsMapping(), insert.isPushablePrimaryKeyCheck(),
            insert.isPushableForeignConstraintCheck(), isModifyForeignKey(), isUkContainsAllSkAndGsiContainsAllUk(),
            isCanSkipPkCheck(),
            getDynamicImplicitDefaultParams(), getUnoptimizedDynamicImplicitDefaultParams());

        final InsertWriter replaceWriter = new InsertWriter(primaryWriter.getTargetTable(), copied);
        return replaceWriter.getInput(executionContext);
    }

    @Override
    public InsertWriter getPrimaryInsertWriter() {
        return Optional.ofNullable(this.primaryInsertWriter)
            .orElseGet(() -> getPrimaryRelocateWriter().getModifyWriter().unwrap(InsertWriter.class));
    }

    private List<String> getPrimaryKeys(TableMeta tableMeta) {
        return tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).map(String::toLowerCase)
            .collect(Collectors.toList());
    }

    private List<String> getPrimaryShardingKeys(TableMeta tableMeta, ExecutionContext executionContext,
                                                String schemaName,
                                                String tableName) {
        if (tableMeta.getPartitionInfo() != null) {
            // for auto
            return tableMeta.getPartitionInfo().getPartitionColumns().stream().map(String::toLowerCase)
                .collect(Collectors.toList());
        } else {
            // for drds
            TddlRule tddlRule = executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTddlRule();
            TableRule tableRule = tddlRule.getTable(tableName);
            return tableRule.getShardColumns().stream().map(String::toLowerCase).collect(Collectors.toList());
        }
    }

    private void populateGsiShardingKeys(TableMeta tableMeta, Map<String, List<String>> gsiShardingKeys,
                                         LinkedHashSet<String> deduplicatedColumns) {
        tableMeta.getGsiPublished().forEach((indexName, gsiIndexMetaBean) -> {
            gsiShardingKeys.put(indexName.toLowerCase(),
                Objects.requireNonNull(OptimizerContext.getContext(gsiIndexMetaBean.tableSchema)).getRuleManager()
                    .getSharedColumns(gsiIndexMetaBean.indexName));
            deduplicatedColumns.addAll(gsiShardingKeys.get(indexName.toLowerCase()));
        });
    }

    private List<String> populatePkSkColumns(TableMeta tableMeta, ExecutionContext executionContext, String schemaName,
                                             String tableName) {
        // 从tableMeta中拿到主键、分区键以及所有GSI的分区键
        final List<String> primaryKeys = getPrimaryKeys(tableMeta);
        List<String> primaryShardingKeys = getPrimaryShardingKeys(tableMeta, executionContext, schemaName, tableName);

        // index name -> gsi sharding keys
        final Map<String, List<String>> gsiShardingKeys = new HashMap<>();

        // 所有期望拿回的列
        final LinkedHashSet<String> deduplicatedColumns = new LinkedHashSet<>(primaryKeys);
        deduplicatedColumns.addAll(primaryShardingKeys);

        // put all gsi sharding key to deduplicatedColumns
        populateGsiShardingKeys(tableMeta, gsiShardingKeys, deduplicatedColumns);

        return new ArrayList<>(deduplicatedColumns);
    }

    private boolean checkGsiCoverAllSk(TableMeta tableMeta, ExecutionContext ec) {

        String schemaName = tableMeta.getSchemaName();
        String tableName = tableMeta.getTableName();
        if (tableMeta.getGsiPublished() == null || tableMeta.getGsiPublished().size() <= 1) {
            // 如果没有GSI或只有一个GSI，则不需要检查
            return true;
        }

        final List<String> returningColumns = populatePkSkColumns(tableMeta, ec, schemaName, tableName);
        // 检查每个GSI是否都包含所有returningColumns
        for (GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean : tableMeta.getGsiPublished().values()) {
            List<String> gsiColumns =
                gsiIndexMetaBean.getIndexColumns().stream().map(GsiMetaManager.GsiIndexColumnMetaBean::getColumnName)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            gsiColumns.addAll(
                gsiIndexMetaBean.getCoveringColumns().stream().map(GsiMetaManager.GsiIndexColumnMetaBean::getColumnName)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList()));
            if (!new HashSet<>(gsiColumns).containsAll(returningColumns)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String explainNodeName() {
        if (isReplace()) {
            return "LogicalReplace";
        } else if (withDuplicateKeyUpdate()) {
            return "LogicalUpsert";
        }
        return "LogicalInsertIgnore";
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExecutionContext executionContext = null;
        if (pw instanceof RelDrdsWriter) {
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        }

        TableMeta tableMeta =
            executionContext.getSchemaManager(this.getSchemaName()).getTable(this.getLogicalTableName());
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());
        final TddlRuleManager or = OptimizerContext.getContext(this.getSchemaName()).getRuleManager();
        final boolean isBroadcast = or.isBroadCastOrReplicas(this.getLogicalTableName());

        // for replace returning, gsi can use returning only on published state
        final boolean gsiCanUseReturning = GlobalIndexMeta
            .isAllGsi(this.getTargetTables().get(0), executionContext, GlobalIndexMeta::isPublished);
        final boolean isColumnMultiWriting =
            TableColumnUtils.isModifying(this.getSchemaName(), this.getLogicalTableName(), executionContext);
        final boolean checkPrimaryKey =
            executionContext.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK);
        final boolean gsiCoverAllSk = checkGsiCoverAllSk(tableMeta, executionContext);

        boolean canUseReturning =
            this.isCanUseReturning() && executionContext.isCheckSupportsReturningAll()
                && !ComplexTaskPlanUtils.canWrite(tableMeta)
                && gsiCoverAllSk
                && gsiCanUseReturning
                && executionContext.isCheckIsAllDnUseXDataSource()
                && !checkForeignKey
                && !checkPrimaryKey && !isColumnMultiWriting && !isBroadcast
                && !ExternalizedDmlRewriter.isReturningForbidden(tableMeta);

        pw.item(RelDrdsWriter.REL_NAME, "LogicalReplace");

        final boolean isSourceSelect = isSourceSelect();
        if (isSourceSelect) {
            pw.item("table", getLogicalTableName());
            pw.item("columns", getInsertRowType());
        } else {
            pw.item("sql", getSqlTemplate().toString().replace("\n", " "));
        }
        if (canUseReturning) {
            pw.item("isReturning", true);
        }
        if (!canUseReturning) {
            pw.item("uniqueKeySelect",
                ukGroupByTable.entrySet().stream().map(e -> "select " + e.getValue() + " on " + e.getKey())
                    .collect(Collectors.toList()));
        }
        if (isSourceSelect) {
            pw.item("mode", insertSelectMode);
        }
        return pw;
    }

    public boolean isHasJsonColumn() {
        return hasJsonColumn;
    }

    public boolean isCanUseReturning() {
        return canUseReturning;
    }
}

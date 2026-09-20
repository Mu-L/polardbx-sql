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
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.RowWriteBinding;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateByReturningWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Modify sharding key
 *
 * @author chenmo.cm
 */
public class LogicalRelocate extends TableModify {

    private final String schemaName;

    // Positions of auto_increment columns update columns
    private final List<Integer> autoIncColumns;

    // Writers group by target table
    private final Map<Integer, List<RelocateWriter>> relocateWriterMap;
    private final Map<Integer, List<DistinctWriter>> modifyWriterMap;

    // Source columns and target columns group by target table
    private final Map<Integer, Mapping> setColumnTargetMappings;
    private final Map<Integer, Mapping> setColumnSourceMappings;
    private final Map<Integer, List<ColumnMeta>> setColumnMetas;
    // If all columns are safe to compare
    private final Map<Integer, Boolean> modifyOnlySafeCompareMap;

    // Primary writer
    private final Map<Integer, DistinctWriter> primaryDistinctWriter;
    private final Map<Integer, RelocateWriter> primaryRelocateWriter;

    // gsi writers for returning
    private final Map<Integer, List<RelocateByReturningWriter>> gsiRelocateByReturningWriterMap;
    private final Map<Integer, List<DistinctWriter>> gsiModifyByReturningWriterMap;

    // primary writer for returning
    private final Map<Integer, RelocateByReturningWriter> primaryRelocateByReturningWriter;

    // System-added ON UPDATE CURRENT_TIMESTAMP columns (not user-specified), grouped by primary table index
    private Map<Integer, Set<String>> addedAutoUpdateColumnMap;

    private SqlNode originalSqlNode;

    // Following variables used by generated columns
    private Map<Integer, List<ColumnMeta>> evalRowColumnMetas;
    private Map<Integer, List<Integer>> inputToEvalFieldMappings;
    private Map<Integer, List<RexNode>> genColRexNodes;

    // Planner-produced physical value actions keyed by the exact leaf writer that consumes them.
    private Map<DistinctWriter, List<RowWriteBinding>> externalizedExactRowTransforms = Collections.emptyMap();

    @Getter
    @Setter
    protected LogicalRelocateInfo relocateInfo = LogicalRelocateInfo.EMPTY;

    protected LogicalRelocate(LogicalModify update, List<Integer> autoIncColumns,
                              Map<Integer, List<RelocateWriter>> relocateWriterMap,
                              Map<Integer, List<DistinctWriter>> modifyWriterMap,
                              Map<Integer, Mapping> setColumnTargetMappings,
                              Map<Integer, Mapping> setColumnSourceMappings,
                              Map<Integer, List<ColumnMeta>> setColumnMetas,
                              Map<Integer, Boolean> modifyOnlySafeCompareMap,
                              Map<Integer, DistinctWriter> primaryDistinctWriter,
                              Map<Integer, RelocateWriter> primaryRelocateWriter,
                              Map<Integer, List<RelocateByReturningWriter>> gsiRelocateByReturningWriterMap,
                              Map<Integer, List<DistinctWriter>> gsiModifyByReturningWriterMap,
                              Map<Integer, RelocateByReturningWriter> primaryRelocateByReturningWriter,
                              Map<Integer, Set<String>> addedAutoUpdateColumnMap,
                              SqlNode originalSqlNode) {
        super(update.getCluster(),
            update.getTraitSet(),
            update.getTable(),
            update.getCatalogReader(),
            update.getInput(),
            update.getOperation(),
            update.getUpdateColumnList(),
            update.getSourceExpressionList(),
            update.isFlattened(),
            update.getKeywords(),
            update.getBatchSize(),
            update.getAppendedColumnIndex(),
            update.getHints(),
            update.getTableInfo());
        this.schemaName = update.getSchemaName();
        this.autoIncColumns = autoIncColumns;
        this.relocateWriterMap = relocateWriterMap;
        this.modifyWriterMap = modifyWriterMap;
        this.setColumnTargetMappings = setColumnTargetMappings;
        this.setColumnSourceMappings = setColumnSourceMappings;
        this.setColumnMetas = setColumnMetas;
        this.modifyOnlySafeCompareMap = modifyOnlySafeCompareMap;
        this.primaryDistinctWriter = primaryDistinctWriter;
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.gsiRelocateByReturningWriterMap = gsiRelocateByReturningWriterMap;
        this.gsiModifyByReturningWriterMap = gsiModifyByReturningWriterMap;
        this.primaryRelocateByReturningWriter = primaryRelocateByReturningWriter;
        this.addedAutoUpdateColumnMap = addedAutoUpdateColumnMap;
        this.originalSqlNode = originalSqlNode;
        this.originalSqlNode = update.getOriginalSqlNode();
    }

    public LogicalRelocate(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader,
                           RelNode input, Operation operation, List<String> updateColumnList,
                           List<RexNode> sourceExpressionList, boolean flattened, List<String> keywords, int batchSize,
                           Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfos, String schemaName,
                           List<Integer> autoIncColumns, Map<Integer, List<RelocateWriter>> relocateWriterMap,
                           Map<Integer, List<DistinctWriter>> modifyWriterMap,
                           Map<Integer, Mapping> setColumnTargetMappings, Map<Integer, Mapping> setColumnSourceMappings,
                           Map<Integer, List<ColumnMeta>> setColumnMetas,
                           Map<Integer, Boolean> modifyOnlySafeCompareMap,
                           Map<Integer, DistinctWriter> primaryDistinctWriter,
                           Map<Integer, RelocateWriter> primaryRelocateWriter,
                           Map<Integer, List<RelocateByReturningWriter>> gsiRelocateByReturningWriterMap,
                           Map<Integer, List<DistinctWriter>> gsiModifyByReturningWriterMap,
                           Map<Integer, RelocateByReturningWriter> primaryRelocateByReturningWriter,
                           Map<Integer, Set<String>> addedAutoUpdateColumnMap,
                           SqlNode originalSqlNode) {
        super(cluster,
            traitSet,
            table,
            catalogReader,
            input,
            operation,
            updateColumnList,
            sourceExpressionList,
            flattened,
            keywords,
            batchSize,
            appendedColumnIndex,
            hints,
            tableInfos);
        this.schemaName = schemaName;
        this.autoIncColumns = autoIncColumns;
        this.relocateWriterMap = relocateWriterMap;
        this.modifyWriterMap = modifyWriterMap;
        this.setColumnTargetMappings = setColumnTargetMappings;
        this.setColumnSourceMappings = setColumnSourceMappings;
        this.setColumnMetas = setColumnMetas;
        this.modifyOnlySafeCompareMap = modifyOnlySafeCompareMap;
        this.primaryDistinctWriter = primaryDistinctWriter;
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.gsiRelocateByReturningWriterMap = gsiRelocateByReturningWriterMap;
        this.gsiModifyByReturningWriterMap = gsiModifyByReturningWriterMap;
        this.primaryRelocateByReturningWriter = primaryRelocateByReturningWriter;
        this.addedAutoUpdateColumnMap = addedAutoUpdateColumnMap;
        this.originalSqlNode = originalSqlNode;
    }

    /**
     * Create LogicalRelocate for modifying sharding column of single primary table only
     *
     * @param update Base LogicalModify
     * @return LogicalRelocate
     */
    public static LogicalRelocate singleTargetWithoutGsi(LogicalModify update,
                                                         List<Integer> autoIncColumns,
                                                         Map<Integer, List<RelocateWriter>> relocateWriterMap,
                                                         Map<Integer, List<DistinctWriter>> modifyWriterMap,
                                                         Map<Integer, Mapping> setColumnTargetMappings,
                                                         Map<Integer, Mapping> setColumnSourceMappings,
                                                         Map<Integer, List<ColumnMeta>> setColumnMetas,
                                                         Map<Integer, Boolean> modifySkOnly,
                                                         Map<Integer, DistinctWriter> primaryDistinctWriter,
                                                         Map<Integer, RelocateWriter> primaryRelocateWriter,
                                                         Map<Integer, List<RelocateByReturningWriter>> gsiRelocateByReturningWriterMap,
                                                         Map<Integer, List<DistinctWriter>> gsiModifyByReturningWriterMap,
                                                         Map<Integer, RelocateByReturningWriter> primaryRelocateByReturningWriter,
                                                         Map<Integer, Set<String>> addedAutoUpdateColumnMap,
                                                         SqlNode originalSqlNode) {
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(update.isUpdate());

        // Single-table update
        Preconditions.checkArgument(update.getTableInfo().isSingleTarget());

        return new LogicalRelocate(update, autoIncColumns, relocateWriterMap,
            modifyWriterMap, setColumnTargetMappings,
            setColumnSourceMappings, setColumnMetas, modifySkOnly, primaryDistinctWriter, primaryRelocateWriter,
            gsiRelocateByReturningWriterMap, gsiModifyByReturningWriterMap, primaryRelocateByReturningWriter,
            addedAutoUpdateColumnMap, originalSqlNode);
    }

    public static LogicalRelocate create(LogicalModify update,
                                         List<Integer> autoIncColumns,
                                         Map<Integer, List<RelocateWriter>> relocateWriterMap,
                                         Map<Integer, List<DistinctWriter>> modifyWriterMap,
                                         Map<Integer, Mapping> setColumnTargetMappings,
                                         Map<Integer, Mapping> setColumnSourceMappings,
                                         Map<Integer, List<ColumnMeta>> setColumnMetas,
                                         Map<Integer, Boolean> modifySkOnly,
                                         Map<Integer, DistinctWriter> primaryDistinctWriter,
                                         Map<Integer, RelocateWriter> primaryRelocateWriter,
                                         Map<Integer, List<RelocateByReturningWriter>> gsiRelocateByReturningWriterMap,
                                         Map<Integer, List<DistinctWriter>> gsiModifyByReturningWriterMap,
                                         Map<Integer, RelocateByReturningWriter> primaryRelocateByReturningWriter,
                                         Map<Integer, Set<String>> addedAutoUpdateColumnMap,
                                         SqlNode originalSqlNode) {
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(update.isUpdate());

        return new LogicalRelocate(update, autoIncColumns, relocateWriterMap,
            modifyWriterMap, setColumnTargetMappings,
            setColumnSourceMappings, setColumnMetas, modifySkOnly, primaryDistinctWriter, primaryRelocateWriter,
            gsiRelocateByReturningWriterMap, gsiModifyByReturningWriterMap, primaryRelocateByReturningWriter,
            addedAutoUpdateColumnMap, originalSqlNode);
    }

    protected String explainNodeName() {
        return "LogicalRelocate";
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
        pw.item("TYPE", getOperation());

        ExecutionContext executionContext = null;
        if (pw instanceof RelDrdsWriter) {
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        }

        boolean canUseReturning = false;
        final ExecutionContext ec = executionContext;
        if (ec != null && isRelocateCanBeOptimizedByReturning()) {
            TableMeta tableMeta =
                ec.getSchemaManager(schemaName).getTable(getLogicalTableName());
            final boolean checkForeignKey =
                ec.foreignKeyChecks() && (tableMeta.hasForeignKey() || tableMeta.hasReferencedForeignKey());
            final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
            List<String> tables = getTargetTableNames();
            boolean haveBroadcast = tables.stream().anyMatch(or::isBroadCastOrReplicas);
            boolean haveSingle = tables.stream().anyMatch(or::isTableInSingleDb);
            final boolean gsiCanUseReturning = GlobalIndexMeta
                .isAllGsi(getTargetTables().get(0), executionContext, GlobalIndexMeta::isPublished)
                && !GlobalIndexMeta.isAnyGsi(getTargetTables().get(0), executionContext,
                (context, gsiMeta) -> ComplexTaskPlanUtils.canWrite(gsiMeta));
            boolean haveGeneratedColumn = tables.stream().anyMatch(
                tableName -> ec.getSchemaManager(schemaName).getTable(tableName)
                    .hasLogicalGeneratedColumn());
            boolean haveMceReturningForbidden = tables.stream().anyMatch(
                tableName -> ExternalizedDmlRewriter.isReturningForbidden(
                    ec.getSchemaManager(schemaName).getTable(tableName)));

            canUseReturning = executionContext.isCheckSupportsReturningAll()
                && executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_RETURNING)
                && executionContext.getParamManager().getBoolean(ConnectionParams.OPTIMIZE_RELOCATE_BY_RETURNING)
                && executionContext.isCheckIsAllDnUseXDataSource()
                && !ComplexTaskPlanUtils.canWrite(tableMeta)
                && gsiCanUseReturning
                && !haveBroadcast
                && !haveSingle
                && !checkForeignKey
                && autoIncColumns.isEmpty()
                && !haveGeneratedColumn
                && !haveMceReturningForbidden;
        }

        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < getUpdateColumnList().size(); i++) {
            stringBuilder.append(getTargetTableNames().get(i));
            stringBuilder.append(".");
            stringBuilder.append(getUpdateColumnList().get(i));
            stringBuilder.append("=");
            stringBuilder.append(getSourceExpressionList().get(i));
            if (i < getUpdateColumnList().size() - 1) {
                stringBuilder.append(", ");
            }
        }
        pw.item("SET", stringBuilder.toString());

        final List<String> relocateSet = relocateWriterMap.values().stream().flatMap(Collection::stream)
            .map(w -> Util.last(w.getTargetTable().getQualifiedName()))
            .collect(Collectors.toList());

        if (!relocateSet.isEmpty()) {
            pw.item("RELOCATE", String.join(", ", relocateSet));
        }

        final List<String> updateSet = modifyWriterMap.values().stream().flatMap(Collection::stream)
            .map(w -> Util.last(w.getTargetTable().getQualifiedName()))
            .collect(Collectors.toList());

        if (!updateSet.isEmpty()) {
            pw.item("UPDATE", String.join(", ", updateSet));
        }

        if (canUseReturning) {
            pw.item("isReturning", true);
        }
        return pw;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        LogicalRelocate newLogicalRelocate = new LogicalRelocate(getCluster(),
            traitSet,
            getTable(),
            getCatalogReader(),
            sole(inputs),
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList(),
            isFlattened(),
            getKeywords(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getSchemaName(),
            getAutoIncColumns(),
            getRelocateWriterMap(),
            getModifyWriterMap(),
            getSetColumnTargetMappings(),
            getSetColumnSourceMappings(),
            getSetColumnMetas(),
            getModifyOnlySafeCompareMap(),
            getPrimaryDistinctWriter(),
            getPrimaryRelocateWriter(),
            getGsiRelocateByReturningWriterMap(),
            getGsiModifyByReturningWriterMap(),
            getPrimaryRelocateByReturningWriter(),
            getAddedAutoUpdateColumnMap(),
            getOriginalSqlNode());
        newLogicalRelocate.evalRowColumnMetas = evalRowColumnMetas;
        newLogicalRelocate.inputToEvalFieldMappings = inputToEvalFieldMappings;
        newLogicalRelocate.genColRexNodes = genColRexNodes;
        newLogicalRelocate.externalizedExactRowTransforms = externalizedExactRowTransforms;
        newLogicalRelocate.relocateInfo = relocateInfo;
        newLogicalRelocate.addedAutoUpdateColumnMap = addedAutoUpdateColumnMap;
        return newLogicalRelocate;
    }

    public List<Integer> getAutoIncColumns() {
        return autoIncColumns;
    }

    public Map<Integer, List<RelocateWriter>> getRelocateWriterMap() {
        return relocateWriterMap;
    }

    public Map<Integer, List<DistinctWriter>> getModifyWriterMap() {
        return modifyWriterMap;
    }

    public Map<Integer, Mapping> getSetColumnTargetMappings() {
        return setColumnTargetMappings;
    }

    public Map<Integer, Mapping> getSetColumnSourceMappings() {
        return setColumnSourceMappings;
    }

    public Map<Integer, List<ColumnMeta>> getSetColumnMetas() {
        return setColumnMetas;
    }

    public Map<Integer, DistinctWriter> getPrimaryDistinctWriter() {
        return primaryDistinctWriter;
    }

    public Map<Integer, RelocateWriter> getPrimaryRelocateWriter() {
        return primaryRelocateWriter;
    }

    public Map<Integer, List<RelocateByReturningWriter>> getGsiRelocateByReturningWriterMap() {
        return gsiRelocateByReturningWriterMap;
    }

    public Map<Integer, List<DistinctWriter>> getGsiModifyByReturningWriterMap() {
        return gsiModifyByReturningWriterMap;
    }

    public Map<Integer, RelocateByReturningWriter> getPrimaryRelocateByReturningWriter() {
        return primaryRelocateByReturningWriter;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public SqlNode getOriginalSqlNode() {
        return originalSqlNode;
    }

    public Map<Integer, Boolean> getModifyOnlySafeCompareMap() {
        return modifyOnlySafeCompareMap;
    }

    public Map<Integer, Set<String>> getAddedAutoUpdateColumnMap() {
        return addedAutoUpdateColumnMap;
    }

    public Map<Integer, List<ColumnMeta>> getEvalRowColumnMetas() {
        return evalRowColumnMetas;
    }

    public Map<DistinctWriter, List<RowWriteBinding>> getExternalizedExactRowTransforms() {
        return externalizedExactRowTransforms;
    }

    public void setExternalizedExactRowTransforms(
        Map<DistinctWriter, List<RowWriteBinding>> externalizedExactRowTransforms) {
        this.externalizedExactRowTransforms = externalizedExactRowTransforms == null ? Collections.emptyMap()
            : externalizedExactRowTransforms;
    }

    public void setEvalRowColumnMetas(
        Map<Integer, List<ColumnMeta>> evalRowColumnMetas) {
        this.evalRowColumnMetas = evalRowColumnMetas;
    }

    public Map<Integer, List<Integer>> getInputToEvalFieldMappings() {
        return inputToEvalFieldMappings;
    }

    public void setInputToEvalFieldMappings(
        Map<Integer, List<Integer>> inputToEvalFieldMappings) {
        this.inputToEvalFieldMappings = inputToEvalFieldMappings;
    }

    public Map<Integer, List<RexNode>> getGenColRexNodes() {
        return genColRexNodes;
    }

    public void setGenColRexNodes(
        Map<Integer, List<RexNode>> genColRexNodes) {
        this.genColRexNodes = genColRexNodes;
    }

    public String getLogicalTableName() {
        return getTargetTableNames().get(0);
    }

    public boolean isRelocateCanBeOptimizedByReturning() {
        return null != this.relocateInfo
            && !LogicalRelocateInfo.EMPTY.equals(this.relocateInfo);
    }

    @Data
    @RequiredArgsConstructor
    public static class LogicalRelocateInfo {
        public static LogicalRelocateInfo EMPTY = new LogicalRelocateInfo();

        private final boolean withOffset;
        private final boolean withFetch;

        @Accessors(chain = true)
        private RelUtils.LogicalModifyViewBuilderFromRelocate lmvBuilder = null;

        @Accessors(chain = true)
        private boolean optimizeByReturning = false;

        private LogicalRelocateInfo() {
            this.withOffset = false;
            this.withFetch = false;
        }

        public static LogicalRelocateInfo create(boolean withOffset, boolean withFetch) {
            return new LogicalRelocateInfo(withOffset, withFetch);
        }
    }
}

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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.job.builder.CreatePartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.CreatePartitionGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar.CreateColumnarIndexJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CciSchemaEvolutionTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.WaitColumnarTableCreationTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiStatisticsInfoSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateColumnarIndex;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTableNoCdcMark;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateSelect;
import com.alibaba.polardbx.executor.sync.GsiStatisticsSyncAction;
import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTable;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.LikeTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author guxu
 */
public class CreatePartitionTableWithGsiJobFactory extends DdlJobFactory {
    private static final Logger logger = LoggerFactory.getLogger(CreatePartitionTableWithGsiJobFactory.class);

    @Deprecated
    private final DDL ddl;
    private final CreateTableWithGsiPreparedData preparedData;

    TreeMap<String, List<List<String>>> primaryTableTopology;
    List<PhyDdlTableOperation> primaryTablePhysicalPlans;
    Map<String, List<PhyDdlTableOperation>> indexTablePhysicalPlansMap;
    private CreatePartitionTableWithGsiBuilder createPartitionTableWithGsiBuilder;

    private final String schemaName;
    private final String primaryTableName;

    private final ExecutionContext executionContext;

    private String selectSql;

    private final SqlCreateTable normalizedOriginalDdl;

    public CreatePartitionTableWithGsiJobFactory(@Deprecated DDL ddl,
                                                 CreateTableWithGsiPreparedData preparedData,
                                                 ExecutionContext executionContext) {
        // ddl.sqlNode will be modified in ReplaceTableNameWithQuestionMarkVisitor
        // after that the table name of CREATE TABLE statement will be replaced with a question mark
        // which will cause an error in CHECK COLUMNAR META and CDC.
        // The right way might be copy a new SqlNode in ReplaceTableNameWithQuestionMarkVisitor
        // every time when table name is replaced (as a SqlShuttle should do).
        // But change ReplaceTableNameWithQuestionMarkVisitor will affect all kinds of ddl statement,
        // should be done sometime later and tested carefully
        this.normalizedOriginalDdl = (SqlCreateTable) SqlNode.clone(ddl.sqlNode);

        CreatePartitionTableWithGsiBuilder createTableWithGsiBuilder =
            new CreatePartitionTableWithGsiBuilder(ddl, preparedData, executionContext);
        createTableWithGsiBuilder.build();
        this.createPartitionTableWithGsiBuilder = createTableWithGsiBuilder;

        TreeMap<String, List<List<String>>> primaryTableTopology = createTableWithGsiBuilder.getPrimaryTableTopology();
        List<PhyDdlTableOperation> primaryTablePhysicalPlans = createTableWithGsiBuilder.getPrimaryTablePhysicalPlans();

        this.ddl = ddl;
        this.preparedData = preparedData;
        this.primaryTableTopology = primaryTableTopology;
        this.primaryTablePhysicalPlans = primaryTablePhysicalPlans;
        this.indexTablePhysicalPlansMap = createTableWithGsiBuilder.getIndexTablePhysicalPlansMap();
        this.executionContext = executionContext;

        this.schemaName = preparedData.getPrimaryTablePreparedData().getSchemaName();
        this.primaryTableName = preparedData.getPrimaryTablePreparedData().getTableName();
    }

    @Override
    protected void validate() {

        /**
         * Check if the archive table name already exists if need creating  archive table and its cci
         */

        if (preparedData.getPrimaryTablePreparedData().getTtlDefinitionInfo() != null) {
            TtlDefinitionInfo ttlDefinitionInfo = preparedData.getPrimaryTablePreparedData().getTtlDefinitionInfo();
            String ttlTableSchema = ttlDefinitionInfo.getTtlInfoRecord().getTableSchema();
            String ttlTableName = ttlDefinitionInfo.getTtlInfoRecord().getTableName();
            String archiveTableName = ttlDefinitionInfo.getArchiveTableName();
            String archiveTableSchema = ttlDefinitionInfo.getArchiveTableSchema();

            if (ttlDefinitionInfo.needPerformExpiredDataArchiving()) {
                boolean foundTargetCciName = false;
                String tarArcCciIdxName = null;
                String arcTmpName = ttlDefinitionInfo.getTmpTableName().toLowerCase();
                Set<String> allIndexTableNameSet = preparedData.getIndexTablePreparedDataMap().keySet();
                for (String idxTblName : allIndexTableNameSet) {
                    if (idxTblName.toLowerCase().startsWith(arcTmpName)) {
                        foundTargetCciName = true;
                        tarArcCciIdxName = idxTblName;
                        break;
                    }
                }
                if (foundTargetCciName) {
                    CreateGlobalIndexPreparedData gsiPrepData =
                        preparedData.getIndexTablePreparedDataMap().get(tarArcCciIdxName);
                    if (gsiPrepData != null && gsiPrepData.isColumnarIndex()) {
                        ViewManager viewManager = OptimizerContext.getContext(archiveTableSchema).getViewManager();
                        if (viewManager != null) {
                            SystemTableView.Row viewInfo = viewManager.select(archiveTableName);
                            if (viewInfo != null) {
//                                throw new TddlRuntimeException(ErrorCode.ERR_TTL, String.format(
//                                    "Failed to create table `%s`.`%s` because the specifying archive table '%s'.'%s' of ttl definition already exists",
//                                    ttlTableSchema, ttlTableName,
//                                    archiveTableSchema, archiveTableName));
                            }
                        }
                    }
                }

            }

        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob result = new ExecutableDdlJob();
        boolean isAutoPartition = this.preparedData.getPrimaryTablePreparedData().isAutoPartition();

        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(
                primaryTableTopology,
                primaryTablePhysicalPlans,
                false,
                isAutoPartition,
                executionContext);
        CreatePartitionTableJobFactory ret =
            new CreatePartitionTableJobFactory(preparedData.getPrimaryTablePreparedData().isAutoPartition(),
                preparedData.getPrimaryTablePreparedData().isTimestampColumnDefault(),
                preparedData.getPrimaryTablePreparedData().getSpecialDefaultValues(),
                preparedData.getPrimaryTablePreparedData().getSpecialDefaultValueFlags(),
                preparedData.getPrimaryTablePreparedData().getAddedForeignKeys(),
                physicalPlanData, executionContext, preparedData.getPrimaryTablePreparedData(), null,
                preparedData.getPrimaryTablePreparedData().getLikeTableInfo());
        boolean hasCci = preparedData.getIndexTablePreparedDataMap().values().stream()
            .anyMatch(CreateGlobalIndexPreparedData::isColumnarIndex);
        ret.setWithCci(hasCci);
        ExecutableDdlJob thisParentJob = ret.create();
        if (preparedData.getPrimaryTablePreparedData().isNeedToGetTableGroupLock()) {
            return thisParentJob;
        }

        ExecutableDdlJob4CreatePartitionTable createTableJob = (ExecutableDdlJob4CreatePartitionTable) thisParentJob;

        createTableJob.removeTaskRelationship(
            createTableJob.getCreateTableAddTablesMetaTask(),
            createTableJob.getCdcDdlMarkTask()
        );
        result.combineTasks(createTableJob);
        result.addExcludeResources(createTableJob.getExcludeResources());
        result.addSharedResources(createTableJob.getSharedResources());

        final List<WaitColumnarTableCreationTask> cciWaitList = new ArrayList<>();
        final List<DdlTask> cciLastTasks = new ArrayList<>();
        final List<CciSchemaEvolutionTask> schemaEvolutionInitializer = new ArrayList<>();
        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        Map<String, DdlTask> indexAddPartitionMetaTasks = new TreeMap<>(String::compareToIgnoreCase);
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData indexPreparedData = entry.getValue();
            if (indexPreparedData.isColumnarIndex()) {
                // Clustered columnar index
                final ExecutableDdlJob4CreateColumnarIndex cciJob = (ExecutableDdlJob4CreateColumnarIndex)
                    CreateColumnarIndexJobFactory.create4CreateCci(ddl, indexPreparedData, executionContext);
                if (indexPreparedData.isNeedToGetTableGroupLock()) {
                    return cciJob;
                }
                cciJob.removeTaskRelationship(
                    cciJob.getInsertColumnarIndexMetaTask(),
                    cciJob.getWaitColumnarTableCreationTask()
                );

                cciWaitList.add(cciJob.getWaitColumnarTableCreationTask());
                cciLastTasks.add(cciJob.getLastTask());

                // Add cci tasks
                result.combineTasks(cciJob);

                // Add relationship with before tasks
                result.addTaskRelationship(
                    createTableJob.getCreateTableAddTablesMetaTask(),
                    cciJob.getCreateColumnarIndexValidateTask()
                );

                // Add Relationship with after tasks
                result.addTaskRelationship(cciJob.getInsertColumnarIndexMetaTask(), createTableJob.getCdcDdlMarkTask());

                // Add exclusive resources
                result.addExcludeResources(cciJob.getExcludeResources());
                result.addSharedResources(cciJob.getSharedResources());

                // Replace index definition in normalizedOriginalDdl
                normalizedOriginalDdl.replaceCciDef(
                    entry.getKey(),
                    indexPreparedData.getIndexDefinition());

                schemaEvolutionInitializer.add(CciSchemaEvolutionTask.createCci(schemaName,
                    indexPreparedData.getPrimaryTableName(),
                    indexPreparedData.getIndexTableName(),
                    buildColumnarOptions(indexPreparedData.getIndexDefinition()),
                    indexPreparedData.getDdlVersionId()));

                // TODO is CreateGsiPreCheckTask necessary ?

            } else {
                // Global secondary index
                ExecutableDdlJob thisJob =
                    CreatePartitionGsiJobFactory.create4CreateTableWithGsi(ddl, indexPreparedData, executionContext);
                DdlTask gsiStatisticsInfoTask = new GsiStatisticsInfoSyncTask(
                    indexPreparedData.getSchemaName(),
                    indexPreparedData.getPrimaryTableName(),
                    indexPreparedData.getIndexTableName(),
                    GsiStatisticsSyncAction.INSERT_RECORD,
                    null);
                thisJob.appendTask(gsiStatisticsInfoTask);
                if (indexPreparedData.isNeedToGetTableGroupLock()) {
                    return thisJob;
                }
                ExecutableDdlJob4CreatePartitionGsi gsiJob = (ExecutableDdlJob4CreatePartitionGsi) thisJob;
                result.combineTasks(gsiJob);
                result.addTaskRelationship(
                    createTableJob.getCreateTableAddTablesMetaTask(),
                    gsiJob.getCreateGsiValidateTask()
                );
                result.addTaskRelationship(gsiJob.getLastTask(), createTableJob.getCdcDdlMarkTask());
                result.addExcludeResources(gsiJob.getExcludeResources());
                result.addSharedResources(gsiJob.getSharedResources());
                result.addTask(gsiJob.getCreateGsiPreCheckTask());
                result.addTaskRelationship(createTableJob.getCreatePartitionTableValidateTask(),
                    gsiJob.getCreateGsiPreCheckTask());
                result.addTaskRelationship(gsiJob.getCreateGsiPreCheckTask(),
                    createTableJob.getCreateTableAddTablesPartitionInfoMetaTask());
                indexAddPartitionMetaTasks.put(indexPreparedData.getIndexTableName(),
                    gsiJob.getCreateTableAddTablesPartitionInfoMetaTask());
            }
        }
        addDependence(indexAddPartitionMetaTasks, result);

        if (!cciWaitList.isEmpty()) {
            final CdcDdlMarkTask cdcDdlMarkTask = createTableJob.getCdcDdlMarkTask();

            // Try to set CDC_ORIGINAL_DDL with this.normalizedOriginalDdl
            updateOriginalDdlSql(cdcDdlMarkTask);
            cdcDdlMarkTask.addSchemaEvolutionInitializers(schemaEvolutionInitializer);

            result.removeTaskRelationship(
                cdcDdlMarkTask,
                createTableJob.getCreateTableShowTableMetaTask());
            DdlTask last = cdcDdlMarkTask;
            for (int i = 0; i < cciWaitList.size(); ++i) {
                WaitColumnarTableCreationTask cciWait = cciWaitList.get(i);
                result.addTaskRelationship(last, cciWait);
                last = cciLastTasks.get(i);
            }
            result.addTaskRelationship(last, createTableJob.getCreateTableShowTableMetaTask());
        }

        if (preparedData.getPrimaryTablePreparedData().getTtlDefinitionInfo() != null) {
            TtlDefinitionInfo ttlDefinitionInfo = preparedData.getPrimaryTablePreparedData().getTtlDefinitionInfo();

            if (ttlDefinitionInfo.needPerformExpiredDataArchiving()) {
                boolean foundTargetIdxName = false;
                String arcTmpName = ttlDefinitionInfo.getTmpTableName().toLowerCase();
                Set<String> allIndexTableNameSet = preparedData.getIndexTablePreparedDataMap().keySet();
                for (String idxTblName : allIndexTableNameSet) {
                    if (idxTblName.toLowerCase().startsWith(arcTmpName)) {
                        foundTargetIdxName = true;
                        break;
                    }
                }
                if (foundTargetIdxName) {
                    TableMeta primTblMeta = preparedData.getPrimaryTablePreparedData().getTableMeta();
                    String ttlTblSchema = ttlDefinitionInfo.getTtlInfoRecord().getTableSchema();
                    String ttlTblName = ttlDefinitionInfo.getTtlInfoRecord().getTableName();
                    String cciName = ttlDefinitionInfo.getTtlInfoRecord().getArcTmpTblName();
                    String arcTblName = ttlDefinitionInfo.getArchiveTableName();
                    String viewName = arcTblName;
                    List<ColumnMeta> colMetaList = new ArrayList<>();
                    colMetaList.addAll(primTblMeta.getAllColumns());
                    String viewDefinition =
                        TtlTaskSqlBuilder.buildSqlForFullScanArcTbl(ttlTblSchema, ttlTblName, cciName, colMetaList);
                    CreateViewAddMetaTask createViewAddMetaTask =
                        new CreateViewAddMetaTask(ttlTblSchema, viewName, false, null, viewDefinition, null, null);
                    CreateViewSyncTask createViewSyncTask = new CreateViewSyncTask(ttlTblSchema, viewName);

                    List<DdlTask> tailNodes =
                        result.getAllZeroOutDegreeVertexes().stream().map(o -> o.getObject()).collect(
                            Collectors.toList());
                    result.addTask(createViewAddMetaTask);
                    result.addTask(createViewSyncTask);
                    result.addTaskRelationship(createViewAddMetaTask, createViewSyncTask);
                    for (int i = 0; i < tailNodes.size(); i++) {
                        DdlTask tailTask = tailNodes.get(i);
                        result.addTaskRelationship(tailTask, createViewAddMetaTask);
                    }
                }
            }
        }

        if (selectSql != null) {
            InsertIntoTask
                insertIntoTask = new InsertIntoTask(schemaName, primaryTableName, selectSql, null, 0);
            ExecutableDdlJob insertJob = new ExecutableDdlJob();
            insertJob.addTask(insertIntoTask);
            ExecutableDdlJob4CreateSelect ans = new ExecutableDdlJob4CreateSelect();
            ans.appendJob2(result);
            ans.appendJob2(insertJob);
            ans.setInsertTask(insertIntoTask);
            //insert 只能rollback，无法重试
            insertIntoTask.setExceptionAction(DdlExceptionAction.ROLLBACK);
            return ans;
        }

        if (preparedData.getPrimaryTablePreparedData().getTableMeta().isPolardbxBlockChain()) {
            //如果是区块链表，需要创建该表的历史记录表
            ExecutableDdlJob4CreatePartitionTableNoCdcMark blockChainJob =
                buildBlockChainHistoryTableJob(schemaName, primaryTableName, executionContext);
            result.combineTasks(blockChainJob);

            result.addTaskRelationship(createTableJob.getCreatePartitionTableValidateTask(),
                blockChainJob.getCreatePartitionTableValidateTask());
            result.addTaskRelationship(createTableJob.getCreateTableShowTableMetaTask(),
                blockChainJob.getCreateTableShowTableMetaTask());
            result.addExcludeResources(blockChainJob.getExcludeResources());
            result.addSharedResources(blockChainJob.getSharedResources());
        }

        return result;
    }

    private Map<String, String> buildColumnarOptions(SqlIndexDefinition indexDefinition) {
        if (indexDefinition == null) {
            return new HashMap<>();
        }
        Map<String, String> options = indexDefinition.getColumnarOptions();
        // Normalize dict columns.
        String dictColumnStr = options.get(ColumnarOptions.DICTIONARY_COLUMNS);
        if (null != dictColumnStr) {
            dictColumnStr = SQLUtils.splitNamesByComma(dictColumnStr.toLowerCase()).stream()
                .map(SqlIdentifier::surroundWithBacktick)
                .collect(Collectors.joining(","));
            options.put(ColumnarOptions.DICTIONARY_COLUMNS, dictColumnStr);
        }
        return options;
    }

    @SuppressWarnings("CatchMayIgnoreException")
    private void updateOriginalDdlSql(CdcDdlMarkTask cdcDdlMarkTask) {
        try {
            // Call SqlCreateTable.toSqlString(SqlDialect dialect) will get user-input create table statement;
            // Call SqlCreateTable.unparse(SqlWriter writer, int leftPrec, int rightPrec) will get create table
            // statement with cci name replaced (suffix added).
            // For GDN use, we need cci name without suffix
            SqlCreateTable.PrepareSqlStringOptions options = new SqlCreateTable.PrepareSqlStringOptions();
            options.setTtlDefinitionAllowed(true);
            String normalizedOriginalDdl = this.normalizedOriginalDdl
//                .toSqlString(MysqlSqlDialect.DEFAULT, false)
                .toSqlStringForCdc(options);

            // For tables with EXTERNALIZE columns, the SQL produced above is the physical form
            // (`col_addr_` VARCHAR(128) COMMENT 'ext_type:LONGTEXT'), which is what gets pushed to DN.
            // For binlog replay by a downstream PolarDB-X, we want the logical form
            // (`col` LONGTEXT EXTERNALIZE) so the downstream re-applies the externalized-column
            // path on its side. Reverse only those addr columns; keep everything else identical.
            Set<String> externalizedPhysicalColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, Long> specialFlags =
                preparedData.getPrimaryTablePreparedData().getSpecialDefaultValueFlags();
            if (specialFlags != null) {
                for (Map.Entry<String, Long> entry : specialFlags.entrySet()) {
                    if (entry.getValue() != null
                        && (entry.getValue() & ColumnsRecord.FLAG_EXTERNALIZED_COLUMN) != 0L) {
                        externalizedPhysicalColumns.add(entry.getKey());
                    }
                }
            }
            normalizedOriginalDdl =
                revertExternalizedColumnsForCdc(normalizedOriginalDdl, externalizedPhysicalColumns);

            cdcDdlMarkTask.setNormalizedOriginalDdl(normalizedOriginalDdl);
        } catch (Exception ignored) {
            logger.error(
                "Failed to get normalized original ddl statement, might cause CHECK COLUMNAR META report an error",
                ignored);
        }
    }

    /**
     * Reverse the per-column physicalization done by
     * {@code LogicalCreateTable.rewriteExternalizedColumns()} so that the binlog SQL carries the
     * logical EXTERNALIZE form. Only columns whose comment matches {@code ext_type:<TYPE>} are
     * touched; all other table elements (columns, indexes, partitioning, options) pass through
     * untouched.
     */
    private static String revertExternalizedColumnsForCdc(String physicalSql,
                                                          Set<String> externalizedPhysicalColumns) {
        if (physicalSql == null || externalizedPhysicalColumns == null || externalizedPhysicalColumns.isEmpty()
            || !physicalSql.contains(ExternalizedColumnInfo.COMMENT_PREFIX)) {
            return physicalSql;
        }
        List<SQLStatement> stmts = SQLUtils.parseStatementsWithDefaultFeatures(physicalSql, JdbcConstants.MYSQL);
        if (stmts.isEmpty() || !(stmts.get(0) instanceof MySqlCreateTableStatement)) {
            return physicalSql;
        }
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) stmts.get(0);
        boolean changed = false;
        for (SQLTableElement el : stmt.getTableElementList()) {
            if (!(el instanceof SQLColumnDefinition)) {
                continue;
            }
            SQLColumnDefinition col = (SQLColumnDefinition) el;
            SQLExpr commentExpr = col.getComment();
            if (!(commentExpr instanceof SQLCharExpr)) {
                continue;
            }
            String fullComment = ((SQLCharExpr) commentExpr).getText();
            String originalType = ExternalizedColumnInfo.extractOriginalType(fullComment);
            if (originalType == null) {
                continue;
            }
            String addrName = SQLUtils.normalizeNoTrim(col.getName().getSimpleName());
            if (!externalizedPhysicalColumns.contains(addrName)
                || !ExternalizedColumnInfo.isAddrColumn(addrName)) {
                continue;
            }
            // Restore the user's original COMMENT stashed at rewrite time so the
            // downstream PolarDB-X reproduces the same column metadata. See GAP-DDL-A.
            String userComment = ExternalizedColumnInfo.extractUserComment(fullComment);

            String logicalName = ExternalizedColumnInfo.toLogicalColumnName(addrName);
            col.setName(SqlIdentifier.surroundWithBacktick(logicalName));
            col.setDataType(new SQLDataTypeImpl(originalType));
            col.setExternalize(true);
            col.setDefaultExpr(null);
            col.setComment((SQLExpr) null);
            col.setCharsetExpr(null);
            col.setCollateExpr(null);
            // Drop NOT NULL / DEFAULT 0 etc that were added during physicalization.
            col.getConstraints().clear();

            if (userComment != null) {
                col.setComment(new SQLCharExpr(userComment));
            }
            changed = true;
        }
        if (!changed) {
            return physicalSql;
        }
        return stmt.toString();
    }

    private void addDependence(Map<String, DdlTask> indexAddPartitionMetaTasks, ExecutableDdlJob result) {
        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap = preparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            if (primaryTableName.equalsIgnoreCase(gsiPreparedData.getTableGroupAlignWithTargetTable())) {
                //do nothing;
            } else if (StringUtils.isNotEmpty(gsiPreparedData.getTableGroupAlignWithTargetTable())
                && !gsiPreparedData.isColumnarIndex()) {
                DdlTask curIndexAddMeta = indexAddPartitionMetaTasks.get(gsiPreparedData.getIndexTableName());
                DdlTask depandIndexAddMeta =
                    indexAddPartitionMetaTasks.get(gsiPreparedData.getTableGroupAlignWithTargetTable());
                result.addTaskRelationship(depandIndexAddMeta, curIndexAddMeta);
            }

        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        if (indexTablePhysicalPlansMap != null) {
            indexTablePhysicalPlansMap.keySet().forEach(indexTableName -> {
                resources.add(concatWithDot(schemaName, indexTableName));
            });
        }
        if (preparedData.getPrimaryTablePreparedData() != null
            && preparedData.getPrimaryTablePreparedData().getLikeTableInfo() != null) {
            LikeTableInfo likeTableInfo = preparedData.getPrimaryTablePreparedData().getLikeTableInfo();
            resources.add(concatWithDot(likeTableInfo.getSchemaName(), likeTableInfo.getTableName()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public CreatePartitionTableWithGsiBuilder getCreatePartitionTableWithGsiBuilder() {
        return createPartitionTableWithGsiBuilder;
    }

    /**
     * 构建区块链表历史记录表的job
     *
     * @param schemaName 原表的库名
     * @param tableName 原表的表名
     * @return 区块链表历史记录表的job
     */
    public static ExecutableDdlJob4CreatePartitionTableNoCdcMark buildBlockChainHistoryTableJob(String schemaName,
                                                                                                String tableName,
                                                                                                ExecutionContext ec) {
        String blockChainHistoryTableName = String.format(BlockChainHistoryTable.tableNameFormat, tableName);
        String createTableSql =
            String.format(BlockChainHistoryTable.createTableNameFormat, schemaName, blockChainHistoryTableName);
        ExecutionContext ecCopy = ec.copy();

        SqlCreateTable createTableAst = (SqlCreateTable) new FastsqlParser().parse(createTableSql).get(0);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ecCopy);
        ExecutionPlan createTablePlan = Planner.getInstance().getPlan(createTableAst, plannerContext);
        LogicalCreateTable logicalCreateTable = (LogicalCreateTable) createTablePlan.getPlan();
        logicalCreateTable.prepareData(ecCopy);

        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();
        // 构建物理表拓扑
        DdlPhyPlanBuilder createTableBuilder =
            new CreatePartitionTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, ecCopy,
                PartitionTableType.PARTITION_TABLE).build();
        PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();

        ExecutableDdlJob4CreatePartitionTable ret = (ExecutableDdlJob4CreatePartitionTable)
            new CreatePartitionTableJobFactory(
                createTablePreparedData.isAutoPartition(),
                createTablePreparedData.isTimestampColumnDefault(),
                createTablePreparedData.getSpecialDefaultValues(),
                createTablePreparedData.getSpecialDefaultValueFlags(),
                createTablePreparedData.getAddedForeignKeys(),
                physicalPlanData, ecCopy,
                createTablePreparedData,
                createTablePreparedData.getPartitionInfo(),
                createTablePreparedData.getLikeTableInfo()).create();

        //去除cdc打标，cdcMark用的是同一个ExecutionContext，两个cdcMark任务无法区分
        return ExecutableDdlJob4CreatePartitionTableNoCdcMark.buildFrom(ret);
    }

}

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.ddl.job.validator.SequenceValidator;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.sync.DropTableSyncAction;
import com.alibaba.polardbx.executor.sync.GsiStatusChangeSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.entity.LogicMeta;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DataDefLanguageLogicView;
import com.alibaba.polardbx.optimizer.core.rel.GsiBackfill;
import com.alibaba.polardbx.optimizer.core.rel.PartitionTableDdlView;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddPrimaryKey;
import org.apache.calcite.sql.SqlAlterColumnDefaultVal;
import org.apache.calcite.sql.SqlAlterRule;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTable.ColumnOpt;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableOptimizePartition;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropForeignKey;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropPrimaryKey;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.util.Util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

public class DdlGmsUtils extends DdlUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlGmsUtils.class);

    private static final ConfigManager CONFIG_MANAGER = MetaDbConfigManager.getInstance();

    public static void beforeDDLStart(PhyDdlTableOperation ddl, ExecutionContext executionContext, String tableSchema) {
        switch (ddl.getKind()) {
        case CREATE_TABLE:
            if (isCreateTableSupported(ddl, executionContext)) {
                beforeCreateTableStart(ddl, tableSchema, executionContext);
            }
            break;
        case DROP_TABLE:
            DDL parent = ddl.getParent();
            if (parent != null &&
                (parent.getSqlNode() instanceof SqlAlterTableGroupMovePartition
                    || parent.getSqlNode() instanceof SqlAlterTableGroupMergePartition
                    || parent.getSqlNode() instanceof SqlAlterTableGroupSplitPartition
                    || parent.getSqlNode() instanceof SqlAlterTableGroupExtractPartition
                    || parent.getSqlNode() instanceof SqlAlterTableSetTableGroup
                    || parent.getSqlNode() instanceof SqlAlterTableDropPartition
                    || parent.getSqlNode() instanceof SqlAlterTableOptimizePartition
                    || parent.getSqlNode() instanceof SqlAlterTableAddPartition
                    || parent.getSqlNode() instanceof SqlAlterTableModifyPartitionValues)) {
                // do nothing

            } else {
                beforeDropTableStart(ddl, tableSchema, executionContext);
            }
            break;
        case RENAME_TABLE:
            if (executionContext.needToRenamePhyTables()) {
                beforeRenameTableStart(ddl, tableSchema, executionContext);
            }
            break;
        case ALTER_TABLE:
            beforeAlterTableStart(ddl, tableSchema, executionContext);
            break;
        case CREATE_INDEX:
            beforeCreateIndexStart(ddl, tableSchema, executionContext);
            break;
        case DROP_INDEX:
            beforeDropIndexStart(ddl, tableSchema, executionContext);
            break;
        default:
            // Nothing to do with other DDLs.
            break;
        }
    }

    public static void onDDLSuccess(PhyDdlTableOperation ddl, ExecutionContext executionContext, String tableSchema) {

        switch (ddl.getKind()) {
        case CREATE_TABLE:
            if (isCreateTableSupported(ddl, executionContext)) {
                onCreateTableSuccess(ddl, tableSchema, executionContext);
            }
            break;
        case DROP_TABLE:
            DDL parent = ddl.getParent();
            if (parent != null &&
                (parent.getSqlNode() instanceof SqlAlterTableGroupMovePartition
                    || parent.getSqlNode() instanceof SqlAlterTableGroupMergePartition
                    || parent.getSqlNode() instanceof SqlAlterTableGroupSplitPartition
                    || parent.getSqlNode() instanceof SqlAlterTableGroupExtractPartition
                    || parent.getSqlNode() instanceof SqlAlterTableSetTableGroup
                    || parent.getSqlNode() instanceof SqlAlterTableAddPartition
                    || parent.getSqlNode() instanceof SqlAlterTableDropPartition
                    || parent.getSqlNode() instanceof SqlAlterTableOptimizePartition
                    || parent.getSqlNode() instanceof SqlAlterTableModifyPartitionValues)) {
                // do nothing
            } else {
                onDropTableSuccess(ddl, tableSchema, executionContext);
            }
            break;
        case RENAME_TABLE:
            onRenameTableSuccess(ddl, tableSchema, executionContext);
            break;
        case ALTER_TABLE:
            onAlterTableSuccess(ddl, tableSchema, executionContext);
            break;
        case CREATE_INDEX:
            onCreateIndexSuccess(ddl, tableSchema, executionContext);
            break;
        case DROP_INDEX:
            onDropIndexSuccess(ddl, tableSchema, executionContext);
            break;
        case TRUNCATE_TABLE:
            onTruncateTableSuccess(ddl, tableSchema, executionContext);
            break;
        default:
            // Nothing to do with other DDLs.
            break;
        }
        finalOperationsOnSuccess(ddl, tableSchema);
    }

    public static void restoreRule(PhyDdlTableOperation ddl, String tableSchema, ExecutionContext executionContext) {
        String tableName = ddl.getLogicalTableName();

        PhyInfoSchemaContext phyInfoSchemaContext = new PhyInfoSchemaContext();
        phyInfoSchemaContext.tableSchema = tableSchema;
        phyInfoSchemaContext.tableName = tableName;

        String seqName = AUTO_SEQ_PREFIX + tableName;
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(tableSchema, seqName);
        if (existingType != Type.NA) {
            if (existingType == Type.GROUP) {
                SequenceRecord sequenceRecord = new SequenceRecord();
                sequenceRecord.schemaName = tableSchema;
                sequenceRecord.name = seqName;
                phyInfoSchemaContext.sequenceRecord = sequenceRecord;
            } else {
                SequenceOptRecord sequenceOptRecord = new SequenceOptRecord();
                sequenceOptRecord.schemaName = tableSchema;
                sequenceOptRecord.name = seqName;
                phyInfoSchemaContext.sequenceRecord = sequenceOptRecord;
            }
        }

        triggerSchemaChange(phyInfoSchemaContext, ddl, executionContext, false);
    }

    private static boolean isCreateTableSupported(PhyDdlTableOperation ddl, ExecutionContext executionContext) {
        if (((SqlCreateTable) ddl.getNativeSqlNode()).isTemporary()) {
            // Don't support creating temporary table yet.
            return false;
        }
        if (ddl.isHint()) {
            // Special handling for hint with Async DDL.
            executionContext.getAsyncDDLContext().setFenceExempted(true);
            return false;
        }
        return true;
    }

    private static void beforeCreateTableStart(PhyDdlTableOperation ddl, String tableSchema,
                                               ExecutionContext executionContext) {
        validateLogicalTableName(ddl);

        String tableName = ddl.getLogicalTableName();

        // Validate hot mapping rules.
        validateMappingRules(OptimizerContext.getContext(tableSchema).getRuleManager().getTddlRule(),
            tableSchema,
            tableName,
            ddl.getTableRule() == null ? null : ddl.getTableRule().getExtPartitions());

        boolean isTableExists = false;
        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            isTableExists = tableInfoManager.checkIfTableExists(tableSchema, tableName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }

        DDL parent = ddl.getParent();
        SqlNode sqlNode = parent != null ? parent.getSqlNode() : null;
        boolean isAddPartition = false;

        AsyncDDLContext asyncDDLContext = executionContext.getAsyncDDLContext();
        if (isTableExists) {
            if (!asyncDDLContext.isAsyncDDLSupported()) {
                // Undo GSI meta.
                onDDLFail(ddl, tableSchema);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_ALREADY_EXISTS, tableName);
        }

        boolean isGsi = isCurrentTableGsi(ddl);
        boolean isAutoPartition = isCreateAutoPartition(ddl);

        if (ddl.getPartitionInfo() == null) {
            addNewTableExtMeta(ddl, tableSchema, tableName, isGsi, isAutoPartition, executionContext);
        } else {
            addNewTablePartitionMeta(ddl, tableSchema, tableName, isGsi, executionContext, isAddPartition);
        }

        if (isGsi) {
            ExecutorContext.getContext(tableSchema).getGsiManager().beginCreateGSI(ddl, ddl.getSchemaName());
        }
    }

    private static void addNewTableExtMeta(PhyDdlTableOperation ddl, String tableSchema, String tableName,
                                           boolean isGsi, boolean isAutoPartition,
                                           ExecutionContext executionContext) {
        // Convert table extension record.
        TableRule newTableRule = ddl.getTableRule();
        TablesExtRecord record =
            TableMetaUtil.convertToTablesExtRecord(newTableRule, tableSchema, tableName, isGsi, isAutoPartition);

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        // Add rule related table meta.
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            tableInfoManager.addTableExt(record);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    private static void addNewTablePartitionMeta(PhyDdlTableOperation ddl,
                                                 String tableSchema,
                                                 String tableName,
                                                 boolean isGsi,
                                                 ExecutionContext executionContext,
                                                 boolean isUpsert) {
        // Convert table extension record.

        PartitionInfo partitionInfo = ddl.getPartitionInfo();
        if (isUpsert) {
            partitionInfo = ((PartitionTableDdlView) ddl.getParent()).getPartitionInfoAltered();
        }

        Map<String, PartitionInfo> gsiPartitionInfoMap =
            ((PartitionTableDdlView) ddl.getParent()).getAllGsiPartitionInfos();
        boolean isCreateIndex =
            ((PartitionTableDdlView) ddl.getParent()).getNativeSqlNode().getKind() == SqlKind.CREATE_INDEX;
        List<PartitionInfo> gsiPartitionInfos = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(gsiPartitionInfoMap)) {
            gsiPartitionInfos
                .addAll(gsiPartitionInfoMap.entrySet().stream().map(o -> o.getValue()).collect(Collectors.toList()));
        }
        int gsiCount = gsiPartitionInfoMap.size();
        int i = -1;
        List<TableGroupDetailConfig> tableGroupConfigs = new ArrayList<>();
        boolean isPrimaryTable = true;
        do {
            TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(partitionInfo);
            List<TablePartitionRecord> partRecList = PartitionInfoUtil.prepareRecordForAllPartitions(partitionInfo);
            Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
                .prepareRecordForAllSubpartitions(partRecList, partitionInfo,
                    partitionInfo.getPartitionBy().getPartitions());
            TableGroupRecord tableGroupRecord = null;
            List<PartitionGroupRecord> partitionGroupRecords = null;

            // need to create a new table group and related partition groups
            if (partitionInfo.getTableGroupId() < 0) {
                // TODO(moyi) auto flag?
                tableGroupRecord = PartitionInfoUtil.prepareRecordForTableGroup(partitionInfo);
                partitionGroupRecords =
                    PartitionInfoUtil.prepareRecordForPartitionGroups(
                        partitionInfo.getTableSchema(), partitionInfo.getPartitionBy().getPhysicalPartitions(), false);
            }

            TablePartRecordInfoContext tablePartRecordInfoContext = new TablePartRecordInfoContext();
            tablePartRecordInfoContext.setLogTbRec(logTableRec);
            tablePartRecordInfoContext.setPartitionRecList(partRecList);
            tablePartRecordInfoContext.setSubPartitionRecMap(subPartRecInfos);
            tablePartRecordInfoContext.setSubPartitionRecList(
                TablePartRecordInfoContext.buildAllSubPartitionRecList(subPartRecInfos));
            List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
            tablePartRecordInfoContexts.add(tablePartRecordInfoContext);

            TableGroupDetailConfig tableGroupConfig =
                new TableGroupDetailConfig(tableGroupRecord, partitionGroupRecords, tablePartRecordInfoContexts,
                    tableGroupRecord.getLocality());
            tableGroupConfigs.add(tableGroupConfig);
            i++;
            isPrimaryTable = false;
        } while (isGsi && (i < gsiCount) && (partitionInfo = gsiPartitionInfos.get(i)) != null);

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            tableInfoManager.setConnection(conn);
            for (TableGroupDetailConfig tableGroupConfig : tableGroupConfigs) {
                tableInfoManager.addTablePartitionInfos(tableGroupConfig, isUpsert);
            }
            conn.commit();
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    private static void beforeDropTableStart(PhyDdlTableOperation ddl, String tableSchema,
                                             ExecutionContext executionContext) {
        validateLogicalTableName(ddl);

        AsyncDDLContext asyncDDLContext = executionContext.getAsyncDDLContext();
        if (asyncDDLContext.isJobRolledBack()) {
            return;
        }

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        String tableName = ddl.getLogicalTableName();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Make all table meta invisible first.
                tableInfoManager.hideTable(tableSchema, tableName);
                TableInfoManager.updateTableVersion(tableSchema, tableName, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "hide table meta");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            syncTableDataId(tableSchema, tableName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    public static void syncTableDataId(String schema, String table) {
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schema, table);
        sync(tableDataId);
    }

    public static void syncTableDataId(List<String> tableDataIdList) {
        sync(tableDataIdList);
    }

    private static void beforeRenameTableStart(PhyDdlTableOperation ddl, String tableSchema,
                                               ExecutionContext executionContext) {
        if (ddl.getLogicalTableName() == null || ddl.getLogicalTableName().isEmpty()
            || ddl.getNewLogicalTableName() == null || ddl.getNewLogicalTableName().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "The source table name and/or target table name shouldn't be empty");
        }

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        String tableName = ddl.getLogicalTableName();
        String newTableName = ddl.getNewLogicalTableName();

        String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Add new table name
                tableInfoManager.addNewTableName(tableSchema, tableName, newTableName);

                CONFIG_MANAGER.notify(tableDataId, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "add new table name");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            sync(tableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    private static void beforeAlterTableStart(PhyDdlTableOperation ddl, String tableSchema,
                                              ExecutionContext executionContext) {
        if (ddl.getNativeSqlNode() != null && ddl.getNativeSqlNode() instanceof SqlAlterTable) {
            validateLogicalTableName(ddl);

            SqlAlterTable alterTable = (SqlAlterTable) ddl.getNativeSqlNode();

            String tableName = alterTable.getOriginTableName().getLastName();
            String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

            List<String> droppedColumns = getAlteredColumns(alterTable, ColumnOpt.DROP);
            List<String> droppedIndexes = new ArrayList<>();

            List<SqlAlterSpecification> alterItems = alterTable.getAlters();
            if (alterItems != null && alterItems.size() > 0) {
                for (SqlAlterSpecification alterItem : alterItems) {
                    if (alterItem instanceof SqlAlterTableDropIndex) {
                        SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alterItem;
                        droppedIndexes.add(dropIndex.getIndexName().getLastName());
                    }
                }
            }
            final String logicalTableName = ddl.getLogicalTableName();
            final DataDefLanguageLogicView parent = ddl.getParent();

            if ((droppedColumns != null && droppedColumns.size() > 0) || droppedIndexes.size() > 0) {
                TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
                long newVersion = 0;
                String primaryTableSchema = null;
                String primaryTableName = null;
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableInfoManager.setConnection(metaDbConn);
                    try {
                        MetaDbUtil.beginTransaction(metaDbConn);

                        if (droppedColumns != null && droppedColumns.size() > 0) {
                            // Hide dropped column meta.
                            tableInfoManager.hideColumns(tableSchema, tableName, droppedColumns);
                        }

                        if (droppedIndexes.size() > 0) {
                            // Hide dropped index meta.
                            tableInfoManager.hideIndexes(tableSchema, tableName, droppedIndexes);
                        }

                        // Hide columns in primary table if drop column in GSI.
                        if (parent.getGsiParentSqlNode().size() > 0) {
                            // Hide column in primary table.
                            final SqlAlterTable parentSqlNode = (SqlAlterTable) parent.getNativeSqlNode();
                            primaryTableSchema = 2 == parentSqlNode.getOriginTableName().names.size() ?
                                parentSqlNode.getOriginTableName().names.get(0) : tableSchema;
                            primaryTableName = parentSqlNode.getOriginTableName().getLastName();
                            if (!tableSchema.equalsIgnoreCase(primaryTableSchema) &&
                                !tableName.equalsIgnoreCase(primaryTableName)) {
                                // Only do this when drop GSI column(We do DDL GSI first and then primary).
                                tableInfoManager.hideColumns(primaryTableSchema, primaryTableName, droppedColumns);
                            } else {
                                // Clear names for no sync.
                                primaryTableSchema = null;
                                primaryTableName = null;
                            }
                        }

                        newVersion = TableInfoManager.updateTableVersion(tableSchema, tableName, metaDbConn);
                        CONFIG_MANAGER.notify(tableDataId, metaDbConn);

                        MetaDbUtil.commit(metaDbConn);
                    } catch (SQLException e) {
                        MetaDbUtil
                            .rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "hide dropped columns/indexes");
                    } finally {
                        MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                    }
                    sync(tableDataId);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                } finally {
                    tableInfoManager.setConnection(null);
                }

                if (newVersion == 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "impossible");
                }

                // Wipe out all active trx when drop column in GSI.
                if (droppedColumns != null && droppedColumns.size() > 0 && parent.getGsiParentSqlNode().size() > 0 &&
                    primaryTableSchema != null && primaryTableName != null) {
                    SyncManagerHelper.sync(new GsiStatusChangeSyncAction(primaryTableSchema,
                            primaryTableName,
                            tableName,
                            executionContext.getConnId(),
                            executionContext.getTraceId()),
                        tableSchema, SyncScope.ALL, true);
                }
            } else if (!parent.getGsiParentSqlNode().isEmpty() && ConfigDataMode.isPolarDbX()) {
                // Only PolarDB-X support clustered index.
                final List<String> addedColumns = getAlteredColumns(alterTable, ColumnOpt.ADD);
                final SqlAlterTable gsiAlterTable =
                    (SqlAlterTable) parent.getGsiParentSqlNode().get(logicalTableName);

                final boolean operationOnGsi = gsiAlterTable != null;
                final boolean addColumn = addedColumns != null && addedColumns.size() > 0 && operationOnGsi;
                final boolean alterDefault =
                    operationOnGsi && gsiAlterTable.getAlters() != null && 1 == gsiAlterTable.getAlters().size()
                        && gsiAlterTable.getAlters().get(0) instanceof SqlAlterColumnDefaultVal;
                final boolean alterColumn = !parent.getAlterColumnSpecificationSets().isEmpty();
                final boolean alterColumnAlterDefault = operationOnGsi && alterColumn &&
                    parent.getAlterColumnSpecificationSets().get(0).stream().anyMatch(
                        DataDefLanguageLogicView.ALTER_COLUMN_DEFAULT::contains);
                final boolean alterColumnRename = operationOnGsi && alterColumn &&
                    parent.getAlterColumnSpecificationSets().get(0).stream().anyMatch(
                        DataDefLanguageLogicView.ALTER_COLUMN_RENAME::contains);

                if (addColumn) {
                    ExecutorContext.getContext(tableSchema).getGsiManager()
                        .beginAlterTableAddColumnsGsi(ddl, ddl.getSchemaName());
                } else if (alterDefault || alterColumnAlterDefault) {
                    TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
                    String primaryTableSchema = null;
                    String primaryTableName = null;
                    long newVersion = 0;
                    try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                        tableInfoManager.setConnection(metaDbConn);
                        try {
                            MetaDbUtil.beginTransaction(metaDbConn);

                            final SqlAlterTable parentSqlNode = (SqlAlterTable) parent.getNativeSqlNode();
                            primaryTableSchema = 2 == parentSqlNode.getOriginTableName().names.size() ?
                                parentSqlNode.getOriginTableName().names.get(0) : ddl.getSchemaName();
                            primaryTableName = parentSqlNode.getOriginTableName().getLastName();

                            // Mark primary table to fill the default value.
                            // Alter table always changes GSI first, so this happens at the first sub job of alter table.
                            if (alterDefault) {
                                final SqlAlterColumnDefaultVal alterColumnDefaultVal =
                                    (SqlAlterColumnDefaultVal) gsiAlterTable.getAlters().get(0);

                                tableInfoManager.beginUpdateColumnDefaultVal(primaryTableSchema, primaryTableName,
                                    alterColumnDefaultVal.getColumnName().getLastName());
                            } else if (alterColumnAlterDefault) {
                                switch (gsiAlterTable.getAlters().get(0).getKind()) {
                                case CHANGE_COLUMN: {
                                    final SqlChangeColumn changeColumn =
                                        (SqlChangeColumn) gsiAlterTable.getAlters().get(0);

                                    // When rename + reset default, no need to begin auto fill.
                                    // Insertion will keep report error until metaDB changed.
                                    if (!alterColumnRename) {
                                        tableInfoManager
                                            .beginUpdateColumnDefaultVal(primaryTableSchema, primaryTableName,
                                                changeColumn.getOldName().getLastName());
                                    }
                                }
                                break;

                                case MODIFY_COLUMN: {
                                    final SqlModifyColumn modifyColumn =
                                        (SqlModifyColumn) gsiAlterTable.getAlters().get(0);

                                    tableInfoManager.beginUpdateColumnDefaultVal(primaryTableSchema, primaryTableName,
                                        modifyColumn.getColName().getLastName());
                                }
                                break;

                                default:
                                    throw GeneralUtil.nestedException("Unknown alter table column while change MetaDB");
                                }
                            }
                            newVersion =
                                TableInfoManager.updateTableVersion(primaryTableSchema, primaryTableName, metaDbConn);
                            CONFIG_MANAGER.notify(tableDataId, metaDbConn);

                            MetaDbUtil.commit(metaDbConn);
                        } catch (SQLException e) {
                            MetaDbUtil
                                .rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "enable default filling.");
                        } finally {
                            MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                        }
                        sync(tableDataId);
                    } catch (SQLException e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                    } finally {
                        tableInfoManager.setConnection(null);
                    }
                    if (newVersion == 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "impossible");
                    }
                    // Need sync all table meta and wipe out active trx.
                    if (primaryTableSchema != null && primaryTableName != null) {
                        SyncManagerHelper.sync(new GsiStatusChangeSyncAction(primaryTableSchema,
                                primaryTableName,
                                tableName,
                                executionContext.getConnId(),
                                executionContext.getTraceId()),
                            tableSchema, SyncScope.ALL, true);
                    }
                }
            }
        }
    }

    private static void beforeCreateIndexStart(PhyDdlTableOperation ddl, String tableSchema,
                                               ExecutionContext executionContext) {
        // NO-OP
    }

    private static void beforeDropIndexStart(PhyDdlTableOperation ddl, String tableSchema,
                                             ExecutionContext executionContext) {
        if (ddl.getNativeSqlNode() != null && ddl.getNativeSqlNode() instanceof SqlDropIndex) {
            SqlDropIndex dropIndex = (SqlDropIndex) ddl.getNativeSqlNode();

            String tableName = dropIndex.getOriginTableName().getLastName();
            String droppedIndex = dropIndex.getIndexName().getLastName();
            String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

            if (TStringUtil.isNotEmpty(droppedIndex) && TStringUtil.isNotEmpty(tableName)) {
                TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableInfoManager.setConnection(metaDbConn);
                    try {
                        MetaDbUtil.beginTransaction(metaDbConn);

                        // Hide dropped index meta.
                        tableInfoManager.hideIndex(tableSchema, tableName, droppedIndex);

                        CONFIG_MANAGER.notify(tableDataId, metaDbConn);

                        MetaDbUtil.commit(metaDbConn);
                    } catch (SQLException e) {
                        MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "hide dropped index");
                    } finally {
                        MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                    }
                    sync(tableDataId);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                } finally {
                    tableInfoManager.setConnection(null);
                }
            }
        }
    }

    public static List<String> getAlteredColumns(SqlAlterTable alterTable, ColumnOpt columnOpt) {
        Map<ColumnOpt, List<String>> columnOpts = alterTable.getColumnOpts();
        if (columnOpts != null && columnOpts.size() > 0) {
            return columnOpts.get(columnOpt);
        }
        return null;
    }

    private static void onCreateTableSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                             ExecutionContext executionContext) {
        if (!(ddl.getNativeSqlNode() instanceof SqlCreateTable)) {
            return;
        }

        String tableName = ddl.getLogicalTableName();
        PhyInfoSchemaContext phyInfoSchemaContext;

        boolean ignoreAddNewTableMeta = false;
        if (((DDL) ddl.getParent()).getSqlNode() instanceof SqlAlterTable) {
            SqlAlterTable sqlAlterTable = (SqlAlterTable) ((DDL) ddl.getParent()).getSqlNode();
            if (sqlAlterTable.isAddPartition()) {
                ignoreAddNewTableMeta = true;
                Long oldTableGroupId = ddl.getPartitionInfo().getTableGroupId();
                TableGroupInfoManager tableGroupInfoManager =
                    OptimizerContext.getContext(tableSchema).getTableGroupInfoManager();
                boolean isDelete = false;
                try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                    isDelete = deleteTheEmptyTableGroup(tableSchema, tableName, oldTableGroupId,
                        conn);
                } catch (Throwable ex) {
                    throw GeneralUtil.nestedException(ex);
                }

                if (isDelete) {
                    tableGroupInfoManager.reloadTableGroupByGroupId(oldTableGroupId);
                }
            }
        }

        if (!ignoreAddNewTableMeta) {
            phyInfoSchemaContext = addNewTableMeta(ddl, tableSchema, tableName, executionContext);
        } else {
            phyInfoSchemaContext = getPhyInfoSchemaContext(ddl, tableSchema, tableName);
        }

        triggerSchemaChange(phyInfoSchemaContext, ddl, executionContext, true);

        // NOTE: we need to read locality from originSqlNode, since the physical ddl has been rewritten
        SqlNode originSqlNode = ((DataDefLanguageLogicView) ddl.getParent()).getSqlNode();
        if (originSqlNode instanceof SqlCreateTable) {
            SqlCreateTable sqlCreate = (SqlCreateTable) originSqlNode;
            if (TStringUtil.isNotBlank(sqlCreate.getLocality())) {
                TableMeta tableMeta =
                    OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);
                LocalityManager.getInstance().setLocalityOfTable(tableMeta.getId(), sqlCreate.getLocality());
            }
        }

        changeGsiStatus(ddl, executionContext, tableSchema);
    }

    private static PhyInfoSchemaContext addNewTableMeta(PhyDdlTableOperation ddl, String tableSchema, String tableName,
                                                        ExecutionContext executionContext) {
        PhyInfoSchemaContext phyInfoSchemaContext = getPhyInfoSchemaContext(ddl, tableSchema, tableName);

        // Add sequence meta if needed.
        SequenceBean sequenceBean = createSequenceIfExists(ddl, tableSchema, tableName, executionContext);
        if (sequenceBean != null) {
            SequenceBaseRecord sequenceRecord = SequenceUtil.convert(sequenceBean, tableSchema, executionContext);
            phyInfoSchemaContext.sequenceRecord = sequenceRecord;
        }

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            tableInfoManager.addTable(phyInfoSchemaContext, 0L, SequenceUtil.buildFailPointInjector(executionContext),
                null, null, null);
            return phyInfoSchemaContext;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    protected static SequenceBean createSequenceIfExists(PhyDdlTableOperation ddl, String tableSchema,
                                                         String tableName, ExecutionContext executionContext) {
        SequenceBean sequenceBean = ddl.getSequence();

        if (sequenceBean == null || !sequenceBean.isNew()) {
            return null;
        }

        // Check if need default sequence type.
        if (sequenceBean.getType() == Type.NA) {
            final TableRule tableRule = ddl.getTableRule();
            if (tableRule != null && (ddl.isPartitioned() || tableRule.isBroadcast())) {
                sequenceBean.setType(AutoIncrementType.GROUP);
            } else {
                return null;
            }
        }

        String seqName = AUTO_SEQ_PREFIX + tableName;

        Type existingSeqType = SequenceManagerProxy.getInstance().checkIfExists(tableSchema, seqName);
        if (existingSeqType != Type.NA) {
            boolean isRecovery = executionContext.getAsyncDDLContext().isJobRecovered();
            boolean allowForCreateTable = ddl.getKind() == SqlKind.CREATE_TABLE && (ddl.isIfNotExists() || isRecovery);
            boolean allowForAlterTable = ddl.getKind() == SqlKind.ALTER_TABLE;
            if (allowForCreateTable || allowForAlterTable) {
                return null;
            }
            // Warn user since the sequence already exists.
            StringBuilder errMsg = new StringBuilder();
            errMsg.append(existingSeqType).append(" SEQUENCE '");
            errMsg.append(seqName).append("' already exists. ");
            errMsg.append(
                "Please try another name to create new sequence or alter an existing sequence instead.");
            throw new SequenceException(errMsg.toString());
        }

        // Use START WITH 1 by default when there is no table option
        // AUTO_INCREMENT = xx specified.
        if (sequenceBean.getStart() == null) {
            sequenceBean.setStart(DEFAULT_START_WITH);
        }

        sequenceBean.setKind(SqlKind.CREATE_SEQUENCE);

        sequenceBean.setName(seqName);

        SequenceValidator.validateSimpleSequence(sequenceBean, executionContext);

        return sequenceBean;
    }

    private static PhyInfoSchemaContext getPhyInfoSchemaContext(PhyDdlTableOperation ddl, String tableSchema,
                                                                String tableName) {
        IGroupExecutor groupExecutor =
            ExecutorContext.getContext(tableSchema).getTopologyHandler().get(ddl.getDbIndex());

        if (groupExecutor == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "Not found group executor for " + ddl.getDbIndex());
        }

        TGroupDataSource dataSource = (TGroupDataSource) groupExecutor.getDataSource();

        String phyTableSchema = DdlHelper.getPhyTableSchema(dataSource);
        String phyTableName = Util.last(Util.last(ddl.getTableNames()));

        if (dataSource == null || TStringUtil.isBlank(tableName) || TStringUtil.isBlank(phyTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "invalid data source or physical table name");
        }

        PhyInfoSchemaContext phyInfoSchemaContext = new PhyInfoSchemaContext();
        phyInfoSchemaContext.dataSource = dataSource;
        phyInfoSchemaContext.tableSchema = tableSchema;
        phyInfoSchemaContext.tableName = tableName;
        phyInfoSchemaContext.phyTableSchema = phyTableSchema;
        phyInfoSchemaContext.phyTableName = phyTableName;
        phyInfoSchemaContext.dnId = DdlHelper.getDnId(dataSource);

        return phyInfoSchemaContext;
    }

    private static void triggerSchemaChange(PhyInfoSchemaContext phyInfoSchemaContext,
                                            PhyDdlTableOperation ddl,
                                            ExecutionContext executionContext, boolean isNew) {
        String tableSchema = phyInfoSchemaContext.tableSchema;
        String tableName = phyInfoSchemaContext.tableName;

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(tableSchema);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

        if (ddl.getKind() == SqlKind.CREATE_TABLE) {
            SqlCreateTable sqlCreateTable = (SqlCreateTable) ddl.getNativeSqlNode();
            //todo just work for DRDS table now
            if (ddl.getPartitionInfo() == null) {
                CdcManagerHelper.getInstance()
                    .notifyDdl(tableSchema, tableName, ddl.getKind().name(), sqlCreateTable.getSourceSql(),
                        executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public,
                        buildExtendParameter(executionContext));
            }
        }

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Show all table meta.
                tableInfoManager.showTable(tableSchema, tableName, phyInfoSchemaContext.sequenceRecord);

                if (isNew) {
                    // Register new table data id.
                    CONFIG_MANAGER.register(tableDataId, metaDbConn);
                    // update table list data id
                    CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
                } else {
                    // Notify existing table data id.
                    /**
                     * Upgrade both the opVersion of data_id and version of tables
                     */
                    tableInfoManager.updateVersionAndNotify(tableSchema, tableName);
                }

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "show table meta");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }

            if (isNew) {
                /**
                 * Sync to refresh table list of db for new created table
                 */
                sync(tableListDataId);

                /**
                 * Update the version of new created table to refresh table meta in metadb
                 */
                try {
                    MetaDbUtil.beginTransaction(metaDbConn);
                    /**
                     * Upgrade both the opVersion of data_id and  version of tables
                     */
                    tableInfoManager.updateVersionAndNotify(tableSchema, tableName);
                    MetaDbUtil.commit(metaDbConn);
                } catch (SQLException e) {
                    MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "show table meta");
                } finally {
                    MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                }
            }

            /**
             * Sync to refresh table meta of memory of all cn
             */
            sync(tableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    private static void onDropTableSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                           ExecutionContext executionContext) {
        if (!(ddl.getNativeSqlNode() instanceof SqlDropTable)) {
            return;
        }

        String tableName = ddl.getLogicalTableName();

        // 在beforeDdlStart阶段已经更新了元数据，即：表已经对外不可见，进入此方法时所有物理表也已经删除成功，所以打标位置没有特别要求
        if (ddl.getPartitionInfo() == null) {
            CdcManagerHelper.getInstance()
                .notifyDdl(tableSchema, tableName, ddl.getKind().name(), executionContext.getOriginSql(),
                    executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public,
                    buildExtendParameter(executionContext));
        }

        // Delete locality of this table
        SchemaManager schemaManager = OptimizerContext.getContext(tableSchema).getLatestSchemaManager();
        LocalityManager lm = LocalityManager.getInstance();
        TableMeta tableMeta = schemaManager.getTableWithNull(tableName);
        if (tableMeta != null && lm.getLocalityOfTable(tableMeta.getId()) != null) {
            lm.deleteLocalityOfTable(tableMeta.getId());
        }

        removeTableMeta(tableSchema, tableName, executionContext);

        changeGsiStatus(ddl, executionContext, tableSchema);

        if (ddl.getPartitionInfo() != null) {
            Long oldTableGroupId = ddl.getPartitionInfo().getTableGroupId();
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(tableSchema).getTableGroupInfoManager();
            boolean isDelete = false;

            try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
                isDelete = deleteTheEmptyTableGroup(tableSchema, tableName, oldTableGroupId,
                    connection);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
            if (isDelete) {
                tableGroupInfoManager.reloadTableGroupByGroupId(oldTableGroupId);
            }
        }
        final String primaryTableName =
            ExecutorContext.getContext(tableSchema).getGsiManager().dropGsi(ddl, tableSchema, false, executionContext);
        if (TStringUtil.isNotBlank(primaryTableName)) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                TableInfoManager.updateTableVersion(tableSchema, primaryTableName, metaDbConn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            }
            SyncManagerHelper.syncThrowExceptions(new DropTableSyncAction(tableSchema, primaryTableName), SyncScope.ALL);
        }
    }

    public static void removeTableMeta(String tableSchema, String tableName, ExecutionContext executionContext) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(tableSchema);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

        // Remove sequence meta if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean = dropSequenceIfExists(tableSchema, tableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, tableSchema, executionContext);
        }

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Remove all table meta.
                tableInfoManager.removeTable(tableSchema, tableName, sequenceRecord, true, true);
                tableInfoManager.removeTableExt(tableSchema, tableName);

                // Unregister the table data id.
                CONFIG_MANAGER.unregister(tableDataId, metaDbConn);

                CONFIG_MANAGER.notify(tableListDataId, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (Exception e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "remove table meta");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }

            sync(tableListDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }

        AsyncDDLContext asyncDDLContext = executionContext.getAsyncDDLContext();
        if (asyncDDLContext.isJobRolledBack() || asyncDDLContext.isJobRecovered()) {
            TableRuleManager.reload(tableSchema, tableName);
        }
    }

    protected static SequenceBean dropSequenceIfExists(String tableSchema, String tableName) {
        String seqName = AUTO_SEQ_PREFIX + tableName;
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(tableSchema, seqName);
        if (existingType != Type.NA) {
            SequenceBean sequenceBean = new SequenceBean();
            sequenceBean.setSchemaName(tableSchema);
            sequenceBean.setName(seqName);
            sequenceBean.setKind(SqlKind.DROP_SEQUENCE);
            return sequenceBean;
        }
        return null;
    }

    private static void onRenameTableSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                             ExecutionContext executionContext) {
        if (!(ddl.getNativeSqlNode() instanceof SqlRenameTable)) {
            return;
        }

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        String tableName = ddl.getLogicalTableName();
        String newTableName = ddl.getNewLogicalTableName();

        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(tableSchema);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);
        String newTableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, newTableName);

        TableRule tableRule = OptimizerContext.getContext(tableSchema).getRuleManager().getTableRule(tableName);
        String newTbNamePattern = tableRule.getTbNamePattern();

        if (!executionContext.needToRenamePhyTables()) {
            newTbNamePattern = TStringUtil.replaceWithIgnoreCase(newTbNamePattern, tableName, newTableName);
        }

        /**
         * When random phy table is enabled, should NOT to rename phy table for single or broadcast tbl
         */
        //For single or broadcast table, always rename it's physical tableName as it's logical tableName
//        TddlRuleManager ruleManager = OptimizerContext.getContext(tableSchema).getRuleManager();
//        if (ruleManager.isTableInSingleDb(tableName) || ruleManager.isBroadCast(tableName)) {
//            newTbNamePattern = newTableName;
//        }

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean = renameSequenceIfExists(tableSchema, tableName, newTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, tableSchema, executionContext);
        }

        // 如果物理表名也发生了变化，需要将新的tablePattern作为附加参数传给cdcManager
        // 如果物理表名也发生了变更，此处所有物理表已经都完成了rename(此时用户针对该逻辑表提交的任何dml操作都会报错)，cdc打标必须先于元数据变更
        // 如果物理表名未进行变更，那么tablePattern不会发生改变，Rename是一个轻量级的操作，打标的位置放到元数据变更之前或之后，都可以
        Map<String, Object> params = buildExtendParameter(executionContext);
        params.put(ICdcManager.TABLE_NEW_NAME, newTableName);
        params.put(ICdcManager.TABLE_NEW_PATTERN, newTbNamePattern);
        if (ddl.getPartitionInfo() == null) {
            CdcManagerHelper.getInstance()
                .notifyDdl(tableSchema, tableName, ddl.getKind().name(), executionContext.getOriginSql(),
                    executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public, params);
        }

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Replace with new table name
                tableInfoManager.renameTable(tableSchema, tableName, newTableName, newTbNamePattern, sequenceRecord);

                // Unregister the old table data id.
                CONFIG_MANAGER.unregister(tableDataId, metaDbConn);

                // Register new table data id.
                CONFIG_MANAGER.register(newTableDataId, metaDbConn);

                CONFIG_MANAGER.notify(tableListDataId, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "add new table name");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }

            // Make each node bind listener for new table dataId.
            sync(tableListDataId);

            // Reload the new table related meta data.
            CONFIG_MANAGER.notify(newTableDataId, metaDbConn);
            CONFIG_MANAGER.sync(newTableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }

        changeGsiStatus(ddl, executionContext, tableSchema);

        // Primary table schema will change when rename GSI.
        final String primaryTableName =
            ExecutorContext.getContext(tableSchema).getGsiManager().renameGsi(ddl, tableSchema);
        if (TStringUtil.isNotBlank(primaryTableName)) {
            String primaryTableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, primaryTableName);
            CONFIG_MANAGER.notify(primaryTableDataId, null);
            sync(primaryTableDataId);
        }
    }

    protected static SequenceBean renameSequenceIfExists(String tableSchema, String tableName, String newTableName) {
        String seqName = AUTO_SEQ_PREFIX + tableName;
        String newSeqName = AUTO_SEQ_PREFIX + newTableName;
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(tableSchema, seqName);
        if (existingType != Type.NA) {
            SequenceBean sequenceBean = new SequenceBean();
            sequenceBean.setSchemaName(tableSchema);
            sequenceBean.setName(seqName);
            sequenceBean.setNewName(newSeqName);
            sequenceBean.setKind(SqlKind.RENAME_SEQUENCE);
            return sequenceBean;
        }
        return null;
    }

    private static void onAlterTableSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                            ExecutionContext executionContext) {
        if (!(ddl.getNativeSqlNode() instanceof SqlAlterTable)) {
            return;
        }

        SqlAlterTable alterTable = (SqlAlterTable) ddl.getNativeSqlNode();

        String tableName = alterTable.getOriginTableName().getLastName();
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

        List<String> droppedColumns = getAlteredColumns(alterTable, ColumnOpt.DROP);
        List<String> addedColumns = getAlteredColumns(alterTable, ColumnOpt.ADD);
        List<String> modifiedColumns = getAlteredColumns(alterTable, ColumnOpt.MODIFY);

        final String logicalTableName = ddl.getLogicalTableName();
        final DataDefLanguageLogicView parent = ddl.getParent();
        final boolean addColumnsWithGsi =
            addedColumns != null && addedColumns.size() > 0 && parent.getGsiParentSqlNode().size() > 0;
        final boolean addDefaultCurrentTimeStampColumnsWithGsi =
            addColumnsWithGsi && isAddDefaultCurrentTimeStampColumns(ddl, executionContext);
        final boolean primaryTableAddColumnsWithGsi =
            addDefaultCurrentTimeStampColumnsWithGsi
                && parent.getGsiParentSqlNode().get(logicalTableName)
                == null; // Handle GSI then primary and this is the primary(final step of alter).

        List<String> updatedColumns = new ArrayList<>();
        if (modifiedColumns != null && modifiedColumns.size() > 0) {
            updatedColumns.addAll(modifiedColumns);
        }

        List<Pair<String, String>> changedColumns = new ArrayList<>();

        // We have to use the first column name as reference to confirm the physical index name
        // because Alter Table allows to add an index without specifying an index name.
        List<String> addedIndexes = new ArrayList<>();
        List<String> addedIndexesWithoutNames = new ArrayList<>();

        List<String> droppedIndexes = new ArrayList<>();
        List<Pair<String, String>> renamedIndexes = new ArrayList<>();

        boolean primaryKeyDropped = false;
        boolean foreignKeyDropped = false;
        List<String> addedPrimaryKeyColumns = new ArrayList<>();

        List<String> autoFillingDefault = new ArrayList<>();
        String primaryTableSchema = null;
        String primaryTableName = null;
        boolean columnReorder = false;

        List<SqlAlterSpecification> alterItems = alterTable.getAlters();
        if (alterItems != null && alterItems.size() > 0) {
            for (SqlAlterSpecification alterItem : alterItems) {
                if (alterItem instanceof SqlChangeColumn) {
                    SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                    changedColumns.add(
                        Pair.of(changeColumn.getNewName().getLastName(), changeColumn.getOldName().getLastName()));

                    // Refresh is needed when reorder.
                    if (changeColumn.isFirst() || changeColumn.getAfterColumn() != null) {
                        columnReorder = true;
                    }
                } else if (alterItem instanceof SqlAlterColumnDefaultVal) {
                    SqlAlterColumnDefaultVal alterColumnDefaultVal = (SqlAlterColumnDefaultVal) alterItem;
                    updatedColumns.add(alterColumnDefaultVal.getColumnName().getLastName());
                } else if (alterItem instanceof SqlAddIndex) {
                    SqlAddIndex addIndex = (SqlAddIndex) alterItem;
                    String firstColumnName = addIndex.getIndexDef().getColumns().get(0).getColumnNameStr();
                    if (addIndex.getIndexName() != null) {
                        addedIndexes.add(addIndex.getIndexName().getLastName());
                    } else {
                        // If user doesn't specify an index name, we use the first column name as reference.
                        addedIndexesWithoutNames.add(firstColumnName);
                    }
                } else if (alterItem instanceof SqlAlterTableDropIndex) {
                    SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alterItem;
                    droppedIndexes.add(dropIndex.getIndexName().getLastName());
                } else if (alterItem instanceof SqlAlterTableRenameIndex) {
                    SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) alterItem;
                    renamedIndexes.add(
                        Pair.of(renameIndex.getNewIndexName().getLastName(), renameIndex.getIndexName().getLastName()));
                } else if (alterItem instanceof SqlModifyColumn) {
                    final SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                    updatedColumns.add(modifyColumn.getColName().getLastName());

                    // Refresh is needed when reorder.
                    if (modifyColumn.isFirst() || modifyColumn.getAfterColumn() != null) {
                        columnReorder = true;
                    }
                } else if (alterItem instanceof SqlDropPrimaryKey) {
                    primaryKeyDropped = true;
                } else if (alterItem instanceof SqlAddPrimaryKey) {
                    SqlAddPrimaryKey addPrimaryKey = (SqlAddPrimaryKey) alterItem;
                    for (SqlIndexColumnName indexColumnName : addPrimaryKey.getColumns()) {
                        addedPrimaryKeyColumns.add(indexColumnName.getColumnNameStr());
                    }
                } else if (alterItem instanceof SqlAddColumn) {
                    SqlAddColumn addColumn = (SqlAddColumn) alterItem;
                    if (addColumn.isFirst() || addColumn.getAfterColumn() != null) {
                        columnReorder = true;
                    }
                } else if (alterItem instanceof SqlDropColumn) {
                    columnReorder = true;
                } else if (alterItem instanceof SqlDropForeignKey) {
                    foreignKeyDropped = true;
                }

                // Special dealing for table with GSI.
                if (!parent.getGsiParentSqlNode().isEmpty()) {
                    final SqlAlterTable parentSqlNode = (SqlAlterTable) parent.getNativeSqlNode();
                    primaryTableSchema = 2 == parentSqlNode.getOriginTableName().names.size() ?
                        parentSqlNode.getOriginTableName().names.get(0) : ddl.getSchemaName();
                    primaryTableName = parentSqlNode.getOriginTableName().getLastName();
                    // Record if alter with GSI and this is primary table.
                    if (primaryTableName.equalsIgnoreCase(alterTable.getOriginTableName().getLastName())) {
                        if (alterItem instanceof SqlAlterColumnDefaultVal) {
                            // Change default alters GSI first and then primary. So this is the final step of this kind of alter.
                            final SqlAlterColumnDefaultVal alterColumnDefaultVal = (SqlAlterColumnDefaultVal) alterItem;
                            autoFillingDefault.add(alterColumnDefaultVal.getColumnName().getLastName());
                        } else if (alterItem instanceof SqlChangeColumn) {
                            // Change column alters GSI first and then primary. So this is the final step of this kind of alter.
                            if (parent.getAlterColumnSpecificationSets().get(0).stream()
                                .anyMatch(DataDefLanguageLogicView.ALTER_COLUMN_DEFAULT::contains) &&
                                parent.getAlterColumnSpecificationSets().get(0).stream()
                                    .noneMatch(DataDefLanguageLogicView.ALTER_COLUMN_RENAME::contains)) {
                                // Change default but not change name.
                                final SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                                autoFillingDefault.add(changeColumn.getOldName().getLastName());
                            }
                        } else if (alterItem instanceof SqlModifyColumn) {
                            // Modify column alters GSI first and then primary. So this is the final step of this kind of alter.
                            if (parent.getAlterColumnSpecificationSets().get(0).stream()
                                .anyMatch(DataDefLanguageLogicView.ALTER_COLUMN_DEFAULT::contains)) {
                                // Change default but not change name.
                                final SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                                autoFillingDefault.add(modifyColumn.getColName().getLastName());
                            }
                        }
                    }
                }
            }
        }

        PhyInfoSchemaContext context = getPhyInfoSchemaContext(ddl, tableSchema, tableName);

        // Change sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean = alterSequenceIfExists(ddl, tableSchema, tableName, executionContext);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, tableSchema, executionContext);
        }

        if (ddl.getPartitionInfo() == null) {
            CdcManagerHelper.getInstance()
                .notifyDdl(tableSchema, tableName, ddl.getKind().name(), executionContext.getOriginSql(),
                    executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public,
                    buildExtendParameter(executionContext));
        }
        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
        long newVersion = 0;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            LogicMeta.LogicalTableMetaDetail logicalTableMetaDetail = tableInfoManager.fetchLogicalTableMetaFromInfoSchema(context);
            tableInfoManager.setConnection(metaDbConn);

            Map<String, Map<String, Object>> columnJdbcExtInfo = logicalTableMetaDetail.getColumnsJdbcExtInfo();

            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Remove dropped column meta if exist.
                if (droppedColumns != null && droppedColumns.size() > 0) {
                    tableInfoManager.removeColumns(tableSchema, tableName, droppedColumns);
                }

                // Add new column meta if exist.
                if (addedColumns != null && addedColumns.size() > 0) {
                    tableInfoManager.addColumns(context, columnJdbcExtInfo, addedColumns, logicalTableMetaDetail);
                }

                // Update existing column meta if exist.
                if (updatedColumns.size() > 0) {
                    tableInfoManager.updateColumns(context, columnJdbcExtInfo, updatedColumns, logicalTableMetaDetail);
                }

                // Disable the auto filling of default on this column.
                if (autoFillingDefault.size() > 0) {
                    for (String columnName : autoFillingDefault) {
                        tableInfoManager.endUpdateColumnDefaultVal(tableSchema, tableName, columnName);
                    }
                }

                // Update existing column meta if exist and column name may be changed as well.
                if (changedColumns.size() > 0) {
                    tableInfoManager.changeColumns(context, columnJdbcExtInfo, changedColumns, logicalTableMetaDetail);
                }

                // Need to refresh all column to ensure the correct order of column.
                if (columnReorder) {
                    tableInfoManager.refreshColumnOrder(context, columnJdbcExtInfo);
                }

                // Add new index meta if existed.
                if (addedIndexes.size() > 0) {
                    tableInfoManager.addIndexes(context, addedIndexes, addedIndexesWithoutNames, logicalTableMetaDetail);
                }

                // Rename existing index meta.
                if (renamedIndexes.size() > 0) {
                    tableInfoManager.renameIndexes(tableSchema, tableName, renamedIndexes);
                }

                // Remove existing index meta.
                if (droppedIndexes.size() > 0) {
                    tableInfoManager.removeIndexes(tableSchema, tableName, droppedIndexes);
                }

                // Drop existing primary key.
                if (primaryKeyDropped) {
                    tableInfoManager.dropPrimaryKey(tableSchema, tableName);
                }

                // Add new primary key.
                if (addedPrimaryKeyColumns != null && addedPrimaryKeyColumns.size() > 0) {
                    tableInfoManager.addPrimaryKey(context);
                }

                // Drop existing foreign key.
                if (foreignKeyDropped) {
                    tableInfoManager.dropPrimaryKey(tableSchema, tableName);
                }

                // Sequence meta if needed.
                if (sequenceRecord != null) {
                    if (sequenceBean.isNew()) {
                        tableInfoManager.createSequence(sequenceRecord, 0L,
                            SequenceUtil.buildFailPointInjector(executionContext));
                    } else {
                        tableInfoManager.alterSequence(sequenceRecord, 0L);
                    }
                }
                if (!addDefaultCurrentTimeStampColumnsWithGsi) {
                    tableInfoManager.showTable(tableSchema, tableName, sequenceRecord);
                }

                newVersion = TableInfoManager.updateTableVersion(tableSchema, tableName, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "maintain column/index meta");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            sync(tableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }

        if (newVersion == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "impossible");
        }
        changeGsiStatus(ddl, executionContext, tableSchema);

        ExecutorContext.getContext(tableSchema).getGsiManager().alterGsi(ddl, tableSchema);

        if (primaryTableAddColumnsWithGsi) {
            changeAddColumnsStatusWithGsi(ddl, tableSchema, executionContext);
        } else if (autoFillingDefault.size() > 0) {
            // Need sync all table meta and wipe out active trx.
            if (primaryTableSchema != null && primaryTableName != null) {
                SyncManagerHelper.sync(new GsiStatusChangeSyncAction(primaryTableSchema,
                        primaryTableName,
                        tableName,
                        executionContext.getConnId(),
                        executionContext.getTraceId()),
                    tableSchema, SyncScope.ALL, true);
            }
        } else {
            if (primaryTableSchema != null && primaryTableName != null) {
                SyncManagerHelper
                    .syncThrowExceptions(new TableMetaChangeSyncAction(primaryTableSchema, primaryTableName), tableSchema,
                        SyncScope.ALL);
            }
        }
    }

    protected static void changeAddColumnsStatusWithGsi(PhyDdlTableOperation ddl, String schemaName,
                                                        ExecutionContext executionContext) {
        SqlAlterTable alterTable = (SqlAlterTable) ddl.getNativeSqlNode();

        String primaryTableName = alterTable.getOriginTableName().getLastName();
        List<String> addedColumns = getAlteredColumns(alterTable, ColumnOpt.ADD);
        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
        final List<String> gsiTableNames = new ArrayList<>(parent.getGsiParentSqlNode().keySet());
        boolean done = false;
        while (!done) {
            ColumnStatus columnStatusBefore;
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                tableInfoManager.setConnection(metaDbConn);
                columnStatusBefore = ColumnStatus.convert(
                    tableInfoManager.queryOneColumn(schemaName, primaryTableName, addedColumns.get(0)).get(0).status);

            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            } finally {
                tableInfoManager.setConnection(null);
            }
            ColumnStatus after;
            IndexStatus indexStatusBefore;
            IndexStatus indexStatusAfter;
            switch (columnStatusBefore) {
            case ABSENT:
                after = ColumnStatus.WRITE_ONLY;
                indexStatusBefore = IndexStatus.ABSENT;
                indexStatusAfter = IndexStatus.WRITE_ONLY;
                break;

            case WRITE_ONLY:
                after = ColumnStatus.WRITE_REORG;
                indexStatusBefore = IndexStatus.WRITE_ONLY;
                indexStatusAfter = IndexStatus.WRITE_REORG;
                break;

            case WRITE_REORG:
                // Data backfill processing.
                gsiAddColumnsBackfill(ddl, schemaName, executionContext);

                after = ColumnStatus.PUBLIC;
                indexStatusBefore = IndexStatus.WRITE_REORG;
                indexStatusAfter = IndexStatus.PUBLIC;
                break;

            case PUBLIC:
                done = true;
                continue;

            default:
                throw new TddlRuntimeException(ErrorCode.ERR_CLUSTERED_INDEX_ADD_COLUMNS,
                    "unexpected column status " + columnStatusBefore.name()
                        + ", Add column with Clustered Index error");
            }

            // Sleep 1s when debug mode. Keep this status for a while.
            final String dbgInfo = executionContext.getParamManager().getString(ConnectionParams.GSI_DEBUG);
            if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                System.out.println("GSI debug slow status: " + columnStatusBefore.name());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }

            // Update status
            updateAddColumnsStatusWithGsi(executionContext, schemaName, primaryTableName, addedColumns, gsiTableNames,
                columnStatusBefore, after, indexStatusBefore, indexStatusAfter);

            // Sleep 1s when debug mode. Keep status changed and not sync for a while.
            if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                System.out.println("GSI debug slow status: " + after.name() + " before sync.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    protected static void updateAddColumnsStatusWithGsi(ExecutionContext executionContext, String schemaName,
                                                        String primaryTableName, List<String> addedColumns,
                                                        List<String> gsiNames, ColumnStatus tableStatusBefore,
                                                        ColumnStatus tableStatusAfter, IndexStatus indexStatusBefore,
                                                        IndexStatus indexStatusAfter) {
        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        List<String> tablesDataId = new ArrayList<>();
        tablesDataId.add(MetaDbDataIdBuilder.getTableDataId(schemaName, primaryTableName));
        for (String gsiName : gsiNames) {
            tablesDataId.add(MetaDbDataIdBuilder.getTableDataId(schemaName, gsiName));
        }

        List<String> updateTables = new ArrayList<>();
        updateTables.add(primaryTableName);
        updateTables.addAll(gsiNames);
        long newVersion = 0;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);

            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                tableInfoManager
                    .updateColumnsStatus(schemaName, updateTables, addedColumns, tableStatusBefore.getValue(),
                        tableStatusAfter.getValue());
                tableInfoManager
                    .updateIndexesColumnsStatus(schemaName, primaryTableName, gsiNames, addedColumns,
                        indexStatusBefore.getValue(),
                        indexStatusAfter.getValue());
                newVersion = TableInfoManager.updateTableVersion(schemaName, primaryTableName, metaDbConn);

                CONFIG_MANAGER.notifyMultiple(tablesDataId, metaDbConn, false);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, schemaName, primaryTableName, "add columns");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            sync(tablesDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }

        if (newVersion == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "impossible");
        }
        // Sync is needed to wipe out active trx.
        for (String gsiName : gsiNames) {
            SyncManagerHelper.sync(new GsiStatusChangeSyncAction(schemaName,
                    primaryTableName,
                    gsiName,
                    executionContext.getConnId(),
                    executionContext.getTraceId()),
                schemaName, SyncScope.ALL, true);
        }
    }

    private static void gsiAddColumnsBackfill(PhyDdlTableOperation ddl, String schemaName,
                                              ExecutionContext executionContext) {
        final SqlAlterTable alterTable = (SqlAlterTable) ddl.getNativeSqlNode();
        final String primaryTableName = alterTable.getOriginTableName().getLastName();

        // Get primary table meta.
        final String schema = alterTable.getOriginTableName().names.size() >= 2 ?
            alterTable.getOriginTableName().names.get(alterTable.getOriginTableName().names.size() - 2) :
            executionContext.getSchemaName();
        final TableMeta tableMeta = executionContext.getSchemaManager(schema).getTable(primaryTableName);

        // Get all which need backfill.
        final List<String> needBackfillColumns = findNeedBackfillColumns(tableMeta, alterTable);
        final List<String> clustered =
            tableMeta.getGsiTableMetaBean().indexMap.entrySet().stream()
                .filter(pair -> pair.getValue().clusteredIndex)
                .map(Map.Entry::getKey).collect(Collectors.toList());

        if (needBackfillColumns.size() > 0 && clustered.size() > 0) {
            executionContext = executionContext.copy();
            executionContext.setBackfillId(executionContext.getDdlJobId());
            GsiBackfill backfill =
                GsiBackfill.createGsiAddColumnsBackfill(schemaName, primaryTableName, clustered, needBackfillColumns,
                    executionContext);

            AsyncDDLContext asyncDDLContext = executionContext.getAsyncDDLContext();

            final boolean origin = asyncDDLContext.isAsyncDDLSupported();
            asyncDDLContext.setAsyncDDLSupported(false);
            try {
                ExecutorHelper.execute(backfill, executionContext);
            } finally {
                asyncDDLContext.setAsyncDDLSupported(origin);
            }
        }
    }

    private static void onCreateIndexSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                             ExecutionContext executionContext) {
        if (ddl.getNativeSqlNode() != null && ddl.getNativeSqlNode() instanceof SqlCreateIndex) {
            SqlCreateIndex createIndex = (SqlCreateIndex) ddl.getNativeSqlNode();

            String tableName = createIndex.getOriginTableName().getLastName();
            String addedIndex = createIndex.getIndexName().getLastName();
            String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

            PhyInfoSchemaContext context = getPhyInfoSchemaContext(ddl, tableSchema, tableName);

            if (ddl.getPartitionInfo() == null) {
                CdcManagerHelper.getInstance()
                    .notifyDdl(tableSchema, tableName, ddl.getKind().name(), executionContext.getOriginSql(),
                        executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public,
                        buildExtendParameter(executionContext));
            }

            if (TStringUtil.isNotEmpty(addedIndex) && TStringUtil.isNotEmpty(tableName)) {
                TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableInfoManager.setConnection(metaDbConn);
                    try {
                        MetaDbUtil.beginTransaction(metaDbConn);

                        // Add new index meta if exist.
                        tableInfoManager.addIndex(context, addedIndex);

                        tableInfoManager.showTable(tableSchema, tableName, null);

                        TableInfoManager.updateTableVersion(tableSchema, tableName, metaDbConn);
                        CONFIG_MANAGER.notify(tableDataId, metaDbConn);

                        MetaDbUtil.commit(metaDbConn);
                    } catch (SQLException e) {
                        MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "add index meta");
                    } finally {
                        MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                    }

                    sync(tableDataId);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                } finally {
                    tableInfoManager.setConnection(null);
                }

                SyncManagerHelper.syncThrowExceptions(new TableMetaChangeSyncAction(tableSchema, tableName), SyncScope.ALL);
            }
        }
    }

    private static void onDropIndexSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                           ExecutionContext executionContext) {
        if (ddl.getNativeSqlNode() != null && ddl.getNativeSqlNode() instanceof SqlDropIndex) {
            SqlDropIndex dropIndex = (SqlDropIndex) ddl.getNativeSqlNode();

            String tableName = dropIndex.getOriginTableName().getLastName();
            String droppedIndex = dropIndex.getIndexName().getLastName();
            String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);

            if (ddl.getPartitionInfo() == null) {
                CdcManagerHelper.getInstance()
                    .notifyDdl(tableSchema, tableName, ddl.getKind().name(), executionContext.getOriginSql(),
                        executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public,
                        buildExtendParameter(executionContext));
            }

            if (TStringUtil.isNotEmpty(droppedIndex) && TStringUtil.isNotEmpty(tableName)) {
                TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableInfoManager.setConnection(metaDbConn);
                    try {
                        MetaDbUtil.beginTransaction(metaDbConn);

                        // Remove dropped index meta.
                        tableInfoManager.removeIndex(tableSchema, tableName, droppedIndex);
                        TableInfoManager.updateTableVersion(tableSchema, tableName, metaDbConn);
                        CONFIG_MANAGER.notify(tableDataId, metaDbConn);

                        MetaDbUtil.commit(metaDbConn);
                    } catch (SQLException e) {
                        MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "remove index meta");
                    } finally {
                        MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                    }

                    sync(tableDataId);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                } finally {
                    tableInfoManager.setConnection(null);
                }

                SyncManagerHelper.syncThrowExceptions(new TableMetaChangeSyncAction(tableSchema, tableName), SyncScope.ALL);
            }
        }
    }

    private static void onTruncateTableSuccess(PhyDdlTableOperation ddl, String tableSchema,
                                               ExecutionContext executionContext) {
        if (ddl.getNativeSqlNode() != null && ddl.getNativeSqlNode() instanceof SqlTruncateTable
            && ddl.getPartitionInfo() == null) {
            String tableName = ddl.getLogicalTableName();
            CdcManagerHelper.getInstance()
                .notifyDdl(tableSchema, tableName, ddl.getKind().name(), executionContext.getOriginSql(),
                    executionContext.getAsyncDDLContext().getJob(), CdcDdlMarkVisibility.Public,
                    buildExtendParameter(executionContext));
        }
    }

    public static void truncateTableIntoRecycleBin(DataDefLanguageLogicView ddl, String tableSchema,
                                                   ExecutionContext executionContext) {
        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)) {
            return;
        }
        String tableName = ddl.getTruncateTableName();
        if (RecycleBin.isRecyclebinTable(tableName)) {
            return;
        }

        RecycleBin bin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
        if (bin == null || bin.hasForeignConstraint(executionContext.getAppName(), tableName)) {
            return;
        }
        String olDbinName = ddl.getBinName();
        String binName = bin.genName();
        bin.add(binName, tableName);

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        String newTableName = binName;

        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(tableSchema);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);
        String newTableDataId = MetaDbDataIdBuilder.getTableDataId(tableSchema, newTableName);

        TableRule tableRule = OptimizerContext.getContext(tableSchema).getRuleManager().getTableRule(tableName);
        TableRule tempTableRule = OptimizerContext.getContext(tableSchema).getRuleManager().getTableRule(olDbinName);
        String newTbNamePattern = tableRule.getTbNamePattern();
        String tmpTbNamePattern = tempTableRule.getTbNamePattern();

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean = renameSequenceIfExists(tableSchema, tableName, newTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, tableSchema, executionContext);
        }

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                // Replace with new table name
                tableInfoManager.renameTable(tableSchema, tableName, binName, newTbNamePattern, sequenceRecord);
                tableInfoManager.renameTable(tableSchema, olDbinName, tableName, tmpTbNamePattern, null);

                // make config change work
                TableInfoManager.updateTableVersion(tableSchema, tableName, metaDbConn);
                CONFIG_MANAGER.notify(tableListDataId, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, tableSchema, tableName, "add new table name");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }

            // Make each node bind listener for new table dataId.
            sync(tableListDataId);

            // Reload the new table related meta data.
            CONFIG_MANAGER.notify(tableDataId, metaDbConn);
            CONFIG_MANAGER.notify(newTableDataId, metaDbConn);
            CONFIG_MANAGER.sync(tableDataId);
            CONFIG_MANAGER.sync(newTableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    protected static SequenceBean alterSequenceIfExists(PhyDdlTableOperation ddl, String tableSchema, String tableName,
                                                        ExecutionContext executionContext) {
        // Check if any new AUTO_INCREMENT column exists and create
        // the corresponding sequence if any.
        SequenceBean sequenceBean = createSequenceIfExists(ddl, tableSchema, tableName, executionContext);

        if (sequenceBean != null && sequenceBean.isNew()) {
            return sequenceBean;
        }

        // If no AUTO_INCREMENT column added and table options
        // exist, then check if table option AUTO_INCREMENT exists
        // and alter the existing sequence accordingly.
        sequenceBean = ddl.getSequence();

        String seqName = AUTO_SEQ_PREFIX + tableName;

        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(tableSchema, seqName);

        if (sequenceBean != null && !sequenceBean.isNew() && existingType != Type.TIME) {
            // Get table option AUTO_INCREMENT = xxx.
            final Long start = sequenceBean.getStart();
            if (start != null && start >= 0) {
                // If it's a single table, maybe there's no sequence.
                if (SequenceManagerProxy.getInstance().isUsingSequence(tableSchema, tableName)) {
                    sequenceBean.setKind(SqlKind.ALTER_SEQUENCE);

                    SequenceValidator.validateSimpleSequence(sequenceBean, executionContext);

                    sequenceBean.setSchemaName(tableSchema);
                    sequenceBean.setName(seqName);

                    if (sequenceBean.getInnerStep() == null) {
                        sequenceBean.setInnerStep(DEFAULT_INNER_STEP);
                    }

                    if (sequenceBean.getToType() == null) {
                        sequenceBean.setToType(Type.NA);
                    }

                    return sequenceBean;
                }
            }
        }

        return null;
    }

    private static void sync(String dataId) {
        try {
            // Sync to trigger immediate call of registered listener associated with the dataId.
            CONFIG_MANAGER.sync(dataId);
        } catch (Exception e) {
            // Wait enough time to make sure that listener action can be done
            // on each node by config manager in case of sync failure.
            int waitingTime =
                2 * (MetaDbConfigManager.DEFAULT_SCAN_INTERVAL + MetaDbConfigManager.DEFAULT_NOTIFY_INTERVAL);
            LOGGER.warn(
                "Failed to sync with config manager. Caused by: " + e.getMessage()
                    + "\nLet's wait at most " + (waitingTime / 1000)
                    + "seconds (twice of the time that scan interval plus notify interval in config manager)", e);
            try {
                Thread.sleep(waitingTime);
            } catch (Exception ignored) {
            }
        }
    }

    private static void sync(List<String> dataIds) {
        try {
            // Sync to trigger immediate call of registered listener associated with the dataIds.
            for (String dataId : dataIds) {
                CONFIG_MANAGER.sync(dataId);
            }
        } catch (Exception e) {
            // Wait enough time to make sure that listener action can be done
            // on each node by config manager in case of sync failure.
            int waitingTime =
                2 * (MetaDbConfigManager.DEFAULT_SCAN_INTERVAL + MetaDbConfigManager.DEFAULT_NOTIFY_INTERVAL);
            LOGGER.warn(
                "Failed to sync with config manager. "
                    + "\nLet's wait at most " + (waitingTime / 1000)
                    + "seconds (twice of the time that scan interval plus notify interval in config manager)", e);
            try {
                Thread.sleep(waitingTime);
            } catch (Exception ignored) {
            }
        }
    }

    private static void finalOperationsOnSuccess(PhyDdlTableOperation ddl, String schemaName) {
        invalidatePlan(ddl, schemaName);
        changeStatisticOnDDLSuccess(ddl, schemaName);
    }

    private static void changeGsiStatus(PhyDdlTableOperation ddl, ExecutionContext executionContext,
                                        String schemaName) {
        // For GSI completion(Status change before sync target table).
        if (needToCreateGsi(ddl)) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MDL)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    "must enable MDL when dynamic create GSI");
            }
            ExecutorContext.getContext(schemaName).getGsiManager().updateGsiStatus(ddl, schemaName, executionContext);
        }
    }

    private static void validateLogicalTableName(PhyDdlTableOperation ddl) {
        if (ddl.getLogicalTableName() == null || ddl.getLogicalTableName().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "The table name shouldn't be empty");
        }
    }

    private static boolean isDefaultCurrentTimeStampColumns(TableMeta primaryTable, SqlColumnDeclaration column) {
        if (column == null) {
            return false;
        }
        // Ref: https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html
        // Types: TIMESTAMP and DATETIME.
        // Function: CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(), NOW(), LOCALTIME, LOCALTIME(), LOCALTIMESTAMP, and LOCALTIMESTAMP().
        // timestamp 默认值可能在不指定的时候，自动带上current timestamp
        /**
         * Ref:
         * If the explicit_defaults_for_timestamp system variable is disabled, the first TIMESTAMP column
         * has both DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP if neither is specified explicitly.
         */
        final boolean hasTimestamp = primaryTable.getPhysicalColumns().stream()
            // This without scale string just basic type.
            .anyMatch(c -> c.getField().getDataType().getStringSqlType().equalsIgnoreCase("timestamp"));
        final String[] currentTimeFunction = {
            "CURRENT_TIMESTAMP",
            "NOW",
            "LOCALTIME",
            "LOCALTIMESTAMP"
        };
        if (column.getDataType().getTypeName().getLastName().toLowerCase().contains("timestamp") ||
            column.getDataType().getTypeName().getLastName().toLowerCase().contains("datetime")) {
            if (column.getDefaultVal() != null && column.getDefaultVal()
                .getValue() instanceof SQLCurrentTimeExpr.Type) {
                return true;
            }
            if (column.getDefaultExpr() != null) {
                for (String func : currentTimeFunction) {
                    if (column.getDefaultExpr().getOperator().getName().equalsIgnoreCase(func)) {
                        return true;
                    }
                }
            }
            if (!hasTimestamp && null == column.getDefaultVal() && null == column.getDefaultExpr()) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAddDefaultCurrentTimeStampColumns(PhyDdlTableOperation ddl, ExecutionContext ec) {
        final SqlNode sqlNode = ddl.getNativeSqlNode();
        if (sqlNode instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
            final String schema = alterTable.getOriginTableName().names.size() >= 2 ?
                alterTable.getOriginTableName().names.get(alterTable.getOriginTableName().names.size() - 2) :
                ec.getSchemaName();
            final String table = alterTable.getOriginTableName().getLastName();
            final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
            for (SqlAlterSpecification alter : alterTable.getAlters()) {
                if (alter instanceof SqlAddColumn) {
                    final SqlAddColumn addColumns = (SqlAddColumn) alter;
                    final SqlColumnDeclaration column = addColumns.getColDef();
                    if (isDefaultCurrentTimeStampColumns(tableMeta, column)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static List<String> findNeedBackfillColumns(TableMeta primaryTable, SqlAlterTable alterTable) {
        List<String> columns = new ArrayList<>();
        for (SqlAlterSpecification alter : alterTable.getAlters()) {
            if (alter instanceof SqlAddColumn) {
                final SqlAddColumn addColumn = (SqlAddColumn) alter;
                final SqlColumnDeclaration column = addColumn.getColDef();
                if (isDefaultCurrentTimeStampColumns(primaryTable, column)) {
                    columns.add(addColumn.getColName().getLastName());
                }
            }
        }

        // Caution: All columns with on update should added.
        primaryTable.getAutoUpdateColumns().forEach(c -> columns.add(c.getName()));
        return columns;
    }

    public static void alterRule(SqlAlterRule sqlAlterRule, ExecutionContext executionContext) {
        String schemaName = executionContext.getSchemaName();
        String tableName = sqlAlterRule.getName().toString();

        TddlRuleManager optimizerRule = OptimizerContext.getContext(schemaName).getRuleManager();

        TableRule tableRule = optimizerRule.getTableRule(tableName);
        if (tableRule == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_PASS_RULE_VALIDATE, "table rule not exist");
        }

        // 校验热点映射规则
        validateMappingRules(optimizerRule.getTddlRule(),
            null,
            tableRule.getVirtualTbName(),
            sqlAlterRule.getAddMappingRules());

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);

            TablesExtRecord existingRecord = tableInfoManager.queryTableExt(schemaName, tableName, false);

            if (sqlAlterRule.getAllowFullTableScan() != -1 || sqlAlterRule.getBroadcast() != -1) {
                alterRuleProperties(sqlAlterRule, existingRecord);
            } else {
                alterMappingRules(tableRule, sqlAlterRule, existingRecord);
            }

            String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, tableName);

            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                tableInfoManager.updateTableExt(existingRecord);
                TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, schemaName, tableName, "alter table properties");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            sync(tableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

    private static void alterRuleProperties(SqlAlterRule sqlAlterRule, TablesExtRecord existingRecord) {
        if (sqlAlterRule.getAllowFullTableScan() != -1) {
            existingRecord.fullTableScan = sqlAlterRule.getAllowFullTableScan();
        }
        if (sqlAlterRule.getBroadcast() != -1) {
            existingRecord.broadcast = sqlAlterRule.getBroadcast();
        }
    }

    private static void alterMappingRules(TableRule tableRule, SqlAlterRule sqlAlterRule,
                                          TablesExtRecord existingRecord) {
        List<MappingRule> allMappingRules = new ArrayList<>();

        if (tableRule.getExtPartitions() != null && tableRule.getExtPartitions().size() > 0) {
            allMappingRules.addAll(tableRule.getExtPartitions());
        }

        if (sqlAlterRule.getAddMappingRules() != null && sqlAlterRule.getAddMappingRules().size() > 0) {
            for (MappingRule mappingRule : sqlAlterRule.getAddMappingRules()) {
                boolean alreadyExists = false;
                for (MappingRule existingMappingRule : allMappingRules) {
                    if (TStringUtil.equals(existingMappingRule.getDb(), mappingRule.getDb())
                        && TStringUtil.equals(existingMappingRule.getTb(), mappingRule.getTb())
                        && TStringUtil.equals(existingMappingRule.getDbKeyValue(), mappingRule.getDbKeyValue())
                        && TStringUtil.equals(existingMappingRule.getTbKeyValue(), mappingRule.getTbKeyValue())) {
                        alreadyExists = true;
                        break;
                    }
                }
                if (!alreadyExists) {
                    allMappingRules.add(mappingRule);
                }
            }
        }

        if (sqlAlterRule.getDropMappingRules() != null && sqlAlterRule.getDropMappingRules().size() > 0) {
            for (MappingRule mappingRule : sqlAlterRule.getDropMappingRules()) {
                Iterator<MappingRule> iterator = allMappingRules.iterator();
                while (iterator.hasNext()) {
                    MappingRule existingMappingRule = iterator.next();
                    if (TStringUtil.equals(existingMappingRule.getDb(), mappingRule.getDb())
                        && TStringUtil.equals(existingMappingRule.getTb(), mappingRule.getTb())
                        && TStringUtil.equals(existingMappingRule.getDbKeyValue(), mappingRule.getDbKeyValue())
                        && TStringUtil.equals(existingMappingRule.getTbKeyValue(), mappingRule.getTbKeyValue())) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        existingRecord.extPartitions = TableMetaUtil.convertExtPartitionsToJSON(allMappingRules);
    }

    private static boolean deleteTheEmptyTableGroup(String tableSchema, String tableName, Long oldTableGroupId,
                                                    Connection metaDbConn) {
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(tableSchema).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigById(oldTableGroupId);
        boolean isEmptyTableGroup =
            tableGroupConfig == null || tableGroupConfig.getAllTables() == null
                || tableGroupConfig.getAllTables().size() == 0 || (
                tableGroupConfig.getAllTables().size() == 1 && tableGroupConfig
                    .getAllTables().get(0).equalsIgnoreCase(tableName));

        boolean isDelete = false;
        if (isEmptyTableGroup) {
            isDelete = TableGroupUtils.deleteEmptyTableGroupInfo(tableSchema, oldTableGroupId, metaDbConn);
        }
        return isDelete;
    }

}

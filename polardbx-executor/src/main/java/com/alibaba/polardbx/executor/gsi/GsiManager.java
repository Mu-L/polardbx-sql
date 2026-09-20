package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gms.util.DdlGmsUtils;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.sync.AlterTableSyncAction;
import com.alibaba.polardbx.executor.sync.GsiStatusChangeSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.IndexRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableRecord;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DataDefLanguageLogicView;
import com.alibaba.polardbx.optimizer.core.rel.GsiBackfill;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class GsiManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(GsiManager.class);

    private final String groupKey;
    private final TopologyHandler topologyHandler;
    private final StorageInfoManager storageInfoManager;

    private GsiManager(TopologyHandler topologyHandler, String groupKey, StorageInfoManager storageInfoManager) {
        this.topologyHandler = topologyHandler;
        this.groupKey = groupKey;
        this.storageInfoManager = storageInfoManager;
    }

    public GsiManager(TopologyHandler topologyHandler, TddlRuleManager tddlRuleManager,
                      StorageInfoManager storageInfoManager) {
        this(topologyHandler, tddlRuleManager.getDefaultDbIndex(), storageInfoManager);
    }

    @Override
    protected void doInit() {
        getGsiMetaManager().init();
    }

    /**
     * For DDL use only, do not consider status of gsi
     *
     * @param indexTableName table name
     * @return whether specified table is a gsi
     */
    public boolean isGsiTable(String schema, String indexTableName) {
        return getGsiMetaManager().isGsiTable(schema, indexTableName);
    }

    /**
     * Get meta of gsi with specified status from system table
     *
     * @param tableName primary table name
     * @param indexName index table name
     * @return gsi meta
     */
    public GsiIndexMetaBean getGsiIndexMeta(String schema, String tableName, String indexName,
                                            EnumSet<IndexStatus> statusSet) {
        return getGsiMetaManager().getIndexMeta(schema, tableName, indexName, statusSet);
    }

    /**
     * Get meta of gsi with specified status from system table
     *
     * @param primaryOrIndexTableName primary or index table name
     * @param statusSet gsi status
     * @return gsi meta
     */
    public GsiMetaBean getGsiTableAndIndexMeta(String schema, String primaryOrIndexTableName,
                                               EnumSet<IndexStatus> statusSet) {
        return getGsiMetaManager().getTableAndIndexMeta(schema, primaryOrIndexTableName, statusSet);
    }

    /**
     * this should only be used in old DDL engine, for new DDL engine, please use CreateGlobalIndexInsertMetaTask instead
     */
    public void beginCreateGSI(PhyDdlTableOperation ddl, String schemaName) {
        final String logicalTableName = ddl.getLogicalTableName();
        final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
        final SqlNode parentSqlNode = parent.getNativeSqlNode();

        final List<IndexRecord> indexRecords = new ArrayList<>();
        final List<TableRecord> tableRecords = new ArrayList<>();

        boolean isNewPartitionTable = DbInfoManager.getInstance().isNewPartitionDb(ddl.getSchemaName());
        if (parentSqlNode instanceof SqlCreate && ((SqlCreate) parentSqlNode).createGsi()) {
            if (parentSqlNode instanceof SqlCreateIndex) {
                final SqlCreateIndex createIndex = (SqlCreateIndex) parentSqlNode;
                GsiUtils.buildIndexMeta(indexRecords,
                    tableRecords,
                    createIndex,
                    ddl.getTableRule(),
                    schemaName,
                    createIndex.getPrimaryTableNode(),
                    IndexStatus.CREATING);
            } else if (parentSqlNode instanceof SqlAlterTable) {
                final SqlAlterTable alterTable = (SqlAlterTable) parentSqlNode;
                final SqlAddIndex addIndex = (SqlAddIndex) alterTable.getAlters().get(0);
                GsiUtils.buildIndexMeta(indexRecords,
                    tableRecords,
                    alterTable,
                    ddl.getTableRule(),
                    schemaName,
                    addIndex.getIndexDef().getPrimaryTableNode(),
                    IndexStatus.CREATING,
                    isNewPartitionTable);
            } else if (parentSqlNode instanceof SqlCreateTable) {
                final SqlCreateTable createTable = (SqlCreateTable) parentSqlNode;
                final SqlNode alterTable = parent.getGsiParentSqlNode().get(logicalTableName);
                if (null != alterTable) {
                    GsiUtils.buildIndexMeta(indexRecords,
                        tableRecords,
                        (SqlAlterTable) alterTable,
                        ddl.getTableRule(),
                        schemaName,
                        createTable,
                        IndexStatus.CREATING,
                        isNewPartitionTable);
                }
            }
        } else {
            return;
        }

        getGsiMetaManager().insertIndexMeta(indexRecords, tableRecords);
    }

    public void beginAlterTableAddColumnsGsi(PhyDdlTableOperation ddl, String schemaName) {
        final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
        final SqlAlterTable parentSqlNode = (SqlAlterTable) parent.getNativeSqlNode();
        final SqlAlterTable alterTable = (SqlAlterTable) ddl.getNativeSqlNode();

        final String tableName = parentSqlNode.getOriginTableName().getLastName();
        final String indexTableName = alterTable.getOriginTableName().getLastName();
        final GsiIndexMetaBean gsiIndexMetaBean =
            getGsiMetaManager().getIndexMeta(schemaName, tableName, indexTableName, IndexStatus.ALL);
        int seqInIndex = gsiIndexMetaBean.indexColumns.size() + gsiIndexMetaBean.coveringColumns.size() + 1;

        final List<IndexRecord> indexRecords = new ArrayList<>();

        GsiUtils.buildIndexMetaByAddColumns(indexRecords, alterTable, schemaName, tableName, indexTableName, seqInIndex,
            IndexStatus.ABSENT);
        getGsiMetaManager().insertIndexMetaByAddColumn(schemaName, tableName, indexRecords);

        // Need to sync and refresh the primary table meta. But clear active trx is not needed so just alter table sync.
        if (ConfigDataMode.isPolarDbX()) {
            DdlGmsUtils.syncTableDataId(schemaName, tableName);
            SyncManagerHelper.syncThrowExceptions(new TableMetaChangeSyncAction(schemaName, tableName), schemaName,
                SyncScope.ALL);
        } else {
            SyncManagerHelper.syncThrowExceptions(new AlterTableSyncAction(schemaName, tableName), SyncScope.ALL);
        }
    }

    public void updateGsiStatus(PhyDdlTableOperation ddl, String schemaName,
                                ExecutionContext executionContext) {
        if (ddl.getParent() instanceof DataDefLanguageLogicView) {
            final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
            final SqlNode parentSqlNode = parent.getNativeSqlNode();
            if (parentSqlNode instanceof SqlCreate && ((SqlCreate) parentSqlNode).createGsi()) {
                if (parentSqlNode instanceof SqlCreateIndex ||
                    parentSqlNode instanceof SqlAlterTable) {
                    /**
                     * For dynamic adding GSI.
                     *
                     * First check the status of current index.
                     * Then change status depend on previous status.
                     * Backfill when changed to write only.
                     * Until successful back fill then change to public.
                     */
                    String primaryTableName = null;
                    String indexName = null;
                    if (parentSqlNode instanceof SqlCreateIndex) {
                        final SqlCreateIndex createIndex = (SqlCreateIndex) parentSqlNode;
                        primaryTableName = createIndex.getOriginTableName().getLastName();
                        indexName = createIndex.getIndexName().getLastName();
                    } else if (parentSqlNode instanceof SqlAlterTable) {
                        final SqlAlterTable alterTable = (SqlAlterTable) parentSqlNode;
                        primaryTableName = alterTable.getOriginTableName().getLastName();
                        final SqlAddIndex addIndex = (SqlAddIndex) alterTable.getAlters().get(0);
                        indexName = addIndex.getIndexName().getLastName();
                    }

                    if (null == primaryTableName || null == indexName) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                            "unknown primary table name or index name, creating GSI error");
                    }

                    // Main status change loop, which depends on current status.
                    boolean gsiOnline = false;
                    while (!gsiOnline) {
                        final GsiIndexMetaBean gsiIndexMeta = ExecutorContext.getContext(schemaName).getGsiManager()
                            .getGsiIndexMeta(schemaName, primaryTableName, indexName, IndexStatus.ALL);

                        final IndexStatus before = gsiIndexMeta.indexStatus;
                        IndexStatus after;
                        switch (before) {
                        case CREATING:
                            after = IndexStatus.DELETE_ONLY;
                            break;

                        case DELETE_ONLY:
                            after = IndexStatus.WRITE_ONLY;
                            break;

                        case WRITE_ONLY:
                            after = IndexStatus.WRITE_REORG;
                            break;

                        case WRITE_REORG:
                            // Data backfill processing.
                            gsiBackfill((SqlCreate) parentSqlNode, executionContext);

                            after = IndexStatus.PUBLIC;
                            break;

                        case PUBLIC:
                            gsiOnline = true;
                            continue;

                        default:
                            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                                "unexpected index status " + before.name() + ", creating GSI error");
                        }

                        // Sleep 1s when debug mode. Keep this status for a while.
                        final String dbgInfo = executionContext.getParamManager().getString(ConnectionParams.GSI_DEBUG);
                        if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                            System.out.println("GSI debug slow status: " + before.name());
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ignore) {
                            }
                        }

                        if (parentSqlNode instanceof SqlAlterTablePartitionKey && after == IndexStatus.PUBLIC) {
                            return;
                        }

                        // Update status

                        long newVersion = getGsiMetaManager()
                            .updateIndexStatus(schemaName, primaryTableName, indexName, before, after);

                        // Sleep 1s when debug mode. Keep status changed and not sync for a while.
                        if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                            System.out.println("GSI debug slow status: " + after.name() + " before sync.");
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ignore) {
                            }
                        }

                        logger.info("Create GSI " + indexName + " state from "
                            + before + " to " + after + " before sync.");

                        //Sync will reload and clear cross status transaction.
                        SyncManagerHelper.sync(new GsiStatusChangeSyncAction(schemaName,
                                primaryTableName,
                                indexName,
                                executionContext.getConnId(),
                                executionContext.getTraceId()),
                            schemaName, SyncScope.ALL, true);

                        logger.info("Create GSI " + indexName + " state from "
                            + before + " to " + after + " after sync.");

                        final String finalStatus =
                            executionContext.getParamManager().getString(ConnectionParams.GSI_FINAL_STATUS_DEBUG);
                        if (StringUtils.equalsIgnoreCase(after.name(), finalStatus)) {
                            return;
                        }
                    }
                } else if (parentSqlNode instanceof SqlCreateTable) {
                    String primaryTableName = parent.getLogicalTableName();
                    if (ddl.getLogicalTableName().equals(primaryTableName)) {
                        // Successful creation of primary table. Then change index status to public.
                        getGsiMetaManager().updateIndexStatus(schemaName, primaryTableName,
                            IndexStatus.CREATING, IndexStatus.PUBLIC);

                        if (ConfigDataMode.isPolarDbX()) {
                            DdlGmsUtils.syncTableDataId(schemaName, primaryTableName);
                            SyncManagerHelper
                                .syncThrowExceptions(new TableMetaChangeSyncAction(schemaName, primaryTableName), schemaName,
                                    SyncScope.ALL);
                        } else {
                            // Sync created index table with alter action.
                            for (String gsiTableName : parent.getGsiParentSqlNode().keySet()) {
                                SyncManagerHelper.syncThrowExceptions(new AlterTableSyncAction(schemaName, gsiTableName), schemaName,
                                    SyncScope.ALL);
                            }
                        }
                    }
                }
            }
        }
    }

    public void undoCreateGsi(PhyDdlTableOperation ddl, String schemaName) {
        if (ddl.getParent() instanceof DataDefLanguageLogicView) {
            final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
            final SqlNode parentSqlNode = parent.getNativeSqlNode();
            if (parentSqlNode instanceof SqlCreate && ((SqlCreate) parentSqlNode).createGsi()) {
                if (parentSqlNode instanceof SqlCreateIndex) {
                    final SqlCreateIndex createIndex = (SqlCreateIndex) parentSqlNode;
                    final String tableName = createIndex.getOriginTableName().getLastName();
                    final String indexName = createIndex.getIndexName().getLastName();

                    getGsiMetaManager().removeIndexMeta(schemaName, tableName, indexName);
                } else if (parentSqlNode instanceof SqlAlterTable) {
                    final SqlAlterTable alterTable = (SqlAlterTable) parentSqlNode;
                    final String tableName = alterTable.getOriginTableName().getLastName();
                    final SqlAddIndex addIndex = (SqlAddIndex) alterTable.getAlters().get(0);
                    final String indexName = addIndex.getIndexName().getLastName();

                    getGsiMetaManager().removeIndexMeta(schemaName, tableName, indexName);
                } else if (parentSqlNode instanceof SqlCreateTable) {
                    final SqlAlterTable alterTable =
                        (SqlAlterTable) parent.getGsiParentSqlNode().get(ddl.getLogicalTableName());
                    final String tableName = null == alterTable ? ddl.getLogicalTableName() :
                        alterTable.getOriginTableName().getLastName();

                    // Drop all GSI which defined in creating status.
                    getGsiMetaManager()
                        .removeIndexMetaWithStatus(schemaName, tableName, IndexStatus.CREATING);
                }
            }
        }
    }

    public void alterGsi(PhyDdlTableOperation ddl, String schemaName) {
        if (ddl.getParent() instanceof DataDefLanguageLogicView) {
            final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
            final SqlNode parentSqlNode = parent.getNativeSqlNode();

            if (parentSqlNode instanceof SqlAlterTable) {
                final SqlAlterTable alterTable = (SqlAlterTable) parentSqlNode;
                if (alterTable.dropColumn()) {
                    final SqlDropColumn dropColumn = (SqlDropColumn) alterTable.getAlters().get(0);
                    final String tableName = alterTable.getOriginTableName().getLastName();
                    final String indexName = ddl.getLogicalTableName();
                    final String columnName = dropColumn.getColName().getLastName();

                    if (!TStringUtil.equalsIgnoreCase(tableName, indexName)) {
                        getGsiMetaManager().removeColumnMeta(schemaName, tableName, indexName, columnName);

                        if (ConfigDataMode.isPolarDbX()) {
                            DdlGmsUtils.syncTableDataId(schemaName, tableName);
                            SyncManagerHelper.syncThrowExceptions(new TableMetaChangeSyncAction(schemaName, tableName), schemaName,
                                SyncScope.ALL);
                        } else {
                            // Need to sync and refresh the table meta. But clear active trx is not needed so just alter table sync.
                            SyncManagerHelper.syncThrowExceptions(new AlterTableSyncAction(schemaName, tableName),
                                SyncScope.ALL);
                        }
                    }
                } else if (alterTable.changeColumn()) {
                    final SqlChangeColumn changeColumn = (SqlChangeColumn) alterTable.getAlters().get(0);
                    final String tableName = alterTable.getOriginTableName().getLastName();
                    final String indexName = ddl.getLogicalTableName();
                    final String oldColumnName = changeColumn.getOldName().getLastName();
                    final String newColumnName = changeColumn.getNewName().getLastName();
                    final String nullable = GsiUtils.nullable(changeColumn.getColDef());

                    if (!TStringUtil.equalsIgnoreCase(tableName, indexName)) {
                        getGsiMetaManager()
                            .changeColumnMeta(schemaName, tableName, indexName, oldColumnName, newColumnName, nullable);
                        if (ConfigDataMode.isPolarDbX()) {
                            DdlGmsUtils.syncTableDataId(schemaName, tableName);
                            SyncManagerHelper.syncThrowExceptions(new TableMetaChangeSyncAction(schemaName, tableName), schemaName,
                                SyncScope.ALL);
                        } else {
                            // Need to sync and refresh the table meta. But clear active trx is not needed so just alter table sync.
                            SyncManagerHelper.syncThrowExceptions(new AlterTableSyncAction(schemaName, tableName),
                                SyncScope.ALL);
                        }
                    }
                }
            }
        }
    }

    public void invalidateGsi(String schemaName, String tableName, String indexName, ExecutionContext ec) {
        boolean invalidated = false;
        while (!invalidated) {
            final GsiIndexMetaBean gsiIndexMeta = ExecutorContext.getContext(schemaName).getGsiManager()
                .getGsiIndexMeta(schemaName, tableName, indexName, IndexStatus.ALL);
            if (null == gsiIndexMeta) {
                return; // No GSI?
            }

            final IndexStatus before = gsiIndexMeta.indexStatus;
            final IndexStatus after;
            switch (before) {
            case CREATING:
            case DELETE_ONLY:
            case WRITE_ONLY:
            case WRITE_REORG:
            case PUBLIC:
                // Stop read first.
                after = IndexStatus.DROP_WRITE_ONLY;
                break;

            case DROP_WRITE_ONLY:
                after = IndexStatus.DROP_DELETE_ONLY;
                break;

            case DROP_DELETE_ONLY:
                after = IndexStatus.ABSENT;
                break;

            case ABSENT:
                invalidated = true;
                continue;

            default:
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    "unexpected index status " + before.name() + ", invalidate GSI error");
            }

            // Sleep 1s when debug mode. Keep this status for a while.
            final String dbgInfo = ec.getParamManager().getString(ConnectionParams.GSI_DEBUG);
            if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                logger.info("GSI debug slow status: " + before.name());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }

            // Update status
            long newVersion = getGsiMetaManager().updateIndexStatus(schemaName, tableName, indexName, before, after);

            // Sleep 1s when debug mode. Keep status changed and not sync for a while.
            if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.equalsIgnoreCase("slow")) {
                logger.info("GSI debug slow status: " + after.name() + " before sync.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }

            logger.info("Drop GSI " + indexName + " state from "
                + before + " to " + after + " before sync.");

            // Sync will reload and clear cross status transaction.
            SyncManagerHelper.sync(new GsiStatusChangeSyncAction(schemaName,
                    tableName,
                    indexName,
                    ec.getConnId(),
                    ec.getTraceId()),
                schemaName, SyncScope.ALL, true);

            logger.info("Drop GSI " + indexName + " state from "
                + before + " to " + after + " after sync.");
        }
    }

    public String dropGsi(PhyDdlTableOperation ddl, String schemaName, boolean invalidate_only, ExecutionContext ec) {
        if (ddl.getParent() instanceof DataDefLanguageLogicView) {
            final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
            final SqlNode parentSqlNode = parent.getNativeSqlNode();
            if (parentSqlNode instanceof SqlAlterTable && ((SqlAlterTable) parentSqlNode).dropIndex()) {
                final SqlAlterTable alterTable = (SqlAlterTable) parentSqlNode;
                final SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alterTable.getAlters().get(0);
                final String tableName = alterTable.getOriginTableName().getLastName();
                final String indexName = dropIndex.getIndexName().getLastName();

                if (invalidate_only) {
                    invalidateGsi(schemaName, tableName, indexName, ec);
                } else {
                    getGsiMetaManager().removeIndexMeta(schemaName, tableName, indexName);
                }
                return tableName;
            } else if (parentSqlNode instanceof SqlDropIndex) {
                final SqlDropIndex dropIndex = (SqlDropIndex) parentSqlNode;
                final String tableName = dropIndex.getOriginTableName().getLastName();
                final String indexName = dropIndex.getIndexName().getLastName();

                if (invalidate_only) {
                    invalidateGsi(schemaName, tableName, indexName, ec);
                } else {
                    getGsiMetaManager().removeIndexMeta(schemaName, tableName, indexName);
                }
                return tableName;
            } else if (parentSqlNode instanceof SqlDropTable) {
                final SqlDropTable sqlDropTable = (SqlDropTable) parentSqlNode;
                final String tableName = sqlDropTable.getOriginTableName().getLastName();
                final String indexName = ddl.getLogicalTableName();

                if (TStringUtil.equals(indexName, tableName)) {
                    /**
                     * Note:
                     * This is simple 'drop table' or step of dropping primary table in 'drop table' with GSI.
                     * GSI table(s) has been dropped before.
                     */
                    if (!invalidate_only) {
                        getGsiMetaManager().clearIndexMeta(schemaName, tableName);
                    }
                } else {
                    // Only invalidate it at step of dropping GSI table in 'drop table' operation.
                    if (invalidate_only) {
                        invalidateGsi(schemaName, tableName, indexName, ec);
                    } else {
                        getGsiMetaManager().removeIndexMeta(schemaName, tableName, indexName);
                    }
                }
                return tableName;
            }
        }

        return null;
    }

    public String renameGsi(PhyDdlTableOperation ddl, String schemaName) {
        if (ddl.getParent() instanceof DataDefLanguageLogicView) {
            final DataDefLanguageLogicView parent = (DataDefLanguageLogicView) ddl.getParent();
            final SqlNode parentSqlNode = parent.getNativeSqlNode();
            if (parentSqlNode instanceof SqlAlterTable && ((SqlAlterTable) parentSqlNode).renameIndex()) {
                final SqlAlterTable alterTable = (SqlAlterTable) parentSqlNode;
                final SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) alterTable.getAlters().get(0);
                final String tableName = alterTable.getOriginTableName().getLastName();
                final String indexName = renameIndex.getOriginIndexName().getLastName();
                final String newIndexName = renameIndex.getOriginNewIndexName().getLastName();

                getGsiMetaManager().renameIndexMeta(schemaName, tableName, indexName, newIndexName);
                return tableName;
            } else if (parentSqlNode instanceof SqlRenameTable) {
                final String tableName = ((SqlRenameTable) parentSqlNode).getOriginTableName().getLastName();
                final String newTableName = ((SqlRenameTable) parentSqlNode).getOriginNewTableName().getLastName();
                final GsiMetaBean gsiIndexMeta = ExecutorContext.getContext(schemaName)
                    .getGsiManager()
                    .getGsiTableAndIndexMeta(schemaName, tableName, IndexStatus.ALL);

                if (gsiIndexMeta.withGsi(tableName)) {
                    getGsiMetaManager().renamePrimaryTableMeta(schemaName, tableName, newTableName);
                    return newTableName;
                }
            }
        }
        return null;
    }

    public GsiMetaManager getGsiMetaManager() {

        DataSource gsiMgrDs = null;
        boolean lowerCaseTableName = true;
        if (!ConfigDataMode.isPolarDbX()) {
            gsiMgrDs = initGroupDatasource(topologyHandler, groupKey);
            lowerCaseTableName = storageInfoManager.isLowerCaseTableNames();
        } else {
            gsiMgrDs = MetaDbDataSource.getInstance().getDataSource();
        }
        return new GsiMetaManager(gsiMgrDs, topologyHandler.getAppName(), topologyHandler.getSchemaName(),
            lowerCaseTableName);
    }

    public static TGroupDataSource initGroupDatasource(TopologyHandler topologyHandler, String groupKey) {
        if (groupKey == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                "group key is null, GSI system table init error");
        }

        IGroupExecutor groupExecutor = topologyHandler.get(groupKey);

        return (TGroupDataSource) groupExecutor.getDataSource();
    }

    private static void gsiBackfill(SqlCreate sqlCreate, ExecutionContext executionContext) {
        if (!sqlCreate.createGsi()) {
            return;
        }

        /**
         * use latest meta to backfill
         */
        ExecutionContext backfillEc = executionContext.copy();
        backfillEc.setBackfillId(executionContext.getDdlJobId());
        backfillEc.refreshTableMeta();

        GsiBackfill backfill = null;
        if (sqlCreate instanceof SqlCreateIndex) {
            final SqlCreateIndex createIndex = (SqlCreateIndex) sqlCreate;
            final List<String> baseTableName = createIndex.getOriginTableName().names;
            final List<String> indexTableName = createIndex.getIndexName().names;
            final String schemaName =
                baseTableName.size() > 1 ? baseTableName.get(0) : backfillEc.getSchemaName();

            backfill = GsiBackfill
                .createGsiBackfill(schemaName, Util.last(baseTableName), Util.last(indexTableName), backfillEc);
        } else if (sqlCreate instanceof SqlAlterTable) {
            final SqlAlterTable alterTable = (SqlAlterTable) sqlCreate;
            final SqlAddIndex addIndex = (SqlAddIndex) alterTable.getAlters().get(0);
            final List<String> indexTableName = addIndex.getIndexName().names;
            final List<String> baseTableName = alterTable.getOriginTableName().names;
            final String schemaName =
                baseTableName.size() > 1 ? baseTableName.get(0) : backfillEc.getSchemaName();

            backfill = GsiBackfill
                .createGsiBackfill(schemaName, Util.last(baseTableName), Util.last(indexTableName), backfillEc);
        }

        Cursor cursor = null;
        try {
            if (executionContext.isRunOnNewDdlEngine()) {
                cursor = ExecutorHelper.execute(backfill, executionContext);
            } else {
                AsyncDDLContext asyncDDLContext = backfillEc.getAsyncDDLContext();
                final boolean origin = asyncDDLContext.isAsyncDDLSupported();
                asyncDDLContext.setAsyncDDLSupported(false);
                try {
                    cursor = ExecutorHelper.execute(backfill, backfillEc);
                } finally {
                    asyncDDLContext.setAsyncDDLSupported(origin);
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }
    }
}

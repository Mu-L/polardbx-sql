package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.AutoIncrementType;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.sync.ClearPlanCacheSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineInvalidatePlanSyncAction;
import com.alibaba.polardbx.executor.sync.CheckTableMetaVersionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.common.trx.ITimestampOracle;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DdlUtils {

    private static final Logger logger = LoggerFactory.getLogger(DdlUtils.class);


    public static boolean isEmpty(String str) {
        return ((str == null) || (str.length() == 0));
    }

    public static void onDDLFail(PhyDdlTableOperation ddl, String schemaName) {
        // Drop GSI meta if needed.
        if (needToCreateGsi(ddl)) {
            ExecutorContext.getContext(schemaName).getGsiManager().undoCreateGsi(ddl, schemaName);
        }
    }

    protected static boolean isCurrentTableGsi(PhyDdlTableOperation ddl) {
        return false;
    }

    private static boolean containTableName(String currentTableName,
                                            List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys) {
        if (globalKeys != null && globalKeys.size() > 0) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> globalKey : globalKeys) {
                String globalKeyName = globalKey.left.getLastName();
                if (TStringUtil.equalsIgnoreCase(currentTableName, globalKeyName)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected static boolean needToCreateGsi(PhyDdlTableOperation ddl) {
        return false;
    }

    protected static boolean isCreateAutoPartition(PhyDdlTableOperation ddl) {
        return false;
    }

    protected static void changeStatisticOnDDLSuccess(PhyDdlTableOperation ddl, String schemaName) {
//        switch (ddl.getKind()) {
//        case RENAME_TABLE:
//            String oldLogicalTableName = ddl.getLogicalTableName();
//            String newLogicalTableName = ddl.getNewLogicalTableName();
//            SyncManagerHelper.syncThrowExceptions(new RenameStatisticSyncAction(schemaName,
//                oldLogicalTableName,
//                newLogicalTableName), schemaName, SyncScope.ALL);
//        case DROP_TABLE:
//            SyncManagerHelper.syncThrowExceptions(
//                new RemoveTableStatisticSyncAction(schemaName, ddl.getLogicalTableName()),
//                schemaName, SyncScope.ALL);
//            break;
//        case ALTER_TABLE:
//            if (ddl.getNativeSqlNode() instanceof SqlAlterTable) {
//                List<String> dropColumnNameList = ((SqlAlterTable) ddl.getNativeSqlNode()).getColumnOpts()
//                    .get(SqlAlterTable.ColumnOpt.DROP);
//                List<String> modifyColumnNameList = ((SqlAlterTable) ddl.getNativeSqlNode()).getColumnOpts()
//                    .get(SqlAlterTable.ColumnOpt.MODIFY);
//                List<String> changeColumnNameList = ((SqlAlterTable) ddl.getNativeSqlNode()).getColumnOpts()
//                    .get(SqlAlterTable.ColumnOpt.CHANGE);
//
//                String logicalTableName = ddl.getLogicalTableName();
//                if (dropColumnNameList != null) {
//                    SyncManagerHelper.syncThrowExceptions(new RemoveColumnStatisticSyncAction(schemaName,
//                        logicalTableName,
//                        dropColumnNameList), schemaName, SyncScope.ALL);
//                }
//
//                if (modifyColumnNameList != null) {
//                    SyncManagerHelper.syncThrowExceptions(new RemoveColumnStatisticSyncAction(schemaName,
//                        logicalTableName,
//                        modifyColumnNameList), schemaName, SyncScope.ALL);
//                }
//
//                if (changeColumnNameList != null) {
//                    SyncManagerHelper.syncThrowExceptions(new RemoveColumnStatisticSyncAction(schemaName,
//                        logicalTableName,
//                        changeColumnNameList), schemaName, SyncScope.ALL);
//                }
//            }
//            break;
//        }
    }

    /**
     * invalidate plan cache in spm by schema and table
     */
    public static void invalidatePlan(PhyDdlTableOperation ddl, String schemaName) {
        String tableName = ddl.getLogicalTableName();
        switch (ddl.getKind()) {
        case DROP_TABLE:
            DdlUtils.invalidatePlan(schemaName, tableName, true);
            break;
        case DROP_INDEX:
        case RENAME_TABLE:
        case ALTER_TABLE:
        case CREATE_INDEX:
            DdlUtils.invalidatePlan(schemaName, tableName, false);
            break;
        case TRUNCATE_TABLE:
        case CREATE_TABLE:
        default:
            // Nothing to do with other DDLs.
            break;
        }
    }

    public static void invalidatePlan(String schema, String table, boolean isForce) {
        SyncManagerHelper.syncWithDefaultDb(new BaselineInvalidatePlanSyncAction(schema, table, isForce),
            SyncScope.ALL);
    }

    public static void invalidatePlanCache(String schema, String table) {
        SyncManagerHelper.syncWithDefaultDb(new ClearPlanCacheSyncAction(schema, table), SyncScope.ALL);
    }

    protected static void processCreateAutoIncrement(PhyDdlTableOperation ddl, ExecutionContext executionContext,
                                                     String schemaName) {
        final String tableName = ddl.getLogicalTableName();
        if (StringUtils.isNotEmpty(tableName)) {
            createSequenceIfAutoIncrementExists(ddl, tableName, executionContext, schemaName);
        }
    }

    protected static boolean createSequenceIfAutoIncrementExists(PhyDdlTableOperation ddl, String tableName,
                                                                 ExecutionContext executionContext, String schemaName) {
        boolean isAutoIncrement = false;
        SequenceBean sequence = ddl.getSequence();
        if (sequence == null) {
            return false;
        }
        if (!sequence.isNew()) {
            return false;
        }
        if (sequence.getType() == Type.NA) {
            final TableRule rule = ddl.getTableRule();
            if (rule != null && (ddl.isPartitioned() || rule.isBroadcast())) {
                sequence.setType(AutoIncrementType.GROUP);
            } else {
                return false;
            }
        }

        // If user specifies IF NOT EXISTS and table was created
        // successfully, then we should check if the corresponding sequence
        // already exists. If yes, then we return without trying to create
        // it again to avoid failure.
        if ((ddl.getKind() == SqlKind.CREATE_TABLE && ddl.isIfNotExists())
            || ddl.getKind() == SqlKind.ALTER_TABLE) {
            Type seqType = SequenceManagerProxy.getInstance().checkIfExists(schemaName,
                SequenceAttribute.AUTO_SEQ_PREFIX + tableName);
            if (seqType != Type.NA) {
                return true;
            }

        }
        // Use START WITH 1 by default when there is no table option
        // AUTO_INCREMENT = xx specified.
        Long startWith = 1L;
        if (sequence != null && sequence.getStart() != null) {
            startWith = sequence.getStart();
        }
        sequence.setStart(startWith);
        sequence.setName(SequenceAttribute.AUTO_SEQ_PREFIX + tableName);
        PhyDdlTableOperation sequencePhyTableOperation = (PhyDdlTableOperation) ddl.copy();
        sequencePhyTableOperation.setKind(SqlKind.CREATE_SEQUENCE);
        sequencePhyTableOperation
            .setDbIndex(OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null));
        sequencePhyTableOperation.setSequence(sequence);
        // Execute the plan to create a new sequence with specified
        // initial value and type.
        Cursor updateCursor = null;
        try {
            updateCursor = ExecutorContext.getContext(schemaName)
                .getTopologyExecutor()
                .execByExecPlanNode(sequencePhyTableOperation, executionContext);
        } finally {
            if (updateCursor != null) {
                updateCursor.close(new ArrayList<>());
            }
        }
        return isAutoIncrement;
    }

    protected static void processDropTableSequence(PhyDdlTableOperation ddl, ExecutionContext executionContext,
                                                   String schemaName) {
        String tableName = ddl.getLogicalTableName();
        if (StringUtils.isNotEmpty(tableName)) {
            // We don't get rule to determine if this is a single table
            // because table rule is always null for the 'drop table'
            // case so that we can't do such determination, so just drop
            // sequence regardless of single table or not.
            String seqName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;
            Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
            if (existingType != Type.NA) {
                SequenceBean sequenceBean = new SequenceBean();
                sequenceBean.setName(seqName);
                sequenceBean.setSchemaName(schemaName);
                sequenceBean.setKind(SqlKind.DROP_SEQUENCE);
                final PhyDdlTableOperation sequenceTableOperation = (PhyDdlTableOperation) ddl.copy();

                sequenceTableOperation.setDbIndex(OptimizerContext.getContext(schemaName)
                    .getRuleManager()
                    .getDefaultDbIndex(null));
                sequenceTableOperation.setSequence(sequenceBean);
                sequenceTableOperation.setKind(SqlKind.DROP_SEQUENCE);
                // Execute the plan to drop the existing sequence.
                Cursor updateCursor = null;
                try {
                    updateCursor = ExecutorContext.getContext(schemaName)
                        .getTopologyExecutor()
                        .execByExecPlanNode(sequenceTableOperation, executionContext);
                } finally {
                    if (updateCursor != null) {
                        updateCursor.close(new ArrayList<>());
                    }
                }
            }
        }
    }

    /**
     * 校验热点映射规则
     */
    public static void validateMappingRules(TddlRule currentTddlRule, String schemaName, String tableName,
                                            List<MappingRule> newMappingRules) {
        if (currentTddlRule != null && newMappingRules != null && newMappingRules.size() > 0) {

            List<MappingRule> existedMappingRules = Lists.newArrayList();
            List<String> tableNames = Lists.newArrayList();
            if (currentTddlRule.getTables() != null && currentTddlRule.getTables().size() > 0) {
                for (TableRule tbRule : currentTddlRule.getTables()) {
                    if (tbRule.getExtPartitions() != null && tbRule.getExtPartitions().size() > 0) {
                        existedMappingRules.addAll(tbRule.getExtPartitions());
                        for (int i = 0; i < tbRule.getExtPartitions().size(); i++) {
                            tableNames.add(tbRule.getVirtualTbName());
                        }
                    }
                }
            }

            Matrix matrix = ExecutorContext.getContext(schemaName).getTopologyHandler().getMatrix();
            for (MappingRule newRule : newMappingRules) {
                // validate if the hot mapping physical table
                String hotTablePattern = tableName + "_hot_";
                if (StringUtils.isNotBlank(newRule.getTb()) && !newRule.getTb().startsWith(hotTablePattern)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_HOT_TABLE_NAME_WRONG_PATTERN,
                        hotTablePattern + "{tag}",
                        hotTablePattern + "0");
                }

                // validate if the hot mapping group exists
                if (matrix.getGroup(newRule.getDb()) == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_HOT_GROUP_NOT_EXISTS, newRule.getDb());
                }

                // validate if the hot mapping physical table is in use
                for (int i = 0; i < existedMappingRules.size(); i++) {
                    MappingRule existedRule = existedMappingRules.get(i);

                    if (StringUtils.equalsIgnoreCase(existedRule.getDb(), newRule.getDb())
                        && StringUtils.isNotBlank(newRule.getTb())
                        && StringUtils.equalsIgnoreCase(existedRule.getTb(), newRule.getTb())) {
                        // if the newRule is the same with existedRule in the
                        // same table, ignore the error
                        if (StringUtils.equalsIgnoreCase(tableName, tableNames.get(i))
                            && StringUtils.equals(existedRule.getDbKeyValue(), newRule.getDbKeyValue())
                            && StringUtils.equals(existedRule.getTbKeyValue(), newRule.getTbKeyValue())) {
                            break;
                        } else {
                            throw new TddlRuntimeException(ErrorCode.ERR_MAPPING_RULE_ALREADY_EXISTS,
                                tableNames.get(i),
                                existedRule.getDb(),
                                existedRule.getTb());
                        }
                    }
                }
            }
        }
    }

    // Check if physical table names in new logical table topology have been
    // occupied by existing logical tables.
    public static void checkPhysicalTableNames(PhyDdlTableOperation ddlTableOperation,
                                               AsyncDDLContext asyncDDLContext) {
        if (ddlTableOperation.getKind() == SqlKind.CREATE_TABLE) {
            String schemaName = asyncDDLContext.getSchemaName();
            TableRule tableRule = ddlTableOperation.getTableRule();

            if (tableRule == null || ddlTableOperation.isHint()) {
                // Skip the check if a hint is specified.
                return;
            }

            String logicalTableName = ddlTableOperation.getLogicalTableName();
            TableRule existingRule =
                OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);

            if (existingRule != null) {
                // The logical table being created already exists, so we skip
                // the check and let original logic handle the scenario:
                // When newly created table has the same rule/topology with
                // existing table, the behavior depends on whether
                // "IF NOT EXISTS" is specified. Otherwise, newly created table
                // with different rule will fail.
                return;
            }

            // Check each physical table with fully qualified name.
            if ((tableRule.getDbPartitionKeys() == null || tableRule.getDbPartitionKeys().isEmpty())
                && (tableRule.getTbPartitionKeys() == null || tableRule.getTbPartitionKeys().isEmpty())) {
                // The group and physical table names are incomplete in the
                // topology for single or broadcast table, so we have to build
                // them manually.
                String physicalTableName = tableRule.getTbNamePattern();
                List<String> groupNames =
                    ExecutorContext.getContext(schemaName).getTopologyHandler().getGroupNames();
                for (String groupName : groupNames) {
                    checkFullyQualifiedPhysicalTableName(groupName + "." + physicalTableName, schemaName);
                }
            } else {
                // We can get all group and physical table names from the
                // topology for sharding table.
                Map<String, Set<String>> topology = tableRule.getActualTopology();
                for (Entry<String, Set<String>> groupAndPhysicalTableNames : topology.entrySet()) {
                    String groupName = groupAndPhysicalTableNames.getKey();
                    for (String physicalTableName : groupAndPhysicalTableNames.getValue()) {
                        checkFullyQualifiedPhysicalTableName(groupName + "." + physicalTableName, schemaName);
                    }
                }
            }
        }
    }

    private static void checkFullyQualifiedPhysicalTableName(String fullyQualifiedPhysicalTableName,
                                                             String schemaName) {
        fullyQualifiedPhysicalTableName = TStringUtil.remove(fullyQualifiedPhysicalTableName, '`').toLowerCase();

        Set<String> logicalTableNames = OptimizerContext.getContext(schemaName)
            .getRuleManager()
            .getLogicalTableNames(fullyQualifiedPhysicalTableName, schemaName);

        if (logicalTableNames != null && logicalTableNames.size() > 0) {
            // If multiple logical tables exists, we just report one to warn.
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "The physical table '" + fullyQualifiedPhysicalTableName
                    + "' already exists and is associated with logical table '"
                    + logicalTableNames.iterator().next() + "'.");
        }
    }

    // Check if multiple logical tables conflict on physical table names when
    // dropping a logical table.
    public static boolean skipPhysicalTable(RelNode relNode, AsyncDDLContext asyncDDLContext) {
        if (relNode instanceof PhyDdlTableOperation) {
            return skipPhysicalTable((PhyDdlTableOperation) relNode, asyncDDLContext.getSchemaName());
        }
        return false;
    }

    public static boolean skipPhysicalTable(PhyDdlTableOperation physicalPlan, String schemaName) {
        if (physicalPlan.getKind() != SqlKind.DROP_TABLE) {
            return false;
        }

        String fullyQualifiedPhysicalTableName = extractFullyQualifiedPhysicalTableName(physicalPlan);

        Set<String> logicalTableNames = OptimizerContext.getContext(schemaName).getRuleManager()
            .getLogicalTableNames(fullyQualifiedPhysicalTableName, schemaName);

        if (logicalTableNames != null && logicalTableNames.size() > 1) {
            StringBuilder sb = new StringBuilder();
            for (String logicalTableName : logicalTableNames) {
                sb.append(",").append(logicalTableName);
            }
            logger.warn("Skipped dropping the physical table '" + fullyQualifiedPhysicalTableName + "' since "
                + logicalTableNames.size() + " logical tables (" + sb.deleteCharAt(0).toString() + ") conflict on it.");
            return true;
        }

        return false;
    }

    private static String extractFullyQualifiedPhysicalTableName(PhyDdlTableOperation tableOperation) {
        Map<Integer, ParameterContext> params = tableOperation.getParam();

        String physicalTableName;
        if (params != null && params.size() > 0) {
            physicalTableName = (String) params.get(1).getValue();
        } else {
            physicalTableName = Util.last(Util.last(tableOperation.getTableNames()));
        }

        String fullyQualifiedPhysicalTableName = tableOperation.getDbIndex() + "." + physicalTableName;

        return TStringUtil.remove(fullyQualifiedPhysicalTableName, '`').toLowerCase();
    }

    /**
     * Generate ddl version id
     */
    public static long generateVersionId(ExecutionContext ec) {
        final ITimestampOracle timestampOracle =
            ec.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }
        return timestampOracle.nextTimestamp();
    }

    /**
     * 校验 tableMeta 中的 version 是否与 tables 系统表中的 version 一致，即检验元数据版本是否已刷新
     */
    public static void checkTableMetaVersion(ExecutionContext executionContext, String schemaName,
                                             String logicalTableName) {
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CHECK_TABLE_META_VERSION)) {
            return;
        }
        if (StringUtils.equalsIgnoreCase(logicalTableName, SystemTables.DUAL)) {
            return;
        }
        if (DbInfoManager.getInstance().isCdcDb(schemaName)) {
            return;
        }

        SyncManagerHelper.syncThrowExceptions(new CheckTableMetaVersionSyncAction(schemaName, logicalTableName),
            schemaName, SyncScope.ALL);
    }
}

package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.Reporter;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlPhysicalLockStatAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlPhysicalLockStatRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesInfoSchemaRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class InplaceBackfillUtils {
    final public static String QUERY_TABLES_EXTENSIONS =
        "select table_name, secondary_engine_attribute from information_schema.tables_extensions where table_schema='%s' and table_name in (%s)";

    final public static String SET_SECONDARY_ENGINE_ATTRIBUTE = "alter table ? secondary_engine_attribute = ?";
    final public static String POLARX_READONLY = "polarx.readonly";
    final public static String POLARX_READONLY_TRUE = "{\"polarx.readonly\":true}";
    final public static String POLARX_READONLY_FALSE = "{\"polarx.readonly\":false}";

    public static List<RelNode> buildReadOnlyPhysicalPlans(String schemaName, String tableName,
                                                           Map<String, Set<String>> sourceTableTopology,
                                                           Map<String, String> groupAndPhyDbMap,
                                                           boolean forRollback,
                                                           ExecutionContext executionContext) {
        List<RelNode> physicalPlans = new ArrayList<>();
        Map<String, Map<String, String>> sourceTableSecondaryEngineValues = new TreeMap<>(String::compareToIgnoreCase);
        for (Map.Entry<String, Set<String>> topology : sourceTableTopology.entrySet()) {
            String groupName = topology.getKey();
            String inCondition = topology.getValue().stream().map(s -> "'" + s.replace("\\", "\\\\")
                    .replace("'", "\\'") + "'")
                .collect(Collectors.joining(","));
            String phyDbName = groupAndPhyDbMap.get(groupName);
            String sql = String.format(QUERY_TABLES_EXTENSIONS, phyDbName, inCondition);
            List<List<Object>> result = StatsUtils.queryGroupByGroupName(schemaName, groupName, sql);
            Map<String, String> tableSecondaryEngineValues = sourceTableSecondaryEngineValues.computeIfAbsent(groupName,
                key -> new TreeMap<>(String::compareToIgnoreCase));
            for (List<Object> row : result) {
                String phyTableName = row.get(0).toString();
                String secondaryEngineAttribute = row.get(1) == null ? "" : row.get(1).toString();
                tableSecondaryEngineValues.put(phyTableName, secondaryEngineAttribute);
            }

            for (String phyTableName : topology.getValue()) {
                String secondaryEngineAttribute = tableSecondaryEngineValues.get(phyTableName);
                if (StringUtils.isEmpty(secondaryEngineAttribute)) {
                    if (forRollback) {
                        // already set readonly to false
                        continue;
                    }
                    secondaryEngineAttribute = POLARX_READONLY_TRUE;
                } else {
                    JSONObject obj = JSONObject.parseObject(secondaryEngineAttribute);
                    if (!forRollback && obj.containsKey(POLARX_READONLY) && obj.getBoolean(POLARX_READONLY)) {
                        // already set readonly to true
                        continue;
                    }
                    if (forRollback && obj.containsKey(POLARX_READONLY) && !obj.getBoolean(POLARX_READONLY)) {
                        // already set readonly to false
                        continue;
                    }
                    if (!forRollback) {
                        obj.put(POLARX_READONLY, true);
                    } else {
                        obj.remove(POLARX_READONLY);
                    }
                    secondaryEngineAttribute = obj.isEmpty() ? "" : obj.toJSONString();
                }
                PhyDdlTableOperation phyDdlTableOperation =
                    PhyDdlTableOperation.create(schemaName, tableName, executionContext);

                phyDdlTableOperation.setSchemaName(schemaName);
                phyDdlTableOperation.setDbIndex(groupName);

                phyDdlTableOperation.setLogicalTableName(tableName);

                phyDdlTableOperation.setTableNames(ImmutableList.of(ImmutableList.of(phyTableName)));

                phyDdlTableOperation.setKind(SqlKind.ALTER_TABLE);

                phyDdlTableOperation.setBytesSql(BytesSql.getBytesSql(SET_SECONDARY_ENGINE_ATTRIBUTE));
                HashMap<Integer, ParameterContext> params = new HashMap<>();

                params.put(1, PlannerUtils.buildParameterContextForTableName(phyTableName, 1));
                ParameterContext param2 = new ParameterContext(ParameterMethod.setString, new Object[] {
                    2, secondaryEngineAttribute
                });
                params.put(2, param2);
                phyDdlTableOperation.setParam(params);

                phyDdlTableOperation.setNativeSqlNode(null);

                phyDdlTableOperation.setDbType(DbType.MYSQL);

                phyDdlTableOperation.setTableRule(null);

                phyDdlTableOperation.setExplain(false);
                phyDdlTableOperation.setPartitioned(false);
                phyDdlTableOperation.setSequence(null);
                phyDdlTableOperation.setIfNotExists(false);

                physicalPlans.add(phyDdlTableOperation);
            }
        }
        return physicalPlans;
    }

    public static PartitionInfo buildNewPartitionInfo(String schemaName, String tableName,
                                                      ExecutionContext ec) {
        PartitionInfo newPartitionInfo = null;
        SchemaManager schemaManager = ec.getSchemaManager(schemaName);
        TableMeta logTblMeta = schemaManager.getTable(tableName);
        PartitionInfo curPartitionInfo = logTblMeta.getPartitionInfo();
        TddlRuleManager ruleMgr = schemaManager.getTddlRuleManager();
        newPartitionInfo =
            ruleMgr.getPartitionInfoManager().getPartitionInfoFromDeltaTable(tableName);

        try (Connection conn = MetaDbUtil.getConnection()) {
            PartitionInfoUtil.updatePartitionInfoByNewCommingPartitionRecords(conn,
                curPartitionInfo.getTableGroupId(), newPartitionInfo, logTblMeta);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        return newPartitionInfo;
    }

    public static Map<String, String> buildGroupAndPhyDbMap(String schemaName,
                                                            TreeMap<String, List<List<String>>> tableTopology) {
        Map<String, String> groupAndPhyDbMap = new TreeMap<>(String::compareToIgnoreCase);
        if (tableTopology == null || tableTopology.isEmpty()) {
            return groupAndPhyDbMap;
        }

        for (String groupName : tableTopology.keySet()) {
            String physicalDbName = GroupInfoUtil.buildPhysicalDbNameFromGroupName(schemaName, groupName);
            groupAndPhyDbMap.put(groupName, physicalDbName);
        }
        return groupAndPhyDbMap;
    }

    public static boolean supportInplaceBackfill(String schemaName, String tableName, PartitionStrategy strategy,
                                                 ExecutionContext ec) {
        boolean isSplitHashPartition = strategy.isHashed() || strategy.isCoHashed();
        boolean enableInplaceBackfill = ec.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_INPLACE_BACKFILL);
        final boolean useChangeSet = ChangeSetUtils.isChangeSetProcedure(ec);
        TableMeta tm = ec.getSchemaManager(schemaName).getTable(tableName);
        boolean hasPK = tm.isHasPrimaryKey();
        return isSplitHashPartition & enableInplaceBackfill & useChangeSet & hasPK;
    }

    public static void prepareBackfillTask(String schemaName,
                                           String logicalTableName,
                                           Map<String, Set<String>> srcTargetTableMap,
                                           Map<String, Set<String>> srcTargetPartitionMap,
                                           Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds,
                                           Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds,
                                           List<List<String>> activePartitionKeys,
                                           Integer hotKeyNum,
                                           boolean firstPartitionLevelActiveForInplaceBackfill,
                                           ExecutionContext ec) {

        if (GeneralUtil.isEmpty(targetTablePartitionBounds) || GeneralUtil.isEmpty(activePartitionKeys)
            || GeneralUtil.isEmpty(srcTargetTableMap)) {
            assert targetTablePartitionBounds != null;
            assert activePartitionKeys != null;
            assert srcTargetTableMap != null;
            SchemaManager sm = ec.getSchemaManager(schemaName);
            TableMeta tableMeta = sm.getTable(logicalTableName);
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            PartitionByDefinition srcPartitionBy = partitionInfo.getPartitionBy();
            PartitionInfo newPartitionInfo =
                InplaceBackfillUtils.buildNewPartitionInfo(schemaName, logicalTableName, ec);
            PartitionByDefinition targetPartitionBy = newPartitionInfo.getPartitionBy();

            List<String> actualPartCols = new ArrayList<>();
            PartitionStrategy srcPartitionStrategy;
            boolean hasSubPartition = targetPartitionBy.getSubPartitionBy() != null;

            List<String> allPartitionCols;
            if (firstPartitionLevelActiveForInplaceBackfill) {
                actualPartCols.addAll(srcPartitionBy.getAllLevelActualPartCols().get(0));
                srcPartitionStrategy = srcPartitionBy.getStrategy();
                allPartitionCols = srcPartitionBy.getPartitionColumnNameList();
            } else {
                if (srcPartitionBy.getSubPartitionBy() == null) {
                    throw new IllegalArgumentException("SubPartitionBy is null when expected.");
                }
                actualPartCols.addAll(srcPartitionBy.getAllLevelActualPartCols().get(1));
                srcPartitionStrategy = srcPartitionBy.getSubPartitionBy().getStrategy();
                allPartitionCols = srcPartitionBy.getSubPartitionBy().getPartitionColumnNameList();
            }

            if (srcPartitionStrategy.isKey()
                && actualPartCols.size() < allPartitionCols.size()) {
                if (hotKeyNum > 0) {
                    //the active partition columns count is hotKeyNum+1
                    for (int i = actualPartCols.size(); i <= hotKeyNum && i < allPartitionCols.size(); i++) {
                        actualPartCols.add(allPartitionCols.get(i));
                    }
                } else {
                    //for less than(xxx,Long.MAX_VALUE) we should consider the first "Long.MAX_VALUE" column as the last active partition column
                    String theFirstDisActivePartCol =
                        allPartitionCols.get(actualPartCols.size());
                    actualPartCols.add(theFirstDisActivePartCol);
                }
            }
            if (actualPartCols.size() > 1) {
                if (srcPartitionStrategy.isCoHashed()
                    || srcPartitionStrategy.isHashed() && !srcPartitionStrategy.isKey()) {
                    activePartitionKeys.add(actualPartCols);
                } else {
                    for (String col : actualPartCols) {
                        activePartitionKeys.add(Collections.singletonList(col));
                    }
                }
            } else {
                activePartitionKeys.add(actualPartCols);
            }

            for (Map.Entry<String, Set<String>> entry : srcTargetPartitionMap.entrySet()) {
                PartitionSpec partitionSpec = srcPartitionBy.getPhysicalPartitionByPartName(entry.getKey());
                String srcPhyTableName = partitionSpec.getLocation().getPhyTableName();
                srcTargetTableMap.put(srcPhyTableName, new TreeSet<>(String::compareToIgnoreCase));

                boolean usePhysicalPartitionBound =
                    !firstPartitionLevelActiveForInplaceBackfill || !hasSubPartition;

                processPartitionBounds(srcPartitionBy, partitionSpec, srcPhyTableName,
                    usePhysicalPartitionBound, hasSubPartition, activePartitionKeys, sourceTablePartitionBounds);

                for (String targetPartitionName : entry.getValue()) {

                    PartitionSpec targetPartitionSpec =
                        targetPartitionBy.getPhysicalPartitionByPartName(targetPartitionName);
                    String targetPhyTableName = targetPartitionSpec.getLocation().getPhyTableName();
                    srcTargetTableMap.get(srcPhyTableName).add(targetPhyTableName);

                    processPartitionBounds(targetPartitionBy, targetPartitionSpec, targetPhyTableName,
                        usePhysicalPartitionBound, hasSubPartition, activePartitionKeys, targetTablePartitionBounds);
                }
            }
        }
    }

    public static void processPartitionBounds(PartitionByDefinition targetPartitionBy,
                                              PartitionSpec targetPartitionSpec,
                                              String targetPhyTableName,
                                              boolean usePhysicalPartitionBound,
                                              boolean hasSubPartition,
                                              List<List<String>> activePartitionKeys,
                                              Map<String, List<Pair<Long, Long>>> partitionBounds) {
        PartitionSpec leftPartSpec = null;

        if (usePhysicalPartitionBound) {
            PartitionBoundSpec partitionBoundSpec = targetPartitionSpec.getBoundSpec();
            int partKeyCnt;
            if (hasSubPartition) {
                partKeyCnt = targetPartitionBy.getAllLevelFullPartColumnCounts().get(1);
            } else {
                partKeyCnt = targetPartitionBy.getAllLevelFullPartColumnCounts().get(0);
            }
            int position = targetPartitionSpec.getPosition().intValue();
            if (position < 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Invalid partition position: " + position);
            }

            if (position != 1) {
                if (hasSubPartition) {
                    Long parentPartPosi = targetPartitionSpec.getParentPartPosi();
                    PartitionSpec parentPartSpec =
                        targetPartitionBy.getNthPartition(parentPartPosi.intValue());
                    leftPartSpec =
                        parentPartSpec.getNthSubPartition(targetPartitionSpec.getPosition().intValue() - 1);
                } else {
                    leftPartSpec =
                        targetPartitionBy.getNthPartition(targetPartitionSpec.getPosition().intValue() - 1);
                }
            }

            if (partitionBoundSpec.isSingleValue()) {
                boolean isHash =
                    partitionBoundSpec.getStrategy().isHashed() && !partitionBoundSpec.getStrategy().isKey();
                boolean isKey = partitionBoundSpec.getStrategy().isKey();
                SearchDatumInfo datum = partitionBoundSpec.getSingleDatum();
                PartitionBoundVal[] bndVal = datum.getDatumInfo();
                List<Pair<Long, Long>> bndValPairs = new ArrayList<>();
                int hashSpaceSize = isHash ? 1 : activePartitionKeys.size();
                for (int i = 0; i < hashSpaceSize; i++) {
                    PartitionField upperField = bndVal[i].getValue();
                    Long upperVal = upperField.longValue();
                    /*
                    if (isKey && partKeyCnt > 1 && i < partKeyCnt - 1) {
                        upperVal = upperField.longValue();
                    } else {
                        upperVal = upperField.longValue() - 1;
                    }*/
                    if (position == 1) {
                        Pair<Long, Long> bndValPair =
                            new Pair<>(PartitionBoundVal.HASH_BOUND_MIN_VAL_LONG, upperVal);
                        bndValPairs.add(bndValPair);
                    } else {
                        PartitionBoundSpec leftSubPartBoundSpec = leftPartSpec.getBoundSpec();
                        SearchDatumInfo leftDatum = leftSubPartBoundSpec.getSingleDatum();
                        PartitionBoundVal[] lftBndVal = leftDatum.getDatumInfo();
                        PartitionBoundVal leftSubPartBndVal = lftBndVal[i];
                        PartitionField lowerField = leftSubPartBndVal.getValue();
                        // 防止lowerField > upperField, 例如分裂p2的场景
                        // PARTITION `p1` VALUES LESS THAN (5634770598966349862,0) ENGINE = InnoDB,
                        // PARTITION `p2` VALUES LESS THAN (5634770598966349862,9223372036854775807) ENGINE = InnoDB
                        Long lowerVal = lowerField.longValue();
                        /*
                        if (isKey && partKeyCnt > 1 && i < partKeyCnt - 1) {
                            lowerVal = lowerField.longValue();
                        } else {
                            lowerVal = Math.min(lowerField.longValue(), upperVal);
                        }*/
                        Pair<Long, Long> bndValPair =
                            new Pair<>(lowerVal, upperVal);
                        bndValPairs.add(bndValPair);
                    }
                }
                partitionBounds.put(targetPhyTableName, bndValPairs);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Unsupported partition bound spec");
            }
        } else {
            int partKeyCnt = targetPartitionBy.getAllLevelFullPartColumnCounts().get(0);
            Long parentPartPosi = targetPartitionSpec.getParentPartPosi();
            PartitionSpec parentPartSpec =
                targetPartitionBy.getNthPartition(parentPartPosi.intValue());
            PartitionBoundSpec partitionBoundSpec = parentPartSpec.getBoundSpec();
            int position = parentPartSpec.getPosition().intValue();
            if (position < 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "Invalid parent partition position: " + position);
            }
            boolean isKey = partitionBoundSpec.getStrategy().isKey();
            if (position != 1) {
                leftPartSpec =
                    targetPartitionBy.getNthPartition(parentPartSpec.getPosition().intValue() - 1);
            }

            if (partitionBoundSpec.isSingleValue()) {
                SearchDatumInfo datum = partitionBoundSpec.getSingleDatum();
                PartitionBoundVal[] bndVal = datum.getDatumInfo();
                boolean isHash =
                    partitionBoundSpec.getStrategy().isHashed() && !partitionBoundSpec.getStrategy().isKey();
                int hashSpaceSize = isHash ? 1 : activePartitionKeys.size();
                List<Pair<Long, Long>> bndValPairs = new ArrayList<>();
                for (int i = 0; i < hashSpaceSize; i++) {
                    PartitionField upperField = bndVal[i].getValue();
                    Long upperVal = upperField.longValue();
                    /*
                    if (isKey && partKeyCnt > 1 && i < partKeyCnt - 1) {
                        upperVal = upperField.longValue();
                    } else {
                        upperVal = upperField.longValue() - 1;
                    }*/
                    if (position == 1) {
                        Pair<Long, Long> bndValPair =
                            new Pair<>(PartitionBoundVal.HASH_BOUND_MIN_VAL_LONG, upperField.longValue() - 1);
                        bndValPairs.add(bndValPair);
                    } else {
                        PartitionBoundSpec leftSubPartBoundSpec = leftPartSpec.getBoundSpec();
                        SearchDatumInfo leftDatum = leftSubPartBoundSpec.getSingleDatum();
                        PartitionBoundVal[] leftDatumDatumInfo = leftDatum.getDatumInfo();
                        PartitionBoundVal leftSubPartBndVal = leftDatumDatumInfo[i];
                        PartitionField lowerField = leftSubPartBndVal.getValue();
                        // 防止lowerField > upperField, 例如分裂p2的场景
                        // PARTITION `p1` VALUES LESS THAN (5634770598966349862,0) ENGINE = InnoDB,
                        // PARTITION `p2` VALUES LESS THAN (5634770598966349862,9223372036854775807) ENGINE = InnoDB
                        Long lowerVal = lowerField.longValue();
                        /*if (isKey && partKeyCnt > 1 && i < partKeyCnt - 1) {
                            lowerVal = lowerField.longValue();
                        } else {
                            lowerVal = Math.min(lowerField.longValue(), upperVal);
                        }*/
                        Pair<Long, Long> bndValPair =
                            new Pair<>(lowerVal, upperVal);
                        bndValPairs.add(bndValPair);
                    }
                }
                partitionBounds.put(targetPhyTableName, bndValPairs);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Unsupported partition bound spec");
            }
        }
    }

    public static List<String> getOrderPrimaryKeyColumns(OmcStorageInfo omcStorageInfo, String phyTableName) {
        List<IndexesInfoSchemaRecord> primaryKeyInfo = OmcUtils.getPrimaryKeyInfo(omcStorageInfo, phyTableName);
        List<String> primaryKeyColumns = new ArrayList<>();
        if (primaryKeyInfo == null || primaryKeyInfo.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Do not support split partition when table has no primary key");
        }
        for (IndexesInfoSchemaRecord record : primaryKeyInfo) {
            String columnName = record.columnName;
            primaryKeyColumns.add(columnName);
        }
        return primaryKeyColumns;
    }

    public static List<String> getAllColumns(OmcStorageInfo omcStorageInfo, String phyTableName) {
        List<ColumnsInfoSchemaRecord> columnsInfo = OmcUtils.getColumnsInfo(omcStorageInfo, phyTableName);
        List<String> tableColumns = new ArrayList<>();
        for (ColumnsInfoSchemaRecord record : columnsInfo) {
            if (StringUtils.containsIgnoreCase(record.extra, "VIRTUAL GENERATED")) {
                continue;
            }
            if (StringUtils.containsIgnoreCase(record.extra, "STORED GENERATED")) {
                continue;
            }
            // prepare backfill columns
            tableColumns.add(record.columnName);
        }
        return tableColumns;
    }

    public static void initOrUpdateDdlPhysicalLockInfo(String schemaName, String tableName, long jobId,
                                                       Long backfillTaskId,
                                                       Map<String, Set<String>> sourceTableTopology,
                                                       Map<String, String> groupAndPhyDbMap) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DdlPhysicalLockStatAccessor accessor = new DdlPhysicalLockStatAccessor();
            accessor.setConnection(metaDbConn);
            long currentTimest = System.currentTimeMillis();
            List<DdlPhysicalLockStatRecord> records = accessor.queryByJobIdAndTable(jobId, schemaName, tableName);
            if (GeneralUtil.isNotEmpty(records)) {
                if (records.get(0).getState() == DdlPhysicalLockStatRecord.STATE_UNLOCKED) {
                    accessor.updateCurLockStartTimeState(jobId, currentTimest, DdlPhysicalLockStatRecord.STATE_LOCKED);
                }
            } else {
                Map<String, Long> rowCounts = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                GsiBackfillManager backfillManager = new GsiBackfillManager(schemaName);
                GsiBackfillManager.BackfillBean backfillBean = backfillManager.loadBackfillMeta(backfillTaskId);
                if (!backfillBean.isEmpty()) {
                    int columIndex = -1;
                    for (GsiBackfillManager.BackfillObjectBean bfo : backfillBean.backfillObjects.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList())) {
                        if (columIndex == -1) {
                            columIndex = (int) bfo.columnIndex;
                        } else if (columIndex != bfo.columnIndex) {
                            continue;
                        }
                        String phyTableName = bfo.physicalTable;
                        if (!rowCounts.containsKey(phyTableName)) {
                            rowCounts.put(phyTableName, 0L);
                        }
                        rowCounts.put(phyTableName, rowCounts.get(phyTableName) + bfo.successRowCount);
                    }
                }
                for (Map.Entry<String, Set<String>> entry : sourceTableTopology.entrySet()) {
                    String phyDb = groupAndPhyDbMap.get(entry.getKey());
                    for (String phyTableName : entry.getValue()) {
                        DdlPhysicalLockStatRecord record = new DdlPhysicalLockStatRecord();
                        record.setJobId(jobId);
                        record.setTableSchema(schemaName);
                        record.setTableName(tableName);
                        record.setPhysicalTable(phyTableName);
                        record.setPhysicalDb(phyDb);
                        record.setStartTime(currentTimest);
                        record.setLockDurationMs(0L);
                        record.setCurLockStartTime(currentTimest);
                        record.setRowCount(rowCounts.get(phyTableName) != null ? rowCounts.get(phyTableName) : 0);
                        record.setDdlType(DdlPhysicalLockStatRecord.DDL_TYPE_SPLIT_PARTITION);
                        record.setState(DdlPhysicalLockStatRecord.STATE_LOCKED);
                        records.add(record);
                    }
                }
                accessor.insert(records);
            }
        } catch (Exception ex) {
            String msg =
                String.format(
                    "initOrUpdateDdlPhysicalLockInfo error, schema:%s, tableName:%s, jobId:%d, backfillId:%d, errorMsg:",
                    schemaName, tableName, jobId, backfillTaskId, ex.getMessage());
            SQLRecorderLogger.ddlLogger.error(msg);
        }
    }

    public static void updateDdlPhysicalLockInfo(String schemaName, String tableName, long jobId, Connection conn) {
        if (conn != null) {
            // Use the provided connection directly
            try {
                DdlPhysicalLockStatAccessor accessor = new DdlPhysicalLockStatAccessor();
                accessor.setConnection(conn);
                long currentTimest = System.currentTimeMillis();
                List<DdlPhysicalLockStatRecord> records = accessor.queryByJobIdAndTable(jobId, schemaName, tableName);
                if (GeneralUtil.isNotEmpty(records)) {
                    if (records.get(0).getState() == DdlPhysicalLockStatRecord.STATE_LOCKED) {
                        Long lockDurationMs = (currentTimest - records.get(0).getCurLockStartTime());
                        accessor.incrementLockDuration(jobId, schemaName, tableName, lockDurationMs, currentTimest,
                            DdlPhysicalLockStatRecord.STATE_UNLOCKED);
                    }
                }
            } catch (Exception ex) {
                String msg =
                    String.format("updateDdlPhysicalLockInfo error, schema:%s, tableName:%s, jobId:%d, errorMsg:",
                        schemaName, tableName, jobId, ex.getMessage());
                SQLRecorderLogger.ddlLogger.info(msg);
            }
        } else {
            // Create a new connection if conn is null
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                DdlPhysicalLockStatAccessor accessor = new DdlPhysicalLockStatAccessor();
                accessor.setConnection(metaDbConn);
                long currentTimest = System.currentTimeMillis();
                List<DdlPhysicalLockStatRecord> records = accessor.queryByJobIdAndTable(jobId, schemaName, tableName);
                if (GeneralUtil.isNotEmpty(records)) {
                    if (records.get(0).getState() == DdlPhysicalLockStatRecord.STATE_LOCKED) {
                        Long lockDurationMs = (currentTimest - records.get(0).getCurLockStartTime());
                        accessor.incrementLockDuration(jobId, schemaName, tableName, lockDurationMs, currentTimest,
                            DdlPhysicalLockStatRecord.STATE_UNLOCKED);
                    }
                }
            } catch (Exception ex) {
                String msg =
                    String.format("updateDdlPhysicalLockInfo error, schema:%s, tableName:%s, jobId:%d, errorMsg:",
                        schemaName, tableName, jobId, ex.getMessage());
                SQLRecorderLogger.ddlLogger.info(msg);
            }
        }
    }
}

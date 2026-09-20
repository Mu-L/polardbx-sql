package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableExchangePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class LogicalAlterTableExchangePartition extends BaseDdlOperation {

    protected AlterTableExchangePartitionPreparedData preparedData;

    public LogicalAlterTableExchangePartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec) {
        if (preparedData != null) {
            return;
        }
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPartitionDb) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Not support exchange partition for DRDS mode table");
        }
        SqlAlterTableExchangePartition sqlAlterTableExchangePartition =
            (SqlAlterTableExchangePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        String targetTableName = Util.last(((SqlIdentifier) sqlAlterTableExchangePartition.getTableName()).names);

        TableMeta srcTableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        PartitionInfo srcPartitionInfo = srcTableMeta.getPartitionInfo();
        TableMeta targetTableMeta = ec.getSchemaManager(schemaName).getTable(targetTableName);
        PartitionInfo targetPartitionInfo = targetTableMeta.getPartitionInfo();

        preparedData = new AlterTableExchangePartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setSourceTableName(logicalTableName);
        preparedData.setTargetTableName(targetTableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setSrcPartIsSubPartition(sqlAlterTableExchangePartition.isSubPartition());
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setValidate(sqlAlterTableExchangePartition.isValidation());
        List<String> srcPartitionNames = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(sqlAlterTableExchangePartition.getSrcPartitions())) {
            Set<String> partSet = new TreeSet<>(String::compareToIgnoreCase);
            for (SqlNode sqlNode : sqlAlterTableExchangePartition.getSrcPartitions()) {
                String srcPartName = Util.last(((SqlIdentifier) sqlNode).names);
                if (partSet.contains(srcPartName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "duplicate partition name " + srcPartName);
                } else {
                    partSet.add(srcPartName);
                    PartitionSpec partitionSpec = srcPartitionInfo.getPartitionBy().getPartitionByPartName(srcPartName);
                    if (partitionSpec == null) {
                        partitionSpec = srcPartitionInfo.getPartitionBy().getPhysicalPartitionByPartName(srcPartName);
                    }
                    if (partitionSpec == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "partition " + srcPartName + " is not exist");
                    }
                    if (partitionSpec.isLogical()) {
                        List<PartitionSpec> subPartitionSpecs = partitionSpec.getSubPartitions();
                        for (int i = 0; i < subPartitionSpecs.size(); i++) {
                            srcPartitionNames.add(subPartitionSpecs.get(i).getName());
                        }
                    } else {
                        srcPartitionNames.add(srcPartName);
                    }
                }
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT);
        }
        preparedData.setSourcePartitionNames(srcPartitionNames);
        List<String> tarPartitionNames = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(sqlAlterTableExchangePartition.getTargetPartitions())) {
            Set<String> partSet = new TreeSet<>(String::compareToIgnoreCase);
            // todo only single table is allowed now
            if (!targetPartitionInfo.isGsiSingleOrSingleTable()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "target table should be single table");
            }
            for (SqlNode sqlNode : sqlAlterTableExchangePartition.getTargetPartitions()) {
                String tarPartName = Util.last(((SqlIdentifier) sqlNode).names);
                if (partSet.contains(tarPartName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "duplicate partition name " + tarPartName);
                } else {
                    partSet.add(tarPartName);
                    PartitionSpec partitionSpec =
                        targetPartitionInfo.getPartitionBy().getPartitionByPartName(tarPartName);
                    if (partitionSpec == null) {
                        partitionSpec =
                            targetPartitionInfo.getPartitionBy().getPhysicalPartitionByPartName(tarPartName);
                    }
                    if (partitionSpec.isLogical()) {
                        List<PartitionSpec> subPartitionSpecs = partitionSpec.getSubPartitions();
                        for (int i = 0; i < subPartitionSpecs.size(); i++) {
                            tarPartitionNames.add(subPartitionSpecs.get(i).getName());
                        }
                    } else {
                        tarPartitionNames.add(tarPartName);
                    }
                }
            }
            if (srcPartitionNames.size() != tarPartitionNames.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "src and target partition size not equal");
            }
        } else {
            if (!targetPartitionInfo.isSingleTable()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "target table should be single table");
            }
            tarPartitionNames.add(targetPartitionInfo.getPartitionBy().getPhysicalPartitions().get(0).getName());
        }
        preparedData.setTargetPartitionNames(tarPartitionNames);
        OptimizerContext optimizerContext =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        TableGroupConfig srcTableGroupConfig =
            optimizerContext.getTableGroupInfoManager().getTableGroupConfigById(srcPartitionInfo.getTableGroupId());
        TableGroupConfig tarTableGroupConfig =
            optimizerContext.getTableGroupInfoManager().getTableGroupConfigById(targetPartitionInfo.getTableGroupId());

        String srcTableGroupName = srcTableGroupConfig.getTableGroupRecord().getTg_name();
        String tarTableGroupName = tarTableGroupConfig.getTableGroupRecord().getTg_name();
        preparedData.setExclusiveSrcTableGroup(srcTableGroupConfig.getTables().size() == 1);
        preparedData.setExclusiveTargetTableGroup(tarTableGroupConfig.getTables().size() == 1);
        preparedData.setSrcTableGroupName(srcTableGroupName);
        preparedData.setTargetTableGroupName(tarTableGroupName);
        preparedData.setSrcTableVersion(srcTableMeta.getVersion());
        preparedData.setTargetTableVersion(targetTableMeta.getVersion());

    }

    public AlterTableExchangePartitionPreparedData getPreparedData(ExecutionContext ec) {
        if (preparedData == null) {
            preparedData(ec);
        }
        return preparedData;
    }

    public static LogicalAlterTableExchangePartition create(DDL ddl) {
        return new LogicalAlterTableExchangePartition(ddl);
    }

}

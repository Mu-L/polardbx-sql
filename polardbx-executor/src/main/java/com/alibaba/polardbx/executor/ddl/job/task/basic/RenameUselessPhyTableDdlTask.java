package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.builder.RenamePhyTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoAccessor;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Getter
@TaskName(name = "RenameUselessPhyTableDdlTask")
public class RenameUselessPhyTableDdlTask extends BasePhyDdlTask {

    //key:groupKey,value:<oldTableName,newTableName> caseinsensitive map
    TreeMap<String, HashMap<String, String>> srcTableTopology;
    //key:groupKey, value:storageId caseinsensitive map
    TreeMap<String, String> groupAndStorageInMap;

    @JSONCreator
    public RenameUselessPhyTableDdlTask(String schemaName, PhysicalPlanData physicalPlanData,
                                        TreeMap<String, String> groupAndStorageInMap,
                                        TreeMap<String, HashMap<String, String>> srcTableTopology) {
        super(schemaName, physicalPlanData);
        this.groupAndStorageInMap = groupAndStorageInMap;
        this.srcTableTopology = srcTableTopology;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        createRecycleBinDbIfNotExists();
        insertRecycleBinMeta();
        super.executeImpl(executionContext);
        updateRecycleBinMeta();
    }

    protected List<RelNode> getPhysicalPlans(ExecutionContext ec) {
        return renamePhyTable(DdlJobDataConverter.convertToPhysicalPlans(physicalPlanData, ec), ec);
    }

    private List<RelNode> renamePhyTable(List<RelNode> inputs, ExecutionContext ec) {
        for (RelNode rel : GeneralUtil.emptyIfNull(inputs)) {
            PhyDdlTableOperation phyDdlTableOperation = (PhyDdlTableOperation) rel;
            String srcTableName = phyDdlTableOperation.getTableNames().get(0).get(0);
            phyDdlTableOperation.setBytesSql(RenamePhyTableBuilder.templateBytesSql);
            Map<String, String> phyTableNamesMap = srcTableTopology.get(phyDdlTableOperation.getDbIndex());

            ((PhyDdlTableOperation) rel).getParam().put(2, new ParameterContext(
                ParameterMethod.setTableName,
                new Object[] {2, PhyRecycleBinInfoRecord.PHY_DB_NAME}));
            ((PhyDdlTableOperation) rel).getParam().put(3, new ParameterContext(
                ParameterMethod.setTableName,
                new Object[] {3, phyTableNamesMap.get(srcTableName)}));
        }
        return inputs;
    }

    private void insertRecycleBinMeta() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            PhyRecycleBinInfoAccessor recycleBinInfoAccessor = new PhyRecycleBinInfoAccessor();
            recycleBinInfoAccessor.setConnection(conn);
            List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
            for (Map.Entry<String, HashMap<String, String>> entry : srcTableTopology.entrySet()) {
                for (Map.Entry<String, String> item : entry.getValue().entrySet()) {
                    PhyRecycleBinInfoRecord record = new PhyRecycleBinInfoRecord();
                    record.setJobId(this.getTaskId());
                    record.setStorageInstId(groupAndStorageInMap.get(entry.getKey()));
                    record.setCurDbName(PhyRecycleBinInfoRecord.PHY_DB_NAME);
                    record.setCurTbName(item.getValue());
                    record.setOriginDbName(GroupInfoUtil.buildPhysicalDbNameFromGroupName(schemaName, entry.getKey()));
                    record.setOriginTbName(item.getKey());
                    record.setStatus(PhyRecycleBinInfoRecord.STATUS_INIT);
                    record.setType(PhyRecycleBinInfoRecord.NORM_DLL_TYPE);
                    records.add(record);
                }
            }
            recycleBinInfoAccessor.insert(records);
            conn.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                " fail to add meta into " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, ex);
        }

    }

    private void updateRecycleBinMeta() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            PhyRecycleBinInfoAccessor recycleBinInfoAccessor = new PhyRecycleBinInfoAccessor();
            recycleBinInfoAccessor.setConnection(conn);
            List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
            for (Map.Entry<String, HashMap<String, String>> entry : srcTableTopology.entrySet()) {
                for (Map.Entry<String, String> item : entry.getValue().entrySet()) {
                    recycleBinInfoAccessor.updateByStorageAndTb(groupAndStorageInMap.get(entry.getKey()),
                        item.getValue(), PhyRecycleBinInfoRecord.STATUS_RENAME);
                }
            }
            recycleBinInfoAccessor.insert(records);
            conn.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                " fail to add meta into " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, ex);
        }

    }

    private void createRecycleBinDbIfNotExists() {
        Set<String> storageIds = new HashSet<>();
        for (Map.Entry<String, String> entry : groupAndStorageInMap.entrySet()) {
            if (storageIds.contains(entry.getValue())) {
                continue;
            }
            storageIds.add(entry.getValue());
            if (ScaleOutUtils.checkPhyDbExistence(schemaName, entry.getKey(), PhyRecycleBinInfoRecord.PHY_DB_NAME)) {
                continue;
            }
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.putIfAbsent(PhyRecycleBinInfoRecord.GROUP_NAME,
                PhyRecycleBinInfoRecord.PHY_DB_NAME);
            try {
                DbTopologyManager.createPhysicalDbInStorageInst(null, null, entry.getValue(),
                    grpPhyDbMap, "createRecycleBinDbIfNotExists");
            } catch (SQLException ex) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " fail to create recycle bin database in:" + entry.getValue(), ex);
            }
        }
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, HashMap<String, String>> entry : srcTableTopology.entrySet()) {
            for (Map.Entry<String, String> item : entry.getValue().entrySet()) {
                sb.append(item.getKey() + " rename to " + item.getValue());
                sb.append(",");
            }
        }
        return "|logicalTableName: " + physicalPlanData.getLogicalTableName() + ":[ " + sb + " ]";
    }

}
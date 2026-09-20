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
import java.util.*;

@Getter
@TaskName(name = "DropFkPhyTableDdlTask")
public class DropFkPhyTableDdlTask extends BasePhyDdlTask {
    @JSONCreator
    public DropFkPhyTableDdlTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        super.executeImpl(executionContext);
    }

    @Override
    public String remark() {
        return "|logicalTableName: " + physicalPlanData.getLogicalTableName() + " ,drop foreign key";
    }

}
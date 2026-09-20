package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.spi.IDataSourceGetter;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.spi.DatasourceMySQLImplement;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

@Getter
@TaskName(name = "CheckPhyTableTask")
public class CheckPhyTableTask extends AnalyzeTablePhyDdlTask {
    public final static String TASK_NAME = "CheckPhyTableTask";
    public final static String TABLE_COLUMN = "TABLE";
    public final static  String OP_COLUMN = "OP";
    public final static String MSG_TYPE = "MSG_TYPE";
    public final static String MSG_TEXT = "MSG_TEXT";

    private String schemaName;
    private String groupKey;
    private String phyTableName;
    private Map<String, String> checkResult;



    public CheckPhyTableTask(String schemaName, String groupKey, String phyTableName, Map<String, String> checkResult) {
        super(ImmutableList.of(schemaName), ImmutableList.of(phyTableName), null, null);
        this.schemaName = schemaName;
        this.groupKey = groupKey;
        this.phyTableName = phyTableName;
        this.checkResult = checkResult;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        IDataSourceGetter mysqlDsGetter = new DatasourceMySQLImplement(schemaName);
        try {
            checkResult = doCheckOnePhysicalTable(groupKey, phyTableName, mysqlDsGetter, executionContext.getTraceId(),
                this.getJobId());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

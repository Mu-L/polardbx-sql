package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class RenamePhyTableBuilder extends DdlPhyPlanBuilder {

    public static BytesSql templateBytesSql = BytesSql.getBytesSql("rename table ? to ?.?");
    private final DdlPreparedData preparedData;

    public RenamePhyTableBuilder(DDL ddl, DdlPreparedData preparedData,
                                 TreeMap<String, List<List<String>>> tableTopology, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
        this.tableTopology = tableTopology;
    }

    public static RenamePhyTableBuilder createBuilder(String schemaName,
                                                      String logicalTableName,
                                                      TreeMap<String, List<List<String>>> tableTopology,
                                                      ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        List<String> newName = new ArrayList<>(2);
        newName.add(schemaName);
        newName.add(logicalTableName);
        SqlIdentifier to = new SqlIdentifier(newName, SqlParserPos.ZERO);
        SqlIdentifier from = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
        //a to a.a只起占位作用，真是表名会在后续替换
        SqlRenameTable sqlRenameTable =
                SqlDdlNodes.renameTable(to, from, "rename table a to a.a", SqlParserPos.ZERO);

        sqlRenameTable = (SqlRenameTable) sqlRenameTable.accept(visitor);

        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        RenameTable renameTable = RenameTable.create(cluster, sqlRenameTable, from, to);

        //LogicalRenameTable logicalRenameTable = LogicalRenameTable.create(renameTable);
        //logicalRenameTable.setSchemaName(schemaName);
        DdlPreparedData preparedData = new DdlPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);

        return new RenamePhyTableBuilder(renameTable, preparedData, tableTopology,
                executionContext);
    }

    @Override
    protected void buildTableRuleAndTopology() {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        if (!isNewPartDb) {
            buildExistingTableRule(preparedData.getTableName());
            //buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
        } else {
            if (partitionInfo == null) {
                partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(ddlPreparedData.getTableName());
            }
            //this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(partitionInfo);
        }
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

}

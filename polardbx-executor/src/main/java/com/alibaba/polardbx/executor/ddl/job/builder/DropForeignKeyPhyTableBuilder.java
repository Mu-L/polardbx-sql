package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class DropForeignKeyPhyTableBuilder extends DdlPhyPlanBuilder {

    public static BytesSql templateBytesSql = BytesSql.getBytesSql("alter table ? drop foreign key ?");
    private final DdlPreparedData preparedData;

    public DropForeignKeyPhyTableBuilder(DDL ddl, DdlPreparedData preparedData,
                                         TreeMap<String, List<List<String>>> tableTopology, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
        this.tableTopology = tableTopology;
    }

    public static DropForeignKeyPhyTableBuilder createBuilder(String schemaName,
                                                              String logicalTableName,
                                                              String foreignKeyName,
                                                              TreeMap<String, List<List<String>>> tableTopology,
                                                              ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SQLAlterTableStatement alterTableStmt =
                (SQLAlterTableStatement) FastsqlUtils.parseSql(String.format("alter table `%s` drop foreign key `%s`", logicalTableName, foreignKeyName)).get(0);
        FastSqlToCalciteNodeVisitor fastSqlToCalciteNodeVisitor =
                new FastSqlToCalciteNodeVisitor(new ContextParameters(false), executionContext);
        alterTableStmt.accept(fastSqlToCalciteNodeVisitor);
        SqlAlterTable sqlAlterTable = (SqlAlterTable) fastSqlToCalciteNodeVisitor.getSqlNode();
        sqlAlterTable = (SqlAlterTable) sqlAlterTable.accept(visitor);

        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        AlterTable alterTable = AlterTable.create(cluster, sqlAlterTable, new SqlIdentifier(logicalTableName, SqlParserPos.ZERO), null);

        DdlPreparedData preparedData = new DdlPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);

        return new DropForeignKeyPhyTableBuilder(alterTable, preparedData, tableTopology,
                executionContext);
    }

    @Override
    protected void buildTableRuleAndTopology() {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        if (!isNewPartDb) {
            buildExistingTableRule(preparedData.getTableName());
        } else {
            if (partitionInfo == null) {
                partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(ddlPreparedData.getTableName());
            }
        }
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

}

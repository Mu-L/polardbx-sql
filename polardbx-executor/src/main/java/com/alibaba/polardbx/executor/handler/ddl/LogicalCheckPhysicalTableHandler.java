package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.builder.DirectPhysicalSqlPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AnalyzeTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CheckPhyTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.OptimizeTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCheckTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOptimizeTable;
import org.apache.calcite.sql.SqlOptimizeTableDdl;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * CHECK PHYSICAL TABLE
 *
 * @author yijin
 * @since 2024/10
 */
public class LogicalCheckPhysicalTableHandler extends LogicalCommonDdlHandler {

    private static final String CHECK_TABLE_DDL_TEMPLATE = "check table ?";

    public LogicalCheckPhysicalTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext ec) {
        SqlOptimizeTableDdl sqlCheckTable = (SqlOptimizeTableDdl) logicalDdlPlan.getNativeSqlNode();
        List<Pair<String, String>> tableNameList =
            extractTableList(sqlCheckTable.getTableNames(), ec.getSchemaName(), ec);
        final int parallelism = ec.getParamManager().getInt(ConnectionParams.OPTIMIZE_TABLE_PARALLELISM);
        if (parallelism < 1 || parallelism > 4096) {
            throw new TddlNestableRuntimeException("CHECK_TABLE_PARALLESIM must in range 0-4096");
        }

        ExecutableDdlJob result = new ExecutableDdlJob();

        EmptyTask emptyTask = new EmptyTask(ec.getSchemaName());
        EmptyTask emptyTask2 = new EmptyTask(ec.getSchemaName());
        for (Pair<String, String> targetTable : tableNameList) {
            List<List<DdlTask>> concurrentCheckPhyTableTaskList =
                genCheckPhyTableTaskList(logicalDdlPlan.relDdl, targetTable.getKey(), targetTable.getValue(),
                    CHECK_TABLE_DDL_TEMPLATE, ec);
            final String fullTableName = DdlJobFactory.concatWithDot(targetTable.getKey(), targetTable.getValue());
            ExecutableDdlJob job = new ExecutableDdlJob();
            job.appendTask(emptyTask);
            job.appendTask(emptyTask2);
            job.addConcurrentTasksBetween(emptyTask, emptyTask2, concurrentCheckPhyTableTaskList);
            job.addExcludeResources(Sets.newHashSet(fullTableName));
            result.appendJob2(job);
        }

        return result;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation logicalDdlPlan, DdlJob ddlJob, ExecutionContext ec) {

        if (ec.getDdlContext().isAsyncMode()) {
            return super.buildResultCursor(logicalDdlPlan, ddlJob, ec);
        }

        ArrayResultCursor result = new ArrayResultCursor("Check Table");
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);

        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl = (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();
        List<Pair<String, String>> tableNameList =
            extractTableList(sqlOptimizeTableDdl.getTableNames(), ec.getSchemaName(), ec);
        Map<Pair<String, String>, String> results = DdlJobManagerUtils.reloadCheckPhyTask(ec.getDdlJobId());
        final String fullTableName =
            DdlJobFactory.concatWithDot(logicalOptimizeTable.getSchemaName(), logicalOptimizeTable.getTableName());

        if (results.isEmpty()) {
            result.addRow(new Object[] {
                fullTableName,
                "check",
                "status",
                "OK"
            });
        } else {
            result.addRow(new Object[] {
                fullTableName,
                "check",
                "status",
                "error"
            });
            for(Pair<String, String> phyGroupTable: results.keySet()){
                result.addRow(new Object[] {
                    fullTableName,
                    "check",
                    "msg",
                    results.get(phyGroupTable)
                });
            }
        }
        return result;
    }

    private List<List<DdlTask>> genCheckPhyTableTaskList(DDL ddl, String schemaName, String tableName, String phySql,
                                                         ExecutionContext executionContext) {
        List<List<DdlTask>> groupDdlTasks = new ArrayList<>();
        Map<String, List<List<String>>> tableTopology = PartitionInfoUtil.getTableTopology(schemaName, tableName);
        int i = 0;
        for (String group : tableTopology.keySet()) {
            groupDdlTasks.add(new ArrayList<>());
            for(List<String> targetTables:tableTopology.get(group)) {
                for (String targetTable : targetTables) {
                    DdlTask ddlTask = new CheckPhyTableTask(schemaName, group, targetTable, new HashMap<>());
                    groupDdlTasks.get(i).add(ddlTask);
                }
            }
            i = i + 1;
        }
        return groupDdlTasks;
    }

    private List<Pair<String, String>> extractTableList(List<SqlNode> tableNameSqlNodeList, String currentSchemaName,
                                                        ExecutionContext ec) {
        if (CollectionUtils.isEmpty(tableNameSqlNodeList)) {
            return new ArrayList<>();
        }
        List<Pair<String, String>> result = new ArrayList<>();
        for (SqlNode sqlNode : tableNameSqlNodeList) {
            String schema = currentSchemaName;
            if (!((SqlIdentifier) sqlNode).isSimple()) {
                schema = ((SqlIdentifier) sqlNode).names.get(0);
            }
            String table = ((SqlIdentifier) sqlNode).getLastName();
            result.add(Pair.of(schema, table));

//            List<String> gsiNames = GlobalIndexMeta.getPublishedIndexNames(table, schema, ec);
//            if (CollectionUtils.isNotEmpty(gsiNames)) {
//                for (String gsi : gsiNames) {
//                    result.add(Pair.of(schema, gsi));
//                }
//            }

            /**
             * The target table names of optimize table should ignore cci
             */
            final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiBeans = tableMeta.getGsiPublished();
            if (gsiBeans != null && !gsiBeans.isEmpty()) {
                for (String gsiName : gsiBeans.keySet()) {
                    GsiMetaManager.GsiIndexMetaBean gsiBean = gsiBeans.get(gsiName);
                    if (gsiBean != null) {
                        continue;
                    }
                    result.add(Pair.of(schema, gsiName));
                }
            }
        }
        return result;
    }

}

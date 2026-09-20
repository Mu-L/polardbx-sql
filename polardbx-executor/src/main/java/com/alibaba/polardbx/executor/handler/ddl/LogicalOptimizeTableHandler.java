/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DirectPhysicalSqlPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.RebuildTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.OptimizeTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RebuildTablePrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOptimizeTableDdl;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder.getPhysicalPlanDataForLocalIndex;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * OPTIMIZE TABLE
 *
 * @author guxu.ygh
 * @since 2022/05
 */
public class LogicalOptimizeTableHandler extends LogicalCommonDdlHandler {

    public LogicalOptimizeTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public void prepareFixedResources(BaseDdlOperation logicalDdlPlan,
                                      ExecutionContext executionContext, Set<String> sharedResources,
                                      Set<String> exclusiveResources, Map<String, Long> tableVersions) {
        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl =
            (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();
        /*
            Optimize table 是多表 DDL，例如 optimize table d2.t1, t2;
            t2 的 schemaName 应该是 executionContext.getSchemaName() 里的。
        */
        String currentSchemaName = executionContext.getSchemaName();

        for (SqlNode sqlNode : sqlOptimizeTableDdl.getTableNames()) {
            String schemaName = currentSchemaName;
            if (!((SqlIdentifier) sqlNode).isSimple()) {
                schemaName = ((SqlIdentifier) sqlNode).names.get(0);
            }
            String table = ((SqlIdentifier) sqlNode).getLastName();
            exclusiveResources.add(concatWithDot(schemaName, table));
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(table);
            if (tableMeta != null) {
                tableVersions.put(table, tableMeta.getVersion());
            }
        }
    }

    @Override
    public boolean tableVersionChanged(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext,
                                       Map<String, Long> tableVersions, String schemaName) {
        /*
            Optimize table 是多表 DDL 且允许跨库，不能像默认实现那样用单一 schema 查所有表。
            逐个按 AST 解析出表所在的 schema，用优化时刻的快照（EC 里的 SchemaManager）
            与最新元数据比对。
        */
        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl =
            (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();

        for (SqlNode sqlNode : sqlOptimizeTableDdl.getTableNames()) {
            String tableSchema = schemaName;
            if (!((SqlIdentifier) sqlNode).isSimple()) {
                tableSchema = ((SqlIdentifier) sqlNode).names.get(0);
            }
            String table = ((SqlIdentifier) sqlNode).getLastName();

            TableMeta oldTableMeta = executionContext.getSchemaManager(tableSchema).getTableWithNull(table);
            if (oldTableMeta == null) {
                continue;
            }
            TableMeta latestTableMeta =
                OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTableWithNull(table);
            if (latestTableMeta == null || latestTableMeta.getVersion() > oldTableMeta.getVersion()) {
                return true;
            }
        }

        return false;
    }

    private static final String OPTIMIZE_TABLE_DDL_TEMPLATE = "optimize table ?";

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext ec) {
        boolean forceOmc = ec.getParamManager().getBoolean(ConnectionParams.FORCE_USING_OMC);
        if (forceOmc) {
            return buildOmcOptimizeTableDdlJob(logicalDdlPlan, ec);
        }

        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl = (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();
        AtomicBoolean lock = new AtomicBoolean(false);
        List<Pair<String, String>> tableNameList =
            extractTableList(sqlOptimizeTableDdl.getTableNames(), ec.getSchemaName(), ec, lock);
        final int parallelism = ec.getParamManager().getInt(ConnectionParams.OPTIMIZE_TABLE_PARALLELISM);
        if (parallelism < 1 || parallelism > 4096) {
            throw new TddlNestableRuntimeException("OPTIMIZE_TABLE_PARALLELISM must in range 0-4096");
        }

        ExecutableDdlJob result = new ExecutableDdlJob();

        for (Pair<String, String> targetTable : tableNameList) {
            OptimizeTablePhyDdlTask phyDdlTask =
                genPhyDdlTask(logicalDdlPlan.relDdl, targetTable.getKey(), targetTable.getValue(),
                    ec);
            final String fullTableName = DdlJobFactory.concatWithDot(targetTable.getKey(), targetTable.getValue());
            ExecutableDdlJob job = new ExecutableDdlJob();
            job.addSequentialTasks(Lists.newArrayList(phyDdlTask.partition(parallelism)));
            job.addExcludeResources(Sets.newHashSet(fullTableName));
            result.appendJob2(job);
        }

        if (lock.get()) {
            result.getExplainOnlineDdlInfo().setAlgorithm(OnlineDdlInfo.DdlAlgorithm.COPY);
            result.getExplainOnlineDdlInfo().setDdlType(OnlineDdlInfo.DdlType.LOCK_TABLE);
            result.getExplainOnlineDdlInfo().setAdviceAlgorithm(OnlineDdlInfo.DdlAlgorithm.OMC20);
            result.getExplainOnlineDdlInfo().setAdviceDdlType(OnlineDdlInfo.DdlType.ONLINE_DDL);
            String hint = "/*+TDDL:cmd_extra(FORCE_USING_OMC=true)*/";
            result.getExplainOnlineDdlInfo().setAdviceOnlineDdlSql(hint + ec.getOriginSql());
        } else {
            result.getExplainOnlineDdlInfo().setOnlineDdlAlgorithm(OnlineDdlInfo.DdlAlgorithm.INPLACE);
            result.getExplainOnlineDdlInfo().setOnlineDdlType(OnlineDdlInfo.DdlType.ONLINE_DDL);
            result.getExplainOnlineDdlInfo().setAdviceOnlineDdlSql(ec.getOriginSql());
        }

        return result;
    }

    private DdlJob buildOmcOptimizeTableDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext ec) {
        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl = (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();

        if (sqlOptimizeTableDdl.getTableNames().size() > 1) {
            throw new TddlNestableRuntimeException("OPTIMIZE TABLE with omc algorithm only support one table");
        }

        if (ec.getDdlContext().getDdlStmt().contains(ForeignKeyUtils.PARTITION_FK_SUB_JOB)) {
            ec.getDdlContext().setFkRepartition(true);
        }

        ec.getParamManager().getProps()
            .put(ConnectionProperties.ONLY_MANUAL_TABLEGROUP_ALLOW, Boolean.FALSE.toString());

        List<Pair<String, String>> tableNameList =
            extractTableList(sqlOptimizeTableDdl.getTableNames(), ec.getSchemaName(), ec, new AtomicBoolean(false));
        // get primary table
        String schemaName = tableNameList.get(0).getKey();
        String tableName = tableNameList.get(0).getValue();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

        // generate alter table SqlNode and BaseDdlOperation
        SqlAlterTable sqlAlterTable =
            (SqlAlterTable) new FastsqlParser().parse("alter table " + surroundWithBacktick(tableName), ec).get(0);
        SqlIdentifier tableNameNode =
            new SqlIdentifier(Lists.newArrayList(schemaName, tableName), SqlParserPos.ZERO);
        final RelOptCluster cluster =
            SqlConverter.getInstance(ec).createRelOptCluster(new PlannerContext(ec));
        AlterTable alterTable = AlterTable.create(cluster, sqlAlterTable, tableNameNode, new HashMap<>());

        LogicalAlterTable logicalAlterTable = LogicalAlterTable.create(alterTable);
        logicalAlterTable.setDbIndex(logicalOptimizeTable.getDbIndex());
        logicalAlterTable.setPhyTable(logicalOptimizeTable.getPhyTable());

        logicalAlterTable.prepareOptimizeTable();

        AlterTablePreparedData alterTablePreparedData = logicalAlterTable.getAlterTablePreparedData();
        AlterTableWithGsiPreparedData gsiData = logicalAlterTable.getAlterTableWithGsiPreparedData();

        DdlPhyPlanBuilder alterTableBuilder =
            AlterTableBuilder.create(logicalAlterTable.relDdl, alterTablePreparedData, ec).build();
        PhysicalPlanData physicalPlanData = alterTableBuilder.genPhysicalPlanData();

        RebuildTablePrepareData rebuildTablePrepareData = new RebuildTablePrepareData();
        final long versionId = DdlUtils.generateVersionId(ec);
        rebuildTablePrepareData.setVersionId(versionId);

        initPrimaryTableDefinition(schemaName, tableName, logicalAlterTable, ec, gsiData, rebuildTablePrepareData);

        logicalAlterTable.prepareModifySk(tableMeta);

        List<CreateGlobalIndexPreparedData> globalIndexesPreparedData =
            logicalAlterTable.getCreateGlobalIndexesPreparedData();

        Map<String, CreateGlobalIndexPreparedData> indexTablePreparedDataMap = new LinkedHashMap<>();

        List<Pair<CreateGlobalIndexPreparedData, PhysicalPlanData>> globalIndexPrepareData = new ArrayList<>();
        List<Pair<CreateGlobalIndexPreparedData, PhysicalPlanData>> globalIndexPrepareDataForLocalIndex =
            new ArrayList<>();
        for (CreateGlobalIndexPreparedData createGsiPreparedData : globalIndexesPreparedData) {
            DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
                logicalAlterTable.relDdl,
                createGsiPreparedData,
                indexTablePreparedDataMap,
                ec).build();

            createGsiPreparedData.setLogicalOptimizeTable(true);
            indexTablePreparedDataMap.put(createGsiPreparedData.getIndexTableName(), createGsiPreparedData);
            globalIndexPrepareData.add(new Pair<>(createGsiPreparedData, builder.genPhysicalPlanData()));

            PhysicalPlanData physicalPlanDataForLocalIndex = getPhysicalPlanDataForLocalIndex(builder, false);
            globalIndexPrepareDataForLocalIndex.add(new Pair<>(createGsiPreparedData, physicalPlanDataForLocalIndex));
        }

        RebuildTableJobFactory jobFactory = new RebuildTableJobFactory(
            schemaName,
            tableName,
            tableName,
            globalIndexPrepareData,
            globalIndexPrepareDataForLocalIndex,
            rebuildTablePrepareData,
            physicalPlanData,
            ec
        );

        ExecutableDdlJob ddlJob = jobFactory.create();

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(tableName, tableMeta.getVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(schemaName, tableVersions);

        ddlJob.addTask(validateTableVersionTask);
        ddlJob.addTaskRelationship(validateTableVersionTask, ddlJob.getHead());

        return ddlJob;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation logicalDdlPlan, DdlJob ddlJob, ExecutionContext ec) {

        if (ec.getDdlContext().isAsyncMode()) {
            return super.buildResultCursor(logicalDdlPlan, ddlJob, ec);
        }

        ArrayResultCursor result = new ArrayResultCursor("OptimizeTable");
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);

        LogicalOptimizeTable logicalOptimizeTable = (LogicalOptimizeTable) logicalDdlPlan;
        SqlOptimizeTableDdl sqlOptimizeTableDdl = (SqlOptimizeTableDdl) logicalOptimizeTable.getNativeSqlNode();
        List<Pair<String, String>> tableNameList =
            extractTableList(sqlOptimizeTableDdl.getTableNames(), ec.getSchemaName(), ec, new AtomicBoolean(false));

        for (Pair<String, String> targetTable : tableNameList) {
            final String fullTableName = DdlJobFactory.concatWithDot(targetTable.getKey(), targetTable.getValue());
            result.addRow(new Object[] {
                fullTableName,
                "optimize",
                "note",
                "Table does not support optimize, doing recreate + analyze instead"
            });
            result.addRow(new Object[] {
                fullTableName,
                "optimize",
                "status",
                "OK"
            });
        }

        return result;
    }

    private OptimizeTablePhyDdlTask genPhyDdlTask(DDL ddl, String schemaName, String tableName,
                                                  ExecutionContext executionContext) {
        ddl.sqlNode =
            SqlPhyDdlWrapper.createForAllocateLocalPartition(new SqlIdentifier(tableName, SqlParserPos.ZERO),
                LogicalOptimizeTableHandler.OPTIMIZE_TABLE_DDL_TEMPLATE);
        DirectPhysicalSqlPlanBuilder builder = new DirectPhysicalSqlPlanBuilder(
            ddl, new ReorganizeLocalPartitionPreparedData(schemaName, tableName), executionContext
        );
        builder.build();
        return new OptimizeTablePhyDdlTask(schemaName, builder.genPhysicalPlanData());
    }

    private List<Pair<String, String>> extractTableList(List<SqlNode> tableNameSqlNodeList, String currentSchemaName,
                                                        ExecutionContext ec, AtomicBoolean lock) {
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

            /**
             * The target table names of optimize table should ignore cci
             */
            List<String> gsiNames = GlobalIndexMeta.getPublishedIndexNames(table, schema, ec);
            if (CollectionUtils.isNotEmpty(gsiNames)) {
                for (String gsi : gsiNames) {
                    result.add(Pair.of(schema, gsi));
                }
            }

            TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
            List<IndexMeta> indexMetas = tableMeta.getSecondaryIndexes();
            for (IndexMeta indexMeta : indexMetas) {
                if (indexMeta.isFullTextIndexOrSpatialIndex()) {
                    lock.set(true);
                }
            }
        }
        return result;
    }

    private void initPrimaryTableDefinition(String schemaName, String tableName,
                                            BaseDdlOperation logicalDdlPlan,
                                            ExecutionContext executionContext,
                                            AlterTableWithGsiPreparedData gsiData,
                                            RebuildTablePrepareData rebuildTablePrepareData) {
        SqlAlterTable ast =
            (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();

        AlterTablePreparedData alterTablePreparedData =
            ((LogicalAlterTable) logicalDdlPlan).getAlterTablePreparedData();
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);

        List<SqlIndexDefinition> gsiList;
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            gsiList = LogicalAlterTableHandler.buildIndexDefinition4Auto(schemaName, tableName, logicalDdlPlan,
                executionContext, gsiData, alterTablePreparedData, rebuildTablePrepareData, new AtomicBoolean(),
                new ArrayList<>(), primaryTableInfo, ast);
        } else {
            gsiList = LogicalAlterTableHandler.buildIndexDefinition4Drds(schemaName, tableName, executionContext,
                gsiData, alterTablePreparedData, rebuildTablePrepareData, primaryTableInfo, ast);
        }

        List<SqlAddIndex> sqlAddIndexList = gsiList.stream().map(e ->
            StringUtils.equalsIgnoreCase(e.getType(), "UNIQUE") ?
                new SqlAddUniqueIndex(SqlParserPos.ZERO, e.getIndexName(), e) :
                new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
        ).collect(Collectors.toList());

        ast.getSkAlters().addAll(sqlAddIndexList);
    }

}

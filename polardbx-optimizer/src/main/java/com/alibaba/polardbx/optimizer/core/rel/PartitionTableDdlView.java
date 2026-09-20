package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.logical.LogicalAlterTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The physical ddl  for partition tables
 *
 * @author chenghui.lch
 */
@Deprecated
public class PartitionTableDdlView extends DataDefLanguageLogicView {

    /**
     * PartitionInfo of create table(or index table) ddl
     */
    private PartitionInfo partitionInfo;

    /**
     * PartitionInfo of create table ddl with gsi
     */
    protected Map<String, PartitionInfo> gsiPartitionInfos;

    /**
     * PartitionInfo of create table ddl with gusi
     */
    protected Map<String, PartitionInfo> gusiPartitionInfos;

    /**
     * PartitionInfo of create table ddl with gsiClustered
     */
    protected Map<String, PartitionInfo> gsiClusteredPartitionInfos;

    /**
     * PartitionInfo of create table ddl with gusiClustered
     */
    protected Map<String, PartitionInfo> gusiClusteredPartitionInfos;

    /**
     * The raw sql partition syntax for create table ddl
     */
    protected SqlNode sqlPartition;

    /**
     * The new PartitionInfo (memory only) of alter table ddl
     */
    protected PartitionInfo partitionInfoAltered;

    /**
     * All the rex expr info collected in DDL
     *
     * <pre>
     *     The rex expr info is collected by Validator
     *     For example 1:
     *      e.g. For the alert stmt: "alter table list_tbl modify partition add values ( 1, 2+3, 4+5 ) ",
     *      then the rexExprInfos will as followed:
     *          SqlLiteral:1(key) --> RexLiteral:1(val)
     *          SqlCall:2+3(key) --> RexCall:2+3(val)
     *          SqlCall:4+5(key) --> RexCall:4+5(val)
     *    , and this info will be useful in calc the bound value of list partitions.
     *
     *    For example 2:
     *      e.g. For the alert stmt: "alter table rng_col_tbl add partition ( partition p4 values less than (1+3+4, '2020-12-12', 'NewYork') ) ENGINE = InnoDB ) ",
     *      then the rexExprInfos will as followed:
     *          SqlCall:1+3+4(key) --> RexCall:1+3+4(val)
     *          SqlLiteral:'2020-12-12'(key) --> RexLiteral:'2020-12-12'(val)
     *          SqlLiteral:'NewYork'(key) --> RexLiteral:'NewYork'(val)
     *      , and this info will be useful in calc the bound value of range columns partitions.
     *  </pre>
     */
    protected Map<SqlNode, RexNode> allRexExprInfoInDdl = new HashMap<>();
    final ExecutionContext executionContext;

    public PartitionInfo getPartitionInfoAltered() {
        return partitionInfoAltered;
    }

    public PartitionTableDdlView(DDL ddlView, PartitionInfo partitionInfo, ExecutionContext ec) {
        super(ddlView);
        gsiPartitionInfos = new LinkedHashMap<>();
        gusiPartitionInfos = new LinkedHashMap<>();
        gsiClusteredPartitionInfos = new LinkedHashMap<>();
        gusiClusteredPartitionInfos = new LinkedHashMap<>();
        this.partitionInfo = partitionInfo;
        this.executionContext = ec;
        if (ddlView.getSqlNode() instanceof SqlAlterTable) {
            Map<SqlNode, RexNode> partBoundExprInfo = null;
            if (ddlView instanceof LogicalAlterTable) {
                partBoundExprInfo = ((LogicalAlterTable) ddlView).getAllRexExprInfo();
            }
            SqlAlterTable sqlAlterTable = (SqlAlterTable) ddlView.getSqlNode();
            String primaryTableName = partitionInfo.getTableName();
            String schName = partitionInfo.getTableSchema();
            TableMeta tableMeta =
                OptimizerContext.getContext(schName).getLatestSchemaManager().getTable(primaryTableName);
            for (SqlAlterSpecification sqlAlterSpecification : sqlAlterTable.getAlters()) {
                if (sqlAlterSpecification instanceof SqlAddIndex) {
                    SqlAddIndex sqlAddIndex = (SqlAddIndex) sqlAlterSpecification;
                    if (sqlAddIndex.getIndexDef().isGlobal()) {
                        if (sqlAddIndex.getIndexDef().isClustered()) {
                            buildGsiPartitionInfo(tableMeta,
                                ImmutableList.of(new Pair<>(sqlAddIndex.getIndexName(), sqlAddIndex.getIndexDef())),
                                partBoundExprInfo,
                                gsiClusteredPartitionInfos);
                        } else {
                            buildGsiPartitionInfo(tableMeta,
                                ImmutableList.of(new Pair<>(sqlAddIndex.getIndexName(), sqlAddIndex.getIndexDef())),
                                partBoundExprInfo,
                                gsiPartitionInfos);
                        }
                    }
                } else if (sqlAlterSpecification instanceof SqlAddUniqueIndex) {
                    SqlAddUniqueIndex sqlAddIndex = (SqlAddUniqueIndex) sqlAlterSpecification;
                    if (sqlAddIndex.getIndexDef().isGlobal()) {
                        if (sqlAddIndex.getIndexDef().isClustered()) {
                            buildGsiPartitionInfo(tableMeta,
                                ImmutableList.of(new Pair<>(sqlAddIndex.getIndexName(), sqlAddIndex.getIndexDef())),
                                partBoundExprInfo,
                                gusiClusteredPartitionInfos);
                        } else {
                            buildGsiPartitionInfo(tableMeta,
                                ImmutableList.of(new Pair<>(sqlAddIndex.getIndexName(), sqlAddIndex.getIndexDef())),
                                partBoundExprInfo,
                                gusiPartitionInfos);
                        }
                    }
                } else if (sqlAlterSpecification instanceof SqlAlterTableDropIndex) {
                    SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) sqlAlterSpecification;
                    // do nothings
                }
            }
        }
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        Map<Integer, ParameterContext> param =
            executionContext.getParams() == null ? null : executionContext.getParams()
                .getCurrentParameter();
        if (targetInput == null) {
            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanBuilder = new PhyDDLViewBuilder(getSqlTemplate(),
                targetTables,
                param,
                this,
                DbType.MYSQL,
                this.schemaName,
                executionContext);
            targetInput = phyTableScanBuilder.build();
        }
        return targetInput;
    }

    public List<RelNode> getInput(ExecutionContext executionContext, SqlNode sqlNode) {
        Map<Integer, ParameterContext> param =
            executionContext.getParams() == null ? null : executionContext.getParams()
                .getCurrentParameter();
        if (targetInput == null) {
            ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
            sqlNode = sqlNode.accept(visitor);

            Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
            PhyDDLViewBuilder phyTableScanBuilder = new PhyDDLViewBuilder(sqlNode,
                targetTables,
                param,
                this,
                DbType.MYSQL,
                this.schemaName,
                executionContext);
            targetInput = phyTableScanBuilder.build();
        }
        return targetInput;
    }

    /**
     * Build and get all PhyDdlTableOperations of DdlView
     * <p>
     */
    @Override
    public List<RelNode> getInput(ExecutionContext executionContext, List<String> groupNames) {
        List relNodes = Arrays.asList();

        Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);

        for (Iterator<String> iterator = targetTables.keySet().iterator(); iterator.hasNext(); ) {
            String next = iterator.next();
            final List<List<String>> lists = targetTables.get(next);
            for (int i = 0; i < lists.size(); i++) {
                PhyDdlTableOperation phyDataDefLanguageOperation = new PhyDdlTableOperation(this);
                phyDataDefLanguageOperation.setDbIndex(next);
                phyDataDefLanguageOperation.setTableNames(lists);
                phyDataDefLanguageOperation.setExplain(false);
                phyDataDefLanguageOperation.setTableRule(null);
                phyDataDefLanguageOperation.setPartitionInfo(partitionInfo);
                if (partitionInfo != null) {
                    phyDataDefLanguageOperation.setPartitioned(this.isPartition());
                }
                phyDataDefLanguageOperation.setShadowDbOnly(false);
                phyDataDefLanguageOperation.setDbType(DbType.MYSQL);

                phyDataDefLanguageOperation.setBytesSql(RelUtils.toNativeBytesSql(sqlNode));
                phyDataDefLanguageOperation.setSchemaName(this.schemaName);
                relNodes.add(phyDataDefLanguageOperation);
            }
        }
        return relNodes;
    }

    private void buildTargetTablesFromPartitionInfo(PartitionInfo partitionInfo, ExecutionContext executionContext,
                                                    Map<String, List<List<String>>> targetTables) {

        // get table type
        PartitionTableType tblType = partitionInfo.getTableType();

        // classify phyTbList for each phy grp
        Map<String, List<String>> grpPhyTbListMap = new HashMap<>();
        if (tblType != PartitionTableType.BROADCAST_TABLE) {

            // get all partitions ( included all subpartitions ) by partition
            List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
            for (int i = 0; i < partitionSpecs.size(); i++) {
                PartitionSpec ps = partitionSpecs.get(i);
                PartitionLocation location = ps.getLocation();
                String grp = location.getGroupKey();
                String phyTb = location.getPhyTableName();
                List<String> phyTbList = grpPhyTbListMap.get(grp);
                if (phyTbList == null) {
                    phyTbList = new ArrayList<>();
                    grpPhyTbListMap.put(grp, phyTbList);
                }
                phyTbList.add(phyTb);
            }

        } else {

            // For broadcast table only
            List<String> grpList = HintUtil.allGroup(partitionInfo.getTableSchema());
            PartitionSpec ps = partitionInfo.getPartitionBy().getPhysicalPartitions().get(0);
            String phyTb = ps.getLocation().getPhyTableName();
            grpList.stream().forEach(grp -> grpPhyTbListMap.computeIfAbsent(grp, g -> new ArrayList<>()).add(phyTb));
        }

        for (Map.Entry<String, List<String>> grpPhyTbListItem : grpPhyTbListMap.entrySet()) {
            List<List<String>> allPhyTbsList = new ArrayList<>();
            String grpName = grpPhyTbListItem.getKey();
            List<String> phyTbListOfDiffIndex = grpPhyTbListItem.getValue();
            for (int i = 0; i < phyTbListOfDiffIndex.size(); i++) {
                List<String> phyTbListOfSameIndex = new ArrayList<>();
                phyTbListOfSameIndex.add(phyTbListOfDiffIndex.get(i));
                allPhyTbsList.add(phyTbListOfSameIndex);
            }
            targetTables.put(grpName, allPhyTbsList);
        }
    }

    /**
     * <pre>
     *  Map
     *     key:
     *          groupKey
     *     val:
     *          list of all phy tbs (in the same group) in on one physical sql
     *
     *           List<List<String>>:
     *              key: index of phy table index
     *              val: list of the same index of phy table of different logical table
     *
     * <pre/>
     *
     *  <></>
     *
     *
     * @param executionContext
     * @return
     */
    @Override
    public Map<String, List<List<String>>> getTargetTables(ExecutionContext executionContext) {

        Map<String, List<List<String>>> results = new HashMap<>();

        SqlKind kind = sqlNode.getKind();
        if (kind == SqlKind.CREATE_TABLE || kind == SqlKind.DROP_TABLE || kind == SqlKind.CREATE_INDEX) {
            buildTargetTablesFromPartitionInfo(partitionInfo, executionContext, results);
            buildGsiTable(gsiPartitionInfos, gsiTargetTables);
            buildGsiTable(gusiPartitionInfos, gusiTargetTables);
            buildGsiTable(gsiClusteredPartitionInfos, gsiClusteredTargetTables);
            buildGsiTable(gusiClusteredPartitionInfos, gusiClusteredTargetTables);

            if (GeneralUtil.isNotEmpty(gsiPartitionInfos)) {
                for (Map.Entry<String, PartitionInfo> entry : gsiPartitionInfos.entrySet()) {
                    Map<String, List<List<String>>> gsiTargetTable = new HashMap<>();
                    buildTargetTablesFromPartitionInfo(entry.getValue(), executionContext, gsiTargetTable);
                    gsiTargetTables.put(entry.getKey(), gsiTargetTable);
                }
            }
            if (kind == SqlKind.CREATE_INDEX) {
                gsiTargetTables.put(((SqlCreateIndex) sqlNode).getIndexName().getLastName(), results);
            } else if (kind == SqlKind.DROP_TABLE) {
                final SqlDropTable dropTable = (SqlDropTable) sqlNode;
                final String tableName = dropTable.getOriginTableName().getLastName();
                final GsiMetaManager.GsiMetaBean gsiMetaBean =
                    executionContext.getSchemaManager(schemaName).getGsi(tableName, IndexStatus.ALL);
                if (gsiMetaBean.withGsi(tableName)) {
                    final GsiMetaManager.GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
                    PartitionInfoManager partitionInfoManager =
                        OptimizerContext.getContext(schemaName).getPartitionInfoManager();
                    for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap
                        .entrySet()) {
                        if (gsiEntry.getValue().columnarIndex) {
                            continue;
                        }
                        final String indexTableName = gsiEntry.getKey();
                        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(indexTableName);
                        Map<String, List<List<String>>> gsiResults = new HashMap<>();
                        buildTargetTablesFromPartitionInfo(partitionInfo, executionContext, gsiResults);
                        this.gsiTargetTables.put(indexTableName, gsiResults);
                    }
                }
            }
        } else if (kind == SqlKind.ALTER_TABLE) {
            final SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
            if (alterTable.isDropPartition()) {
//                final SqlAlterTableDropPartition dropPartition = (SqlAlterTableDropPartition) alterTable.getAlters()
//                    .get(0);
//                PartitionInfoUtil.validateDropPartition(partitionInfo.getPartitionBy().getPartitions(), dropPartition);
//                final String partitionName = dropPartition.getPartitionName().getLastName();
//                Map<String, List<PhysicalPartitionInfo>> groupAndPartitionMap = partitionInfo.getPhysicalPartitionTopology(ImmutableList.of(partitionName));
//                for (Map.Entry<String, List<PhysicalPartitionInfo>> entry : groupAndPartitionMap.entrySet()) {
//                    List<List<String>> physicalTables = new ArrayList();
//                    for (PhysicalPartitionInfo part : entry.getValue()) {
//                        List<String> physicalTablesOfSameIndex = new ArrayList<>();
//                        physicalTablesOfSameIndex.add(part.getPartName());
//                        physicalTables.add(physicalTablesOfSameIndex);
//                    }
//                    results.put(entry.getKey(), physicalTables);
//                }
                // Do nothings
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Not support alter table drop partition");
            } else if (alterTable.isAddPartition()) {
                // Do nothings 
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Not support alter table add partition");
            } else if (alterTable.isTruncatePartition()) {
                SqlAlterTableTruncatePartition truncatePart =
                    (SqlAlterTableTruncatePartition) alterTable.getAlters().get(0);
                SqlNode tarPartName = truncatePart.getPartitionNames().get(0);
                String tartPartNameStr = ((SqlIdentifier) tarPartName).getLastName();
                PartitionInfo partInfo = this.getPartitionInfo();
                PartitionSpec pSepc = partInfo.getPartitionBy().getPartitionByPartName(tartPartNameStr);
                PartitionLocation location = pSepc.getLocation();
                String grpKey = location.getGroupKey();
                String phyTableName = location.getPhyTableName();
                List<List<String>> phyTbsInfo = new ArrayList<>();
                List<String> phyTbs = new ArrayList<>();
                phyTbs.add(phyTableName);
                phyTbsInfo.add(phyTbs);
                results.put(grpKey, phyTbsInfo);
                return results;
            } else if (alterTable.createGsi() || alterTable.dropIndex()) {
                List<Pair<SqlIdentifier, SqlIndexDefinition>> indexsInfo = new ArrayList<>();
                PartitionInfoManager partitionInfoManager =
                    OptimizerContext.getContext(schemaName).getPartitionInfoManager();
                boolean isDropIndex = false;
                boolean isDropLocalIndex = false;
                for (SqlAlterSpecification alterSpecification : alterTable.getAlters()) {
                    if (alterSpecification instanceof SqlAddIndex) {
                        indexsInfo.add(new Pair<>(
                            ((SqlAddIndex) alterSpecification).getIndexName(),
                            ((SqlAddIndex) alterSpecification).getIndexDef()));
                    } else if (alterSpecification instanceof SqlAlterTableDropIndex) {
                        isDropIndex = true;
                        String indexName = ((SqlAlterTableDropIndex) alterSpecification).getIndexName().getLastName();
                        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(indexName);
                        if (partitionInfo != null) {
                            gsiPartitionInfos.putIfAbsent(indexName, partitionInfo);
                        } else {
                            isDropLocalIndex = true;
                        }

                    }
                }
                if (!isDropIndex) {
                    String primaryTableName = partitionInfo.getTableName();
                    String schName = partitionInfo.getTableSchema();
                    TableMeta tableMeta =
                        OptimizerContext.getContext(schName).getLatestSchemaManager().getTable(primaryTableName);

                    Map<SqlNode, RexNode> partBoundExprInfo = null;
                    if (relNode instanceof LogicalAlterTable) {
                        partBoundExprInfo = ((LogicalAlterTable) relNode).getAllRexExprInfo();
                    }
                    buildGsiPartitionInfo(tableMeta, indexsInfo, partBoundExprInfo, gsiPartitionInfos);
                } else {
                    // drop index
                    if (isDropLocalIndex) {
                        // drop local index
                        buildTargetTablesFromPartitionInfo(partitionInfo, executionContext, results);
                    }
                }

                /**
                 * Init gsiTargetTables by gsiPartitionInfos
                 */
                if (GeneralUtil.isNotEmpty(gsiPartitionInfos)) {
                    for (Map.Entry<String, PartitionInfo> entry : gsiPartitionInfos.entrySet()) {
                        Map<String, List<List<String>>> gsiTargetTable = new HashMap<>();
                        buildTargetTablesFromPartitionInfo(entry.getValue(), executionContext, gsiTargetTable);
                        gsiTargetTables.put(entry.getKey(), gsiTargetTable);
                    }
                }
            }
        } else {
            results = super.getTargetTables(executionContext);
        }

        return results;
    }

    @Override
    protected SqlNode buildSqlTemplate() {
        SqlNode sqlTemplate = getNativeSqlNode();
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);
        SqlNode sqlTemplateReplacedTbName = sqlTemplate.accept(visitor);
        return sqlTemplateReplacedTbName;
    }

    private void buildGsiTable(Map<String, PartitionInfo> gsiPartitionInfos,
                               Map<String, Map<String, List<List<String>>>> gsiTargetTables) {
        if (GeneralUtil.isNotEmpty(gsiPartitionInfos)) {
            for (Map.Entry<String, PartitionInfo> entry : gsiPartitionInfos.entrySet()) {
                Map<String, List<List<String>>> gsiTargetTable = new HashMap<>();
                buildTargetTablesFromPartitionInfo(entry.getValue(), executionContext, gsiTargetTable);
                gsiTargetTables.put(entry.getKey(), gsiTargetTable);
            }
        }
    }

    protected void buildGsiPartitionInfo(TableMeta tableMeta,
                                         List<Pair<SqlIdentifier, SqlIndexDefinition>> indexsInfo,
                                         Map<SqlNode, RexNode> partBoundExprInfo,
                                         Map<String, PartitionInfo> gsiPartitionInfos) {
        List<ColumnMeta> allColMetas = tableMeta.getAllColumns();
        if (GeneralUtil.isNotEmpty(indexsInfo)) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> pair : indexsInfo) {
                String indexName = pair.getKey().getLastName();
                PartitionInfo partitionInfo =
                    PartitionInfoBuilder.buildPartitionInfoByPartDefAst(schemaName, indexName, null, false, null,
                        (SqlPartitionBy) pair.getValue().getPartitioning(), partBoundExprInfo, null, allColMetas,
                        PartitionTableType.GSI_TABLE,
                        executionContext);
                partitionInfo.setTableType(PartitionTableType.GSI_TABLE);
                gsiPartitionInfos.put(indexName, partitionInfo);
                gsiIndexDefs.put(indexName, pair.getValue());
            }
        }
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public Map<String, PartitionInfo> getAllGsiPartitionInfos() {
        Map<String, PartitionInfo> allGsiPartitionInfos = new LinkedHashMap<>();
        allGsiPartitionInfos.putAll(gsiPartitionInfos);
        allGsiPartitionInfos.putAll(gusiPartitionInfos);
        allGsiPartitionInfos.putAll(gsiClusteredPartitionInfos);
        allGsiPartitionInfos.putAll(gusiClusteredPartitionInfos);

        return allGsiPartitionInfos;
    }

    public void setGsiPartitionInfos(
        Map<String, PartitionInfo> gsiPartitionInfos) {
        this.gsiPartitionInfos = gsiPartitionInfos;
    }

    public void setAllRexExprInfoInDdl(
        Map<SqlNode, RexNode> allRexExprInfoInDdl) {
        this.allRexExprInfoInDdl = allRexExprInfoInDdl;
    }

    public Map<SqlNode, RexNode> getAllRexExprInfoInDdl() {
        return allRexExprInfoInDdl;
    }

    public void setPartitionInfoAltered(PartitionInfo partitionInfoAltered) {
        this.partitionInfoAltered = partitionInfoAltered;
    }
}

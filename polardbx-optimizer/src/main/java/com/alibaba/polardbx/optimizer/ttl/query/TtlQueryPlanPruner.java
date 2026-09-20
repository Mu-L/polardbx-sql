package com.alibaba.polardbx.optimizer.ttl.query;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartByDefFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ConditionResultSet;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalHybridUnion;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlSubPartition;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TtlQueryPlanPruner extends RelShuttleImpl {

    private ExecutionContext ec;

    private TableMeta ttlTableMeta = null;

    private static final int logicalViewIndex = 0;

    private LogicalView ttlLogicalView = null;

    private Map<Pair<String, String>, TtlQueryType> ttlQueryTypeMap = new HashMap<>();

    public TtlQueryPlanPruner(ExecutionContext ec) {
        this.ec = ec;
    }

    private boolean checkTtlUnion(LogicalUnion union) {
        if (!(union instanceof LogicalHybridUnion)) {
            return false;
        }
        if (!union.all) {
            return false;
        }
        if (union.getInputs().size() != 2) {
            return false;
        }

        LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
        union.getInput(logicalViewIndex).accept(logicalViewFinder);
        List<LogicalView> logicalViews = logicalViewFinder.getResult();
        LogicalView logicalView = null;
        if (logicalViews.isEmpty()) {
            return false;
        } else if (logicalViews.size() == 1) {
            logicalView = logicalViews.get(0);
        } else {
            //当有多个LogicalView时,说明行存表走了BKAJoin
            for (int i = 0; i < logicalViews.size(); i++) {
                logicalView = logicalViews.get(i);
                if (logicalView.getClass() == LogicalView.class) {
                    break;
                }
            }
        }

        String schemaName = logicalView.getSchemaName();
        String tableName = logicalView.getLogicalTableName();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        //索引选择
        if (tableMeta.isGsi() || tableMeta.isColumnar()) {
            schemaName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableSchema;
            tableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        }

        TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
        if (ttlDefinitionInfo == null) {
            return false;
        }

        this.ttlTableMeta = tableMeta;
        this.ttlLogicalView = logicalView;
        return true;
    }

    @Override
    public RelNode visit(LogicalUnion union) {

        if (!checkTtlUnion(union)) {
            return super.visit(union);
        }

        String schemaName = ttlTableMeta.getSchemaName();
        String tableName = ttlTableMeta.getTableName();
        TtlDefinitionInfo ttlDefinitionInfo = ttlTableMeta.getTtlDefinitionInfo();

        String ttlQueryBoundary = TtlQueryUtil.getTtlQueryBoundary(ec, schemaName, tableName);
        if (ttlQueryBoundary == null) {
            //当冷热数据分界线是null时，说明还没有清理过历史数据
            setTtlQueryType(schemaName, tableName, TtlQueryType.HOT_ONLY);
            return union.getInput(logicalViewIndex);
        }

        PartitionInfo mockPartitionInfo = mockRangePartitionInfo(ec, union.getCluster().getRexBuilder(), ttlTableMeta,
            ttlDefinitionInfo.getTtlColMeta(ec), ttlQueryBoundary);
        TtlQueryType ttlQueryType = prune(ttlLogicalView, mockPartitionInfo, ec);
        setTtlQueryType(schemaName, tableName, ttlQueryType);
        if (ttlQueryType == TtlQueryType.HOT_ONLY || ttlQueryType == TtlQueryType.COLD_ONLY) {
            return union.getInput(ttlQueryType == TtlQueryType.HOT_ONLY ? logicalViewIndex : 1 - logicalViewIndex);
        }

        Map<String, String> refColQueryBoundaryMap =
            TtlQueryUtil.getRefColQueryBoundary(ec.getSchemaManager(schemaName).getTable(tableName));
        if (refColQueryBoundaryMap != null) {
            for (Map.Entry<String, String> entry : refColQueryBoundaryMap.entrySet()) {
                String relColName = entry.getKey();
                String refColQueryBoundary = entry.getValue();
                ColumnMeta refColMeta = ttlTableMeta.getColumn(relColName);
                if (refColMeta == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("column [%s.%s] does not exist.", tableName, relColName));
                }
                if (refColQueryBoundary == null || refColMeta.getDataType().convertFrom(refColQueryBoundary) == null) {
                    continue;
                }
                PartitionInfo mockRefColPartitionInfo =
                    mockListRangePartitionInfo(ec, union.getCluster().getRexBuilder(), ttlTableMeta, refColMeta,
                        refColQueryBoundary);
                ttlQueryType = prune(ttlLogicalView, mockRefColPartitionInfo, ec);
                if (ttlQueryType == TtlQueryType.HOT_ONLY || ttlQueryType == TtlQueryType.COLD_ONLY) {
                    setTtlQueryType(schemaName, tableName, ttlQueryType);
                    return union.getInput(
                        ttlQueryType == TtlQueryType.HOT_ONLY ? logicalViewIndex : 1 - logicalViewIndex);
                }
            }
        }

        return super.visit(union);
    }

    private void setTtlQueryType(String schema, String table, TtlQueryType type) {
        Pair<String, String> key = Pair.of(schema, table);
        ttlQueryTypeMap.put(key, type);
    }

    private static TtlQueryType prune(LogicalView logicalView, PartitionInfo partitionInfo, ExecutionContext ec) {
        boolean useSelectPartitions = logicalView.useSelectPartitions();
        if (useSelectPartitions) {
            throw new UnsupportedOperationException("not support select partition");
        }

        RelNode pushedRelNode = logicalView.getPushedRelNode();
        TableScanFinder tableScanFinder = new TableScanFinder();
        pushedRelNode.accept(tableScanFinder);
        TableScan tableScan = tableScanFinder.getResult().get(0).getValue();

        ConditionExtractor conditionExtractor = ConditionExtractor.predicateFrom(pushedRelNode);
        ExtractionResult extractionResult = conditionExtractor.extract();
        ConditionResultSet conditionResultSet = extractionResult.conditionOf(tableScan.getTable());
        final List<RexNode> predicates = conditionResultSet.intersect().toRexNodes().stream().filter(
            rexNode -> {
                AtomicBoolean ttlQueryBoundaryFinder = new AtomicBoolean(false);
                rexNode.accept(new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (call.getOperator() == TddlOperatorTable.TTL_QUERY_BOUNDARY) {
                            ttlQueryBoundaryFinder.set(true);
                            return call;
                        }
                        return super.visitCall(call);
                    }
                });
                return !ttlQueryBoundaryFinder.get();
            }
        ).collect(Collectors.toList());

        if (predicates.isEmpty()) {
            return TtlQueryType.HOT_AND_COLD;
        }

        final RexNode comparison =
            RexUtil.composeConjunction(logicalView.getCluster().getRexBuilder(), predicates, true);
        PartitionPruneStep partitionPruneStep =
            PartitionPruner.generatePartitionPrueStepInfo(partitionInfo, tableScan, comparison, ec);
        PartPrunedResult partPrunedResult = PartitionPruner.doPruningByStepInfo(partitionPruneStep, ec);
        BitSet prunedPartitions = partPrunedResult.getPartBitSet();

        if (partitionInfo.getPartitionBy().getSubPartitionBy() == null) {
            if (prunedPartitions.get(0) && !prunedPartitions.get(1)) {
                return TtlQueryType.COLD_ONLY;
            } else if (!prunedPartitions.get(0) && prunedPartitions.get(1)) {
                return TtlQueryType.HOT_ONLY;
            }
        } else {
            if (partPrunedResult.getPartBitSet().get(0)) {
                return TtlQueryType.HOT_AND_COLD;
            }
            if (prunedPartitions.get(1) && !prunedPartitions.get(2)) {
                return TtlQueryType.COLD_ONLY;
            } else if (!prunedPartitions.get(1) && prunedPartitions.get(2)) {
                return TtlQueryType.HOT_ONLY;
            }
        }
        return TtlQueryType.HOT_AND_COLD;
    }

    private static PartitionInfo mockRangePartitionInfo(ExecutionContext ec, RexBuilder rexBuilder, TableMeta tableMeta,
                                                        ColumnMeta ttlColumnMeta, String ttlQueryBoundary) {
        String ttlQueryBoundaryInSql = ttlQueryBoundary;
        DataType dataType = ttlColumnMeta.getDataType();
        if (!DataTypeUtil.isNumberSqlType(dataType)) {
            ttlQueryBoundaryInSql = "'" + ttlQueryBoundary + "'";
        }
        String partitionBy = String.format(
            "PARTITION BY RANGE(%s)\n" +
                "(\n" +
                "   PARTITION p1 VALUES LESS THAN(%s),\n" +
                "   PARTITION pm VALUES LESS THAN(MAXVALUE)\n" +
                ")",
            ttlColumnMeta.getOriginColumnName(), ttlQueryBoundaryInSql);
        MySqlCreateTableParser parser = new MySqlCreateTableParser(ByteString.from(partitionBy));
        SQLPartitionBy SQLPartitionBy = parser.parsePartitionBy();
        FastSqlToCalciteNodeVisitor fastSqlToCalciteNodeVisitor = new FastSqlToCalciteNodeVisitor(null, null);
        SQLPartitionBy.accept(fastSqlToCalciteNodeVisitor);
        SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) fastSqlToCalciteNodeVisitor.getSqlNode();

        Map<SqlNode, RexNode> boundExprInfo = new HashMap<>();
        for (SqlNode sqlNode : sqlPartitionBy.getPartitions()) {
            SqlPartition sqlPartition = (SqlPartition) sqlNode;
            SqlNode lessThanValue = sqlPartition.getValues().getLessThanValue();
            if (lessThanValue instanceof SqlLiteral) {
                RexNode lessThanValueRexNode = null;
                if (!DataTypeUtil.isNumberSqlType(dataType)) {
                    lessThanValueRexNode = rexBuilder.makeLiteral(
                        DataTypes.StringType.convertFrom(((SqlLiteral) lessThanValue).toValue()));
                } else {
                    lessThanValueRexNode = rexBuilder.makeBigIntLiteral(
                        DataTypes.LongType.convertFrom(((SqlLiteral) lessThanValue).toValue()));
                }
                boundExprInfo.put(lessThanValue, lessThanValueRexNode);
            } else {
                boundExprInfo.put(lessThanValue, rexBuilder.makeLiteral("MAXVALUE"));
            }
        }

        if (dataType.getClass() == TimestampType.class) {
            boolean origin =
                ec.getParamManager().getBoolean(ConnectionParams.ALLOW_USING_TIMESTAMP_IN_RANGE_LIST_PARTITION);
            try {
                ec.getParamManager().getProps()
                    .put(ConnectionProperties.ALLOW_USING_TIMESTAMP_IN_RANGE_LIST_PARTITION, "true");
                return mockPartitionInfo(sqlPartitionBy, tableMeta, ec, boundExprInfo);
            } finally {
                ec.getParamManager().getProps()
                    .put(ConnectionProperties.ALLOW_USING_TIMESTAMP_IN_RANGE_LIST_PARTITION, String.valueOf(origin));
            }
        }

        return mockPartitionInfo(sqlPartitionBy, tableMeta, ec, boundExprInfo);
    }

    public static PartitionInfo mockPartitionInfo(SqlPartitionBy sqlPartitionBy, TableMeta tableMeta,
                                                  ExecutionContext ec, Map<SqlNode, RexNode> boundExprInfo) {
        String tableName = tableMeta.getTableName();
        String schemaName = tableMeta.getSchemaName();
        PartitionInfo partitionInfo = new PartitionInfo();
        PartitionStrategy partStrategy =
            PartitionInfoBuilder.buildPartByStrategy(schemaName, sqlPartitionBy, PartitionTableType.PARTITION_TABLE);
        boolean containNextLevelPartSpec = sqlPartitionBy != null && sqlPartitionBy.getSubPartitionBy() != null;
        LocalityDesc localityDesc = new LocalityDesc();
        long partFlags = 0L;

        /**
         * Build PartitionBy for partition
         */
        BuildPartByDefFromAstParams partByAstParam = new BuildPartByDefFromAstParams();
        partByAstParam.setSchemaName(schemaName);
        partByAstParam.setTableName(tableName);
        partByAstParam.setTableGroupName(null);
        partByAstParam.setJoinGroupName(null);
        partByAstParam.setPartByAstColumns(sqlPartitionBy == null ? null : sqlPartitionBy.getColumns());
        partByAstParam.setPartByAstPartitions(sqlPartitionBy == null ? null : sqlPartitionBy.getPartitions());
        partByAstParam.setPartCntAst(sqlPartitionBy == null ? null : sqlPartitionBy.getPartitionsCount());
        partByAstParam.setPartByStrategy(partStrategy);
        partByAstParam.setBoundExprInfo(boundExprInfo);
        partByAstParam.setPkColMetas(new ArrayList<>(tableMeta.getPrimaryKey()));
        partByAstParam.setAllColMetas(tableMeta.getAllColumns());
        partByAstParam.setTblType(PartitionTableType.PARTITION_TABLE);
        partByAstParam.setEc(ec);
        partByAstParam.setBuildSubPartBy(false);
        partByAstParam.setContainNextLevelPartSpec(containNextLevelPartSpec);
        partByAstParam.setTtlTemporary(false);
        partByAstParam.setLocality(localityDesc);
        PartitionByDefinition partByDef =
            PartitionInfoBuilder.buildCompletePartByDefByAstParams(partByAstParam, sqlPartitionBy);
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
        partitionInfo.setPartitionBy(partByDef);
        partitionInfo.getPartitionBy().setSubPartitionBy(subPartByDef);

        partitionInfo.setMetaVersion(1L);
        partitionInfo.setSpTemplateFlag(subPartByDef == null ? TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED :
            subPartByDef.isUseSubPartTemplate() ? TablePartitionRecord.SUBPARTITION_TEMPLATE_USING :
                TablePartitionRecord.SUBPARTITION_TEMPLATE_UNUSED);
        partitionInfo.setTableGroupId(TableGroupRecord.INVALID_TABLE_GROUP_ID);
        partitionInfo.setTableName(tableName);
        partitionInfo.setTableSchema(schemaName);
        partitionInfo.setAutoFlag(TablePartitionRecord.PARTITION_AUTO_BALANCE_DISABLE);

        if (partByAstParam.isNoPartitionKeyTable()) {
            partFlags |= TablePartitionRecord.FLAG_NO_PARTITION_KEY_TABLE;
        }

        partitionInfo.setPartFlags(partFlags);
        partitionInfo.setTableType(PartitionTableType.PARTITION_TABLE);

        //PartitionInfoUtil.generateTableNamePattern(partitionInfo, tableName);
        //PartitionInfoUtil.generatePartitionLocation(partitionInfo, null, false, null, ec, localityDesc);
        //partitionInfo.initPartSpecSearcher();

        return partitionInfo;
    }

    private static PartitionInfo mockListRangePartitionInfo(ExecutionContext ec, RexBuilder rexBuilder,
                                                            TableMeta tableMeta, ColumnMeta ttlColumnMeta,
                                                            String ttlQueryBoundary) {
        String ttlQueryBoundaryInSql = ttlQueryBoundary;
        DataType dataType = ttlColumnMeta.getDataType();
        if (!DataTypeUtil.isNumberSqlType(dataType)) {
            ttlQueryBoundaryInSql = "'" + ttlQueryBoundary + "'";
        }
        String partitionBy = String.format(
            "PARTITION BY LIST(%s)\n"
                + "SUBPARTITION BY RANGE(%s)\n" +
                "(\n" +
                "   PARTITION p1 VALUES IN (%s) (\n" +
                "       SUBPARTITION sp1 VALUES LESS THAN(MAXVALUE)\n" +
                "   ),\n" +
                "   PARTITION p2 VALUES IN (DEFAULT) (\n" +
                "       SUBPARTITION sp2 VALUES LESS THAN(%s),\n" +
                "       SUBPARTITION sp3 VALUES LESS THAN(MAXVALUE)\n" +
                "   )\n" +
                ")",
            ttlColumnMeta.getOriginColumnName(), ttlColumnMeta.getOriginColumnName(), ttlQueryBoundaryInSql,
            ttlQueryBoundaryInSql);
        MySqlCreateTableParser parser = new MySqlCreateTableParser(ByteString.from(partitionBy));
        SQLPartitionBy SQLPartitionBy = parser.parsePartitionBy();
        FastSqlToCalciteNodeVisitor fastSqlToCalciteNodeVisitor = new FastSqlToCalciteNodeVisitor(null, null);
        SQLPartitionBy.accept(fastSqlToCalciteNodeVisitor);
        SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) fastSqlToCalciteNodeVisitor.getSqlNode();

        Map<SqlNode, RexNode> boundExprInfo = new HashMap<>();
        for (SqlNode sqlPartitionNode : sqlPartitionBy.getPartitions()) {
            SqlPartition sqlPartition = (SqlPartition) sqlPartitionNode;
            SqlNode inValue = sqlPartition.getValues().getItems().get(0).getValue();
            if (inValue instanceof SqlLiteral) {
                RexNode inValueRexNode =
                    rexBuilder.makeLiteral(DataTypes.StringType.convertFrom(((SqlLiteral) inValue).toValue()));
                boundExprInfo.put(inValue, inValueRexNode);
            } else {
                boundExprInfo.put(inValue, rexBuilder.makeLiteral("DEFAULT"));
            }

            if (sqlPartition.getSubPartitions().size() > 0) {
                for (SqlNode sqlSubPartitionNode : sqlPartition.getSubPartitions()) {
                    SqlSubPartition sqlSubPartition = (SqlSubPartition) sqlSubPartitionNode;
                    SqlNode lessThanValue = sqlSubPartition.getValues().getLessThanValue();
                    if (lessThanValue instanceof SqlLiteral) {
                        RexNode lessThanValueRexNode = rexBuilder.makeLiteral(
                            DataTypes.StringType.convertFrom(((SqlLiteral) lessThanValue).toValue()));
                        boundExprInfo.put(lessThanValue, lessThanValueRexNode);
                    } else {
                        boundExprInfo.put(lessThanValue, rexBuilder.makeLiteral("MAXVALUE"));
                    }
                }
            }
        }

        return mockPartitionInfo(sqlPartitionBy, tableMeta, ec, boundExprInfo);
    }

    public Map<Pair<String, String>, TtlQueryType> getTtlQueryTypeMap() {
        return ttlQueryTypeMap;
    }
}

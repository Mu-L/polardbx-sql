package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCheckTableRouting;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * CHECK TABLE ROUTING
 *
 * @author chenghui.lch
 */
public class LogicalCheckTableRoutingHandler extends HandlerCommon {

    public LogicalCheckTableRoutingHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlCheckTableRouting checkTableRouting = (SqlCheckTableRouting) dal.getNativeSqlNode();
        boolean isExplain = checkTableRouting.getExplain();
        boolean checkIndexRouting = checkTableRouting.getCheckIndexRouting();
        SqlIdentifier indexNameAst = (SqlIdentifier) checkTableRouting.getIndexName();
        SqlIdentifier fullTableNameAst = (SqlIdentifier) checkTableRouting.getTableName();
        SqlNodeList partitionNamesAst = checkTableRouting.getPartitions();
        String tarTableName = fullTableNameAst.getLastName();
        String tarTableSchema = executionContext.getSchemaName();
        if (fullTableNameAst.names.size() == 2) {
            tarTableSchema = fullTableNameAst.names.get(0);
        }
        tarTableName = SQLUtils.normalizeNoTrim(tarTableName);
        tarTableSchema = SQLUtils.normalizeNoTrim(tarTableSchema);
        List<String> tarPartitionNames = new ArrayList<>();
        if (partitionNamesAst != null) {
            List<SqlNode> partNameAstList = partitionNamesAst.getList();
            for (int i = 0; i < partNameAstList.size(); i++) {
                SqlIdentifier partNameAst = (SqlIdentifier) partitionNamesAst.get(i);
                String partNameStr = SQLUtils.normalizeNoTrim(partNameAst.getLastName());
                tarPartitionNames.add(partNameStr);
            }
        }
        final String schemaName = tarTableSchema;

        String tarIndexNameStr = null;
        String tarIndexTableNameStr = null;
        if (checkIndexRouting && indexNameAst != null) {
            tarIndexNameStr = SQLUtils.normalizeNoTrim(indexNameAst.getLastName());
            TableMeta tblMeta = executionContext.getSchemaManager(tarTableSchema).getTable(tarTableName);
            GsiMetaManager.GsiIndexMetaBean gsiMeta = tblMeta.findGlobalSecondaryIndexByName(tarIndexNameStr);
            tarIndexTableNameStr = gsiMeta.indexTableName;
            tarTableName = tarIndexTableNameStr;
        }

        final String checkRoutingQuerySql =
            buildCheckTableRoutingQuerySql(tarTableSchema, tarTableName, tarPartitionNames, executionContext);

        if (isExplain) {
            return handleForExplain(checkRoutingQuerySql, executionContext);
        }

        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        List<Map<String, Object>> checkRoutingQueryRs = new ArrayList<>();
        TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            schemaName,
            sessionVariables,
            (transConn) -> {
                List<Map<String, Object>> queryRs = TtlJobUtil.execLogicalQueryOnInnerConnection(
                    serverConfigManager,
                    schemaName,
                    transConn,
                    executionContext,
                    checkRoutingQuerySql);
                checkRoutingQueryRs.addAll(queryRs);
                return checkRoutingQueryRs;
            }
        );

        // generate a cursor
        List<String> resultSetColNameList = new ArrayList<>();
        ArrayResultCursor result =
            buildCheckTableRoutingCursorMeta(tarTableSchema, tarTableName, executionContext, resultSetColNameList);
        for (int i = 0; i < checkRoutingQueryRs.size(); i++) {
            Map<String, Object> oneRowRs = checkRoutingQueryRs.get(i);
            Object[] oneRowVal = new Object[resultSetColNameList.size()];
            for (int j = 0; j < resultSetColNameList.size(); j++) {
                String col = resultSetColNameList.get(j);
                Object val = oneRowRs.get(col);
                oneRowVal[j] = val;
            }
            result.addRow(oneRowVal);
        }

        return result;
    }

    protected Cursor handleForExplain(String finalQuerySql, ExecutionContext executionContext) {
        // generate a cursor
        ArrayResultCursor result = new ArrayResultCursor("checkTableRouting");
        result.addColumn("check_table_routing", DataTypes.StringType);
        result.addRow(new Object[] {finalQuerySql});
        return result;

    }

    protected ArrayResultCursor buildCheckTableRoutingCursorMeta(String tarTableSchema,
                                                                 String tarTableName,
                                                                 ExecutionContext ec,
                                                                 List<String> resultSetColNameList) {
        TableMeta tblMeta = ec.getSchemaManager(tarTableSchema).getTable(tarTableName);
        PartitionInfo partInfo = tblMeta.getPartitionInfo();
        List<String> partColNames = partInfo.getPartitionColumns();

        resultSetColNameList.add("route_part");
        resultSetColNameList.add("real_part");
        resultSetColNameList.addAll(partColNames);

        // generate a cursor
        ArrayResultCursor result = new ArrayResultCursor("checkTableRouting");
        for (int i = 0; i < resultSetColNameList.size(); i++) {
            result.addColumn(resultSetColNameList.get(i), DataTypes.StringType);
        }
//        result.init();
        return result;
    }

    protected String buildCheckTableRoutingQuerySql(String tarTableSchema,
                                                    String tarPrimOrGsiTableName,
                                                    List<String> partitionNames,
                                                    ExecutionContext ec) {
        TableMeta tblMeta = ec.getSchemaManager(tarTableSchema).getTable(tarPrimOrGsiTableName);
        PartitionInfo partInfo = tblMeta.getPartitionInfo();
        List<PartitionSpec> phyPartSpecList = partInfo.getPartitionBy().getPhysicalPartitions();
        if (partitionNames != null && !partitionNames.isEmpty()) {
            phyPartSpecList = partInfo.getPhysicalPartitionSpecsByPartitionNames(partitionNames);
        }
        List<String> checkRouteQuerySqlList = new ArrayList<>();

        String allLevelPartColStrList = "";
        boolean useSubPartBy = partInfo.getPartitionBy().getSubPartitionBy() != null;
        List<List<String>> allLevelPartCols = partInfo.getAllLevelFullPartCols();
        List<String> partColNames = allLevelPartCols.get(0);
        String partColNameStrList = String.join(" ,", partColNames);

        List<String> subPartColNames = new ArrayList<>();
        String subpartColNameStrList = null;
        allLevelPartColStrList = partColNameStrList;
        if (useSubPartBy) {
            subPartColNames = allLevelPartCols.get(1);
            subpartColNameStrList = String.join(" ,", subPartColNames);
            allLevelPartColStrList += " ," + subpartColNameStrList;
        }
        allLevelPartColStrList = allLevelPartColStrList.toLowerCase();
        String routingTupleTableSchema = tarTableSchema;
        String routingTupleTableName = tarPrimOrGsiTableName;

        String routingTableMeta = ec.getParamManager().getString(ConnectionParams.CHECK_ROUTING_TABLE_META);
        if (!StringUtils.isEmpty(routingTableMeta)) {
            String[] routingDbTbName = routingTableMeta.split("\\.");
            if (routingDbTbName.length == 1) {
                routingTupleTableSchema = ec.getSchemaName();
                routingTupleTableName = SQLUtils.normalizeNoTrim(routingDbTbName[0]);
            } else if (routingDbTbName.length == 2) {
                routingTupleTableSchema = SQLUtils.normalizeNoTrim(routingDbTbName[0]);
                routingTupleTableName = SQLUtils.normalizeNoTrim(routingDbTbName[1]);
            }
        }

        String checkQueryHint = "/*+TDDL:cmd_extra(PART_ROUTE_IGNORE_EXCEPTION=TRUE)*/";
        String checkRoutingQueryTemp =
            "select route_part, real_part, %s from (select part_route('%s','%s', %s) as route_part, '%s' as real_part, %s from `%s`.`%s` partition(`%s`)) tmp_%s where route_part != '%s'";

        for (int i = 0; i < phyPartSpecList.size(); i++) {
            PartitionSpec tarPhyPart = phyPartSpecList.get(i);
            String tarPartPhyName = tarPhyPart.getName();
            String querySql = String.format(
                checkRoutingQueryTemp,
                allLevelPartColStrList,
                routingTupleTableSchema,
                routingTupleTableName,
                allLevelPartColStrList,
                tarPartPhyName,
                allLevelPartColStrList,
                tarTableSchema,
                tarPrimOrGsiTableName,
                tarPartPhyName,
                tarPartPhyName,
                tarPartPhyName
            );
            checkRouteQuerySqlList.add(querySql);
        }

        String finalQuerySql = "";
        for (int i = 0; i < checkRouteQuerySqlList.size(); i++) {
            String querySql = checkRouteQuerySqlList.get(i);
            if (i > 0) {
                finalQuerySql += String.format("\n union ( %s )", querySql);
            } else {
                finalQuerySql = querySql;
            }
        }
        finalQuerySql = checkQueryHint + finalQuerySql;
        return finalQuerySql;
    }
}

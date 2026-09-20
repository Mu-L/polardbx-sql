package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableProperties;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableDetail;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.rule.TableRule;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTablePropertiesHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaTablePropertiesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableProperties;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        schemaNames.addAll(OptimizerContext.getActiveSchemaNames());

        final int schemaIndex = InformationSchemaTableDetail.getTableSchemaIndex();
        final int tableIndex = InformationSchemaTableDetail.getTableNameIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        schemaNames = virtualView.applyFilters(schemaIndex, params, schemaNames);

        // tableIndex
        Set<String> indexTableNames = virtualView.getEqualsFilterValues(tableIndex, params);

        for (String tableSchemaName : schemaNames) {
            boolean isNewPartition = DbInfoManager.getInstance().isNewPartitionDb(tableSchemaName);
            TddlRuleManager tddlRuleManager = OptimizerContext.getContext(tableSchemaName).getRuleManager();
            Collection<TableMeta> tableMetas = executionContext.getSchemaManager(tableSchemaName).getAllTables();
            for (TableMeta tableMeta : tableMetas) {
                if("dual".equalsIgnoreCase(tableMeta.getTableName())) {
                    continue;
                }
                if (GeneralUtil.isNotEmpty(indexTableNames) && !indexTableNames.contains(tableMeta.getTableName())) {
                    continue;
                }
                if (isNewPartition) {
                    PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
                    if (partitionInfo != null && partitionInfo.isBlockFullTableScan()) {
                        cursor.addRow(new Object[] {tableSchemaName, tableMeta.getTableName(), Boolean.FALSE});
                    } else {
                        cursor.addRow(new Object[] {tableSchemaName, tableMeta.getTableName(), Boolean.TRUE});
                    }
                } else {
                    TableRule tableRule = tddlRuleManager.getTableRule(tableMeta.getTableName());
                    if (tableRule != null && !tableRule.isAllowFullTableScan()) {
                        cursor.addRow(new Object[] {tableSchemaName, tableMeta.getTableName(), Boolean.FALSE});
                    } else {
                        cursor.addRow(new Object[] {tableSchemaName, tableMeta.getTableName(), Boolean.TRUE});
                    }
                }
            }
        }
        return cursor;
    }
}

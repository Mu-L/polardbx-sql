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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.GatherCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

/**
 * @author roy
 */
public class LogicalExplainHandler extends LogicalViewHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalExplainHandler.class);

    public LogicalExplainHandler(IRepository repo) {
        super();
        this.repo = repo;
    }

    public Cursor newHandle(RelNode logicalPlan, ExecutionContext executionContext) {
        List<Cursor> inputCursors = new ArrayList<>();

        if (logicalPlan instanceof LogicalView) {

            List<RexDynamicParam> scalarList = ((LogicalView) logicalPlan).getScalarList();
            if (scalarList != null && scalarList.size() > 0) {
                throw new NotSupportException("explain execute for apply subquery");
            }
        }

        QueryConcurrencyPolicy queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;
        boolean isDelete = false;
        List<RelNode> inputs = new ArrayList<>();
        if (logicalPlan instanceof LogicalModifyView) {
            inputs.addAll(((LogicalModifyView) logicalPlan).getInput(executionContext));
            isDelete = ((LogicalModifyView) logicalPlan).getTableModify().isDelete();
        } else if (logicalPlan instanceof LogicalView) {
            //be same with LogicalViewHandler
            PhyTableOperationUtil.enableIntraGroupParallelism(((LogicalView) logicalPlan).getSchemaName(),
                executionContext);
            inputs.addAll(ExecUtils.getInputs((LogicalView) logicalPlan, executionContext, false));
        } else if (logicalPlan instanceof PhyTableOperation) {
            if (((PhyTableOperation) logicalPlan).getKind().belongsTo(SqlKind.DML)) {
                ((PhyTableOperation) logicalPlan).setPhyOperationBuilder(null);
            }
            ((PhyTableOperation) logicalPlan).setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else if (logicalPlan instanceof DirectTableOperation || logicalPlan instanceof DirectMultiDBTableOperation) {
            ((DirectTableOperation) logicalPlan).setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else if (logicalPlan instanceof SingleTableOperation) {
            SingleTableOperation operation = (SingleTableOperation) logicalPlan;
            if (operation.getKind() == SqlKind.REPLACE || operation.getKind() == SqlKind.INSERT) {
                throw new NotSupportException("explain execute insert or ddl ");
            }
            operation.setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else {
            throw new NotSupportException(" explain execute " + logicalPlan.getClass().getSimpleName());
        }

        /**
         * explain only execute one for every logical view.
         */
        String schema = ((LogicalView) logicalPlan).getSchemaName();
        if (StringUtils.isEmpty(schema)) {
            schema = executionContext.getSchemaName();
        }

        int explainExecutePhyTbLevel =
            executionContext.getParamManager().getInt(ConnectionParams.EXPLAIN_EXECUTE_PHYTB_LEVEL);
        String explainExecutePhyTbPattern =
            executionContext.getParamManager().getString(ConnectionParams.EXPLAIN_EXECUTE_PHYTB_PATTERN);
        if (explainExecutePhyTbLevel < 0 && explainExecutePhyTbPattern == null) {
            executeWithConcurrentPolicy(executionContext,
                Lists.newArrayList(inputs.get(0)),
                queryConcurrencyPolicy,
                inputCursors,
                schema);
            if (inputCursors.size() == 1) {
                return inputCursors.get(0);
            } else {
                return new GatherCursor(inputCursors, executionContext);
            }
        } else {
            inputs = ExecUtils.filterByExplainExecutePhyTbPattern(executionContext, inputs);
            //并发执行explain execute
            inputs = executeSubNodesBlockConcurrent(executionContext, inputs, inputCursors, schema);
            List<Pair<String, List<List<String>>>> phyTableNames = new ArrayList<>();
            Set<String> logicalTableNames = new HashSet<>();
            for (RelNode input : inputs) {
                if (input instanceof PhyTableOperation) {
                    phyTableNames.add(
                        Pair.of(((PhyTableOperation) input).getDbIndex(), ((PhyTableOperation) input).getTableNames()));
                    logicalTableNames.addAll(((PhyTableOperation) input).getLogicalTableNames());
                } else {
                    throw new UnsupportedOperationException("explain error");
                }
            }

            return distinctResult(schema, logicalTableNames, phyTableNames, inputCursors,
                explainExecutePhyTbPattern == null ? explainExecutePhyTbLevel : 2,
                isDelete);
        }

    }

    /**
     * explain execute返回结果进行处理：
     * - 将逻辑表名替换为物理表名
     * - 找出使用索引不同的物理表
     * - 找出所有物理表扫描行数的极值
     * <p>
     * explain execute下发的物理sql必须保证与真实执行时一模一样，展示的执行计划才具有意义。
     * LogicalView执行时：
     * 1. 首先按照Group对所有物理表进行分组
     * 2. 然后对于每个Group，会按照unionOptHelper的逻辑，对单个Group下的物理表进行再分组union
     * 3. 最后单个Group下的分组union会创建为PhyTableOperation对象
     * <p>
     * unionSize = 2
     * LogicalView ->
     * PhyTableOperation(group_1_phyTb_1 union all group_1_phyTb_2)
     * PhyTableOperation(group_1_phyTb_3 union all group_1_phyTb_4)
     * PhyTableOperation(group_2_phyTb_1 union all group_2_phyTb_2)
     * PhyTableOperation(group_2_phyTb_3 union all group_2_phyTb_4)
     * <p>
     * 物理sql执行计划 ：explain group_1_phyTb_1 union all group_1_phyTb_2
     * 1. Mysql执行计划中一张表只会出现一次/行，即一张表只有一种access type
     * 2. Mysql执行计划中不会改变union 的顺序, 参考Mysql代码：Query_expression::explain_query_term
     * <p>
     * 有了上述保证，才能正确将explain execute结果中的逻辑表名替换为物理表名
     *
     * @param phyTableNames 三层List，第一层表示不同PhyTableOperation，第二层表示同一个PhyTableOperation下的多个分区（union分组），第三层表示同一个分区下的多个物理表（join下推）
     */
    private Cursor distinctResult(String schema, Set<String> logicalTableNames,
                                  List<Pair<String, List<List<String>>>> phyTableNames,
                                  List<Cursor> cursors,
                                  int explainExecutePhyTbLevel, boolean isDelete) {
        if (cursors.size() == 0) {
            return null;
        }
        try {
            ArrayResultCursor explainResultCursor = new ArrayResultCursor("EXPLAIN");
            Cursor firstCursor = cursors.get(0);
            Pair<CursorMeta, List<Object[]>> firstPair = getAllRowsAndCursorMeta(firstCursor);
            CursorMeta firstCursorMeta = firstPair.getKey();
            List<Object[]> firstCursorRows = firstPair.getValue();

            if (firstCursorRows.size() == 0) {
                return null;
            }

            int colCount = firstCursorMeta.getColumns().size();
            explainResultCursor.setCursorMeta(firstCursorMeta);

            int keyColIndex = -1;
            int extraColIndex = -1;
            int tableColIndex = -1;
            int rowsColIndex = -1;

            for (int i = 0; i < colCount; i++) {
                switch (firstCursorMeta.getColumns().get(i).getName().toLowerCase()) {
                case "key":
                    keyColIndex = i;
                    break;
                case "extra":
                    extraColIndex = i;
                    break;
                case "table":
                    tableColIndex = i;
                    break;
                case "rows":
                    rowsColIndex = i;
                    break;

                }
            }
            if (keyColIndex < 0 || extraColIndex < 0 || tableColIndex < 0 || rowsColIndex < 0) {
                return null;
            }

            //结果去重
            Map<String, Map<String, Set<String>>> phyTableNameSetMap = new HashMap<>();
            Map<String, Pair<String, Integer>> minScanRowMap = new HashMap<>();
            Map<String, Pair<String, Integer>> maxScanRowMap = new HashMap<>();
            Map<String, String> scanIndexMap = new HashMap<>();
            Map<String, List<String>> diffPlanPhyTables = new HashMap<>();
            Set<Integer> diffPlanCursorIndex = new HashSet<>();
            List<List<Object[]>> allRows = new ArrayList<>(cursors.size());
            for (int i = 0; i < cursors.size(); i++) {
                List<Object[]> cursorRows =
                    i == 0 ? firstCursorRows : getAllRowsAndCursorMeta(cursors.get(i)).getValue();
                allRows.add(cursorRows);
                String cursorDbIndex = phyTableNames.get(i).getKey();
                List<List<String>> cursorPhyTableNames = phyTableNames.get(i).getValue();
                int j = 0;
                List<String> partitionPhyTableNames = new LinkedList<>(cursorPhyTableNames.get(j));
                for (Object[] row : cursorRows) {

                    if (row[tableColIndex] == null) {
                        continue;
                    }

                    String logicalTableName = null;
                    String phyTableName = null;
                    if (isDelete && logicalTableNames.size() == 1) {
                        //单表删除, explain execute返回的table列就是物理表名
                        logicalTableName = logicalTableNames.iterator().next();
                        phyTableName = row[tableColIndex].toString();
                    } else {
                        if (partitionPhyTableNames.isEmpty()) {
                            partitionPhyTableNames = new ArrayList<>(cursorPhyTableNames.get(++j));
                        }

                        //缓存逻辑表的拓扑结构
                        logicalTableName = row[tableColIndex].toString();
                        if (logicalTableNames.contains(logicalTableName)) {
                            phyTableNameSetMap.computeIfAbsent(logicalTableName,
                                k -> PartitionInfoUtil.getTableTopologySet(schema, k));
                        } else {
                            //逻辑表alias：TddlRelToSqlConverter.visit(TableScan)时会调用SqlValidatorUtil#uniquify，为相同的tableName生成alias
                            //如果table列不是逻辑表名，说明其肯定是别名或者<unionM,N>/<derivedN>/<subqueryN>其他情况
                            if (logicalTableName.startsWith("<") && logicalTableName.endsWith(">")) {
                                continue;
                            }
                            boolean isAlias = false;
                            for (String realLogicalTableName : logicalTableNames) {
                                if (logicalTableName.startsWith(realLogicalTableName)) {
                                    isAlias = true;
                                    phyTableNameSetMap.computeIfAbsent(logicalTableName,
                                        k -> PartitionInfoUtil.getTableTopologySet(schema, realLogicalTableName));
                                    break;
                                }
                            }

                            if (!isAlias) {
                                continue;
                            }
                        }
                        //找到对应的无表名
                        phyTableName = popPhyTableName(phyTableNameSetMap.get(logicalTableName).get(cursorDbIndex),
                            partitionPhyTableNames);
                    }

                    if (logicalTableName == null || phyTableName == null) {
                        throw new RuntimeException("explain execute error");
                    }

                    //分库分表需要增加groupName保持唯一性
                    final String phyTableId = DbInfoManager.getInstance().isNewPartitionDb(schema) ? phyTableName :
                        cursorDbIndex + "." + phyTableName;
                    row[tableColIndex] = Pair.of(logicalTableName, phyTableId);
                    Integer scanRows =
                        row[rowsColIndex] == null ? null : Integer.parseInt(row[rowsColIndex].toString());
                    String scanIndex = row[keyColIndex] == null ? null : row[keyColIndex].toString();
                    minScanRowMap.computeIfAbsent(logicalTableName, k -> Pair.of(phyTableId, scanRows));
                    maxScanRowMap.computeIfAbsent(logicalTableName, k -> Pair.of(phyTableId, scanRows));
                    if (scanIndexMap.isEmpty()) {
                        scanIndexMap.put(logicalTableName, scanIndex);
                    }
                    diffPlanPhyTables.computeIfAbsent(logicalTableName, k -> {
                        List<String> list = new ArrayList<>();
                        list.add(phyTableId);
                        return list;
                    });

                    if (maxScanRowMap.get(logicalTableName).getValue() == null ||
                        (scanRows != null && scanRows > maxScanRowMap.get(logicalTableName).getValue())) {
                        maxScanRowMap.put(logicalTableName, Pair.of(phyTableId, scanRows));
                    }
                    if (minScanRowMap.get(logicalTableName).getValue() == null ||
                        (scanRows != null && scanRows < minScanRowMap.get(logicalTableName).getValue())) {
                        minScanRowMap.put(logicalTableName, Pair.of(phyTableId, scanRows));
                    }

                    if (!Objects.equals(scanIndexMap.get(logicalTableName), scanIndex)) {
                        diffPlanPhyTables.get(logicalTableName).add(phyTableId);
                        diffPlanCursorIndex.add(i);
                    }
                }
            }

            //输出结果
            for (int i = 0; i < allRows.size(); i++) {
                List<Object[]> cursorRows = allRows.get(i);
                if (i == 0) {
                    for (Object[] row : cursorRows) {
                        if (row[tableColIndex] instanceof Pair) {
                            String logicalTableName = (String) ((Pair) row[tableColIndex]).getKey();
                            String phyTableName = (String) ((Pair) row[tableColIndex]).getValue();
                            row[extraColIndex] = "Scan rows(" + minScanRowMap.get(logicalTableName).getValue() + ", "
                                + maxScanRowMap.get(logicalTableName).getValue() + "); "
                                + Optional.ofNullable(row[extraColIndex]).orElse("");
                            if (diffPlanPhyTables.get(logicalTableName).size() > 1) {
                                StringJoiner diffPlanPhyTablesStr = new StringJoiner(",");
                                for (String diffPlanPhyTable : diffPlanPhyTables.get(logicalTableName)) {
                                    diffPlanPhyTablesStr.add(diffPlanPhyTable);
                                }
                                row[extraColIndex] =
                                    "Different plan(" + diffPlanPhyTablesStr.toString() + "); " + Optional.ofNullable(
                                        row[extraColIndex]).orElse("");
                            }
                            row[tableColIndex] = explainExecutePhyTbLevel == 0 ? logicalTableName : phyTableName;
                        }
                        explainResultCursor.addRow(row);
                    }
                } else {
                    if (explainExecutePhyTbLevel == 0) {
                        break;
                    }
                    if (explainExecutePhyTbLevel == 2 || (explainExecutePhyTbLevel == 1 && diffPlanCursorIndex.contains(
                        i))) {
                        for (Object[] row : cursorRows) {
                            if (row[tableColIndex] instanceof Pair) {
                                String logicalTableName = (String) ((Pair) row[tableColIndex]).getKey();
                                String phyTableName = (String) ((Pair) row[tableColIndex]).getValue();
                                row[tableColIndex] = phyTableName;
                            }
                            explainResultCursor.addRow(row);
                        }
                    }
                }
            }

            return explainResultCursor;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private String popPhyTableName(Set<String> phyTableNameSet, List<String> candidatePhyTableNames) {
        Iterator<String> iterator = candidatePhyTableNames.iterator();
        while (iterator.hasNext()) {
            String phyTableName = iterator.next();
            if (phyTableNameSet.contains(phyTableName)) {
                iterator.remove();
                return phyTableName;
            }
        }
        return null;
    }

    private Pair<CursorMeta, List<Object[]>> getAllRowsAndCursorMeta(Cursor cursor)
        throws SQLException, UnsupportedEncodingException {
        List<Object[]> cursorRows = new ArrayList<>();
        CursorMeta cursorMeta = null;
        Row row;
        while ((row = cursor.next()) != null) {
            if (cursorMeta == null) {
                cursorMeta = CursorMeta.build(ExplainExecutorUtil.getRowMetas(row, "explain"));
            }
            row.setCursorMeta(cursorMeta);
            cursorRows.add(row.getValues().toArray(new Object[0]));
        }
        cursor.close(new ArrayList<>());
        return Pair.of(cursorMeta, cursorRows);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_NEW_EXPLAIN_EXECUTE)) {
            return newHandle(logicalPlan, executionContext);
        }

        List<Cursor> inputCursors = new ArrayList<>();

        if (logicalPlan instanceof LogicalView) {

            List<RexDynamicParam> scalarList = ((LogicalView) logicalPlan).getScalarList();
            if (scalarList != null && scalarList.size() > 0) {
                throw new NotSupportException("explain execute for apply subquery");
            }
        }

        QueryConcurrencyPolicy queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;

        RelNode input;
        if (logicalPlan.getClass().getSimpleName().equals("LogicalView") || logicalPlan.getClass().getSimpleName()
            .equals("LogicalIndexScan")) {
            input = ExecUtils.getInputs((LogicalView) logicalPlan, executionContext, false).get(0);
        } else if (logicalPlan instanceof LogicalModifyView) {
            input = ((LogicalModifyView) logicalPlan).getInput(executionContext).get(0);
        } else if (logicalPlan instanceof PhyTableOperation) {
            if (((PhyTableOperation) logicalPlan).getKind().belongsTo(SqlKind.DML)) {
                ((PhyTableOperation) logicalPlan).setPhyOperationBuilder(null);
            }
            ((PhyTableOperation) logicalPlan).setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else if (logicalPlan instanceof DirectTableOperation || logicalPlan instanceof DirectMultiDBTableOperation) {
            ((DirectTableOperation) logicalPlan).setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else if (logicalPlan instanceof SingleTableOperation) {
            SingleTableOperation operation = (SingleTableOperation) logicalPlan;
            if (operation.getKind() == SqlKind.REPLACE || operation.getKind() == SqlKind.INSERT) {
                throw new NotSupportException("explain execute insert or ddl ");
            }
            operation.setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else {
            throw new NotSupportException(" explain execute " + logicalPlan.getClass().getSimpleName());
        }

        /**
         * explain only execute one for every logical view.
         */
        String schema = ((LogicalView) logicalPlan).getSchemaName();
        if (StringUtils.isEmpty(schema)) {
            schema = executionContext.getSchemaName();
        }
        executeWithConcurrentPolicy(executionContext,
            Lists.newArrayList(input),
            queryConcurrencyPolicy,
            inputCursors,
            schema);

        if (inputCursors.size() == 1) {
            return inputCursors.get(0);
        } else {
            return new GatherCursor(inputCursors, executionContext);
        }
    }
}

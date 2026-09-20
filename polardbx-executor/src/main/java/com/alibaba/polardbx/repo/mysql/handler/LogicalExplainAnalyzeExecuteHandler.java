package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.*;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LogicalExplainAnalyzeExecuteHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalExplainAnalyzeExecuteHandler.class);

    public LogicalExplainAnalyzeExecuteHandler(IRepository repo) {
        super();
        this.repo = repo;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        List<Cursor> inputCursors = new ArrayList<>();

        if (logicalPlan instanceof LogicalView) {

            List<RexDynamicParam> scalarList = ((LogicalView) logicalPlan).getScalarList();
            if (scalarList != null && scalarList.size() > 0) {
                throw new NotSupportException("explain execute for apply subquery");
            }
        }

        if (logicalPlan instanceof BaseQueryOperation && ((BaseQueryOperation) logicalPlan).getKind()
            .belongsTo(SqlKind.DML)) {
            throw new NotSupportException("explain analyze execute dml");
        }

        List<RelNode> inputs = new ArrayList<>();
        if (logicalPlan instanceof LogicalView) {
            //be same with LogicalViewHandler
            PhyTableOperationUtil.enableIntraGroupParallelism(((LogicalView) logicalPlan).getSchemaName(),
                executionContext);
            inputs.addAll(ExecUtils.getInputs((LogicalView) logicalPlan, executionContext, false));
        } else if (logicalPlan instanceof PhyTableOperation) {
            inputs.add(logicalPlan);
            inputCursors.add(repo.getCursorFactory().repoCursor(executionContext, logicalPlan));
        } else if (logicalPlan instanceof DirectTableOperation || logicalPlan instanceof DirectMultiDBTableOperation) {
            //如果sql中所有表均为单库单表， 则会直接生成DirectTableOperation/DirectMultiDBTableOperation
            inputs.add(logicalPlan);
            inputCursors.add(repo.getCursorFactory().repoCursor(executionContext, logicalPlan));
        } else if (logicalPlan instanceof SingleTableOperation) {
            //impossible case, 根据代码逻辑来看，现在都会被转换为DirectTableOperation
            //单库单表，则会将LogicalView转换为SingleTableOperation
            inputs.add(logicalPlan);
            inputCursors.add(repo.getCursorFactory().repoCursor(executionContext, logicalPlan));
        } else if (logicalPlan instanceof DirectShardingKeyTableOperation) {
            //分库分表情况下，命中分区键的点查
            inputs.add(logicalPlan);
            inputCursors.add(repo.getCursorFactory().repoCursor(executionContext, logicalPlan));
        } else {
            throw new NotSupportException("explain analyze execute " + logicalPlan.getClass().getSimpleName());
        }

        String schema = logicalPlan instanceof LogicalView ? ((LogicalView) logicalPlan).getSchemaName() : null;
        if (StringUtils.isEmpty(schema)) {
            schema = executionContext.getSchemaName();
        }

        if (inputCursors.isEmpty()) {
            if (executionContext.getParamManager().getString(ConnectionParams.EXPLAIN_EXECUTE_PHYTB_PATTERN) != null){
                inputs = ExecUtils.filterByExplainExecutePhyTbPattern(executionContext, inputs);
            } else if (executionContext.getParamManager().getInt(ConnectionParams.EXPLAIN_EXECUTE_PHYTB_LEVEL) != 2) {
                inputs = Collections.singletonList(inputs.get(0));
            }

            inputs = executeSubNodesBlockConcurrent(executionContext,
                inputs,
                inputCursors,
                schema);
        }

        ArrayResultCursor resultCursor = new ArrayResultCursor("EXPLAIN_ANALYZE_EXECUTE");
        resultCursor.addColumn("table", DataTypes.StringType);
        resultCursor.addColumn("physical_plan", DataTypes.StringType);
        for (int i = 0; i < inputCursors.size(); i++) {
            Cursor cursor = inputCursors.get(i);
            RelNode relNode = inputs.get(i);
            Pair<String, List<List<String>>> pair =
                ExecUtils.getGroupAndPhyTableNames((BaseTableOperation) relNode, executionContext);
            List<List<String>> tableNames = pair.getValue();
            String groupName = pair.getKey();
            Row row = cursor.next();
            String physicalPlan = DataTypes.StringType.convertFrom(row.getObject(row.getColNum() - 1));
            resultCursor.addRow(
                new Object[] {createPhysicalTableId(schema, groupName, tableNames) + "\n", physicalPlan});
        }
        return resultCursor;
    }

    private String createPhysicalTableId(String schema, String groupName, List<List<String>> tableNames) {
        StringJoiner stringJoiner = new StringJoiner(",");
        for (List<String> tableName : tableNames) {
            StringJoiner sj = new StringJoiner(",");
            for (String name : tableName) {
                if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                    sj.add(name);
                } else {
                    sj.add(groupName + "." + name);
                }
            }
            stringJoiner.add("[" + sj + "]");
        }
        return "[" + stringJoiner + "]";
    }

}

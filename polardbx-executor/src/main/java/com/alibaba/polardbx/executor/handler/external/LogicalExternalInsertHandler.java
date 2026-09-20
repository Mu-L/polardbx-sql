package com.alibaba.polardbx.executor.handler.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.external.ConnectorRuntime;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalExternalInsert;
import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LogicalExternalInsertHandler extends HandlerCommon {

    public LogicalExternalInsertHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalExternalInsert insert = (LogicalExternalInsert) logicalPlan;
        ExternalTableInsertHandler handler = buildHandler(insert.getTableSink());

        List<List<Object>> rows = collectRows(insert, executionContext);

        int affectedRows = 0;
        try {
            handler.open();
            affectedRows = handler.writeRows(rows);
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                "Failed to write to external table: " + e.getMessage());
        } finally {
            try {
                handler.close();
            } catch (IOException closeEx) {
                // Best-effort close; log and ignore.
            }
        }

        return new AffectRowCursor(affectedRows);
    }

    private static ExternalTableInsertHandler buildHandler(TableSink tableSink) {
        String type = tableSink.connectorType();
        ConnectorDescriptor descriptor = ConnectorRegistry.getInstance().get(type);
        if (!(descriptor instanceof ConnectorRuntime)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, " can't find executor for " + type);
        }
        ConnectorRuntime plugin = (ConnectorRuntime) descriptor;
        return plugin.createInsertHandler(tableSink);
    }

    private static List<List<Object>> collectRows(LogicalExternalInsert insert,
                                                  ExecutionContext executionContext) {
        RelDataType rowType = insert.getInput().getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        Map<Integer, ParameterContext> params =
            executionContext.getParams() != null
                ? executionContext.getParams().getCurrentParameter()
                : Collections.emptyMap();

        List<Object> row = new ArrayList<>(fields.size());
        for (int i = 1; i <= fields.size(); i++) {
            ParameterContext pc = params.get(i);
            row.add(pc != null ? pc.getValue() : null);
        }

        List<List<Object>> rows = new ArrayList<>();
        rows.add(row);
        return rows;
    }
}

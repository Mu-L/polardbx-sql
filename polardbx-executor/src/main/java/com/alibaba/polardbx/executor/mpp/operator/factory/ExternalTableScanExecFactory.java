package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.external.ConnectorRuntime;
import com.alibaba.polardbx.executor.operator.ExternalTableScanExec;
import com.alibaba.polardbx.executor.operator.external.ExternalTableScanHandler;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;

import java.util.List;

public class ExternalTableScanExecFactory extends ExecutorFactory {

    private final ExternalTableScan externalTableScan;

    public ExternalTableScanExecFactory(ExternalTableScan externalTableScan) {
        this.externalTableScan = externalTableScan;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        ExternalTableScanHandler handler = buildHandler(externalTableScan, context);
        List<DataType> dataTypes = CalciteUtils.getTypes(externalTableScan.getRowType());
        ExternalTableScanExec exec = new ExternalTableScanExec(handler, dataTypes, context);
        registerRuntimeStat(exec, externalTableScan, context);
        return exec;
    }

    private static ExternalTableScanHandler buildHandler(ExternalTableScan scan,
                                                         ExecutionContext context) {
        TableSource tableSource = scan.getTableSource();
        String type = tableSource.connectorType();
        ConnectorDescriptor descriptor = ConnectorRegistry.getInstance().get(type);
        if (!(descriptor instanceof ConnectorRuntime)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, " can't find executor for " + type);
        }
        ConnectorRuntime plugin = (ConnectorRuntime) descriptor;
        return plugin.createScanHandler(tableSource, scan, context);
    }
}

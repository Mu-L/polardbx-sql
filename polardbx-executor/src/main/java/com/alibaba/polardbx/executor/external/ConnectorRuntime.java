package com.alibaba.polardbx.executor.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.handler.external.ExternalTableInsertHandler;
import com.alibaba.polardbx.executor.operator.external.ExternalTableScanHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;

public interface ConnectorRuntime extends ConnectorDescriptor {

    default ExternalTableScanHandler createScanHandler(
        TableSource source, ExternalTableScan scan, ExecutionContext context) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
            "Connector '" + type() + "' does not support scan");
    }

    default ExternalTableInsertHandler createInsertHandler(TableSink sink) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
            "Connector '" + type() + "' does not support insert");
    }
}

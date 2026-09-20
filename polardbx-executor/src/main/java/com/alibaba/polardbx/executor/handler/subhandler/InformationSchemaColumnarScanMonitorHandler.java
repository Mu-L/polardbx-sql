package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.operator.scan.ColumnarScanMonitor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaColumnarScanMonitor;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;

public class InformationSchemaColumnarScanMonitorHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaColumnarScanMonitorHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        ColumnarScanMonitor columnarScanMonitor = ColumnarScanMonitor.getInstance();
        List<Object[]> packets = columnarScanMonitor.generatePackets();
        for (Object[] packet : packets) {
            cursor.addRow(packet);
        }
        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaColumnarScanMonitor;
    }
}

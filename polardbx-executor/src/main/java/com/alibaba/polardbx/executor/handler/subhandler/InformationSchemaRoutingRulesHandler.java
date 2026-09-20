package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowRoutingRulesHandler;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaRoutingRules;
import com.alibaba.polardbx.optimizer.view.VirtualView;

public class InformationSchemaRoutingRulesHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaRoutingRulesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        LogicalShowRoutingRulesHandler logicalShowRoutingRulesHandler = new LogicalShowRoutingRulesHandler(null);

        return logicalShowRoutingRulesHandler.handle(executionContext);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaRoutingRules;
    }
}

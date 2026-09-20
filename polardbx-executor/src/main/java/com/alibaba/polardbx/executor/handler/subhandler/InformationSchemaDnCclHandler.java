package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowDnCclHandler;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDnCcl;
import com.alibaba.polardbx.optimizer.view.VirtualView;

/**
 * @author liugaoji
 */
public class InformationSchemaDnCclHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaDnCclHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        LogicalShowDnCclHandler showDnCclHandler = new LogicalShowDnCclHandler(null);
        return showDnCclHandler.handle();
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDnCcl;
    }

}

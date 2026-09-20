package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupAccessor;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaColumnarWarmup;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class InformationSchemaColumnarWarmupHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaColumnarWarmupHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        // query MetaDB columnar_warmup by inst id.
        List<ColumnarWarmupRecord> recordList;
        String instanceId = InstIdUtil.getInstId();
        try (Connection connection = MetaDbUtil.getConnection()) {

            ColumnarWarmupAccessor columnarWarmupAccessor = new ColumnarWarmupAccessor();

            columnarWarmupAccessor.setConnection(connection);

            recordList = columnarWarmupAccessor.queryByInstId(instanceId);

        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }

        // build rows for each record.
        for (ColumnarWarmupRecord record : recordList) {

            Object[] row = new Object[] {
                record.getTaskId(),
                record.getCreateTime(),
                record.getUpdateTime(),
                record.getInstanceId(),
                record.getSchemaName(),
                record.getCronExpression(),
                record.getSqlDef(),
                record.getStatus()
            };

            cursor.addRow(row);
        }

        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaColumnarWarmup;
    }
}

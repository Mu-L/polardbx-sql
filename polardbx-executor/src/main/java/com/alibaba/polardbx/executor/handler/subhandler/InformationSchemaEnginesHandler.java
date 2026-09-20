package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.EnginesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaEngines;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.util.List;

public class InformationSchemaEnginesHandler extends BaseVirtualViewSubClassHandler {
    private final String sql = "show engines";

    public InformationSchemaEnginesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaEngines;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor resultCursor) {
        List<EnginesRecord> enginesRecords;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            enginesRecords = MetaDbUtil.query(sql, EnginesRecord.class, metaDbConn);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                "fail to access metadb" + e.getMessage());
        }
        for (EnginesRecord record : enginesRecords) {
            resultCursor.addRow(new Object[] {
                record.engine, record.support, record.comment, record.transcations, record.XA, record.savePoints
            });
        }
        return resultCursor;
    }
}

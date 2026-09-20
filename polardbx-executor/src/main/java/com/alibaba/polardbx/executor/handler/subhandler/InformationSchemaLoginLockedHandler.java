package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.CollectStatisticProgressSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCollectStatisticProgress;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLoginLocked;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class InformationSchemaLoginLockedHandler extends BaseVirtualViewSubClassHandler{

    private static final String SELECT_LOGIN_LOCKED_USER_SQL =
            "select limit_key, max_error_limit, error_count, expire_date from `" + GmsSystemTables.USER_LOGIN_ERROR_LIMIT + "` " +
                    "where error_count >= max_error_limit and expire_date > now()";

    public InformationSchemaLoginLockedHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaLoginLocked;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            ResultSet rs = metaConn.createStatement().executeQuery(SELECT_LOGIN_LOCKED_USER_SQL);
            ArrayResultCursor result = new ArrayResultCursor("LoginLocked");
            result.addColumn("limit_key", DataTypes.StringType);
            result.addColumn("max_error_limit", DataTypes.IntegerType);
            result.addColumn("error_count", DataTypes.IntegerType);
            result.addColumn("expire_date", DataTypes.TimestampType);
            while (rs.next()) {
                result.addRow(new Object[] {
                        rs.getString("limit_key"),
                        rs.getInt("max_error_limit"),
                        rs.getInt("error_count"),
                        rs.getTimestamp("expire_date")
                });
            }
            return result;
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

}

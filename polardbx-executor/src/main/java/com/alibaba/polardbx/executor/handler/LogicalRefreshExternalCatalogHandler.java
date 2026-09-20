package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ExternalCatalogSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlRefreshExternalCatalog;

public class LogicalRefreshExternalCatalogHandler extends HandlerCommon {

    public LogicalRefreshExternalCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlRefreshExternalCatalog refresh = (SqlRefreshExternalCatalog) dal.getNativeSqlNode();
        if (refresh.getExternalDbName() != null || refresh.getExternalTableName() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                "REFRESH EXTERNAL TABLE is not supported");
        }

        String catalogName = refresh.getCatalogName();
        PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.ALTER, executionContext);
        SyncManagerHelper.syncThrowExceptions(new ExternalCatalogSyncAction(catalogName, "REFRESH"), SyncScope.ALL);
        return new AffectRowCursor(0);
    }
}

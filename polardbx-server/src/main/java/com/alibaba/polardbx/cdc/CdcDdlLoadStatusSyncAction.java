package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.common.cdc.entity.DdlLoadStatusInfo;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-18 12:20
 **/
@Slf4j
@Data
public class CdcDdlLoadStatusSyncAction implements ISyncAction {
    @SneakyThrows
    @Override
    public ResultCursor sync() {
        if (ConfigDataMode.isPolarDbX() && ConfigDataMode.isMasterMode() && ExecUtils.hasLeadership(null)) {
            return doSync();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_SYNC_PRIVILEGE_FAILED,
                "current node is not leader, can`t do query cdc ddl load status info Action");
        }
    }

    @SneakyThrows
    public ArrayResultCursor doSync() {
        DdlLoadStatusInfo ddlLoadStatusInfo = DdlSqlAsyncLoader.getInstance().getDdlLoadStatusInfo();
        ArrayResultCursor result = new ArrayResultCursor("SHOW SLAVE STATUS");
        result.addColumn("MAX_DDL_ID", DataTypes.LongType, true);
        result.addColumn("EXEC_DDL_ID", DataTypes.LongType, true);
        result.addColumn("DELAY_TIME", DataTypes.LongType, true);
        result.addColumn("DELAY_COUNT", DataTypes.LongType, true);
        result.addColumn("ERROR_INFO", DataTypes.StringType, true);
        result.addColumn("STATUS", DataTypes.StringType, true);
        result.initMeta();
        result.addRow(new Object[] {
            ddlLoadStatusInfo.getMaxDdlId(),
            ddlLoadStatusInfo.getExecDdlId(),
            ddlLoadStatusInfo.getDelayTime(),
            ddlLoadStatusInfo.getDelayCount(),
            ddlLoadStatusInfo.getErrorInfo(),
            ddlLoadStatusInfo.getStatus()
        });
        return result;
    }
}

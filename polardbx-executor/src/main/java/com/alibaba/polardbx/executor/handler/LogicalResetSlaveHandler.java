/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlResetSlave;
import org.apache.http.entity.ContentType;

import java.sql.Connection;
import java.util.Properties;

import static com.alibaba.polardbx.gms.util.MetaDbUtil.getConnection;

/**
 * @author shicai.xsc 2021/5/26 16:37
 * @since 5.0.0.0
 */
public class LogicalResetSlaveHandler extends LogicalReplicationBaseHandler {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalResetSlaveHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlResetSlave sqlNode = (SqlResetSlave) dal.getNativeSqlNode();
        sqlNode.getParams().put(RplConstants.IS_ALL, String.valueOf(sqlNode.isAll()));

        if (sqlNode.isDdlLoad()) {
            resetSlaveDdlLoad(sqlNode);
        } else {
            resetSlaveNormal(sqlNode);
        }

        return new AffectRowCursor(0);
    }

    @SneakyThrows
    private void resetSlaveDdlLoad(SqlResetSlave sqlNode) {
        try (Connection metaDbConn = getConnection()) {
            metaDbConn.setAutoCommit(false);

            Properties properties = new Properties();
            properties.put(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_ENABLE, "false");
            properties.put(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_STATUS, "STOPPED");
            properties.put(ConnectionProperties.ENABLE_TRANSACTION_RECOVER_TASK, "true");
            MetaDbUtil.setGlobal(metaDbConn, properties);
            MetaDbUtil.upsertDdlLoadCheckPoint(metaDbConn, 0L);

            MetaDbUtil.commit(metaDbConn);
        }

        CdcManagerHelper.getInstance().resetCdcDdlRecordAutoIncrementSeq();
    }

    private void resetSlaveNormal(SqlResetSlave sqlNode) {
        String daemonEndpoint = CdcTargetUtil.getReplicaDaemonMasterTarget();
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/replica/resetSlave",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(sqlNode.getParams()), 10000);
        } catch (Exception e) {
            cdcLogger.error("reset slave error!", e);
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e, e.getMessage());
        }
        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            cdcLogger.warn("reset slave failed! code:" + httpResult.getCode() + ", msg:" + httpResult.getMsg());
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }
    }
}

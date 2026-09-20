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
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlStartSlave;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;

import java.util.Properties;

import static com.alibaba.polardbx.common.cdc.CdcConstants.DDL_LOAD_STATUS_RUNNING;

/**
 * @author shicai.xsc 2021/3/5 14:32
 * @desc
 * @since 5.0.0.0
 */
public class LogicalStartSlaveHandler extends LogicalReplicationBaseHandler {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalStartSlaveHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlStartSlave sqlNode = (SqlStartSlave) dal.getNativeSqlNode();

        if (sqlNode.isDdlLoad()) {
            startSlaveForDdlLoad(sqlNode, executionContext);
        } else {
            startSlaveForNormal(sqlNode);
        }

        return new AffectRowCursor(0);
    }

    @SneakyThrows
    private void startSlaveForDdlLoad(SqlStartSlave sqlNode, ExecutionContext executionContext) {
        InstConfigRecord instConfigRecord1 = MetaDbUtil.getGlobal(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_ENABLE);
        if (instConfigRecord1 == null || StringUtils.equalsIgnoreCase("false", instConfigRecord1.paramVal)) {
            cdcLogger.warn("ddl load is not open, can`t start slave!");
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT,
                "ddl load is not open, can`t start slave!");
        }

        InstConfigRecord instConfigRecord2 = MetaDbUtil.getGlobal(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_STATUS);
        if (StringUtils.equals(instConfigRecord2.paramVal, DDL_LOAD_STATUS_RUNNING)) {
            cdcLogger.warn("ddl load is already in running state, ignore start slave");
            return;
        }

        Properties properties = new Properties();
        properties.put(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_STATUS, DDL_LOAD_STATUS_RUNNING);
        MetaDbUtil.setGlobal(properties);
    }

    private void startSlaveForNormal(SqlStartSlave sqlNode) {
        String daemonEndpoint = CdcTargetUtil.getReplicaDaemonMasterTarget();
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/replica/startSlave",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(sqlNode.getParams()), 10000);
        } catch (Exception e) {
            cdcLogger.error("start slave error!", e);
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e, e.getMessage());
        }
        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            cdcLogger.warn("start slave failed! code:" + httpResult.getCode() + ", msg:" + httpResult.getMsg());
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }
    }
}

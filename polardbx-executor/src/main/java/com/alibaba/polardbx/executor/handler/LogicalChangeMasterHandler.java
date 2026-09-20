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
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
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
import org.apache.calcite.sql.SqlChangeMaster;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;

import java.sql.Connection;
import java.util.Properties;

public class LogicalChangeMasterHandler extends LogicalReplicationBaseHandler {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalChangeMasterHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlChangeMaster sqlNode = (SqlChangeMaster) dal.getNativeSqlNode();

        if (sqlNode.isDdlLoad()) {
            changeMasterDdlLoad(sqlNode, executionContext);
        } else {
            changeMasterNormal(sqlNode, executionContext);
        }

        return new AffectRowCursor(0);
    }

    @SneakyThrows
    private void changeMasterDdlLoad(SqlChangeMaster sqlNode, ExecutionContext executionContext) {
        InstConfigRecord instConfigRecord1 = MetaDbUtil.getGlobal(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_ENABLE);
        if (instConfigRecord1 != null && StringUtils.equalsIgnoreCase("true", instConfigRecord1.paramVal)) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICA_NOT_SUPPORT, "ddl_load has opened, can`t open again!");
        }

        CdcDdlRecord maxIdCdcDdlRecord = CdcManagerHelper.getInstance().getMaxIdCdcDdlRecord();
        long maxId = maxIdCdcDdlRecord == null ? 0 : maxIdCdcDdlRecord.getId();

        try (Connection metaDBConn = MetaDbUtil.getConnection()) {
            metaDBConn.setAutoCommit(false);

            Properties properties = new Properties();
            properties.put(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_ENABLE, "true");
            properties.put(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_STATUS, "STOPPED");
            properties.put(ConnectionProperties.ENABLE_TRANSACTION_RECOVER_TASK, "false");
            MetaDbUtil.setGlobal(properties);
            MetaDbUtil.upsertDdlLoadCheckPoint(metaDBConn, maxId);

            metaDBConn.commit();
        }
    }

    private void changeMasterNormal(SqlChangeMaster sqlNode, ExecutionContext executionContext) {
        String daemonEndpoint = CdcTargetUtil.getReplicaDaemonMasterTarget();
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/replica/changeMaster",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(sqlNode.getParams()), 10000);
        } catch (Exception e) {
            cdcLogger.error("change master error!", e);
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e, e.getMessage());
        }
        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }
    }
}
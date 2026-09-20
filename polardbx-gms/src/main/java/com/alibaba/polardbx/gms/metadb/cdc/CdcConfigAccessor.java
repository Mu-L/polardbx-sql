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

package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.BINLOG_SYSTEM_CONFIG_TABLE;

/**
 * @author yudong
 * @since 2023/6/20 17:12
 **/
public class CdcConfigAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(CdcConfigAccessor.class);

    private static final String CDC_SYSETM_CONFIG_TABLE = wrap(BINLOG_SYSTEM_CONFIG_TABLE);

    private static final String SELECT_ALL = "select * from " + CDC_SYSETM_CONFIG_TABLE;

    private final static String REPLACE_CONFIG_VALUE =
        "replace into " + CDC_SYSETM_CONFIG_TABLE + " set config_value=?, config_key=?";

    private static final String QUERY_BY_KEY =
        "select * from " + CDC_SYSETM_CONFIG_TABLE + " where config_key=?";

    public void updateInstConfigValue(Properties props) {
        upsertConfigValue(null, props, BINLOG_SYSTEM_CONFIG_TABLE, REPLACE_CONFIG_VALUE,
            MetaDbDataIdBuilder.getCdcSystemConfigDataId());
    }

    public List<CdcConfigRecord> queryAll() {
        return queryBySql(SELECT_ALL, null);
    }

    private List<CdcConfigRecord> queryBySql(String sql, Map<Integer, ParameterContext> params) {
        try {
            return MetaDbUtil.query(sql, params, CdcConfigRecord.class, connection);
        } catch (Throwable t) {
            logger.error("Failed to query system table " + BINLOG_SYSTEM_CONFIG_TABLE + " sql: " + sql, t);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, t, "query",
                BINLOG_SYSTEM_CONFIG_TABLE, t.getMessage());
        }
    }

    public List<CdcConfigRecord> queryByKey(String key) throws Exception {
        return MetaDbUtil.query(QUERY_BY_KEY, ImmutableMap.of(1, new ParameterContext(ParameterMethod.setString,
            new Object[] {1, key})), CdcConfigRecord.class, connection);
    }

}

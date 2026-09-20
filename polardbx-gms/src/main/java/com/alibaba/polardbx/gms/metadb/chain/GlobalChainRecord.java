/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.chain;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GlobalChainRecord implements SystemTableRecord {
    public long blockId;
    public String traceId;
    public String ip;

    public long port;
    public String user;
    public String schemaName;
    public String tableName;
    public String opHash;
    public String blockHash;
    public String extra;
    public long tso;
    public String createTime;
    public String updateTime;

    @Override
    public GlobalChainRecord fill(ResultSet rs) throws SQLException {
        this.blockId = rs.getLong("block_id");
        this.traceId = rs.getString("trace_id");
        this.ip = rs.getString("ip");
        this.port = rs.getLong("port");
        this.user = rs.getString("user");
        this.schemaName = rs.getString("schema_name");
        this.tableName = rs.getString("table_name");
        this.opHash = rs.getString("op_hash");
        this.blockHash = rs.getString("block_hash");
        this.extra = rs.getString("extra");
        this.tso = rs.getLong("tso");
        this.createTime = rs.getString("gmt_created");
        this.updateTime = rs.getString("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int i = 1;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, traceId);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, ip);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setLong, port);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, user);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, opHash);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, blockHash);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, extra);
        MetaDbUtil.setParameter(i++, params, ParameterMethod.setLong, tso);
        return params;
    }
}

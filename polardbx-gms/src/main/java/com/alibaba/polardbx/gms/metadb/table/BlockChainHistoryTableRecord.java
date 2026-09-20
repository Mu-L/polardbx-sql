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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class BlockChainHistoryTableRecord implements SystemTableRecord {
    public long blockId;
    public String traceId;
    public String startTime;

    public long recNum;
    public String opType;
    public String hashIns;
    public String hashDel;
    public String blockHash;

    public Long longPk;
    public byte[] bytesPk;

    public long tso;
    public String extra;
    public String createTime;
    public String updateTime;

    @Override
    public BlockChainHistoryTableRecord fill(ResultSet rs) throws SQLException {
        this.blockId = rs.getLong("block_id");
        this.traceId = rs.getString("trace_id");
        this.startTime = rs.getString("start_time");
        this.recNum = rs.getLong("rec_num");
        this.opType = rs.getString("op_type");
        this.hashIns = rs.getString("hash_ins");
        this.hashDel = rs.getString("hash_del");
        this.blockHash = rs.getString("block_hash");
        this.longPk = rs.getLong("long_pk");
        if (rs.wasNull()) {
            this.longPk = null;
        }
        this.bytesPk = rs.getBytes("bytes_pk");
        this.tso = rs.getLong("tso");
        this.extra = rs.getString("extra");
        this.createTime = rs.getString("gmt_created");
        this.updateTime = rs.getString("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        // skip auto increment primary-index
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.traceId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.startTime);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.recNum);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.opType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.hashIns);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.hashDel);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.blockHash);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.longPk);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBytes, this.bytesPk);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tso);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        return params;
    }
}

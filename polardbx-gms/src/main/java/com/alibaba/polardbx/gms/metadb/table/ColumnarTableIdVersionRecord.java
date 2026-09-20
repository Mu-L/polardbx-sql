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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ColumnarTableIdVersionRecord implements SystemTableRecord {
    public Long newTableId;
    public Long oldTableId;
    public Long newVersionId;
    public Long oldVersionId;
    public String extra;
    public Date createTime;
    public Date updateTime;

    public ColumnarTableIdVersionRecord() {
    }

    public ColumnarTableIdVersionRecord(Long newTableId, Long oldTableId, Long newVersionId, Long oldVersionId) {
        this.newTableId = newTableId;
        this.oldTableId = oldTableId;
        this.newVersionId = newVersionId;
        this.oldVersionId = oldVersionId;
    }

    @Override
    public ColumnarTableIdVersionRecord fill(ResultSet rs) throws SQLException {
        this.newTableId = rs.getLong("new_table_id");
        this.oldTableId = rs.getLong("old_table_id");
        this.newVersionId = rs.getLong("new_version_id");
        this.oldVersionId = rs.getLong("old_version_id");
        this.extra = rs.getString("extra");
        this.createTime = rs.getTimestamp("create_time");
        this.updateTime = rs.getTimestamp("update_time");

        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.newTableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.oldTableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.newVersionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.oldVersionId);
        return params;
    }
}

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
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TableConstraintsRecord implements SystemTableRecord {
    public String constraintCatalog;
    public String constraintSchema;
    public String constraintName;
    public String tableSchema;
    public String tableName;
    public String constraintType;
    public String enforced;

    @Override
    public TableConstraintsRecord fill(ResultSet rs) throws SQLException {
        this.constraintCatalog = rs.getString("constraint_catalog");
        this.constraintSchema = rs.getString("constraint_schema");
        this.constraintName = rs.getString("constraint_name");
        this.tableSchema = rs.getString("table_schema");
        this.tableName = rs.getString("table_name");
        this.constraintType = rs.getString("constraint_type");
        if (InstanceVersion.isMYSQL80()) {
            this.enforced = rs.getString("enforced");
        }

        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(7);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.constraintCatalog);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.constraintSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.constraintName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableSchema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.constraintType);
        if (InstanceVersion.isMYSQL80()) {
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.enforced);
        }

        return params;
    }

}

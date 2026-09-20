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

package com.alibaba.polardbx.gms.metadb.model;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ModelConfigRecord implements SystemTableRecord {

    public long id;
    public String name;
    public String model;
    public String provider;
    public String endpoint;
    public String apiKey;
    public String modelParams;
    public String status;
    public String description;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;

    @Override
    public ModelConfigRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.name = rs.getString("name");
        this.model = rs.getString("model");
        this.provider = rs.getString("provider");
        this.endpoint = rs.getString("endpoint");
        this.apiKey = rs.getString("api_key");
        this.modelParams = rs.getString("model_params");
        this.status = rs.getString("status");
        this.description = rs.getString("description");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.name);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.model);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.provider);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.endpoint);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.apiKey);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.modelParams);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.status);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.description);
        return params;
    }
}

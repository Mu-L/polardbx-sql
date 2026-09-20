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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Getter
public class ColumnarIndexEvolutionRecord implements SystemTableRecord {
    public long id;
    public long tableId;
    public long indexId;
    public String indexName;
    public int indexType;
    public long versionId;
    public long ddlJobId;

    public Timestamp create;
    public ColumnarIndexesRecord indexRecord;

    public static final int PRIMARY_KEY = 0;
    public static final int SORT_KEY = 1;

    public ColumnarIndexEvolutionRecord() {
    }

    public ColumnarIndexEvolutionRecord(long tableId, String indexName, int indexType, long versionId, long ddlJobId,
                                        ColumnarIndexesRecord indexRecord) {
        this.tableId = tableId;
        this.indexName = indexName;
        this.indexType = indexType;
        this.versionId = versionId;
        this.ddlJobId = ddlJobId;
        this.indexRecord = indexRecord;
    }

    public ColumnarIndexEvolutionRecord(long tableId, long indexId, String indexName, int indexType, long versionId,
                                        long ddlJobId, ColumnarIndexesRecord indexRecord) {
        this.tableId = tableId;
        this.indexId = indexId;
        this.indexName = indexName;
        this.indexType = indexType;
        this.versionId = versionId;
        this.ddlJobId = ddlJobId;
        this.indexRecord = indexRecord;
    }

    public static String serializeToJson(ColumnarIndexesRecord indexRecord) {
        JSONObject indexJson = new JSONObject();

        indexJson.put("indexColumnType", indexRecord.indexColumnType);
        indexJson.put("indexLocation", indexRecord.indexLocation);
        indexJson.put("indexTableName", indexRecord.indexTableName);
        indexJson.put("indexStatus", indexRecord.indexStatus);
        indexJson.put("version", indexRecord.version);
        indexJson.put("flag", indexRecord.flag);
        indexJson.put("visible", indexRecord.visible);
        indexJson.put("visitFrequency", indexRecord.visitFrequency);

        indexJson.put("tableSchema", indexRecord.tableSchema);
        indexJson.put("tableName", indexRecord.tableName);
        indexJson.put("nonUnique", indexRecord.nonUnique);
        indexJson.put("indexSchema", indexRecord.indexSchema);
        indexJson.put("indexName", indexRecord.indexName);
        indexJson.put("seqInIndex", indexRecord.seqInIndex);
        indexJson.put("columnName", indexRecord.columnName);
        indexJson.put("collation", indexRecord.collation);
        indexJson.put("cardinality", indexRecord.cardinality);
        indexJson.put("subPart", indexRecord.subPart);
        indexJson.put("packed", indexRecord.packed);
        indexJson.put("nullable", indexRecord.nullable);
        indexJson.put("indexType", indexRecord.indexType);
        indexJson.put("comment", indexRecord.comment);
        indexJson.put("indexComment", indexRecord.indexComment);

        return indexJson.toJSONString();
    }

    public static ColumnarIndexesRecord deserializeFromJson(String json) {
        ColumnarIndexesRecord indexRecord = new ColumnarIndexesRecord();
        JSONObject indexRecordJson = JSON.parseObject(json);

        indexRecord.indexColumnType = indexRecordJson.getLongValue("indexColumnType");
        indexRecord.indexLocation = indexRecordJson.getLongValue("indexLocation");
        indexRecord.indexTableName = indexRecordJson.getString("indexTableName");
        indexRecord.indexStatus = indexRecordJson.getLongValue("indexStatus");
        indexRecord.version = indexRecordJson.getLongValue("version");
        indexRecord.flag = indexRecordJson.getLongValue("flag");
        indexRecord.visible = indexRecordJson.getLongValue("visible");

        indexRecord.tableSchema = indexRecordJson.getString("tableSchema");
        indexRecord.tableName = indexRecordJson.getString("tableName");
        indexRecord.nonUnique = indexRecordJson.getLongValue("nonUnique");
        indexRecord.indexSchema = indexRecordJson.getString("indexSchema");
        indexRecord.indexName = indexRecordJson.getString("indexName");
        indexRecord.seqInIndex = indexRecordJson.getLongValue("seqInIndex");
        indexRecord.columnName = indexRecordJson.getString("columnName");
        indexRecord.collation = indexRecordJson.getString("collation");
        indexRecord.cardinality = indexRecordJson.getLongValue("cardinality");
        indexRecord.subPart = indexRecordJson.getLongValue("subPart");
        indexRecord.packed = indexRecordJson.getString("packed");
        indexRecord.nullable = indexRecordJson.getString("nullable");
        indexRecord.indexType = indexRecordJson.getString("indexType");
        indexRecord.comment = indexRecordJson.getString("comment");
        indexRecord.indexComment = indexRecordJson.getString("indexComment");

        return indexRecord;
    }

    @Override
    public ColumnarIndexEvolutionRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.indexId = rs.getLong("index_id");
        this.tableId = rs.getLong("table_id");
        this.indexName = rs.getString("index_name");
        this.indexType = rs.getInt("index_type");
        this.versionId = rs.getLong("version_id");
        this.ddlJobId = rs.getLong("ddl_job_id");
        this.indexRecord = deserializeFromJson(rs.getString("index_record"));
        this.create = rs.getTimestamp("gmt_created");
        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.indexId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.tableId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.indexName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.indexType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.versionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.ddlJobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, serializeToJson(this.indexRecord));
        return params;
    }
}

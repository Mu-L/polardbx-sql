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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarTableIdVersionAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");
    private static final String COLUMNAR_TABLE_VERSION_ID_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_ID_VERSION);

    private static final String SELECT_ALL_COLUMNS =
        "select `new_table_id`, `old_table_id`, `new_version_id`, `old_version_id`, `extra`, `create_time` , `update_time`";

    private static final String INSERT_COLUMNAR_TABLE_VERSION_ID_RECORDS =
        "insert into " + COLUMNAR_TABLE_VERSION_ID_TABLE +
            "(`new_table_id`, `old_table_id`, `new_version_id`, `old_version_id`) values (?, ?, ?, ?)";

    private static final String SELECT_BY_NEW_TABLE_ID = SELECT_ALL_COLUMNS +
        " from " + COLUMNAR_TABLE_VERSION_ID_TABLE + " where `new_table_id` = ?";

    private static final String SELECT_BY_OLD_TABLE_ID = SELECT_ALL_COLUMNS +
        " from " + COLUMNAR_TABLE_VERSION_ID_TABLE + " where `old_table_id` = ?";

    private static final String DELETE_BY_OLD_TABLE_IDS =
        "delete from " + COLUMNAR_TABLE_VERSION_ID_TABLE + " where `old_table_id` in (%s)";

    public int[] insert(List<ColumnarTableIdVersionRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarTableIdVersionRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNAR_TABLE_VERSION_ID_RECORDS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNAR_TABLE_VERSION_ID_RECORDS, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_TABLE_VERSION_ID_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_TABLE_VERSION_ID_TABLE, e.getMessage());
        }
    }

    public List<ColumnarTableIdVersionRecord> queryByNewTableId(Long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        return query(SELECT_BY_NEW_TABLE_ID, COLUMNAR_TABLE_VERSION_ID_TABLE,
            ColumnarTableIdVersionRecord.class, params);
    }

    public List<ColumnarTableIdVersionRecord> queryByOldTableId(Long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        return query(SELECT_BY_OLD_TABLE_ID, COLUMNAR_TABLE_VERSION_ID_TABLE,
            ColumnarTableIdVersionRecord.class, params);
    }

    public int deleteByOldTableId(List<Long> tableIds) {
        if (tableIds == null || tableIds.isEmpty()) {
            return 0;
        }
        final Map<Integer, ParameterContext> params =
            MetaDbUtil.buildParameters(ParameterMethod.setLong, tableIds.toArray());
        return delete(String.format(DELETE_BY_OLD_TABLE_IDS, concatParams(tableIds.size())),
            COLUMNAR_TABLE_VERSION_ID_TABLE, params);
    }
}

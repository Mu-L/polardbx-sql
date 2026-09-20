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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableConstraintsAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableConstraintsAccessor.class);

    private static final String TABLE_CONSTRAINTS_TABLE = wrap(GmsSystemTables.TABLE_CONSTRAINTS);

    private static final String TABLE_CONSTRAINTS_TABLE_INFO_SCHEMA = "information_schema.table_constraints";

    private static final String FROM_TABLE_CONSTRAINTS_TABLE = " from " + TABLE_CONSTRAINTS_TABLE;

    private static final String INSERT_TABLE_CONSTRAINTS_TABLE =
        "insert into " + TABLE_CONSTRAINTS_TABLE
            + "(`constraint_catalog`, `constraint_schema`, `constraint_name`, `table_schema`, `table_name`, `constraint_type`, `enforced`) "
            + "values(?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_TABLE_CONSTRAINTS_TABLE_57 =
        "insert into " + TABLE_CONSTRAINTS_TABLE
            + "(`constraint_catalog`, `constraint_schema`, `constraint_name`, `table_schema`, `table_name`, `constraint_type`) "
            + "values(?, ?, ?, ?, ?, ?)";

    private static final String DELETE_CONSTRAINTS_BY_SCHEMA =
        "delete from " + TABLE_CONSTRAINTS_TABLE + " where `table_schema` = ?";

    private static final String DELETE_CONSTRAINTS_BY_SCHEMA_TABLE =
        "delete from " + TABLE_CONSTRAINTS_TABLE + " where `table_schema` = ? and `table_name` = ?";

    private static final String DELETE_CONSTRAINTS_BY_SCHEMA_CONSTRAINT =
        "delete from " + TABLE_CONSTRAINTS_TABLE
            + " where `table_schema` = ? and `table_name` = ? and `constraint_name` in (%s)";

    private static final String SELECT_CLAUSE =
        "select `constraint_catalog`, `constraint_schema`, `constraint_name`, `table_schema`, `table_name`, `constraint_type`, `enforced` ";

    // no `enforced` column in mysql5.7
    private static final String SELECT_CLAUSE_57 =
        "select `constraint_catalog`, `constraint_schema`, `constraint_name`, `table_schema`, `table_name`, `constraint_type` ";

    private static final String WHERE_SCHEMA = " where `table_schema` = ? ";

    private static final String WHERE_SCHEMA_TABLE =
        " where `table_schema` = ? and `table_name` = ? ";

    private static final String AND_CONSTRAINT_TYPE =
        " and `constraint_type`= ? ";

    private static final String AND_CONSTRAINT_NAME = " and `constraint_name` = ? ";

    private static final String SELECT_INFO_SCHEMA =
        SELECT_CLAUSE + " from " + TABLE_CONSTRAINTS_TABLE_INFO_SCHEMA + WHERE_SCHEMA_TABLE;

    private static final String SELECT_INFO_SCHEMA_57 =
        SELECT_CLAUSE_57 + " from " + TABLE_CONSTRAINTS_TABLE_INFO_SCHEMA + WHERE_SCHEMA_TABLE;

    private static final String WHERE_SCHEMA_TABLE_TYPE = WHERE_SCHEMA_TABLE + AND_CONSTRAINT_TYPE;

    private static final String SELECT_CONSTRAINT_BY_SCHEMA =
        SELECT_CLAUSE + FROM_TABLE_CONSTRAINTS_TABLE + WHERE_SCHEMA;

    private static final String SELECT_CONSTRAINT_BY_SCHEMA_TABLE =
        SELECT_CLAUSE + FROM_TABLE_CONSTRAINTS_TABLE + WHERE_SCHEMA_TABLE;

    private static final String SELECT_CONSTRAINT_BY_SCHEMA_TABLE_TYPE =
        SELECT_CLAUSE + FROM_TABLE_CONSTRAINTS_TABLE + WHERE_SCHEMA_TABLE_TYPE;

    private static final String UPDATE_FOR_TABLE =
        "update " + TABLE_CONSTRAINTS_TABLE + "set `table_name` = ? " + WHERE_SCHEMA_TABLE;

//    private static final String UPDATE_FOR_TABLE =
//        "update " + TABLE_CONSTRAINTS_TABLE + "set `table_name` = ?, `constraint_name` = ?" + WHERE_SCHEMA_TABLE
//            + AND_CONSTRAINT_NAME;

    public int[] insert(List<TableConstraintsRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (TableConstraintsRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            if (InstanceVersion.isMYSQL80()) {
                DdlMetaLogUtil.logSql(INSERT_TABLE_CONSTRAINTS_TABLE, paramsBatch);
                return MetaDbUtil.insert(INSERT_TABLE_CONSTRAINTS_TABLE, paramsBatch, connection);
            } else {
                DdlMetaLogUtil.logSql(INSERT_TABLE_CONSTRAINTS_TABLE_57, paramsBatch);
                return MetaDbUtil.insert(INSERT_TABLE_CONSTRAINTS_TABLE_57, paramsBatch, connection);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + TABLE_CONSTRAINTS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                TABLE_CONSTRAINTS_TABLE,
                e.getMessage());
        }
    }

    public void deleteByConstraint(String tableSchema, String tableName, List<String> constraintNames) {
        Map<Integer, ParameterContext> params = buildParams(tableSchema, tableName, constraintNames);
        delete(String.format(DELETE_CONSTRAINTS_BY_SCHEMA_CONSTRAINT, concatParams(constraintNames)),
            TABLE_CONSTRAINTS_TABLE, params);
    }

    public void deleteBySchemaTable(String tableSchema, String tableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        try {
            DdlMetaLogUtil.logSql(DELETE_CONSTRAINTS_BY_SCHEMA_TABLE, params);
            MetaDbUtil.delete(DELETE_CONSTRAINTS_BY_SCHEMA_TABLE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteBySchema(String tableSchema) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
        try {
            DdlMetaLogUtil.logSql(DELETE_CONSTRAINTS_BY_SCHEMA, params);
            MetaDbUtil.delete(DELETE_CONSTRAINTS_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<TableConstraintsRecord> queryInfoSchema(String phyTableSchema, String phyTableName,
                                                        DataSource dataSource) {
        List<TableConstraintsRecord> records =
            query(SELECT_INFO_SCHEMA, TABLE_CONSTRAINTS_TABLE_INFO_SCHEMA, TableConstraintsRecord.class, phyTableSchema,
                phyTableName,
                dataSource);
        return records;
    }

    public List<TableConstraintsRecord> queryInfoSchema57(String phyTableSchema, String phyTableName,
                                                        DataSource dataSource) {
        List<TableConstraintsRecord> records =
            query(SELECT_INFO_SCHEMA_57, TABLE_CONSTRAINTS_TABLE_INFO_SCHEMA, TableConstraintsRecord.class, phyTableSchema,
                phyTableName,
                dataSource);
        return records;
    }

    public List<TableConstraintsRecord> queryConstraintsBySchema(String tableSchema) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
        return query(SELECT_CONSTRAINT_BY_SCHEMA, TABLE_CONSTRAINTS_TABLE, TableConstraintsRecord.class,
            params);
    }

    public List<TableConstraintsRecord> queryConstraintsBySchemaTable(String tableSchema,
                                                                      String tableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        return query(SELECT_CONSTRAINT_BY_SCHEMA_TABLE, TABLE_CONSTRAINTS_TABLE, TableConstraintsRecord.class,
            params);
    }

    public List<TableConstraintsRecord> queryConstraintsBySchemaTableType(String tableSchema,
                                                                          String tableName,
                                                                          String constraintType) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, constraintType);
        return query(SELECT_CONSTRAINT_BY_SCHEMA_TABLE_TYPE, TABLE_CONSTRAINTS_TABLE, TableConstraintsRecord.class,
            params);
    }

//    public int updateTableConstraintsTable(String tableSchema, String tableName, String constraintName,
//                                     String originalTableName) {
//        Map<Integer, ParameterContext> params = MetaDbUtil
//            .buildStringParameters(new String[] {
//                tableName,
//                constraintName,
//                tableSchema,
//                originalTableName,
//                constraintName,
//            });
//        return update(UPDATE_FOR_TABLE, TABLE_CONSTRAINTS_TABLE, params);
//    }

    public int updateTableConstraintsTable(String tableSchema, String tableName, String originalTableName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                tableName,
                tableSchema,
                originalTableName,
            });
        return update(UPDATE_FOR_TABLE, TABLE_CONSTRAINTS_TABLE, params);
    }
}

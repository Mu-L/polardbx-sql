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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateIndexInDatabasePreparedData;
import lombok.Getter;
import org.apache.calcite.rel.ddl.CreateIndexInDatabase;
import org.apache.calcite.sql.SqlCreateIndexInDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

@Getter
public class LogicalCreateIndexInDatabase extends BaseDdlOperation {
    private SqlCreateIndexInDatabase sqlCreateIndexInDatabase;
    private final String dbName;
    private boolean isColumnar;

    private CreateIndexInDatabasePreparedData createIndexInDatabasePreparedData;

    public LogicalCreateIndexInDatabase(CreateIndexInDatabase createIndexInDatabase) {
        super(createIndexInDatabase.getCluster(), createIndexInDatabase.getTraitSet(), createIndexInDatabase);
        this.sqlCreateIndexInDatabase = (SqlCreateIndexInDatabase) relDdl.sqlNode;
        this.dbName = sqlCreateIndexInDatabase.getDbName().getLastName();
        this.schemaName = dbName;
        this.isColumnar = sqlCreateIndexInDatabase.isColumnar();
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
        this.setTableName("nonsense");
    }

    public static LogicalCreateIndexInDatabase create(CreateIndexInDatabase createIndexInDatabase) {
        return new LogicalCreateIndexInDatabase(createIndexInDatabase);
    }

    public void prepareData() {
        createIndexInDatabasePreparedData = new CreateIndexInDatabasePreparedData();
        createIndexInDatabasePreparedData.setDbName(dbName);
        createIndexInDatabasePreparedData.setColumnar(isColumnar);
        genAllAddCciSql();
    }

    private void genAllAddCciSql() {
        List<TableMeta> allTables =
            new ArrayList<>(OptimizerContext.getContext(dbName).getLatestSchemaManager().getAllUserTables());
        for (TableMeta tableMeta : allTables) {
            // only create default cci on table without cci
            if (GeneralUtil.isEmpty(tableMeta.getColumnarIndexPublished())) {
                genAddCciSql(tableMeta);
            }
        }
    }

    private void genAddCciSql(TableMeta tableMeta) {
        String tableName = tableMeta.getTableName();
        // Create default index name by cci_[tableName]_[pk1]_[pk2]
        StringBuilder indexName = new StringBuilder();
        indexName.append("cci_").append(tableName);
        for (ColumnMeta column : tableMeta.getPrimaryKey()) {
            indexName.append("_").append(column.getName());
        }
        String sql = String.format("CREATE CLUSTERED COLUMNAR INDEX `%s` ON `%s`",
            indexName, tableName);
        String rollbackSql = String.format("DROP INDEX `%s` ON `%s`", indexName, tableName);
        createIndexInDatabasePreparedData.getAddCciSql().add(new Pair<>(sql, rollbackSql));
        createIndexInDatabasePreparedData.getTableNameAndIndexNameList()
            .add(new Pair<>(tableName, indexName.toString()));
    }

    public boolean isColumnar() {
        return isColumnar;
    }
}

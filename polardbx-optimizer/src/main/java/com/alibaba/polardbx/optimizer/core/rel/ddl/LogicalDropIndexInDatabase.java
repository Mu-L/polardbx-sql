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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropIndexInDatabasePreparedData;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import lombok.Getter;
import org.apache.calcite.rel.ddl.DropIndexInDatabase;
import org.apache.calcite.sql.SqlDropIndexInDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class LogicalDropIndexInDatabase extends BaseDdlOperation {
    private SqlDropIndexInDatabase sqlDropIndexInDatabase;
    private final String dbName;
    private boolean isColumnar;

    private DropIndexInDatabasePreparedData dropIndexInDatabasePreparedData;

    public LogicalDropIndexInDatabase(DropIndexInDatabase dropIndexInDatabase) {
        super(dropIndexInDatabase.getCluster(), dropIndexInDatabase.getTraitSet(), dropIndexInDatabase);
        this.sqlDropIndexInDatabase = (SqlDropIndexInDatabase) relDdl.sqlNode;
        this.dbName = sqlDropIndexInDatabase.getDbName().getLastName();
        this.schemaName = dbName;
        this.isColumnar = sqlDropIndexInDatabase.isColumnar();
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
        this.setTableName("nonsense");
    }

    public static LogicalDropIndexInDatabase create(DropIndexInDatabase dropIndexInDatabase) {
        return new LogicalDropIndexInDatabase(dropIndexInDatabase);
    }

    public void prepareData() {
        dropIndexInDatabasePreparedData = new DropIndexInDatabasePreparedData();
        dropIndexInDatabasePreparedData.setDbName(dbName);
        dropIndexInDatabasePreparedData.setColumnar(isColumnar);
        genAllAddCciSql();
    }

    private void genAllAddCciSql() {
        List<TableMeta> allTables =
            new ArrayList<>(OptimizerContext.getContext(dbName).getLatestSchemaManager().getAllUserTables());
        for (TableMeta tableMeta : allTables) {
            genAddCciSql(tableMeta);
        }
    }

    private void genAddCciSql(TableMeta tableMeta) {
        String tableName = tableMeta.getTableName();
        // Drop all columnar index
        if (tableMeta.getColumnarIndexPublished() == null) {
            return;
        }
        Set<String> columnarNames = tableMeta.getColumnarIndexPublished().keySet();
        List<String> originColumnarNames =
            columnarNames.stream().map(TddlSqlToRelConverter::unwrapGsiName).collect(Collectors.toList());
        for (String indexName : originColumnarNames) {
            String sql = String.format("DROP INDEX `%s` ON `%s`", indexName, tableName);
            String rollbackSql = String.format("CREATE CLUSTERED COLUMNAR INDEX `%s` ON `%s`",
                indexName, tableName);
            dropIndexInDatabasePreparedData.getDropCciSql().add(new Pair<>(sql, rollbackSql));
            dropIndexInDatabasePreparedData.getTableNameAndIndexNameList()
                .add(new Pair<>(tableName, indexName.toString()));
        }
    }

    public boolean isColumnar() {
        return isColumnar;
    }
}

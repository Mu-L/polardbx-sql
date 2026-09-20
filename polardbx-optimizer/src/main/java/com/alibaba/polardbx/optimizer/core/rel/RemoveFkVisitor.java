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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

public class RemoveFkVisitor extends SqlShuttle {
    @Override
    public SqlNode visit(SqlCall call) {
        SqlCall copy = SqlNode.clone(call);
        SqlKind kind = copy.getKind();
        if (kind == SqlKind.CREATE_TABLE) {
            SqlCreateTable sqlCreateTable = (SqlCreateTable) copy;

            List<SQLStatement> statementList =
                SQLUtils.parseStatementsWithDefaultFeatures(sqlCreateTable.getSourceSql(), JdbcConstants.MYSQL);
            MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);
            List<SQLTableElement> removeFKs = new ArrayList<>();
            List<SQLTableElement> originElementList = new ArrayList<>(stmt.getTableElementList());
            for (SQLTableElement sqlTableElement : originElementList) {
                if (sqlTableElement instanceof MysqlForeignKey) {
                    removeFKs.add(sqlTableElement);
                }
            }
            stmt.getTableElementList().removeAll(removeFKs);

            sqlCreateTable.removeForeignKeys();
            sqlCreateTable.setLogicalReferencedTables(null);
            sqlCreateTable.setSourceSql(stmt.toString());
            return sqlCreateTable;
        }
        return super.visit(call);
    }
}

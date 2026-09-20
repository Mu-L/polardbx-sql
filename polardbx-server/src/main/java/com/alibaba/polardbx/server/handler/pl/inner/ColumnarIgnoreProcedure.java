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

package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ColumnarIgnoreProcedure extends BaseInnerProcedure {
    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("STATUS", DataTypes.StringType);
        /**
         * 支持参数：
         * columnar_ignore(indexId1,indexId2,indexId3...)，通过列存tableId匹配
         */
        List<Long> tableIds = parseTableIds(statement.getParameters(), statement);

        if (!DynamicConfig.getInstance().enableColumnarIgnore()) {
            cursor.addRow(new Object[] {"FAIL: NOT ALLOW TO IGNORE CCI"});
            return;
        }

        //返回结果
        try {
            ExecUtils.columnarIgnore(tableIds);
            cursor.addRow(new Object[] {"OK"});
        } catch (Exception e) {
            cursor.addRow(new Object[] {"FAIL"});
        }
    }

    private List<Long> parseTableIds(List<SQLExpr> params, SQLCallStatement statement) {
        List<Long> tableIds = new ArrayList<>();
        for (SQLExpr param : params) {
            if (!(param instanceof SQLNumericLiteralExpr)) {
                throw new IllegalArgumentException("columnar_ignore parameters need Long number");
            }
            long tableId = ((SQLNumericLiteralExpr) param).getNumber().longValue();
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                accessor.setConnection(metaDbConn);
                List<ColumnarTableMappingRecord> records = accessor.queryTableId(tableId);
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        statement.toString() + " indexId: " + tableId + " not found columnar index");
                }
                if (!ColumnarTableStatus.PUBLIC.name().equalsIgnoreCase(records.get(0).status)) {
                    throw new IllegalArgumentException(
                        statement.toString() + " indexId: " + tableId + " is not PUBLIC columnar index, status is "
                            + records.get(0).status);
                }
                tableIds.add(tableId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return tableIds;
    }
}

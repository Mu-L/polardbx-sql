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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InnerProcedureHandlerTest {

    private static SQLCallStatement parseCall(String sql) {
        return (SQLCallStatement) FastsqlUtils.parseSql(sql, SQLParserFeature.IgnoreNameQuotes).get(0);
    }

    private static void mockCheckpointAndPurge(MockedStatic<MetaDbUtil> metaDbUtilMockedStatic) {
        Connection mockConnection = mock(Connection.class);
        metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

        List<ColumnarCheckpointsRecord> checkpointRecords = new ArrayList<>();
        ColumnarCheckpointsRecord checkpointRecord = new ColumnarCheckpointsRecord();
        checkpointRecord.setBinlogTso(123L);
        checkpointRecord.setCheckpointTso(123L);
        checkpointRecords.add(checkpointRecord);
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
            Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(checkpointRecords);
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
            Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(new ArrayList<>());
    }

    /**
     * Test unknown inner procedure returns ERR_PROCEDURE_NOT_FOUND error
     */
    @Test
    public void testHandleUnknownProcedure() {
        ServerConnection c = mock(ServerConnection.class);
        InnerProcedureHandler.handle(parseCall("call polardbx.not_exist_procedure()"), c, false);
        verify(c).writeErrMessage(eq(ErrorCode.ERR_PROCEDURE_NOT_FOUND),
            Mockito.contains("not_exist_procedure"));
        verify(c, never()).createResultHandler(Mockito.anyBoolean());
    }

    /**
     * Test known inner procedure executes and sends result
     */
    @Test
    public void testHandleKnownProcedureSendsResult() throws Exception {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            mockCheckpointAndPurge(metaDbUtilMockedStatic);

            ServerConnection c = mock(ServerConnection.class);
            QueryResultHandler resultHandler = mock(QueryResultHandler.class);
            when(c.createResultHandler(Mockito.anyBoolean())).thenReturn(resultHandler);

            InnerProcedureHandler.handle(parseCall("call polardbx.columnar_snapshot_files(123)"), c, false);

            verify(resultHandler).sendSelectResult(Mockito.any(ResultSet.class), Mockito.any(), anyLong());
            verify(resultHandler).sendPacketEnd(false);
            verify(c, never()).writeErrMessage(Mockito.any(ErrorCode.class), anyString());
        }
    }

    /**
     * Test error during sending result: writeErrMessage called, packet end still sent (finally path)
     */
    @Test
    public void testHandleSendResultError() throws Exception {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            mockCheckpointAndPurge(metaDbUtilMockedStatic);

            ServerConnection c = mock(ServerConnection.class);
            QueryResultHandler resultHandler = mock(QueryResultHandler.class);
            when(c.createResultHandler(Mockito.anyBoolean())).thenReturn(resultHandler);
            Mockito.doThrow(new RuntimeException("mock send error")).when(resultHandler)
                .sendSelectResult(Mockito.any(ResultSet.class), Mockito.any(), anyLong());

            InnerProcedureHandler.handle(parseCall("call polardbx.columnar_snapshot_files(123)"), c, false);

            verify(c).writeErrMessage(eq(ErrorCode.ERR_PROCEDURE_EXECUTE), Mockito.contains("mock send error"));
            // finally路径：出错后仍发送结束包
            verify(resultHandler).sendPacketEnd(false);
        }
    }

    /**
     * Test BaseInnerProcedure default getResultCursor wraps execute result in ArrayResultCursor
     */
    @Test
    public void testBaseInnerProcedureDefaultGetResultCursor() {
        BaseInnerProcedure procedure = new BaseInnerProcedure() {
            @Override
            void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
                cursor.addColumn("name", DataTypes.StringType);
                cursor.initMeta();
                cursor.addRow(new Object[] {"value1"});
            }
        };

        ResultCursor cursor = procedure.getResultCursor(null, parseCall("call polardbx.mock_proc()"), "mock_proc");
        Assert.assertTrue(cursor instanceof ArrayResultCursor);
        List<Row> rows = ((ArrayResultCursor) cursor).getRows();
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("value1", rows.get(0).getObject(0));
    }
}

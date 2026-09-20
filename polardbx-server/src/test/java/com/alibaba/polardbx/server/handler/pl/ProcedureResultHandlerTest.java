/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.PlContext;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.executor.utils.ResultSetUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ProcedureResultHandlerTest {

    @Mock
    private IPacketOutputProxy mockProxy;

    @Mock
    private ServerConnection mockConnection;

    @Mock
    private PlContext mockPlContext;

    @Test
    public void testConstructorStartsOuterPacketOutput() {
        new ProcedureResultHandler(mockProxy, mockConnection, mockPlContext, false);

        verify(mockProxy).packetBegin();
    }

    @Test
    public void testWriteAffectRowsReusesOuterPacketOutput() {
        ProcedureResultHandler target =
            new ProcedureResultHandler(mockProxy, mockConnection, mockPlContext, false);

        target.writeAffectRows();
        target.writeAffectRows();

        // One outer scope begin plus one begin per OK packet write.
        verify(mockProxy, times(3)).packetBegin();
        verify(mockProxy, times(2)).packetEnd();
    }

    @Test
    public void testSendSelectResultReusesOuterPacketOutput() throws Exception {
        ProcedureResultHandler target =
            new ProcedureResultHandler(mockProxy, mockConnection, mockPlContext, false);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);

        try (MockedStatic<ResultSetUtil> mockedResultSetUtil = mockStatic(ResultSetUtil.class)) {
            mockedResultSetUtil
                .when(() -> ResultSetUtil.resultSetToPacket(any(), any(), any(), any(), any(), anyLong()))
                .thenReturn(mockProxy);

            target.sendSelectResult(mockResultSet, new AtomicLong(), 100L);
        }

        // ResultSetUtil receives the existing proxy; the handler does not open another outer scope.
        verify(mockProxy, times(1)).packetBegin();
    }

    @Test
    public void testSelectForDeeperUseDoesNotOpenAnotherPacketOutput() throws Exception {
        ProcedureResultHandler target =
            new ProcedureResultHandler(mockProxy, mockConnection, mockPlContext, false);
        target.setSelectForDeeperUse(true);
        TResultSet mockResultSet = Mockito.mock(TResultSet.class);

        try (MockedStatic<PLUtils> mockedPlUtils = mockStatic(PLUtils.class)) {
            mockedPlUtils.when(() -> PLUtils.buildCacheCursor(any(), any())).thenReturn(null);

            target.sendSelectResult(mockResultSet, new AtomicLong(), 100L);
        }

        // Internal SELECT processing reuses the constructor-opened outer scope without adding another one.
        verify(mockProxy, times(1)).packetBegin();
    }

    @Test
    public void testWriteBackToClientEndsOuterPacketOutput() {
        ProcedureResultHandler target =
            new ProcedureResultHandler(mockProxy, mockConnection, mockPlContext, false);

        target.writeBackToClient();

        verify(mockProxy).packetEnd();
    }
}

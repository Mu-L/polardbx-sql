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

package com.alibaba.polardbx.util;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.server.util.GlobalChainUtils;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class GlobalChainUtilsTest {

    @Test
    public void getTsoTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(new ArrayList<ColumnarCheckpointsRecord>());

            long tso = GlobalChainUtils.getTso();
            Assert.assertEquals(-1, tso);

            ColumnarCheckpointsRecord record = new ColumnarCheckpointsRecord();
            record.binlogTso = 100L;
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(ImmutableList.of(record));

            tso = GlobalChainUtils.getTso();
            Assert.assertEquals(100, tso);
        }
    }

    @Test
    public void getHashTest() throws Exception {
        try (MockedStatic<InnerConnectionManager> mockedStatic = mockStatic(InnerConnectionManager.class)) {
            InnerConnectionManager innerConnectionManager = mock(InnerConnectionManager.class);
            mockedStatic.when(InnerConnectionManager::getInstance).thenReturn(innerConnectionManager);
            IInnerConnection innerConnection = mock(IInnerConnection.class);
            when(innerConnectionManager.getConnection(Mockito.anyString())).thenReturn(innerConnection);
            Statement statement = mock(Statement.class);
            ResultSet resultSet = mock(ResultSet.class);
            when(innerConnection.createStatement()).thenReturn(statement);
            when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true).thenReturn(false);
            when(resultSet.getString(Mockito.any())).thenReturn("abcdef");

            StringBuilder sb = new StringBuilder();
            long hash = GlobalChainUtils.getUserTableHash("schemaName", "tableName", 100L, sb);

            Assert.assertEquals(1686792553, hash);

            when(resultSet.next()).thenReturn(true).thenReturn(false);
            when(resultSet.getString(Mockito.any())).thenReturn(PasswdUtil.encrypt("1234567"));
            long hash2 = GlobalChainUtils.getHistTableHash("schemaName", "tableName", 100L, sb);
            Assert.assertEquals(19088743, hash2);

            when(resultSet.next()).thenReturn(false);
            long hash3 = GlobalChainUtils.getHistTableHash("schemaName", "tableName", 100L, sb);
            Assert.assertEquals(-1, hash3);

            //error
            when(resultSet.next()).thenThrow(new RuntimeException("mock exception"));
            try {
                GlobalChainUtils.getUserTableHash("schemaName", "tableName", 100L, sb);
                Assert.fail();
            } catch (Exception ignored) {
            }

            try {
                GlobalChainUtils.getHistTableHash("schemaName", "tableName", 100L, sb);
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void checkHistChainValidTest() throws Exception {
        try (MockedStatic<InnerConnectionManager> mockedStatic = mockStatic(InnerConnectionManager.class)) {
            InnerConnectionManager innerConnectionManager = mock(InnerConnectionManager.class);
            mockedStatic.when(InnerConnectionManager::getInstance).thenReturn(innerConnectionManager);
            IInnerConnection innerConnection = mock(IInnerConnection.class);
            when(innerConnectionManager.getConnection(Mockito.anyString())).thenReturn(innerConnection);
            Statement statement = mock(Statement.class);
            ResultSet resultSet = mock(ResultSet.class);
            when(innerConnection.createStatement()).thenReturn(statement);
            when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);

            when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
            when(resultSet.getString(Mockito.anyInt())).thenReturn(PasswdUtil.encrypt("1234567"));
            when(resultSet.getString(Mockito.anyString())).thenReturn("abc").thenReturn("def")
                .thenReturn(PasswdUtil.encrypt("276345"));
            when(resultSet.getLong(Mockito.anyInt())).thenReturn(100L);

            StringBuilder sb = new StringBuilder();
            boolean result = GlobalChainUtils.checkHistChainValid("schemaName", "tableName", 100L, sb);
            Assert.assertEquals(false, result);
        }
    }

}

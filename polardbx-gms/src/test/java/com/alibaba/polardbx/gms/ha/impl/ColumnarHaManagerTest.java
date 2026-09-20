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

package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * @author lijiu.lzw
 */
public class ColumnarHaManagerTest {

    @Test
    public void buildContextTest() throws Exception {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarLeaseRecord> records = new ArrayList<>();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(ColumnarLeaseRecord.class), Mockito.any())).thenReturn(records);
            Connection connection = Mockito.mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(connection);

            ColumnarHaContext columnarHaContext = ColumnarHaManager.buildColumnarHaContextFromMetaDb();
            Assert.assertNull(columnarHaContext);

            ColumnarLeaseRecord record1 = new ColumnarLeaseRecord();
            records.add(record1);
            columnarHaContext = ColumnarHaManager.buildColumnarHaContextFromMetaDb();
            Assert.assertNull(columnarHaContext);

            ColumnarLeaseRecord record2 = new ColumnarLeaseRecord();
            records.add(record2);

            record1.id = 1;
            record1.owner = "127.0.0.1@123@1741241905182";
            record1.lease = System.currentTimeMillis() + 10000;

            record2.id = 10;
            record2.owner = "127.0.0.2:3070";

            columnarHaContext = ColumnarHaManager.buildColumnarHaContextFromMetaDb();
            Assert.assertNull(columnarHaContext);

            record2.owner = "127.0.0.2:3070:3080";

            columnarHaContext = ColumnarHaManager.buildColumnarHaContextFromMetaDb();
            Assert.assertNull(columnarHaContext);

            record2.owner = "127.0.0.1:3070:3080";

            columnarHaContext = ColumnarHaManager.buildColumnarHaContextFromMetaDb();
            Assert.assertNotNull(columnarHaContext);
            Assert.assertEquals("127.0.0.1", columnarHaContext.getCurrAvailableNodeAddr());
            Assert.assertEquals(3080, columnarHaContext.getCurrRpcPort());
            Assert.assertEquals(1, columnarHaContext.getAllColumnarHaInfoMap().size());
            Assert.assertEquals(record1.lease, columnarHaContext.getLeaseTime());

            //error
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(ColumnarLeaseRecord.class), Mockito.any())).thenThrow(new Exception("mock error"));

            columnarHaContext = ColumnarHaManager.buildColumnarHaContextFromMetaDb();
            Assert.assertNull(columnarHaContext);
        }
    }

    @Test
    public void refreshTest2() {
        ColumnarHaManager columnarHaManager = new ColumnarHaManager();
        ColumnarHaContext columnarHaContext = new ColumnarHaContext("127.0.0.1", 3080, new HashMap<>());
        columnarHaManager.setColumnarHaContext(columnarHaContext);

        ColumnarHaManager.refreshColumnarHaContext(columnarHaManager, null);

        ColumnarHaContext columnarHaContext2 = new ColumnarHaContext("127.0.0.1", 3080, new HashMap<>());
        columnarHaContext.setLeaseTime(System.currentTimeMillis() + 10);
        columnarHaContext2.setLeaseTime(System.currentTimeMillis() + 10000);

        columnarHaManager.setColumnarHaContext(null);
        ColumnarHaManager.refreshColumnarHaContext(columnarHaManager, columnarHaContext2);
        Assert.assertEquals("127.0.0.1", columnarHaManager.getColumnarHaContext().getCurrAvailableNodeAddr());
        Assert.assertEquals(3080, columnarHaManager.getColumnarHaContext().getCurrRpcPort());
        Assert.assertEquals(columnarHaContext2.getLeaseTime(), columnarHaManager.getColumnarHaContext().getLeaseTime());

        columnarHaManager.setColumnarHaContext(columnarHaContext);
        ColumnarHaManager.refreshColumnarHaContext(columnarHaManager, columnarHaContext2);
        Assert.assertEquals("127.0.0.1", columnarHaManager.getColumnarHaContext().getCurrAvailableNodeAddr());
        Assert.assertEquals(3080, columnarHaManager.getColumnarHaContext().getCurrRpcPort());
        Assert.assertEquals(columnarHaContext2.getLeaseTime(), columnarHaContext.getLeaseTime());

        ColumnarHaContext columnarHaContext3 = new ColumnarHaContext("127.0.0.2", 3088, new HashMap<>());
        columnarHaContext3.setLeaseTime(System.currentTimeMillis() + 1000000);
        ColumnarHaManager.refreshColumnarHaContext(columnarHaManager, columnarHaContext3);
        Assert.assertEquals("127.0.0.2", columnarHaManager.getColumnarHaContext().getCurrAvailableNodeAddr());
        Assert.assertEquals(3088, columnarHaManager.getColumnarHaContext().getCurrRpcPort());
        Assert.assertEquals(columnarHaContext3.getLeaseTime(), columnarHaManager.getColumnarHaContext().getLeaseTime());
    }

    @Test
    public void watchDogTest3() throws Exception {
        try (
            final MockedStatic<ColumnarHaManager> columnarHaManagerMockedStatic = mockStatic(ColumnarHaManager.class)) {
            ColumnarHaContext chc = mock(ColumnarHaContext.class);
            columnarHaManagerMockedStatic.when(ColumnarHaManager::buildColumnarHaContextFromMetaDb).thenReturn(chc);
            columnarHaManagerMockedStatic.when(
                    () -> ColumnarHaManager.refreshColumnarHaContext(Mockito.any(), Mockito.any()))
                .thenAnswer(invocationOnMock -> null);
            ColumnarHaManager columnarHaManager = new ColumnarHaManager();

            Thread thread = new Thread(() -> ColumnarHaManager.columnarHaWatchdog(columnarHaManager));
            thread.start();

            Thread.sleep(500);
            thread.interrupt();
            thread.join();

            ColumnarHaContext columnarHaContext = new ColumnarHaContext("127.0.0.1", 3080, new HashMap<>());
            columnarHaContext.setLeaseTime(System.currentTimeMillis() + 50000);
            columnarHaManager.setColumnarHaContext(columnarHaContext);
            thread = new Thread(() -> ColumnarHaManager.columnarHaWatchdog(columnarHaManager));
            thread.start();

            Thread.sleep(500);
            thread.interrupt();
            thread.join();
        }
    }
}

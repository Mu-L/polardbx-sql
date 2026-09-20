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

import com.alibaba.polardbx.common.trx.ITimestampOracle;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import java.util.HashMap;

// 添加缺少的导入
import com.alibaba.polardbx.config.ConfigDataMode;

public class StorageHaManagerTest {
    @Test
    public void testChangePeriod() {
        final StorageHaManager manager = new StorageHaManager();
        manager.adjustStorageHaTaskPeriod(1000);
        Assert.assertEquals(1000, manager.checkStorageTaskPeriod);
    }

    @Test
    public void testTaskRun() {
        final StorageHaManager manager = Mockito.mock(StorageHaManager.class);
        manager.adjustStorageHaTaskPeriod(1000);

        final StorageHaManager.CheckStorageHaTask checkStorageHaTask = new StorageHaManager.CheckStorageHaTask(manager);
        checkStorageHaTask.run();
    }

    @Test
    public void testStorageHaSwitchTaskRun() throws SQLException {
        boolean before = InstanceVersion.isMYSQL80();
        try (MockedStatic<ConnPoolConfigManager> connPoolConfigManagerMockedStatic
            = Mockito.mockStatic(ConnPoolConfigManager.class);
            MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic
                = Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic
                = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<ITimestampOracle> timestampOracleMockedStatic
                = Mockito.mockStatic(ITimestampOracle.class);
            MockedStatic<DbTopologyManager> dbTopologyManagerMockedStatic
                = Mockito.mockStatic(DbTopologyManager.class)) {
            InstanceVersion.setMYSQL80(true);
            StorageInstHaContext haContext = new StorageInstHaContext();
            haContext.storageInstId = "dn-inst-id";

            ConnPoolConfig connPoolConfig = new ConnPoolConfig();
            ConnPoolConfigManager connPoolConfigManager = Mockito.mock(ConnPoolConfigManager.class);
            Mockito.when(connPoolConfigManager.getConnPoolConfig()).thenReturn(connPoolConfig);
            connPoolConfigManagerMockedStatic.when(ConnPoolConfigManager::getInstance)
                .thenReturn(connPoolConfigManager);

            MetaDbDataSource metaDbDataSource = Mockito.mock(MetaDbDataSource.class);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(metaDbDataSource);

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(), Mockito.any(), Mockito.any()))
                .thenReturn(new ArrayList<>());

            ITimestampOracle timestampOracle = Mockito.mock(ITimestampOracle.class);
            timestampOracleMockedStatic.when(ITimestampOracle::getInstance).thenReturn(timestampOracle);
            Mockito.when(timestampOracle.nextTimestamp()).thenReturn(0L);

            Connection connection = Mockito.mock(Connection.class);
            Mockito.when(connection.createStatement()).thenReturn(Mockito.mock(Statement.class));
            dbTopologyManagerMockedStatic.when(
                    () -> DbTopologyManager.getConnectionForStorage(Mockito.any(StorageInstHaContext.class)))
                .thenReturn(connection);

            StorageHaManager storageHaManager = Mockito.mock(StorageHaManager.class);
            String newAddr = "127.0.0.1:3307";
            Map<String, StorageNodeHaInfo> newStorageNodeHaInfoMap = ImmutableMap.of(
                newAddr, new StorageNodeHaInfo(newAddr, StorageRole.LEADER, true, 13306, "admin", "password", true, 10)
            );

            StorageHaManager.StorageHaSwitchTask storageHaSwitchTask = new StorageHaManager.StorageHaSwitchTask(
                haContext, storageHaManager, newAddr, true, "admin", "password", newStorageNodeHaInfoMap
            );

            storageHaSwitchTask.run();
        } finally {
            InstanceVersion.setMYSQL80(before);
        }

    }

    @Test
    public void testReloadStorageInstsBySpecifyingStorageInstIdList1() throws Exception {
        final StorageHaManager manager = new StorageHaManager();
        ConfigDataMode.setInstanceRole(InstanceRole.COLUMNAR_SLAVE);
        manager.reloadStorageInstsBySpecifyingStorageInstIdList(new ArrayList<>());
    }

    @Test(expected = Throwable.class)
    public void testReloadStorageInstsBySpecifyingStorageInstIdList2() throws Exception {
        final StorageHaManager manager = new StorageHaManager();
        ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        manager.reloadStorageInstsBySpecifyingStorageInstIdList(new ArrayList<>());
    }

    @Test
    public void testRefreshStorageInsts1() throws Exception {
        final StorageHaManager manager = new StorageHaManager();
        ConfigDataMode.setInstanceRole(InstanceRole.COLUMNAR_SLAVE);
        manager.refreshStorageInsts(new ArrayList<>(), new HashMap<>());
    }

    @Test
    public void testRefreshStorageInsts2() throws Exception {
        final StorageHaManager manager = new StorageHaManager();
        ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        manager.refreshStorageInsts(new ArrayList<>(), new HashMap<>());
    }
}
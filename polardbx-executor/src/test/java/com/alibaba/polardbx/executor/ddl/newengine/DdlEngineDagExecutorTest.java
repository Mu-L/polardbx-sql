/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.sync.DestroyPhysicalBackfillDataSourcesSyncAction;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DdlEngineDagExecutorTest {

    private static final Long ROOT_JOB_ID = 123L;

    @Test
    public void testDestroyPhysicalBackfillDataSourcesForPhysicalBackfillJob() {
        DdlJobManager ddlJobManager = Mockito.mock(DdlJobManager.class);
        Mockito.when(ddlJobManager.existPhysicalBackfillTask(ROOT_JOB_ID)).thenReturn(true);

        try (MockedStatic<GmsSyncManagerHelper> mockedSyncManager =
            Mockito.mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<PhysicalBackfillUtils> mockedBackfillUtils =
                Mockito.mockStatic(PhysicalBackfillUtils.class)) {
            DdlEngineDagExecutor.destroyPhysicalBackfillDataSources(ddlJobManager, ROOT_JOB_ID);

            Mockito.verify(ddlJobManager, Mockito.times(1)).existPhysicalBackfillTask(ROOT_JOB_ID);
            mockedSyncManager.verify(() -> GmsSyncManagerHelper.sync(
                Mockito.argThat(action -> isCleanupActionForJob(action, ROOT_JOB_ID)),
                Mockito.eq(SystemDbHelper.DEFAULT_DB_NAME),
                Mockito.eq(SyncScope.MASTER_ONLY),
                Mockito.eq(false)), Mockito.times(1));
            mockedBackfillUtils.verify(
                () -> PhysicalBackfillUtils.destroyDataSources(ROOT_JOB_ID), Mockito.times(1));
        }
    }

    @Test
    public void testDestroyPhysicalBackfillDataSourcesForOrdinaryJob() {
        DdlJobManager ddlJobManager = Mockito.mock(DdlJobManager.class);
        Mockito.when(ddlJobManager.existPhysicalBackfillTask(ROOT_JOB_ID)).thenReturn(false);

        try (MockedStatic<GmsSyncManagerHelper> mockedSyncManager =
            Mockito.mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<PhysicalBackfillUtils> mockedBackfillUtils =
                Mockito.mockStatic(PhysicalBackfillUtils.class)) {
            DdlEngineDagExecutor.destroyPhysicalBackfillDataSources(ddlJobManager, ROOT_JOB_ID);

            Mockito.verify(ddlJobManager, Mockito.times(1)).existPhysicalBackfillTask(ROOT_JOB_ID);
            mockedSyncManager.verifyNoInteractions();
            mockedBackfillUtils.verify(
                () -> PhysicalBackfillUtils.destroyDataSources(ROOT_JOB_ID), Mockito.times(1));
        }
    }

    @Test
    public void testLocalCleanupContinuesWhenSyncFails() {
        DdlJobManager ddlJobManager = Mockito.mock(DdlJobManager.class);
        Mockito.when(ddlJobManager.existPhysicalBackfillTask(ROOT_JOB_ID)).thenReturn(true);

        try (MockedStatic<GmsSyncManagerHelper> mockedSyncManager =
            Mockito.mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<PhysicalBackfillUtils> mockedBackfillUtils =
                Mockito.mockStatic(PhysicalBackfillUtils.class)) {
            mockedSyncManager.when(() -> GmsSyncManagerHelper.sync(
                Mockito.any(IGmsSyncAction.class),
                Mockito.eq(SystemDbHelper.DEFAULT_DB_NAME),
                Mockito.eq(SyncScope.MASTER_ONLY),
                Mockito.eq(false))).thenThrow(new RuntimeException("sync failed"));

            DdlEngineDagExecutor.destroyPhysicalBackfillDataSources(ddlJobManager, ROOT_JOB_ID);

            mockedSyncManager.verify(() -> GmsSyncManagerHelper.sync(
                Mockito.argThat(action -> isCleanupActionForJob(action, ROOT_JOB_ID)),
                Mockito.eq(SystemDbHelper.DEFAULT_DB_NAME),
                Mockito.eq(SyncScope.MASTER_ONLY),
                Mockito.eq(false)), Mockito.times(1));
            mockedBackfillUtils.verify(
                () -> PhysicalBackfillUtils.destroyDataSources(ROOT_JOB_ID), Mockito.times(1));
        }
    }

    @Test
    public void testCleanupBroadcastsWhenTaskLookupFails() {
        DdlJobManager ddlJobManager = Mockito.mock(DdlJobManager.class);
        Mockito.when(ddlJobManager.existPhysicalBackfillTask(ROOT_JOB_ID))
            .thenThrow(new RuntimeException("lookup failed"));

        try (MockedStatic<GmsSyncManagerHelper> mockedSyncManager =
            Mockito.mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<PhysicalBackfillUtils> mockedBackfillUtils =
                Mockito.mockStatic(PhysicalBackfillUtils.class)) {
            DdlEngineDagExecutor.destroyPhysicalBackfillDataSources(ddlJobManager, ROOT_JOB_ID);

            mockedSyncManager.verify(() -> GmsSyncManagerHelper.sync(
                Mockito.argThat(action -> isCleanupActionForJob(action, ROOT_JOB_ID)),
                Mockito.eq(SystemDbHelper.DEFAULT_DB_NAME),
                Mockito.eq(SyncScope.MASTER_ONLY),
                Mockito.eq(false)), Mockito.times(1));
            mockedBackfillUtils.verify(
                () -> PhysicalBackfillUtils.destroyDataSources(ROOT_JOB_ID), Mockito.times(1));
        }
    }

    private static boolean isCleanupActionForJob(IGmsSyncAction action, Long rootJobId) {
        return action instanceof DestroyPhysicalBackfillDataSourcesSyncAction
            && rootJobId.equals(((DestroyPhysicalBackfillDataSourcesSyncAction) action).getRootJobId());
    }
}

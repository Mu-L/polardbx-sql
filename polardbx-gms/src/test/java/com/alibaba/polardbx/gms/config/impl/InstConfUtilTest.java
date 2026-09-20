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

package com.alibaba.polardbx.gms.config.impl;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.StringConfigParam;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Calendar;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class InstConfUtilTest {
    @Test
    public void testGetBool() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "true");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            Assert.assertTrue(InstConfUtil.getValBool(ConnectionProperties.ALERT_STATISTIC_INTERRUPT));
        }
    }

    @Test
    public void testRemainTimeInMaintenanceTimeWindow(){

        String MAINTENANCE_TIME_START = "02:00";

        String MAINTENANCE_TIME_END = "05:00";

        Assert.assertEquals(InstConfUtil.remainTimeInMaintenanceTimeWindow(new Calendar.Builder().setTimeOfDay(3,30,0).build(),
                MAINTENANCE_TIME_START, MAINTENANCE_TIME_END), 90 * 60);

        Assert.assertEquals(InstConfUtil.remainTimeInMaintenanceTimeWindow(new Calendar.Builder().setTimeOfDay(23,30,0).build(),
                MAINTENANCE_TIME_END, MAINTENANCE_TIME_START), 150 * 60);
        Assert.assertEquals(InstConfUtil.remainTimeInMaintenanceTimeWindow(new Calendar.Builder().setTimeOfDay(1,30,0).build(),
                MAINTENANCE_TIME_END, MAINTENANCE_TIME_START), 30 * 60);
        Assert.assertEquals(InstConfUtil.remainTimeInMaintenanceTimeWindow(new Calendar.Builder().setTimeOfDay(3,30,0).build(),
                "", ""), 0);

    }

}

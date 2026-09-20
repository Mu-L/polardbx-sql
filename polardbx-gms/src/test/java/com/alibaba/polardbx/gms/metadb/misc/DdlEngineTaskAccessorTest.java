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

package com.alibaba.polardbx.gms.metadb.misc;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DdlEngineTaskAccessorTest {

    @Test
    public void testExistPhysicalBackfillTaskByRootJobId() throws Exception {
        long rootJobId = 123L;
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getInt(1)).thenReturn(1);

        DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
        accessor.setConnection(connection);

        Assert.assertTrue(accessor.existPhysicalBackfillTask(rootJobId));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(connection).prepareStatement(sqlCaptor.capture());
        Assert.assertTrue(sqlCaptor.getValue().contains(
            "where `root_job_id` = ? and `name` = ? limit 1"));
        Mockito.verify(preparedStatement).setLong(1, rootJobId);
        Mockito.verify(preparedStatement).setString(2, "PhysicalBackfillTask");
    }
}

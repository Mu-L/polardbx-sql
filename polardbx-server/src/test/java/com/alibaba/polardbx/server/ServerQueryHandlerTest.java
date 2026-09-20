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

package com.alibaba.polardbx.server;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.handler.NaturalLanguageHandler;
import com.alibaba.polardbx.server.response.PurgeTransHandler;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ServerQueryHandlerTest {
    @Test
    public void testQueryRaw() throws Exception {
        final ServerConnection serverConnection = mock(ServerConnection.class);
        final ServerQueryHandler serverQueryHandler = spy(new ServerQueryHandler(serverConnection));
        when(serverConnection.isEnableANSIQuotes()).thenReturn(false);

        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        final CobarConfig cobarConfig = Mockito.mock(CobarConfig.class);
        Field field = CobarServer.class.getDeclaredField("config");
        field.setAccessible(true); // Make the private field accessible
        field.set(CobarServer.getInstance(), cobarConfig);
        when(cobarConfig.isLock()).thenReturn(false);

        try (MockedStatic<NaturalLanguageHandler> naturalLanguageHandler = Mockito.mockStatic(
            NaturalLanguageHandler.class)) {
            naturalLanguageHandler.when(() -> NaturalLanguageHandler.isEnabled(serverConnection)).thenReturn(false);

            doReturn(CompletableFuture.completedFuture(true)).when(serverQueryHandler)
                .executeStatement(any(), any(), anyBoolean());

            final String sql = "select 1;select 2";
            serverQueryHandler.queryRaw(sql.getBytes(), 0, sql.length(), Charset.defaultCharset());

            doReturn(CompletableFuture.completedFuture(false)).when(serverQueryHandler)
                .executeStatement(any(), any(), anyBoolean());

            serverQueryHandler.queryRaw(sql.getBytes(), 0, sql.length(), Charset.defaultCharset());

            doThrow(new RuntimeException("test throw")).when(serverQueryHandler)
                .executeStatement(any(), any(), anyBoolean());

            try {
                serverQueryHandler.queryRaw(sql.getBytes(), 0, sql.length(), Charset.defaultCharset());
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("test throw"));
            }
        }
    }

    @Test
    public void testExecuteStatement_PurgeTrans() throws Exception {
        final ServerConnection serverConnection = mock(ServerConnection.class);
        final ServerQueryHandler serverQueryHandler = new ServerQueryHandler(serverConnection);

        try (MockedConstruction<PurgeTransHandler> purgeTransHandlerMock = mockConstruction(
            PurgeTransHandler.class, (mock, context) -> when(mock.execute()).thenReturn(true))) {

            final ByteString sql = ByteString.from("PURGE TRANS BEFORE 7");
            final boolean result =
                serverQueryHandler.executeStatement(serverConnection, sql, false).get();

            Assert.assertTrue(result);
            Assert.assertEqual(1, purgeTransHandlerMock.constructed().size());
        }
    }
}

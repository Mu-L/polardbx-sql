package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.pl.inner.JdbcUrlShowProcedure;
import com.alibaba.polardbx.server.response.JdbcUrlRecordSyncAction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcUrlShowProcedureTest {

    @Test
    public void testJdbcUrlShowProcedure() {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<GmsSyncManagerHelper> syncManagerMock = Mockito.mockStatic(GmsSyncManagerHelper.class);
            MockedStatic<CobarServer> cobarServerMock = Mockito.mockStatic(CobarServer.class)) {

            // 1. Setup mock CobarServer with processors and connections
            CobarServer mockServer = Mockito.mock(CobarServer.class);
            cobarServerMock.when(CobarServer::getInstance).thenReturn(mockServer);

            NIOProcessor mockProcessor = Mockito.mock(NIOProcessor.class);
            Mockito.when(mockServer.getProcessors()).thenReturn(new NIOProcessor[] {mockProcessor});

            Map<Integer, FrontendConnection> frontends = new HashMap<>();

            // Create 3 test connections with different states
            ServerConnection conn1 = Mockito.mock(ServerConnection.class);
            Mockito.when(conn1.getJdbcUrl()).thenReturn("jdbc:mysql://host1:3306/db1");
            Mockito.when(conn1.getId()).thenReturn(1L);
            Mockito.when(conn1.getUser()).thenReturn("user1");
            Mockito.when(conn1.getHost()).thenReturn("host1");
            Mockito.when(conn1.getPort()).thenReturn(3306);
            Mockito.when(conn1.getSchema()).thenReturn("db1");
            Mockito.when(conn1.getLastActiveTime()).thenReturn(System.nanoTime() - 5000000000L); // 5 sec ago

            ServerConnection conn2 = Mockito.mock(ServerConnection.class);
            Mockito.when(conn2.getJdbcUrl()).thenReturn("jdbc:mysql://host2:3306/db2");
            Mockito.when(conn2.getId()).thenReturn(2L);
            Mockito.when(conn2.getUser()).thenReturn("user2");
            Mockito.when(conn2.getHost()).thenReturn("host2");
            Mockito.when(conn2.getPort()).thenReturn(3306);
            Mockito.when(conn2.getSchema()).thenReturn("db2");
            Mockito.when(conn2.getLastActiveTime()).thenReturn(System.nanoTime() - 10000000000L); // 10 sec ago

            // Connection without JDBC URL (should be filtered out)
            ServerConnection conn3 = Mockito.mock(ServerConnection.class);
            Mockito.when(conn3.getJdbcUrl()).thenReturn(null);

            frontends.put(1, conn1);
            frontends.put(2, conn2);
            frontends.put(3, conn3);

            // 2. Setup mock sync result
            List<List<Map<String, Object>>> syncResult = new ArrayList<>();
            List<Map<String, Object>> metricList = new ArrayList<>();

            Map<String, Object> metric1 = new HashMap<>();
            metric1.put("ID", 100L);
            metric1.put("USER", "sync_user1");
            metric1.put("HOST", "sync_host1:3306");
            metric1.put("DB", "sync_db1");
            metric1.put("TIME", 15L);
            metric1.put("JDBC_CONNECTION", "sync_jdbc1");
            metricList.add(metric1);

            Map<String, Object> metric2 = new HashMap<>();
            metric2.put("ID", 200L);
            metric2.put("USER", "sync_user2");
            metric2.put("HOST", "sync_host2:3306");
            metric2.put("DB", "sync_db2");
            metric2.put("TIME", 25L);
            metric2.put("JDBC_CONNECTION", "sync_jdbc2");
            metricList.add(metric2);

            syncResult.add(metricList);

            syncManagerMock.when(() ->
                GmsSyncManagerHelper.sync(Mockito.any(JdbcUrlRecordSyncAction.class), Mockito.anyString(), Mockito.any(
                    SyncScope.class))
            ).thenReturn(syncResult);

            // 3. Execute the procedure
            SQLCallStatement statement = Mockito.mock(SQLCallStatement.class);
            ArrayResultCursor cursor = new ArrayResultCursor("jdbc_url_show");
            JdbcUrlShowProcedure procedure = new JdbcUrlShowProcedure();
            procedure.execute(null, statement, cursor);

            // 4. Verify results - should contain both local and sync results
            List<Row> rows = cursor.getRows();
            Assert.assertEquals(2, rows.size()); // 2 from sync + 2 from local

            // Verify sync results
            Assert.assertEquals(100L, rows.get(0).getObject(0));
            Assert.assertEquals("sync_user1", rows.get(0).getObject(1));
            Assert.assertEquals("sync_host1:3306", rows.get(0).getObject(2));
            Assert.assertEquals("sync_db1", rows.get(0).getObject(3));
            Assert.assertEquals(15L, rows.get(0).getObject(4));
            Assert.assertEquals("sync_jdbc1", rows.get(0).getObject(5));

            // Verify sync results
            Assert.assertEquals(200L, rows.get(1).getObject(0));
            Assert.assertEquals("sync_user2", rows.get(1).getObject(1));
            Assert.assertEquals("sync_host2:3306", rows.get(1).getObject(2));
            Assert.assertEquals("sync_db2", rows.get(1).getObject(3));
            Assert.assertEquals(25L, rows.get(1).getObject(4));
            Assert.assertEquals("sync_jdbc2", rows.get(1).getObject(5));

        } finally {
            ConfigDataMode.setMode(mode);
        }
    }
}
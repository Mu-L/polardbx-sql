package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.stats.MatrixStatistics;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ShowStatsTest {

    @Test
    public void testGetRow() {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (final MockedConstruction<CobarServer> cobarServerMockedConstruction =
            mockConstruction(CobarServer.class, (mock, context) -> {
            });
            MockedStatic<CobarServer> cobarServerMockedStatic = mockStatic(CobarServer.class)) {
            CobarServer cobarServer = mock(CobarServer.class);
            cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);

            when(cobarServer.getServerExecutor()).thenReturn(mock(ServerThreadPool.class));
            TDataSource tDataSource = mock(TDataSource.class);
            MatrixStatistics matrixStatistics = new MatrixStatistics(false);
            when(tDataSource.getStatistics()).thenReturn(matrixStatistics);
            RowDataPacket row = ShowStats.getRow(tDataSource, "utf8");
            assert row != null;
        } finally {
            ConfigDataMode.setMode(mode);
        }

    }
}

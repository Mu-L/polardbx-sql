package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.rpc.ColumnarDeltaRpcClient;
import com.alibaba.polardbx.rpc.columnar.ColumnarDeltaRequest;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class ShowResponseTest {

    private ServerConnection c = Mockito.mock(ServerConnection.class);

    @Before
    public void setUp() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
        Mockito.when(c.allocate()).thenReturn(new ByteBufferHolder(byteBuffer));
        Mockito.when(c.checkWriteBuffer(Mockito.any(), Mockito.anyInt()))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        Mockito.doCallRealMethod().when(c).writeToBuffer(Mockito.any(), Mockito.any());
        Mockito.when(c.writeToBuffer(Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.any()))
            .thenAnswer(invocationOnMock -> {
                byte[] src = invocationOnMock.getArgument(0);
                int offset = invocationOnMock.getArgument(1);
                int length = invocationOnMock.getArgument(2);
                ByteBufferHolder bufferHolder = invocationOnMock.getArgument(3);

                bufferHolder.put(src, offset, length);
                return bufferHolder;
            });
    }

    @Test
    public void testShowDeltaStatus() {
        ColumnarDeltaRpcClient mockClient = new ColumnarDeltaRpcClient();
        try (MockedStatic<ColumnarDeltaRpcClient> mockStatic = Mockito.mockStatic(ColumnarDeltaRpcClient.class)) {
            mockStatic.when(ColumnarDeltaRpcClient::getInstance).thenReturn(mockClient);
            ShowDeltaStatus.execute(c);
        }
    }

    @Test
    public void testShowDeltaConnection() {
        ColumnarDeltaRpcClient mockClient = new ColumnarDeltaRpcClient();
        mockClient.register(new ColumnarDeltaRpcClient.StreamStats(
            ColumnarDeltaRequest.newBuilder().setFileName("test.orc").setOffset(456).setLength(789).build(), "123"));
        try (MockedStatic<ColumnarDeltaRpcClient> mockStatic = Mockito.mockStatic(ColumnarDeltaRpcClient.class)) {
            mockStatic.when(ColumnarDeltaRpcClient::getInstance).thenReturn(mockClient);
            ShowDeltaConnection.execute(c);
        }
    }
}

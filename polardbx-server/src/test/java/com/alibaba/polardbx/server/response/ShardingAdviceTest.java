package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.executor.whatIf.ShardingWhatIf;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.optimizer.sharding.advisor.ShardResultForOutput;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for ShardingAdvice response
 *
 * @author shengyu
 */
public class ShardingAdviceTest {

    private ServerConnection mockConnection;

    @Before
    public void setUp() {
        // Mock ServerConnection
        mockConnection = mock(ServerConnection.class);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8192);
        when(mockConnection.allocate()).thenReturn(new ByteBufferHolder(byteBuffer));
        when(mockConnection.checkWriteBuffer(any(), anyInt()))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        doCallRealMethod().when(mockConnection).writeToBuffer(any(), any());
        when(mockConnection.writeToBuffer(any(), anyInt(), anyInt(), any()))
            .thenAnswer(invocationOnMock -> {
                byte[] src = invocationOnMock.getArgument(0);
                int offset = invocationOnMock.getArgument(1);
                int length = invocationOnMock.getArgument(2);
                ByteBufferHolder bufferHolder = invocationOnMock.getArgument(3);
                bufferHolder.put(src, offset, length);
                return bufferHolder;
            });
        when(mockConnection.getResultSetCharset()).thenReturn("utf8");
        when(mockConnection.getSchema()).thenReturn("testSchema");
        when(mockConnection.isEofDeprecated()).thenReturn(false);
    }

    @Test
    public void testResponseMultipleTimes() {
        // Mock ShardResultForOutput
        ShardResultForOutput mockResult = mock(ShardResultForOutput.class);

        // Mock ShardingWhatIf
        ShardingWhatIf mockWhatIf = mock(ShardingWhatIf.class);

        // Execute response multiple times to verify consistency
        boolean result1 = ShardingAdvice.response(mockConnection, false, mockResult, mockWhatIf);
        boolean result2 = ShardingAdvice.response(mockConnection, true, mockResult, mockWhatIf);
        boolean result3 = ShardingAdvice.response(mockConnection, false, mockResult, mockWhatIf);

        // Verify all results are true
        assertTrue("First call should return true", result1);
        assertTrue("Second call should return true", result2);
        assertTrue("Third call should return true", result3);

        // Verify allocate was called for each execution
        verify(mockConnection, Mockito.times(3)).allocate();
    }
}

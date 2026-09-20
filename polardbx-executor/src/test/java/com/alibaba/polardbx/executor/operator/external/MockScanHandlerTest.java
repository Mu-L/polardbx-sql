package com.alibaba.polardbx.executor.operator.external;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MockScanHandlerTest {

    private BlockBuilder[] mockBlockBuilders(int count) {
        BlockBuilder[] builders = new BlockBuilder[count];
        for (int i = 0; i < count; i++) {
            builders[i] = mock(BlockBuilder.class);
            when(builders[i].build()).thenReturn(mock(Block.class));
        }
        return builders;
    }

    @Test
    public void testOpenWithMockData() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.data", "1,alice,95|2,bob,88|3,carol,72");
        List<DataType> types = Arrays.asList(
            DataTypes.IntegerType, DataTypes.StringType, DataTypes.IntegerType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(3);
        Chunk chunk = handler.nextChunk(builders, 10);
        assertNotNull(chunk);
    }

    @Test
    public void testOpenWithNativeQueryData() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("native_query_sql", "SELECT 1");
        options.put("mock.native_query.data", "10,hello|20,world");
        List<DataType> types = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(2);
        Chunk chunk = handler.nextChunk(builders, 10);
        assertNotNull(chunk);
    }

    @Test
    public void testOpenWithTableSpecificData() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.mydb.mytable.data", "1,a|2,b");
        List<DataType> types = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(2);
        Chunk chunk = handler.nextChunk(builders, 10);
        assertNotNull(chunk);
    }

    @Test
    public void testOpenEmptyData() throws IOException {
        Map<String, String> options = new HashMap<>();
        List<DataType> types = Arrays.asList(DataTypes.StringType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(1);
        Chunk chunk = handler.nextChunk(builders, 10);
        assertNull(chunk);
    }

    @Test
    public void testNextChunkExhausted() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.data", "1,a");
        List<DataType> types = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(2);
        Chunk first = handler.nextChunk(builders, 10);
        assertNotNull(first);
        Chunk second = handler.nextChunk(builders, 10);
        assertNull(second);
    }

    @Test
    public void testWriteValueTypes() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.data", "100,123456789012,3.14,hello,null,,abc");
        List<DataType> types = Arrays.asList(
            DataTypes.IntegerType,
            DataTypes.LongType,
            DataTypes.DoubleType,
            DataTypes.StringType,
            DataTypes.StringType,
            DataTypes.StringType,
            DataTypes.IntegerType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(7);
        Chunk chunk = handler.nextChunk(builders, 10);
        assertNotNull(chunk);

        verify(builders[0]).writeInt(100);
        verify(builders[1]).writeLong(123456789012L);
        verify(builders[2]).writeDouble(3.14);
        verify(builders[3]).writeString("hello");
        verify(builders[4]).appendNull();
        verify(builders[5]).appendNull();
        verify(builders[6]).writeString("abc");
    }

    @Test
    public void testMissingColumnAppendsNull() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.data", "1");
        List<DataType> types = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(2);
        Chunk chunk = handler.nextChunk(builders, 10);
        assertNotNull(chunk);

        verify(builders[0]).writeInt(1);
        verify(builders[1]).appendNull();
    }

    @Test
    public void testChunkLimit() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.data", "1,a|2,b|3,c|4,d");
        List<DataType> types = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();

        BlockBuilder[] builders = mockBlockBuilders(2);
        Chunk first = handler.nextChunk(builders, 2);
        assertNotNull(first);
        Chunk second = handler.nextChunk(builders, 2);
        assertNotNull(second);
        Chunk third = handler.nextChunk(builders, 2);
        assertNull(third);
    }

    @Test
    public void testClose() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("mock.data", "1,a");
        List<DataType> types = Arrays.asList(DataTypes.IntegerType);

        MockScanHandler handler = new MockScanHandler(options, types);
        handler.open();
        handler.close();
    }

    @Test
    public void testGetOutputTypes() {
        List<DataType> types = Arrays.asList(DataTypes.IntegerType, DataTypes.StringType);
        MockScanHandler handler = new MockScanHandler(new HashMap<>(), types);
        assertEquals(types, handler.getOutputTypes());
    }
}

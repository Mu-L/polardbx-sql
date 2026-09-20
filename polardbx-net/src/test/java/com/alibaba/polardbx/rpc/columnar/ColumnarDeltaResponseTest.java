package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import org.junit.Test;
import static org.junit.Assert.*;

public class ColumnarDeltaResponseTest {

    // Case1: 默认值测试
    @Test
    public void testDefaultInstance() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();

        assertEquals(ColumnarDeltaResponse.Status.OK, response.getStatus());
        assertEquals(ByteString.EMPTY, response.getData());
        assertEquals("", response.getErrorMessage());
    }

    // Case2: 全字段构造测试
    @Test
    public void testFullFieldConstruction() {
        // 测试所有状态枚举值
        for (ColumnarDeltaResponse.Status status : ColumnarDeltaResponse.Status.values()) {
            if (status == ColumnarDeltaResponse.Status.UNRECOGNIZED) continue;

            ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
                .setStatus(status)
                .setData(ByteString.copyFromUtf8("test_data_" + status.getNumber()))
                .setErrorMessage("error_" + status.getNumber())
                .build();

            assertEquals(status, response.getStatus());
            assertTrue(response.getData().size() > 0);
            assertTrue(response.getErrorMessage().startsWith("error_"));
        }
    }

    // Case4: 序列化/反序列化一致性
    @Test
    public void testSerializationRoundTrip() throws Exception {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(bytes);

        assertEquals(original, parsed);
        assertEquals(original.hashCode(), parsed.hashCode());
    }

    // Case7: 非法枚举值处理
    @Test
    public void testUnrecognizedStatus() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setStatusValue(99) // 非法枚举值
            .build();

        assertEquals(ColumnarDeltaResponse.Status.UNRECOGNIZED, response.getStatus());
        assertEquals(99, response.getStatusValue());
    }

    // Case8: equals/hashCode一致性
    @Test
    public void testEqualsAndHashCode() {
        ColumnarDeltaResponse response1 = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.RATE_LIMITED)
            .setData(ByteString.copyFromUtf8("data"))
            .setErrorMessage("error")
            .build();

        ColumnarDeltaResponse response2 = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.RATE_LIMITED)
            .setData(ByteString.copyFromUtf8("data"))
            .setErrorMessage("error")
            .build();

        assertTrue(response1.equals(response2));
        assertEquals(response1.hashCode(), response2.hashCode());
    }

    // Case9: Builder功能验证
    @Test
    public void testBuilderClear() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.INTERNAL_ERROR)
            .setData(ByteString.copyFromUtf8("data"))
            .setErrorMessage("error");

        builder.clear();
        ColumnarDeltaResponse cleared = builder.build();

        assertEquals(ColumnarDeltaResponse.Status.OK, cleared.getStatus());
        assertEquals(ByteString.EMPTY, cleared.getData());
        assertEquals("", cleared.getErrorMessage());
    }
}
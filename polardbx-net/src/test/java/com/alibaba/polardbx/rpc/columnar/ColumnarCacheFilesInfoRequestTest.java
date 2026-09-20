package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class ColumnarCacheFilesInfoRequestTest {

    // 测试用例1：验证默认实例有效性
    @Test
    public void testDefaultInstance() {
        ColumnarCacheFilesInfoRequest instance = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertNotNull("默认实例不应为空", instance);
        assertSame("多次调用应返回相同实例",
            instance,
            ColumnarCacheFilesInfoRequest.getDefaultInstance());
    }

    // 测试用例2：验证Builder构造
    @Test
    public void testBuilder() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        assertNotNull("Builder实例不应为空", builder);
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals("构建实例应与默认实例相等",
            ColumnarCacheFilesInfoRequest.getDefaultInstance(),
            request);
    }

    // 测试用例3：空消息序列化验证
    @Test
    public void testEmptySerialization() throws IOException {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();

        // 验证序列化大小
        assertEquals("空消息序列化大小应为0", 0, request.getSerializedSize());

        // 验证实际序列化
        byte[] bytes = request.toByteArray();
        assertEquals("序列化字节数组长度应为0", 0, bytes.length);
    }

    // 测试用例4：全格式反序列化验证
    @Test
    public void testParsingFromAllFormats() throws IOException {
        byte[] emptyData = new byte[0];

        // 测试ByteBuffer解析
        ColumnarCacheFilesInfoRequest fromBuffer = ColumnarCacheFilesInfoRequest.parseFrom(
            ByteBuffer.wrap(emptyData));

        // 测试InputStream解析
        ColumnarCacheFilesInfoRequest fromStream = ColumnarCacheFilesInfoRequest.parseFrom(
            new ByteArrayInputStream(emptyData));

        // 测试ByteString解析
        ColumnarCacheFilesInfoRequest fromByteString = ColumnarCacheFilesInfoRequest.parseFrom(
            ByteString.copyFrom(emptyData));

        assertEquals("不同解析方式应得到相同实例", fromBuffer, fromStream);
        assertEquals("不同解析方式应得到相同实例", fromStream, fromByteString);
    }

    // 测试用例5：相等性验证
    @Test
    public void testEquality() {
        ColumnarCacheFilesInfoRequest instance1 = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest instance2 = ColumnarCacheFilesInfoRequest.newBuilder().build();

        assertTrue("默认实例与新建实例应相等", instance1.equals(instance2));
        assertEquals("哈希码应相同", instance1.hashCode(), instance2.hashCode());
        assertFalse("与null比较应返回false", instance1.equals(null));
        assertFalse("与其他类型对象比较应返回false", instance1.equals(new Object()));
    }

    // 测试用例6：异常情况验证
    @Test(expected = InvalidProtocolBufferException.class)
    public void testInvalidParsing() throws IOException {
        // 构造无效数据（示例：非空的无效数据）
        byte[] invalidData = new byte[]{0x0A, 0x0B}; // 包含未定义字段的数据
        ColumnarCacheFilesInfoRequest.parseFrom(invalidData);
    }

    // 测试用例7：扩展注册表验证
    @Test
    public void testWithExtensionRegistry() throws IOException {
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(
            new byte[0],
            com.google.protobuf.ExtensionRegistryLite.getEmptyRegistry());

        assertEquals("应正确解析空数据",
            ColumnarCacheFilesInfoRequest.getDefaultInstance(),
            parsed);
    }
}

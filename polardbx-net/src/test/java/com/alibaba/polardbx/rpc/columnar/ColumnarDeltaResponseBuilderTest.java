package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageV3;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ColumnarDeltaResponseBuilderTest {

    private ColumnarDeltaResponse.Builder builder;
    private ColumnarDeltaResponse.Builder otherBuilder;

    @Before
    public void setUp() {
        // 初始化 Builder
        builder = ColumnarDeltaResponse.newBuilder();
        otherBuilder = ColumnarDeltaResponse.newBuilder();
    }

    /**
     * 测试 getDescriptor 方法
     */
    @Test
    public void testGetDescriptor() {
        Descriptors.Descriptor descriptor = ColumnarDeltaResponse.getDescriptor();
        assertNotNull("Descriptor should not be null", descriptor);
    }

    /**
     * 测试 internalGetFieldAccessorTable 方法
     */
    @Test
    public void testInternalGetFieldAccessorTable() {
        ColumnarDeltaResponse response = builder.build();
        GeneratedMessageV3.FieldAccessorTable fieldAccessorTable = response.internalGetFieldAccessorTable();
        assertNotNull("FieldAccessorTable should not be null", fieldAccessorTable);
    }

    /**
     * 测试 clear 方法
     */
    @Test
    public void testClear() {
        builder.setStatusValue(1).setData(ByteString.copyFromUtf8("test"));
        builder.clear();
        assertEquals("Status should be reset to default", 0, builder.getStatusValue());
        assertTrue("Data should be empty", builder.getData().isEmpty());
    }

    /**
     * 测试 clone 方法
     */
    @Test
    public void testClone() {
        builder.setStatusValue(1).setData(ByteString.copyFromUtf8("test"));
        ColumnarDeltaResponse.Builder clonedBuilder = builder.clone();
        assertEquals("Cloned builder should have the same status", 1, clonedBuilder.getStatusValue());
        assertEquals("Cloned builder should have the same data", ByteString.copyFromUtf8("test"), clonedBuilder.getData());
    }

    /**
     * 测试 mergeFrom 方法
     */
    @Test
    public void testMergeFrom() {
        ColumnarDeltaResponse other = otherBuilder
            .setStatusValue(2)
            .setData(ByteString.copyFromUtf8("otherData"))
            .build();
        builder.mergeFrom(other);
        assertEquals("Status should be merged", 2, builder.getStatusValue());
        assertEquals("Data should be merged", ByteString.copyFromUtf8("otherData"), builder.getData());
    }

    /**
     * 测试 isInitialized 方法
     */
    @Test
    public void testIsInitialized() {
        assertTrue("Object should always be initialized", builder.isInitialized());
    }

    /**
     * 测试 getStatusValue 和 setStatusValue 方法
     */
    @Test
    public void testStatusValue() {
        builder.setStatusValue(3);
        assertEquals("Status value should match", 3, builder.getStatusValue());
    }

    /**
     * 测试 getData 和 setData 方法
     */
    @Test
    public void testGetDataAndSetData() {
        ByteString data = ByteString.copyFromUtf8("testData");
        builder.setData(data);
        assertEquals("Data should match", data, builder.getData());
    }

    /**
     * 测试 getErrorMessage 和 setErrorMessage 方法
     */
    @Test
    public void testErrorMessage() {
        builder.setErrorMessage("Error occurred");
        assertEquals("Error message should match", "Error occurred", builder.getErrorMessage());
    }

    /**
     * 测试 mergeFrom(CodedInputStream, ExtensionRegistryLite) 方法
     */
    @Test
    public void testMergeFromCodedInputStream() throws IOException {
        ColumnarDeltaResponse.Builder partialBuilder = otherBuilder;
        ExtensionRegistryLite registry = ExtensionRegistryLite.newInstance();

        // 模拟输入流
        byte[] bytes = partialBuilder.build().toByteArray();
        try (MockedStatic<ColumnarDeltaResponse> mockedParser = Mockito.mockStatic(ColumnarDeltaResponse.class)) {
            mockedParser.when(() -> ColumnarDeltaResponse.parseFrom(bytes, registry))
                .thenReturn(partialBuilder.build());

            builder.mergeFrom(partialBuilder.build().toByteArray(), registry);
            assertEquals("Merged object should match", partialBuilder.build(), builder.build());
        }
    }
}

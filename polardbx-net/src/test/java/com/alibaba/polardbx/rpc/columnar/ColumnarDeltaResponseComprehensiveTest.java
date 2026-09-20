package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.*;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ColumnarDeltaResponseComprehensiveTest {

    // 测试默认实例
    @Test
    public void testDefaultInstance() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        assertNotNull(response);
        assertEquals(ColumnarDeltaResponse.Status.OK, response.getStatus());
        assertEquals(ByteString.EMPTY, response.getData());
        assertEquals("", response.getErrorMessage());
    }

    // 测试getUnknownFields方法
    @Test
    public void testGetUnknownFields() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        UnknownFieldSet unknownFields = response.getUnknownFields();
        assertNotNull(unknownFields);
    }

    // 测试getDescriptor方法
    @Test
    public void testGetDescriptor() {
        Descriptors.Descriptor descriptor = ColumnarDeltaResponse.getDescriptor();
        assertNotNull(descriptor);
        assertEquals("orc.proto.ColumnarDeltaResponse", descriptor.getFullName());
    }

    // 测试Status枚举
    @Test
    public void testStatusEnum() {
        // 测试所有已知的状态值
        assertEquals(0, ColumnarDeltaResponse.Status.OK_VALUE);
        assertEquals(1, ColumnarDeltaResponse.Status.INVALID_ARGUMENT_VALUE);
        assertEquals(2, ColumnarDeltaResponse.Status.NOT_FOUND_VALUE);
        assertEquals(3, ColumnarDeltaResponse.Status.DATA_CORRUPTED_VALUE);
        assertEquals(4, ColumnarDeltaResponse.Status.RATE_LIMITED_VALUE);
        assertEquals(5, ColumnarDeltaResponse.Status.INTERNAL_ERROR_VALUE);
        
        // 测试枚举值的获取
        assertEquals(ColumnarDeltaResponse.Status.OK, ColumnarDeltaResponse.Status.forNumber(0));
        assertEquals(ColumnarDeltaResponse.Status.INVALID_ARGUMENT, ColumnarDeltaResponse.Status.forNumber(1));
        assertEquals(ColumnarDeltaResponse.Status.NOT_FOUND, ColumnarDeltaResponse.Status.forNumber(2));
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, ColumnarDeltaResponse.Status.forNumber(3));
        assertEquals(ColumnarDeltaResponse.Status.RATE_LIMITED, ColumnarDeltaResponse.Status.forNumber(4));
        assertEquals(ColumnarDeltaResponse.Status.INTERNAL_ERROR, ColumnarDeltaResponse.Status.forNumber(5));
        assertNull(ColumnarDeltaResponse.Status.forNumber(99));
        
        // 测试valueOf方法（已废弃）
        assertEquals(ColumnarDeltaResponse.Status.OK, ColumnarDeltaResponse.Status.valueOf(0));
        assertEquals(ColumnarDeltaResponse.Status.INVALID_ARGUMENT, ColumnarDeltaResponse.Status.valueOf(1));
        assertEquals(ColumnarDeltaResponse.Status.NOT_FOUND, ColumnarDeltaResponse.Status.valueOf(2));
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, ColumnarDeltaResponse.Status.valueOf(3));
        assertEquals(ColumnarDeltaResponse.Status.RATE_LIMITED, ColumnarDeltaResponse.Status.valueOf(4));
        assertEquals(ColumnarDeltaResponse.Status.INTERNAL_ERROR, ColumnarDeltaResponse.Status.valueOf(5));
        assertNull(ColumnarDeltaResponse.Status.valueOf(99));
        
        // 测试internalGetValueMap方法
        assertNotNull(ColumnarDeltaResponse.Status.internalGetValueMap());
        
        // 测试getDescriptor方法
        assertNotNull(ColumnarDeltaResponse.Status.getDescriptor());
        
        // 测试getValueDescriptor方法
        assertNotNull(ColumnarDeltaResponse.Status.OK.getValueDescriptor());
        
        // 测试getDescriptorForType方法
        assertNotNull(ColumnarDeltaResponse.Status.OK.getDescriptorForType());
    }

    // 测试getStatusValue方法
    @Test
    public void testGetStatusValue() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .build();
        assertEquals(3, response.getStatusValue());
    }

    // 测试getStatus方法
    @Test
    public void testGetStatus() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .build();
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, response.getStatus());
    }

    // 测试getData方法
    @Test
    public void testGetData() {
        ByteString data = ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03});
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setData(data)
            .build();
        assertEquals(data, response.getData());
    }

    // 测试getErrorMessage方法
    @Test
    public void testGetErrorMessage() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setErrorMessage("test error")
            .build();
        assertEquals("test error", response.getErrorMessage());
    }

    // 测试getErrorMessageBytes方法
    @Test
    public void testGetErrorMessageBytes() {
        String errorMessage = "test error";
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setErrorMessage(errorMessage)
            .build();
        ByteString expectedBytes = ByteString.copyFromUtf8(errorMessage);
        assertEquals(expectedBytes, response.getErrorMessageBytes());
    }

    // 测试isInitialized方法
    @Test
    public void testIsInitialized() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        assertTrue(response.isInitialized());
    }

    // 测试writeTo方法
    @Test
    public void testWriteTo() throws IOException {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        response.writeTo(codedOutputStream);
        codedOutputStream.flush();
        
        byte[] bytes = outputStream.toByteArray();
        assertTrue(bytes.length > 0);
    }

    // 测试getSerializedSize方法
    @Test
    public void testGetSerializedSize() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        int size = response.getSerializedSize();
        assertTrue(size > 0);
    }

    // 测试equals方法（相同对象）
    @Test
    public void testEqualsSameObject() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        assertTrue(response.equals(response));
    }

    // 测试equals方法（不同类对象）
    @Test
    public void testEqualsDifferentClass() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        assertFalse(response.equals("string"));
    }

    // 测试equals方法（相同内容）
    @Test
    public void testEqualsSameContent() {
        ColumnarDeltaResponse response1 = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ColumnarDeltaResponse response2 = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        assertTrue(response1.equals(response2));
    }

    // 测试equals方法（不同内容）
    @Test
    public void testEqualsDifferentContent() {
        ColumnarDeltaResponse response1 = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ColumnarDeltaResponse response2 = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.OK)
            .setData(ByteString.copyFrom(new byte[]{0x04, 0x05, 0x06}))
            .setErrorMessage("different error")
            .build();
            
        assertFalse(response1.equals(response2));
    }

    // 测试hashCode方法
    @Test
    public void testHashCode() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        int hashCode = response.hashCode();
        assertTrue(hashCode != 0);
    }

    // 测试parseFrom方法（ByteBuffer）
    @Test
    public void testParseFromByteBuffer() throws InvalidProtocolBufferException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteBuffer buffer = original.toByteString().asReadOnlyByteBuffer();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(buffer);
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteBuffer，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteBufferWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteBuffer buffer = original.toByteString().asReadOnlyByteBuffer();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(
            buffer, ExtensionRegistryLite.getEmptyRegistry());
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteString）
    @Test
    public void testParseFromByteString() throws InvalidProtocolBufferException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteString byteString = original.toByteString();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(byteString);
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteString，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteStringWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteString byteString = original.toByteString();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(
            byteString, ExtensionRegistryLite.getEmptyRegistry());
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（byte[]）
    @Test
    public void testParseFromByteArray() throws InvalidProtocolBufferException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(bytes);
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（byte[]，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteArrayWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(
            bytes, ExtensionRegistryLite.getEmptyRegistry());
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（InputStream）
    @Test
    public void testParseFromInputStream() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(inputStream);
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（InputStream，带ExtensionRegistryLite）
    @Test
    public void testParseFromInputStreamWithExtensionRegistry() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());
        
        assertEquals(original, parsed);
    }

    // 测试parseDelimitedFrom方法（InputStream）
    @Test
    public void testParseDelimitedFromInputStream() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();
        
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseDelimitedFrom(inputStream);
        
        assertEquals(original, parsed);
    }

    // 测试parseDelimitedFrom方法（InputStream，带ExtensionRegistryLite）
    @Test
    public void testParseDelimitedFromInputStreamWithExtensionRegistry() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();
        
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseDelimitedFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（CodedInputStream）
    @Test
    public void testParseFromCodedInputStream() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(inputStream);
        
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（CodedInputStream，带ExtensionRegistryLite）
    @Test
    public void testParseFromCodedInputStreamWithExtensionRegistry() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        ColumnarDeltaResponse parsed = ColumnarDeltaResponse.parseFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());
        
        assertEquals(original, parsed);
    }

    // 测试newBuilderForType方法
    @Test
    public void testNewBuilderForType() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        ColumnarDeltaResponse.Builder builder = response.newBuilderForType();
        assertNotNull(builder);
    }

    // 测试newBuilder方法
    @Test
    public void testNewBuilder() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        assertNotNull(builder);
    }

    // 测试newBuilder方法（带原型）
    @Test
    public void testNewBuilderWithPrototype() {
        ColumnarDeltaResponse prototype = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder(prototype);
        ColumnarDeltaResponse response = builder.build();
        
        assertEquals(prototype, response);
    }

    // 测试toBuilder方法
    @Test
    public void testToBuilder() {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ColumnarDeltaResponse.Builder builder = original.toBuilder();
        ColumnarDeltaResponse response = builder.build();
        
        assertEquals(original, response);
    }

    // 测试getDefaultInstance方法
    @Test
    public void testGetDefaultInstance() {
        ColumnarDeltaResponse defaultInstance = ColumnarDeltaResponse.getDefaultInstance();
        assertNotNull(defaultInstance);
    }

    // 测试parser方法
    @Test
    public void testParser() {
        Parser<ColumnarDeltaResponse> parser = ColumnarDeltaResponse.parser();
        assertNotNull(parser);
    }

    // 测试getParserForType方法
    @Test
    public void testGetParserForType() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        Parser<ColumnarDeltaResponse> parser = response.getParserForType();
        assertNotNull(parser);
    }

    // 测试getDefaultInstanceForType方法
    @Test
    public void testGetDefaultInstanceForType() {
        ColumnarDeltaResponse response = ColumnarDeltaResponse.getDefaultInstance();
        ColumnarDeltaResponse defaultInstance = response.getDefaultInstanceForType();
        assertNotNull(defaultInstance);
    }

    // 测试Builder的getDescriptor方法
    @Test
    public void testBuilderGetDescriptor() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        Descriptors.Descriptor descriptor = builder.getDescriptor();
        assertNotNull(descriptor);
    }

    // 测试Builder的clear方法
    @Test
    public void testBuilderClear() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error");
            
        builder.clear();
        ColumnarDeltaResponse response = builder.build();
        
        assertEquals(ColumnarDeltaResponse.Status.OK, response.getStatus());
        assertEquals(ByteString.EMPTY, response.getData());
        assertEquals("", response.getErrorMessage());
    }

    // 测试Builder的getDescriptorForType方法
    @Test
    public void testBuilderGetDescriptorForType() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        Descriptors.Descriptor descriptor = builder.getDescriptorForType();
        assertNotNull(descriptor);
    }

    // 测试Builder的getDefaultInstanceForType方法
    @Test
    public void testBuilderGetDefaultInstanceForType() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        ColumnarDeltaResponse defaultInstance = builder.getDefaultInstanceForType();
        assertNotNull(defaultInstance);
    }

    // 测试Builder的build方法
    @Test
    public void testBuilderBuild() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error");
            
        ColumnarDeltaResponse response = builder.build();
        assertNotNull(response);
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, response.getStatus());
        assertEquals(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}), response.getData());
        assertEquals("test error", response.getErrorMessage());
    }

    // 测试Builder的buildPartial方法
    @Test
    public void testBuilderBuildPartial() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error");
            
        ColumnarDeltaResponse response = builder.buildPartial();
        assertNotNull(response);
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, response.getStatus());
        assertEquals(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}), response.getData());
        assertEquals("test error", response.getErrorMessage());
    }

    // 测试Builder的clone方法
    @Test
    public void testBuilderClone() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error");
            
        ColumnarDeltaResponse.Builder clonedBuilder = builder.clone();
        assertNotNull(clonedBuilder);
        assertEquals(3, clonedBuilder.getStatusValue());
        assertEquals(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}), clonedBuilder.getData());
        assertEquals("test error", clonedBuilder.getErrorMessage());
    }

    // 测试Builder的mergeFrom方法（Message）
    @Test
    public void testBuilderMergeFromMessage() {
        ColumnarDeltaResponse other = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.mergeFrom(other);
        
        ColumnarDeltaResponse response = builder.build();
        assertEquals(other, response);
    }

    // 测试Builder的mergeFrom方法（ColumnarDeltaResponse）
    @Test
    public void testBuilderMergeFromColumnarDeltaResponse() {
        ColumnarDeltaResponse other = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.mergeFrom(other);
        
        ColumnarDeltaResponse response = builder.build();
        assertEquals(other, response);
    }

    // 测试Builder的isInitialized方法
    @Test
    public void testBuilderIsInitialized() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        assertTrue(builder.isInitialized());
    }

    // 测试Builder的mergeFrom方法（CodedInputStream, ExtensionRegistryLite）
    @Test
    public void testBuilderMergeFromCodedInputStream() throws IOException {
        ColumnarDeltaResponse original = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED)
            .setData(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03}))
            .setErrorMessage("test error")
            .build();
            
        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.mergeFrom(inputStream, ExtensionRegistryLite.getEmptyRegistry());
        
        ColumnarDeltaResponse response = builder.build();
        assertEquals(original, response);
    }

    // 测试Builder的getStatusValue方法
    @Test
    public void testBuilderGetStatusValue() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED);
        assertEquals(3, builder.getStatusValue());
    }

    // 测试Builder的setStatusValue方法
    @Test
    public void testBuilderSetStatusValue() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setStatusValue(3);
        assertEquals(3, builder.getStatusValue());
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, builder.getStatus());
    }

    // 测试Builder的getStatus方法
    @Test
    public void testBuilderGetStatus() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED);
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, builder.getStatus());
    }

    // 测试Builder的setStatus方法
    @Test
    public void testBuilderSetStatus() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED);
        assertEquals(ColumnarDeltaResponse.Status.DATA_CORRUPTED, builder.getStatus());
        assertEquals(3, builder.getStatusValue());
    }

    // 测试Builder的clearStatus方法
    @Test
    public void testBuilderClearStatus() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setStatus(ColumnarDeltaResponse.Status.DATA_CORRUPTED);
        builder.clearStatus();
        assertEquals(ColumnarDeltaResponse.Status.OK, builder.getStatus());
        assertEquals(0, builder.getStatusValue());
    }

    // 测试Builder的getData方法
    @Test
    public void testBuilderGetData() {
        ByteString data = ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03});
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setData(data);
        assertEquals(data, builder.getData());
    }

    // 测试Builder的setData方法
    @Test
    public void testBuilderSetData() {
        ByteString data = ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03});
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setData(data);
        assertEquals(data, builder.getData());
    }

    // 测试Builder的clearData方法
    @Test(expected = NullPointerException.class)
    public void testBuilderClearDataWithNull() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setData(null); // This should throw NullPointerException
    }

    // 测试Builder的clearData方法
    @Test
    public void testBuilderClearData() {
        ByteString data = ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03});
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setData(data);
        builder.clearData();
        assertEquals(ByteString.EMPTY, builder.getData());
    }

    // 测试Builder的getErrorMessage方法
    @Test
    public void testBuilderGetErrorMessage() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setErrorMessage("test error");
        assertEquals("test error", builder.getErrorMessage());
    }

    // 测试Builder的setErrorMessage方法
    @Test
    public void testBuilderSetErrorMessage() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setErrorMessage("test error");
        assertEquals("test error", builder.getErrorMessage());
    }

    // 测试Builder的clearErrorMessage方法
    @Test(expected = NullPointerException.class)
    public void testBuilderClearErrorMessageWithNull() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setErrorMessage(null); // This should throw NullPointerException
    }

    // 测试Builder的clearErrorMessage方法
    @Test
    public void testBuilderClearErrorMessage() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setErrorMessage("test error");
        builder.clearErrorMessage();
        assertEquals("", builder.getErrorMessage());
    }

    // 测试Builder的getErrorMessageBytes方法
    @Test
    public void testBuilderGetErrorMessageBytes() {
        String errorMessage = "test error";
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder()
            .setErrorMessage(errorMessage);
        ByteString expectedBytes = ByteString.copyFromUtf8(errorMessage);
        assertEquals(expectedBytes, builder.getErrorMessageBytes());
    }

    // 测试Builder的setErrorMessageBytes方法
    @Test
    public void testBuilderSetErrorMessageBytes() {
        ByteString errorMessageBytes = ByteString.copyFromUtf8("test error");
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setErrorMessageBytes(errorMessageBytes);
        assertEquals("test error", builder.getErrorMessage());
        assertEquals(errorMessageBytes, builder.getErrorMessageBytes());
    }

    // 测试Builder的setErrorMessageBytes方法（null值）
    @Test(expected = NullPointerException.class)
    public void testBuilderSetErrorMessageBytesWithNull() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        builder.setErrorMessageBytes(null); // This should throw NullPointerException
    }

    // 测试Builder的setUnknownFields方法
    @Test
    public void testBuilderSetUnknownFields() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder().build();
        builder.setUnknownFields(unknownFields);
    }

    // 测试Builder的mergeUnknownFields方法
    @Test
    public void testBuilderMergeUnknownFields() {
        ColumnarDeltaResponse.Builder builder = ColumnarDeltaResponse.newBuilder();
        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder().build();
        builder.mergeUnknownFields(unknownFields);
    }
}
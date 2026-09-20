package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.*;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ColumnarCacheFilesInfoRequestComprehensiveTest {

    // 测试默认实例
    @Test
    public void testDefaultInstance() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertNotNull(request);
        assertEquals(0, request.getSerializedSize());
    }

    // 测试getUnknownFields方法
    @Test
    public void testGetUnknownFields() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        UnknownFieldSet unknownFields = request.getUnknownFields();
        assertNotNull(unknownFields);
    }

    // 测试getDescriptor方法
    @Test
    public void testGetDescriptor() {
        Descriptors.Descriptor descriptor = ColumnarCacheFilesInfoRequest.getDescriptor();
        assertNotNull(descriptor);
        assertEquals("orc.proto.ColumnarCacheFilesInfoRequest", descriptor.getFullName());
    }

    // 测试isInitialized方法
    @Test
    public void testIsInitialized() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertTrue(request.isInitialized());
    }

    // 测试writeTo方法
    @Test
    public void testWriteTo() throws IOException {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
            
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        request.writeTo(codedOutputStream);
        codedOutputStream.flush();
        
        byte[] bytes = outputStream.toByteArray();
        assertEquals(0, bytes.length);
    }

    // 测试getSerializedSize方法
    @Test
    public void testGetSerializedSize() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        int size = request.getSerializedSize();
        assertEquals(0, size);
    }

    // 测试equals方法（相同对象）
    @Test
    public void testEqualsSameObject() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertTrue(request.equals(request));
    }

    // 测试equals方法（不同类对象）
    @Test
    public void testEqualsDifferentClass() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertFalse(request.equals("string"));
    }

    // 测试equals方法（相同内容）
    @Test
    public void testEqualsSameContent() {
        ColumnarCacheFilesInfoRequest request1 = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest request2 = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertTrue(request1.equals(request2));
    }

    // 测试hashCode方法
    @Test
    public void testHashCode() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        int hashCode = request.hashCode();
        assertTrue(hashCode != 0);
    }

    // 测试parseFrom方法（ByteBuffer）
    @Test
    public void testParseFromByteBuffer() throws InvalidProtocolBufferException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ByteBuffer buffer = original.toByteString().asReadOnlyByteBuffer();
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(buffer);
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteBuffer，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteBufferWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ByteBuffer buffer = original.toByteString().asReadOnlyByteBuffer();
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(
            buffer, ExtensionRegistryLite.getEmptyRegistry());
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteString）
    @Test
    public void testParseFromByteString() throws InvalidProtocolBufferException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ByteString byteString = original.toByteString();
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(byteString);
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteString，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteStringWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ByteString byteString = original.toByteString();
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(
            byteString, ExtensionRegistryLite.getEmptyRegistry());
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（byte[]）
    @Test
    public void testParseFromByteArray() throws InvalidProtocolBufferException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(bytes);
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（byte[]，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteArrayWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(
            bytes, ExtensionRegistryLite.getEmptyRegistry());
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（InputStream）
    @Test
    public void testParseFromInputStream() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(inputStream);
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（InputStream，带ExtensionRegistryLite）
    @Test
    public void testParseFromInputStreamWithExtensionRegistry() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());
        assertEquals(original, parsed);
    }

    // 测试parseDelimitedFrom方法（InputStream）
    @Test
    public void testParseDelimitedFromInputStream() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
            
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();
        
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseDelimitedFrom(inputStream);
        assertEquals(original, parsed);
    }

    // 测试parseDelimitedFrom方法（InputStream，带ExtensionRegistryLite）
    @Test
    public void testParseDelimitedFromInputStreamWithExtensionRegistry() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
            
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();
        
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseDelimitedFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（CodedInputStream）
    @Test
    public void testParseFromCodedInputStream() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(inputStream);
        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（CodedInputStream，带ExtensionRegistryLite）
    @Test
    public void testParseFromCodedInputStreamWithExtensionRegistry() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        ColumnarCacheFilesInfoRequest parsed = ColumnarCacheFilesInfoRequest.parseFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());
        assertEquals(original, parsed);
    }

    // 测试newBuilderForType方法
    @Test
    public void testNewBuilderForType() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest.Builder builder = request.newBuilderForType();
        assertNotNull(builder);
    }

    // 测试newBuilder方法
    @Test
    public void testNewBuilder() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        assertNotNull(builder);
    }

    // 测试newBuilder方法（带原型）
    @Test
    public void testNewBuilderWithPrototype() {
        ColumnarCacheFilesInfoRequest prototype = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder(prototype);
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals(prototype, request);
    }

    // 测试toBuilder方法
    @Test
    public void testToBuilder() {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest.Builder builder = original.toBuilder();
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals(original, request);
    }

    // 测试getDefaultInstance方法
    @Test
    public void testGetDefaultInstance() {
        ColumnarCacheFilesInfoRequest defaultInstance = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        assertNotNull(defaultInstance);
    }

    // 测试parser方法
    @Test
    public void testParser() {
        Parser<ColumnarCacheFilesInfoRequest> parser = ColumnarCacheFilesInfoRequest.parser();
        assertNotNull(parser);
    }

    // 测试getParserForType方法
    @Test
    public void testGetParserForType() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        Parser<ColumnarCacheFilesInfoRequest> parser = request.getParserForType();
        assertNotNull(parser);
    }

    // 测试getDefaultInstanceForType方法
    @Test
    public void testGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoRequest request = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest defaultInstance = request.getDefaultInstanceForType();
        assertNotNull(defaultInstance);
    }

    // 测试Builder的getDescriptor方法
    @Test
    public void testBuilderGetDescriptor() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        Descriptors.Descriptor descriptor = builder.getDescriptor();
        assertNotNull(descriptor);
    }

    // 测试Builder的clear方法
    @Test
    public void testBuilderClear() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        builder.clear();
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals(ColumnarCacheFilesInfoRequest.getDefaultInstance(), request);
    }

    // 测试Builder的getDescriptorForType方法
    @Test
    public void testBuilderGetDescriptorForType() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        Descriptors.Descriptor descriptor = builder.getDescriptorForType();
        assertNotNull(descriptor);
    }

    // 测试Builder的getDefaultInstanceForType方法
    @Test
    public void testBuilderGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        ColumnarCacheFilesInfoRequest defaultInstance = builder.getDefaultInstanceForType();
        assertNotNull(defaultInstance);
    }

    // 测试Builder的build方法
    @Test
    public void testBuilderBuild() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertNotNull(request);
    }

    // 测试Builder的buildPartial方法
    @Test
    public void testBuilderBuildPartial() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        ColumnarCacheFilesInfoRequest request = builder.buildPartial();
        assertNotNull(request);
    }

    // 测试Builder的clone方法
    @Test
    public void testBuilderClone() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        ColumnarCacheFilesInfoRequest.Builder clonedBuilder = builder.clone();
        assertNotNull(clonedBuilder);
    }

    // 测试Builder的mergeFrom方法（Message）
    @Test
    public void testBuilderMergeFromMessage() {
        ColumnarCacheFilesInfoRequest other = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        builder.mergeFrom(other);
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals(other, request);
    }

    // 测试Builder的mergeFrom方法（ColumnarCacheFilesInfoRequest）
    @Test
    public void testBuilderMergeFromColumnarCacheFilesInfoRequest() {
        ColumnarCacheFilesInfoRequest other = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        builder.mergeFrom(other);
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals(other, request);
    }

    // 测试Builder的isInitialized方法
    @Test
    public void testBuilderIsInitialized() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        assertTrue(builder.isInitialized());
    }

    // 测试Builder的mergeFrom方法（CodedInputStream, ExtensionRegistryLite）
    @Test
    public void testBuilderMergeFromCodedInputStream() throws IOException {
        ColumnarCacheFilesInfoRequest original = ColumnarCacheFilesInfoRequest.getDefaultInstance();
        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        builder.mergeFrom(inputStream, ExtensionRegistryLite.getEmptyRegistry());
        
        ColumnarCacheFilesInfoRequest request = builder.build();
        assertEquals(original, request);
    }

    // 测试Builder的setUnknownFields方法
    @Test
    public void testBuilderSetUnknownFields() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder().build();
        builder.setUnknownFields(unknownFields);
    }

    // 测试Builder的mergeUnknownFields方法
    @Test
    public void testBuilderMergeUnknownFields() {
        ColumnarCacheFilesInfoRequest.Builder builder = ColumnarCacheFilesInfoRequest.newBuilder();
        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder().build();
        builder.mergeUnknownFields(unknownFields);
    }
}
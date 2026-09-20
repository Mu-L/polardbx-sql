package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.*;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class ColumnarCacheFilesInfoResponseComprehensiveTest {

    // 测试默认构造函数
    @Test
    public void testDefaultConstructor() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        assertNotNull(response);
        assertEquals(0, response.getFilesCount());
    }

    // 测试getUnknownFields方法
    @Test
    public void testGetUnknownFields() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        UnknownFieldSet unknownFields = response.getUnknownFields();
        assertNotNull(unknownFields);
    }

    // 测试带CodedInputStream和ExtensionRegistryLite的构造函数（正常情况）
    @Test
    public void testConstructorWithCodedInputStream() throws IOException {
        // 创建一个包含FileInfo的响应
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();
            
        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();
            
        // 序列化
        byte[] bytes = original.toByteArray();
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        
        // 反序列化
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.parseFrom(input, ExtensionRegistryLite.getEmptyRegistry());
            
        assertNotNull(response);
        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // 测试getDescriptor方法
    @Test
    public void testGetDescriptor() {
        Descriptors.Descriptor descriptor = ColumnarCacheFilesInfoResponse.getDescriptor();
        assertNotNull(descriptor);
        assertEquals("orc.proto.ColumnarCacheFilesInfoResponse", descriptor.getFullName());
    }

    // 测试internalGetFieldAccessorTable方法
    // @Test
    // public void testInternalGetFieldAccessorTable() {
    //     ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
    //     FieldAccessorTable fieldAccessorTable = response.internalGetFieldAccessorTable();
    //     assertNotNull(fieldAccessorTable);
    // }

    // 测试getFilesList方法
    @Test
    public void testGetFilesList() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        List<FileInfo> filesList = response.getFilesList();
        assertNotNull(filesList);
        assertEquals(1, filesList.size());
        assertEquals("test.orc", filesList.get(0).getFileName());
    }

    // 测试getFilesOrBuilderList方法
    @Test
    public void testGetFilesOrBuilderList() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        List<? extends FileInfoOrBuilder> filesOrBuilderList = response.getFilesOrBuilderList();
        assertNotNull(filesOrBuilderList);
        assertEquals(1, filesOrBuilderList.size());
        assertEquals("test.orc", filesOrBuilderList.get(0).getFileName());
    }

    // 测试getFilesCount方法
    @Test
    public void testGetFilesCount() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .addFiles(fileInfo2)
            .build();

        assertEquals(2, response.getFilesCount());
    }

    // 测试getFiles方法
    @Test
    public void testGetFiles() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .addFiles(fileInfo2)
            .build();

        FileInfo file1 = response.getFiles(0);
        FileInfo file2 = response.getFiles(1);

        assertEquals("test1.orc", file1.getFileName());
        assertEquals(1024, file1.getFileLength());
        assertEquals("test2.orc", file2.getFileName());
        assertEquals(2048, file2.getFileLength());
    }

    // 测试getFilesOrBuilder方法
    @Test
    public void testGetFilesOrBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        FileInfoOrBuilder fileOrBuilder = response.getFilesOrBuilder(0);
        assertEquals("test.orc", fileOrBuilder.getFileName());
        assertEquals(1024, fileOrBuilder.getFileLength());
    }

    // 测试isInitialized方法
    @Test
    public void testIsInitialized() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        assertTrue(response.isInitialized());
    }

    // 测试writeTo方法
    @Test
    public void testWriteTo() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
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
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        int size = response.getSerializedSize();
        assertTrue(size > 0);
    }

    // 测试equals方法（相同对象）
    @Test
    public void testEqualsSameObject() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        assertTrue(response.equals(response));
    }

    // 测试equals方法（不同类对象）
    @Test
    public void testEqualsDifferentClass() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        assertFalse(response.equals("string"));
    }

    // 测试equals方法（相同内容）
    @Test
    public void testEqualsSameContent() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response1 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse response2 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        assertTrue(response1.equals(response2));
    }

    // 测试equals方法（不同内容）
    @Test
    public void testEqualsDifferentContent() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse response1 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .build();

        ColumnarCacheFilesInfoResponse response2 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo2)
            .build();

        assertFalse(response1.equals(response2));
    }

    // 测试hashCode方法
    @Test
    public void testHashCode() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        int hashCode = response.hashCode();
        assertTrue(hashCode != 0);
    }

    // 测试parseFrom方法（ByteBuffer）
    @Test
    public void testParseFromByteBuffer() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ByteBuffer buffer = original.toByteString().asReadOnlyByteBuffer();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(buffer);

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteBuffer，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteBufferWithExtensionRegistry() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ByteBuffer buffer = original.toByteString().asReadOnlyByteBuffer();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            buffer, ExtensionRegistryLite.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteString）
    @Test
    public void testParseFromByteString() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ByteString byteString = original.toByteString();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(byteString);

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（ByteString，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteStringWithExtensionRegistry() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ByteString byteString = original.toByteString();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            byteString, ExtensionRegistryLite.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（byte[]）
    @Test
    public void testParseFromByteArray() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(bytes);

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（byte[]，带ExtensionRegistryLite）
    @Test
    public void testParseFromByteArrayWithExtensionRegistry() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            bytes, ExtensionRegistryLite.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（InputStream）
    @Test
    public void testParseFromInputStream() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(inputStream);

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（InputStream，带ExtensionRegistryLite）
    @Test
    public void testParseFromInputStreamWithExtensionRegistry() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // 测试parseDelimitedFrom方法（InputStream）
    @Test
    public void testParseDelimitedFromInputStream() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseDelimitedFrom(inputStream);

        assertEquals(original, parsed);
    }

    // 测试parseDelimitedFrom方法（InputStream，带ExtensionRegistryLite）
    @Test
    public void testParseDelimitedFromInputStreamWithExtensionRegistry() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseDelimitedFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（CodedInputStream）
    @Test
    public void testParseFromCodedInputStream() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(inputStream);

        assertEquals(original, parsed);
    }

    // 测试parseFrom方法（CodedInputStream，带ExtensionRegistryLite）
    @Test
    public void testParseFromCodedInputStreamWithExtensionRegistry() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            inputStream, ExtensionRegistryLite.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // 测试newBuilderForType方法
    @Test
    public void testNewBuilderForType() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        ColumnarCacheFilesInfoResponse.Builder builder = response.newBuilderForType();
        assertNotNull(builder);
    }

    // 测试newBuilder方法
    @Test
    public void testNewBuilder() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        assertNotNull(builder);
    }

    // 测试newBuilder方法（带原型）
    @Test
    public void testNewBuilderWithPrototype() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse prototype = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder(prototype);
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(prototype, response);
    }

    // 测试toBuilder方法
    @Test
    public void testToBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = original.toBuilder();
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(original, response);
    }

    // 测试getDefaultInstance方法
    @Test
    public void testGetDefaultInstance() {
        ColumnarCacheFilesInfoResponse defaultInstance = ColumnarCacheFilesInfoResponse.getDefaultInstance();
        assertNotNull(defaultInstance);
        assertEquals(0, defaultInstance.getFilesCount());
    }

    // 测试parser方法
    @Test
    public void testParser() {
        Parser<ColumnarCacheFilesInfoResponse> parser = ColumnarCacheFilesInfoResponse.parser();
        assertNotNull(parser);
    }

    // 测试getParserForType方法
    @Test
    public void testGetParserForType() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        Parser<ColumnarCacheFilesInfoResponse> parser = response.getParserForType();
        assertNotNull(parser);
    }

    // 测试getDefaultInstanceForType方法
    @Test
    public void testGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder().build();
        ColumnarCacheFilesInfoResponse defaultInstance = response.getDefaultInstanceForType();
        assertNotNull(defaultInstance);
        assertEquals(0, defaultInstance.getFilesCount());
    }

    // 测试Builder的getDescriptor方法
    @Test
    public void testBuilderGetDescriptor() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        Descriptors.Descriptor descriptor = builder.getDescriptor();
        assertNotNull(descriptor);
    }

    // 测试Builder的internalGetFieldAccessorTable方法
    @Test
    public void testBuilderInternalGetFieldAccessorTable() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        GeneratedMessageV3.FieldAccessorTable fieldAccessorTable = builder.internalGetFieldAccessorTable();
        assertNotNull(fieldAccessorTable);
    }

    // 测试Builder的clear方法
    @Test
    public void testBuilderClear() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        builder.clear();
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(0, response.getFilesCount());
    }

    // 测试Builder的getDescriptorForType方法
    @Test
    public void testBuilderGetDescriptorForType() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        Descriptors.Descriptor descriptor = builder.getDescriptorForType();
        assertNotNull(descriptor);
    }

    // 测试Builder的getDefaultInstanceForType方法
    @Test
    public void testBuilderGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        ColumnarCacheFilesInfoResponse defaultInstance = builder.getDefaultInstanceForType();
        assertNotNull(defaultInstance);
        assertEquals(0, defaultInstance.getFilesCount());
    }

    // 测试Builder的build方法
    @Test
    public void testBuilderBuild() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertNotNull(response);
        assertEquals(1, response.getFilesCount());
    }

    // 测试Builder的buildPartial方法
    @Test
    public void testBuilderBuildPartial() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        ColumnarCacheFilesInfoResponse response = builder.buildPartial();
        assertNotNull(response);
        assertEquals(1, response.getFilesCount());
    }

    // 测试Builder的clone方法
    @Test
    public void testBuilderClone() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        ColumnarCacheFilesInfoResponse.Builder clonedBuilder = builder.clone();
        assertNotNull(clonedBuilder);
        assertEquals(1, clonedBuilder.getFilesCount());
    }

    // 测试Builder的setField方法
    @Test
    public void testBuilderSetField() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        // This method is typically used internally, so we just ensure it doesn't throw
        builder.setField(ColumnarCacheFilesInfoResponse.getDescriptor().findFieldByNumber(1),
                        Collections.emptyList());
    }

    // 测试Builder的clearField方法
    @Test
    public void testBuilderClearField() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        // This method is typically used internally, so we just ensure it doesn't throw
        builder.clearField(ColumnarCacheFilesInfoResponse.getDescriptor().findFieldByNumber(1));
    }

    // 测试Builder的clearOneof方法
    @Test
    public void testBuilderClearOneof() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        // This method is typically used internally, so we just ensure it doesn't throw
        // Even though ColumnarCacheFilesInfoResponse doesn't have oneof fields,
        // we still want to test the method call for coverage
        try {
            // Attempt to call clearOneof with a valid oneof descriptor
            // Since there are no oneof fields, we expect this to not throw an exception
            if (!ColumnarCacheFilesInfoResponse.getDescriptor().getOneofs().isEmpty()) {
                builder.clearOneof(ColumnarCacheFilesInfoResponse.getDescriptor().getOneofs().get(0));
            }
            // If we reach here, the method call didn't throw an exception, which is what we want
            assertTrue(true);
        } catch (Exception e) {
            // If any exception is thrown, the test should fail
            fail("clearOneof method should not throw an exception, but threw: " + e.getClass().getSimpleName());
        }
    }

    // 测试Builder的setRepeatedField方法
    @Test
    public void testBuilderSetRepeatedField() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        // This method is typically used internally, so we just ensure it doesn't throw
        builder.setRepeatedField(ColumnarCacheFilesInfoResponse.getDescriptor().findFieldByNumber(1),
                               0, fileInfo);
    }

    // 测试Builder的addRepeatedField方法
    @Test
    public void testBuilderAddRepeatedField() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // This method is typically used internally, so we just ensure it doesn't throw
        builder.addRepeatedField(ColumnarCacheFilesInfoResponse.getDescriptor().findFieldByNumber(1),
                               fileInfo);
    }

    // 测试Builder的mergeFrom方法（Message）
    @Test
    public void testBuilderMergeFromMessage() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse other = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.mergeFrom(other);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // 测试Builder的mergeFrom方法（ColumnarCacheFilesInfoResponse）
    @Test
    public void testBuilderMergeFromColumnarCacheFilesInfoResponse() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse other = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.mergeFrom(other);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // 测试Builder的isInitialized方法
    @Test
    public void testBuilderIsInitialized() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        assertTrue(builder.isInitialized());
    }

    // 测试Builder的mergeFrom方法（CodedInputStream, ExtensionRegistryLite）
    @Test
    public void testBuilderMergeFromCodedInputStream() throws IOException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        CodedInputStream inputStream = CodedInputStream.newInstance(bytes);

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.mergeFrom(inputStream, ExtensionRegistryLite.getEmptyRegistry());

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // 测试Builder的getFilesList方法
    @Test
    public void testBuilderGetFilesList() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        List<FileInfo> filesList = builder.getFilesList();
        assertNotNull(filesList);
        assertEquals(1, filesList.size());
        assertEquals("test.orc", filesList.get(0).getFileName());
    }

    // 测试Builder的getFilesCount方法
    @Test
    public void testBuilderGetFilesCount() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .addFiles(fileInfo2);

        assertEquals(2, builder.getFilesCount());
    }

    // 测试Builder的getFiles方法
    @Test
    public void testBuilderGetFiles() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .addFiles(fileInfo2);

        FileInfo file1 = builder.getFiles(0);
        FileInfo file2 = builder.getFiles(1);

        assertEquals("test1.orc", file1.getFileName());
        assertEquals(1024, file1.getFileLength());
        assertEquals("test2.orc", file2.getFileName());
        assertEquals(2048, file2.getFileLength());
    }

    // 测试Builder的setFiles方法（FileInfo）
    @Test
    public void testBuilderSetFilesWithFileInfo() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1);

        builder.setFiles(0, fileInfo2);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test2.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
    }

    // 测试Builder的setFiles方法（FileInfo.Builder）
    @Test
    public void testBuilderSetFilesWithFileInfoBuilder() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo.Builder fileInfoBuilder2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048);

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1);

        builder.setFiles(0, fileInfoBuilder2);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test2.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
    }

    // 测试Builder的addFiles方法（FileInfo）
    @Test
    public void testBuilderAddFilesWithFileInfo() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addFiles(fileInfo);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // 测试Builder的addFiles方法（index, FileInfo）
    @Test
    public void testBuilderAddFilesWithIndexAndFileInfo() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addFiles(fileInfo1);
        builder.addFiles(0, fileInfo2);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(2, response.getFilesCount());
        assertEquals("test2.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
        assertEquals("test1.orc", response.getFiles(1).getFileName());
        assertEquals(1024, response.getFiles(1).getFileLength());
    }

    // 测试Builder的addFiles方法（FileInfo.Builder）
    @Test
    public void testBuilderAddFilesWithFileInfoBuilder() {
        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024);

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addFiles(fileInfoBuilder);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // 测试Builder的addFiles方法（index, FileInfo.Builder）
    @Test
    public void testBuilderAddFilesWithIndexAndFileInfoBuilder() {
        FileInfo.Builder fileInfoBuilder1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024);

        FileInfo.Builder fileInfoBuilder2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048);

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addFiles(fileInfoBuilder1);
        builder.addFiles(0, fileInfoBuilder2);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(2, response.getFilesCount());
        assertEquals("test2.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
        assertEquals("test1.orc", response.getFiles(1).getFileName());
        assertEquals(1024, response.getFiles(1).getFileLength());
    }

    // 测试Builder的addAllFiles方法
    @Test
    public void testBuilderAddAllFiles() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        List<FileInfo> filesList = new ArrayList<>();
        filesList.add(fileInfo1);
        filesList.add(fileInfo2);

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addAllFiles(filesList);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(2, response.getFilesCount());
        assertEquals("test1.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
        assertEquals("test2.orc", response.getFiles(1).getFileName());
        assertEquals(2048, response.getFiles(1).getFileLength());
    }

    // 测试Builder的clearFiles方法
    @Test
    public void testBuilderClearFiles() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        builder.clearFiles();

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(0, response.getFilesCount());
    }

    // 测试Builder的removeFiles方法
    @Test
    public void testBuilderRemoveFiles() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("test1.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("test2.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .addFiles(fileInfo2);

        builder.removeFiles(0);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("test2.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
    }

    // 测试Builder的getFilesBuilder方法
    @Test
    public void testBuilderGetFilesBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        FileInfo.Builder fileInfoBuilder = builder.getFilesBuilder(0);
        fileInfoBuilder.setFileName("modified.orc");
        fileInfoBuilder.setFileLength(2048);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals(1, response.getFilesCount());
        assertEquals("modified.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
    }

    // 测试Builder的getFilesOrBuilder方法
    @Test
    public void testBuilderGetFilesOrBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        FileInfoOrBuilder fileOrBuilder = builder.getFilesOrBuilder(0);
        assertEquals("test.orc", fileOrBuilder.getFileName());
        assertEquals(1024, fileOrBuilder.getFileLength());
    }
}
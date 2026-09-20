package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ColumnarCacheFilesInfoResponseTest {

    // TC1: Verify default values
    @Test
    public void testDefaultInstance() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.getDefaultInstance();

        assertEquals(0, response.getFilesCount());
        assertTrue(response.getFilesList().isEmpty());
    }

    // TC2: Verify Builder sets values correctly
    @Test
    public void testBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // TC3: Equality check for identical objects
    @Test
    public void testEquals_SameValues_ReturnsTrue() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("data.orc")
            .setFileLength(100)
            .build();

        ColumnarCacheFilesInfoResponse resp1 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();
        ColumnarCacheFilesInfoResponse resp2 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        assertEquals(resp1, resp2);
        assertEquals(resp1.hashCode(), resp2.hashCode());
    }

    // TC4: Equality check for different values
    @Test
    public void testEquals_DifferentFiles_ReturnsFalse() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("file1.orc")
            .setFileLength(1)
            .build();
        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("file2.orc")
            .setFileLength(2)
            .build();

        ColumnarCacheFilesInfoResponse resp1 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .build();
        ColumnarCacheFilesInfoResponse resp2 = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo2)
            .build();

        assertFalse(resp1.equals(resp2));
    }

    // TC5: Serialization and parsing round-trip
    @Test
    public void testSerializationRoundTrip() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("roundtrip.orc")
            .setFileLength(42)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(bytes);

        assertEquals(parsed, original);
    }

    // TC6: Test getFilesOrBuilderList method
    @Test
    public void testGetFilesOrBuilderList() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        List filesOrBuilderList = response.getFilesOrBuilderList();
        assertEquals(1, filesOrBuilderList.size());
        assertEquals("test.orc", ((FileInfoOrBuilder) filesOrBuilderList.get(0)).getFileName());
        assertEquals(1024, ((FileInfoOrBuilder) filesOrBuilderList.get(0)).getFileLength());
    }

    // TC7: Test getFilesOrBuilder method
    @Test
    public void testGetFilesOrBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        FileInfoOrBuilder fileInfoOrBuilder = response.getFilesOrBuilder(0);
        assertEquals("test.orc", fileInfoOrBuilder.getFileName());
        assertEquals(1024, fileInfoOrBuilder.getFileLength());
    }

    // TC8: Test isInitialized method
    @Test
    public void testIsInitialized() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .build();

        assertTrue(response.isInitialized());
    }

    // TC9: Test getSerializedSize method
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

    // TC10: Test writeTo method
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
        response.writeTo(outputStream);

        byte[] bytes = outputStream.toByteArray();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(bytes);

        assertEquals(response, parsed);
    }

    // TC11: Test parseFrom with ByteBuffer
    @Test
    public void testParseFromByteBuffer() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            java.nio.ByteBuffer.wrap(bytes));

        assertEquals(original, parsed);
    }

    // TC12: Test parseFrom with ByteBuffer and ExtensionRegistry
    @Test
    public void testParseFromByteBufferWithExtensionRegistry() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            java.nio.ByteBuffer.wrap(bytes), ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC13: Test parseFrom with ByteString
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

    // TC14: Test parseFrom with ByteString and ExtensionRegistry
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
            byteString, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC15: Test parseFrom with InputStream
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

    // TC16: Test parseFrom with InputStream and ExtensionRegistry
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
            inputStream, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC17: Test parseDelimitedFrom with InputStream
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

    // TC18: Test parseDelimitedFrom with InputStream and ExtensionRegistry
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
            inputStream, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC19: Test parseFrom with CodedInputStream
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
        CodedInputStream codedInputStream = CodedInputStream.newInstance(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(codedInputStream);

        assertEquals(original, parsed);
    }

    // TC20: Test parseFrom with CodedInputStream and ExtensionRegistry
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
        CodedInputStream codedInputStream = CodedInputStream.newInstance(bytes);
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            codedInputStream, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC21: Test newBuilderForType method
    @Test
    public void testNewBuilderForType() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.getDefaultInstance();
        ColumnarCacheFilesInfoResponse.Builder builder = response.newBuilderForType();

        assertNotNull(builder);
    }

    // TC22: Test newBuilder method
    @Test
    public void testNewBuilder() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        assertNotNull(builder);
    }

    // TC23: Test newBuilder with prototype
    @Test
    public void testNewBuilderWithPrototype() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("prototype.orc")
            .setFileLength(50)
            .build();

        ColumnarCacheFilesInfoResponse prototype = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder(prototype);
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(prototype, response);
    }

    // TC24: Test toBuilder method
    @Test
    public void testToBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("original.orc")
            .setFileLength(50)
            .build();

        ColumnarCacheFilesInfoResponse original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = original.toBuilder();
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(original, response);
        assertNotSame(original, response);
    }

    // TC25: Test getParser method
    @Test
    public void testGetParser() {
        com.google.protobuf.Parser parser = ColumnarCacheFilesInfoResponse.parser();

        assertNotNull(parser);
    }

    // TC26: Test getParserForType method
    @Test
    public void testGetParserForType() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.getDefaultInstance();
        com.google.protobuf.Parser parser = response.getParserForType();

        assertNotNull(parser);
    }

    // TC27: Test getDefaultInstanceForType method
    @Test
    public void testGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.getDefaultInstance();
        ColumnarCacheFilesInfoResponse defaultInstance = response.getDefaultInstanceForType();

        assertNotNull(defaultInstance);
        assertEquals(response, defaultInstance);
    }

    // TC28: Test equals with self
    @Test
    public void testEqualsWithSelf() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        assertEquals(response, response);
    }

    // TC29: Test equals with null
    @Test
    public void testEqualsWithNull() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        assertFalse(response.equals(null));
    }

    // TC30: Test equals with different class
    @Test
    public void testEqualsWithDifferentClass() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        assertFalse(response.equals("string"));
    }

    // TC31: Test parseFrom with invalid data
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromInvalidData() throws InvalidProtocolBufferException {
        byte[] invalidData = new byte[] {0x00, 0x01, 0x02, 0x03};
        ColumnarCacheFilesInfoResponse.parseFrom(invalidData);
    }

    // TC32: Test parseFrom with invalid data and ExtensionRegistry
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromInvalidDataWithExtensionRegistry() throws InvalidProtocolBufferException {
        byte[] invalidData = new byte[] {0x00, 0x01, 0x02, 0x03};
        ColumnarCacheFilesInfoResponse.parseFrom(invalidData, ExtensionRegistry.getEmptyRegistry());
    }

    // TC33: Test parseFrom with ByteString containing invalid data
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromByteStringInvalidData() throws InvalidProtocolBufferException {
        ByteString invalidByteString = ByteString.copyFrom(new byte[] {0x00, 0x01, 0x02, 0x03});
        ColumnarCacheFilesInfoResponse.parseFrom(invalidByteString);
    }

    // TC34: Test empty files list
    @Test
    public void testEmptyFilesList() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .build();

        assertEquals(0, response.getFilesCount());
        assertTrue(response.getFilesList().isEmpty());
    }

    // TC35: Test multiple files
    @Test
    public void testMultipleFiles() {
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
        assertEquals("test1.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
        assertEquals("test2.orc", response.getFiles(1).getFileName());
        assertEquals(2048, response.getFiles(1).getFileLength());
    }

    // TC36: Test Builder addAllFiles method
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

        List filesList = new ArrayList();
        filesList.add(fileInfo1);
        filesList.add(fileInfo2);

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addAllFiles(filesList)
            .build();

        assertEquals(2, response.getFilesCount());
        assertEquals("test1.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
        assertEquals("test2.orc", response.getFiles(1).getFileName());
        assertEquals(2048, response.getFiles(1).getFileLength());
    }

    // TC37: Test Builder clearFiles method
    @Test
    public void testBuilderClearFiles() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .clearFiles();

        assertEquals(0, builder.getFilesCount());
        assertTrue(builder.getFilesList().isEmpty());
    }

    // TC38: Test Builder removeFiles method
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

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .addFiles(fileInfo2)
            .removeFiles(0)
            .build();

        assertEquals(1, response.getFilesCount());
        assertEquals("test2.orc", response.getFiles(0).getFileName());
        assertEquals(2048, response.getFiles(0).getFileLength());
    }

    // TC39: Test Builder getFilesBuilder method
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

    // TC40: Test Builder getFilesBuilderList method
    @Test
    public void testBuilderGetFilesBuilderList() {
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

        List fileInfoBuilderList = builder.getFilesBuilderList();
        ((FileInfo.Builder) fileInfoBuilderList.get(0)).setFileName("modified1.orc");
        ((FileInfo.Builder) fileInfoBuilderList.get(0)).setFileLength(3072);
        ((FileInfo.Builder) fileInfoBuilderList.get(1)).setFileName("modified2.orc");
        ((FileInfo.Builder) fileInfoBuilderList.get(1)).setFileLength(4096);

        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(2, response.getFilesCount());
        assertEquals("modified1.orc", response.getFiles(0).getFileName());
        assertEquals(3072, response.getFiles(0).getFileLength());
        assertEquals("modified2.orc", response.getFiles(1).getFileName());
        assertEquals(4096, response.getFiles(1).getFileLength());
    }

    // TC41: Test Builder addFilesBuilder method
    @Test
    public void testBuilderAddFilesBuilder() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        FileInfo.Builder fileInfoBuilder = builder.addFilesBuilder();
        fileInfoBuilder.setFileName("test.orc");
        fileInfoBuilder.setFileLength(1024);

        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // TC42: Test Builder addFilesBuilder with index
    @Test
    public void testBuilderAddFilesBuilderWithIndex() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("existing.orc")
            .setFileLength(512)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        FileInfo.Builder fileInfoBuilder = builder.addFilesBuilder(0);
        fileInfoBuilder.setFileName("inserted.orc");
        fileInfoBuilder.setFileLength(1024);

        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(2, response.getFilesCount());
        assertEquals("inserted.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
        assertEquals("existing.orc", response.getFiles(1).getFileName());
        assertEquals(512, response.getFiles(1).getFileLength());
    }

    // TC43: Test Builder setFiles with FileInfo
    @Test
    public void testBuilderSetFilesWithFileInfo() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("original.orc")
            .setFileLength(512)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("replacement.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .setFiles(0, fileInfo2)
            .build();

        assertEquals(1, response.getFilesCount());
        assertEquals("replacement.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // TC44: Test Builder setFiles with FileInfo.Builder
    @Test
    public void testBuilderSetFilesWithFileInfoBuilder() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("original.orc")
            .setFileLength(512)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder()
            .setFileName("replacement.orc")
            .setFileLength(1024);

        builder.setFiles(0, fileInfoBuilder);
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(1, response.getFilesCount());
        assertEquals("replacement.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // TC45: Test Builder addFiles with FileInfo
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

    // TC46: Test Builder addFiles with index and FileInfo
    @Test
    public void testBuilderAddFilesWithIndexAndFileInfo() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("first.orc")
            .setFileLength(512)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("second.orc")
            .setFileLength(1024)
            .build();

        FileInfo fileInfo3 = FileInfo.newBuilder()
            .setFileName("third.orc")
            .setFileLength(2048)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addFiles(fileInfo1);
        builder.addFiles(fileInfo3);
        builder.addFiles(1, fileInfo2);
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(3, response.getFilesCount());
        assertEquals("first.orc", response.getFiles(0).getFileName());
        assertEquals(512, response.getFiles(0).getFileLength());
        assertEquals("second.orc", response.getFiles(1).getFileName());
        assertEquals(1024, response.getFiles(1).getFileLength());
        assertEquals("third.orc", response.getFiles(2).getFileName());
        assertEquals(2048, response.getFiles(2).getFileLength());
    }

    // TC47: Test Builder addFiles with FileInfo.Builder
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

    // TC48: Test Builder addFiles with index and FileInfo.Builder
    @Test
    public void testBuilderAddFilesWithIndexAndFileInfoBuilder() {
        FileInfo.Builder fileInfoBuilder1 = FileInfo.newBuilder()
            .setFileName("first.orc")
            .setFileLength(512);

        FileInfo.Builder fileInfoBuilder2 = FileInfo.newBuilder()
            .setFileName("second.orc")
            .setFileLength(1024);

        FileInfo.Builder fileInfoBuilder3 = FileInfo.newBuilder()
            .setFileName("third.orc")
            .setFileLength(2048);

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        builder.addFiles(fileInfoBuilder1);
        builder.addFiles(fileInfoBuilder3);
        builder.addFiles(1, fileInfoBuilder2);
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(3, response.getFilesCount());
        assertEquals("first.orc", response.getFiles(0).getFileName());
        assertEquals(512, response.getFiles(0).getFileLength());
        assertEquals("second.orc", response.getFiles(1).getFileName());
        assertEquals(1024, response.getFiles(1).getFileLength());
        assertEquals("third.orc", response.getFiles(2).getFileName());
        assertEquals(2048, response.getFiles(2).getFileLength());
    }

    // TC49: Test Builder mergeFrom with Message
    @Test
    public void testBuilderMergeFromMessage() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("first.orc")
            .setFileLength(512)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("second.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse other = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo2)
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1)
            .mergeFrom(other)
            .build();

        assertEquals(2, response.getFilesCount());
        assertEquals("first.orc", response.getFiles(0).getFileName());
        assertEquals(512, response.getFiles(0).getFileLength());
        assertEquals("second.orc", response.getFiles(1).getFileName());
        assertEquals(1024, response.getFiles(1).getFileLength());
    }

    // TC50: Test Builder mergeFrom with ColumnarCacheFilesInfoResponse
    @Test
    public void testBuilderMergeFromColumnarCacheFilesInfoResponse() {
        FileInfo fileInfo1 = FileInfo.newBuilder()
            .setFileName("first.orc")
            .setFileLength(512)
            .build();

        FileInfo fileInfo2 = FileInfo.newBuilder()
            .setFileName("second.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse other = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo2)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo1);

        builder.mergeFrom(other);

        ColumnarCacheFilesInfoResponse response = builder.build();

        assertEquals(2, response.getFilesCount());
        assertEquals("first.orc", response.getFiles(0).getFileName());
        assertEquals(512, response.getFiles(0).getFileLength());
        assertEquals("second.orc", response.getFiles(1).getFileName());
        assertEquals(1024, response.getFiles(1).getFileLength());
    }

    // TC51: Test Builder isInitialized
    @Test
    public void testBuilderIsInitialized() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        assertTrue(builder.isInitialized());
    }

    // TC52: Test Builder clear
    @Test
    public void testBuilderClear() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .clear();

        assertEquals(0, builder.getFilesCount());
        assertTrue(builder.getFilesList().isEmpty());
    }

    // TC53: Test Builder clone
    @Test
    public void testBuilderClone() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        ColumnarCacheFilesInfoResponse.Builder clonedBuilder = builder.clone();

        assertEquals(builder.getFilesCount(), clonedBuilder.getFilesCount());
        assertEquals(builder.getFiles(0).getFileName(), clonedBuilder.getFiles(0).getFileName());
        assertEquals(builder.getFiles(0).getFileLength(), clonedBuilder.getFiles(0).getFileLength());
    }

    // TC54: Test Builder buildPartial
    @Test
    public void testBuilderBuildPartial() {
        FileInfo fileInfo = FileInfo.newBuilder()
            .setFileName("test.orc")
            .setFileLength(1024)
            .build();

        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo);

        ColumnarCacheFilesInfoResponse response = builder.buildPartial();

        assertEquals(1, response.getFilesCount());
        assertEquals("test.orc", response.getFiles(0).getFileName());
        assertEquals(1024, response.getFiles(0).getFileLength());
    }

    // TC55: Test Builder getDescriptorForType
    @Test
    public void testBuilderGetDescriptorForType() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        com.google.protobuf.Descriptors.Descriptor descriptor = builder.getDescriptorForType();

        assertNotNull(descriptor);
    }

    // TC56: Test Builder getDefaultInstanceForType
    @Test
    public void testBuilderGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        ColumnarCacheFilesInfoResponse defaultInstance = builder.getDefaultInstanceForType();

        assertNotNull(defaultInstance);
        assertEquals(0, defaultInstance.getFilesCount());
    }
}
package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ColumnarDeltaRequestTest {

    // TC1: Verify default values
    @Test
    public void testDefaultInstance() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.getDefaultInstance();

        assertTrue(request.getFileName().isEmpty());
        assertEquals(0, request.getOffset());
        assertEquals(0, request.getLength());
    }

    // TC2: Verify Builder sets values correctly
    @Test
    public void testBuilder() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.del")
            .setOffset(100)
            .setLength(200)
            .build();

        assertEquals("test.del", request.getFileName());
        assertEquals(100, request.getOffset());
        assertEquals(200, request.getLength());
    }

    // TC3: Equality check for identical objects
    @Test
    public void testEquals_SameValues_ReturnsTrue() {
        ColumnarDeltaRequest req1 = ColumnarDeltaRequest.newBuilder()
            .setFileName("data.csv").setOffset(5).setLength(10).build();
        ColumnarDeltaRequest req2 = ColumnarDeltaRequest.newBuilder()
            .setFileName("data.csv").setOffset(5).setLength(10).build();

        assertEquals(req1, req2);
        assertEquals(req1.hashCode(), req2.hashCode());
    }

    // TC4: Equality check for different values
    @Test
    public void testEquals_DifferentFileName_ReturnsFalse() {
        ColumnarDeltaRequest req1 = ColumnarDeltaRequest.newBuilder()
            .setFileName("file1.csv").setOffset(0).setLength(1).build();
        ColumnarDeltaRequest req2 = ColumnarDeltaRequest.newBuilder()
            .setFileName("file2.csv").setOffset(0).setLength(1).build();

        assertFalse(req1.equals(req2));
    }

    // TC5: Serialization and parsing round-trip
    @Test
    public void testSerializationRoundTrip() throws InvalidProtocolBufferException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("roundtrip.bin")
            .setOffset(42)
            .setLength(84)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(bytes);

        assertEquals(parsed, original);
    }

    // TC6: Test getFileNameBytes method
    @Test
    public void testGetFileNameBytes() {
        String fileName = "test.csv";
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName(fileName)
            .build();

        ByteString bytes = request.getFileNameBytes();
        assertNotNull(bytes);
        assertEquals(fileName, bytes.toStringUtf8());
    }

    // TC7: Test isInitialized method
    @Test
    public void testIsInitialized() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        assertTrue(request.isInitialized());
    }

    // TC8: Test getSerializedSize method
    @Test
    public void testGetSerializedSize() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        int size = request.getSerializedSize();
        assertTrue(size > 0);
    }

    // TC9: Test writeTo method
    @Test
    public void testWriteTo() throws IOException {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        request.writeTo(outputStream);

        byte[] bytes = outputStream.toByteArray();
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(bytes);

        assertEquals(request, parsed);
    }

    // TC10: Test parseFrom with ByteBuffer
    @Test
    public void testParseFromByteBuffer() throws InvalidProtocolBufferException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(
            java.nio.ByteBuffer.wrap(bytes));

        assertEquals(original, parsed);
    }

    // TC11: Test parseFrom with ByteBuffer and ExtensionRegistry
    @Test
    public void testParseFromByteBufferWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        byte[] bytes = original.toByteArray();
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(
            java.nio.ByteBuffer.wrap(bytes), ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC12: Test parseFrom with ByteString
    @Test
    public void testParseFromByteString() throws InvalidProtocolBufferException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ByteString byteString = original.toByteString();
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(byteString);

        assertEquals(original, parsed);
    }

    // TC13: Test parseFrom with ByteString and ExtensionRegistry
    @Test
    public void testParseFromByteStringWithExtensionRegistry() throws InvalidProtocolBufferException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ByteString byteString = original.toByteString();
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(
            byteString, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC14: Test parseFrom with InputStream
    @Test
    public void testParseFromInputStream() throws IOException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(inputStream);

        assertEquals(original, parsed);
    }

    // TC15: Test parseFrom with InputStream and ExtensionRegistry
    @Test
    public void testParseFromInputStreamWithExtensionRegistry() throws IOException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        byte[] bytes = original.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(
            inputStream, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC16: Test parseDelimitedFrom with InputStream
    @Test
    public void testParseDelimitedFromInputStream() throws IOException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseDelimitedFrom(inputStream);

        assertEquals(original, parsed);
    }

    // TC17: Test parseDelimitedFrom with InputStream and ExtensionRegistry
    @Test
    public void testParseDelimitedFromInputStreamWithExtensionRegistry() throws IOException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        original.writeDelimitedTo(outputStream);
        byte[] bytes = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseDelimitedFrom(
            inputStream, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC18: Test parseFrom with CodedInputStream
    @Test
    public void testParseFromCodedInputStream() throws IOException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        byte[] bytes = original.toByteArray();
        CodedInputStream codedInputStream = CodedInputStream.newInstance(bytes);
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(codedInputStream);

        assertEquals(original, parsed);
    }

    // TC19: Test parseFrom with CodedInputStream and ExtensionRegistry
    @Test
    public void testParseFromCodedInputStreamWithExtensionRegistry() throws IOException {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        byte[] bytes = original.toByteArray();
        CodedInputStream codedInputStream = CodedInputStream.newInstance(bytes);
        ColumnarDeltaRequest parsed = ColumnarDeltaRequest.parseFrom(
            codedInputStream, ExtensionRegistry.getEmptyRegistry());

        assertEquals(original, parsed);
    }

    // TC20: Test newBuilderForType method
    @Test
    public void testNewBuilderForType() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.getDefaultInstance();
        ColumnarDeltaRequest.Builder builder = request.newBuilderForType();

        assertNotNull(builder);
    }

    // TC21: Test newBuilder method
    @Test
    public void testNewBuilder() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder();

        assertNotNull(builder);
    }

    // TC22: Test newBuilder with prototype
    @Test
    public void testNewBuilderWithPrototype() {
        ColumnarDeltaRequest prototype = ColumnarDeltaRequest.newBuilder()
            .setFileName("prototype.csv")
            .setOffset(50)
            .setLength(100)
            .build();

        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder(prototype);
        ColumnarDeltaRequest request = builder.build();

        assertEquals(prototype, request);
    }

    // TC23: Test toBuilder method
    @Test
    public void testToBuilder() {
        ColumnarDeltaRequest original = ColumnarDeltaRequest.newBuilder()
            .setFileName("original.csv")
            .setOffset(50)
            .setLength(100)
            .build();

        ColumnarDeltaRequest.Builder builder = original.toBuilder();
        ColumnarDeltaRequest request = builder.build();

        assertEquals(original, request);
        assertNotSame(original, request);
    }

    // TC24: Test getParser method
    @Test
    public void testGetParser() {
        com.google.protobuf.Parser<ColumnarDeltaRequest> parser = ColumnarDeltaRequest.parser();

        assertNotNull(parser);
    }

    // TC25: Test getParserForType method
    @Test
    public void testGetParserForType() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.getDefaultInstance();
        com.google.protobuf.Parser<ColumnarDeltaRequest> parser = request.getParserForType();

        assertNotNull(parser);
    }

    // TC26: Test getDefaultInstanceForType method
    @Test
    public void testGetDefaultInstanceForType() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.getDefaultInstance();
        ColumnarDeltaRequest defaultInstance = request.getDefaultInstanceForType();

        assertNotNull(defaultInstance);
        assertEquals(request, defaultInstance);
    }

    // TC27: Test Builder setFileName with null value
    @Test(expected = NullPointerException.class)
    public void testBuilderSetFileNameWithNull() {
        ColumnarDeltaRequest.newBuilder().setFileName(null);
    }

    // TC28: Test Builder setFileNameBytes with null value
    @Test(expected = NullPointerException.class)
    public void testBuilderSetFileNameBytesWithNull() {
        ColumnarDeltaRequest.newBuilder().setFileNameBytes(null);
    }

    // TC29: Test Builder getFileNameBytes
    @Test
    public void testBuilderGetFileNameBytes() {
        String fileName = "test.csv";
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setFileName(fileName);

        ByteString bytes = builder.getFileNameBytes();
        assertNotNull(bytes);
        assertEquals(fileName, bytes.toStringUtf8());
    }

    // TC30: Test Builder clearFileName
    @Test
    public void testBuilderClearFileName() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .clearFileName();

        assertTrue(builder.getFileName().isEmpty());
    }

    // TC31: Test Builder clearOffset
    @Test
    public void testBuilderClearOffset() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setOffset(100)
            .clearOffset();

        assertEquals(0, builder.getOffset());
    }

    // TC32: Test Builder clearLength
    @Test
    public void testBuilderClearLength() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setLength(200)
            .clearLength();

        assertEquals(0, builder.getLength());
    }

    // TC33: Test Builder clear
    @Test
    public void testBuilderClear() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .clear();

        assertTrue(builder.getFileName().isEmpty());
        assertEquals(0, builder.getOffset());
        assertEquals(0, builder.getLength());
    }

    // TC34: Test Builder getDescriptorForType
    @Test
    public void testBuilderGetDescriptorForType() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder();
        com.google.protobuf.Descriptors.Descriptor descriptor = builder.getDescriptorForType();

        assertNotNull(descriptor);
    }

    // TC35: Test Builder getDefaultInstanceForType
    @Test
    public void testBuilderGetDefaultInstanceForType() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder();
        ColumnarDeltaRequest defaultInstance = builder.getDefaultInstanceForType();

        assertNotNull(defaultInstance);
        assertTrue(defaultInstance.getFileName().isEmpty());
        assertEquals(0, defaultInstance.getOffset());
        assertEquals(0, defaultInstance.getLength());
    }

    // TC36: Test Builder buildPartial
    @Test
    public void testBuilderBuildPartial() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200);

        ColumnarDeltaRequest request = builder.buildPartial();

        assertEquals("test.csv", request.getFileName());
        assertEquals(100, request.getOffset());
        assertEquals(200, request.getLength());
    }

    // TC37: Test Builder clone
    @Test
    public void testBuilderClone() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200);

        ColumnarDeltaRequest.Builder clonedBuilder = builder.clone();

        assertEquals(builder.getFileName(), clonedBuilder.getFileName());
        assertEquals(builder.getOffset(), clonedBuilder.getOffset());
        assertEquals(builder.getLength(), clonedBuilder.getLength());
    }

    // TC38: Test Builder setField
    @Test
    public void testBuilderSetField() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder();
        com.google.protobuf.Descriptors.Descriptor descriptor = ColumnarDeltaRequest.getDescriptor();
        com.google.protobuf.Descriptors.FieldDescriptor fileNameField = descriptor.findFieldByName("file_name");

        builder.setField(fileNameField, "test.csv");

        assertEquals("test.csv", builder.getFileName());
    }

    // TC39: Test Builder clearField
    @Test
    public void testBuilderClearField() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv");

        com.google.protobuf.Descriptors.Descriptor descriptor = ColumnarDeltaRequest.getDescriptor();
        com.google.protobuf.Descriptors.FieldDescriptor fileNameField = descriptor.findFieldByName("file_name");

        builder.clearField(fileNameField);

        assertTrue(builder.getFileName().isEmpty());
    }

    // TC40: Test Builder mergeFrom with Message
    @Test
    public void testBuilderMergeFromMessage() {
        ColumnarDeltaRequest other = ColumnarDeltaRequest.newBuilder()
            .setFileName("other.csv")
            .setOffset(50)
            .setLength(100)
            .build();

        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .mergeFrom(other);

        assertEquals("other.csv", builder.getFileName());
        assertEquals(50, builder.getOffset());
        assertEquals(100, builder.getLength());
    }

    // TC41: Test Builder mergeFrom with ColumnarDeltaRequest
    @Test
    public void testBuilderMergeFromColumnarDeltaRequest() {
        ColumnarDeltaRequest other = ColumnarDeltaRequest.newBuilder()
            .setFileName("other.csv")
            .setOffset(50)
            .setLength(100)
            .build();

        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder()
            .mergeFrom(other);

        assertEquals("other.csv", builder.getFileName());
        assertEquals(50, builder.getOffset());
        assertEquals(100, builder.getLength());
    }

    // TC42: Test Builder isInitialized
    @Test
    public void testBuilderIsInitialized() {
        ColumnarDeltaRequest.Builder builder = ColumnarDeltaRequest.newBuilder();

        assertTrue(builder.isInitialized());
    }

    // TC43: Test equals with self
    @Test
    public void testEqualsWithSelf() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        assertEquals(request, request);
    }

    // TC44: Test equals with null
    @Test
    public void testEqualsWithNull() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        assertFalse(request.equals(null));
    }

    // TC45: Test equals with different class
    @Test
    public void testEqualsWithDifferentClass() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        assertFalse(request.equals("string"));
    }

    // TC46: Test equals with different offset
    @Test
    public void testEqualsWithDifferentOffset() {
        ColumnarDeltaRequest req1 = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ColumnarDeltaRequest req2 = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(150)
            .setLength(200)
            .build();

        assertFalse(req1.equals(req2));
    }

    // TC47: Test equals with different length
    @Test
    public void testEqualsWithDifferentLength() {
        ColumnarDeltaRequest req1 = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(200)
            .build();

        ColumnarDeltaRequest req2 = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(100)
            .setLength(250)
            .build();

        assertFalse(req1.equals(req2));
    }

    // TC48: Test parseFrom with invalid data
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromInvalidData() throws InvalidProtocolBufferException {
        byte[] invalidData = new byte[] { 0x00, 0x01, 0x02, 0x03 };
        ColumnarDeltaRequest.parseFrom(invalidData);
    }

    // TC49: Test parseFrom with invalid data and ExtensionRegistry
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromInvalidDataWithExtensionRegistry() throws InvalidProtocolBufferException {
        byte[] invalidData = new byte[] { 0x00, 0x01, 0x02, 0x03 };
        ColumnarDeltaRequest.parseFrom(invalidData, ExtensionRegistry.getEmptyRegistry());
    }

    // TC50: Test parseFrom with ByteString containing invalid data
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromByteStringInvalidData() throws InvalidProtocolBufferException {
        ByteString invalidByteString = ByteString.copyFrom(new byte[] { 0x00, 0x01, 0x02, 0x03 });
        ColumnarDeltaRequest.parseFrom(invalidByteString);
    }

    // TC51: Test parseFrom with ByteString containing invalid data and ExtensionRegistry
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromByteStringInvalidDataWithExtensionRegistry() throws InvalidProtocolBufferException {
        ByteString invalidByteString = ByteString.copyFrom(new byte[] { 0x00, 0x01, 0x02, 0x03 });
        ColumnarDeltaRequest.parseFrom(invalidByteString, ExtensionRegistry.getEmptyRegistry());
    }

    // TC52: Test parseFrom with ByteBuffer containing invalid data
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromByteBufferInvalidData() throws InvalidProtocolBufferException {
        java.nio.ByteBuffer invalidBuffer = java.nio.ByteBuffer.wrap(new byte[] { 0x00, 0x01, 0x02, 0x03 });
        ColumnarDeltaRequest.parseFrom(invalidBuffer);
    }

    // TC53: Test parseFrom with ByteBuffer containing invalid data and ExtensionRegistry
    @Test(expected = InvalidProtocolBufferException.class)
    public void testParseFromByteBufferInvalidDataWithExtensionRegistry() throws InvalidProtocolBufferException {
        java.nio.ByteBuffer invalidBuffer = java.nio.ByteBuffer.wrap(new byte[] { 0x00, 0x01, 0x02, 0x03 });
        ColumnarDeltaRequest.parseFrom(invalidBuffer, ExtensionRegistry.getEmptyRegistry());
    }

    // TC54: Test empty file name
    @Test
    public void testEmptyFileName() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("")
            .setOffset(100)
            .setLength(200)
            .build();

        assertTrue(request.getFileName().isEmpty());
        assertEquals(100, request.getOffset());
        assertEquals(200, request.getLength());
    }

    // TC55: Test zero offset and length
    @Test
    public void testZeroOffsetAndLength() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(0)
            .setLength(0)
            .build();

        assertEquals("test.csv", request.getFileName());
        assertEquals(0, request.getOffset());
        assertEquals(0, request.getLength());
    }

    // TC56: Test negative offset and length
    @Test
    public void testNegativeOffsetAndLength() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(-100)
            .setLength(-200)
            .build();

        assertEquals("test.csv", request.getFileName());
        assertEquals(-100, request.getOffset());
        assertEquals(-200, request.getLength());
    }

    // TC57: Test maximum integer values for offset and length
    @Test
    public void testMaxIntegerValues() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(Integer.MAX_VALUE)
            .setLength(Integer.MAX_VALUE)
            .build();

        assertEquals("test.csv", request.getFileName());
        assertEquals(Integer.MAX_VALUE, request.getOffset());
        assertEquals(Integer.MAX_VALUE, request.getLength());
    }

    // TC58: Test minimum integer values for offset and length
    @Test
    public void testMinIntegerValues() {
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName("test.csv")
            .setOffset(Integer.MIN_VALUE)
            .setLength(Integer.MIN_VALUE)
            .build();

        assertEquals("test.csv", request.getFileName());
        assertEquals(Integer.MIN_VALUE, request.getOffset());
        assertEquals(Integer.MIN_VALUE, request.getLength());
    }

    // TC59: Test special characters in file name
    @Test
    public void testSpecialCharactersInFileName() {
        String specialFileName = "test_文件_@#$%^&*()_+=[]{}|;':\",./<>?.csv";
        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName(specialFileName)
            .build();

        assertEquals(specialFileName, request.getFileName());
    }

    // TC60: Test very long file name
    @Test
    public void testVeryLongFileName() {
        StringBuilder longFileNameBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longFileNameBuilder.append("a");
        }
        String longFileName = longFileNameBuilder.toString();

        ColumnarDeltaRequest request = ColumnarDeltaRequest.newBuilder()
            .setFileName(longFileName)
            .build();

        assertEquals(longFileName, request.getFileName());
    }
}
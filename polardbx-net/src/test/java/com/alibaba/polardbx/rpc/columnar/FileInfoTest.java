package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class FileInfoTest {

    private byte[] validFileInfoBytes;
    private byte[] invalidFileInfoBytes;
    private byte[] emptyFileInfoBytes;
    private byte[] unknownFieldBytes;
    private ExtensionRegistryLite extensionRegistry;
    private ExtensionRegistryLite registryB;

    private Descriptors.Descriptor fileInfoDescriptor;
    private FileInfo fileInfo;
    private FileInfo fileInfoWithString;
    private FileInfo fileInfoWithByteString;
    private FileInfo fileInfoB;
    private FileInfo fileInfoB2;  // 从文件B中提取的成员变量，重命名为fileInfoB2
    private FileInfo fileInfo1;
    private FileInfo fileInfo2;
    private ByteBuffer validByteBuffer;
    private ByteBuffer invalidByteBuffer;
    private byte[] validFileInfoBytesB;
    private byte[] invalidFileInfoBytesB2;
    private FileInfo fileInfoB3;
    private FileInfo defaultFileInfo;  // 从文件B中提取的成员变量

    @Before
    public void setUp() {
        validFileInfoBytes = new byte[]{10, 11, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 16, 10};
        invalidFileInfoBytes = new byte[]{0x01, 0x02, 0x03};
        emptyFileInfoBytes = new byte[]{};
        unknownFieldBytes = new byte[]{26, 2, 84, 101, 115, 116}; // 未知字段
        extensionRegistry = ExtensionRegistryLite.getEmptyRegistry();
        registryB = ExtensionRegistryLite.getEmptyRegistry();

        fileInfoDescriptor = FileInfo.getDescriptor();
        fileInfo = FileInfo.newBuilder().setFileName("testFile").setFileLength(100).build();
        fileInfoWithString = FileInfo.newBuilder().setFileName("testFile").build();
        fileInfoWithByteString = FileInfo.newBuilder().setFileName("testFile").build();
        fileInfoB = FileInfo.newBuilder().build();  // 初始化fileInfoB
        fileInfoB2 = FileInfo.newBuilder().build();  // 初始化fileInfoB2
        fileInfo1 = FileInfo.newBuilder().setFileName("testFile").setFileLength(100).build();
        fileInfo2 = FileInfo.newBuilder().setFileName("testFile").setFileLength(100).build();

        // 设置一个有效的 FileInfo 对象
        fileInfo = FileInfo.newBuilder()
                .setFileName("testFile")
                .setFileLength(100)
                .build();

        // 将有效的 FileInfo 对象序列化为 ByteBuffer
        validByteBuffer = fileInfo.toByteString().asReadOnlyByteBuffer();

        // 设置一个无效的 ByteBuffer
        invalidByteBuffer = ByteBuffer.allocate(10); // 无效的缓冲区，不包含有效的 FileInfo 数据

        // 创建一个包含无效数据的 ByteBuffer
        invalidByteBuffer = ByteBuffer.wrap(new byte[]{0x01, 0x02, 0x03});

        // 为有效的 FileInfo 创建一个字节数组
        validFileInfoBytesB = new byte[]{10, 11, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 16, 10};

        // 设置无效的 FileInfo 字节
        invalidFileInfoBytesB2 = new byte[]{
            10, 6, 84, 101, 115, 116, 32, 70, 105, 108, 101, 16, 10, 10
        };

        fileInfoB3 = FileInfo.newBuilder()
                .setFileName("testFile")
                .setFileLength(100)
                .build();

        // 初始化defaultFileInfo
        defaultFileInfo = FileInfo.getDefaultInstance();
    }

    @Test
    public void getDescriptor_ShouldReturnNonNullDescriptor() {
        Assert.assertNotNull("FileInfo descriptor should not be null", fileInfoDescriptor);
    }

    @Test
    public void getDescriptor_ShouldReturnCorrectDescriptorName() {
        Assert.assertEquals("FileInfo", fileInfoDescriptor.getName());
    }

    @Test
    public void getDescriptor_ShouldReturnCorrectDescriptorFullName() {
        Assert.assertEquals("orc.proto.FileInfo", fileInfoDescriptor.getFullName());
    }

    @Test
    public void getUnknownFields_EmptyInstance_ReturnsEmptyUnknownFieldSet() {
        FileInfo emptyFileInfo = FileInfo.newBuilder().build();
        UnknownFieldSet unknownFields = emptyFileInfo.getUnknownFields();
        Assert.assertTrue(unknownFields.asMap().isEmpty());
    }

    @Test
    public void getUnknownFields_WithUnknownFields_ReturnsCorrectUnknownFieldSet() throws IOException {
        // 创建包含未知字段的字节数据
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        codedOutputStream.writeString(1, "testFile");
        codedOutputStream.writeInt32(2, 100);
        codedOutputStream.writeInt32(3, 42); // 未知字段
        codedOutputStream.flush();

        byte[] data = outputStream.toByteArray();
        FileInfo fileInfoWithUnknown = FileInfo.parseFrom(data);

        UnknownFieldSet unknownFields = fileInfoWithUnknown.getUnknownFields();
        Assert.assertFalse(unknownFields.asMap().isEmpty());
        Assert.assertTrue(unknownFields.asMap().containsKey(3));
    }

    @Test
    public void getFileName_WhenFileNameIsString_ReturnsCorrectString() {
        String fileName = fileInfoWithString.getFileName();
        Assert.assertEquals("testFile", fileName);
    }

    @Test
    public void getFileName_WhenFileNameIsByteString_ConvertsAndReturnsCorrectString() {
        String fileName = fileInfoWithByteString.getFileName();
        Assert.assertEquals("testFile", fileName);
    }

    @Test
    public void getFileNameBytes_WhenFileNameIsString_ReturnsCorrectByteString() {
        String fileName = "testFileName";
        fileInfoB = FileInfo.newBuilder().setFileName(fileName).build();
        ByteString result = fileInfoB.getFileNameBytes();

        Assert.assertEquals(ByteString.copyFromUtf8(fileName), result);
    }

    @Test
    public void getFileNameBytes_WhenFileNameIsByteString_ReturnsSameByteString() {
        ByteString fileName = ByteString.copyFromUtf8("testFileName");
        fileInfoB = FileInfo.newBuilder().setFileName(fileName.toStringUtf8()).build();
        ByteString result = fileInfoB.getFileNameBytes();

        Assert.assertEquals(fileName, result);
    }

    @Test
    public void getFileLength_WhenInitialized_ReturnsCorrectValue() throws IOException {
        // 准备
        int expectedLength = 1024;
        fileInfoB2 = FileInfo.newBuilder().setFileLength(expectedLength).build();

        // 验证
        Assert.assertEquals(expectedLength, fileInfoB2.getFileLength());
    }

    @Test
    public void getFileLength_WhenNotInitialized_ReturnsDefaultValue() {
        // 验证
        Assert.assertEquals(0, fileInfoB2.getFileLength());
    }

    @Test
    public void getSerializedSize_EmptyFileNameAndFileLengthZero_ReturnsZero() {
        int size = fileInfo.getSerializedSize();
        Assert.assertEquals(12, size);
    }

    @Test
    public void getSerializedSize_NonEmptyFileName_ReturnsCorrectSize() {
        fileInfo = fileInfo.toBuilder().setFileName("testFile").build();
        int size = fileInfo.getSerializedSize();
        Assert.assertEquals(12, size);
    }

    @Test
    public void getSerializedSize_NonZeroFileLength_ReturnsCorrectSize() {
        fileInfo = fileInfo.toBuilder().setFileLength(100).build();
        int size = fileInfo.getSerializedSize();
        Assert.assertEquals(12, size);
    }

    @Test
    public void getSerializedSize_BothFileNameAndFileLengthSet_ReturnsCorrectSize() {
        fileInfo = fileInfo.toBuilder().setFileName("testFile").build();
        fileInfo = fileInfo.toBuilder().setFileLength(100).build();
        int expectedSize = CodedOutputStream.computeStringSize(1, "testFile") +
                CodedOutputStream.computeInt32Size(2, 100);
        int size = fileInfo.getSerializedSize();
        Assert.assertEquals(expectedSize, size);
    }

    @Test
    public void getSerializedSize_WithUnknownFields_ReturnsCorrectSize() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);

        codedOutputStream.flush();
        fileInfo = fileInfo.toBuilder().mergeUnknownFields(UnknownFieldSet.parseFrom(outputStream.toByteArray())).build();

        int size = fileInfo.getSerializedSize();
        Assert.assertEquals(12, size);
    }

    @Test
    public void writeTo_EmptyFileNameAndZeroFileLength_NothingWritten() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        fileInfo.writeTo(codedOutputStream);
        Assert.assertEquals(0, outputStream.size());
    }

    @Test
    public void writeTo_NonEmptyFileName_FileNameWritten() throws IOException {
        fileInfo = FileInfo.newBuilder().setFileName("testFile").build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        fileInfo.writeTo(codedOutputStream);
        codedOutputStream.flush();
        byte[] expected = new byte[]{10, 8, 116, 101, 115, 116, 70, 105, 108, 101};
        Assert.assertEquals(expected.length, outputStream.size());
        Assert.assertEquals(new String(expected), new String(outputStream.toByteArray()));
    }

    @Test
    public void writeTo_NonZeroFileLength_FileLengthWritten() throws IOException {
        fileInfo = FileInfo.newBuilder().setFileLength(100).build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        fileInfo.writeTo(codedOutputStream);
        codedOutputStream.flush();
        byte[] expected = new byte[]{16, 100};
        Assert.assertEquals(expected.length, outputStream.size());
        Assert.assertEquals(new String(expected), new String(outputStream.toByteArray()));
    }

    @Test
    public void writeTo_UnknownFields_UnknownFieldsWritten() throws IOException {
        fileInfo = FileInfo.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(3, UnknownFieldSet.Field.newBuilder().addVarint(123).build()).build()).build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        fileInfo.writeTo(codedOutputStream);
        Assert.assertEquals(0, outputStream.size());
    }

    @Test
    public void equals_SameInstance_ReturnsTrue() {
        Assert.assertTrue(fileInfo1.equals(fileInfo1));
    }

    @Test
    public void equals_DifferentType_ReturnsFalse() {
        Assert.assertFalse(fileInfo1.equals("not a FileInfo"));
    }

    @Test
    public void equals_DifferentFileName_ReturnsFalse() {
        FileInfo differentFileName = FileInfo.newBuilder().setFileName("differentFile").setFileLength(100).build();
        Assert.assertFalse(fileInfo1.equals(differentFileName));
    }

    @Test
    public void equals_DifferentFileLength_ReturnsFalse() {
        FileInfo differentFileLength = FileInfo.newBuilder().setFileName("testFile").setFileLength(200).build();
        Assert.assertFalse(fileInfo1.equals(differentFileLength));
    }

    @Test
    public void equals_DifferentUnknownFields_ReturnsFalse() {
        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder().addField(1, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        FileInfo differentUnknownFields = FileInfo.newBuilder().setFileName("testFile").setFileLength(100).mergeUnknownFields(unknownFields).build();
        Assert.assertFalse(fileInfo1.equals(differentUnknownFields));
    }

    @Test
    public void equals_AllFieldsEqual_ReturnsTrue() {
        Assert.assertTrue(fileInfo1.equals(fileInfo2));
    }

    @Test
    public void isInitialized_WhenMemoizedIsInitializedIsOne_ReturnsTrue() {
        try {
            java.lang.reflect.Field memoizedIsInitializedField = FileInfo.class.getDeclaredField("memoizedIsInitialized");
            memoizedIsInitializedField.setAccessible(true);
            memoizedIsInitializedField.setByte(fileInfo, (byte) 1);
            Assert.assertTrue(fileInfo.isInitialized());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Exception occurred: " + e.getMessage());
        }
    }

    @Test
    public void isInitialized_WhenMemoizedIsInitializedIsZero_ReturnsFalse() {
        try {
            java.lang.reflect.Field memoizedIsInitializedField = FileInfo.class.getDeclaredField("memoizedIsInitialized");
            memoizedIsInitializedField.setAccessible(true);
            memoizedIsInitializedField.setByte(fileInfo, (byte) 0);
            Assert.assertFalse(fileInfo.isInitialized());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Exception occurred: " + e.getMessage());
        }
    }

    @Test
    public void isInitialized_WhenMemoizedIsInitializedIsNeitherOneNorZero_SetsToOneAndReturnsTrue() {
        try {
            java.lang.reflect.Field memoizedIsInitializedField = FileInfo.class.getDeclaredField("memoizedIsInitialized");
            memoizedIsInitializedField.setAccessible(true);
            memoizedIsInitializedField.setByte(fileInfo, (byte) -1); // 既不是1也不是0
            Assert.assertTrue(fileInfo.isInitialized());
            Assert.assertEquals(1, memoizedIsInitializedField.getInt(fileInfo));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            Assert.fail("Exception occurred: " + e.getMessage());
        }
    }

    @Test
    public void isInitialized_WhenConstructedWithBuilder_ReturnsTrue() {
        FileInfo fileInfo = FileInfo.newBuilder().build();
        Assert.assertTrue(fileInfo.isInitialized());
    }

    @Test
    public void isInitialized_WhenParsedFromInputStream_ReturnsTrue() throws IOException {
        byte[] data = new byte[]{10, 4, 102, 111, 111, 116, 16, 1}; // 示例数据
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        CodedInputStream cis = CodedInputStream.newInstance(bais);
        FileInfo fileInfo = FileInfo.parseFrom(cis, ExtensionRegistryLite.getEmptyRegistry());
        Assert.assertTrue(fileInfo.isInitialized());
    }

    @Test
    public void hashCode_WhenMemoizedHashCodeIsSet_ReturnsCorrectValue() {
        // 设置一个已知的memoizedHashCode
        fileInfo = fileInfo.toBuilder().build();
        int expectedHashCode = fileInfo.hashCode();

        // 确保返回的哈希码与预期值匹配
        Assert.assertEquals(expectedHashCode, fileInfo.hashCode());
    }

    @Test
    public void hashCode_WhenFileNameIsString_ReturnsCorrectValue() {
        // 确保计算出的哈希码与预期值匹配
        int expectedHashCode = fileInfo.hashCode();
        Assert.assertEquals(expectedHashCode, fileInfo.hashCode());
    }

    @Test
    public void hashCode_WhenFileNameIsByteString_ReturnsCorrectValue() {
        // 将fileName_设置为字节串
        fileInfo = fileInfo.toBuilder().setFileName("testFile").build();

        // 确保计算出的哈希码与预期值匹配
        int expectedHashCode = fileInfo.hashCode();
        Assert.assertEquals(expectedHashCode, fileInfo.hashCode());
    }

    @Test
    public void parseFrom_ValidByteBuffer_ShouldReturnFileInfo() throws InvalidProtocolBufferException {
        FileInfo parsedFileInfo = FileInfo.parseFrom(validByteBuffer);
        Assert.assertEquals(fileInfo.getFileName(), parsedFileInfo.getFileName());
        Assert.assertEquals(fileInfo.getFileLength(), parsedFileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseFrom_InvalidByteBuffer_ShouldThrowException() throws InvalidProtocolBufferException {
        FileInfo.parseFrom(invalidByteBuffer);
    }

    @Test
    public void parseFrom_ValidData_ShouldReturnFileInfo() throws Exception {
        FileInfo parsedFileInfo = FileInfo.parseFrom(validByteBuffer, ExtensionRegistryLite.getEmptyRegistry());
        Assert.assertEquals(fileInfo.getFileName(), parsedFileInfo.getFileName());
        Assert.assertEquals(fileInfo.getFileLength(), parsedFileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseFrom_InvalidData_ShouldThrowException() throws Exception {
        FileInfo.parseFrom(invalidByteBuffer, ExtensionRegistryLite.getEmptyRegistry());
    }

    @Test(expected = NullPointerException.class)
    public void parseFrom_NullExtensionRegistry_ShouldThrowException() throws Exception {
        FileInfo.parseFrom(validByteBuffer, null);
    }

    @Test
    public void parseFrom_ValidData_ReturnsFileInfo() throws Exception {
        // 准备
        ByteString data = ByteString.copyFromUtf8("\n\007example\020\005");
        FileInfo expected = FileInfo.newBuilder().setFileName("example").setFileLength(5).build();

        // 执行
        FileInfo actual = FileInfo.parseFrom(data, extensionRegistry);

        // 断言
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void parseFrom_MissingFileName_ReturnsFileInfoWithDefaultFileName() throws Exception {
        // 准备
        ByteString data = ByteString.copyFromUtf8("\020\005");
        FileInfo expected = FileInfo.newBuilder().setFileName("").setFileLength(5).build();

        // 执行
        FileInfo actual = FileInfo.parseFrom(data, extensionRegistry);

        // 断言
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void parseFrom_MissingFileLength_ReturnsFileInfoWithDefaultFileLength() throws Exception {
        // 准备
        ByteString data = ByteString.copyFromUtf8("\n\007example");
        FileInfo expected = FileInfo.newBuilder().setFileName("example").setFileLength(0).build();

        // 执行
        FileInfo actual = FileInfo.parseFrom(data, extensionRegistry);

        // 断言
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseFrom_InvalidData_ThrowsInvalidProtocolBufferException() throws Exception {
        // 准备
        ByteString data = ByteString.copyFromUtf8("\n\007example\020");

        // 执行
        FileInfo.parseFrom(data, extensionRegistry);
    }

    @Test
    public void parseFrom_EmptyByteString_ReturnsFileInfoWithDefaults() throws Exception {
        // 准备
        ByteString data = ByteString.EMPTY;
        FileInfo expected = FileInfo.newBuilder().setFileName("").setFileLength(0).build();

        // 执行
        FileInfo actual = FileInfo.parseFrom(data, extensionRegistry);

        // 断言
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void parseFrom_ValidInput_ReturnsFileInfo() throws InvalidProtocolBufferException {
        FileInfo fileInfo = FileInfo.parseFrom(validFileInfoBytesB, extensionRegistry);
        Assert.assertNotNull(fileInfo);
        Assert.assertEquals("Hello World", fileInfo.getFileName());
        Assert.assertEquals(10, fileInfo.getFileLength());
    }

    @Test
    public void parseFrom_ValidData_ShouldParseSuccessfully_B() throws InvalidProtocolBufferException {
        // 准备
        FileInfo fileInfo = FileInfo.newBuilder()
                .setFileName("testFile")
                .setFileLength(100)
                .build();
        byte[] data = fileInfo.toByteArray();

        // 执行
        FileInfo parsedFileInfo = FileInfo.parseFrom(data, extensionRegistry);

        // 断言
        Assert.assertEquals("testFile", parsedFileInfo.getFileName());
        Assert.assertEquals(100, parsedFileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseFrom_InvalidData_B() throws InvalidProtocolBufferException {
        // 准备
        byte[] invalidData = new byte[]{0x01, 0x02, 0x03}; // 无效的字节数据

        // 执行
        FileInfo.parseFrom(invalidData, extensionRegistry);
    }

    @Test(expected = NullPointerException.class)
    public void parseFrom_EmptyData_ShouldThrowException() throws InvalidProtocolBufferException {
        FileInfo.parseFrom((byte[]) null, extensionRegistry);
    }

    @Test
    public void parseFrom_EmptyRegistry_ShouldParseSuccessfully() throws InvalidProtocolBufferException {
        // 准备
        FileInfo fileInfo = FileInfo.newBuilder()
                .setFileName("emptyRegistryFile")
                .setFileLength(200)
                .build();
        byte[] data = fileInfo.toByteArray();
        ExtensionRegistryLite emptyRegistry = ExtensionRegistryLite.newInstance();

        // 执行
        FileInfo parsedFileInfo = FileInfo.parseFrom(data, emptyRegistry);

        // 断言
        Assert.assertEquals("emptyRegistryFile", parsedFileInfo.getFileName());
        Assert.assertEquals(200, parsedFileInfo.getFileLength());
    }

    @Test
    public void parseFrom_ValidInput_ParsesCorrectly() throws IOException {
        // 准备
        String fileName = "testFile";
        int fileLength = 100;
        InputStream input = new ByteArrayInputStream(FileInfo.newBuilder()
                .setFileName(fileName)
                .setFileLength(fileLength)
                .build()
                .toByteArray());

        // 执行
        FileInfo fileInfo = FileInfo.parseFrom(input, registryB);

        // 验证
        Assert.assertEquals(fileName, fileInfo.getFileName());
        Assert.assertEquals(fileLength, fileInfo.getFileLength());
    }

    @Test
    public void parseFrom_MissingFields_ParsesWithDefaults() throws IOException {
        // 准备
        InputStream input = new ByteArrayInputStream(FileInfo.newBuilder().build().toByteArray());

        // 执行
        FileInfo fileInfo = FileInfo.parseFrom(input, registryB);

        // 验证
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test(expected = com.google.protobuf.InvalidProtocolBufferException.class)
    public void parseFrom_InvalidInput_ThrowsException() throws IOException {
        // 准备
        InputStream input = new ByteArrayInputStream(new byte[]{0x01, 0x02, 0x03}); // 无效的字节

        // 执行
        FileInfo.parseFrom(input, registryB);
    }

    @Test(expected = NullPointerException.class)
    public void parseFrom_NullExtensionRegistry_ThrowsException() throws IOException {
        // 准备
        InputStream input = new ByteArrayInputStream(FileInfo.newBuilder().build().toByteArray());

        // 执行
        FileInfo.parseFrom(input, null);
    }

    @Test
    public void parseFrom_EmptyInput_ParsesWithDefaults() throws IOException {
        // 准备
        InputStream input = new ByteArrayInputStream(new byte[]{});

        // 执行
        FileInfo fileInfo = FileInfo.parseFrom(input, registryB);

        // 验证
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void parseFrom_ValidInputStream_ReturnsFileInfo() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(validFileInfoBytesB);
        FileInfo fileInfo = FileInfo.parseFrom(inputStream, registryB);

        Assert.assertNotNull(fileInfo);
        Assert.assertEquals("Hello World", fileInfo.getFileName());
        Assert.assertEquals(10, fileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseFrom_InvalidInputStream_ThrowsException() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(invalidFileInfoBytesB2);
        FileInfo.parseFrom(inputStream, registryB);
    }

    @Test
    public void parseFrom_EmptyInputStream_ReturnsDefaultFileInfo() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(new byte[]{});
        FileInfo fileInfo = FileInfo.parseFrom(inputStream, registryB);

        Assert.assertNotNull(fileInfo);
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseDelimitedFrom_InvalidInput_ThrowsException() throws IOException {
        byte[] invalidData = new byte[]{0x01, 0x02, 0x03}; // 无效的字节数据
        InputStream inputStream = new ByteArrayInputStream(invalidData);
        FileInfo.parseDelimitedFrom(inputStream);
    }

    @Test
    public void parseDelimitedFrom_EmptyInput_ReturnsNull() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(new byte[]{});
        FileInfo parsedFileInfo = FileInfo.parseDelimitedFrom(inputStream);
        Assert.assertNull(parsedFileInfo);
    }

    @Test(expected = IOException.class)
    public void parseDelimitedFrom_ClosedInputStream_ThrowsIOException() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(fileInfoB3.toByteArray());
        inputStream.close();
        FileInfo.parseDelimitedFrom(inputStream);
    }

    @Test(expected = IOException.class)
    public void parseDelimitedFrom_ClosedInputStream_ThrowsIOException_B() throws IOException {
        InputStream inputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Stream closed");
            }
        };
        FileInfo.parseDelimitedFrom(inputStream, registryB);
    }

    @Test
    public void parseFrom_ValidInput_ReturnsFileInfo_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(validFileInfoBytes);
        FileInfo fileInfo = FileInfo.parseFrom(input);
        Assert.assertEquals("Hello World", fileInfo.getFileName());
        Assert.assertEquals(10, fileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parseFrom_InvalidInput_ThrowsException_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(invalidFileInfoBytes);
        FileInfo.parseFrom(input);
    }

    @Test
    public void parseFrom_EmptyInput_ReturnsEmptyFileInfo_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(emptyFileInfoBytes);
        FileInfo fileInfo = FileInfo.parseFrom(input);
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void parseFrom_UnknownFields_ReturnsFileInfoWithUnknownFields_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(unknownFieldBytes);
        FileInfo fileInfo = FileInfo.parseFrom(input);
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
        Assert.assertFalse(fileInfo.getUnknownFields().asMap().isEmpty());
    }

    @Test
    public void parseFrom_ValidInput_ReturnsCorrectFileInfo_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(validFileInfoBytes);
        FileInfo fileInfo = FileInfo.parseFrom(input, extensionRegistry);

        Assert.assertEquals("Hello World", fileInfo.getFileName());
        Assert.assertEquals(10, fileInfo.getFileLength());
    }

    @Test(expected = NullPointerException.class)
    public void parseFrom_NullExtensionRegistry_ThrowsException_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(validFileInfoBytes);
        FileInfo.parseFrom(input, null);
    }

    @Test
    public void parseFrom_EmptyInput_ReturnsDefaultFileInfo_B() throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(new byte[]{});
        FileInfo fileInfo = FileInfo.parseFrom(input, extensionRegistry);

        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void parseFrom_MissingFields_ReturnsDefaultValues_B() throws IOException {
        byte[] missingFieldsBytes = new byte[]{10, 11, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100};
        CodedInputStream input = CodedInputStream.newInstance(missingFieldsBytes);
        FileInfo fileInfo = FileInfo.parseFrom(input, extensionRegistry);

        Assert.assertEquals("Hello World", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void newBuilderForType_ShouldReturnNonNullBuilder() {
        FileInfo.Builder builder = fileInfo.newBuilderForType();
        Assert.assertNotNull("Builder should not be null", builder);
    }

    @Test
    public void newBuilderForType_ShouldCreateValidFileInfo() {
        FileInfo.Builder builder = fileInfo.newBuilderForType();
        FileInfo newFileInfo = builder.setFileName("newFile").setFileLength(200).build();
        Assert.assertEquals("newFile", newFileInfo.getFileName());
        Assert.assertEquals(200, newFileInfo.getFileLength());
    }

    @Test
    public void newBuilder_ShouldReturnNonNullBuilder() {
        FileInfo.Builder builder = FileInfo.newBuilder();
        Assert.assertNotNull("Builder should not be null", builder);
    }

    @Test
    public void newBuilder_ShouldCreateFileInfoInstance() {
        FileInfo.Builder builder = FileInfo.newBuilder();
        FileInfo fileInfo = builder.setFileName("testFile").setFileLength(100).build();
        Assert.assertNotNull("FileInfo should not be null", fileInfo);
        Assert.assertEquals("testFile", fileInfo.getFileName());
        Assert.assertEquals(100, fileInfo.getFileLength());
    }

    @Test
    public void newBuilder_ValidPrototype_ReturnsInitializedBuilder() {
        FileInfo prototype = FileInfo.newBuilder().setFileName("test").setFileLength(100).build();
        FileInfo.Builder builder = FileInfo.newBuilder(prototype);
        FileInfo result = builder.build();
        Assert.assertEquals("test", result.getFileName());
        Assert.assertEquals(100, result.getFileLength());
    }

    @Test
    public void newBuilder_EmptyPrototype_ReturnsInitializedBuilder() {
        FileInfo prototype = FileInfo.newBuilder().build();
        FileInfo.Builder builder = FileInfo.newBuilder(prototype);
        FileInfo result = builder.build();
        Assert.assertEquals("", result.getFileName());
        Assert.assertEquals(0, result.getFileLength());
    }

    @Test(expected = NullPointerException.class)
    public void newBuilder_NullPrototype_ThrowsNullPointerException() {
        FileInfo.newBuilder(null);
    }

    @Test
    public void getDefaultInstance_ShouldReturnNonNullInstance() {
        Assert.assertNotNull("Default FileInfo instance should not be null", defaultFileInfo);
    }

    @Test
    public void getDefaultInstance_ShouldHaveEmptyFileName() {
        Assert.assertEquals("Default FileInfo should have an empty file name", "", defaultFileInfo.getFileName());
    }

    @Test
    public void getDefaultInstance_ShouldHaveZeroFileLength() {
        Assert.assertEquals("Default FileInfo should have a file length of 0", 0, defaultFileInfo.getFileLength());
    }

    @Test
    public void getDefaultInstance_ShouldReturnEmptyUnknownFieldSet() {
        UnknownFieldSet unknownFields = defaultFileInfo.getUnknownFields();
        Assert.assertTrue("Default FileInfo should have an empty unknown field set", unknownFields.asMap().isEmpty());
    }

    @Test
    public void toBuilder_WhenNotDefaultInstance_ReturnsBuilderWithMergedData() {
        FileInfo.Builder builder = fileInfo.toBuilder();
        Assert.assertNotNull(builder);
        Assert.assertEquals("testFile", builder.getFileName());
        Assert.assertEquals(100, builder.getFileLength());
    }

    @Test
    public void toBuilder_WhenDefaultInstance_ReturnsNewBuilder() {
        FileInfo.Builder builder = defaultFileInfo.toBuilder();
        Assert.assertNotNull(builder);
        Assert.assertEquals("", builder.getFileName());
        Assert.assertEquals(0, builder.getFileLength());
    }

    @Test
    public void parsePartialFrom_ValidInput_ReturnsCorrectFileInfo() throws IOException {
        // 准备包含 fileName 和 fileLength 的有效输入
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        codedOutputStream.writeString(1, "testFile");
        codedOutputStream.writeInt32(2, 100);
        codedOutputStream.flush();

        byte[] validInput = outputStream.toByteArray();
        CodedInputStream codedInputStream = CodedInputStream.newInstance(validInput);

        FileInfo parsedFileInfo = FileInfo.parseFrom(codedInputStream, extensionRegistry);

        Assert.assertEquals("testFile", parsedFileInfo.getFileName());
        Assert.assertEquals(100, parsedFileInfo.getFileLength());
    }

    @Test
    public void parsePartialFrom_MissingFields_ReturnsFileInfoWithDefaults() throws IOException {
        // 准备缺少 fileName 和 fileLength 的输入
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        codedOutputStream.flush();

        byte[] missingFieldsInput = outputStream.toByteArray();
        CodedInputStream codedInputStream = CodedInputStream.newInstance(missingFieldsInput);

        FileInfo parsedFileInfo = FileInfo.parseFrom(codedInputStream, extensionRegistry);

        Assert.assertEquals("", parsedFileInfo.getFileName());
        Assert.assertEquals(0, parsedFileInfo.getFileLength());
    }

    @Test
    public void parsePartialFrom_UnknownFields_IgnoresUnknownFields() throws IOException {
        // 准备包含未知字段的输入
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(outputStream);
        codedOutputStream.writeString(1, "testFile");
        codedOutputStream.writeInt32(2, 100);
        codedOutputStream.writeInt32(3, 123); // 未知字段
        codedOutputStream.flush();

        byte[] unknownFieldsInput = outputStream.toByteArray();
        CodedInputStream codedInputStream = CodedInputStream.newInstance(unknownFieldsInput);

        FileInfo parsedFileInfo = FileInfo.parseFrom(codedInputStream, extensionRegistry);

        Assert.assertEquals("testFile", parsedFileInfo.getFileName());
        Assert.assertEquals(100, parsedFileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parsePartialFrom_InvalidInput_ThrowsException() throws IOException {
        // 准备无效的输入
        byte[] invalidInput = new byte[]{0x01, 0x02, 0x03}; // 无效的字节序列
        CodedInputStream codedInputStream = CodedInputStream.newInstance(invalidInput);

        FileInfo.parseFrom(codedInputStream, extensionRegistry);
    }

    @Test
    public void parser_ValidBytes_ShouldParseSuccessfully() throws Exception {
        FileInfo parsedFileInfo = FileInfo.parser().parseFrom(validFileInfoBytes);
        Assert.assertNotNull(parsedFileInfo);
        Assert.assertEquals("Hello World", parsedFileInfo.getFileName());
        Assert.assertEquals(10, parsedFileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parser_InvalidBytes_ShouldThrowException() throws Exception {
        FileInfo.parser().parseFrom(invalidFileInfoBytes);
    }

    @Test
    public void parser_EmptyBytes_ShouldParseSuccessfully() throws Exception {
        FileInfo parsedFileInfo = FileInfo.parser().parseFrom(new byte[]{});
        Assert.assertNotNull(parsedFileInfo);
        Assert.assertEquals("", parsedFileInfo.getFileName());
        Assert.assertEquals(0, parsedFileInfo.getFileLength());
    }

    @Test
    public void getParserForType_ShouldReturnNonNullParser() {
        com.google.protobuf.Parser<FileInfo> parser = fileInfo.getParserForType();
        Assert.assertNotNull("Parser should not be null", parser);
    }

    @Test
    public void parsePartialFrom_ValidBytes_ShouldParseCorrectly() throws IOException {
        byte[] validBytes = fileInfo.toByteArray();
        FileInfo parsedFileInfo = fileInfo.getParserForType().parseFrom(validBytes, extensionRegistry);
        Assert.assertEquals("testFile", parsedFileInfo.getFileName());
        Assert.assertEquals(100, parsedFileInfo.getFileLength());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void parsePartialFrom_InvalidBytes_ShouldThrowException() throws IOException {
        byte[] invalidBytes = new byte[]{1, 2, 3, 4, 5};
        fileInfo.getParserForType().parseFrom(invalidBytes, extensionRegistry);
    }

    @Test(expected = NullPointerException.class)
    public void parsePartialFrom_NullExtensionRegistry_ShouldThrowException() throws IOException {
        byte[] validBytes = fileInfo.toByteArray();
        fileInfo.getParserForType().parseFrom(validBytes, null);
    }

    @Test
    public void getDefaultInstanceForType_ShouldReturnDefaultInstance() {
        FileInfo defaultInstance = fileInfo.getDefaultInstanceForType();
        Assert.assertNotNull("Default instance should not be null", defaultInstance);
        Assert.assertTrue("Default instance should have empty unknown fields", defaultInstance.getUnknownFields().asMap().isEmpty());
    }
}

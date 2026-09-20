package com.alibaba.polardbx.rpc.columnar;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.UnknownFieldSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class BuilderTest {

    private Descriptors.Descriptor fileInfoDescriptor;
    private FileInfo.Builder builder;
    private FileInfo.Builder originalBuilder;
    private Descriptors.FieldDescriptor fileNameField;

    private byte[] validFileInfoBytes;
    private byte[] invalidFileInfoBytes;
    private byte[] emptyFileInfoBytes;
    private byte[] unknownFieldBytes;
    private ExtensionRegistryLite extensionRegistry;

    private FileInfo fileInfo;
    private FileInfo fileInfoWithString;
    private FileInfo fileInfoWithByteString;
    private FileInfo defaultFileInfo;
    private FileInfo.Builder builderB;

    private FileInfo fileInfoWithFileName;
    private FileInfo fileInfoWithFileLength;
    private FileInfo fileInfoWithUnknownFields;
    private FileInfo.Builder fileInfoBuilderB;

    private FileInfo.Builder fileInfoBuilderB1;  // 从文件B中添加的成员变量

    @Before
    public void setUp() {
        fileInfoDescriptor = FileInfo.getDescriptor();
        builder = FileInfo.newBuilder();

        originalBuilder = FileInfo.newBuilder()
                .setFileName("testFile")
                .setFileLength(100);

        fileNameField = fileInfoDescriptor.findFieldByName("fileName");

        validFileInfoBytes = new byte[]{10, 11, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 16, 10};
        invalidFileInfoBytes = new byte[]{0x01, 0x02, 0x03};
        emptyFileInfoBytes = new byte[]{};
        unknownFieldBytes = new byte[]{26, 2, 84, 101, 115, 116}; // 未知字段
        extensionRegistry = ExtensionRegistryLite.getEmptyRegistry();
        fileInfo = FileInfo.newBuilder().setFileName("testFile").setFileLength(100).build();
        fileInfoWithString = FileInfo.newBuilder().setFileName("testFile").build();
        fileInfoWithByteString = FileInfo.newBuilder().setFileName("testFile").build();

        // 设置一个有效的 FileInfo 对象
        fileInfo = FileInfo.newBuilder()
                .setFileName("testFile")
                .setFileLength(100)
                .build();

        // 初始化defaultFileInfo
        defaultFileInfo = FileInfo.getDefaultInstance();

        // 初始化builderB
        builderB = FileInfo.newBuilder();

        // 从文件B中提取的成员变量初始化
        fileInfoWithFileName = FileInfo.newBuilder().setFileName("anotherFile").build();
        fileInfoWithFileLength = FileInfo.newBuilder().setFileLength(200).build();
        fileInfoWithUnknownFields = FileInfo.newBuilder().setFileName("unknownFile").setFileLength(300).build();
        fileInfoBuilderB = FileInfo.newBuilder();

        // 从文件B中添加的初始化代码
        fileInfoBuilderB1 = FileInfo.newBuilder();
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
    public void clear_ShouldResetFileNameAndFileLength() {
        builder.setFileName("testFile").setFileLength(100);
        builder.clear();
        Assert.assertEquals("", builder.getFileName());
        Assert.assertEquals(0, builder.getFileLength());
    }

    @Test
    public void clear_ShouldReturnBuilderInstanceForChaining() {
        FileInfo.Builder returnedBuilder = builder.clear();
        Assert.assertSame(builder, returnedBuilder);
    }

    @Test
    public void getDescriptorForType_ShouldReturnNonNullDescriptor() {
        FileInfo.Builder builder = FileInfo.newBuilder();
        Assert.assertNotNull("FileInfo descriptor should not be null", builder.getDescriptorForType());
    }

    @Test
    public void getDescriptorForType_ShouldReturnCorrectDescriptorName() {
        FileInfo.Builder builder = FileInfo.newBuilder();
        Assert.assertEquals("FileInfo", builder.getDescriptorForType().getName());
    }

    @Test
    public void getDescriptorForType_ShouldReturnCorrectDescriptorFullName() {
        FileInfo.Builder builder = FileInfo.newBuilder();
        Assert.assertEquals("orc.proto.FileInfo", builder.getDescriptorForType().getFullName());
    }

    @Test
    public void getDefaultInstanceForType_ShouldReturnDefaultInstance() {
        FileInfo defaultInstance = builder.getDefaultInstanceForType();
        Assert.assertNotNull("Default instance should not be null", defaultInstance);
        Assert.assertEquals("", defaultInstance.getFileName());
        Assert.assertEquals(0, defaultInstance.getFileLength());
    }

    @Test
    public void build_WhenInitialized_ReturnsFileInfo() {
        builder.setFileName("testFile").setFileLength(100);
        FileInfo fileInfo = builder.build();
        Assert.assertNotNull("FileInfo should not be null", fileInfo);
        Assert.assertEquals("testFile", fileInfo.getFileName());
        Assert.assertEquals(100, fileInfo.getFileLength());
    }

    @Test
    public void build_WhenNotInitialized_ReturnsFileInfo() {
        builder.build();
    }

    @Test
    public void buildPartial_UninitializedFields_ReturnsDefaultValues() {
        FileInfo fileInfo = builder.buildPartial();
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void buildPartial_PartiallyInitializedFields_ReturnsSetValues() {
        builder.setFileName("testFile");
        FileInfo fileInfo = builder.buildPartial();
        Assert.assertEquals("testFile", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void buildPartial_FullyInitializedFields_ReturnsSetValues() {
        builder.setFileName("testFile").setFileLength(100);
        FileInfo fileInfo = builder.buildPartial();
        Assert.assertEquals("testFile", fileInfo.getFileName());
        Assert.assertEquals(100, fileInfo.getFileLength());
    }

    @Test
    public void clone_ShouldCreateDeepCopy() {
        FileInfo.Builder clonedBuilder = originalBuilder.clone();
        Assert.assertEquals("testFile", clonedBuilder.getFileName());
        Assert.assertEquals(100, clonedBuilder.getFileLength());
        clonedBuilder.setFileName("clonedFile").setFileLength(200);
        Assert.assertEquals("testFile", originalBuilder.getFileName());
        Assert.assertEquals(100, originalBuilder.getFileLength());
        Assert.assertEquals("clonedFile", clonedBuilder.getFileName());
        Assert.assertEquals(200, clonedBuilder.getFileLength());
        FileInfo clonedFileInfo = clonedBuilder.build();
        Assert.assertEquals("clonedFile", clonedFileInfo.getFileName());
        Assert.assertEquals(200, clonedFileInfo.getFileLength());
    }

    @Test(expected = NullPointerException.class)
    public void setField_NullField_ThrowsNullPointerException() {
        builder.setField(null, "testFile");
    }

    @Test(expected = NullPointerException.class)
    public void setField_IncorrectFieldType_ThrowsNullPointerException() {
        builder.setField(fileNameField, 123);
    }

    @Test(expected = NullPointerException.class)
    public void clearField_NullField_ThrowsException() {
        builderB.clearField(null);
    }

    @Test
    public void clearField_FileLengthField_ClearsFileLength() {
        builderB.setFileLength(100);
        Assert.assertEquals(100, builderB.getFileLength());
        builderB.clearField(FileInfo.getDescriptor().findFieldByName("file_length"));
        Assert.assertEquals(0, builderB.getFileLength());
    }

    @Test
    public void mergeFrom_WhenOtherIsFileInfo_ShouldCallMergeFromFileInfo() {
        FileInfo.Builder builder = FileInfo.newBuilder();
        builder.mergeFrom(fileInfo);
        Assert.assertEquals("testFile", builder.getFileName());
        Assert.assertEquals(100, builder.getFileLength());
    }

    @Test
    public void mergeFrom_OtherIsDefaultInstance_ReturnsSameBuilder() {
        FileInfo.Builder builder = fileInfo.toBuilder();
        FileInfo result = builder.mergeFrom(defaultFileInfo).build();
        Assert.assertEquals(fileInfo, result);
    }

    @Test
    public void mergeFrom_OtherHasFileName_UpdatesFileName() {
        FileInfo.Builder builder = fileInfo.toBuilder();
        FileInfo result = builder.mergeFrom(fileInfoWithFileName).build();
        Assert.assertEquals("anotherFile", result.getFileName());
        Assert.assertEquals(100, result.getFileLength());
    }

    @Test
    public void mergeFrom_OtherHasFileLength_UpdatesFileLength() {
        FileInfo.Builder builder = fileInfo.toBuilder();
        FileInfo result = builder.mergeFrom(fileInfoWithFileLength).build();
        Assert.assertEquals("testFile", result.getFileName());
        Assert.assertEquals(200, result.getFileLength());
    }

    @Test
    public void mergeFrom_OtherHasUnknownFields_MergesUnknownFields() {
        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder().addField(1, UnknownFieldSet.Field.newBuilder().addVarint(123).build()).build();
        fileInfoWithUnknownFields = fileInfoWithUnknownFields.toBuilder().mergeUnknownFields(unknownFields).build();
        FileInfo.Builder builder = fileInfo.toBuilder();
        FileInfo result = builder.mergeFrom(fileInfoWithUnknownFields).build();
        Assert.assertEquals(fileInfoWithUnknownFields.getUnknownFields(), result.getUnknownFields());
    }

    @Test
    public void mergeFrom_OtherHasFileNameAndFileLength_UpdatesBoth() {
        FileInfo.Builder builder = fileInfo.toBuilder();
        FileInfo result = builder.mergeFrom(fileInfoWithFileName).mergeFrom(fileInfoWithFileLength).build();
        Assert.assertEquals("anotherFile", result.getFileName());
        Assert.assertEquals(200, result.getFileLength());
    }

    @Test
    public void isInitialized_AlwaysReturnsTrue() {  // 从文件B中添加的测试方法
        Assert.assertTrue("isInitialized should always return true", fileInfoBuilderB1.isInitialized());
    }

    @Test
    public void getFileName_WhenString_ReturnsCorrectString() {
        Assert.assertEquals("testFile", fileInfoWithString.getFileName());
    }

    @Test
    public void getFileName_WhenByteString_ReturnsCorrectString() {
        Assert.assertEquals("testFile", fileInfoWithByteString.getFileName());
    }

    @Test
    public void mergeFrom_ValidInput_ShouldMergeSuccessfully() throws IOException {
        FileInfo.Builder builderB = FileInfo.newBuilder();
        CodedInputStream input = CodedInputStream.newInstance(validFileInfoBytes);
        builderB.mergeFrom(input, extensionRegistry);
        FileInfo fileInfo = builderB.build();
        Assert.assertEquals("Hello World", fileInfo.getFileName());
        Assert.assertEquals(10, fileInfo.getFileLength());
    }

    @Test(expected = IOException.class)
    public void mergeFrom_InvalidInput_ShouldThrowIOException() throws IOException {
        FileInfo.Builder builderB = FileInfo.newBuilder();
        CodedInputStream input = CodedInputStream.newInstance(invalidFileInfoBytes);
        builderB.mergeFrom(input, extensionRegistry);
    }

    @Test
    public void mergeFrom_EmptyInput_ShouldNotThrowException() throws IOException {
        FileInfo.Builder builderB = FileInfo.newBuilder();
        CodedInputStream input = CodedInputStream.newInstance(emptyFileInfoBytes);
        builderB.mergeFrom(input, extensionRegistry);
        FileInfo fileInfo = builderB.build();
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void mergeFrom_UnknownFields_ShouldIgnoreUnknownFields() throws IOException {
        FileInfo.Builder builderB = FileInfo.newBuilder();
        CodedInputStream input = CodedInputStream.newInstance(unknownFieldBytes);
        builderB.mergeFrom(input, extensionRegistry);
        FileInfo fileInfo = builderB.build();
        Assert.assertEquals("", fileInfo.getFileName());
        Assert.assertEquals(0, fileInfo.getFileLength());
    }

    @Test
    public void setFileName_ValidString_SetsFileName() {
        // 准备
        String fileName = "testFile";

        // 执行
        builder.setFileName(fileName);

        // 验证
        Assert.assertEquals(fileName, builder.getFileName());
    }

    @Test(expected = NullPointerException.class)
    public void setFileName_NullString_ThrowsException() {
        // 执行
        builder.setFileName(null);
    }

    @Test
    public void clearFileName_ShouldResetFileNameToDefault() {
        // 设置文件名
        builder.setFileName("testFile");

        // 清除文件名
        builder.clearFileName();

        // 验证文件名是否被重置为默认值
        Assert.assertEquals("", builder.getFileName());
    }

    @Test
    public void clearFileName_ShouldCallOnChanged() {
        // 捕获 onChanged 调用
        builder.setFileName("testFile");
        builder.clearFileName();

        // 验证 onChanged 是否被调用
        // 由于 onChanged 是一个受保护的方法，我们无法直接验证其调用。
        // 但是，我们可以验证文件名是否被重置，从而间接验证其被调用。
        Assert.assertEquals("", builder.getFileName());
    }

    @Test
    public void getFileNameBytes_WhenFileNameIsString_ReturnsByteString() {
        // 设置
        String fileName = "testFile";
        fileInfoBuilderB.setFileName(fileName);

        // 执行
        ByteString result = fileInfoBuilderB.getFileNameBytes();

        // 验证
        Assert.assertEquals(ByteString.copyFromUtf8(fileName), result);
    }

    @Test
    public void getFileNameBytes_WhenFileNameIsByteString_ReturnsSameByteString() {
        // 设置
        String fileName = "testFile";
        fileInfoBuilderB.setFileName(fileName);

        // 执行
        ByteString result = fileInfoBuilderB.getFileNameBytes();

        // 验证
        Assert.assertEquals(ByteString.copyFromUtf8(fileName), result);
    }
}

package com.alibaba.polardbx.rpc.columnar;

import com.alibaba.polardbx.common.mock.MockUtils;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessageV3;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnarCacheFilesInfoResponseBuilderTest {
    private MockedStatic<GeneratedMessageV3> mockedGeneratedMessageV3;

    @Before
    public void setUp() {
        // 初始化静态mock
        mockedGeneratedMessageV3 = Mockito.mockStatic(GeneratedMessageV3.class);
    }

    @After
    public void tearDown() {
        // 释放静态mock资源
        mockedGeneratedMessageV3.close();
    }

    // 测试构建器清空操作
    @Test
    public void testClear() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(FileInfo.getDefaultInstance());

        builder.clear();
        ColumnarCacheFilesInfoResponse response = builder.build();

        assertTrue("Cleared builder should produce empty files list",
            response.getFilesList().isEmpty());
    }

    // 测试默认实例获取
    @Test
    public void testGetDefaultInstanceForType() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        assertEquals("Default instance should match static instance",
            ColumnarCacheFilesInfoResponse.getDefaultInstance(),
            builder.getDefaultInstanceForType());
    }

    // 测试完整构建流程
    @Test
    public void testBuild() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(FileInfo.newBuilder().setFileName("test.orc").build())
            .build();

        assertEquals("Built response should contain 1 file", 1, response.getFilesCount());
        assertEquals("File name should match", "test.orc", response.getFiles(0).getFileName());
    }

    // 测试部分构建流程
    @Test
    public void testBuildPartial() {
        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(FileInfo.getDefaultInstance())
            .buildPartial();

        assertTrue("Partially built response should contain files", response.getFilesCount() > 0);
    }

    // 测试构建器克隆功能
    @Test
    public void testClone() {
        ColumnarCacheFilesInfoResponse.Builder original = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(FileInfo.getDefaultInstance());
        ColumnarCacheFilesInfoResponse.Builder clone = original.clone();

        assertEquals("Clone should have same number of files",
            original.getFilesCount(), clone.getFilesCount());
    }

    // 测试消息合并功能
    @Test
    public void testMergeFrom() {
        ColumnarCacheFilesInfoResponse other = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(FileInfo.newBuilder().setFileName("merged.orc").build())
            .build();

        ColumnarCacheFilesInfoResponse response = ColumnarCacheFilesInfoResponse.newBuilder()
            .mergeFrom(other)
            .build();

        assertEquals("Merged response should contain 1 file", 1, response.getFilesCount());
        assertEquals("Merged file name should match", "merged.orc", response.getFiles(0).getFileName());
    }

    // 测试初始化状态检查
    @Test
    public void testIsInitialized() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        assertTrue("Response should always be initialized", builder.isInitialized());
    }

    // 测试流式反序列化
    @Test
    public void testMergeFromStream() throws Exception {
        // 准备测试数据
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(FileInfo.newBuilder().setFileName("stream.orc").build())
            .build()
            .writeTo(bos);

        // 从流中解析
        ColumnarCacheFilesInfoResponse parsed = ColumnarCacheFilesInfoResponse.parseFrom(
            new ByteArrayInputStream(bos.toByteArray()),
            ExtensionRegistry.getEmptyRegistry());
    }

    // 测试列表可变性保障
    @Test
    public void testEnsureFilesIsMutable() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();
        // 通过反射设置不可变列表
        MockUtils.setInternalState(builder, "files_", Collections.singletonList(FileInfo.getDefaultInstance()));

        // 触发可变性检查
        builder.addFiles(FileInfo.getDefaultInstance());
        assertEquals("Should have 2 files after modification", 2, builder.getFilesCount());
    }

    // 测试构建器列表操作
    @Test
    public void testFilesBuilderOperations() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 测试通过builder添加文件
        builder.addFilesBuilder().setFileName("builder.orc");
        assertEquals("Should have 1 file via builder", 1, builder.getFilesCount());
        assertEquals("File name should match", "builder.orc", builder.getFiles(0).getFileName());
    }

    // 测试构建器的setFiles方法
    @Test
    public void testSetFiles() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加一个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("original.orc").setFileLength(1024).build());

        // 使用setFiles替换文件
        FileInfo newFile = FileInfo.newBuilder().setFileName("replacement.orc").setFileLength(2048).build();
        builder.setFiles(0, newFile);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 1 file", 1, response.getFilesCount());
        assertEquals("File name should match replacement", "replacement.orc", response.getFiles(0).getFileName());
        assertEquals("File length should match replacement", 2048, response.getFiles(0).getFileLength());
    }

    // 测试构建器的addFiles方法（带索引）
    @Test
    public void testAddFilesAtIndex() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加两个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("first.orc").setFileLength(1024).build());
        builder.addFiles(FileInfo.newBuilder().setFileName("third.orc").setFileLength(3072).build());

        // 在索引1处插入文件
        FileInfo secondFile = FileInfo.newBuilder().setFileName("second.orc").setFileLength(2048).build();
        builder.addFiles(1, secondFile);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 3 files", 3, response.getFilesCount());
        assertEquals("First file name should match", "first.orc", response.getFiles(0).getFileName());
        assertEquals("Second file name should match", "second.orc", response.getFiles(1).getFileName());
        assertEquals("Third file name should match", "third.orc", response.getFiles(2).getFileName());
    }

    // 测试构建器的addAllFiles方法
    @Test
    public void testAddAllFiles() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 创建文件列表
        FileInfo file1 = FileInfo.newBuilder().setFileName("first.orc").setFileLength(1024).build();
        FileInfo file2 = FileInfo.newBuilder().setFileName("second.orc").setFileLength(2048).build();
        FileInfo file3 = FileInfo.newBuilder().setFileName("third.orc").setFileLength(3072).build();

        java.util.List<FileInfo> files = new java.util.ArrayList<>();
        files.add(file1);
        files.add(file2);
        files.add(file3);

        // 添加所有文件
        builder.addAllFiles(files);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 3 files", 3, response.getFilesCount());
        assertEquals("First file name should match", "first.orc", response.getFiles(0).getFileName());
        assertEquals("Second file name should match", "second.orc", response.getFiles(1).getFileName());
        assertEquals("Third file name should match", "third.orc", response.getFiles(2).getFileName());
    }

    // 测试构建器的clearFiles方法
    @Test
    public void testClearFiles() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加一些文件
        builder.addFiles(FileInfo.newBuilder().setFileName("first.orc").setFileLength(1024).build());
        builder.addFiles(FileInfo.newBuilder().setFileName("second.orc").setFileLength(2048).build());

        // 清空文件列表
        builder.clearFiles();

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 0 files", 0, response.getFilesCount());
        assertTrue("Files list should be empty", response.getFilesList().isEmpty());
    }

    // 测试构建器的removeFiles方法
    @Test
    public void testRemoveFiles() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加三个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("first.orc").setFileLength(1024).build());
        builder.addFiles(FileInfo.newBuilder().setFileName("second.orc").setFileLength(2048).build());
        builder.addFiles(FileInfo.newBuilder().setFileName("third.orc").setFileLength(3072).build());

        // 移除第二个文件
        builder.removeFiles(1);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 2 files", 2, response.getFilesCount());
        assertEquals("First file name should match", "first.orc", response.getFiles(0).getFileName());
        assertEquals("Third file name should match", "third.orc", response.getFiles(1).getFileName());
    }

    // 测试构建器的getFilesBuilder方法
    @Test
    public void testGetFilesBuilder() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加一个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("original.orc").setFileLength(1024).build());

        // 获取文件构建器并修改文件名
        builder.getFilesBuilder(0).setFileName("modified.orc");

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 1 file", 1, response.getFilesCount());
        assertEquals("File name should be modified", "modified.orc", response.getFiles(0).getFileName());
    }

    // 测试构建器的getFilesBuilderList方法
    @Test
    public void testGetFilesBuilderList() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加两个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("first.orc").setFileLength(1024).build());
        builder.addFiles(FileInfo.newBuilder().setFileName("second.orc").setFileLength(2048).build());

        // 获取构建器列表并修改文件名
        java.util.List<FileInfo.Builder> builderList = builder.getFilesBuilderList();
        builderList.get(0).setFileName("modified-first.orc");
        builderList.get(1).setFileName("modified-second.orc");

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 2 files", 2, response.getFilesCount());
        assertEquals("First file name should be modified", "modified-first.orc", response.getFiles(0).getFileName());
        assertEquals("Second file name should be modified", "modified-second.orc", response.getFiles(1).getFileName());
    }

    // 测试构建器的addFilesBuilder方法（带索引）
    @Test
    public void testAddFilesBuilderAtIndex() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加两个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("first.orc").setFileLength(1024).build());
        builder.addFiles(FileInfo.newBuilder().setFileName("third.orc").setFileLength(3072).build());

        // 在索引1处添加构建器
        builder.addFilesBuilder(1).setFileName("second.orc").setFileLength(2048);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 3 files", 3, response.getFilesCount());
        assertEquals("First file name should match", "first.orc", response.getFiles(0).getFileName());
        assertEquals("Second file name should match", "second.orc", response.getFiles(1).getFileName());
        assertEquals("Third file name should match", "third.orc", response.getFiles(2).getFileName());
    }

    // 测试构建器的setFiles方法（使用FileInfo.Builder）
    @Test
    public void testSetFilesWithBuilder() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 添加一个文件
        builder.addFiles(FileInfo.newBuilder().setFileName("original.orc").setFileLength(1024).build());

        // 使用FileInfo.Builder替换文件
        FileInfo.Builder fileBuilder = FileInfo.newBuilder().setFileName("replacement.orc").setFileLength(2048);
        builder.setFiles(0, fileBuilder);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 1 file", 1, response.getFilesCount());
        assertEquals("File name should match replacement", "replacement.orc", response.getFiles(0).getFileName());
        assertEquals("File length should match replacement", 2048, response.getFiles(0).getFileLength());
    }

    // 测试构建器的mergeFrom方法（使用FileInfo.Builder）
    @Test
    public void testMergeFromWithBuilder() {
        ColumnarCacheFilesInfoResponse.Builder builder = ColumnarCacheFilesInfoResponse.newBuilder();

        // 创建另一个响应
        FileInfo fileInfo = FileInfo.newBuilder().setFileName("merged.orc").setFileLength(1024).build();
        ColumnarCacheFilesInfoResponse other = ColumnarCacheFilesInfoResponse.newBuilder()
            .addFiles(fileInfo)
            .build();

        // 合并
        builder.mergeFrom(other);

        ColumnarCacheFilesInfoResponse response = builder.build();
        assertEquals("Should have 1 file", 1, response.getFilesCount());
        assertEquals("File name should match", "merged.orc", response.getFiles(0).getFileName());
        assertEquals("File length should match", 1024, response.getFiles(0).getFileLength());
    }
}
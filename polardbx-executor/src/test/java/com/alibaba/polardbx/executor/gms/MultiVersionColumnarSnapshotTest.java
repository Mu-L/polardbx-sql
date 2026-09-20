package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.common.mock.MockUtils.getInternalState;
import static com.alibaba.polardbx.common.mock.MockUtils.setInternalState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class MultiVersionColumnarSnapshotTest {

    private MultiVersionColumnarSnapshot multiVersionColumnarSnapshot;

    @Mock
    private DynamicColumnarManager mockColumnarManager;

    @Mock
    private Connection mockConnection;

    @Mock
    private FilesAccessor mockFilesAccessor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Mock the DynamicColumnarManager to return a fixed minTso
        when(mockColumnarManager.getMinTso()).thenReturn(100L);
        when(mockColumnarManager.getMinCompactionTso(anyLong())).thenReturn(0L);
        // Mock putPurgedFile to do nothing
        doAnswer(invocation -> null).when(mockColumnarManager).putPurgedFile(anyString());

        multiVersionColumnarSnapshot = new MultiVersionColumnarSnapshot(mockColumnarManager, "testSchema", 1L);

        // 预先设置latestLoadedTso，避免调用loadUntilTso方法
        setInternalState(multiVersionColumnarSnapshot, "latestLoadedTso", 300L);
    }

    @Test
    public void testGenerateSnapshotWithValidParameters() {
        // Prepare test data
        String partitionName = "partition1";
        long lowerTso = 150L;
        long tso = 200L;

        // Mock the file system utils to return specific file types
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType(anyString())).thenReturn(ColumnarFileType.ORC);

            // Generate snapshot
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // Verify results
            assertNotNull(snapshot);
            assertNotNull(snapshot.getOrcFiles());
            assertNotNull(snapshot.getCsvFiles());
            assertNotNull(snapshot.getDelFiles());
        }
    }

    @Test
    public void testLoadNewTso() {
        String partitionName = "partition1";
        long lowerTso = 150L;
        long newTso = 400L;

        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class);
            MockedConstruction<FilesAccessor> ignored = mockConstruction(FilesAccessor.class,
                (mock, context) -> {
                    when(mock.queryColumnarDeltaFilesByTsoAndTableId(anyLong(), anyLong(), anyString(), anyString()))
                        .thenReturn(Collections.emptyList());
                })) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType(anyString())).thenReturn(ColumnarFileType.ORC);

            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            // Generate snapshot
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, newTso);

            assertNotNull(snapshot);
            assertNotNull(snapshot.getOrcFiles());
            assertNotNull(snapshot.getCsvFiles());
            assertNotNull(snapshot.getDelFiles());
        }
    }

    @Test
    public void testGenerateSnapshotWithDifferentFileTypes() {
        // Prepare test data
        String partitionName = "partition1";
        long lowerTso = 150L;
        long tso = 200L;

        // Mock the file system utils to return different file types
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            // Mock file type returns
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file1.orc")).thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file2.csv")).thenReturn(ColumnarFileType.CSV);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file3.del")).thenReturn(ColumnarFileType.DEL);

            // Generate snapshot
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // Verify results
            assertNotNull(snapshot);
        }
    }

    @Test
    public void testGenerateSnapshotWithRemovedFiles() {
        // Prepare test data
        String partitionName = "partition1";
        long lowerTso = 150L;
        long tso = 200L;

        // Mock the file system utils to return specific file types
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType(anyString())).thenReturn(ColumnarFileType.ORC);

            // Generate snapshot
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // Verify results
            assertNotNull(snapshot);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testGenerateSnapshotWithPurgedTso() {
        // Prepare test data with tso less than minTso
        String partitionName = "partition1";
        long lowerTso = 50L;
        long tso = 80L; // Less than minTso (100L)

        // Mock columnar manager to return higher minTso
        when(mockColumnarManager.getMinTso()).thenReturn(100L);

        // This should throw TddlRuntimeException
        multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);
    }

    @Test
    public void testGenerateSnapshotWithEmptyPartition() {
        // Prepare test data with empty partition
        String partitionName = "emptyPartition";
        long lowerTso = 150L;
        long tso = 200L;

        // Generate snapshot for empty partition
        MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
            multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

        // Verify results
        assertNotNull(snapshot);
        assertEquals(0, snapshot.getOrcFiles().size());
        assertEquals(0, snapshot.getCsvFiles().size());
        assertEquals(0, snapshot.getDelFiles().size());
    }

    @Test
    public void testGenerateSnapshotWithBoundaryConditions() {
        // Prepare test data with boundary conditions
        String partitionName = "partition1";
        long lowerTso = 150L;
        long tso = 200L;

        // Mock the file system utils to return specific file types
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType(anyString())).thenReturn(ColumnarFileType.ORC);

            // Generate snapshot
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // Verify results
            assertNotNull(snapshot);
        }
    }

    @Test
    public void testGenerateSnapshotWithFileFilteringLogic() {
        // 准备测试数据
        String partitionName = "partitionName";
        long lowerTso = 150L;
        long tso = 200L;

        // 创建测试文件和时间戳信息
        MultiVersionColumnarSnapshot.ColumnarTsoInfo file1TsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(160L, null, 100L); // commitTs在范围内，无removeTs
        MultiVersionColumnarSnapshot.ColumnarTsoInfo file2TsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(140L, null, 100L); // commitTs小于lowerTso，无removeTs
        MultiVersionColumnarSnapshot.ColumnarTsoInfo file3TsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(170L, 180L, 100L); // commitTs在范围内，但removeTs也在范围内
        MultiVersionColumnarSnapshot.ColumnarTsoInfo file4TsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(180L, 210L, 100L); // commitTs在范围内，removeTs大于tso

        // 直接操作allPartsTsoInfo来设置测试数据
        Map<String, Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo>> allPartsTsoInfo =
            (Map<String, Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo>>)
                getInternalState(multiVersionColumnarSnapshot, "allPartsTsoInfo");

        Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo> partitionFiles = new ConcurrentHashMap<>();
        partitionFiles.put("file1.orc", file1TsoInfo);
        partitionFiles.put("file2.orc", file2TsoInfo);
        partitionFiles.put("file3.orc", file3TsoInfo);
        partitionFiles.put("file4.orc", file4TsoInfo);
        allPartsTsoInfo.put(partitionName, partitionFiles);

        // Mock文件系统工具返回特定的文件类型
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file1.orc")).thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file2.orc")).thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file3.orc")).thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file4.orc")).thenReturn(ColumnarFileType.ORC);

            // 生成快照
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // 验证结果
            assertNotNull(snapshot);
            // 应该有两个文件被包含：
            // file1.orc: commitTs(160) >= lowerTso(150) 且 <= tso(200)，且无removeTs，应该被包含
            // file4.orc: commitTs(180) >= lowerTso(150) 且 <= tso(200)，且removeTs(210) > tso(200)，应该被包含
            assertEquals(2, snapshot.getOrcFiles().size());
            // 检查包含的文件
            assertTrue(snapshot.getOrcFiles().contains("file1.orc"));
            assertTrue(snapshot.getOrcFiles().contains("file4.orc"));
        }
    }

    @Test
    public void testGenerateSnapshotWithDifferentFileTypesFiltering() {
        // 准备测试数据
        String partitionName = "partition1";
        long lowerTso = 150L;
        long tso = 200L;

        // 创建测试文件和时间戳信息
        MultiVersionColumnarSnapshot.ColumnarTsoInfo orcFileTsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(160L, null, 100L); // ORC文件
        MultiVersionColumnarSnapshot.ColumnarTsoInfo csvFileTsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(160L, null, 100L); // CSV文件
        MultiVersionColumnarSnapshot.ColumnarTsoInfo delFileTsoInfo =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(160L, null, 100L); // DEL文件

        // 直接操作allPartsTsoInfo来设置测试数据
        Map<String, Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo>> allPartsTsoInfo =
            (Map<String, Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo>>)
                getInternalState(multiVersionColumnarSnapshot, "allPartsTsoInfo");

        Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo> partitionFiles = new ConcurrentHashMap<>();
        partitionFiles.put("file1.orc", orcFileTsoInfo);
        partitionFiles.put("file2.csv", csvFileTsoInfo);
        partitionFiles.put("file3.del", delFileTsoInfo);
        allPartsTsoInfo.put(partitionName, partitionFiles);

        // Mock文件系统工具返回特定的文件类型
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file1.orc")).thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file2.csv")).thenReturn(ColumnarFileType.CSV);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("file3.del")).thenReturn(ColumnarFileType.DEL);

            // 生成快照
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // 验证结果
            assertNotNull(snapshot);
            assertEquals(1, snapshot.getOrcFiles().size());
            assertEquals("file1.orc", snapshot.getOrcFiles().get(0));
            assertEquals(1, snapshot.getCsvFiles().size());
            assertEquals("file2.csv", snapshot.getCsvFiles().get(0));
            assertEquals(1, snapshot.getDelFiles().size());
            assertEquals("file3.del", snapshot.getDelFiles().get(0));
        }
    }

    @Test
    public void testGenerateSnapshotWithComplexFileFiltering() {
        // 准备测试数据，覆盖更多边界情况
        String partitionName = "partition1";
        long lowerTso = 150L;
        long tso = 200L;

        // 创建多种情况的测试文件和时间戳信息
        MultiVersionColumnarSnapshot.ColumnarTsoInfo validOrcFile =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(160L, null, 100L); // 有效的ORC文件
        MultiVersionColumnarSnapshot.ColumnarTsoInfo oldOrcFile =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(140L, null, 100L); // 太旧的ORC文件
        MultiVersionColumnarSnapshot.ColumnarTsoInfo removedOrcFile =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(170L, 180L, 100L); // 已被删除的ORC文件
        MultiVersionColumnarSnapshot.ColumnarTsoInfo validCsvFile =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(180L, null, 100L); // 有效的CSV文件
        MultiVersionColumnarSnapshot.ColumnarTsoInfo validDelFile =
            new MultiVersionColumnarSnapshot.ColumnarTsoInfo(190L, 210L, 100L); // 有效的DEL文件（removeTs > tso）

        // 直接操作allPartsTsoInfo来设置测试数据
        Map<String, Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo>> allPartsTsoInfo =
            (Map<String, Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo>>)
                getInternalState(multiVersionColumnarSnapshot, "allPartsTsoInfo");

        Map<String, MultiVersionColumnarSnapshot.ColumnarTsoInfo> partitionFiles = new ConcurrentHashMap<>();
        partitionFiles.put("validFile.orc", validOrcFile);
        partitionFiles.put("oldFile.orc", oldOrcFile);
        partitionFiles.put("removedFile.orc", removedOrcFile);
        partitionFiles.put("validFile.csv", validCsvFile);
        partitionFiles.put("validFile.del", validDelFile);
        allPartsTsoInfo.put(partitionName, partitionFiles);

        // Mock文件系统工具返回特定的文件类型
        try (MockedStatic<FileSystemUtils> fileSystemUtilsMock = mockStatic(FileSystemUtils.class)) {
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("validFile.orc"))
                .thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("oldFile.orc")).thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("removedFile.orc"))
                .thenReturn(ColumnarFileType.ORC);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("validFile.csv"))
                .thenReturn(ColumnarFileType.CSV);
            fileSystemUtilsMock.when(() -> FileSystemUtils.getFileType("validFile.del"))
                .thenReturn(ColumnarFileType.DEL);

            // 生成快照
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot =
                multiVersionColumnarSnapshot.generateSnapshot(partitionName, lowerTso, tso);

            // 验证结果
            assertNotNull(snapshot);
            // 验证ORC文件：只有validFile.orc应该被包含（commitTs >= lowerTso且无removeTs）
            assertEquals(1, snapshot.getOrcFiles().size());
            assertEquals("validFile.orc", snapshot.getOrcFiles().get(0));

            // 验证CSV文件：validFile.csv应该被包含（commitTs <= tso且无removeTs）
            assertEquals(1, snapshot.getCsvFiles().size());
            assertEquals("validFile.csv", snapshot.getCsvFiles().get(0));

            // 验证DEL文件：validFile.del应该被包含（commitTs <= tso且removeTs > tso）
            assertEquals(1, snapshot.getDelFiles().size());
            assertEquals("validFile.del", snapshot.getDelFiles().get(0));
        }
    }
}
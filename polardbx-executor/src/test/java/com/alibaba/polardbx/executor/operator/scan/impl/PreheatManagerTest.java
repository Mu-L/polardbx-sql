package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.orc.ORCMetaReaderImpl;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.discover.ClusterNodeManager;
import com.alibaba.polardbx.common.orc.ORCMetaReader;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.MppScope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PreheatManagerTest {

    private static final long PREHEAT_FILE_META_SIZE = 1L << 20; // 1MB

    @Test
    public void testPreheatManager() throws Throwable {

        try (MockedStatic<ServiceProvider> mockedServiceProvider = Mockito.mockStatic(ServiceProvider.class);
            MockedStatic<ORCMetaReader> mockedOrcMetaReader = Mockito.mockStatic(ORCMetaReader.class);
            MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class)
        ) {

            // mock columnar mode.
            mockedConfigDataMode.when(() -> ConfigDataMode.isColumnarMode()).thenReturn(true);

            ServiceProvider serviceProvider = Mockito.mock(ServiceProvider.class);
            mockedServiceProvider.when(ServiceProvider::getInstance).thenReturn(serviceProvider);

            Server server = Mockito.mock(MppServer.class);
            Mockito.when(serviceProvider.getServer()).thenReturn(server);

            InternalNodeManager nodeManager = Mockito.mock(ClusterNodeManager.class);
            Mockito.when(server.getNodeManager()).thenReturn(nodeManager);

            AllNodes allNodes = Mockito.mock(AllNodes.class);
            Mockito.when(nodeManager.getAllNodes()).thenReturn(allNodes);

            // mock 3 node with different ip host and port.
            InternalNode node1 = Mockito.mock(InternalNode.class);
            Mockito.when(node1.getHostPort()).thenReturn("11.196.49.49:3029");
            InternalNode node2 = Mockito.mock(InternalNode.class);
            Mockito.when(node2.getHostPort()).thenReturn("11.196.60.168:3127");
            InternalNode node3 = Mockito.mock(InternalNode.class);
            Mockito.when(node3.getHostPort()).thenReturn("11.196.60.95:3036");
            List<InternalNode> allWorkers = new ArrayList<>();
            allWorkers.add(node1);
            allWorkers.add(node2);
            allWorkers.add(node3);
            Mockito.when(allNodes.getAllWorkers(MppScope.ALL)).thenReturn(allWorkers);

            // current node is node2.
            Mockito.when(nodeManager.getCurrentNode()).thenReturn(node2);

            ORCMetaReader orcMetaReader = Mockito.mock(ORCMetaReaderImpl.class);

            mockedOrcMetaReader.when(
                () -> ORCMetaReader.create(
                    Mockito.any(Configuration.class),
                    Mockito.any(FileSystem.class),
                    Mockito.nullable(Set.class)  // allow nullable
                )
            ).thenReturn(orcMetaReader);

            // the same preheat file meta.
            PreheatFileMeta preheatFileMeta = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta.getMemorySize()).thenReturn(PREHEAT_FILE_META_SIZE);
            Mockito.when(orcMetaReader.preheat(Mockito.any(Path.class))).thenReturn(preheatFileMeta);

            PreheatMetaManager preheatMetaManager = PreheatMetaManager.getInstance();

            final long entries =
                DynamicConfig.getInstance().getPreheatedCacheMaxMemorySize() / PREHEAT_FILE_META_SIZE * 2;
            for (int i = 0; i < entries; i++) {
                Path newPath = Mockito.mock(Path.class);
                preheatMetaManager.get(newPath, Mockito.mock(FileSystem.class));
            }

            System.out.println("hitCount: " + preheatMetaManager.hitCount());
            System.out.println("missCount: " + preheatMetaManager.missCount());
            System.out.println("entries: " + preheatMetaManager.entries());
            System.out.println("memorySize: " + preheatMetaManager.memorySize());
            System.out.println("exceed: " + preheatMetaManager.exceedCount());

        }
    }

    @Test
    public void testFetchTask() throws Throwable {
//
//        try (MockedStatic<ServiceProvider> mockedServiceProvider = Mockito.mockStatic(ServiceProvider.class);
//            MockedStatic<ORCMetaReader> mockedOrcMetaReader = Mockito.mockStatic(ORCMetaReader.class);
//            MockedStatic<ColumnarManager> mockedColumnarManager = Mockito.mockStatic(ColumnarManager.class);
//            MockedStatic<MetaDbUtil> mockedMetaDBUtil = Mockito.mockStatic(MetaDbUtil.class);
//            MockedStatic<FilesAccessor> mockedFilesAccessor = Mockito.mockStatic(FilesAccessor.class);
//            MockedStatic<FileSystemManager> mockedFileSystemManager = Mockito.mockStatic(FileSystemManager.class);
//            MockedStatic<FileSystemUtils> mockedFileSystemUtils = Mockito.mockStatic(FileSystemUtils.class)
//        ) {
//            ServiceProvider serviceProvider = Mockito.mock(ServiceProvider.class);
//            mockedServiceProvider.when(ServiceProvider::getInstance).thenReturn(serviceProvider);
//
//            Server server = Mockito.mock(MppServer.class);
//            Mockito.when(serviceProvider.getServer()).thenReturn(server);
//
//            InternalNodeManager nodeManager = Mockito.mock(ClusterNodeManager.class);
//            Mockito.when(server.getNodeManager()).thenReturn(nodeManager);
//
//            AllNodes allNodes = Mockito.mock(AllNodes.class);
//            Mockito.when(nodeManager.getAllNodes()).thenReturn(allNodes);
//
//            // mock 3 node with different ip host and port.
//            InternalNode node1 = Mockito.mock(InternalNode.class);
//            Mockito.when(node1.getHostPort()).thenReturn("11.196.49.49:3029");
//            InternalNode node2 = Mockito.mock(InternalNode.class);
//            Mockito.when(node2.getHostPort()).thenReturn("11.196.60.168:3127");
//            InternalNode node3 = Mockito.mock(InternalNode.class);
//            Mockito.when(node3.getHostPort()).thenReturn("11.196.60.95:3036");
//            List<InternalNode> allWorkers = new ArrayList<>();
//            allWorkers.add(node1);
//            allWorkers.add(node2);
//            allWorkers.add(node3);
//            Mockito.when(allNodes.getAllWorkers(MppScope.ALL)).thenReturn(allWorkers);
//
//            // current node is node2.
//            Mockito.when(nodeManager.getCurrentNode()).thenReturn(node2);
//
//            ORCMetaReader orcMetaReader = Mockito.mock(ORCMetaReaderImpl.class);
//
//            mockedOrcMetaReader.when(
//                    () -> ORCMetaReader.create(Mockito.any(Configuration.class), Mockito.any(FileSystem.class)))
//                .thenReturn(orcMetaReader);
//
//            // the same preheat file meta.
//            PreheatFileMeta preheatFileMeta = Mockito.mock(PreheatFileMeta.class);
//            Mockito.when(preheatFileMeta.getMemorySize()).thenReturn(PREHEAT_FILE_META_SIZE);
//            Mockito.when(orcMetaReader.preheat(Mockito.any(Path.class))).thenReturn(preheatFileMeta);
//
//            PreheatMetaManagerImpl.FetchMetaTask fetchMetaTask = new PreheatMetaManagerImpl.FetchMetaTask();
//
//            ColumnarManager columnarManager = Mockito.mock(ColumnarManager.class);
//            mockedColumnarManager.when(() -> ColumnarManager.getInstance()).thenReturn(columnarManager);
//            Mockito.when(columnarManager.latestTso()).thenReturn(System.currentTimeMillis());
//
//            Connection connection = Mockito.mock(Connection.class);
//            mockedMetaDBUtil.when(() -> MetaDbUtil.getConnection()).thenReturn(connection);
//
//            FilesAccessor filesAccessor = Mockito.mock(FilesAccessor.class);
//            mockedFilesAccessor.when(() -> FilesAccessor.create()).thenReturn(filesAccessor);
//
//            List<FilesRecordSimplifiedV2> mockFilesRecords = new ArrayList<>();
//            for (int i = 0; i < 1000; i++) {
//                mockFilesRecords.add(
//                    new FilesRecordSimplifiedV2(i + ".orc", "p1", "OSS"));
//            }
//
//            Mockito.when(filesAccessor.lookingForNewFiles(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong(),
//                    Mockito.anyLong()))
//                .thenReturn(mockFilesRecords);
//
//            FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
//            mockedFileSystemManager.when(() -> FileSystemManager.getFileSystemGroup(Engine.OSS))
//                .thenReturn(fileSystemGroup);
//            Mockito.when(fileSystemGroup.getMaster()).thenReturn(Mockito.mock(FileSystem.class));
//
//            mockedFileSystemUtils.when(
//                    () -> FileSystemUtils.buildPath(Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
//                .thenAnswer(any -> Mockito.mock(Path.class));
//
//            fetchMetaTask.run();
//
//            PreheatMetaManager preheatMetaManager = PreheatMetaManager.getInstance();
//            System.out.println("hitCount: " + preheatMetaManager.hitCount());
//            System.out.println("missCount: " + preheatMetaManager.missCount());
//            System.out.println("entries: " + preheatMetaManager.entries());
//            System.out.println("memorySize: " + preheatMetaManager.memorySize());
//            System.out.println("exceed: " + preheatMetaManager.exceedCount());
//        }
    }
}

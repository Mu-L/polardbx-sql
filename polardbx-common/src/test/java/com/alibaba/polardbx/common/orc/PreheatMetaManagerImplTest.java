package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import io.airlift.slice.SizeOf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.Set;

public class PreheatMetaManagerImplTest {

    private static final long PREHEAT_META_MEMORY_SIZE = 1024 * 1024L;
    private static final long PATH_MEMORY_SIZE =
        ClassLayout.parseClass(String.class).instanceSize() + VMSupport.align(
            (int) SizeOf.sizeOfCharArray("path1".length()));
    private static final long INIT_MAX_MEMORY_SIZE = (PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 4;

    @Test
    public void test() {

        try (MockedStatic<ORCMetaReader> orcMetaReaderMockedStatic = Mockito.mockStatic(ORCMetaReader.class);
            MockedStatic<DynamicConfig> dynamicConfigMockedStatic = Mockito.mockStatic(DynamicConfig.class)) {

            DynamicConfig dynamicConfig = Mockito.mock(DynamicConfig.class);
            dynamicConfigMockedStatic.when(() -> DynamicConfig.getInstance()).thenReturn(dynamicConfig);
            Mockito.when(dynamicConfig.getPreheatedCacheMaxMemorySize()).thenReturn(INIT_MAX_MEMORY_SIZE);

            PreheatMetaManager preheatMetaManager = PreheatMetaManager.getInstance();
            FileSystem fileSystem = Mockito.mock(FileSystem.class);

            ORCMetaReader orcMetaReader = Mockito.mock(ORCMetaReader.class);

            orcMetaReaderMockedStatic.when(
                () -> ORCMetaReader.create(
                    Mockito.any(Configuration.class),
                    Mockito.any(FileSystem.class),
                    Mockito.nullable(Set.class)  // allow nullable
                )
            ).thenReturn(orcMetaReader);

            // path * 6
            Path path1 = Mockito.mock(Path.class);
            Mockito.when(path1.toString()).thenReturn("path1");
            Path path2 = Mockito.mock(Path.class);
            Mockito.when(path2.toString()).thenReturn("path2");
            Path path3 = Mockito.mock(Path.class);
            Mockito.when(path3.toString()).thenReturn("path3");
            Path path4 = Mockito.mock(Path.class);
            Mockito.when(path4.toString()).thenReturn("path4");
            Path path5 = Mockito.mock(Path.class);
            Mockito.when(path5.toString()).thenReturn("path5");
            Path path6 = Mockito.mock(Path.class);
            Mockito.when(path6.toString()).thenReturn("path6");

            // PreheatFileMeta * 6 with 1MB size.
            PreheatFileMeta preheatFileMeta1 = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta1.getMemorySize()).thenReturn(PREHEAT_META_MEMORY_SIZE);
            PreheatFileMeta preheatFileMeta2 = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta2.getMemorySize()).thenReturn(PREHEAT_META_MEMORY_SIZE);
            PreheatFileMeta preheatFileMeta3 = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta3.getMemorySize()).thenReturn(PREHEAT_META_MEMORY_SIZE);
            PreheatFileMeta preheatFileMeta4 = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta4.getMemorySize()).thenReturn(PREHEAT_META_MEMORY_SIZE);
            PreheatFileMeta preheatFileMeta5 = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta5.getMemorySize()).thenReturn(PREHEAT_META_MEMORY_SIZE);
            PreheatFileMeta preheatFileMeta6 = Mockito.mock(PreheatFileMeta.class);
            Mockito.when(preheatFileMeta6.getMemorySize()).thenReturn(PREHEAT_META_MEMORY_SIZE);

            // preheat
            Mockito.when(orcMetaReader.preheat(path1)).thenReturn(preheatFileMeta1);
            Mockito.when(orcMetaReader.preheat(path2)).thenReturn(preheatFileMeta2);
            Mockito.when(orcMetaReader.preheat(path3)).thenReturn(preheatFileMeta3);
            Mockito.when(orcMetaReader.preheat(path4)).thenReturn(preheatFileMeta4);
            Mockito.when(orcMetaReader.preheat(path5)).thenReturn(preheatFileMeta5);
            Mockito.when(orcMetaReader.preheat(path6)).thenReturn(preheatFileMeta6);

            PreheatFileMeta fileMeta;
            fileMeta = preheatMetaManager.get(path1, fileSystem);
            Assert.assertTrue(fileMeta == preheatFileMeta1);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE), preheatMetaManager.memorySize());
            Assert.assertEquals(1, preheatMetaManager.entries()); // cached

            fileMeta = preheatMetaManager.get(path2, fileSystem);
            Assert.assertTrue(fileMeta == preheatFileMeta2);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 2, preheatMetaManager.memorySize());
            Assert.assertEquals(2, preheatMetaManager.entries()); // cached

            fileMeta = preheatMetaManager.get(path3, fileSystem);
            Assert.assertTrue(fileMeta == preheatFileMeta3);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 3, preheatMetaManager.memorySize());
            Assert.assertEquals(3, preheatMetaManager.entries()); // cached

            fileMeta = preheatMetaManager.get(path4, fileSystem);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 4, preheatMetaManager.memorySize());
            Assert.assertTrue(fileMeta == preheatFileMeta4);
            Assert.assertEquals(4, preheatMetaManager.entries()); // cached

            fileMeta = preheatMetaManager.get(path5, fileSystem);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 4, preheatMetaManager.memorySize());
            Assert.assertTrue(fileMeta == preheatFileMeta5);
            Assert.assertEquals(4, preheatMetaManager.entries()); // evicted.

            // resize to 5 files.
            preheatMetaManager.resizeMaximumMemorySize((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 5);

            fileMeta = preheatMetaManager.get(path1, fileSystem);
            Assert.assertTrue(fileMeta == preheatFileMeta1);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 5, preheatMetaManager.memorySize());
            Assert.assertEquals(5, preheatMetaManager.entries()); // cached

            // resize to 3 files.
            preheatMetaManager.resizeMaximumMemorySize((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 3);
            Assert.assertEquals((PREHEAT_META_MEMORY_SIZE + PATH_MEMORY_SIZE) * 3, preheatMetaManager.memorySize());
            Assert.assertEquals(3, preheatMetaManager.entries()); // cached

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
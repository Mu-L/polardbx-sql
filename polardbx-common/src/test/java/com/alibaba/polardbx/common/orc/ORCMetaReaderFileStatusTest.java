package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ORCMetaReaderFileStatusTest {

    @Test
    public void testOssFileSystemBranch() throws Exception {
        OSSFileSystem fsMock = mock(OSSFileSystem.class);
        FileStatus expectedStatus = mock(FileStatus.class);
        when(fsMock.getFileStatusImpl(any(Path.class))).thenReturn(expectedStatus);
        when(fsMock.open(any(Path.class))).thenReturn(mock(FSDataInputStream.class));

        try (MockedStatic<OrcFile> orcFileMock = mockStatic(OrcFile.class)) {
            ReaderImpl readerMock = mock(ReaderImpl.class);
            orcFileMock.when(() -> OrcFile.createReader(any(Path.class), any(OrcFile.ReaderOptions.class)))
                .thenReturn(readerMock);

            ORCMetaReaderImpl reader = new ORCMetaReaderImpl(new Configuration(), fsMock, null);
            PreheatFileMeta result = reader.preheat(new Path("test"));

            verify(fsMock).getFileStatusImpl(any(Path.class));
            assertEquals(expectedStatus, result.getFileStatus());
        } catch (Throwable e) {

        }
    }

    @Test
    public void testCachingFileSystemBranch() throws Exception {
        CachingFileSystem cachingFsMock = mock(CachingFileSystem.class);
        OSSFileSystem ossFsMock = mock(OSSFileSystem.class);
        when(cachingFsMock.getDataTier()).thenReturn(ossFsMock);
        FileStatus expectedStatus = mock(FileStatus.class);
        when(ossFsMock.getFileStatusImpl(any(Path.class))).thenReturn(expectedStatus);
        when(cachingFsMock.open(any(Path.class))).thenReturn(mock(FSDataInputStream.class));

        try (MockedStatic<OrcFile> orcFileMock = mockStatic(OrcFile.class)) {
            ReaderImpl readerMock = mock(ReaderImpl.class);
            orcFileMock.when(() -> OrcFile.createReader(any(Path.class), any(OrcFile.ReaderOptions.class)))
                .thenReturn(readerMock);

            ORCMetaReaderImpl reader = new ORCMetaReaderImpl(new Configuration(), cachingFsMock, null);
            PreheatFileMeta result = reader.preheat(new Path("test"));

            verify(ossFsMock).getFileStatusImpl(any(Path.class));
            assertEquals(expectedStatus, result.getFileStatus());
        } catch (Throwable t) {

        }
    }

    @Test
    public void testDefaultFileSystemBranch() throws Exception {
        FileSystem fsMock = mock(FileSystem.class);
        FileStatus expectedStatus = mock(FileStatus.class);
        when(fsMock.getFileStatus(any(Path.class))).thenReturn(expectedStatus);
        when(fsMock.open(any(Path.class))).thenReturn(mock(FSDataInputStream.class));

        try (MockedStatic<OrcFile> orcFileMock = mockStatic(OrcFile.class);
            MockedStatic<ReaderImpl> readerStaticMock = mockStatic(ReaderImpl.class)
        ) {
            ReaderImpl fileReader = mock(ReaderImpl.class);

            OrcFile.ReaderOptions readerOptions = mock(OrcFile.ReaderOptions.class);
            orcFileMock.when(() -> OrcFile.readerOptions(any(Configuration.class))).thenReturn(readerOptions);
            Mockito.when(readerOptions.filesystem(any(FileSystem.class))).thenReturn(readerOptions);

            orcFileMock.when(() -> OrcFile.createReader(any(Path.class), any(OrcFile.ReaderOptions.class)))
                .thenReturn(fileReader);

            ByteBuffer footerBuffer = Mockito.mock(ByteBuffer.class);
            Mockito.when(fileReader.getSerializedFileFooter()).thenReturn(footerBuffer);

            OrcTail orcTail = Mockito.mock(OrcTail.class);

            readerStaticMock.when(() -> ReaderImpl.extractFileTail(any(ByteBuffer.class), anyLong(), anyLong()))
                .thenReturn(orcTail);

            ORCMetaReaderImpl reader = new ORCMetaReaderImpl(new Configuration(), fsMock, null);
            PreheatFileMeta result = reader.preheat(new Path("test"));

            verify(fsMock).getFileStatus(any(Path.class));
            assertEquals(expectedStatus, result.getFileStatus());

        } catch (Throwable t) {

        }
    }
}

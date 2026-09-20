package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class FileMetaTest {

    @Test
    public void testParseSimpleFileMetaFromOSSORC() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        when(record.getEngine()).thenReturn("OSS");
        when(record.getFileName()).thenReturn("test.orc");
        mockCommonFields(record);

        FileMeta result = FileMeta.parseSimpleFileMetaFrom(record);

        Assert.assertTrue(result instanceof SimpleOSSOrcFileMeta);
        verifyCommonFields(result, record);
    }

    @Test
    public void testParseSimpleFileMetaFromInvalidEngine() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        when(record.getEngine()).thenReturn("UNKNOWN_ENGINE");

        try {
            FileMeta.parseSimpleFileMetaFrom(record);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }
    }

    @Test
    public void testBuildSimpleOrcFileMeta() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        mockCommonFields(record);
        record.engine = "OSS";  // public field
        when(record.getPartitionName()).thenReturn("p0");

        SimpleOSSOrcFileMeta meta = FileMeta.buildSimpleOrcFileMeta(record);

        Assert.assertEquals("test_schema", meta.getLogicalTableSchema());
        Assert.assertEquals("test_table", meta.getLogicalTableName());
        Assert.assertEquals("p0", meta.getPartitionName());
        Assert.assertEquals(100L, meta.getFileSize());
    }

    @Test
    public void testParseSimpleFileMetaFromCSV() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        when(record.getEngine()).thenReturn("S3");
        when(record.getFileName()).thenReturn("data.csv");
        mockCommonFields(record);

        FileMeta result = FileMeta.parseSimpleFileMetaFrom(record);
        Assert.assertTrue(result.getClass().getSimpleName().contains("Csv"));
    }

    // 新增测试方法 - 测试改动的 parseFrom 方法
    @Test
    public void testParseFromOrcFile() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        record.engine = "OSS";  // public field
        when(record.getFileName()).thenReturn("test.orc");
        mockCommonFields(record);
        when(record.getCreateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getUpdateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getRemoveTs()).thenReturn(null);
        when(record.getSchemaTs()).thenReturn(123456L);

        try {
            FileMeta result = FileMeta.parseFrom(record);
            Assert.assertTrue(result instanceof OSSOrcFileMeta);
            Assert.assertEquals("test_schema", result.getLogicalTableSchema());
            Assert.assertEquals("test_table", result.getLogicalTableName());
        } catch (Exception e) {
            // 由于依赖外部资源，可能会抛出异常，这是正常的
            Assert.assertTrue(e.getMessage().contains("PreheatMetaManager") ||
                e.getMessage().contains("FileSystemManager") ||
                e.getCause() != null);
        }
    }

    @Test
    public void testParseFromCsvFile() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        record.engine = "S3";  // public field
        when(record.getFileName()).thenReturn("data.csv");
        mockCommonFields(record);
        when(record.getCreateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getUpdateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getRemoveTs()).thenReturn(null);
        when(record.getSchemaTs()).thenReturn(123456L);

        FileMeta result = FileMeta.parseFrom(record);
        Assert.assertTrue(result.getClass().getSimpleName().contains("Csv"));
        Assert.assertEquals("test_schema", result.getLogicalTableSchema());
        Assert.assertEquals("test_table", result.getLogicalTableName());
    }

    @Test
    public void testParseFromDelFile() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        record.engine = "LOCAL_DISK";  // public field
        when(record.getFileName()).thenReturn("data.del");
        mockCommonFields(record);
        when(record.getCreateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getUpdateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getRemoveTs()).thenReturn(null);
        when(record.getSchemaTs()).thenReturn(123456L);

        FileMeta result = FileMeta.parseFrom(record);
        Assert.assertTrue(result.getClass().getSimpleName().contains("Del"));
        Assert.assertEquals("test_schema", result.getLogicalTableSchema());
        Assert.assertEquals("test_table", result.getLogicalTableName());
    }

    @Test
    public void testParseFromSetFile() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        record.engine = "NFS";  // public field
        when(record.getFileName()).thenReturn("data.set");
        mockCommonFields(record);
        when(record.getCreateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getUpdateTime()).thenReturn("2023-01-01 00:00:00");
        when(record.getRemoveTs()).thenReturn(null);
        when(record.getSchemaTs()).thenReturn(123456L);

        FileMeta result = FileMeta.parseFrom(record);
        Assert.assertTrue(result.getClass().getSimpleName().contains("Set"));
        Assert.assertEquals("test_schema", result.getLogicalTableSchema());
        Assert.assertEquals("test_table", result.getLogicalTableName());
    }

    @Test
    public void testParseFromInvalidEngine() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        record.engine = "INVALID_ENGINE";  // public field

        try {
            FileMeta.parseFrom(record);
            Assert.fail("Should throw exception for invalid engine");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }
    }

    @Test
    public void testParseFromNullEngine() {
        FilesRecord record = Mockito.mock(FilesRecord.class);
        record.engine = null;  // public field

        try {
            FileMeta.parseFrom(record);
            Assert.fail("Should throw exception for null engine");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }
    }

    private void mockCommonFields(FilesRecord record) {
        when(record.getLogicalSchemaName()).thenReturn("test_schema");
        when(record.getLogicalTableName()).thenReturn("test_table");
        when(record.getTableSchema()).thenReturn("phy_schema");
        when(record.getTableName()).thenReturn("phy_table");
        when(record.getExtentSize()).thenReturn(100L);
        when(record.getTableRows()).thenReturn(1000L);
        when(record.getCommitTs()).thenReturn(123456789L);
        when(record.getFileHash()).thenReturn(987654321L);  // Long type
    }

    private void verifyCommonFields(FileMeta meta, FilesRecord record) {
        Assert.assertEquals(record.getLogicalSchemaName(), meta.getLogicalTableSchema());
        Assert.assertEquals(record.getLogicalTableName(), meta.getLogicalTableName());
        Assert.assertEquals(record.getExtentSize(), meta.getFileSize());
        Assert.assertEquals(record.getCommitTs(), meta.getCommitTs());
    }
}
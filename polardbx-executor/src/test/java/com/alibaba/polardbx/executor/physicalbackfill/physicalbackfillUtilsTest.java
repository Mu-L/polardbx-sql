package com.alibaba.polardbx.executor.physicalbackfill;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.polardbx.gms.util.MetaDbUtil;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
//@RunWith(PowerMockRunner.class)
//@PowerMockIgnore("javax.management.*")
//@PrepareForTest({PhysicalBackfillUtils.class, PhysicalBackfillManager.class, ScaleOutPlanUtil.class})
public class physicalbackfillUtilsTest {

    @Before
    public void setUp() throws Exception {
        getDataSourcePool().clear();
    }

    @After
    public void tearDown() throws Exception {
        getDataSourcePool().clear();
    }

    @Test
    public void testDestroyDataSourcesOnlyTargetJob() throws Exception {
        long targetJobId = 1L;
        long otherJobId = 2L;
        XDataSource targetDataSource = mock(XDataSource.class);
        XDataSource otherDataSource = mock(XDataSource.class);

        Map<Long, Map<String, XDataSource>> dataSourcePool = getDataSourcePool();
        dataSourcePool.put(targetJobId, newDataSources("target", targetDataSource));
        dataSourcePool.put(otherJobId, newDataSources("other", otherDataSource));

        PhysicalBackfillUtils.destroyDataSources(targetJobId);

        verify(targetDataSource, times(1)).close();
        verify(otherDataSource, never()).close();
        Assert.assertFalse(dataSourcePool.containsKey(targetJobId));
        Assert.assertSame(otherDataSource, dataSourcePool.get(otherJobId).get("other"));
    }

    @Test
    public void testDestroyDataSourcesRepeatedlyDoesNotCloseTwice() throws Exception {
        long jobId = 1L;
        XDataSource dataSource = mock(XDataSource.class);
        Map<Long, Map<String, XDataSource>> dataSourcePool = getDataSourcePool();
        dataSourcePool.put(jobId, newDataSources("target", dataSource));

        PhysicalBackfillUtils.destroyDataSources(jobId);
        PhysicalBackfillUtils.destroyDataSources(jobId);

        verify(dataSource, times(1)).close();
        Assert.assertFalse(dataSourcePool.containsKey(jobId));
    }

    @Test
    public void testDestroyDataSourcesRetriesFailedClose() throws Exception {
        long jobId = 1L;
        String dataSourceKey = "target";
        XDataSource dataSource = mock(XDataSource.class);
        doThrow(new RuntimeException("close failed")).doNothing().when(dataSource).close();
        Map<Long, Map<String, XDataSource>> dataSourcePool = getDataSourcePool();
        dataSourcePool.put(jobId, newDataSources(dataSourceKey, dataSource));

        PhysicalBackfillUtils.destroyDataSources(jobId);

        verify(dataSource, times(1)).close();
        Assert.assertTrue(dataSourcePool.containsKey(jobId));
        Assert.assertSame(dataSource, dataSourcePool.get(jobId).get(dataSourceKey));

        PhysicalBackfillUtils.destroyDataSources(jobId);

        verify(dataSource, times(2)).close();
        Assert.assertFalse(dataSourcePool.containsKey(jobId));
    }

    private Map<String, XDataSource> newDataSources(String key, XDataSource dataSource) {
        Map<String, XDataSource> dataSources = new ConcurrentHashMap<>();
        dataSources.put(key, dataSource);
        return dataSources;
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Map<String, XDataSource>> getDataSourcePool() throws Exception {
        Field field = PhysicalBackfillUtils.class.getDeclaredField("dataSourcePool");
        field.setAccessible(true);
        return (Map<Long, Map<String, XDataSource>>) field.get(null);
    }

    @Test
    // 测试backfillId非空，tableSchema非空，tableName非空，type=2，查询到一个回填对象记录
    public void testRollbackCopyIbd_withTypeTwo() throws Exception {
        Long backfillId = 1709745034997297152L;
        String tableSchema = "d1";
        String tableName = "t1";
        int type = 2;
        ExecutionContext ec = mock(ExecutionContext.class);

        List<PhysicalBackfillManager.BackfillObjectRecord> backfillObjectRecords = new ArrayList<>();
        backfillObjectRecords.add(mockBackfillObjectRecord());

        DbGroupInfoRecord dbGroupInfoRecord = mock(DbGroupInfoRecord.class);
        dbGroupInfoRecord.groupType = 0;
        dbGroupInfoRecord.groupName = "d1_p00000_group";
        dbGroupInfoRecord.phyDbName = "d1_p00000";

        try (MockedStatic<ScaleOutPlanUtil> mockedScaleOutPlanUtil = Mockito.mockStatic(ScaleOutPlanUtil.class);
            MockedStatic<PhysicalBackfillUtils> mockedPhysicalBackfillUtils = Mockito.mockStatic(
                PhysicalBackfillUtils.class);
            MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<PhysicalBackfillManager> mocked = Mockito.mockConstruction(PhysicalBackfillManager.class,
                (mock, context) -> {
                    Mockito.when(mock.queryBackfillObject(backfillId, tableSchema, tableName))
                        .thenReturn(backfillObjectRecords);
                })) {

            mockedScaleOutPlanUtil.when(() -> ScaleOutPlanUtil.getDbGroupInfoByGroupName(Mockito.anyString()))
                .thenReturn(dbGroupInfoRecord);

            mockedPhysicalBackfillUtils.when(() -> PhysicalBackfillUtils.deleteInnodbDataFile(
                    anyLong(),
                    anyString(),
                    anyString(),
                    anyString(),
                    anyString(),
                    anyInt(),
                    anyString(),
                    anyBoolean(),
                    any(ExecutionContext.class)))
                .thenAnswer(invocation -> null);  // 对于 void 方法使用 thenAnswer 回调并返回 null
            Connection mockConnection = mock(Connection.class);
            mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            // Call the actual method that triggers the static method call
            //PowerMockito.doCallRealMethod().when(PhysicalBackfillUtils.class, "rollbackCopyIbd", backfillId, tableSchema, tableName, type, ec);

            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.rollbackCopyIbd(0L, backfillId, tableSchema, tableName, type, ec))
                .thenCallRealMethod();

            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.deleteInnodbDataFiles(anyLong(), anyString(), any(Pair.class), anyString(),
                        anyString(), anyString(), anyBoolean(), any(ExecutionContext.class))).thenCallRealMethod()
                .thenCallRealMethod();

            PhysicalBackfillUtils.rollbackCopyIbd(0L, backfillId, tableSchema, tableName, type, ec);

            // 验证 staticMethod 被正确的用参数调用
            mockedScaleOutPlanUtil.verify(() -> ScaleOutPlanUtil.getDbGroupInfoByGroupName(Mockito.anyString()),
                Mockito.times(1));
            mockedPhysicalBackfillUtils.verify(
                () -> PhysicalBackfillUtils.deleteInnodbDataFile(Mockito.anyLong(), Mockito.anyString(),
                    Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.any(ExecutionContext.class)), Mockito.times(2));
        }
    }

    @Test
    // 测试backfillId非空，tableSchema非空，tableName非空，type=1，查询到一个回填对象记录
    public void testRollbackCopyIbd_withTypeOne() throws Exception {
        Long backfillId = 1709745034997297152L;
        String tableSchema = "d1";
        String tableName = "t1";
        int type = 1;
        ExecutionContext ec = mock(ExecutionContext.class);

        List<PhysicalBackfillManager.BackfillObjectRecord> backfillObjectRecords = new ArrayList<>();
        backfillObjectRecords.add(mockBackfillObjectRecord());

        DbGroupInfoRecord dbGroupInfoRecord = mock(DbGroupInfoRecord.class);
        dbGroupInfoRecord.groupType = 0;
        dbGroupInfoRecord.groupName = "d1_p00000_group";
        dbGroupInfoRecord.phyDbName = "d1_p00000";

        try (MockedStatic<ScaleOutPlanUtil> mockedScaleOutPlanUtil = Mockito.mockStatic(ScaleOutPlanUtil.class);
            MockedStatic<PhysicalBackfillUtils> mockedPhysicalBackfillUtils = Mockito.mockStatic(
                PhysicalBackfillUtils.class);
            MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<PhysicalBackfillManager> mocked = Mockito.mockConstruction(PhysicalBackfillManager.class,
                (mock, context) -> {
                    Mockito.when(mock.queryBackfillObject(backfillId, tableSchema, tableName))
                        .thenReturn(backfillObjectRecords);
                })) {

            mockedScaleOutPlanUtil.when(() -> ScaleOutPlanUtil.getDbGroupInfoByGroupName(Mockito.anyString()))
                .thenReturn(dbGroupInfoRecord);

            mockedPhysicalBackfillUtils.when(() -> PhysicalBackfillUtils.deleteInnodbDataFile(
                    anyLong(),
                    anyString(),
                    anyString(),
                    anyString(),
                    anyString(),
                    anyInt(),
                    anyString(),
                    anyBoolean(),
                    any(ExecutionContext.class)))
                .thenAnswer(invocation -> null);  // 对于 void 方法使用 thenAnswer 回调并返回 null

            // Mock MetaDbUtil.getConnection() to return a mock connection
            Connection mockConnection = mock(Connection.class);
            mockedMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            // Call the actual method that triggers the static method call
            //PowerMockito.doCallRealMethod().when(PhysicalBackfillUtils.class, "rollbackCopyIbd", backfillId, tableSchema, tableName, type, ec);

            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.rollbackCopyIbd(0L, backfillId, tableSchema, tableName, type, ec))
                .thenCallRealMethod();

            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.deleteInnodbDataFiles(anyLong(), anyString(), any(Pair.class), anyString(),
                        anyString(), anyString(), anyBoolean(), any(ExecutionContext.class))).thenCallRealMethod()
                .thenCallRealMethod();

            PhysicalBackfillUtils.rollbackCopyIbd(0L, backfillId, tableSchema, tableName, type, ec);

            // 验证 staticMethod 被正确的用参数调用
            mockedScaleOutPlanUtil.verify(() -> ScaleOutPlanUtil.getDbGroupInfoByGroupName(Mockito.anyString()),
                Mockito.times(0));
            mockedPhysicalBackfillUtils.verify(
                () -> PhysicalBackfillUtils.deleteInnodbDataFile(Mockito.anyLong(), Mockito.anyString(),
                    Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.any(ExecutionContext.class)), Mockito.times(1));
        }
    }

    @Test
    // 测试正常情况，ibd文件名包含IBD但不包含TEMP_FILE_POSTFIX
    public void testConvertToCfgFileName_normal() {
        Assert.assertEquals("test.cfg", PhysicalBackfillUtils.convertToCfgFileName("test.ibd", "cfg"));
    }

    @Test
    // 测试临时文件情况，ibd文件名同时包含IBD和TEMP_FILE_POSTFIX
    public void testConvertToCfgFileName_tempFile() {
        Assert.assertEquals("test.cfg.TEMPFILE",
            PhysicalBackfillUtils.convertToCfgFileName("test.ibd.TEMPFILE", "cfg"));
    }

    @Test
    // 测试空字符串输入
    public void testConvertToCfgFileName_emptyString() {
        try {
            PhysicalBackfillUtils.convertToCfgFileName("", ".cfg");
            Assert.fail("Expected an AssertionError to be thrown");
        } catch (AssertionError e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    // 测试不含IBD的文件名
    public void testConvertToCfgFileName_withoutIBD() {
        try {
            Assert.assertEquals("test.cfg", PhysicalBackfillUtils.convertToCfgFileName("test", ".cfg"));
            Assert.fail("Expected an AssertionError to be thrown");
        } catch (AssertionError e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    // 测试ibdIndex为-1的情况
    public void testConvertToCfgFileName_ibdIndexMinusOne() {
        try {
            PhysicalBackfillUtils.convertToCfgFileName("test_IBD-", ".cfg");
            Assert.fail("Expected an AssertionError to be thrown");
        } catch (AssertionError e) {
            Assert.assertTrue(true);
        }
    }

    private PhysicalBackfillManager.BackfillObjectRecord mockBackfillObjectRecord() {
        PhysicalBackfillManager.BackfillObjectRecord record = new PhysicalBackfillManager.BackfillObjectRecord();
        record.setJobId(1709745034997297152l);
        record.setTableSchema("d1");
        record.setTableName("t1");
        record.setIndexSchema("d1");
        record.setIndexName("t1");
        record.setPhysicalDb("d1_p00000");
        record.setPhysicalTable("t1_za6v_00000");
        record.setSourceGroupName("d1_p00000_group");
        record.setTargetGroupName("d1_p00001_group");
        record.setSourceFileName("d1_p00000/t1_za6v_00000");
        record.setSourceDirName("./d1_p00000/t1_za6v_00000.ibd.TEMPFILE");
        record.setTargetFileName("d1_p00001/t1_za6v_00000");
        record.setTargetDirName("./d1_p00001/t1_za6v_00000.ibd.TEMPFILE");
        record.setStatus(2);
        record.setDetailInfo(
            "{\"msg\":\"\",\"sourceHostAndPort\":{\"key\":\"26.12.155.86\",\"value\":31159},\"targetHostAndPorts\":[{\"key\":\"11.112.141.109\",\"value\":31175},{\"key\":\"26.12.156.152\",\"value\":31096}]}");
        record.setTotalBatch(2);
        record.setBatchSize(65536);
        record.setOffset(2);
        record.setStartTime("2024-03-27 17:10:43");
        record.setEndTime("2024-03-27 17:10:43");
        record.setLsn(0);
        return record;
    }
}

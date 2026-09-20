package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test for {@link CloneTableDataFileTaskTest}
 *
 * @author luoyanxin
 */
public class CloneTableDataFileTaskTest {
    private BatchConsumer consumerMock;
    private ExecutionContext ecMock;
    private ServerThreadPool serverThreadPoolMock;

    @Before
    public void setUp() {
        consumerMock = mock(BatchConsumer.class);
        Mockito.doNothing().when(consumerMock)
            .consume(any(), any(), any(), any(), any(PolarxPhysicalBackfill.TransferFileDataOperator.class));
        ecMock = mock(ExecutionContext.class);
        serverThreadPoolMock = mock(ServerThreadPool.class);
        Mockito.when(ecMock.getExecutorService()).thenReturn(serverThreadPoolMock);
    }

    @After
    public void tearDown() {
        Mockito.reset(consumerMock, ecMock, serverThreadPoolMock);
    }

    public void testForeachPhysicalFile_success() throws Exception {
        prepareAndRunCloneTableDataFileTask(false, false, false, false);
        prepareAndRunCloneTableDataFileTask(false, true, false, false);
    }

    @Test
    public void testForeachPhysicalFile_failed() throws Exception {
        try {
            prepareAndRunCloneTableDataFileTask(false, false, false, true);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().indexOf("SCALEOUT") > 0);
        }
        /*
        try {
            prepareAndRunCloneTableDataFileTask(false, false, false);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().indexOf("the status of BackfillBean is empty or init ") > 0);
        }

        try {
            prepareAndRunCloneTableDataFileTask(true, false, true);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NullPointerException);
        }
        try {
            prepareAndRunCloneTableDataFileTask(true, true, true);
            Assert.fail("should throw exception");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause() instanceof NullPointerException);
        }*/
    }

    @Test
    public void testGenerateTargetFileDirWithNewDbFormat() throws Exception {
        // Test new partition database format
        List list = new ArrayList();
        list.add(Pair.of("11.112.141.109", 31175));
        list.add(Pair.of("26.12.156.152", 31096));

        CloneTableDataFileTask task = spy(new CloneTableDataFileTask("schema", "t1",
            Pair.of("d1_p00000", "d1_p00000_group"),
            Pair.of("d1_p00001", "d1_p00001_group"), "t1_za6v_00000", ImmutableList.of(""), "dn0",
            Pair.of("26.12.155.86", 31159), list, 1000L, 0l, false));

        try (MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            Mockito.when(dbInfoManager.isNewPartitionDb(anyString())).thenReturn(true);
            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);
            // Use reflection to access private method
            Method generateTargetFileDirMethod = CloneTableDataFileTask.class.getDeclaredMethod(
                "generateTargetFileDir", String.class, String.class, boolean.class);
            generateTargetFileDirMethod.setAccessible(true);

            // Test case 1: New DB format with @ symbol
            String result = (String) generateTargetFileDirMethod.invoke(task,
                "d1@1_p00000/t1_za6v_00000", "d1_p00001", false);
            Assert.assertEquals("d1@1_p00001/t1_za6v_00000", result);

            // Test case 2: New DB format without @ symbol
            result = (String) generateTargetFileDirMethod.invoke(task,
                "d1_p00000/t1_za6v_00000", "d1_p00001", false);
            Assert.assertEquals("d1_p00001/t1_za6v_00000", result);

            result = (String) generateTargetFileDirMethod.invoke(task,
                "d1_p00000/t1_@za6v_00000", "d1_p00001", false);
            Assert.assertEquals("d1_p00001/t1_@za6v_00000", result);

            // Test case 3: New DB format with full path generation
            result = (String) generateTargetFileDirMethod.invoke(task,
                "./d1@1_p00000/t1_za6v_00000.ibd", "d1_p00001", true);
            Assert.assertEquals("./d1@1_p00001/t1_za6v_00000.ibd", result);

            result = (String) generateTargetFileDirMethod.invoke(task,
                "./d1@1_p00000/t1_@za6v_00000.ibd", "d1_p00001", true);
            Assert.assertEquals("./d1@1_p00001/t1_@za6v_00000.ibd", result);

            result = (String) generateTargetFileDirMethod.invoke(task,
                "./d1_p00000/t1_za6v_00000.ibd", "d1_p00001", true);
            Assert.assertEquals("./d1_p00001/t1_za6v_00000.ibd", result);
        }
    }

    @Test
    public void testGenerateTargetFileDirWithOldDbFormat() throws Exception {
        // Test old partition database format
        List list = new ArrayList();
        list.add(Pair.of("11.112.141.109", 31175));
        list.add(Pair.of("26.12.156.152", 31096));

        CloneTableDataFileTask task = spy(new CloneTableDataFileTask("schema", "t1",
            Pair.of("d1_000000", "d1_000000_group"),
            Pair.of("d1_000001", "d1_000001_group"), "t1_za6v_00000", ImmutableList.of(""), "dn0",
            Pair.of("26.12.155.86", 31159), list, 1000L, 0l, false));

        // Mock DbInfoManager to return false for isNewPartitionDb
        try (MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class)) {
            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            Mockito.when(dbInfoManager.isNewPartitionDb(anyString())).thenReturn(false);
            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);

            // Use reflection to access private method
            Method generateTargetFileDirMethod = CloneTableDataFileTask.class.getDeclaredMethod(
                "generateTargetFileDir", String.class, String.class, boolean.class);
            generateTargetFileDirMethod.setAccessible(true);

            // Test case 1: DRDS DB format with @ symbol
            String result = (String) generateTargetFileDirMethod.invoke(task,
                "d1@1_000000/t1_za6v_00000", "d1_000001", false);
            Assert.assertEquals("d1@1_000001/t1_za6v_00000", result);

            // Test case 2: DRDS DB format without @ symbol
            result = (String) generateTargetFileDirMethod.invoke(task,
                "d1_000000/t1_za6v_00000", "d1_000001", false);
            Assert.assertEquals("d1_000001/t1_za6v_00000", result);

            // Test case 3: DRDS DB format with full path generation
            result = (String) generateTargetFileDirMethod.invoke(task,
                "./d1@1_000000/t1_za6v_00000.ibd", "d1_000001", true);
            Assert.assertEquals("./d1@1_000001/t1_za6v_00000.ibd", result);

            result = (String) generateTargetFileDirMethod.invoke(task,
                "./d1_000000/t1_za6v_00000.ibd", "d1_000001", true);
            Assert.assertEquals("./d1_000001/t1_za6v_00000.ibd", result);
        }
    }

    @Test
    public void testProcessPartitionFile() throws Exception {
        // Test processPartitionFile method directly
        List list = new ArrayList();
        list.add(Pair.of("11.112.141.109", 31175));
        list.add(Pair.of("26.12.156.152", 31096));

        CloneTableDataFileTask task = spy(new CloneTableDataFileTask("schema", "t1",
            Pair.of("d1_p00000", "d1_p00000_group"),
            Pair.of("d1_p00001", "d1_p00001_group"), "t1_za6v_00000", ImmutableList.of(""), "dn0",
            Pair.of("26.12.155.86", 31159), list, 1000L, 0l, false));

        // Use reflection to access private method and fields
        Method processPartitionFileMethod = CloneTableDataFileTask.class.getDeclaredMethod(
            "processPartitionFile", String.class, String.class, Pattern.class, Pattern.class, String.class, int.class,
            AtomicBoolean.class);
        processPartitionFileMethod.setAccessible(true);

        Field newDbTargetPatternField = CloneTableDataFileTask.class.getDeclaredField("NEW_DB_TARGET_PATTERN");
        newDbTargetPatternField.setAccessible(true);
        Pattern newDbTargetPattern = (Pattern) newDbTargetPatternField.get(null);

        Field newDbSourcePatternField = CloneTableDataFileTask.class.getDeclaredField("NEW_DB_SOURCE_PATTERN");
        newDbSourcePatternField.setAccessible(true);
        Pattern newDbSourcePattern = (Pattern) newDbSourcePatternField.get(null);

        // Test case 1: Valid pattern match
        AtomicBoolean find = new AtomicBoolean(false);
        String result = (String) processPartitionFileMethod.invoke(task,
            "d@1_p00000/t1_za6v_00000.ibd", "d1_p00001",
            newDbTargetPattern, newDbSourcePattern,
            "_p", 7, find);
        Assert.assertTrue(find.get());
        Assert.assertEquals("d@1_p00001/t1_za6v_00000.ibd", result);

        // Test case 2: No match
        find.set(false);
        result = (String) processPartitionFileMethod.invoke(task,
            "d1_dd00000/t1_za6v_00000.ibd", "d1_dd00001",
            newDbTargetPattern, newDbSourcePattern,
            "_p", 7, find);
        Assert.assertFalse(find.get());
        Assert.assertNull(result);
    }

    /**
     * 准备并运行物理回填任务，根据参数模拟不同的初始化状态、加密情况及连接是否为空的情况
     *
     * @param inited 是否已初始化
     * @param encrypted 数据是否加密
     * @param connectIsNull 连接是否为null
     * @throws Exception 可能抛出的异常
     */
    public void prepareAndRunCloneTableDataFileTask(boolean inited, boolean encrypted, boolean connectIsNull,
                                                    boolean throwException)
        throws Exception {
        //OptimizerContext.getContext(schemaName)
        // Arrange
        OptimizerContext optimizerContext = new OptimizerContext("d1");
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedStatic<ScaleOutPlanUtil> mockedScaleOutPlanUtil = Mockito.mockStatic(ScaleOutPlanUtil.class);
            MockedStatic<PhysicalBackfillUtils> mockedPhysicalBackfillUtils = Mockito.mockStatic(
                PhysicalBackfillUtils.class);
            MockedConstruction<PhysicalBackfillManager> mocked = Mockito.mockConstruction(PhysicalBackfillManager.class,
                (mock, context) -> {
                    Mockito.when(mock.loadBackfillMeta(anyLong(), anyString(), anyString(), anyString(), anyString()))
                        .thenReturn(mockBackfillBean(inited));
                }); MockedStatic<OptimizerContext> mockedOptimizerContext = Mockito.mockStatic(OptimizerContext.class)
            ; MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class);
            MockedConstruction<FutureTask> mockedFuture = Mockito.mockConstruction(FutureTask.class);) {
            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.getTempIbdFileInfo(anyLong(), any(Pair.class), any(Pair.class),
                        any(Pair.class),
                        any(String.class), any(String.class), any(Pair.class), any(Long.class), any(Boolean.class),
                        any(List.class)))
                .thenAnswer(invocation -> {
                    List<Pair<Long, Long>> offsetAndSize = invocation.getArgument(8);
                    // 根据你的测试需要，对 offsetAndSize 进行修改
                    offsetAndSize.add(new Pair<>(123L, 456L));
                    return null;
                });
            Mockito.when(dbInfoManager.isNewPartitionDb(anyString()))
                .thenReturn(true);
            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);
            ParamManager paramManager = new ParamManager(new HashMap<String, String>());
            paramManager.getProps().put("PHYSICAL_BACKFILL_SPEED_LIMIT", "-1");
            optimizerContext.setParamManager(paramManager);
            mockedOptimizerContext.when(() -> OptimizerContext.getContext(anyString())).thenReturn(optimizerContext);

            mockedScaleOutPlanUtil.when(() -> ScaleOutPlanUtil.getDbGroupInfoByGroupName(anyString())).thenReturn(
                getMockTarGroupInfoRecord());

            List list = new ArrayList();
            list.add(Pair.of("11.112.141.109", 31175));
            list.add(Pair.of("26.12.156.152", 31096));
            CloneTableDataFileTask task =
                spy(new CloneTableDataFileTask("schema", "t1", Pair.of("d1_p00000", "d1_p00000_group"),
                    Pair.of("d1_p00001", "d1_p00001_group"), "t1_za6v_00000", ImmutableList.of(""), "dn0",
                    Pair.of("26.12.156.152", 31096), list, 1000L, 0l,
                    encrypted));
            task.setTaskId(1L);

            XConnection conn = mock(XConnection.class);
            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.getXConnectionForStorage(anyLong(), anyString(), anyString(), anyInt(),
                        anyString(),
                        anyString(), anyInt()))
                .thenReturn(connectIsNull ? null : conn);

            if (throwException) {
                when(conn.exeCloneFile(any())).thenThrow(new SQLException("test"));
                when(conn.execQuery(anyString())).thenThrow(new SQLException("test"));
            }
            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.getUserPasswd(anyString()))
                .thenReturn(Pair.of("user1", "pwd1"));

            Map<String, Pair<String, String>> srcFileAndDirs = new HashMap<>();
            srcFileAndDirs.put("p1", Pair.of("d1_p00000", "t1_za6v_00000.ibd"));
            srcFileAndDirs.put("p2", Pair.of("d1_p00000", "t1_za6v_00001.ibd"));
            mockedPhysicalBackfillUtils.when(
                () -> PhysicalBackfillUtils.getSourceTableInfo(anyLong(), any(), anyString(), anyString(), any(),
                    anyBoolean(),
                    any())).thenReturn(srcFileAndDirs);

            PolarxPhysicalBackfill.TransferFileDataOperator transferFileDataOperator =
                mock(PolarxPhysicalBackfill.TransferFileDataOperator.class);
            Mockito.when(transferFileDataOperator.getBufferLen()).thenReturn(2000l).thenReturn(1l);
            Mockito.when(conn.execReadBufferFromFile(any())).thenReturn(transferFileDataOperator);

            mockedPhysicalBackfillUtils.when(
                    () -> PhysicalBackfillUtils.convertToCfgFileName(any(String.class), any(String.class)))
                .thenCallRealMethod();

            Mockito.doNothing().when(task).updateTaskStateInNewTxn(any());
            task.beforeTransaction(ecMock);

            mockedPhysicalBackfillUtils.verify(
                () -> PhysicalBackfillUtils.convertToCfgFileName(any(String.class), any(String.class)),
                Mockito.times(encrypted ? 4 : 2)
            );
        }
    }

    private PhysicalBackfillManager.BackfillBean mockBackfillBean(boolean isInit) {
        return new PhysicalBackfillManager.BackfillBean(1709745034997297152l, "d1", "t1", "d1", "t1",
            mockBackfillObjectRecord(isInit));
    }

    private PhysicalBackfillManager.BackfillObjectBean mockBackfillObjectRecord(boolean isInit) {
        PhysicalBackfillDetailInfoFieldJSON detailInfo = PhysicalBackfillDetailInfoFieldJSON.fromJson(
            "{\"msg\":\"\",\"sourceHostAndPort\":{\"key\":\"26.12.155.86\",\"value\":31159},\"targetHostAndPorts\":[{\"key\":\"11.112.141.109\",\"value\":31175},{\"key\":\"26.12.156.152\",\"value\":31096}]}");
        PhysicalBackfillManager.BackfillObjectBean record =
            new PhysicalBackfillManager.BackfillObjectBean(1, 1709745034997297152l, "d1", "t1", "d1", "t1",
                "d1_p00000", "t1_za6v_00000", "", "d1_p00000_group", "d1_p00001_group", "d1_p00000/t1_za6v_00000",
                "./d1_p00000/t1_za6v_00000.ibd.TEMPFILE", "d1_p00001/t1_za6v_00000",
                "./d1_p00000/t1_za6v_00001",
                isInit ? PhysicalBackfillManager.BackfillStatus.RUNNING : PhysicalBackfillManager.BackfillStatus.INIT,
                detailInfo, 2, 65536,
                0, 2, "2024-03-27 17:10:43", "2024-03-27 17:10:43", null, 0, 0);
        return record;
    }

    private DbGroupInfoRecord getMockTarGroupInfoRecord() {
        DbGroupInfoRecord tarDbGroupInfoRecord = new DbGroupInfoRecord();
        tarDbGroupInfoRecord.groupName = "d1_p00001_group";
        tarDbGroupInfoRecord.phyDbName = "d1_p00001";
        tarDbGroupInfoRecord.groupType = DbGroupInfoRecord.GROUP_TYPE_NORMAL;
        tarDbGroupInfoRecord.id = 1L;
        return tarDbGroupInfoRecord;
    }
}
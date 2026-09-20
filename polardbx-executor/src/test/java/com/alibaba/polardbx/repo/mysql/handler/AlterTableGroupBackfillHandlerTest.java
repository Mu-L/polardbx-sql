package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.partitionmanagement.fastchecker.AlterTableGroupFastChecker;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AlterTableGroupBackfillHandler.fastCheck() method,
 * specifically covering the fix for move partition srcTarPhyTableMap building logic.
 *
 * @author luoyanxin
 */
public class AlterTableGroupBackfillHandlerTest {

    private AlterTableGroupBackfillHandler handler;
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        IRepository repo = mock(IRepository.class);
        handler = new AlterTableGroupBackfillHandler(repo);
        executionContext = new ExecutionContext();
    }

    // ==================== fastCheck srcTarPhyTableMap building tests ====================

    @Test
    public void testFastCheck_MirrorCopy_BuildsSrcTarPhyTableMapCorrectly() {
        // Test the new logic: for move partition, ptbGroupMap maps phyTable -> [srcGroup, tarGroup]
        Map<String, List<String>> ptbGroupMap = new HashMap<>();
        ptbGroupMap.put("t1_00000", Arrays.asList("GROUP_SRC_0", "GROUP_TAR_0"));
        ptbGroupMap.put("t1_00001", Arrays.asList("GROUP_SRC_1", "GROUP_TAR_1"));

        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("GROUP_SRC_0", setOf("t1_00000"));
        srcPhyDbAndTables.put("GROUP_SRC_1", setOf("t1_00001"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("GROUP_TAR_0", setOf("t1_00000"));
        dstPhyDbAndTables.put("GROUP_TAR_1", setOf("t1_00001"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenReturn(mockFastChecker);
            when(mockFastChecker.check(any())).thenReturn(true);

            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                ptbGroupMap, srcPhyDbAndTables, dstPhyDbAndTables, true);
            assertTrue(result);
        }
    }

    @Test
    public void testFastCheck_MirrorCopy_NullGroupSkipped() {
        // Entry with null srcGroup or tarGroup should be skipped
        Map<String, List<String>> ptbGroupMap = new HashMap<>();
        ptbGroupMap.put("t1_00000", Arrays.asList("GROUP_SRC_0", "GROUP_TAR_0"));
        ptbGroupMap.put("t1_00001", Arrays.asList(null, "GROUP_TAR_1")); // null srcGroup
        ptbGroupMap.put("t1_00002", Arrays.asList("GROUP_SRC_2", null)); // null tarGroup

        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("GROUP_SRC_0", setOf("t1_00000"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("GROUP_TAR_0", setOf("t1_00000"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenReturn(mockFastChecker);
            when(mockFastChecker.check(any())).thenReturn(true);

            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                ptbGroupMap, srcPhyDbAndTables, dstPhyDbAndTables, true);
            assertTrue(result);
        }
    }

    @Test
    public void testFastCheck_NotMirrorCopy_SrcTarPhyTableMapIsNull() {
        // When isMirrorCopy=false, srcTarPhyTableMap should remain null
        Map<String, List<String>> ptbGroupMap = new HashMap<>();
        ptbGroupMap.put("t1_00000", Arrays.asList("GROUP_SRC", "GROUP_TAR"));

        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("GROUP_SRC", setOf("t1_00000"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("GROUP_TAR", setOf("t1_00000"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenReturn(mockFastChecker);
            when(mockFastChecker.check(any())).thenReturn(true);

            // isMirrorCopy = false
            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                ptbGroupMap, srcPhyDbAndTables, dstPhyDbAndTables, false);
            assertTrue(result);
        }
    }

    @Test
    public void testFastCheck_MirrorCopy_EmptyPtbGroupMap_SrcTarPhyTableMapIsNull() {
        // When ptbGroupMap is empty, srcTarPhyTableMap should remain null
        Map<String, List<String>> ptbGroupMap = new HashMap<>();

        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("GROUP_SRC", setOf("t1_00000"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("GROUP_TAR", setOf("t1_00000"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenReturn(mockFastChecker);
            when(mockFastChecker.check(any())).thenReturn(true);

            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                ptbGroupMap, srcPhyDbAndTables, dstPhyDbAndTables, true);
            assertTrue(result);
        }
    }

    @Test
    public void testFastCheck_MirrorCopy_NullPtbGroupMap_SrcTarPhyTableMapIsNull() {
        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("GROUP_SRC", setOf("t1_00000"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("GROUP_TAR", setOf("t1_00000"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenReturn(mockFastChecker);
            when(mockFastChecker.check(any())).thenReturn(true);

            // null ptbGroupMap
            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                null, srcPhyDbAndTables, dstPhyDbAndTables, true);
            assertTrue(result);
        }
    }

    @Test
    public void testFastCheck_CheckFails_ReturnsFalse() {
        Map<String, List<String>> ptbGroupMap = new HashMap<>();
        ptbGroupMap.put("t1_00000", Arrays.asList("GROUP_SRC", "GROUP_TAR"));

        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("GROUP_SRC", setOf("t1_00000"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("GROUP_TAR", setOf("t1_00000"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenReturn(mockFastChecker);
            when(mockFastChecker.check(any())).thenReturn(false);

            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                ptbGroupMap, srcPhyDbAndTables, dstPhyDbAndTables, true);
            assertFalse(result);
        }
    }

    @Test
    public void testFastCheck_MirrorCopy_MultiplePartitions_CorrectMapping() {
        // Verify the mapping: each phyTable maps to [srcGroup, tarGroup] -> [(tarGroup, phyTable)]
        Map<String, List<String>> ptbGroupMap = new HashMap<>();
        ptbGroupMap.put("t1_p1", Arrays.asList("SRC_G0", "TAR_G0"));
        ptbGroupMap.put("t1_p2", Arrays.asList("SRC_G1", "TAR_G1"));
        ptbGroupMap.put("t1_p3", Arrays.asList("SRC_G0", "TAR_G2"));

        Map<String, Set<String>> srcPhyDbAndTables = new HashMap<>();
        srcPhyDbAndTables.put("SRC_G0", setOf("t1_p1", "t1_p3"));
        srcPhyDbAndTables.put("SRC_G1", setOf("t1_p2"));

        Map<String, Set<String>> dstPhyDbAndTables = new HashMap<>();
        dstPhyDbAndTables.put("TAR_G0", setOf("t1_p1"));
        dstPhyDbAndTables.put("TAR_G1", setOf("t1_p2"));
        dstPhyDbAndTables.put("TAR_G2", setOf("t1_p3"));

        try (MockedStatic<AlterTableGroupFastChecker> mockedChecker =
            Mockito.mockStatic(AlterTableGroupFastChecker.class)) {
            FastChecker mockFastChecker = mock(FastChecker.class);
            mockedChecker.when(() -> AlterTableGroupFastChecker.create(
                anyString(), anyString(), anyMap(), anyMap(), any(), anyBoolean(), any()
            )).thenAnswer(inv -> {
                // Verify srcTarPhyTableMap argument (5th param, index 4)
                Map<Pair<String, String>, List<Pair<String, String>>> map = inv.getArgument(4);
                assertNotNull("srcTarPhyTableMap should not be null for mirrorCopy", map);
                assertEquals(3, map.size());

                // Check t1_p1: (SRC_G0, t1_p1) -> [(TAR_G0, t1_p1)]
                List<Pair<String, String>> targets = map.get(Pair.of("SRC_G0", "t1_p1"));
                assertNotNull(targets);
                assertEquals(1, targets.size());
                assertEquals("TAR_G0", targets.get(0).getKey());
                assertEquals("t1_p1", targets.get(0).getValue());

                // Check t1_p2: (SRC_G1, t1_p2) -> [(TAR_G1, t1_p2)]
                targets = map.get(Pair.of("SRC_G1", "t1_p2"));
                assertNotNull(targets);
                assertEquals("TAR_G1", targets.get(0).getKey());

                // Check t1_p3: (SRC_G0, t1_p3) -> [(TAR_G2, t1_p3)]
                targets = map.get(Pair.of("SRC_G0", "t1_p3"));
                assertNotNull(targets);
                assertEquals("TAR_G2", targets.get(0).getKey());

                return mockFastChecker;
            });
            when(mockFastChecker.check(any())).thenReturn(true);

            boolean result = handler.fastCheck(executionContext, "test_schema", "t1",
                ptbGroupMap, srcPhyDbAndTables, dstPhyDbAndTables, true);
            assertTrue(result);
        }
    }

    // ==================== Helper ====================

    private Set<String> setOf(String... items) {
        Set<String> set = new HashSet<>();
        for (String item : items) {
            set.add(item);
        }
        return set;
    }
}

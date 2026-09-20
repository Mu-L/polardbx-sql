package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

/**
 * Reproduces AONE-83312328: TablePartitionAccessor throws
 * ERR_GMS_ACCESS_TO_SYSTEM_TABLE with only 1 param, while the message
 * template "Failed to {0} the system table {1}. Caused by: {2}." requires 3,
 * leaving {1} and {2} as unreplaced literal placeholders in the final message.
 *
 * <p>This class exercises every catch block in {@link TablePartitionAccessor}
 * that constructs an {@code ERR_GMS_ACCESS_TO_SYSTEM_TABLE} exception, to make
 * sure each of them passes the full 3-param (action, table, cause) form.
 */
public class TablePartitionAccessorErrorMessageTest {

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void assertGmsAccessException(TddlRuntimeException e, String expectedAction,
                                                 String expectedTable) {
        assertEquals(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE.getCode(), e.getErrorCode());
        String message = e.getMessage();
        assertFalse(
            "Error message must not contain unreplaced placeholder {1}, but was: " + message,
            message.contains("{1}"));
        assertFalse(
            "Error message must not contain unreplaced placeholder {2}, but was: " + message,
            message.contains("{2}"));
        assertTrue(
            "Error message must contain real operation type and table name, but was: " + message,
            message.contains(expectedAction) && message.contains(expectedTable));
    }

    private static void assertThrowsGmsAccessException(ThrowingRunnable runnable, String expectedAction,
                                                       String expectedTable) throws Exception {
        try {
            runnable.run();
            fail("should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertGmsAccessException(e, expectedAction, expectedTable);
        }
    }

    @Test
    public void testGetTablePartitionsByDbNameTbNameLevelErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));

            try {
                accessor.getTablePartitionsByDbNameTbNameLevel("test_schema", "test_table", 0, false);
                fail("should throw TddlRuntimeException when MetaDbUtil.query fails");
            } catch (TddlRuntimeException e) {
                assertGmsAccessException(e, "query", "table_partitions");
            }
        }
    }

    @Test
    public void testGetTablePartitionsFromDeltaBySchTgIdForUpdateErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsFromDeltaBySchTgIdForUpdate("test_schema", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsFromDeltaBySchTbForUpdateErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsFromDeltaBySchTbForUpdate("test_schema", "test_table"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbNameLevelErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbNameLevel("test_schema", 0),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetPublicTablePartitionsByDbNameTbNameLevelErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getPublicTablePartitionsByDbNameTbNameLevel("test_schema", "test_table", 0),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetValidTablePartitionsByDbNameTbNameLevelErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getValidTablePartitionsByDbNameTbNameLevel("test_schema", "test_table", 0),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetInValidTablePartitionsByDbNameTbNameLevelErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getInValidTablePartitionsByDbNameTbNameLevel("test_schema", "test_table", 0),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbNameGroupIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbNameGroupId("test_schema", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbTbLvl0ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbTbLvl0("test_schema", "test_table", false),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetAllTablePartitionsByDbNameGroupIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getAllTablePartitionsByDbNameGroupId("test_schema", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbNamePartGroupIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbNamePartGroupId("test_schema", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbNameTbNameErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbNameTbName("test_schema", "test_table", false),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbNameTbNamePtNameErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbNameTbNamePtName("test_schema", "test_table", "p1"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testUpdateGroupIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateGroupId(1L, 2L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdateGroupIdByIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateGroupIdById(1L, 2L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdateGroupIdAndPartNameByIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateGroupIdAndPartNameById(1L, "p1", 2L),
                "update", GmsSystemTables.TABLE_PARTITIONS);
        }
    }

    @Test
    public void testUpdateStatusForPartitionedTableErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateStatusForPartitionedTable("test_schema", "test_table", 1),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdatePartBoundDescForOnePartitionErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updatePartBoundDescForOnePartition("test_schema", "test_table", "p1", "desc"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdatePartitionNameByGroupIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updatePartitionNameByGroupId(1L, "p1", "p2"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdateVersionErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateVersion("test_schema", "test_table", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testAlterNameAndTypeErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.alterNameAndType("test_schema", "test_table", "new_table", 0),
                "update", "table_partitions");
        }
    }

    @Test
    public void testAddColumnForPartExprAndPartDescErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.addColumnForPartExprAndPartDesc("test_schema", "test_table",
                    Collections.singletonList("col1")),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdatePartExprErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.emptyList());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updatePartExpr("test_schema", "test_table", "new_expr"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdateTablePartitionsGroupInfoErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        List<TablePartitionRecord> partitionRecords = Collections.singletonList(new TablePartitionRecord());
        Map<String, List<TablePartitionRecord>> subPartitionInfos = new HashMap<>();

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenReturn(Collections.singletonList(new TablePartitionRecord()));
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateTablePartitionsGroupInfo("test_schema", "test_table",
                    new TablePartitionRecord(), partitionRecords, subPartitionInfos),
                "update", "table_partitions");
        }
    }

    @Test
    public void testUpdateTablePartitionsPartFlagErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateTablePartitionsPartFlag("test_schema", "test_table", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDecreaseRefCountBySchTgidFromDeltaTableErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.decreaseRefCountBySchTgidFromDeltaTable("test_schema", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDecreaseRefCountBySchTbFromDeltaTableErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.decreaseRefCountBySchTbFromDeltaTable("test_schema", "test_table"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testIncreaseRefCountBySchTbFromDeltaTableErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.increaseRefCountBySchTbFromDeltaTable("test_schema", "test_table"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDeleteTablePartitionConfigsErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deleteTablePartitionConfigs("test_schema", "test_table"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDeleteTablePartitionConfigsForDeltaTableErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deleteTablePartitionConfigsForDeltaTable("test_schema", "test_table"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDeleteTablePartitionsErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deleteTablePartitions("test_schema", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDeleteTablePartitionsByIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deleteTablePartitionsById(1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDeletePartitionConfigsErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deletePartitionConfigs("test_schema", "test_table", "p1"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDeletePartitionConfigsFromDeltaErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deletePartitionConfigsFromDelta("test_schema", "test_table", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testAddNewTablePartitionConfigsErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        TablePartitionRecord logicalTableRecord = new TablePartitionRecord();
        logicalTableRecord.tableSchema = "test_schema";
        logicalTableRecord.tableName = "test_table";

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.addNewTablePartitionConfigs(logicalTableRecord, new ArrayList<>(),
                    new HashMap<>(), false, false),
                "query", "table_partitions");
        }
    }

    @Test
    public void testUpdateTablePartitionConfig4RepartitionOptimizeErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updateTablePartitionConfig4RepartitionOptimize(new TablePartitionRecord(),
                    new ArrayList<>(), "test_table"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testAddNewTablePartitionsInfoErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.addNewTablePartitionsInfo(Collections.singletonList(new TablePartitionRecord()),
                    false, false),
                "query", "table_partitions");
        }
    }

    @Test
    public void testAddNewTablePartitionsWithIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), anyList(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.addNewTablePartitionsWithId(Collections.singletonList(new TablePartitionRecord())),
                "query", "table_partitions");
        }
    }

    @Test
    public void testAddNewTablePartitionsArchiveErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.addNewTablePartitionsArchive("test_schema", "test_table", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testRenameErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.rename("test_schema", "test_table", "new_table"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testRenamePhyTableNameErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.renamePhyTableName(1L, "new_phy_table"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionByGidAndPartNameFromDeltaErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionByGidAndPartNameFromDelta("test_schema", "test_table", 1L, "p1"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDeleteTablePartitionByGidAndPartNameFromDeltaErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deleteTablePartitionByGidAndPartNameFromDelta("test_schema", "test_table", 1L, "p1"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetTablePartitionsByDbNameIdErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getTablePartitionsByDbNameId("test_schema", 1L),
                "query", "table_partitions");
        }
    }

    @Test
    public void testGetFirstLevelPartitionsBySchTgIdNameErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.getFirstLevelPartitionsBySchTgIdName("test_schema", 1L, "p1"),
                "query", "table_partitions");
        }
    }

    @Test
    public void testDisableStatusBySchGidL2ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.disableStatusBySchGidL2("test_schema", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDisableStatusBySchTbGidL2ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.disableStatusBySchTbGidL2("test_schema", "test_table", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDisableStatusBySchTempPartL2ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.disableStatusBySchTempPartL2("test_schema", "test_table", "tp1"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDisableStatusBySchPartL1ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.disableStatusBySchPartL1("test_schema", "test_table", "p1"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDeletePartitionBySchGidL2ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deletePartitionBySchGidL2("test_schema", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDeletePartitionBySchTbGidL2ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deletePartitionBySchTbGidL2("test_schema", "test_table", 1L),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDeletePartitionBySchTempPartL2ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deletePartitionBySchTempPartL2("test_schema", "test_table", "tp1"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDeletePartitionBySchPartL1ErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deletePartitionBySchPartL1("test_schema", "test_table", "p1"),
                "update", "table_partitions");
        }
    }

    @Test
    public void testDeleteTablePartitionsArchiveErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.deleteTablePartitionsArchive(60L),
                "update", GmsSystemTables.TABLE_PARTITIONS_ARCHIVE);
        }
    }

    @Test
    public void testUpdatePartFlagsBySchTbErrorMessageIsFullyFormatted() throws Exception {
        TablePartitionAccessor accessor = new TablePartitionAccessor();
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new SQLException("connection refused by metadb"));
            assertThrowsGmsAccessException(
                () -> accessor.updatePartFlagsBySchTb("test_schema", "test_table", 1L),
                "update", GmsSystemTables.TABLE_PARTITIONS);
        }
    }
}

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.FetchIndexUsageSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLogicalIndexUsage;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for InformationSchemaLogicalIndexUsageHandler.
 * <p>
 * Coverage targets:
 * - PhysicalNameExtractor.extractLogicalSchemaName: all 3 patterns
 * - PhysicalNameExtractor.extractLogicalTableName: all 4 digit lengths + edge cases
 * - handle(): aggregation, GSI identification, PARTITION_COUNT/AVG_USAGE, filters
 */
public class InformationSchemaLogicalIndexUsageHandlerTest {

    // ==================== PhysicalNameExtractor.extractLogicalSchemaName ====================

    @Test
    public void testExtractLogicalSchemaName_Null() {
        assertNull(PhysicalNameExtractor.extractLogicalSchemaName(null));
    }

    @Test
    public void testExtractLogicalSchemaName_PartitionPattern() {
        // Pattern 1: ends with _p##### -> remove last 7 chars
        assertEquals("dxlauto", PhysicalNameExtractor.extractLogicalSchemaName("dxlauto_p00000"));
        assertEquals("mydb", PhysicalNameExtractor.extractLogicalSchemaName("mydb_p12345"));
        assertEquals("test_schema", PhysicalNameExtractor.extractLogicalSchemaName("test_schema_p99999"));
    }

    @Test
    public void testExtractLogicalSchemaName_NonPartitionPattern() {
        // Pattern 2: ends with _###### -> remove last 7 chars
        assertEquals("slt", PhysicalNameExtractor.extractLogicalSchemaName("slt_000003"));
        assertEquals("mydb", PhysicalNameExtractor.extractLogicalSchemaName("mydb_123456"));
        assertEquals("test_db", PhysicalNameExtractor.extractLogicalSchemaName("test_db_000000"));
    }

    @Test
    public void testExtractLogicalSchemaName_AsIs() {
        // Neither pattern matches -> returned unchanged
        assertEquals("plain_schema", PhysicalNameExtractor.extractLogicalSchemaName("plain_schema"));
        assertEquals("db_12", PhysicalNameExtractor.extractLogicalSchemaName("db_12"));
        assertEquals("test", PhysicalNameExtractor.extractLogicalSchemaName("test"));
        assertEquals("db_12345", PhysicalNameExtractor.extractLogicalSchemaName("db_12345"));
    }

    // ==================== PhysicalNameExtractor.extractLogicalTableName ====================

    @Test
    public void testExtractLogicalTableName_Null() {
        assertNull(PhysicalNameExtractor.extractLogicalTableName(null));
    }

    @Test
    public void testExtractLogicalTableName_FiveDigits_WithRandomSuffix() {
        assertEquals("orders_log", PhysicalNameExtractor.extractLogicalTableName("orders_log_v3gg_00001"));
        assertEquals("orders_log", PhysicalNameExtractor.extractLogicalTableName("orders_log_v3gg_00099"));
        assertEquals("my_table", PhysicalNameExtractor.extractLogicalTableName("my_table_xean_00128"));
    }

    @Test
    public void testExtractLogicalTableName_FiveDigits_NoRandomSuffix() {
        // "orders_00001" -> strip _00001 -> "orders", no underscore left -> "orders"
        assertEquals("orders", PhysicalNameExtractor.extractLogicalTableName("orders_00001"));
    }

    @Test
    public void testExtractLogicalTableName_FiveDigits_LongSuffixNotRandom() {
        // Suffix after last '_' is >5 chars -> NOT treated as random suffix
        assertEquals("my_table_longtime", PhysicalNameExtractor.extractLogicalTableName("my_table_longtime_00001"));
    }

    @Test
    public void testExtractLogicalTableName_FourDigits() {
        assertEquals("table", PhysicalNameExtractor.extractLogicalTableName("table_abc_1234"));
        assertEquals("my_tbl", PhysicalNameExtractor.extractLogicalTableName("my_tbl_wxyz_9999"));
    }

    @Test
    public void testExtractLogicalTableName_ThreeDigits() {
        assertEquals("tab", PhysicalNameExtractor.extractLogicalTableName("tab_xyz_123"));
        assertEquals("order_tbl", PhysicalNameExtractor.extractLogicalTableName("order_tbl_abc_099"));
    }

    @Test
    public void testExtractLogicalTableName_TwoDigits() {
        assertEquals("orderby_nosort_100_2_tab1",
            PhysicalNameExtractor.extractLogicalTableName("orderby_nosort_100_2_tab1_0vgk_15"));
        assertEquals("t1", PhysicalNameExtractor.extractLogicalTableName("t1_ab_99"));
    }

    @Test
    public void testExtractLogicalTableName_AsIs() {
        assertEquals("plain_table", PhysicalNameExtractor.extractLogicalTableName("plain_table"));
        assertEquals("table1", PhysicalNameExtractor.extractLogicalTableName("table1"));
    }

    // ==================== handle() — basic aggregation ====================

    @Test
    public void testHandle_EmptyResult() {
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor();

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            assertTrue(output.getRows().isEmpty());
        }
    }

    @Test
    public void testHandle_BasicAggregation_MultiplePhysicalToOneLogical() {
        // Two physical shards -> aggregated into one logical row
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"mydb_p00001", "orders_v3gg_00002", "idx_user", 200L, 20L, 2000L, 800L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("mydb", row.getString(0));              // LOGICAL_SCHEMA
            assertEquals("orders", row.getString(1));            // LOGICAL_TABLE
            assertEquals("idx_user", row.getString(3));          // INDEX_NAME
            assertEquals(Long.valueOf(300L), row.getObject(4));  // TOTAL_USAGE = 100+200
            assertEquals(Long.valueOf(30L), row.getObject(7));   // COUNT_FETCH = 10+20
            assertEquals(Long.valueOf(3000L), row.getObject(8)); // SUM_TIMER_WAIT = 1000+2000
            assertEquals(Long.valueOf(800L), row.getObject(9));  // MAX_TIMER_WAIT = max(500,800)
        }
    }

    @Test
    public void testHandle_MultipleIndexesSameTable() {
        // Two different indexes on the same logical table -> two output rows
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_order", 50L, 5L, 500L, 300L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            assertEquals(2, output.getRows().size());
        }
    }

    @Test
    public void testHandle_NullCountStar_TreatedAsZero() {
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", null, null, null, null}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(0L), rows.get(0).getObject(4)); // TOTAL_USAGE = 0
            assertEquals(Long.valueOf(0L), rows.get(0).getObject(6)); // AVG_USAGE = 0
        }
    }

    // ==================== handle() — filter tests ====================

    @Test
    public void testHandle_FilterBySchemaName() {
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"other_p00000", "users_abc_00001", "idx_email", 50L, 5L, 500L, 300L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(Collections.singleton("mydb"), null, null),
                buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            assertEquals("mydb", rows.get(0).getString(0));
        }
    }

    @Test
    public void testHandle_FilterByTableName() {
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"mydb_p00000", "users_abc_00001", "idx_email", 50L, 5L, 500L, 300L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, Collections.singleton("orders"), null),
                buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            assertEquals("orders", rows.get(0).getString(1));
        }
    }

    @Test
    public void testHandle_FilterByIndexName_InjectedIntoSql() {
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L}
        );

        Set<String> indexFilter = new HashSet<>(Arrays.asList("idx_user", "idx_order"));

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class, (mock, ctx) -> {
                    String constructedSql = (String) ctx.arguments().get(1);
                    assertTrue("SQL should contain INDEX_NAME IN clause",
                        constructedSql.contains("AND INDEX_NAME IN"));
                    assertTrue("SQL should contain idx_user", constructedSql.contains("idx_user"));
                    assertTrue("SQL should contain idx_order", constructedSql.contains("idx_order"));
                    when(mock.sync()).thenReturn(syncCursor);
                })) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, indexFilter), buildMockCtx("mydb"), output);
        }
    }

    // ==================== handle() — GSI tests ====================

    @Test
    public void testHandle_GsiTableIndex_FillsGsiName() {
        // physical table "orders_gsi_user_id_v3gg_00001" -> logical "orders_gsi_user_id"
        // GSI mapping: "orders_gsi_user_id" -> parent "orders"
        InformationSchemaLogicalIndexUsageHandler handler =
            spy(new InformationSchemaLogicalIndexUsageHandler(null));

        Map<String, String> gsiMap = Collections.singletonMap("orders_gsi_user_id", "orders");
        doReturn(gsiMap).when(handler).buildGsiMapping("mydb");

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_gsi_user_id_v3gg_00001", "idx_status", 150L, 15L, 1500L, 700L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("mydb", row.getString(0));                 // LOGICAL_SCHEMA
            assertEquals("orders", row.getString(1));               // LOGICAL_TABLE = parent table
            assertEquals("orders_gsi_user_id", row.getString(2));   // GSI_NAME = GSI table
            assertEquals("idx_status", row.getString(3));           // INDEX_NAME
            assertEquals(Long.valueOf(150L), row.getObject(4));     // TOTAL_USAGE
            assertEquals(Long.valueOf(1L), row.getObject(5));       // PARTITION_COUNT
            assertEquals(Long.valueOf(150L), row.getObject(6));     // AVG_USAGE = 150/1
        }
    }

    @Test
    public void testHandle_GsiTableMultipleShards_AggregatesUnderParent() {
        // Two physical shards of the same GSI -> aggregated under parent table
        InformationSchemaLogicalIndexUsageHandler handler =
            spy(new InformationSchemaLogicalIndexUsageHandler(null));

        Map<String, String> gsiMap = Collections.singletonMap("orders_gsi_user_id", "orders");
        doReturn(gsiMap).when(handler).buildGsiMapping("mydb");

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_gsi_user_id_v3gg_00001", "idx_status", 100L, 10L, 1000L, 500L},
            new Object[] {"mydb_p00001", "orders_gsi_user_id_v3gg_00002", "idx_status", 200L, 20L, 2000L, 800L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("orders", row.getString(1));               // parent table
            assertEquals("orders_gsi_user_id", row.getString(2));   // GSI name
            assertEquals(Long.valueOf(300L), row.getObject(4));     // TOTAL_USAGE
            assertEquals(Long.valueOf(2L), row.getObject(5));       // PARTITION_COUNT
            assertEquals(Long.valueOf(150L), row.getObject(6));     // AVG_USAGE = 300/2
            assertEquals(Long.valueOf(30L), row.getObject(7));      // COUNT_FETCH = 10+20
            assertEquals(Long.valueOf(3000L), row.getObject(8));    // SUM_TIMER_WAIT = 1000+2000
            assertEquals(Long.valueOf(800L), row.getObject(9));     // MAX_TIMER_WAIT = max(500,800)
        }
    }

    @Test
    public void testHandle_GsiAndPrimaryTableSameIndex_AggregatedSeparately() {
        // Primary table "orders" and its GSI "orders_gsi_user_id" both have "idx_user"
        // -> two separate output rows (different gsiName in key)
        InformationSchemaLogicalIndexUsageHandler handler =
            spy(new InformationSchemaLogicalIndexUsageHandler(null));

        Map<String, String> gsiMap = Collections.singletonMap("orders_gsi_user_id", "orders");
        doReturn(gsiMap).when(handler).buildGsiMapping("mydb");

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            // Primary table shard
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 500L, 50L, 5000L, 900L},
            // GSI table shard
            new Object[] {"mydb_p00000", "orders_gsi_user_id_v3gg_00001", "idx_user", 300L, 30L, 3000L, 700L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            // Two rows: one for primary (gsiName=""), one for GSI (gsiName="orders_gsi_user_id")
            List<Row> rows = output.getRows();
            assertEquals(2, rows.size());

            // Verify both have OBJECT_NAME = "orders" but different GSI_NAME
            Set<String> gsiNames = new HashSet<>();
            for (Row r : rows) {
                assertEquals("orders", r.getString(1));
                gsiNames.add(r.getString(2));
            }
            assertTrue(gsiNames.contains(""));
            assertTrue(gsiNames.contains("orders_gsi_user_id"));
        }
    }

    @Test
    public void testHandle_GsiMappingUnavailable_GracefulDegradation() {
        // When OptimizerContext returns null (typical in unit tests), buildGsiMapping
        // returns an empty map and all rows have empty GSI_NAME — same as before
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_gsi_user_id_v3gg_00001", "idx_status", 100L, 10L, 1000L, 500L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            // OptimizerContext is null (default in test env), so buildGsiMapping returns {}
            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            // GSI_NAME should be empty (graceful degradation)
            assertEquals("", rows.get(0).getString(2));
        }
    }

    @Test
    public void testHandle_FilterByTableName_ExcludesGsiParent() {
        // Filter on OBJECT_NAME="orders" should include GSI rows whose parent is "orders"
        InformationSchemaLogicalIndexUsageHandler handler =
            spy(new InformationSchemaLogicalIndexUsageHandler(null));

        Map<String, String> gsiMap = Collections.singletonMap("orders_gsi_user_id", "orders");
        doReturn(gsiMap).when(handler).buildGsiMapping("mydb");

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_gsi_user_id_v3gg_00001", "idx_status", 50L, 5L, 500L, 300L},
            new Object[] {"mydb_p00000", "users_v3gg_00001", "idx_name", 80L, 8L, 800L, 400L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, Collections.singleton("orders"), null),
                buildMockCtx("mydb"), output);

            // Only the GSI row mapped to parent "orders" should pass
            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            assertEquals("orders", rows.get(0).getString(1));
            assertEquals("orders_gsi_user_id", rows.get(0).getString(2));
        }
    }

    // ==================== Helpers ====================

    private ArrayResultCursor buildSyncResultCursor(Object[]... rows) {
        ArrayResultCursor cursor = new ArrayResultCursor("index_usage");
        cursor.addColumn("OBJECT_SCHEMA", DataTypes.StringType);
        cursor.addColumn("OBJECT_NAME", DataTypes.StringType);
        cursor.addColumn("INDEX_NAME", DataTypes.StringType);
        cursor.addColumn("COUNT_STAR", DataTypes.LongType);
        cursor.addColumn("COUNT_FETCH", DataTypes.LongType);
        cursor.addColumn("SUM_TIMER_WAIT", DataTypes.LongType);
        cursor.addColumn("MAX_TIMER_WAIT", DataTypes.LongType);
        cursor.initMeta();
        for (Object[] row : rows) {
            cursor.addRow(row);
        }
        return cursor;
    }

    /**
     * Output cursor mirrors the full 10-column schema of logical_index_usage:
     * LOGICAL_SCHEMA, LOGICAL_TABLE, GSI_NAME, INDEX_NAME, TOTAL_USAGE, PARTITION_COUNT, AVG_USAGE,
     * COUNT_FETCH, SUM_TIMER_WAIT, MAX_TIMER_WAIT
     */
    private ArrayResultCursor buildOutputCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("logical_index_usage");
        cursor.addColumn("LOGICAL_SCHEMA", DataTypes.StringType);
        cursor.addColumn("LOGICAL_TABLE", DataTypes.StringType);
        cursor.addColumn("GSI_NAME", DataTypes.StringType);
        cursor.addColumn("INDEX_NAME", DataTypes.StringType);
        cursor.addColumn("TOTAL_USAGE", DataTypes.LongType);
        cursor.addColumn("PARTITION_COUNT", DataTypes.LongType);
        cursor.addColumn("AVG_USAGE", DataTypes.LongType);
        cursor.addColumn("COUNT_FETCH", DataTypes.LongType);
        cursor.addColumn("SUM_TIMER_WAIT", DataTypes.LongType);
        cursor.addColumn("MAX_TIMER_WAIT", DataTypes.LongType);
        cursor.initMeta();
        return cursor;
    }

    /**
     * Builds a mock VirtualView with optional equality filters.
     * Column indices: 0=LOGICAL_SCHEMA, 1=LOGICAL_TABLE, 3=INDEX_NAME
     */
    private InformationSchemaLogicalIndexUsage buildMockView(Set<String> schemaFilter,
                                                             Set<String> tableFilter,
                                                             Set<String> indexFilter) {
        InformationSchemaLogicalIndexUsage mockView =
            mock(InformationSchemaLogicalIndexUsage.class);
        when(mockView.getEqualsFilterValues(anyInt(), any())).thenReturn(null);

        if (schemaFilter != null) {
            when(mockView.getEqualsFilterValues(
                eq(InformationSchemaLogicalIndexUsage.getObjectSchemaIndex()), any()))
                .thenReturn(schemaFilter);
        }
        if (tableFilter != null) {
            when(mockView.getEqualsFilterValues(
                eq(InformationSchemaLogicalIndexUsage.getObjectNameIndex()), any()))
                .thenReturn(tableFilter);
        }
        if (indexFilter != null) {
            when(mockView.getEqualsFilterValues(
                eq(InformationSchemaLogicalIndexUsage.getIndexNameIndex()), any()))
                .thenReturn(indexFilter);
        }
        return mockView;
    }

    private ExecutionContext buildMockCtx(String schemaName) {
        ExecutionContext mockCtx = mock(ExecutionContext.class);
        Parameters mockParams = mock(Parameters.class);
        when(mockCtx.getParams()).thenReturn(mockParams);
        when(mockCtx.getSchemaName()).thenReturn(schemaName);
        when(mockParams.getCurrentParameter()).thenReturn(Collections.emptyMap());
        return mockCtx;
    }

    private PhysicalToLogicalTableMapping buildTestMapping() {
        Map<String, String> tableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMap.put("orders_v3gg_00001", "orders");
        tableMap.put("orders_v3gg_00002", "orders");
        tableMap.put("users_abc_00001", "users");
        tableMap.put("orders_gsi_user_id_v3gg_00001", "orders_gsi_user_id");
        tableMap.put("orders_gsi_user_id_v3gg_00002", "orders_gsi_user_id");
        tableMap.put("users_v3gg_00001", "users");

        Map<String, String> schemaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        schemaMap.put("mydb_p00000", "mydb");
        schemaMap.put("mydb_p00001", "mydb");
        schemaMap.put("other_p00000", "other");

        return new PhysicalToLogicalTableMapping(tableMap, schemaMap);
    }
}

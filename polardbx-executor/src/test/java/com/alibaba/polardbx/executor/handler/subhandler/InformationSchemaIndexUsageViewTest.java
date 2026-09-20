package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.FetchIndexUsageSyncAction;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLogicalIndexUsage;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPhysicalIndexUsage;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for InformationSchemaLogicalIndexUsage and
 * InformationSchemaPhysicalIndexUsage (View + Handler).
 */
public class InformationSchemaIndexUsageViewTest {

    private RelOptCluster cluster;

    @Before
    public void setUp() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCostFactory costFactory = DrdsRelOptCostImpl.FACTORY;
        RelOptPlanner planner = new VolcanoPlanner(costFactory, PlannerContext.EMPTY_CONTEXT);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(DrdsRelMetadataProvider.INSTANCE);
    }

    // ================================================================
    //  View schema tests
    // ================================================================

    @Test
    public void testLogicalView_SchemaDefinition() {
        VirtualView view = VirtualView.create(cluster, VirtualViewType.LOGICAL_INDEX_USAGE);
        assertNotNull(view);
        assertTrue(view instanceof InformationSchemaLogicalIndexUsage);

        List<RelDataTypeField> fields = view.getRowType().getFieldList();
        assertEquals(10, fields.size());

        String[] expectedNames = {
            "LOGICAL_SCHEMA", "LOGICAL_TABLE", "GSI_NAME", "INDEX_NAME",
            "TOTAL_USAGE", "PARTITION_COUNT", "AVG_USAGE", "COUNT_FETCH",
            "SUM_TIMER_WAIT", "MAX_TIMER_WAIT"
        };
        SqlTypeName[] expectedTypes = {
            SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
            SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
            SqlTypeName.BIGINT, SqlTypeName.BIGINT
        };
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedNames[i], fields.get(i).getName());
            assertEquals(expectedTypes[i], fields.get(i).getType().getSqlTypeName());
        }
    }

    @Test
    public void testPhysicalView_SchemaDefinition() {
        VirtualView view = VirtualView.create(cluster, VirtualViewType.PHYSICAL_INDEX_USAGE);
        assertNotNull(view);
        assertTrue(view instanceof InformationSchemaPhysicalIndexUsage);

        List<RelDataTypeField> fields = view.getRowType().getFieldList();
        assertEquals(9, fields.size());

        String[] expectedNames = {
            "OBJECT_SCHEMA", "OBJECT_NAME", "INDEX_NAME", "COUNT_STAR",
            "LOGICAL_SCHEMA", "LOGICAL_TABLE", "COUNT_FETCH",
            "SUM_TIMER_WAIT", "MAX_TIMER_WAIT"
        };
        SqlTypeName[] expectedTypes = {
            SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.BIGINT,
            SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.BIGINT,
            SqlTypeName.BIGINT, SqlTypeName.BIGINT
        };
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedNames[i], fields.get(i).getName());
            assertEquals(expectedTypes[i], fields.get(i).getType().getSqlTypeName());
        }
    }

    // ================================================================
    //  LogicalIndexUsageHandler — core logic
    // ================================================================

    @Test
    public void testLogicalHandler_Aggregation() {
        InformationSchemaLogicalIndexUsageHandler handler =
            new InformationSchemaLogicalIndexUsageHandler(null);

        // Two shards of the same table+index should be aggregated into one row
        ArrayResultCursor syncCursor = buildDnResultCursor(
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

            ArrayResultCursor output = buildLogicalOutputCursor();
            handler.handle(buildLogicalMockView(), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("mydb", row.getString(0));
            assertEquals("orders", row.getString(1));
            assertEquals(Long.valueOf(300L), row.getObject(4));  // TOTAL = 100+200
            assertEquals(Long.valueOf(2L), row.getObject(5));    // PARTITION_COUNT
            assertEquals(Long.valueOf(150L), row.getObject(6));  // AVG = 300/2
            assertEquals(Long.valueOf(30L), row.getObject(7));   // COUNT_FETCH = 10+20
            assertEquals(Long.valueOf(3000L), row.getObject(8)); // SUM_TIMER_WAIT
            assertEquals(Long.valueOf(800L), row.getObject(9));  // MAX_TIMER_WAIT
        }
    }

    @Test
    public void testLogicalHandler_GsiIdentification() {
        InformationSchemaLogicalIndexUsageHandler handler =
            spy(new InformationSchemaLogicalIndexUsageHandler(null));

        Map<String, String> gsiMap = Collections.singletonMap("orders_gsi_user_id", "orders");
        doReturn(gsiMap).when(handler).buildGsiMapping("mydb");

        // GSI physical table should map to parent table with GSI_NAME filled
        ArrayResultCursor syncCursor = buildDnResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 500L, 50L, 5000L, 900L},
            new Object[] {"mydb_p00000", "orders_gsi_user_id_v3gg_00001", "idx_user", 300L, 30L, 3000L, 700L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildLogicalOutputCursor();
            handler.handle(buildLogicalMockView(), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(2, rows.size());

            // Find GSI row and primary row
            Row gsiRow = null, primaryRow = null;
            for (Row r : rows) {
                if ("orders_gsi_user_id".equals(r.getString(2))) {
                    gsiRow = r;
                } else {
                    primaryRow = r;
                }
            }
            assertNotNull(gsiRow);
            assertNotNull(primaryRow);
            assertEquals("orders", gsiRow.getString(1));  // parent table
            assertEquals("orders", primaryRow.getString(1));
            assertEquals("", primaryRow.getString(2));    // empty GSI_NAME for primary
        }
    }

    // ================================================================
    //  PhysicalIndexUsageHandler — core logic
    // ================================================================

    @Test
    public void testPhysicalHandler_LogicalColumnMapping() {
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildDnResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildPhysicalOutputCursor();
            handler.handle(buildPhysicalMockView(), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("mydb_p00000", row.getString(0));       // OBJECT_SCHEMA
            assertEquals("orders_v3gg_00001", row.getString(1)); // OBJECT_NAME
            assertEquals("idx_user", row.getString(2));          // INDEX_NAME
            assertEquals(Long.valueOf(100L), row.getObject(3));  // COUNT_STAR
            assertEquals("mydb", row.getString(4));              // LOGICAL_SCHEMA
            assertEquals("orders", row.getString(5));            // LOGICAL_TABLE
        }
    }

    // ================================================================
    //  PhysicalNameExtractor
    // ================================================================

    @Test
    public void testPhysicalNameExtractor() {
        // Schema: _p##### pattern
        assertEquals("dxlauto", PhysicalNameExtractor.extractLogicalSchemaName("dxlauto_p00000"));
        // Schema: _###### pattern
        assertEquals("slt", PhysicalNameExtractor.extractLogicalSchemaName("slt_000003"));
        // Schema: no match
        assertEquals("plain", PhysicalNameExtractor.extractLogicalSchemaName("plain"));
        assertEquals(null, PhysicalNameExtractor.extractLogicalSchemaName(null));

        // Table: 5-digit with random suffix
        assertEquals("orders_log", PhysicalNameExtractor.extractLogicalTableName("orders_log_v3gg_00001"));
        // Table: 5-digit without random suffix
        assertEquals("orders", PhysicalNameExtractor.extractLogicalTableName("orders_00001"));
        // Table: no match
        assertEquals("plain_table", PhysicalNameExtractor.extractLogicalTableName("plain_table"));
        assertEquals(null, PhysicalNameExtractor.extractLogicalTableName(null));
    }

    // ================================================================
    //  Helpers
    // ================================================================

    private ArrayResultCursor buildDnResultCursor(Object[]... rows) {
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

    private ArrayResultCursor buildLogicalOutputCursor() {
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

    private ArrayResultCursor buildPhysicalOutputCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("physical_index_usage");
        cursor.addColumn("OBJECT_SCHEMA", DataTypes.StringType);
        cursor.addColumn("OBJECT_NAME", DataTypes.StringType);
        cursor.addColumn("INDEX_NAME", DataTypes.StringType);
        cursor.addColumn("COUNT_STAR", DataTypes.LongType);
        cursor.addColumn("LOGICAL_SCHEMA", DataTypes.StringType);
        cursor.addColumn("LOGICAL_TABLE", DataTypes.StringType);
        cursor.addColumn("COUNT_FETCH", DataTypes.LongType);
        cursor.addColumn("SUM_TIMER_WAIT", DataTypes.LongType);
        cursor.addColumn("MAX_TIMER_WAIT", DataTypes.LongType);
        cursor.initMeta();
        return cursor;
    }

    private PhysicalToLogicalTableMapping buildTestMapping() {
        Map<String, String> tableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMap.put("orders_v3gg_00001", "orders");
        tableMap.put("orders_v3gg_00002", "orders");
        tableMap.put("users_abc_00001", "users");
        tableMap.put("orders_gsi_user_id_v3gg_00001", "orders_gsi_user_id");
        tableMap.put("orders_gsi_user_id_v3gg_00002", "orders_gsi_user_id");

        Map<String, String> schemaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        schemaMap.put("mydb_p00000", "mydb");
        schemaMap.put("mydb_p00001", "mydb");
        schemaMap.put("other_p00000", "other");

        return new PhysicalToLogicalTableMapping(tableMap, schemaMap);
    }

    private InformationSchemaLogicalIndexUsage buildLogicalMockView() {
        InformationSchemaLogicalIndexUsage mockView = mock(InformationSchemaLogicalIndexUsage.class);
        when(mockView.getEqualsFilterValues(anyInt(), any())).thenReturn(null);
        return mockView;
    }

    private InformationSchemaPhysicalIndexUsage buildPhysicalMockView() {
        InformationSchemaPhysicalIndexUsage mockView = mock(InformationSchemaPhysicalIndexUsage.class);
        when(mockView.getEqualsFilterValues(anyInt(), any())).thenReturn(null);
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
}

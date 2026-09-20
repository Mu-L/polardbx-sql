package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.when;

public class PhysicalPlanBuilderTest {

    private ExecutionContext executionContext;
    private TableMeta tableMeta;
    private SchemaManager schemaManager;

    @Before
    public void setUp() {
        // Mock ExecutionContext
        executionContext = Mockito.mock(ExecutionContext.class);
        when(executionContext.getExtraCmds()).thenReturn(new HashMap<>());

        schemaManager = Mockito.mock(SchemaManager.class);
        when(executionContext.getSchemaManager("test_schema")).thenReturn(schemaManager);

        // Mock TableMeta
        tableMeta = Mockito.mock(TableMeta.class);
        when(tableMeta.getTableName()).thenReturn("test_table");
        when(tableMeta.getSchemaName()).thenReturn("test_schema");
    }

    @Test
    public void testBuildDeleteForChangeSetWithForceIndex() {
        // Prepare primary keys
        List<String> primaryKeys = Arrays.asList("id", "name");

        // Create PhysicalPlanBuilder
        PhysicalPlanBuilder builder = new PhysicalPlanBuilder("test_schema", executionContext);

        // Call the method under test
        Pair<SqlDelete, PhyTableOperation> result =
            builder.buildDeleteForChangeSet(tableMeta, primaryKeys);

        // Verify the result
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getKey()); // SqlDelete
        Assert.assertNotNull(result.getValue()); // PhyTableOperation

        SqlDelete sqlDelete = result.getKey();

        String sql = sqlDelete.toString();

        Assert.assertEquals(sql, "DELETE `tb` FROM ? AS `tb` FORCE INDEX(PRIMARY)\n"
            + "WHERE (((`id`, `name`)) IN ())");
    }

    @Test
    public void testTableNameEscapingInBuildSelectUnionAndParam() {
        // Test cases for table name escaping
        // Case 1: Simple table name without backticks
        String simpleTableName = "my_table";
        String escapedSimpleTableName = PlannerUtils.buildTableNameParamForXDriver(simpleTableName);
        Assert.assertEquals("`my_table`", escapedSimpleTableName);

        // Case 2: Table name with backticks that need to be doubled
        String tableNameWithBackticks = "my`table";
        String escapedTableNameWithBackticks = PlannerUtils.buildTableNameParamForXDriver(tableNameWithBackticks);
        Assert.assertEquals("`my``table`", escapedTableNameWithBackticks);

        // Case 3: Table name with schema
        String tableNameWithSchema = "schema.my_table";
        String escapedTableNameWithSchema = PlannerUtils.buildTableNameParamForXDriver(tableNameWithSchema);
        // Note: The exact format depends on whether schema exists, but it should contain escaped backticks
        Assert.assertTrue(escapedTableNameWithSchema.contains("``") || escapedTableNameWithSchema.startsWith("`"));

        // Case 4: Complex table name with multiple backticks
        String complexTableName = "table`with`multiple`backticks";
        String escapedComplexTableName = PlannerUtils.buildTableNameParamForXDriver(complexTableName);
        Assert.assertEquals("`table``with``multiple``backticks`", escapedComplexTableName);

        // Case 5: Table name with consecutive backticks
        String tableNameWithConsecutiveBackticks = "table``consecutive";
        String escapedTableNameWithConsecutiveBackticks =
            PlannerUtils.buildTableNameParamForXDriver(tableNameWithConsecutiveBackticks);
        Assert.assertEquals("`table````consecutive`", escapedTableNameWithConsecutiveBackticks);

        // Case 6: Table name with many consecutive backticks
        String tableNameWithManyConsecutiveBackticks = "table``````many";
        String escapedTableNameWithManyConsecutiveBackticks =
            PlannerUtils.buildTableNameParamForXDriver(tableNameWithManyConsecutiveBackticks);
        Assert.assertEquals("`table````````````many`", escapedTableNameWithManyConsecutiveBackticks);

        // Case 7: Table name starting and ending with backticks
        String tableNameStartEndWithBackticks = "`starts_and_ends`";
        String escapedTableNameStartEndWithBackticks =
            PlannerUtils.buildTableNameParamForXDriver(tableNameStartEndWithBackticks);
        Assert.assertEquals("```starts_and_ends```", escapedTableNameStartEndWithBackticks);
    }
}
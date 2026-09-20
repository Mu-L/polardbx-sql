package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for GsiColsReplaceRule#checkIndexScan method
 *
 * @author fangwu
 */
public class GsiColsReplaceRuleTest extends BaseRuleTest {

    /**
     * Test checkIndexScan with null mainTableName - should return false
     */
    @Test
    public void testCheckIndexScanWithNullMainTableName() {
        // Create a mock LogicalIndexScan with null mainTableName
        LogicalIndexScan mockIndexScan = mock(LogicalIndexScan.class);
        when(mockIndexScan.getMainTableName()).thenReturn(null);

        boolean result = GsiColsReplaceRule.checkIndexScan(mockIndexScan);
        assertFalse("Should return false when mainTableName is null", result);
    }

    /**
     * Test checkIndexScan with empty mainTableName - should return false
     */
    @Test
    public void testCheckIndexScanWithEmptyMainTableName() {
        LogicalIndexScan mockIndexScan = mock(LogicalIndexScan.class);
        when(mockIndexScan.getMainTableName()).thenReturn("");

        boolean result = GsiColsReplaceRule.checkIndexScan(mockIndexScan);
        assertFalse("Should return false when mainTableName is empty", result);
    }

    /**
     * Test checkIndexScan with multiple table names - should return false
     */
    @Test
    public void testCheckIndexScanWithMultipleTableNames() {
        LogicalIndexScan mockIndexScan = mock(LogicalIndexScan.class);
        when(mockIndexScan.getMainTableName()).thenReturn("test_table");
        when(mockIndexScan.getTableNames()).thenReturn(Arrays.asList("table1", "table2"));

        boolean result = GsiColsReplaceRule.checkIndexScan(mockIndexScan);
        assertFalse("Should return false when tableNames size is not 1", result);
    }

    /**
     * Test checkIndexScan with valid Project-Filter-Scan structure - should return true
     */
    @Test
    public void testCheckIndexScanWithValidProjectFilterScan() {
        // Create real objects for this test
        LogicalTableScan tableScan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        // Create a filter on top of table scan
        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(10)));
        LogicalFilter filter = LogicalFilter.create(tableScan, filterCondition);

        // Create a project on top of filter
        LogicalProject project = LogicalProject.create(filter,
            Arrays.asList(rexBuilder.makeInputRef(filter, 0)),
            Arrays.asList("col1"));

        // Create LogicalView and LogicalIndexScan
        LogicalView logicalView = LogicalView.create(tableScan, tableScan.getTable());
        logicalView.push(filter);
        logicalView.push(project);

        LogicalIndexScan indexScan = new LogicalIndexScan("emp", filter, tableScan.getTable(), null, null, null, false);
        indexScan.getTableNames().clear();
        indexScan.getTableNames().add("emp");

        boolean result = GsiColsReplaceRule.checkIndexScan(indexScan);
        assertTrue("Should return true for valid Project-Filter-Scan structure", result);
    }

    /**
     * Test checkIndexScan with valid Filter-Scan structure - should return true
     */
    @Test
    public void testCheckIndexScanWithValidFilterScan() {
        // Create real objects for this test
        LogicalTableScan tableScan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        // Create a filter on top of table scan
        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(tableScan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(10)));
        LogicalFilter filter = LogicalFilter.create(tableScan, filterCondition);

        // Create LogicalView and LogicalIndexScan
        LogicalView logicalView = LogicalView.create(tableScan, tableScan.getTable());
        logicalView.push(filter);

        LogicalIndexScan indexScan = new LogicalIndexScan("emp", filter, tableScan.getTable(), null, null, null, false);
        indexScan.getTableNames().clear();
        indexScan.getTableNames().add("emp");

        boolean result = GsiColsReplaceRule.checkIndexScan(indexScan);
        assertTrue("Should return true for valid Filter-Scan structure", result);
    }

    /**
     * Test checkIndexScan with invalid structure (only TableScan) - should return false
     */
    @Test
    public void testCheckIndexScanWithInvalidStructure() {
        // Create real objects for this test
        LogicalTableScan tableScan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        // Create LogicalView and LogicalIndexScan with only TableScan (no Filter or Project)
        LogicalView logicalView = LogicalView.create(tableScan, tableScan.getTable());
        LogicalIndexScan indexScan =
            new LogicalIndexScan("emp", tableScan, tableScan.getTable(), null, null, null, false);
        indexScan.getTableNames().clear();
        indexScan.getTableNames().add("emp");

        boolean result = GsiColsReplaceRule.checkIndexScan(indexScan);
        assertFalse("Should return false for invalid structure (only TableScan)", result);
    }

    /**
     * Test checkIndexScan with Project-Scan structure (missing Filter) - should return false
     */
    @Test
    public void testCheckIndexScanWithProjectScanOnly() {
        // Create real objects for this test
        LogicalTableScan tableScan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        // Create a project directly on top of table scan (no filter)
        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        LogicalProject project = LogicalProject.create(tableScan,
            Arrays.asList(rexBuilder.makeInputRef(tableScan, 0)),
            Arrays.asList("col1"));

        // Create LogicalView and LogicalIndexScan
        LogicalView logicalView = LogicalView.create(tableScan, tableScan.getTable());
        logicalView.push(project);

        LogicalIndexScan indexScan =
            new LogicalIndexScan("emp", project, tableScan.getTable(), null, null, null, false);
        indexScan.getTableNames().clear();
        indexScan.getTableNames().add("emp");

        boolean result = GsiColsReplaceRule.checkIndexScan(indexScan);
        assertFalse("Should return false for Project-Scan structure without Filter", result);
    }

    /**
     * Test checkIndexScan with complex nested structure - should return false
     */
    @Test
    public void testCheckIndexScanWithComplexStructure() {
        LogicalIndexScan mockIndexScan = mock(LogicalIndexScan.class);
        when(mockIndexScan.getMainTableName()).thenReturn("test_table");
        when(mockIndexScan.getTableNames()).thenReturn(Arrays.asList("test_table"));

        // Mock a complex nested structure that doesn't match expected patterns
        RelNode mockComplexNode = mock(RelNode.class);
        when(mockIndexScan.getPushedRelNode()).thenReturn(mockComplexNode);

        boolean result = GsiColsReplaceRule.checkIndexScan(mockIndexScan);
        assertFalse("Should return false for complex structure that doesn't match expected patterns", result);
    }

    /**
     * Test checkIndexScan with Project-Filter-NonTableScan structure - should return false
     */
    @Test
    public void testCheckIndexScanWithProjectFilterNonTableScan() {
        LogicalIndexScan mockIndexScan = mock(LogicalIndexScan.class);
        when(mockIndexScan.getMainTableName()).thenReturn("test_table");
        when(mockIndexScan.getTableNames()).thenReturn(Arrays.asList("test_table"));

        // Mock Project-Filter-NonTableScan structure
        LogicalProject mockProject = mock(LogicalProject.class);
        LogicalFilter mockFilter = mock(LogicalFilter.class);
        RelNode mockNonTableScan = mock(RelNode.class); // Not a LogicalTableScan

        when(mockIndexScan.getPushedRelNode()).thenReturn(mockProject);
        when(mockProject.getInput()).thenReturn(mockFilter);
        when(mockFilter.getInput()).thenReturn(mockNonTableScan);

        boolean result = GsiColsReplaceRule.checkIndexScan(mockIndexScan);
        assertFalse("Should return false when filter input is not LogicalTableScan", result);
    }

    /**
     * Test checkIndexScan with Filter-NonTableScan structure - should return false
     */
    @Test
    public void testCheckIndexScanWithFilterNonTableScan() {
        LogicalIndexScan mockIndexScan = mock(LogicalIndexScan.class);
        when(mockIndexScan.getMainTableName()).thenReturn("test_table");
        when(mockIndexScan.getTableNames()).thenReturn(Arrays.asList("test_table"));

        // Mock Filter-NonTableScan structure
        LogicalFilter mockFilter = mock(LogicalFilter.class);
        RelNode mockNonTableScan = mock(RelNode.class); // Not a LogicalTableScan

        when(mockIndexScan.getPushedRelNode()).thenReturn(mockFilter);
        when(mockFilter.getInput()).thenReturn(mockNonTableScan);

        boolean result = GsiColsReplaceRule.checkIndexScan(mockIndexScan);
        assertFalse("Should return false when filter input is not LogicalTableScan", result);
    }

    /**
     * Test generateUniqueColumnName with base name that doesn't exist in existing names
     */
    @Test
    public void testGenerateUniqueColumnNameWithNonExistingBaseName() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col1");
        existingNames.add("col2");
        existingNames.add("col3");

        String result = GsiColsReplaceRule.generateUniqueColumnName("newCol", existingNames);
        assertEquals("Should return base name when it doesn't exist", "newCol", result);
    }

    /**
     * Test generateUniqueColumnName with base name that exists in existing names
     */
    @Test
    public void testGenerateUniqueColumnNameWithExistingBaseName() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col1");
        existingNames.add("col2");
        existingNames.add("col3");

        String result = GsiColsReplaceRule.generateUniqueColumnName("col1", existingNames);
        assertEquals("Should return base name with suffix 0 when base name exists", "col10", result);
    }

    /**
     * Test generateUniqueColumnName with base name and multiple suffixed versions exist
     */
    @Test
    public void testGenerateUniqueColumnNameWithMultipleSuffixedVersions() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col1");
        existingNames.add("col10");
        existingNames.add("col11");
        existingNames.add("col12");

        String result = GsiColsReplaceRule.generateUniqueColumnName("col1", existingNames);
        assertEquals("Should return base name with suffix 3 when 0,1,2 already exist", "col13", result);
    }

    /**
     * Test generateUniqueColumnName with empty existing names set
     */
    @Test
    public void testGenerateUniqueColumnNameWithEmptyExistingNames() {
        Set<String> existingNames = new HashSet<>();

        String result = GsiColsReplaceRule.generateUniqueColumnName("anyName", existingNames);
        assertEquals("Should return base name when existing names is empty", "anyName", result);
    }

    /**
     * Test generateUniqueColumnName with null base name
     */
    @Test
    public void testGenerateUniqueColumnNameWithNullBaseName() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col1");

        String result = GsiColsReplaceRule.generateUniqueColumnName(null, existingNames);
        assertEquals("Should return null when base name is null", null, result);
    }

    /**
     * Test generateUniqueColumnName with empty base name
     */
    @Test
    public void testGenerateUniqueColumnNameWithEmptyBaseName() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col1");

        String result = GsiColsReplaceRule.generateUniqueColumnName("", existingNames);
        assertEquals("Should return empty string when base name is empty", "", result);
    }

    /**
     * Test generateUniqueColumnName with base name that has numeric suffix already
     */
    @Test
    public void testGenerateUniqueColumnNameWithNumericSuffixBaseName() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col10");
        existingNames.add("col100");
        existingNames.add("col101");

        String result = GsiColsReplaceRule.generateUniqueColumnName("col10", existingNames);
        assertEquals("Should return base name with suffix 0 when base name with numeric suffix exists", "col102",
            result);
    }

    /**
     * Test generateUniqueColumnName with special characters in base name
     */
    @Test
    public void testGenerateUniqueColumnNameWithSpecialCharacters() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col_name");
        existingNames.add("col_name0");

        String result = GsiColsReplaceRule.generateUniqueColumnName("col_name", existingNames);
        assertEquals("Should handle special characters correctly", "col_name1", result);
    }

    /**
     * Test generateUniqueColumnName with case sensitive names
     */
    @Test
    public void testGenerateUniqueColumnNameCaseSensitive() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("ColName");
        existingNames.add("colname");

        String result = GsiColsReplaceRule.generateUniqueColumnName("COLNAME", existingNames);
        assertEquals("Should be case sensitive and return base name", "COLNAME", result);
    }

    /**
     * Test generateUniqueColumnName with large number of existing suffixed versions
     */
    @Test
    public void testGenerateUniqueColumnNameWithLargeNumberOfSuffixes() {
        Set<String> existingNames = new HashSet<>();
        existingNames.add("col");
        for (int i = 0; i < 100; i++) {
            existingNames.add("col" + i);
        }

        String result = GsiColsReplaceRule.generateUniqueColumnName("col", existingNames);
        assertEquals("Should find next available suffix", "col100", result);
    }
}
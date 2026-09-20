package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDnCclDryRun;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaDnCclDryRunHandler.CREATE_CN_CCL_DETECT_RULE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for InformationSchemaDnCclDryRunHandler specific methods
 * <p>
 * This test focuses on testing specific methods without SyncManagerHelper dependencies
 *
 * @author liugaoji
 */
public class InformationSchemaDnCclDryRunHandlerTest {

    private InformationSchemaDnCclDryRunHandler target;

    @Before
    public void setUp() {
        // Create handler with null VirtualViewHandler to avoid dependency issues
        target = new InformationSchemaDnCclDryRunHandler(null);
    }

    /**
     * Comprehensive test for specific methods
     * <p>
     * This single test method covers:
     * 1. processCclDetectKeyResults method
     * 2. extractTemplateIds method
     * 3. processPlanCacheResults method
     * 4. buildResultRows method
     * 5. CCLDetectKeyInfo inner class
     * 6. isSupport method
     * 7. Various edge cases and null handling
     */
    @Test
    public void testSpecificMethodsComprehensively() throws Exception {

        // ========== Test Scenario 1: processCclDetectKeyResults method ==========

        // Prepare CCL detect key results with various scenarios
        List<List<Map<String, Object>>> cclResults = prepareCclResults();

        // Call processCclDetectKeyResults directly (now public)
        Map<String, InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo> cclDetectKeyMap =
            target.processCclDetectKeyResults(cclResults);

        // Verify processCclDetectKeyResults results
        assertNotNull(cclDetectKeyMap);
        assertEquals(3, cclDetectKeyMap.size()); // Should have 3 valid entries
        assertTrue(cclDetectKeyMap.containsKey("inst1;template1"));
        assertTrue(cclDetectKeyMap.containsKey("inst2;template2"));
        assertTrue(cclDetectKeyMap.containsKey("inst6;template6"));

        // ========== Test Scenario 2: extractTemplateIds method ==========

        // Call extractTemplateIds directly (now public)
        Set<String> templateIds = target.extractTemplateIds(cclDetectKeyMap);

        // Verify extractTemplateIds results
        assertNotNull(templateIds);
        assertEquals(3, templateIds.size());
        assertTrue(templateIds.contains("template1"));
        assertTrue(templateIds.contains("template2"));
        assertTrue(templateIds.contains("template6"));

        // ========== Test Scenario 3: processPlanCacheResults method ==========

        // Prepare plan cache results with various scenarios
        List<List<Map<String, Object>>> planCacheResults = preparePlanCacheResults();

        // Call processPlanCacheResults directly (now public)
        Map<String, String> templateIdToSqlMap = target.processPlanCacheResults(planCacheResults);

        // Verify processPlanCacheResults results
        assertNotNull(templateIdToSqlMap);
        assertEquals(3, templateIdToSqlMap.size()); // Should have 3 SQL mappings (template6 deduplicated)
        assertEquals("SELECT * FROM table1 WHERE id = ?", templateIdToSqlMap.get("template1"));
        assertEquals("UPDATE table2 SET name = ? WHERE id = ?", templateIdToSqlMap.get("template2"));
        assertEquals("INSERT INTO table6 VALUES (?, ?, ?)", templateIdToSqlMap.get("template6"));

        // ========== Test Scenario 4: buildResultRows method ==========

        // Call buildResultRows directly (now public)
        ArrayResultCursor testCursor = new ArrayResultCursor("DN_CCL_DRYRUN");
        testCursor.initMeta();

        target.buildResultRows(cclDetectKeyMap, templateIdToSqlMap, testCursor);

        // Verify buildResultRows results
        List<Row> rows = testCursor.getRows();
        assertEquals(3, rows.size());

        // Since HashMap iteration order is not guaranteed, we need to verify by content rather than position
        // Create a map to find rows by instId
        Map<String, Row> rowsByInstId = new HashMap<>();
        for (Row row : rows) {
            rowsByInstId.put((String) row.getObject(0), row);
        }

        // Verify inst1 row
        Row inst1Row = rowsByInstId.get("inst1");
        assertNotNull(inst1Row);
        assertEquals("inst1", inst1Row.getObject(0));
        assertEquals("template1", inst1Row.getObject(1));
        assertTrue(inst1Row.getObject(2).toString().contains("template1"));
        assertEquals("SELECT * FROM table1 WHERE id = ?", inst1Row.getObject(3));

        // Verify inst2 row
        Row inst2Row = rowsByInstId.get("inst2");
        assertNotNull(inst2Row);
        assertEquals("inst2", inst2Row.getObject(0));
        assertEquals("template2", inst2Row.getObject(1));
        assertTrue(inst2Row.getObject(2).toString().contains("template2"));
        assertEquals("UPDATE table2 SET name = ? WHERE id = ?", inst2Row.getObject(3));

        // Verify inst6 row - tests deduplication
        Row inst6Row = rowsByInstId.get("inst6");
        assertNotNull(inst6Row);
        assertEquals("inst6", inst6Row.getObject(0));
        assertEquals("template6", inst6Row.getObject(1));
        assertTrue(inst6Row.getObject(2).toString().contains("template6"));
        assertEquals("INSERT INTO table6 VALUES (?, ?, ?)", inst6Row.getObject(3));

        // ========== Test Scenario 5: CCLDetectKeyInfo inner class testing ==========

        // Test the inner CCLDetectKeyInfo class (now public)
        InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo keyInfo =
            new InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo("testInst", "testTemplate", 100L);

        // Verify fields
        assertEquals("testInst", keyInfo.instId);
        assertEquals("testTemplate", keyInfo.templateId);
        assertEquals(Long.valueOf(100), keyInfo.concurrencyCount);

        // ========== Test Scenario 6: isSupport method testing ==========

        // Test isSupport method with null (should return false for safety)
        boolean nullResult = target.isSupport(null);
        assertFalse(nullResult);

        // ========== Test Scenario 7: Edge cases testing ==========

        // Test empty CCL results
        List<List<Map<String, Object>>> emptyCclResults = new ArrayList<>();
        Map<String, InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo> emptyCclDetectKeyMap =
            target.processCclDetectKeyResults(emptyCclResults);
        assertEquals(0, emptyCclDetectKeyMap.size());

        // Test empty plan cache results
        List<List<Map<String, Object>>> emptyPlanCacheResults = new ArrayList<>();
        Map<String, String> emptyTemplateIdToSqlMap = target.processPlanCacheResults(emptyPlanCacheResults);
        assertEquals(0, emptyTemplateIdToSqlMap.size());

        // Test buildResultRows with no matching SQL
        ArrayResultCursor noMatchCursor = new ArrayResultCursor("DN_CCL_DRYRUN");
        noMatchCursor.initMeta();

        // Create a CCL detect key map with no matching SQL
        Map<String, InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo> noMatchCclDetectKeyMap = new HashMap<>();
        InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo noMatchKeyInfo =
            new InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo("inst_no_match", "template_no_match", 99L);
        noMatchCclDetectKeyMap.put("inst_no_match;template_no_match", noMatchKeyInfo);

        Map<String, String> emptyTemplateMap = new HashMap<>();
        target.buildResultRows(noMatchCclDetectKeyMap, emptyTemplateMap, noMatchCursor);

        List<Row> noMatchRows = noMatchCursor.getRows();
        assertEquals(1, noMatchRows.size());
        Row noMatchRow = noMatchRows.get(0);
        assertEquals("inst_no_match", noMatchRow.getObject(0));
        assertEquals("template_no_match", noMatchRow.getObject(1));
        assertTrue(noMatchRow.getObject(2).toString().contains("template_no_match"));
        assertEquals("", noMatchRow.getObject(3)); // Empty SQL when no match

        // ========== Test Scenario 8: Null handling in processCclDetectKeyResults ==========

        // Test with null nodeRows
        List<List<Map<String, Object>>> nullNodeRowsResults = new ArrayList<>();
        nullNodeRowsResults.add(null);
        Map<String, InformationSchemaDnCclDryRunHandler.CCLDetectKeyInfo> nullNodeRowsMap =
            target.processCclDetectKeyResults(nullNodeRowsResults);
        assertEquals(0, nullNodeRowsMap.size());

        // ========== Test Scenario 9: Null handling in processPlanCacheResults ==========

        // Test with null nodeRows
        List<List<Map<String, Object>>> nullPlanNodeRowsResults = new ArrayList<>();
        nullPlanNodeRowsResults.add(null);
        Map<String, String> nullPlanNodeRowsMap = target.processPlanCacheResults(nullPlanNodeRowsResults);
        assertEquals(0, nullPlanNodeRowsMap.size());
    }

    // Helper methods to prepare test data

    private List<List<Map<String, Object>>> prepareCclResults() {
        List<List<Map<String, Object>>> cclResults = new ArrayList<>();

        // Node 1: Valid data with multiple entries
        List<Map<String, Object>> node1Results = new ArrayList<>();
        Map<String, Object> cclRow1 = new HashMap<>();
        cclRow1.put("INST_ID", "inst1");
        cclRow1.put("TEMPLATE_ID", "template1");
        cclRow1.put("CONCURRENCY_COUNT", 10L);
        node1Results.add(cclRow1);

        Map<String, Object> cclRow2 = new HashMap<>();
        cclRow2.put("INST_ID", "inst2");
        cclRow2.put("TEMPLATE_ID", "template2");
        cclRow2.put("CONCURRENCY_COUNT", 20L);
        node1Results.add(cclRow2);

        // Node 2: Partial invalid data (missing fields) - tests null handling
        List<Map<String, Object>> node2Results = new ArrayList<>();
        Map<String, Object> cclRow3 = new HashMap<>();
        cclRow3.put("INST_ID", "inst3");
        cclRow3.put("TEMPLATE_ID", null); // null templateId
        cclRow3.put("CONCURRENCY_COUNT", 30L);
        node2Results.add(cclRow3);

        Map<String, Object> cclRow4 = new HashMap<>();
        cclRow4.put("INST_ID", null); // null instId
        cclRow4.put("TEMPLATE_ID", "template4");
        cclRow4.put("CONCURRENCY_COUNT", 40L);
        node2Results.add(cclRow4);

        Map<String, Object> cclRow5 = new HashMap<>();
        cclRow5.put("INST_ID", "inst5");
        cclRow5.put("TEMPLATE_ID", "template5");
        cclRow5.put("CONCURRENCY_COUNT", null); // null concurrencyCount
        node2Results.add(cclRow5);

        // Node 3: null results - tests null nodeRows handling
        List<Map<String, Object>> node3Results = null;

        // Node 4: Valid data for template matching
        List<Map<String, Object>> node4Results = new ArrayList<>();
        Map<String, Object> cclRow6 = new HashMap<>();
        cclRow6.put("INST_ID", "inst6");
        cclRow6.put("TEMPLATE_ID", "template6");
        cclRow6.put("CONCURRENCY_COUNT", 60L);
        node4Results.add(cclRow6);

        cclResults.add(node1Results);
        cclResults.add(node2Results);
        cclResults.add(node3Results);
        cclResults.add(node4Results);

        return cclResults;
    }

    private List<List<Map<String, Object>>> preparePlanCacheResults() {
        List<List<Map<String, Object>>> planCacheResults = new ArrayList<>();

        // Node 1: Valid plan cache data
        List<Map<String, Object>> planNode1Results = new ArrayList<>();
        Map<String, Object> planRow1 = new HashMap<>();
        planRow1.put("TEMPLATE_ID", "template1");
        planRow1.put("PARAMETERIZED_SQL", "SELECT * FROM table1 WHERE id = ?");
        planNode1Results.add(planRow1);

        Map<String, Object> planRow2 = new HashMap<>();
        planRow2.put("TEMPLATE_ID", "template2");
        planRow2.put("PARAMETERIZED_SQL", "UPDATE table2 SET name = ? WHERE id = ?");
        planNode1Results.add(planRow2);

        // Node 2: Partial invalid plan cache data - tests null handling
        List<Map<String, Object>> planNode2Results = new ArrayList<>();
        Map<String, Object> planRow3 = new HashMap<>();
        planRow3.put("TEMPLATE_ID", null); // null templateId
        planRow3.put("PARAMETERIZED_SQL", "DELETE FROM table3 WHERE id = ?");
        planNode2Results.add(planRow3);

        Map<String, Object> planRow4 = new HashMap<>();
        planRow4.put("TEMPLATE_ID", "template6");
        planRow4.put("PARAMETERIZED_SQL", null); // null parameterizedSql
        planNode2Results.add(planRow4);

        // Node 3: null results - tests null nodeRows handling
        List<Map<String, Object>> planNode3Results = null;

        // Node 4: Valid plan cache data with duplicate templateId (tests deduplication)
        List<Map<String, Object>> planNode4Results = new ArrayList<>();
        Map<String, Object> planRow5 = new HashMap<>();
        planRow5.put("TEMPLATE_ID", "template6");
        planRow5.put("PARAMETERIZED_SQL", "INSERT INTO table6 VALUES (?, ?, ?)");
        planNode4Results.add(planRow5);

        planCacheResults.add(planNode1Results);
        planCacheResults.add(planNode2Results);
        planCacheResults.add(planNode3Results);
        planCacheResults.add(planNode4Results);

        return planCacheResults;
    }
}
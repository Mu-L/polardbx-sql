package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.spi.IRepository;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class LogicalShowDnCclHandlerTest {

    @Test
    public void testProcessPlanCacheResults() throws Exception {
        // Create handler instance
        IRepository mockRepo = Mockito.mock(IRepository.class);
        LogicalShowDnCclHandler handler = new LogicalShowDnCclHandler(mockRepo);

        // Get private method using reflection
        Method method = LogicalShowDnCclHandler.class.getDeclaredMethod(
            "processPlanCacheResults", List.class);
        method.setAccessible(true);

        // Test Case 1: Normal case with valid data
        List<List<Map<String, Object>>> normalResults = new ArrayList<>();
        List<Map<String, Object>> nodeRows1 = new ArrayList<>();

        Map<String, Object> row1 = new HashMap<>();
        row1.put("TEMPLATE_ID", "template_001");
        row1.put("PARAMETERIZED_SQL", "SELECT * FROM table1 WHERE id = ?");
        nodeRows1.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("TEMPLATE_ID", "template_002");
        row2.put("PARAMETERIZED_SQL", "UPDATE table1 SET name = ? WHERE id = ?");
        nodeRows1.add(row2);

        normalResults.add(nodeRows1);

        @SuppressWarnings("unchecked")
        Map<String, String> result1 = (Map<String, String>) method.invoke(handler, normalResults);

        assertNotNull("Result should not be null", result1);
        assertEquals("Should contain 2 entries", 2, result1.size());
        assertEquals("SELECT * FROM table1 WHERE id = ?", result1.get("template_001"));
        assertEquals("UPDATE table1 SET name = ? WHERE id = ?", result1.get("template_002"));

        // Test Case 2: Empty results
        List<List<Map<String, Object>>> emptyResults = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Map<String, String> result2 = (Map<String, String>) method.invoke(handler, emptyResults);

        assertNotNull("Result should not be null for empty input", result2);
        assertEquals("Should be empty map", 0, result2.size());

        // Test Case 3: Results with null nodeRows
        List<List<Map<String, Object>>> nullNodeRowsResults = new ArrayList<>();
        nullNodeRowsResults.add(null); // null nodeRows should be skipped

        List<Map<String, Object>> validNodeRows = new ArrayList<>();
        Map<String, Object> validRow = new HashMap<>();
        validRow.put("TEMPLATE_ID", "template_003");
        validRow.put("PARAMETERIZED_SQL", "DELETE FROM table1 WHERE id = ?");
        validNodeRows.add(validRow);
        nullNodeRowsResults.add(validNodeRows);

        @SuppressWarnings("unchecked")
        Map<String, String> result3 = (Map<String, String>) method.invoke(handler, nullNodeRowsResults);

        assertNotNull("Result should not be null", result3);
        assertEquals("Should contain 1 entry (null nodeRows skipped)", 1, result3.size());
        assertEquals("DELETE FROM table1 WHERE id = ?", result3.get("template_003"));

        // Test Case 4: Results with null templateId
        List<List<Map<String, Object>>> nullTemplateIdResults = new ArrayList<>();
        List<Map<String, Object>> nodeRowsWithNullTemplateId = new ArrayList<>();

        Map<String, Object> rowWithNullTemplateId = new HashMap<>();
        rowWithNullTemplateId.put("TEMPLATE_ID", null); // null templateId should be skipped
        rowWithNullTemplateId.put("PARAMETERIZED_SQL", "SELECT * FROM table2");
        nodeRowsWithNullTemplateId.add(rowWithNullTemplateId);

        Map<String, Object> validRowAfterNull = new HashMap<>();
        validRowAfterNull.put("TEMPLATE_ID", "template_004");
        validRowAfterNull.put("PARAMETERIZED_SQL", "INSERT INTO table2 VALUES (?)");
        nodeRowsWithNullTemplateId.add(validRowAfterNull);

        nullTemplateIdResults.add(nodeRowsWithNullTemplateId);

        @SuppressWarnings("unchecked")
        Map<String, String> result4 = (Map<String, String>) method.invoke(handler, nullTemplateIdResults);

        assertNotNull("Result should not be null", result4);
        assertEquals("Should contain 1 entry (null templateId skipped)", 1, result4.size());
        assertEquals("INSERT INTO table2 VALUES (?)", result4.get("template_004"));

        // Test Case 5: Results with null parameterizedSql
        List<List<Map<String, Object>>> nullSqlResults = new ArrayList<>();
        List<Map<String, Object>> nodeRowsWithNullSql = new ArrayList<>();

        Map<String, Object> rowWithNullSql = new HashMap<>();
        rowWithNullSql.put("TEMPLATE_ID", "template_005");
        rowWithNullSql.put("PARAMETERIZED_SQL", null); // null parameterizedSql should be skipped
        nodeRowsWithNullSql.add(rowWithNullSql);

        Map<String, Object> validRowAfterNullSql = new HashMap<>();
        validRowAfterNullSql.put("TEMPLATE_ID", "template_006");
        validRowAfterNullSql.put("PARAMETERIZED_SQL", "CREATE TABLE test (id INT)");
        nodeRowsWithNullSql.add(validRowAfterNullSql);

        nullSqlResults.add(nodeRowsWithNullSql);

        @SuppressWarnings("unchecked")
        Map<String, String> result5 = (Map<String, String>) method.invoke(handler, nullSqlResults);

        assertNotNull("Result should not be null", result5);
        assertEquals("Should contain 1 entry (null parameterizedSql skipped)", 1, result5.size());
        assertEquals("CREATE TABLE test (id INT)", result5.get("template_006"));

        // Test Case 6: Duplicate templateId (test deduplication - last one wins)
        List<List<Map<String, Object>>> duplicateResults = new ArrayList<>();
        List<Map<String, Object>> nodeRowsWithDuplicates = new ArrayList<>();

        Map<String, Object> firstDuplicate = new HashMap<>();
        firstDuplicate.put("TEMPLATE_ID", "template_duplicate");
        firstDuplicate.put("PARAMETERIZED_SQL", "First SQL statement");
        nodeRowsWithDuplicates.add(firstDuplicate);

        Map<String, Object> secondDuplicate = new HashMap<>();
        secondDuplicate.put("TEMPLATE_ID", "template_duplicate");
        secondDuplicate.put("PARAMETERIZED_SQL", "Second SQL statement");
        nodeRowsWithDuplicates.add(secondDuplicate);

        duplicateResults.add(nodeRowsWithDuplicates);

        @SuppressWarnings("unchecked")
        Map<String, String> result6 = (Map<String, String>) method.invoke(handler, duplicateResults);

        assertNotNull("Result should not be null", result6);
        assertEquals("Should contain 1 entry (deduplication)", 1, result6.size());
        assertEquals("Second SQL statement", result6.get("template_duplicate"));

        // Test Case 7: Multiple nodeRows from different nodes
        List<List<Map<String, Object>>> multiNodeResults = new ArrayList<>();

        // First node results
        List<Map<String, Object>> node1Rows = new ArrayList<>();
        Map<String, Object> node1Row = new HashMap<>();
        node1Row.put("TEMPLATE_ID", "template_node1");
        node1Row.put("PARAMETERIZED_SQL", "SELECT * FROM node1_table");
        node1Rows.add(node1Row);
        multiNodeResults.add(node1Rows);

        // Second node results
        List<Map<String, Object>> node2Rows = new ArrayList<>();
        Map<String, Object> node2Row = new HashMap<>();
        node2Row.put("TEMPLATE_ID", "template_node2");
        node2Row.put("PARAMETERIZED_SQL", "SELECT * FROM node2_table");
        node2Rows.add(node2Row);
        multiNodeResults.add(node2Rows);

        @SuppressWarnings("unchecked")
        Map<String, String> result7 = (Map<String, String>) method.invoke(handler, multiNodeResults);

        assertNotNull("Result should not be null", result7);
        assertEquals("Should contain 2 entries from different nodes", 2, result7.size());
        assertEquals("SELECT * FROM node1_table", result7.get("template_node1"));
        assertEquals("SELECT * FROM node2_table", result7.get("template_node2"));

        // Test Case 8: Mixed valid and invalid entries
        List<List<Map<String, Object>>> mixedResults = new ArrayList<>();
        List<Map<String, Object>> mixedNodeRows = new ArrayList<>();

        // Valid entry
        Map<String, Object> validMixedRow = new HashMap<>();
        validMixedRow.put("TEMPLATE_ID", "template_valid");
        validMixedRow.put("PARAMETERIZED_SQL", "Valid SQL statement");
        mixedNodeRows.add(validMixedRow);

        // Invalid entry with null templateId
        Map<String, Object> invalidMixedRow1 = new HashMap<>();
        invalidMixedRow1.put("TEMPLATE_ID", null);
        invalidMixedRow1.put("PARAMETERIZED_SQL", "Invalid SQL 1");
        mixedNodeRows.add(invalidMixedRow1);

        // Invalid entry with null parameterizedSql
        Map<String, Object> invalidMixedRow2 = new HashMap<>();
        invalidMixedRow2.put("TEMPLATE_ID", "template_invalid");
        invalidMixedRow2.put("PARAMETERIZED_SQL", null);
        mixedNodeRows.add(invalidMixedRow2);

        // Another valid entry
        Map<String, Object> validMixedRow2 = new HashMap<>();
        validMixedRow2.put("TEMPLATE_ID", "template_valid2");
        validMixedRow2.put("PARAMETERIZED_SQL", "Another valid SQL");
        mixedNodeRows.add(validMixedRow2);

        mixedResults.add(mixedNodeRows);

        @SuppressWarnings("unchecked")
        Map<String, String> result8 = (Map<String, String>) method.invoke(handler, mixedResults);

        assertNotNull("Result should not be null", result8);
        assertEquals("Should contain 2 valid entries", 2, result8.size());
        assertEquals("Valid SQL statement", result8.get("template_valid"));
        assertEquals("Another valid SQL", result8.get("template_valid2"));
        assertFalse("Should not contain invalid template", result8.containsKey("template_invalid"));
    }
}

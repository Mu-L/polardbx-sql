package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * JSON_TABLE function test cases
 *
 * @author AI Assistant
 */
public class JsonTableTest extends AutoReadBaseTestCase {

    public JsonTableTest() {
    }

    /**
     * Test JSON_TABLE with advanced features including NESTED, ON ERROR, and ON EMPTY clauses.
     * This test covers complex JSON structures with nested objects and error handling scenarios.
     */
    @Test
    public void testJsonTableAdvancedFeatures() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        // Create table for advanced JSON_TABLE testing
        String createTableSql = "CREATE TABLE pm_advanced_json (\n" +
            "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
            "    project_data JSON\n" +
            ") single";

        // Insert complex test data with nested structures and potential error scenarios
        String insertSql = "INSERT INTO pm_advanced_json (project_data) VALUES\n" +
            "('{\n" +
            "    \"projectId\": \"PROJ001\",\n" +
            "    \"projectName\": \"Advanced System\",\n" +
            "    \"budget\": 150000.50,\n" +
            "    \"invalidNumber\": \"not_a_number\",\n" +
            "    \"teams\": [\n" +
            "        {\n" +
            "            \"teamId\": \"T001\",\n" +
            "            \"teamName\": \"Development Team\",\n" +
            "            \"members\": [\n" +
            "                {\"name\": \"Alice\", \"role\": \"Lead\", \"salary\": 80000},\n" +
            "                {\"name\": \"Bob\", \"role\": \"Developer\", \"salary\": 65000}\n" +
            "            ]\n" +
            "        },\n" +
            "        {\n" +
            "            \"teamId\": \"T002\",\n" +
            "            \"teamName\": \"QA Team\",\n" +
            "            \"members\": [\n" +
            "                {\"name\": \"Charlie\", \"role\": \"Tester\", \"salary\": 55000},\n" +
            "                {\"name\": \"Diana\", \"role\": \"QA Lead\"}\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}'),\n" +
            "('{\n" +
            "    \"projectId\": \"PROJ002\",\n" +
            "    \"projectName\": \"Simple Project\",\n" +
            "    \"budget\": null,\n" +
            "    \"teams\": [\n" +
            "        {\n" +
            "            \"teamId\": \"T003\",\n" +
            "            \"teamName\": \"Solo Team\",\n" +
            "            \"members\": [\n" +
            "                {\"name\": \"Eve\", \"role\": \"Full Stack\", \"salary\": 75000}\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}')";

        // Complex JSON_TABLE query with NESTED, ON ERROR, and ON EMPTY clauses
        String selectSql = "SELECT\n" +
            "    jt.projectId,\n" +
            "    jt.projectName,\n" +
            "    jt.budget,\n" +
            "    jt.budgetAsInt,\n" +
            "    jt.teamId,\n" +
            "    jt.teamName,\n" +
            "    jt.memberName,\n" +
            "    jt.memberRole,\n" +
            "    jt.memberSalary\n" +
            "FROM\n" +
            "    pm_advanced_json,\n" +
            "    JSON_TABLE (\n" +
            "        project_data,\n" +
            "        '$' COLUMNS (\n" +
            "            projectId VARCHAR(20) PATH '$.projectId',\n" +
            "            projectName VARCHAR(100) PATH '$.projectName',\n" +
            "            budget DECIMAL(10,2) PATH '$.budget' NULL ON EMPTY,\n" +
            "            budgetAsInt INT PATH '$.invalidNumber' DEFAULT '0' ON ERROR,\n" +
            "            NESTED PATH '$.teams[*]' COLUMNS (\n" +
            "                teamId VARCHAR(20) PATH '$.teamId',\n" +
            "                teamName VARCHAR(100) PATH '$.teamName',\n" +
            "                NESTED PATH '$.members[*]' COLUMNS (\n" +
            "                    memberName VARCHAR(50) PATH '$.name',\n" +
            "                    memberRole VARCHAR(50) PATH '$.role',\n" +
            "                    memberSalary DECIMAL(10,2) PATH '$.salary' DEFAULT '0.00' ON EMPTY\n" +
            "                )\n" +
            "            )\n" +
            "        )\n" +
            "    ) AS jt\n" +
            "ORDER BY jt.projectId, jt.teamId, jt.memberName";

        try {
            // Execute DDL and DML
            tddlConnection.createStatement().execute("DROP TABLE IF EXISTS pm_advanced_json");
            tddlConnection.createStatement().execute(createTableSql);
            tddlConnection.createStatement().execute(insertSql);

            // Test the complex query
            Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Collect actual results
            List<String> actualResults = new ArrayList<>();
            while (resultSet.next()) {
                String projectId = resultSet.getString("projectId");
                String projectName = resultSet.getString("projectName");
                String budget = resultSet.getString("budget");
                String budgetAsInt = resultSet.getString("budgetAsInt");
                String teamId = resultSet.getString("teamId");
                String teamName = resultSet.getString("teamName");
                String memberName = resultSet.getString("memberName");
                String memberRole = resultSet.getString("memberRole");
                String memberSalary = resultSet.getString("memberSalary");

                actualResults.add(String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s",
                    projectId, projectName, budget, budgetAsInt, teamId, teamName,
                    memberName, memberRole, memberSalary));
            }
            resultSet.close();
            statement.close();

            // Verify result count - should have 5 rows (2+2+1 members from nested structure)
            Assert.assertEquals("Expected 5 rows in result", 5, actualResults.size());

            // Expected results with nested data and error handling
            Set<String> expectedResults = new HashSet<>();
            // PROJ001 results
            expectedResults.add("PROJ001|Advanced System|150000.50|0|T001|Development Team|Alice|Lead|80000.00");
            expectedResults.add("PROJ001|Advanced System|150000.50|0|T001|Development Team|Bob|Developer|65000.00");
            expectedResults.add("PROJ001|Advanced System|150000.50|0|T002|QA Team|Charlie|Tester|55000.00");
            expectedResults.add(
                "PROJ001|Advanced System|150000.50|0|T002|QA Team|Diana|QA Lead|0.00"); // ON EMPTY default
            // PROJ002 results
            expectedResults.add("PROJ002|Simple Project|null|null|T003|Solo Team|Eve|Full Stack|75000.00");

            // Verify all expected results are present
            Set<String> actualResultsSet = new HashSet<>(actualResults);
            for (String actual : actualResultsSet) {
                if (!expectedResults.contains(actual)) {
                    throw new AssertionError("Unexpected result: " + actual);
                }
            }

            // Additional verification for error handling features
            boolean foundErrorHandling = false;
            boolean foundEmptyHandling = false;

            for (String result : actualResults) {
                String[] parts = result.split("\\|");
                // Check ON ERROR handling - budgetAsInt should be 0 for invalid number
                if ("0".equals(parts[3])) {
                    foundErrorHandling = true;
                }
                // Check ON EMPTY handling - memberSalary should be 0.00 for Diana (missing salary)
                if ("Diana".equals(parts[6]) && "0.00".equals(parts[8])) {
                    foundEmptyHandling = true;
                }
            }

            Assert.assertTrue("ON ERROR handling should work correctly", foundErrorHandling);
            Assert.assertTrue("ON EMPTY handling should work correctly", foundEmptyHandling);

        } finally {
            // Clean up
            try {
                tddlConnection.createStatement().execute("DROP TABLE IF EXISTS pm_advanced_json");
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Test JSON_TABLE with partitioned table
     */
    @Test
    public void testJsonTablePartitioned() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        // Create partitioned table
        String createTableSql = "CREATE TABLE pm_work_risk_2 (\n" +
            "  id INT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  project_name VARCHAR(255),\n" +
            "  risk_level VARCHAR(50),\n" +
            "  review_node_info JSON\n" +
            ") partition by hash(id) partitions 2";

        // Insert test data
        String insertSql = "INSERT INTO pm_work_risk_2 (project_name, risk_level, review_node_info) VALUES\n" +
            "('Project Alpha', 'High', '[\n" +
            " {\"postName\": \"Initial Review\", \"approvalUser\": \"John Doe\", \"approvalOpinion\": \"Approved with minor comments\", \"approvalTime\": \"2023-10-26 10:00:00\"},\n"
            +
            " {\"postName\": \"Security Review\", \"approvalUser\": \"Jane Smith\", \"approvalOpinion\": \"Approved\", \"approvalTime\": \"2023-10-27 11:30:00\"}\n"
            +
            " ]'),\n" +
            "('Project Beta', 'Medium', '[\n" +
            " {\"postName\": \"Initial Review\", \"approvalUser\": \"Alice Johnson\", \"approvalOpinion\": \"Needs more details\", \"approvalTime\": \"2023-10-28 09:15:00\"}\n"
            +
            " ]'),\n" +
            "('Project Gamma', 'Low', '[\n" +
            " {\"postName\": \"Initial Review\", \"approvalUser\": \"Bob Williams\", \"approvalOpinion\": \"Approved\", \"approvalTime\": \"2023-10-29 14:00:00\"},\n"
            +
            " {\"postName\": \"Final Approval\", \"approvalUser\": \"Carol Davis\", \"approvalOpinion\": \"Approved\", \"approvalTime\": \"2023-10-30 16:45:00\"},\n"
            +
            " {\"postName\": \"Deployment Check\", \"approvalUser\": \"David Green\", \"approvalOpinion\": \"Passed\", \"approvalTime\": \"2023-10-31 09:00:00\"}\n"
            +
            " ]')";

        // Query with JSON_TABLE
        String selectSql = "/*+TDDL:node('p1,p2')*/ SELECT\n" +
            "\tjt.postName,\n" +
            "\tjt.approvalUser,\n" +
            "\tjt.approvalOpinion,\n" +
            "\tjt.approvalTime\n" +
            "FROM\n" +
            "\tpm_work_risk_2,\n" +
            "\tJSON_TABLE (\n" +
            "\t\treview_node_info,\n" +
            "\t\t'$[*]' COLUMNS (\n" +
            "\t\t\tpostName VARCHAR (50) PATH '$.postName',\n" +
            "\t\t\tapprovalUser VARCHAR (50) PATH '$.approvalUser',\n" +
            "\t\t\tapprovalOpinion VARCHAR (50) PATH '$.approvalOpinion',\n" +
            "\t\t\tapprovalTime VARCHAR (50) PATH '$.approvalTime'\n" +
            "\t\t)\n" +
            "\t) AS jt";

        try {
            // Execute DDL and DML
            tddlConnection.createStatement().execute("DROP TABLE IF EXISTS pm_work_risk_2");
            tddlConnection.createStatement().execute(createTableSql);
            tddlConnection.createStatement().execute(insertSql);

            // Test the query
            Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Collect actual results
            List<String> actualResults = new ArrayList<>();
            while (resultSet.next()) {
                String postName = resultSet.getString("postName");
                String approvalUser = resultSet.getString("approvalUser");
                String approvalOpinion = resultSet.getString("approvalOpinion");
                String approvalTime = resultSet.getString("approvalTime");
                actualResults.add(postName + "|" + approvalUser + "|" + approvalOpinion + "|" + approvalTime);
            }
            resultSet.close();
            statement.close();

            // Verify result count
            Assert.assertEquals("Expected 6 rows in result", 6, actualResults.size());

            // Expected results for partitioned table
            Set<String> expectedResults = new HashSet<>();
            expectedResults.add("Initial Review|Alice Johnson|Needs more details|2023-10-28 09:15:00");
            expectedResults.add("Initial Review|Bob Williams|Approved|2023-10-29 14:00:00");
            expectedResults.add("Final Approval|Carol Davis|Approved|2023-10-30 16:45:00");
            expectedResults.add("Deployment Check|David Green|Passed|2023-10-31 09:00:00");
            expectedResults.add("Initial Review|John Doe|Approved with minor comments|2023-10-26 10:00:00");
            expectedResults.add("Security Review|Jane Smith|Approved|2023-10-27 11:30:00");

            // Verify all expected results are present
            Set<String> actualResultsSet = new HashSet<>(actualResults);
            Assert.assertEquals("Result content mismatch", expectedResults, actualResultsSet);
        } finally {
            // Clean up
            try {
                tddlConnection.createStatement().execute("DROP TABLE IF EXISTS pm_work_risk_2");
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Test JSON_TABLE with single table and direct plan
     */
    @Test
    public void testJsonTableSingle() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        // Create single table
        String createTableSql = "CREATE TABLE pm_work_risk_1 (\n" +
            "    id INT PRIMARY KEY AUTO_INCREMENT,\n" +
            "    project_name VARCHAR(255),\n" +
            "    risk_level VARCHAR(50),\n" +
            "    review_node_info JSON\n" +
            ") single";

        // Insert test data
        String insertSql = "INSERT INTO pm_work_risk_1 (project_name, risk_level, review_node_info) VALUES\n" +
            "('Project Alpha', 'High', '[\n" +
            "    {\"postName\": \"Initial Review\", \"approvalUser\": \"John Doe\", \"approvalOpinion\": \"Approved with minor comments\", \"approvalTime\": \"2023-10-26 10:00:00\"},\n"
            +
            "    {\"postName\": \"Security Review\", \"approvalUser\": \"Jane Smith\", \"approvalOpinion\": \"Approved\", \"approvalTime\": \"2023-10-27 11:30:00\"}\n"
            +
            "]'),\n" +
            "('Project Beta', 'Medium', '[\n" +
            "    {\"postName\": \"Initial Review\", \"approvalUser\": \"Alice Johnson\", \"approvalOpinion\": \"Needs more details\", \"approvalTime\": \"2023-10-28 09:15:00\"}\n"
            +
            "]'),\n" +
            "('Project Gamma', 'Low', '[\n" +
            "    {\"postName\": \"Initial Review\", \"approvalUser\": \"Bob Williams\", \"approvalOpinion\": \"Approved\", \"approvalTime\": \"2023-10-29 14:00:00\"},\n"
            +
            "    {\"postName\": \"Final Approval\", \"approvalUser\": \"Carol Davis\", \"approvalOpinion\": \"Approved\", \"approvalTime\": \"2023-10-30 16:45:00\"},\n"
            +
            "    {\"postName\": \"Deployment Check\", \"approvalUser\": \"David Green\", \"approvalOpinion\": \"Passed\", \"approvalTime\": \"2023-10-31 09:00:00\"}\n"
            +
            "]')";

        // Query with JSON_TABLE and direct plan hint
        String selectSql = "SELECT\n" +
            "    jt.postName,\n" +
            "    jt.approvalUser,\n" +
            "    jt.approvalOpinion,\n" +
            "    jt.approvalTime\n" +
            "FROM\n" +
            "    pm_work_risk_1,\n" +
            "    JSON_TABLE (\n" +
            "        review_node_info,\n" +
            "        '$[*]' COLUMNS (\n" +
            "            postName VARCHAR (50) PATH '$.postName',\n" +
            "            approvalUser VARCHAR (50) PATH '$.approvalUser',\n" +
            "            approvalOpinion VARCHAR (50) PATH '$.approvalOpinion',\n" +
            "            approvalTime VARCHAR (50) PATH '$.approvalTime'\n" +
            "        )\n" +
            "    ) AS jt";

        try {
            // Execute DDL and DML
            tddlConnection.createStatement().execute("DROP TABLE IF EXISTS pm_work_risk_1");
            tddlConnection.createStatement().execute(createTableSql);
            tddlConnection.createStatement().execute(insertSql);

            // Test the query
            Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Collect actual results
            List<String> actualResults = new ArrayList<>();
            while (resultSet.next()) {
                String postName = resultSet.getString("postName");
                String approvalUser = resultSet.getString("approvalUser");
                String approvalOpinion = resultSet.getString("approvalOpinion");
                String approvalTime = resultSet.getString("approvalTime");
                actualResults.add(postName + "|" + approvalUser + "|" + approvalOpinion + "|" + approvalTime);
            }
            resultSet.close();
            statement.close();

            // Verify result count
            Assert.assertEquals("Expected 6 rows in result", 6, actualResults.size());

            // Expected results for single table (same data as partitioned table)
            Set<String> expectedResults = new HashSet<>();
            expectedResults.add("Initial Review|John Doe|Approved with minor comments|2023-10-26 10:00:00");
            expectedResults.add("Security Review|Jane Smith|Approved|2023-10-27 11:30:00");
            expectedResults.add("Initial Review|Alice Johnson|Needs more details|2023-10-28 09:15:00");
            expectedResults.add("Initial Review|Bob Williams|Approved|2023-10-29 14:00:00");
            expectedResults.add("Final Approval|Carol Davis|Approved|2023-10-30 16:45:00");
            expectedResults.add("Deployment Check|David Green|Passed|2023-10-31 09:00:00");

            // Verify all expected results are present
            Set<String> actualResultsSet = new HashSet<>(actualResults);
            Assert.assertEquals("Result content mismatch", expectedResults, actualResultsSet);
        } finally {
            // Clean up
            try {
                tddlConnection.createStatement().execute("DROP TABLE IF EXISTS pm_work_risk_1");
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

}
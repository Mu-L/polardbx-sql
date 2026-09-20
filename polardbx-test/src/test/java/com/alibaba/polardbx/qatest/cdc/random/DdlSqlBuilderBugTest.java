/**
 * Copyright (c) 2013-Present, Alibaba Group Holding Limited.
 * All rights reserved.
 * <p>
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */
package com.alibaba.polardbx.qatest.cdc.random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression tests for AONE-62283862:
 * CDC QATest TestModeTwo#testRandomDmlWithDdl 随机失败
 * <p>
 * Bug 1: DdlSqlBuilder.buildCreateIndexSql() generates invalid SQL when flag=false:
 * "create index `idx` on table `tbl`(`col`)"  -- INVALID MySQL syntax
 * Should be:
 * "create index `idx` on `tbl`(`col`)"        -- valid MySQL syntax
 * <p>
 * Bug 2: DdlSqlBuilder.buildDropIndexSql() generates invalid SQL when flag=false:
 * "drop index `idx` on table `tbl`"           -- INVALID MySQL syntax
 * Should be:
 * "drop index `idx` on `tbl`"                 -- valid MySQL syntax
 * <p>
 * Both methods use RandomUtils.nextBoolean(), so the invalid branch fires ~50% of the time.
 * Running 100 iterations makes it statistically certain to hit the broken branch.
 * <p>
 * These tests do NOT require a database connection and can run as pure unit tests.
 */
public class DdlSqlBuilderBugTest {

    private static final String TABLE_NAME = "t_random";

    /**
     * Creates a ColumnSeeds pre-populated with a few column entries (no DB connection needed).
     */
    private ColumnSeeds buildColumnSeeds() {
        ColumnSeeds seeds = new ColumnSeeds("test_db", TABLE_NAME);
        seeds.COLUMN_NAME_COLUMN_TYPE_MAPPING.put("c_col1", "varchar(100)");
        seeds.COLUMN_NAME_COLUMN_TYPE_MAPPING.put("c_col2", "int");
        seeds.COLUMN_NAME_COLUMN_TYPE_MAPPING.put("c_col3", "bigint");
        return seeds;
    }

    private DdlSqlBuilder buildDdlSqlBuilder(ColumnSeeds seeds) {
        return new DdlSqlBuilder(TABLE_NAME, seeds,
            false, false, false, false, false, false);
    }

    /**
     * Bug 1: buildCreateIndexSql() must never produce "on table" syntax.
     * <p>
     * Current buggy code (DdlSqlBuilder.java:112):
     * String.format("create index `%s` on table `%s`(`%s`)", indexName, tableName, columnName)
     * <p>
     * Valid MySQL CREATE INDEX syntax is "CREATE INDEX idx ON tbl (col)", not "ON TABLE tbl".
     * With flag=false (~50% probability), the invalid form is generated.
     * <p>
     * This test MUST FAIL on current (buggy) code.
     */
    @Test
    public void testCreateIndexSqlNeverUsesOnTableSyntax() {
        ColumnSeeds seeds = buildColumnSeeds();
        DdlSqlBuilder builder = buildDdlSqlBuilder(seeds);

        for (int i = 0; i < 100; i++) {
            Pair<String, String> result = builder.buildCreateIndexSql();
            Assert.assertNotNull("buildCreateIndexSql() must not return null when columns exist", result);

            String sql = result.getValue();
            Assert.assertFalse(
                "CREATE INDEX SQL contains invalid 'on table' syntax at iteration " + i + ". Got: " + sql,
                sql.toLowerCase().matches("create index.*\\bon table\\b.*")
            );
        }
    }

    /**
     * Bug 2: buildDropIndexSql() must never produce "on table" syntax.
     * <p>
     * Current buggy code (DdlSqlBuilder.java:127):
     * String.format("drop index `%s` on table `%s`", indexName, tableName)
     * <p>
     * Valid MySQL DROP INDEX syntax is "DROP INDEX idx ON tbl", not "ON TABLE tbl".
     * With flag=false (~50% probability), the invalid form is generated.
     * <p>
     * This test MUST FAIL on current (buggy) code.
     */
    @Test
    public void testDropIndexSqlNeverUsesOnTableSyntax() {
        ColumnSeeds seeds = buildColumnSeeds();
        seeds.INDEX_SET.add("c_col1_index");
        seeds.INDEX_SET.add("c_col2_index");
        DdlSqlBuilder builder = buildDdlSqlBuilder(seeds);

        for (int i = 0; i < 100; i++) {
            Pair<String, String> result = builder.buildDropIndexSql();
            Assert.assertNotNull("buildDropIndexSql() must not return null when INDEX_SET is non-empty", result);

            String sql = result.getValue();
            Assert.assertFalse(
                "DROP INDEX SQL contains invalid 'on table' syntax at iteration " + i + ". Got: " + sql,
                sql.toLowerCase().matches("drop index.*\\bon table\\b.*")
            );
        }
    }

    /**
     * Bug 1 supplement: When buildCreateIndexSql() returns a CREATE INDEX variant,
     * the SQL must start with "create index" followed by "on `tableName`", not "on table".
     * <p>
     * This test MUST FAIL on current (buggy) code.
     */
    @Test
    public void testCreateIndexSqlVariantHasCorrectTableReference() {
        ColumnSeeds seeds = buildColumnSeeds();
        DdlSqlBuilder builder = buildDdlSqlBuilder(seeds);

        boolean seenCreateIndex = false;
        for (int i = 0; i < 200; i++) {
            Pair<String, String> result = builder.buildCreateIndexSql();
            if (result == null) {
                continue;
            }
            String sql = result.getValue();
            if (sql.startsWith("create index")) {
                seenCreateIndex = true;
                // Valid form: "create index `idx` on `t_random`(`col`)"
                // Invalid form: "create index `idx` on table `t_random`(`col`)"
                Assert.assertTrue(
                    "CREATE INDEX SQL must use 'on `" + TABLE_NAME + "`' not 'on table `" + TABLE_NAME + "`'. Got: "
                        + sql,
                    sql.contains("on `" + TABLE_NAME + "`")
                );
            }
        }
        Assert.assertTrue("Must encounter at least one CREATE INDEX variant in 200 iterations", seenCreateIndex);
    }

    /**
     * Bug 2 supplement: When buildDropIndexSql() returns a DROP INDEX variant,
     * the SQL must use "on `tableName`", not "on table `tableName`".
     * <p>
     * This test MUST FAIL on current (buggy) code.
     */
    @Test
    public void testDropIndexSqlVariantHasCorrectTableReference() {
        ColumnSeeds seeds = buildColumnSeeds();
        seeds.INDEX_SET.add("c_col1_index");
        DdlSqlBuilder builder = buildDdlSqlBuilder(seeds);

        boolean seenDropIndex = false;
        for (int i = 0; i < 200; i++) {
            Pair<String, String> result = builder.buildDropIndexSql();
            if (result == null) {
                continue;
            }
            String sql = result.getValue();
            if (sql.startsWith("drop index")) {
                seenDropIndex = true;
                // Valid form: "drop index `idx` on `t_random`"
                // Invalid form: "drop index `idx` on table `t_random`"
                Assert.assertTrue(
                    "DROP INDEX SQL must use 'on `" + TABLE_NAME + "`' not 'on table `" + TABLE_NAME + "`'. Got: "
                        + sql,
                    sql.contains("on `" + TABLE_NAME + "`")
                );
            }
        }
        Assert.assertTrue("Must encounter at least one DROP INDEX variant in 200 iterations", seenDropIndex);
    }
}

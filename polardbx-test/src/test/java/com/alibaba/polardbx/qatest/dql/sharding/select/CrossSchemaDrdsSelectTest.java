package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Reproduce NPE for AONE #85114084:
 * In DRDS mode (legacy sharding mode), a cross-schema qualified SELECT
 * issued without first executing "use <schema>" to switch to the schema
 * referenced in the SQL triggers a NullPointerException, because
 * PlanShardInfo.getRelShardInfo(schema, table) returns null and callers
 * chain further method calls on it without a null check.
 * <p>
 * The test creates both DRDS schemas explicitly so it does not depend on
 * environment-specific pre-created databases.
 */
public class CrossSchemaDrdsSelectTest extends BaseTestCase {
    private static final String TARGET_SCHEMA = "cross_schema_drds_target";
    private static final String TARGET_TABLE = "cross_schema_select_table";
    private static final String DEFAULT_SCHEMA = "cross_schema_drds_default";

    @BeforeClass
    public static void prepareSchemas() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(c, "drop database if exists " + TARGET_SCHEMA);
            JdbcUtil.executeUpdateSuccess(c, "drop database if exists " + DEFAULT_SCHEMA);
            JdbcUtil.executeUpdateSuccess(c, "create database " + TARGET_SCHEMA + " partition_mode = 'drds'");
            JdbcUtil.executeUpdateSuccess(c, "create database " + DEFAULT_SCHEMA + " partition_mode = 'drds'");
        }
        try (Connection c = getPolardbxConnection0(TARGET_SCHEMA)) {
            JdbcUtil.executeUpdateSuccess(c,
                "create table " + TARGET_TABLE + " (id bigint primary key) dbpartition by hash(id)");
            JdbcUtil.executeUpdateSuccess(c, "insert into " + TARGET_TABLE + " values (1),(2),(3)");
        }
    }

    @AfterClass
    public static void cleanSchemas() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(c, "drop database if exists " + TARGET_SCHEMA);
            JdbcUtil.executeUpdateSuccess(c, "drop database if exists " + DEFAULT_SCHEMA);
        }
    }

    @Test
    public void testCrossSchemaSelectWithoutUseDb() throws SQLException {
        String sql = "select id from " + TARGET_SCHEMA + "." + TARGET_TABLE + " where id in (1,2,3) order by id";
        try (Connection c = getPolardbxConnection0(DEFAULT_SCHEMA);
            Statement stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

            Set<Long> ids = new HashSet<>();
            while (rs.next()) {
                ids.add(rs.getLong("id"));
            }
            Assert.assertEquals("cross-schema select without USE should return all 3 rows, but got NPE / wrong result",
                3, ids.size());
            Assert.assertTrue(ids.contains(1L) && ids.contains(2L) && ids.contains(3L));
        }
    }
}

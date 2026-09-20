package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoAccessor;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;

/**
 * AONE-85331776: BaselineInfoAccessor.deleteBaselineByInstSchemaBaselineId() is expected to
 * delete all non-fixed baselines of the given inst+schema when the kept-baseline-id list is
 * empty. The current implementation passes the empty list to concatInt() which returns null,
 * producing "BASELINE_INFO.ID NOT IN(null)". By SQL three-valued logic this predicate is
 * UNKNOWN for every row, so nothing is deleted and stale schema-level baselines can never be
 * cleaned by the BASELINE_SYNC job.
 */
public class BaselineEmptyListCleanTest extends BaseTestCase {

    private static final String SCHEMA = "spm_empty_clean_db";
    private static final String INST_ID = "spm_empty_clean_inst";

    private static final String INSERT_BASELINE =
        "insert into spm_baseline (`id`, `inst_id`, `schema_name`, `sql`, `table_set`) "
            + "values (%d, '%s', '%s', 'select %d', '[]')";

    private static final String INSERT_PLAN =
        "insert into spm_plan (`id`, `inst_id`, `schema_name`, `baseline_id`, `plan`, `choose_count`, "
            + "`cost`, `estimate_execution_time`, `accepted`, `fixed`, `trace_id`, `tables_hashcode`, `version`) "
            + "values (%d, '%s', '%s', %d, '{}', 0, 0.0, 0.0, 1, %d, 'trace-%d', 0, 0)";

    @BeforeClass
    public static void beforeClass() {
        if (MetaDbDataSource.getInstance() == null) {
            String addr = ConnectionManager.getInstance().getMetaAddress() + ":"
                + ConnectionManager.getInstance().getMetaPort();
            String dbName = PropertiesUtil.getMetaDB;
            String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
            String usr = ConnectionManager.getInstance().getMetaUser();
            String pwd = PropertiesUtil.configProp.getProperty(ConfigConstant.META_PASSWORD);
            MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);
        }
    }

    @Test
    public void testDeleteWithEmptyBaselineIds() throws Exception {
        prepareData();
        assertBaselineCount(3);

        try (BaselineInfoAccessor accessor = newMetaDbAccessor()) {
            accessor.deleteBaselineByInstSchemaBaselineId(INST_ID, SCHEMA, Collections.emptyList());
        }

        // empty kept-id list means "keep nothing": all non-fixed baselines (id 1, 2) must be
        // removed, while the fixed baseline (id 3) is preserved by the FIXED != 1 condition
        assertBaselineCount(1);
        assertBaselineExists(3);
    }

    @Test
    public void testDeleteWithNonEmptyBaselineIds() throws Exception {
        prepareData();
        assertBaselineCount(3);

        try (BaselineInfoAccessor accessor = newMetaDbAccessor()) {
            // keep baseline 1; baseline 2 (non-fixed) must be removed, baseline 3 is fixed
            accessor.deleteBaselineByInstSchemaBaselineId(INST_ID, SCHEMA, Arrays.asList(1));
        }

        assertBaselineExists(1);
        assertBaselineNotExists(2);
        assertBaselineExists(3);
    }

    private void prepareData() throws SQLException {
        cleanupData();
        try (Connection c = getMetaConnection(); Statement stmt = c.createStatement()) {
            // baseline 1 and 2: non-fixed, baseline 3: fixed
            stmt.executeUpdate(String.format(INSERT_BASELINE, 1, INST_ID, SCHEMA, 1));
            stmt.executeUpdate(String.format(INSERT_BASELINE, 2, INST_ID, SCHEMA, 2));
            stmt.executeUpdate(String.format(INSERT_BASELINE, 3, INST_ID, SCHEMA, 3));
            stmt.executeUpdate(String.format(INSERT_PLAN, 1, INST_ID, SCHEMA, 1, 0, 1));
            stmt.executeUpdate(String.format(INSERT_PLAN, 2, INST_ID, SCHEMA, 2, 0, 2));
            stmt.executeUpdate(String.format(INSERT_PLAN, 3, INST_ID, SCHEMA, 3, 1, 3));
        }
    }

    private BaselineInfoAccessor newMetaDbAccessor() throws SQLException {
        BaselineInfoAccessor accessor = new BaselineInfoAccessor(false);
        accessor.setConnection(getMetaConnection());
        return accessor;
    }

    @After
    public void cleanupData() throws SQLException {
        try (Connection c = getMetaConnection(); Statement stmt = c.createStatement()) {
            stmt.executeUpdate("delete from spm_plan where inst_id = '" + INST_ID + "'");
            stmt.executeUpdate("delete from spm_baseline where inst_id = '" + INST_ID + "'");
        }
    }

    private int countBaselines() throws SQLException {
        try (Connection c = getMetaConnection(); Statement stmt = c.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "select count(1) from spm_baseline where inst_id = '" + INST_ID + "' and schema_name = '" + SCHEMA
                    + "'");
            Assert.assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private void assertBaselineCount(int expected) throws SQLException {
        int actual = countBaselines();
        Assert.assertEquals(
            "stale baselines of schema [" + SCHEMA + "] should be cleaned, remaining count mismatch", expected,
            actual);
    }

    private void assertBaselineExists(int baselineId) throws SQLException {
        Assert.assertTrue("baseline " + baselineId + " should exist", exists(baselineId));
    }

    private void assertBaselineNotExists(int baselineId) throws SQLException {
        Assert.assertFalse("baseline " + baselineId + " should be deleted", exists(baselineId));
    }

    private boolean exists(int baselineId) throws SQLException {
        try (Connection c = getMetaConnection(); Statement stmt = c.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "select count(1) from spm_baseline where inst_id = '" + INST_ID + "' and schema_name = '" + SCHEMA
                    + "' and id = " + baselineId);
            Assert.assertTrue(rs.next());
            return rs.getInt(1) > 0;
        }
    }
}

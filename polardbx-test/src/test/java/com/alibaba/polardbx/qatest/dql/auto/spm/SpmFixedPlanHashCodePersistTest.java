package com.alibaba.polardbx.qatest.dql.auto.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Regression test for AONE-85060571: after {@code PlanManager.tryUpdatePlan()}
 * rebuilds a FIXED PLAN because of a table-version(hashcode) change,
 * {@code BaselineInfoAccessor.persist()} only updates LAST_EXECUTE_TIME/CHOOSE_COUNT
 * when the plan id does not change, so the new TABLES_HASHCODE is never written
 * back to metadb.spm_plan. The next BASELINE_SYNC (baseline load) then reloads the
 * stale hashcode from metadb and the same fixed plan is rebuilt again ("整点重复重建").
 *
 * @author fangwu
 */
public class SpmFixedPlanHashCodePersistTest extends BaseTestCase {

    private static final String DB_NAME = "SPM_FIX_HASHCODE_TEST_DB";
    private static final String TB_NAME = "spm_fix_hashcode_tb";

    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `name` varchar(20) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`id`) partitions 2";

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));
        }
    }

    @AfterClass
    public static void clean() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("delete from metadb.spm_baseline where schema_name='" + DB_NAME + "'");
            c.createStatement().execute("delete from metadb.spm_plan where schema_name='" + DB_NAME + "'");
            c.createStatement().execute("drop database if exists " + DB_NAME);
        }
    }

    /**
     * Reproduces the acceptance criteria of AONE-85060571:
     * 1. create a fixed plan and record its tables_hashcode in metadb.
     * 2. run a DDL that bumps the table version (hence tables_hashcode) without
     * changing the plan JSON/plan id (comment-only DDL).
     * 3. trigger tryUpdatePlan() by running the fixed sql again, which should
     * report SPM_FIX_DDL_HASHCODE_UPDATE and refresh the in-memory hashcode.
     * 4. persist the baseline (equivalent of persistBaseline()/updateBaseline()
     * used by the hourly BASELINE_SYNC job) and assert metadb.spm_plan.tables_hashcode
     * has actually been updated to the new value.
     * <p>
     * Under the current buggy code, step 4 fails because
     * BaselineInfoAccessor.persist() takes the UPDATE_PLAN_STATS branch
     * (plan id unchanged) and never writes the new TABLES_HASHCODE.
     */
    @Test
    public void testFixedPlanHashCodePersistedAfterHashCodeRebuild() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String sql = "select id from " + TB_NAME + " where id=1";
            String hint = "/*TDDL:cmd_extra(ENABLE_POST_PLANNER=false)*/";

            // step 1: create fixed plan
            ResultSet rs = c.createStatement().executeQuery("baseline fix sql " + hint + sql);
            Assert.assertTrue(rs.next());
            int baselineId = rs.getInt("BASELINE_ID");
            rs.close();

            c.createStatement().execute("baseline persist");

            Long initialHashCode = queryPersistedTablesHashCode(baselineId);
            Assert.assertTrue(initialHashCode != null, "expect the initial fixed plan persisted in metadb.spm_plan");

            // step 2: bump table version without changing the plan shape (comment-only DDL).
            // DDL invalidation is synchronous. It keeps the fixed plan row in metadb, while the
            // in-memory plan is marked with REBUILD_PLAN_HASH_CODE so the next access rebuilds it.
            c.createStatement().execute("alter table " + TB_NAME + " comment='rebuild_trigger_1'");
            Long hashCodeBeforeRebuild = queryPersistedTablesHashCode(baselineId);
            Assert.assertTrue(hashCodeBeforeRebuild != null,
                "DDL invalidation must retain the fixed plan row in metadb.spm_plan");
            Assert.assertTrue(hashCodeBeforeRebuild.longValue() == initialHashCode.longValue(),
                "DDL invalidation must not overwrite the persisted hash before the fixed plan is rebuilt");

            // step 3: trigger tryUpdatePlan() -> SPM_FIX_DDL_HASHCODE_UPDATE (rebuild in memory only)
            c.createStatement().executeQuery(sql).close();

            // step 4: simulate persistBaseline()/updateBaseline() used by hourly BASELINE_SYNC
            c.createStatement().execute("baseline persist");

            Long persistedHashCode = queryPersistedTablesHashCode(baselineId);
            Assert.assertTrue(persistedHashCode != null,
                "expect the rebuilt fixed plan persisted in metadb.spm_plan");

            Assert.assertTrue(
                persistedHashCode.longValue() != hashCodeBeforeRebuild.longValue(),
                "expect metadb.spm_plan.tables_hashcode to be refreshed to the real table hashcode "
                    + "after fix plan rebuild (initial=" + initialHashCode + "), "
                    + "but it stayed at the stale pre-rebuild value " + hashCodeBeforeRebuild
                    + " (BaselineInfoAccessor.persist() skipped TABLES_HASHCODE update because plan id was unchanged)");
        }
    }

    private Long queryPersistedTablesHashCode(int baselineId) throws SQLException {
        try (Connection meta = getMetaConnection()) {
            try (ResultSet rs = meta.createStatement().executeQuery(
                "select TABLES_HASHCODE from spm_plan where schema_name='" + DB_NAME
                    + "' and baseline_id=" + baselineId + " and fixed=1 order by gmt_modified desc limit 1")) {
                return rs.next() ? rs.getLong("TABLES_HASHCODE") : null;
            }
        }
    }
}

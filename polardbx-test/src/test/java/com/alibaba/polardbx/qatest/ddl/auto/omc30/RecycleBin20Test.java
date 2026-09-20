package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

public class RecycleBin20Test extends DDLBaseNewDBTestCase {
    @Before
    public void beforeMethod() {
        hint = "/*+TDDL:cmd_extra(FORCE_USING_OMC_30 = true)*/";
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testRecycleBin() {
        JdbcUtil.executeSuccess(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = true");

        String tableName = "omc_30_recycle_bin";
        dropTableIfExists(tableName);

        String sql = String.format(
            "create table %s (a int primary key, b int) partition by key(a) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s values(1, 1), (2, 2), (6, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s)*/", jobId);
        sql = myHint + hint + String.format("alter table %s modify column b int, algorithm = omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkRecycleBin(jobId, true, 3);
    }

    @Test
    public void testRecycleBin2() {
        JdbcUtil.executeSuccess(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = false");

        String tableName = "omc_30_recycle_bin2";
        dropTableIfExists(tableName);

        String sql =
            String.format("create table %s (a int primary key, b int) partition by key(a) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s values(1, 1), (2, 2), (3， 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s)*/", jobId);
        sql = myHint + hint + String.format("alter table %s modify column b int, algorithm = omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkRecycleBin(jobId, false, 3);
    }

    /**
     * 部分成功部分失败 + 回滚
     */
    @Test
    public void testRecycleBin3() {
        JdbcUtil.executeSuccess(tddlConnection, "set ENABLE_PHY_RECYCLEBIN = true");

        String tableName = "omc_30_recycle_bin";
        dropTableIfExists(tableName);

        String sql = String.format(
            "create table %s (a int primary key, b int) partition by key(a) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s values(1, 1), (2, 2), (6, 6), (4, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s)*/", jobId);
        sql = myHint + hint + String.format("alter table %s add unique idx_uk(b), algorithm = omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        checkRecycleBin(jobId, true, 4);
    }

    void checkRecycleBin(long jobId, boolean exists, int recycleBinCount) {
        String sql =
            String.format("select changeset_id from metadb.omc_record where job_id = %s and status = 2", jobId);
        List<String> changesetIds = JdbcUtil.executeQueryAndGetColumnResult(sql, tddlConnection, 1);

        Assert.assertEquals(recycleBinCount, changesetIds.size());

        for (String changesetId : changesetIds) {
            sql = String.format(
                "select cur_tb_name,storage_inst_id from metadb.phy_recycle_bin_info where cur_tb_name like '%%%s_omc_bin'",
                changesetId);
            String tbName = JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 1);
            String storageId = JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2);

            if (!exists) {
                Assert.assertEquals("", tbName);
                Assert.assertEquals("", storageId);
                continue;
            }

            String group = getGroupByStorageInstId(storageId, tddlDatabase1);

            sql = String.format("/*+TDDL:node('%s')*/select count(1) from %s.%s", group,
                PhyRecycleBinInfoRecord.PHY_DB_NAME, tbName);

            try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    Assert.assertEquals(1, count);
                }
            } catch (Exception e) {
                if (exists) {
                    Assert.fail("recycle bin not exists");
                }
            }
        }
    }
}

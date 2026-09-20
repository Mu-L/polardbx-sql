package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class showTest extends DDLBaseNewDBTestCase {

    @Test
    public void testShow() {
        String sql = "show node";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show connection";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show full connection";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "clear procedure cache";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show workload";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "resize procedure cache 100";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show profile";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show stats";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show slow";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show table replicate status";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show global deadlocks";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "show trans";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Test
    public void testShowStorage() {
        List<String> storageList = LocalityTestUtils.getDatanodes(tddlConnection);
        LocalityTestUtils.flushStorageLabel(storageList, tddlConnection);

        // wait flush
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<String, String> storageLabelMap = new HashMap<>();
        String sql = "show storage";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            while (rs.next()) {
                storageLabelMap.put(rs.getString("STORAGE_INST_ID"), rs.getString("STORAGE_INST_LABEL"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < storageList.size(); i++) {
            String storageInstId = storageList.get(i);
            String label = storageLabelMap.get(storageInstId);

            Assert.assertEquals(label, "set" + (i + 1));
        }

        Map<String, String> fullStorageLabelMap = new HashMap<>();
        String fullSql = "show full storage";
        try (ResultSet rs = JdbcUtil.executeQuery(fullSql, tddlConnection)) {
            while (rs.next()) {
                fullStorageLabelMap.put(rs.getString("DN"), rs.getString("DN_LABEL"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < storageList.size(); i++) {
            String storageInstId = storageList.get(i);
            String label = storageLabelMap.get(storageInstId);

            Assert.assertEquals(label, "set" + (i + 1));
        }
    }
}

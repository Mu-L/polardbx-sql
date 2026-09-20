package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

@NotThreadSafe
public class OmcCheckerTest extends DDLBaseNewDBTestCase {

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testSnapshotTooOld() {
        if (!isMySQL80()) {
            return;
        }

        try {
            String setPurge = "set global innodb_undo_retention = 0";
            JdbcUtil.executeUpdateSuccess(tddlConnection, setPurge);

            String tableName = "omc_check_snapshot_test";
            dropTableIfExists(tableName);
            String sql = String.format("create table %s (a int primary key, b int) single", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("insert into table %s values (0, 1), (1, 2), (2, 3)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            String hint = "/*+TDDL:cmd_extra(FP_OMC_CHECK_WITH_TSO_SUSPEND=60000)*/";
            sql = hint + String.format("alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
            execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

            sql = String.format("select * from %s where a=0", tableName);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            try {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "0");
                Assert.assertEquals(rs.getString(2), "1");
            } catch (SQLException e) {
                throw new RuntimeException("", e);
            } finally {
                JdbcUtil.close(rs);
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global innodb_undo_retention = 1800");
        }
    }
}

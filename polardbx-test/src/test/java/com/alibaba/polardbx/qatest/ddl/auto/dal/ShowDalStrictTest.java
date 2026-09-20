package com.alibaba.polardbx.qatest.ddl.auto.dal;

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

public class ShowDalStrictTest extends LocalityTestBase {

    @Test
    public void testSingleTableLocality() {
        String databaseName = "`test_locality_db`";
        String tableName = "`test_table`";

        final String dn = chooseDatanode(tddlConnection);
        final String localitySql = " LOCALITY='dn=" + dn + "'";
        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + databaseName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table " + tableName + " (id int)" + localitySql);

        String normal = JdbcUtil.executeQueryAndGetStringResult("show tables" + tableName, tddlConnection, 0);
        String columanr = JdbcUtil.executeQueryAndGetStringResult(
            "/*+TDDL: ENABLE_LOGICAL_TABLE_META=true*/ show tables " + tableName, tddlConnection, 0);
        Assert.assertTrue(normal.equals(columanr));

        // drop and check again
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database " + databaseName);
    }
}

package com.alibaba.polardbx.qatest.ddl.datamigration.locality;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StoragePoolTaskUtils;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolUtils;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class RebalanceLogTest extends LocalityTestBase {

    private static final Log logger = LogFactory.getLog(RebalanceLogTest.class);

    private final String databaseName1 = "test_rebalance_drds_db";
    private final String databaseName2 = "test_rebalance_auto_db";

    public void testAlterStoragePoolWithPlanId(Connection tddlConnection) throws InterruptedException {
        String storagePoolName = "sp3";
        List<String> dnList = getDatanodes(tddlConnection, storagePoolName);
        String dn = chooseDatanode(tddlConnection, storagePoolName, false);
        String alterStoragePoolSp3Sql = "alter storage pool sp3 drain node '" + dn + "'";
        String alterStoragePoolSp3BackSql = "alter storage pool sp3 append node '" + dn + "'";
        Long planId1 = Long.parseLong(
            JdbcUtil.executeQueryAndGetFirstStringResult("select ddl_plan_id()", tddlConnection));
        logger.info(" get plan id " + planId1);
        String hint = StoragePoolTaskUtils.constructPlanIdHint(planId1);
        String queryPlanIdSql = String.format(
            "select plan_id from information_schema.ddl_plan where gmt_created >= now() - interval 5 second and resource = '%s'",
            ActionUtils.genRebalanceTenantResourceName(storagePoolName));
        logger.info("Executing SQL: " + alterStoragePoolSp3Sql);
        Thread.sleep(5000);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + alterStoragePoolSp3Sql);
        logger.info("Executing SQL: " + queryPlanIdSql);
        String fetchedPlanId1 = JdbcUtil.executeQueryAndGetFirstStringResult(queryPlanIdSql, tddlConnection);
        logger.info("fetched plan id " + fetchedPlanId1);
        logger.info("Waiting for DDL to complete");
        DdlStateCheckUtil.waitTillDdlDone(logger, tddlConnection);

        Long planId2 = Long.parseLong(
            JdbcUtil.executeQueryAndGetFirstStringResult("select ddl_plan_id()", tddlConnection));
        logger.info(" get plan id " + planId2);
        hint = StoragePoolTaskUtils.constructPlanIdHint(planId2);
        Thread.sleep(5000);
        logger.info("Executing SQL: " + alterStoragePoolSp3BackSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + alterStoragePoolSp3BackSql);
        logger.info("Executing SQL: " + queryPlanIdSql);
        String fetchedPlanId2 = JdbcUtil.executeQueryAndGetFirstStringResult(queryPlanIdSql, tddlConnection);
        logger.info("fetched plan id " + fetchedPlanId2);
        logger.info("Waiting for DDL to complete");
        DdlStateCheckUtil.waitTillDdlDone(logger, tddlConnection);

        if (planId1.toString().equals(fetchedPlanId1) && planId2.toString().equals(fetchedPlanId2)) {
            return;
        }
        Assert.fail("plan id not match " + planId1 + " " + fetchedPlanId1 + " " + planId2 + " " + fetchedPlanId2);
    }

    // test if drain node _recycle was rejected when the node is still in use.
    // which is fixed by add a validate task to drain node _recycle.
    public void testAlterRecycleDrainNode(Connection tddlConnection) throws InterruptedException {
        String database = "test_rebalance_db_drain_recyle";
        String storagePoolName = "sp3";
        final String createTableSql1 =
            "create table t1 (a int) partition by hash(a) partitions 32";
        final String localityDesc = " LOCALITY = \"storage_pools='" + storagePoolName + "'\"";
        JdbcUtil.dropDatabase(tddlConnection, database);
        String dn = chooseDatanode(tddlConnection, storagePoolName, false);
        List<String> dnList = getDatanodes(tddlConnection, storagePoolName);
        String alterStoragePoolSp3Sql = "alter storage pool sp3 drain node '" + dn + "'";
        String alterStoragePoolSp3BackSql = "alter storage pool sp3 append node '" + dn + "'";
        String alterStoragePoolRecycleSql = "alter storage pool _recycle drain node '" + dn + "'";
        String createDbSql = " create database  " + database + " mode = auto " + localityDesc;
        String dropStoragePoolSp3Sql = " drop storage pool " + storagePoolName;
        String dnStr = String.join(",", dnList);
        String createStoragePoolSp3Sql = "create storage pool " + storagePoolName + " dn_list='" + dnStr + "'";

        logger.info("Executing SQL: " + createDbSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createDbSql);

        logger.info("Executing SQL: " + "use " + database);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + database);

        logger.info("Executing SQL: " + createTableSql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        logger.info("Executing SQL: " + alterStoragePoolSp3Sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterStoragePoolSp3Sql);

        logger.info("Executing SQL (expected to fail): " + alterStoragePoolRecycleSql);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterStoragePoolRecycleSql, "failed to");

        logger.info("Waiting for DDL to complete");
        DdlStateCheckUtil.waitTillDdlDone(logger, tddlConnection);

        logger.info("Executing SQL: " + alterStoragePoolSp3BackSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterStoragePoolSp3BackSql);

        logger.info("Waiting for DDL to complete");
        DdlStateCheckUtil.waitTillDdlDone(logger, tddlConnection);

        // We don't support drop storage pool if it's still in use
        logger.info("Executing SQL: " + dropStoragePoolSp3Sql);
        JdbcUtil.executeUpdateFailed(tddlConnection, dropStoragePoolSp3Sql,
            "The storage pool definition contains storage inst still in use! ");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "use polardbx");

        logger.info("Dropping database: " + database);
        JdbcUtil.dropDatabase(tddlConnection, database);

        logger.info("Executing SQL: " + dropStoragePoolSp3Sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropStoragePoolSp3Sql);

        logger.info("Executing SQL: " + alterStoragePoolSp3Sql);
        JdbcUtil.executeUpdateFailed(tddlConnection, alterStoragePoolSp3Sql, "failed to");

        logger.info("Executing SQL: " + createStoragePoolSp3Sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createStoragePoolSp3Sql);
    }

    public void testRebalanceDrdsDb(Connection tddlConnection) throws InterruptedException {
        final String createTableSql1 =
            "create table t1 (a int) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        final String tableName1 = "t1";
        JdbcUtil.dropDatabase(tddlConnection, databaseName1);

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + databaseName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName1);

        // before

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        // check information_schema.locality_info

        // check show topology
        List<String> dnListOfDb = getDnListOfDb(tddlConnection, databaseName1, true);
        List<String> actualDn = getDnListOfTable(tddlConnection, databaseName1, tableName1);
        Assert.assertEquals(new HashSet<String>(dnListOfDb), new HashSet<String>(actualDn));

        // drop and check again
        final String dn = chooseDatanode(tddlConnection, false);
        final String drainNodeSql = String.format("rebalance database drain_node = '%s'", dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, drainNodeSql);

        final String querySql =
            String.format("select count(1) from metadb.ddl_engine where schema_name = '%s' and state != 'SUCCESS'",
                databaseName1);

        // wait for rebalance complete
        boolean waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database drain_node for " + databaseName1);
        }

        List<String> afterDrainNodeDbList = getDnListOfDb(tddlConnection, databaseName1, true);
        List<String> afterDrainNodeTableDnList = getDnListOfTable(tddlConnection, databaseName1, tableName1);
        Assert.assertEquals("db and table dn list not equal after drain node",
            new HashSet<String>(afterDrainNodeDbList), new HashSet<String>(afterDrainNodeTableDnList));
        List<String> expectedDrainNodeDnList = new ArrayList<>(dnListOfDb);
        expectedDrainNodeDnList.remove(dn);
        Assert.assertEquals("expected dn after drain node not meet", new HashSet<String>(afterDrainNodeTableDnList),
            new HashSet<String>(expectedDrainNodeDnList));

        final String rebalanceSql = String.format("rebalance database");
        JdbcUtil.executeUpdateSuccess(tddlConnection, rebalanceSql);

        // wait for rebalance complete
        waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database for " + databaseName1);
        }

        List<String> afterRebalanceDbList = getDnListOfDb(tddlConnection, databaseName1, true);
        List<String> afterRebalanceTableList = getDnListOfTable(tddlConnection, databaseName1, tableName1);
        Assert.assertEquals("db and table dn list not equal after rebalance",
            new HashSet<String>(afterRebalanceTableList), new HashSet<String>(afterRebalanceTableList));
        Assert.assertEquals("expected dn after rebalance not meet", new HashSet<String>(afterRebalanceDbList),
            new HashSet<String>(dnListOfDb));
    }

    public void testRebalanceAutoDb(Connection tddlConnection) throws InterruptedException {
        final String createTableSql1 = "create table t1 (a int) partition by hash(a) partitions 16";
        final String tableName1 = "t1";
        JdbcUtil.dropDatabase(tddlConnection, databaseName2);

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + databaseName2 + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName1);

        // before

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        // check information_schema.locality_info

        // check show topology
        List<String> dnListOfDb = getDnListOfDb(tddlConnection, databaseName2, true);
        List<String> actualDn = getDnListOfTable(tddlConnection, databaseName2, tableName1);
        Assert.assertEquals(new HashSet<String>(dnListOfDb), new HashSet<String>(actualDn));

        // drop and check again
        final String dn = chooseDatanode(tddlConnection, false);
        final String drainNodeSql = String.format("rebalance database drain_node = '%s'", dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, drainNodeSql);

        final String querySql =
            String.format("select count(1) from metadb.ddl_engine where schema_name = '%s' and state != 'SUCCESS'",
                databaseName2);

        // wait for rebalance complete
        boolean waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database drain_node for " + databaseName2);
        }

        List<String> afterDrainNodeDbList = getDnListOfDb(tddlConnection, databaseName2, true);
        List<String> afterDrainNodeTableDnList = getDnListOfTable(tddlConnection, databaseName2, tableName1);
        Assert.assertEquals("db and table dn list not equal after drain node",
            new HashSet<String>(afterDrainNodeDbList), new HashSet<String>(afterDrainNodeTableDnList));
        List<String> expectedDrainNodeDnList = new ArrayList<>(dnListOfDb);
        expectedDrainNodeDnList.remove(dn);
        Assert.assertEquals("expected dn after drain node not meet", new HashSet<String>(afterDrainNodeTableDnList),
            new HashSet<String>(expectedDrainNodeDnList));

        final String rebalanceSql = String.format("rebalance database");
        JdbcUtil.executeUpdateSuccess(tddlConnection, rebalanceSql);

        // wait for rebalance complete
        waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance database for " + databaseName2);
        }

        List<String> afterRebalanceDbList = getDnListOfDb(tddlConnection, databaseName2, true);
        List<String> afterRebalanceTableList = getDnListOfTable(tddlConnection, databaseName2, tableName1);
        Assert.assertEquals("db and table dn list not equal after rebalance",
            new HashSet<String>(afterRebalanceTableList), new HashSet<String>(afterRebalanceTableList));
        Assert.assertEquals("expected dn after rebalance not meet", new HashSet<String>(afterRebalanceDbList),
            new HashSet<String>(dnListOfDb));
    }
}

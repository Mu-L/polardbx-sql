package com.alibaba.polardbx.qatest.ddl.datamigration.locality;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.TOPOLOGY_DN_ID;

@NotThreadSafe
public class AlterLocalityTest extends LocalityTestBase {

    private static final Log logger = LogFactory.getLog(AlterLocalityTest.class);

    private final String databaseName2 = "test_rebalance_auto_db";

    @After
    public void dropDb() {
    }

    @Test
    public void testMultipleRoundSetPartitionLocality()
        throws FileNotFoundException, InterruptedException, SQLException {
        final String createTableSql1 =
            "create table t1 (a int) partition by hash(a) partitions 8";
        final String tableName1 = "t1";
        final String dbName = databaseName2;
        JdbcUtil.dropDatabase(tddlConnection, dbName);

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + dbName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + dbName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName1);

        // before

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        // check information_schema.locality_info

        // check show topology
        List<String> dnListOfDb = getDnListOfDb(tddlConnection, dbName, true);
        List<String> actualDn = getDnListOfTable(tddlConnection, dbName, tableName1);
        Assert.assertEquals(new HashSet<String>(dnListOfDb), new HashSet<String>(actualDn));

        // drop and check again
        final Map<String, Map<String, String>> topology =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        Map<String, String> tableToTableGroupMap = DdlStateCheckUtil.getTableToTableGroupMap(tddlConnection, dbName);
        String p1dn = topology.get("p1").get(TOPOLOGY_DN_ID);
        String p2dn = topology.get("p2").get(TOPOLOGY_DN_ID);
        String tg = tableToTableGroupMap.get(tableName1);
        // first round
        final String alterLocalityOnce =
            String.format("alter tablegroup %s set partitions p1 locality='dn=%s'", tg, p2dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterLocalityOnce);

        DdlStateCheckUtil.waitPlanIdOrSchemaNameFinish(tddlConnection, dbName);
        // wait for rebalance complete
        final Map<String, Map<String, String>> topologyAfterFirstRound =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        Assert.assertEquals("alter tablegroup should make effect for p1 partition ",
            topologyAfterFirstRound.get("p1").get(TOPOLOGY_DN_ID), p2dn);
        for (int i = 2; i <= 8; i++) {
            String pName = String.format("p%s", i);
            Assert.assertEquals("alter tablegroup should make no effect for " + pName + " partition ",
                topologyAfterFirstRound.get(pName).get(TOPOLOGY_DN_ID), topology.get(pName).get(TOPOLOGY_DN_ID));
        }

        // second round
        final String alterLocalityTwice =
            String.format("alter tablegroup %s set partitions p1 locality='dn=%s'", tg, p1dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterLocalityTwice);
        final String queryPlanIdSql =
            String.format("select max(plan_id) from metadb.ddl_plan where table_schema = '%s' and state != 'SUCCESS'",
                dbName);
        Long planId = Long.parseLong(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(queryPlanIdSql, tddlConnection)).get(0).get(0).toString());
        final String updatePlanState =
            String.format(" update metadb.ddl_plan set state = 'SUCCESS' where plan_id = '%s'", planId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updatePlanState);
        final Map<String, Map<String, String>> topologyAfterSecondRound =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        Assert.assertEquals("alter tablegroup should not make effect for p1 partition ",
            topologyAfterSecondRound.get("p1").get(TOPOLOGY_DN_ID), p2dn);
        for (int i = 2; i <= 8; i++) {
            String pName = String.format("p%s", i);
            Assert.assertEquals("alter tablegroup should make no effect for " + pName + " partition ",
                topologyAfterSecondRound.get(pName).get(TOPOLOGY_DN_ID), topology.get(pName).get(TOPOLOGY_DN_ID));
        }

        // third round
        final String alterLocalityThird =
            String.format("alter tablegroup %s set partitions p1 locality='dn=%s'", tg, p1dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterLocalityThird);
        DdlStateCheckUtil.waitPlanIdOrSchemaNameFinish(tddlConnection, dbName);
        final Map<String, Map<String, String>> topologyAfterThirdRound =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        Assert.assertEquals("alter tablegroup should make effect for p1 partition ",
            topologyAfterThirdRound.get("p1").get(TOPOLOGY_DN_ID), p1dn);
        for (int i = 2; i <= 8; i++) {
            String pName = String.format("p%s", i);
            Assert.assertEquals("alter tablegroup should make no effect for " + pName + " partition ",
                topologyAfterThirdRound.get(pName).get(TOPOLOGY_DN_ID), topology.get(pName).get(TOPOLOGY_DN_ID));
        }
    }

    @Test
    public void testMultipleRoundLocality() throws FileNotFoundException, InterruptedException, SQLException {
        final String createTableSql1 =
            "create table t1 (a int) partition by hash(a) partitions 8";
        final String tableName1 = "t1";
        final String dbName = databaseName2;
        JdbcUtil.dropDatabase(tddlConnection, dbName);

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + dbName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + dbName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName1);

        // before

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);

        // check information_schema.locality_info

        // check show topology
        List<String> dnListOfDb = getDnListOfDb(tddlConnection, dbName, true);
        List<String> actualDn = getDnListOfTable(tddlConnection, dbName, tableName1);
        Assert.assertEquals(new HashSet<String>(dnListOfDb), new HashSet<String>(actualDn));

        // drop and check again
        final Map<String, Map<String, String>> topology =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        Map<String, String> tableToTableGroupMap = DdlStateCheckUtil.getTableToTableGroupMap(tddlConnection, dbName);
        String p1dn = topology.get("p1").get(TOPOLOGY_DN_ID);
        String p2dn = topology.get("p2").get(TOPOLOGY_DN_ID);
        String tg = tableToTableGroupMap.get(tableName1);
        // first round
        final String alterLocalityOnce =
            String.format("alter tablegroup %s set locality='dn=%s'", tg, p2dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterLocalityOnce);

        DdlStateCheckUtil.waitPlanIdOrSchemaNameFinish(tddlConnection, dbName);
        // wait for rebalance complete
        final Map<String, Map<String, String>> topologyAfterFirstRound =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        for (int i = 1; i <= 8; i++) {
            String pName = String.format("p%s", i);
            Assert.assertEquals("alter tablegroup should make effect for " + pName + " partition ",
                topologyAfterFirstRound.get(pName).get(TOPOLOGY_DN_ID), p2dn);
        }

        // second round
        final String alterLocalityTwice =
            String.format("alter tablegroup %s set locality='dn=%s'", tg, p1dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterLocalityTwice);
        final String queryPlanIdSql =
            String.format("select max(plan_id) from metadb.ddl_plan where table_schema = '%s' and state != 'SUCCESS'",
                dbName);
        Long planId = Long.parseLong(
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(queryPlanIdSql, tddlConnection)).get(0).get(0).toString());
        final String updatePlanState =
            String.format(" update metadb.ddl_plan set state = 'SUCCESS' where plan_id = '%s'", planId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, updatePlanState);
        final Map<String, Map<String, String>> topologyAfterSecondRound =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        for (int i = 1; i <= 8; i++) {
            String pName = String.format("p%s", i);
            Assert.assertEquals("alter tablegroup should make no effect for " + pName + " partition ",
                topologyAfterSecondRound.get(pName).get(TOPOLOGY_DN_ID), p2dn);
        }

        // third round
        final String alterLocalityThird =
            String.format("alter tablegroup %s set partitions p1 locality='dn=%s'", tg, p1dn);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterLocalityThird);
        DdlStateCheckUtil.waitPlanIdOrSchemaNameFinish(tddlConnection, dbName);
        final Map<String, Map<String, String>> topologyAfterThirdRound =
            DdlStateCheckUtil.getTableTopology(tddlConnection, tableName1);
        Assert.assertEquals("alter tablegroup should make effect for p1 partition ",
            topologyAfterThirdRound.get("p1").get(TOPOLOGY_DN_ID), p1dn);
        for (int i = 1; i <= 8; i++) {
            String pName = String.format("p%s", i);
            Assert.assertEquals("alter tablegroup should make effect for " + pName + " partition ",
                topologyAfterThirdRound.get(pName).get(TOPOLOGY_DN_ID), p1dn);
        }
    }

}

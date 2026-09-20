package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.rebalance;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.pkrange.PkTest;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.balancer.action.ActionExpandDatabase.ACTION_EXPAND_DATABASE_NAME;

@NotThreadSafe
@RunWith(Parameterized.class)
public class SubJobConcurrentTaskForRebalanceClusterTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public SubJobConcurrentTaskForRebalanceClusterTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("empty_table", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testSubJobConcurrentTaskForRebalanceClusterExplain() throws SQLException, InterruptedException {
        List<String> schemaNames = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String schemaName = "sub_job_concurrent_test_for_cluster_" + i;
            schemaNames.add(schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
            logger.info("process schema " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t1(a int, b int) partition by hash(a) partitions 16 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t2(a bigint, b int) partition by hash(a) partitions 17 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t3(a varchar(32), b int) partition by hash(a) partitions 18 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t4(a varchar(16), b int) partition by hash(a) partitions 19 ");
        }
        String dbListHint = String.format(ConnectionProperties.REBALANCE_DB_LIST_WHEN_REBALANCE_CLUSTER_ONLY_DEBUG + "="
            + "'%s'", String.join(",", schemaNames));
        String rebalanceDdl = String.format(
            "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=64,%s,REBALANCE_DB_PARALLELISM=3)*/rebalance cluster shuffle_data_dist=1 explain = true;",
            dbListHint);

        String expectedSql =
            "REBALANCE DATABASE MAX_ACTIONS =  50 SHUFFLE_DATA_DIST=1 EXPLAIN=true ASYNC=true DEBUG=false";
        List<String> sqls = new ArrayList<>();
        List<String> rebalanceSchemaNames = new ArrayList<>();
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, rebalanceDdl)) {
            while (resultSet.next()) {
                String name = resultSet.getString("NAME");
                String schema = resultSet.getString("SCHEMA");
                String sql = resultSet.getString("ACTION");
                if (name.equals(ACTION_EXPAND_DATABASE_NAME)) {
                    sqls.add(sql);
                    rebalanceSchemaNames.add(schema);
                }
            }
        }
        logger.info("sqls: " + JSON.toJSONString(sqls));
        logger.info("schemas: " + JSON.toJSONString(rebalanceSchemaNames));
        schemaNames.sort(String::compareTo);
        rebalanceSchemaNames.sort(String::compareTo);
        Assert.assertTrue(schemaNames.equals(rebalanceSchemaNames));
        Assert.assertTrue(!sqls.stream().anyMatch(o -> !o.equals(expectedSql)));
    }

    @Test
    public void testSubJobConcurrentTaskForRebalanceClusterForAutoDatabase() throws SQLException, InterruptedException {
        List<String> schemaNames = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String schemaName = "sub_job_concurrent_test_for_cluster_" + i;
            schemaNames.add(schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
            logger.info("process schema " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t1(a int, b int) partition by hash(a) partitions 16 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t2(a bigint, b int) partition by hash(a) partitions 17 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t3(a varchar(32), b int) partition by hash(a) partitions 18 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t4(a varchar(16), b int) partition by hash(a) partitions 19 ");
        }
        String dbListStr = String.format("'%s'", String.join(",", schemaNames));
        String dbListHint = String.format(ConnectionProperties.REBALANCE_DB_LIST_WHEN_REBALANCE_CLUSTER_ONLY_DEBUG + "="
            + dbListStr);
        JdbcUtil.executeUpdate(tddlConnection, "use polardbx");
        String rebalanceDdl = String.format(
            "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=64,%s,REBALANCE_DB_PARALLELISM=3,REBALANCE_CLUSTER_PARALLELISM=2,PHYSICAL_BACKFILL_ENABLE=false)*/rebalance cluster shuffle_data_dist=1",
            dbListHint);

        List<String> sqls = new ArrayList<>();
        List<String> rebalanceSchemaNames = new ArrayList<>();
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, rebalanceDdl)) {
            while (resultSet.next()) {
                String name = resultSet.getString("NAME");
                String schema = resultSet.getString("SCHEMA");
                String sql = resultSet.getString("ACTION");
                if (name.equals(ACTION_EXPAND_DATABASE_NAME)) {
                    sqls.add(sql);
                    rebalanceSchemaNames.add(schema);
                }
            }
        }
        JdbcUtil.executeUpdate(tddlConnection, "use " + schemaNames.get(0));
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, rebalanceDdl);
        logger.info("job id is " + jobId);
        logger.info("sqls: " + JSON.toJSONString(sqls));
        logger.info("schemas: " + JSON.toJSONString(rebalanceSchemaNames));
        String dbStrList = schemaNames.stream().map(o -> "'" + o + "'").collect(Collectors.joining(","));
        String sql =
            String.format(
                " select ddl_stmt, schema_name from metadb.ddl_engine where schema_name in (%s) and state not in ('COMPLETED', 'ROLLBACK_COMPLETED');",
                dbStrList);
        Boolean checkOk = true;
        for (int i = 0; i < 2000; i++) {
            Set<String> runningSchemaNames = new HashSet<>();
            try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                while (resultSet.next()) {
                    String schemaName = resultSet.getString("schema_name");
                    runningSchemaNames.add(schemaName);
                }
            }
            if (runningSchemaNames.size() > 2) {
                checkOk = false;
            }
            logger.info(" times " + i + " concurrency is " + runningSchemaNames.size());
            Thread.sleep(1000);
            if (DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId)) {
                break;
            }
        }
        if (DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId)) {
            if (!checkOk) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "rebalance cluster didn't meet concurrency requirement " + jobId);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "rebalance cluster didn't end success, job id is " + jobId);
        }
    }

    @Test
    public void testSubJobConcurrentTaskForRebalanceClusterForAutoDatabaseWithPhysicalBackfill()
        throws SQLException, InterruptedException {
        List<String> schemaNames = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String schemaName = "sub_job_concurrent_test_for_cluster_with_data_" + i;
            schemaNames.add(schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
            logger.info("process schema " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t1(a int, b int) partition by hash(a) partitions 16 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t2(a bigint, b int) partition by hash(a) partitions 17 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t3(a varchar(32), b int) partition by hash(a) partitions 18 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t4(a varchar(16), b int) partition by hash(a) partitions 19 ");
        }
        List<Pair<String, Boolean>> storageList =
            DdlStateCheckUtil.getRealStorageList();
        if (storageList.size() <= 1 && !storageList.get(0).getValue()) {
            return;
        }
        String deletableDn = storageList.get(0).getKey();
        String drainNodeRebalanceSql = String.format(" rebalance database drain_node = '%s' ", deletableDn);
        for (int i = 0; i < 4; i++) {
            String schemaName = "sub_job_concurrent_test_for_cluster_with_data_drds_" + i;
            schemaNames.add(schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = drds");
            logger.info("process schema " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t1(a int, b int) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 2 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t2(a int, b int) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 3 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table t3(a int, b int) dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4 ");
            JdbcUtil.executeUpdateSuccess(tddlConnection, drainNodeRebalanceSql);
            Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, drainNodeRebalanceSql);
            DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, "somerandom_badstring_forthis");
        }
        String dbListStr = String.format("'%s'", String.join(",", schemaNames));
        String dbListHint =
            String.format(ConnectionProperties.REBALANCE_DB_LIST_WHEN_REBALANCE_CLUSTER_ONLY_DEBUG + "="
                + dbListStr);
        JdbcUtil.executeUpdate(tddlConnection, "use polardbx");
        String rebalanceDdl = String.format(
            "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=64,%s,REBALANCE_DB_PARALLELISM=3,REBALANCE_CLUSTER_PARALLELISM=2,PHYSICAL_BACKFILL_ENABLE=true,TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=-1)*/rebalance cluster shuffle_data_dist=1",
            dbListHint);

        List<String> sqls = new ArrayList<>();
        List<String> rebalanceSchemaNames = new ArrayList<>();
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, rebalanceDdl)) {
            while (resultSet.next()) {
                String name = resultSet.getString("NAME");
                String schema = resultSet.getString("SCHEMA");
                String sql = resultSet.getString("ACTION");
                if (name.equals(ACTION_EXPAND_DATABASE_NAME)) {
                    sqls.add(sql);
                    rebalanceSchemaNames.add(schema);
                }
            }
        }
        JdbcUtil.executeUpdate(tddlConnection, "use " + schemaNames.get(0));
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, rebalanceDdl);
        logger.info("job id is " + jobId);
        logger.info("sqls: " + JSON.toJSONString(sqls));
        logger.info("schemas: " + JSON.toJSONString(rebalanceSchemaNames));
        String dbStrList = schemaNames.stream().map(o -> "'" + o + "'").collect(Collectors.joining(","));
        String sql =
            String.format(
                " select ddl_stmt, schema_name from metadb.ddl_engine where schema_name in (%s) and state not in ('COMPLETED', 'ROLLBACK_COMPLETED');",
                dbStrList);
        Boolean checkOk = true;
        for (int i = 0; i < 2000; i++) {
            Set<String> runningSchemaNames = new HashSet<>();
            try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
                while (resultSet.next()) {
                    String schemaName = resultSet.getString("schema_name");
                    runningSchemaNames.add(schemaName);
                }
            }
            if (runningSchemaNames.size() > 2) {
                checkOk = false;
            }
            logger.info(" times " + i + " concurrency is " + runningSchemaNames.size());
            Thread.sleep(1000);
            if (DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId)) {
                break;
            }
        }
        if (DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId)) {
            if (!checkOk) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "rebalance cluster didn't meet concurrency requirement " + jobId);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "rebalance cluster didn't end success, job id is " + jobId);
        }
    }
}
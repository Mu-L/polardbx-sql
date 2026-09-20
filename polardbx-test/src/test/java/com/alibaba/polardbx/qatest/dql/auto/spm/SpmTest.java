package com.alibaba.polardbx.qatest.dql.auto.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.clearspring.analytics.util.Lists;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * @author fangwu
 */
@FixMethodOrder(MethodSorters.JVM)
public class SpmTest extends BaseTestCase {
    private static final String DB_NAME = "SPM_TEST_DB";
    private static final String TB_NAME = "SPM_TEST_TB";

    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `l_i_order` (`order_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`order_id`) partitions 2";

    private static final String BASELINE_ADD =
        "baseline add sql /*TDDL:a()*/ select 1 from %s a join %s b on a.%s=b.%s";

    private static final String BASELINE_INSERT =
        "INSERT IGNORE INTO SPM_BASELINE (ID, INST_ID, SCHEMA_NAME, GMT_MODIFIED, GMT_CREATED, `SQL`, TABLE_SET, EXTEND_FIELD) VALUES "
            + "(?, ?, ?, now(), now(), ?, '', 'test_marketing_spm_test')";

    private static final String PLAN_INSERT =
        "INSERT IGNORE INTO SPM_PLAN (ID, INST_ID, SCHEMA_NAME, BASELINE_ID, GMT_MODIFIED, GMT_CREATED, PLAN, CHOOSE_COUNT, COST, ESTIMATE_EXECUTION_TIME, ACCEPTED, FIXED, TRACE_ID,TABLES_HASHCODE, EXTEND_FIELD) VALUES "
            + "(?, ?, ?, ?, now(), now(), '', 1, 1.0, 1, 1, 0, '', -1, 'test_marketing_spm_test')";

    private static final String CREATE_BASELINE_INFO =
        "create table if not exists `" + GmsSystemTables.BASELINE_INFO + "` (\n"
            + "  `id` bigint not null,\n"
            + "  `schema_name` varchar(64) not null default '',\n"
            + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
            + "  `gmt_created` timestamp default current_timestamp,\n"
            + "  `sql` mediumtext not null,\n"
            + "  `table_set` text not null,\n"
            + "  `extend_field` longtext default null comment 'json string extend field',\n"
            + "  primary key `id_key` (`schema_name`, `id`)\n"
            + ") engine=innodb default charset=utf8";

    private static final String CREATE_PLAN_INFO =
        "create table if not exists `" + GmsSystemTables.PLAN_INFO + "` (\n"
            + "  `id` bigint(20) not null,\n"
            + "  `schema_name` varchar(64) not null,\n"
            + "  `baseline_id` bigint(20) not null,\n"
            + "  `gmt_modified` timestamp default current_timestamp on update current_timestamp,\n"
            + "  `gmt_created` timestamp default current_timestamp,\n"
            + "  `last_execute_time` timestamp null default null,\n"
            + "  `plan` longtext not null,\n"
            + "  `plan_type` varchar(255) null,\n"
            + "  `plan_error` longtext null,\n"
            + "  `choose_count` bigint(20) not null,\n"
            + "  `cost` double not null,\n"
            + "  `estimate_execution_time` double not null,\n"
            + "  `accepted` tinyint(4) not null,\n"
            + "  `fixed` tinyint(4) not null,\n"
            + "  `trace_id` varchar(255) not null,\n"
            + "  `origin` varchar(255) default null,\n"
            + "  `estimate_optimize_time` double default null,\n"
            + "  `cpu` double default null,\n"
            + "  `memory` double default null,\n"
            + "  `io` double default null,\n"
            + "  `net` double default null,\n"
            + "  `tables_hashcode` bigint not null default 0,\n"
            + "  `extend_field` longtext default null comment 'json string extend field',\n"
            + "  primary key `primary_key` (`schema_name`, `id`, `baseline_id`)\n,"
            + "  key `baseline_id_key` (`schema_name`, `baseline_id`)\n"
            + ") engine=innodb default charset=utf8";

    private static final String MOVE_SPM_BASELINE_TO_BASELINE_INFO =
        "REPLACE INTO BASELINE_INFO (`ID`, `SCHEMA_NAME`, `GMT_MODIFIED`, `GMT_CREATED`, `SQL`, `TABLE_SET`, `EXTEND_FIELD`) "
            + "SELECT  DISTINCT `ID`, `SCHEMA_NAME`, `GMT_MODIFIED`, `GMT_CREATED`, `SQL`, `TABLE_SET`, `EXTEND_FIELD`"
            + " FROM SPM_BASELINE WHERE SCHEMA_NAME='" + DB_NAME + "'";

    private static final String MOVE_SPM_PLAN_TO_PLAN_INFO =
        "REPLACE INTO PLAN_INFO (`ID`, `SCHEMA_NAME`, `BASELINE_ID`, `GMT_MODIFIED`, `GMT_CREATED`, `LAST_EXECUTE_TIME`, `PLAN`, `PLAN_TYPE`, "
            + "`PLAN_ERROR`, `CHOOSE_COUNT`, `COST`, `ESTIMATE_EXECUTION_TIME`, `ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, "
            + "`ESTIMATE_OPTIMIZE_TIME`, `CPU`, `MEMORY`, `IO`, `NET`, `TABLES_HASHCODE`, `EXTEND_FIELD`) "
            + "SELECT distinct `ID`, `SCHEMA_NAME`, `BASELINE_ID`, `GMT_MODIFIED`, `GMT_CREATED`, `LAST_EXECUTE_TIME`, `PLAN`, `PLAN_TYPE`, "
            + "`PLAN_ERROR`, `CHOOSE_COUNT`, `COST`, `ESTIMATE_EXECUTION_TIME`, `ACCEPTED`, `FIXED`, `TRACE_ID`, `ORIGIN`, "
            + "`ESTIMATE_OPTIMIZE_TIME`, `CPU`, `MEMORY`, `IO`, `NET`, `TABLES_HASHCODE`, `EXTEND_FIELD` FROM SPM_PLAN WHERE SCHEMA_NAME='"
            + DB_NAME + "'";

    private static final String DELETE_SPM_BASELINE_BY_SCHEMA =
        "DELETE FROM SPM_BASELINE WHERE SCHEMA_NAME='" + DB_NAME + "'";

    private static final String DELETE_SPM_PLAN_BY_SCHEMA = "DELETE FROM SPM_PLAN WHERE SCHEMA_NAME='" + DB_NAME + "'";

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));

            String baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "ID", "ID");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "ID", "order_id");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "order_id", "ID");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "buyer_id", "order_id");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "buyer_id", "buyer_id");
            c.createStatement().execute(baselineAddSql);

            baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "seller_id", "seller_id");
            c.createStatement().execute(baselineAddSql);

            c.createStatement().execute("baseline persist");
        }
    }

    @AfterClass
    public static void clean() throws SQLException {
        cleanUp();
    }

    private static void cleanUp() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("delete from metadb.spm_baseline where EXTEND_FIELD='test_marketing_spm_test'");
            c.createStatement().execute("delete from metadb.spm_plan where EXTEND_FIELD='test_marketing_spm_test'");
        }
    }

    @Test
    public void testInstIdExist() throws Exception {
        // test metadb table meta
        try (Connection c = getMetaConnection()) {
            ResultSet rs = c.createStatement().executeQuery("desc spm_baseline");
            boolean hasInstId = false;
            while (rs.next()) {
                String fieldName = rs.getString("field");
                if (fieldName.equalsIgnoreCase("INST_ID")) {
                    hasInstId = true;
                    break;
                }
            }

            assert hasInstId;

            rs = c.createStatement().executeQuery("desc spm_baseline");
            hasInstId = false;
            while (rs.next()) {
                String fieldName = rs.getString("field");
                if (fieldName.equalsIgnoreCase("INST_ID")) {
                    hasInstId = true;
                    break;
                }
            }

            assert hasInstId;
        }
    }

    @Ignore
    public void testPlanCurd() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // check baseline in mem
            ResultSet rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "'");

            rs.next();
            int count = rs.getInt(1);
            assert count == 6;
            rs.close();

            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery("select count(*) from metadb.spm_baseline where schema_name='" + DB_NAME
                    + "'");

            rs.next();
            count = rs.getInt(1);
            assert count == 6;
            rs.close();

            rs = c.createStatement()
                .executeQuery("select count(*) from metadb.spm_plan where schema_name='" + DB_NAME
                    + "' AND INST_ID IS NOT NULL");

            rs.next();
            count = rs.getInt(1);
            assert count == 6;
            rs.close();

            // test delete plan
            // get plan id
            rs = c.createStatement()
                .executeQuery(
                    "select baseline_id, plan_id from information_schema.spm where schema_name='" + DB_NAME
                        + "' and ACCEPTED");

            rs.next();
            String planId = rs.getString("PLAN_ID");
            int baselineId = rs.getInt("baseline_id");
            rs.close();
            // delete plan by plan id
            c.createStatement().execute("baseline delete " + baselineId);
            // check baseline in mem
            rs = c.createStatement()
                .executeQuery(
                    "select count(1) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and baseline_id=" + baselineId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and plan_id=" + planId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();

            // test delete baseline
            c.createStatement().execute("baseline delete " + baselineId);
            // check baseline in mem
            rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and baseline_id=" + baselineId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();

            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery(
                    "select count(distinct BASELINE_ID) from information_schema.spm where schema_name='" + DB_NAME
                        + "' and baseline_id=" + baselineId);

            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
        }
    }

    @Test
    public void testSpmReload() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // test baseline reload
            String baselineAddSql = String.format(BASELINE_ADD, TB_NAME, TB_NAME, "seller_id", "seller_id");
            ResultSet rs = c.createStatement().executeQuery(baselineAddSql);
            rs.next();
            int baselineId = rs.getInt("BASELINE_ID");
            rs.close();

            // check if this baseline id in mem
            rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where baseline_id = " + baselineId);
            rs.next();
            assert rs.getInt(1) > 0;
            rs.close();

            // change its inst id
            c.createStatement()
                .executeUpdate("update metadb.spm_baseline set inst_id='spm_test' where id=" + baselineId);

            // reload baseline
            c.createStatement().executeQuery("baseline load");

            // check if this baseline id still in mem
            rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where baseline_id = " + baselineId);
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            // check metadb
            rs = c.createStatement().executeQuery("select count(1) from metadb.spm_baseline where inst_id = ''");
            rs.next();
            int count = rs.getInt(1);
            assert count == 0;
            rs.close();

            rs = c.createStatement().executeQuery("select count(1) from metadb.spm_plan where inst_id =''");
            rs.next();
            count = rs.getInt(1);
            assert count == 0;
            rs.close();
        }
    }

    @Ignore
    public void testPlanMigration() throws Exception {
        try (Connection c = getMetaConnection()) {
            // create baseline_info
            c.createStatement().execute(CREATE_BASELINE_INFO);
            c.createStatement().execute(CREATE_PLAN_INFO);
            c.createStatement().execute(MOVE_SPM_BASELINE_TO_BASELINE_INFO);
            c.createStatement().execute(MOVE_SPM_PLAN_TO_PLAN_INFO);
            // remove plan by schema
            c.createStatement().execute(DELETE_SPM_BASELINE_BY_SCHEMA);
            c.createStatement().execute(DELETE_SPM_PLAN_BY_SCHEMA);
        }

        // refresh baseline
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("set global ENABLE_SPM=true");
            // reload baseline
            c.createStatement().executeQuery("baseline load");

            // check mem
            ResultSet rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) > 0;
            rs.close();

            // check metadb
            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_baseline where schema_name = '" + DB_NAME + "'");
            rs.next();
            int count = rs.getInt(1);
            assert count > 0;
            rs.close();

            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_plan where schema_name = '" + DB_NAME + "'");
            rs.next();
            count = rs.getInt(1);
            assert count > 0;
            rs.close();
        }
    }

    /**
     * trigger schedule job, and check if each cn node got the same baseline collection,
     * and if this baseline collection had been persisted correctly
     */
    @Ignore("test sync job by ut")
    public void testSPMBaseLineSyncScheduledJob() throws Exception {
        // trigger schedule job
        String sql;
        long now = System.currentTimeMillis();
        sql = "select schedule_id from metadb.SCHEDULED_JOBS where executor_type='BASELINE_SYNC'";
        String schedule_id = null;
        try (Connection c = this.getPolardbxConnection()) {
            ResultSet resultSet = c.createStatement().executeQuery(sql);
            resultSet.next();
            schedule_id = resultSet.getString("schedule_id");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        if (schedule_id == null) {
            Assert.fail("Cannot find schedule id for BASELINE_SYNC");
        }
        sql = " fire schedule " + schedule_id;
        JdbcUtil.executeUpdateSuccess(this.getPolardbxConnection(), sql);

        // get all inst id
        ResultSet rs = JdbcUtil.executeQuery("select inst_id, ip from server_info", this.getMetaConnection());
        Set<String> instIds = Sets.newHashSet();
        Set<String> ips = Sets.newHashSet();
        while (rs.next()) {
            instIds.add(rs.getString("inst_id"));
            ips.add(rs.getString("ip"));
        }
        rs.close();
        // get sync merge info
        rs = JdbcUtil.executeQuery(
            "select event from information_schema.module_event where module_name='spm' and "
                + "event like '%spm merge baseline%' and timestamp>FROM_UNIXTIME(" + now + "/1000)",
            this.getPolardbxConnection());

        List<String> syncInstIds = Lists.newArrayList();
        while (rs.next()) {
            String event = rs.getString("event");
            int start = event.indexOf("succeed ended,result: ") + "succeed ended,result: ".length();
            int end = event.indexOf(' ', start);

            syncInstIds.add(event.substring(start, end));
        }

        assert syncInstIds.containsAll(instIds);

        // make sure every node had been synced to load baseline
        sql =
            "select distinct host from information_schema.module_event where module_name='spm' and "
                + "event like '%BaselineLoadSyncAction%' and timestamp>FROM_UNIXTIME(" + now + "/1000)";
        rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        Set<String> hostSet = Sets.newHashSet();
        while (rs.next()) {
            hostSet.add(rs.getString("host"));
        }
        assert hostSet.containsAll(ips);
    }

    /**
     * this test method should be the last one to be executed in this case
     */
    @Ignore("this case will drop database and affect other test cases")
    public void testDropDB() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // drop database
            c.createStatement().execute("use information_schema");
            c.createStatement().execute("drop database " + DB_NAME);

            // check baseline in mem
            ResultSet rs = c.createStatement()
                .executeQuery("select count(1) from information_schema.spm where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            // check baseline in metadb
            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_baseline where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();

            rs = c.createStatement()
                .executeQuery("select count(1) from metadb.spm_plan where schema_name = '" + DB_NAME + "'");
            rs.next();
            assert rs.getInt(1) == 0;
            rs.close();
        }
    }

    /**
     * this test method should be the last one to be executed in this case
     */
    @Test
    public void testBaselineFixArgs() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // prepare table
            String t1 = "test_hash_tb1";
            String t2 = "test_hash_tb2";
            String createTable = "CREATE TABLE IF NOT EXISTS `%s` (\n"
                + "\t`id` int NOT NULL,\n"
                + "\t`name` varchar(30) DEFAULT NULL,\n"
                + "\t`int_col2` int DEFAULT NULL,\n"
                + "\t`create_time` datetime DEFAULT NULL,\n"
                + "\t`name1` varchar(30) DEFAULT NULL,\n"
                + "\t`name2` varchar(30) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb3";
            c.createStatement().execute(String.format(createTable, t1));
            c.createStatement().execute(String.format(createTable, t2));

            // test baseline fix hint
            String hint = "/*TDDL:EXECUTOR_MODE=AP_LOCAL cmd_extra(PARALLELISM=1000, ENable_bka_join=false)*/";
            String sql = "select t1.int_col2 from test_hash_tb1 t1 join test_hash_tb2 t2 on t1.id=t2.id limit 1,1";
            ResultSet rs = c.createStatement().executeQuery("baseline fix sql  " + hint + sql);
            rs.next();
            String plan = rs.getString("PLAN").toLowerCase();
            rs.close();

            Assert.assertTrue(plan.contains("parallelism=1000"));
            Assert.assertTrue(plan.contains("enable_bka_join=false"));
            Assert.assertTrue(plan.contains("executor_mode=ap_local"));

            String executionPlan = explainStr(c, "baseline " + sql).toLowerCase();

            Assert.assertTrue(executionPlan.contains("parallelism=1000"));
            Assert.assertTrue(executionPlan.contains("enable_bka_join=false"));
            Assert.assertTrue(executionPlan.contains("executor_mode=ap_local"));

            // set global ENABLE_PRUNING_IN=true and IN_PRUNE_MAX_TIME=100000
            c.createStatement().execute("set global ENABLE_PRUNING_IN=true");
            c.createStatement().execute("set global IN_PRUNE_MAX_TIME=100000");

            // test IN_PRUNE_MAX_TIME args working correctly in baseline fix
            sql = "select * from test_hash_tb1 where id in (1,2,3,4,5,6,7,8,9)";
            c.createStatement()
                .executeQuery("baseline fix sql  /*TDDL:IN_PRUNE_MAX_TIME=10 IN_SUB_QUERY_THRESHOLD=1000*/" + sql);

            executionPlan = explainStr(c, sql);

            Assert.assertTrue(executionPlan.contains("pruningInfo="));

            sql = "select * from test_hash_tb1 where id in (1,2,3,4,5,6,7,8,9,10,11)";
            executionPlan = explainStr(c, sql);
            Assert.assertTrue(!executionPlan.contains("pruningInfo="));
        }
    }

    @Test
    public void testBaselineFixArgsWithExecutorModeAndWorkloadType() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // prepare table
            String t1 = "test_hash_tb1";
            String t2 = "test_hash_tb2";
            String createTable = "CREATE TABLE IF NOT EXISTS `%s` (\n"
                + "\t`id` int NOT NULL,\n"
                + "\t`name` varchar(30) DEFAULT NULL,\n"
                + "\t`int_col2` int DEFAULT NULL,\n"
                + "\t`create_time` datetime DEFAULT NULL,\n"
                + "\t`name1` varchar(30) DEFAULT NULL,\n"
                + "\t`name2` varchar(30) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb3";
            c.createStatement().execute(String.format(createTable, t1));
            c.createStatement().execute(String.format(createTable, t2));

            // test baseline fix hint
            String hint = "/*TDDL:EXECUTOR_MODE=MPP WORKLOAD_TYPE=AP*/";
            String sql = "select t1.int_col2 from test_hash_tb1 t1 join test_hash_tb2 t2 on t1.id=t2.id limit 1";
            ResultSet rs = c.createStatement().executeQuery("baseline fix sql  " + hint + sql);
            rs.next();
            String plan = rs.getString("PLAN").toLowerCase();
            rs.close();

            Assert.assertTrue(plan.contains("executor_mode=mpp"));
            Assert.assertTrue(plan.contains("workload_type=ap"));

            String executionPlan = explainStr(c, "baseline " + sql).toLowerCase();

            Assert.assertTrue(executionPlan.contains("executor_mode=mpp"));
            Assert.assertTrue(executionPlan.contains("workload_type=ap"));
        }
    }

    /**
     * Join Order Hints:  JOIN_FIXED_ORDER()
     * Optimizer Hints:  MAX_EXECUTION_TIME(1000)
     * Subquery Hints: SEMIJOIN(MATERIALIZATION) NO_SEMIJOIN()
     * resource control:  RESOURCE_GROUP(group_name)
     */
    @Test
    public void testBaselineFixArgsWithDNHint() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // prepare table
            String t1 = "test_hash_tb1";
            String t2 = "test_hash_tb2";
            String createTable = "CREATE TABLE IF NOT EXISTS `%s` (\n"
                + "\t`id` int NOT NULL,\n"
                + "\t`name` varchar(30) DEFAULT NULL,\n"
                + "\t`int_col2` int DEFAULT NULL,\n"
                + "\t`create_time` datetime DEFAULT NULL,\n"
                + "\t`name1` varchar(30) DEFAULT NULL,\n"
                + "\t`name2` varchar(30) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb3";
            c.createStatement().execute(String.format(createTable, t1));
            c.createStatement().execute(String.format(createTable, t2));

            // test baseline fix hint
            String hint = "/*TDDL:DN_HINT=JOIN_FIXED_ORDER()*/";
            String sql = "select t1.int_col2 from test_hash_tb1 t1 join test_hash_tb2 t2 on t1.id=t2.id order by t1.id";
            ResultSet rs = c.createStatement().executeQuery("baseline fix sql  " + hint + sql);
            rs.next();
            String plan = rs.getString("PLAN").toLowerCase();
            rs.close();

            Assert.assertTrue(plan.contains("dn_hint=join_fixed_order()"));

            c.createStatement().executeQuery("trace " + sql);
            rs = c.createStatement().executeQuery("show trace;");
            int count = 0;
            while (rs.next()) {
                String sqlContent = rs.getString("STATEMENT").toLowerCase();
                Assert.assertTrue(sqlContent.contains("/*+join_fixed_order()*/"));
                count++;
            }

            assert count > 0;
        }
    }

    @Test
    public void testShardingWithPushDownColSubquery() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // prepare table
            String t1 = "test_key_tb1";
            String t2 = "test_key_tb2";
            String createTable = "CREATE TABLE IF NOT EXISTS %s(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(id, name)\n"
                + "PARTITIONS 8;";
            c.createStatement().execute(String.format(createTable, t1));
            c.createStatement().execute(String.format(createTable, t2));

            // test baseline fix hint
            String sql =
                "select t2.bid, (select name from test_key_tb1 t1 where t1.id=t2.id) as name from test_key_tb2 t2 where name in('a','b')";
            c.createStatement().executeQuery(sql);

            sql =
                "select t2.bid, (select name from test_key_tb1 t1 where t1.id=t2.id) as name from test_key_tb2 t2 where name in('a','b', 'c')";
            c.createStatement().executeQuery(sql);

        }
    }

    @Test
    public void testBaselineFixExpr() throws Exception {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // prepare table
            String t1 = "test_key_tb1";
            String t2 = "test_key_tb2";
            String createTable = "CREATE TABLE if not exists %s(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(id, name)\n"
                + "PARTITIONS 8;";
            c.createStatement().execute(String.format(createTable, t1));
            c.createStatement().execute(String.format(createTable, t2));

            // baseline fix expr
            String sql = "BASELINE FIX "
                + "EXPR test_key_tb2.bid in (4,5,6) or test_key_tb1.birthday>='2025-03-25' "
                + "SQL /*TDDL:bka_join(t1, t2)*/ "
                + "select * from test_key_tb2 t1 join test_key_tb1 t2 on t1.id=t2.bid where t1.bid=3 and t2.birthday='2025-03-25';";
            c.createStatement().executeQuery(sql);

            sql = "select * from test_key_tb2 t1 join test_key_tb1 t2 on t1.id=t2.bid where t1.bid=3 and t2.birthday='2025-03-25'";

            String explainStr = explainStr(c, sql);

            Assert.assertTrue(explainStr.toLowerCase().contains("bkajoin")&&explainStr.toLowerCase().contains("spm_fix"));

            sql = "select * from test_key_tb2 t1 join test_key_tb1 t2 on t1.id=t2.bid where t1.bid=3 and t2.birthday='2025-03-24'";

            explainStr = explainStr(c, sql);

            Assert.assertTrue(explainStr.toLowerCase().contains("plan_cache"));

            // priority test
            sql = "BASELINE FIX "
                + "SQL /*TDDL:hash_join(test_key_tb2, test_key_tb1)*/ "
                + "select * from test_key_tb2 t1 join test_key_tb1 t2 on t1.id=t2.bid where t1.bid=3 and t2.birthday='2025-03-24'";
            c.createStatement().executeQuery(sql);

            sql = "select * from test_key_tb2 t1 join test_key_tb1 t2 on t1.id=t2.bid where t1.bid=4 and t2.birthday='2025-03-25'";

            explainStr = explainStr(c, sql);

            Assert.assertTrue(explainStr.toLowerCase().contains("bkajoin")&&explainStr.toLowerCase().contains("spm_fix"));

            sql = "select * from test_key_tb2 t1 join test_key_tb1 t2 on t1.id=t2.bid where t1.bid=3 and t2.birthday='2025-03-24'";

            explainStr = explainStr(c, sql);

            Assert.assertTrue(explainStr.toLowerCase().contains("hashjoin")&&explainStr.toLowerCase().contains("spm_fix"));
        }
    }

    @Test
    public void testBaselineClean() throws SQLException {
        String instIdExist;

        try (Connection c = getPolardbxConnection(DB_NAME)) {
            // get current inst id
            String sqlGetInstId =
                "select INST_ID from information_schema.spm where inst_id is not null and inst_id !='' limit 1";
            try (ResultSet rs = c.createStatement().executeQuery(sqlGetInstId)) {
                rs.next();
                instIdExist = rs.getString(1);
            }

            if (StringUtils.isEmpty(instIdExist)) {
                Assert.fail("instId is empty");
            }
        }

        // add other schema baseline
        try (Connection c = getMetaConnection()) {
            String sql = "select 1 from test1 where 1=0";
            int bid = sql.hashCode();

            String schemaName = "spm_test_clean_schema_name1";
            PreparedStatement ps = c.prepareStatement(BASELINE_INSERT);
            ps.setInt(1, bid);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setString(4, sql);
            ps.executeUpdate();

            int pid = -9998;
            ps = c.prepareStatement(PLAN_INSERT);
            ps.setInt(1, pid);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setInt(4, bid);
            ps.addBatch();
            ps.setInt(1, pid + 1);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setInt(4, bid);
            ps.addBatch();
            ps.executeBatch();
        }

        // add other baseline
        try (Connection c = getMetaConnection()) {
            String sql = "select 1 from test1 where 1=0";
            int bid = sql.hashCode();
            String schemaName = DB_NAME;
            PreparedStatement ps = c.prepareStatement(BASELINE_INSERT);
            ps.setInt(1, bid);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setString(4, sql);

            ps.executeUpdate();

            int pid = -9997;
            ps = c.prepareStatement(PLAN_INSERT);
            ps.setInt(1, pid);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setInt(4, bid);
            ps.addBatch();
            ps.setInt(1, pid + 1);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setInt(4, bid);
            ps.addBatch();
            ps.executeBatch();
        }

        // add other plan
        try (Connection c = getMetaConnection()) {
            int bid = -8888;
            String schemaName = DB_NAME;
            PreparedStatement ps = c.prepareStatement(PLAN_INSERT);
            int pid = -9996;
            ps.setInt(1, pid);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setInt(4, bid);
            ps.addBatch();
            ps.setInt(1, pid + 1);
            ps.setString(2, instIdExist);
            ps.setString(3, schemaName);
            ps.setInt(4, bid);
            ps.addBatch();
            ps.executeBatch();
        }

        // sync baseline
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            ResultSet rs = c.createStatement()
                .executeQuery("select schedule_id from metadb.scheduled_jobs where executor_type='BASELINE_SYNC'");

            rs.next();
            int scheduleId = rs.getInt(1);

            c.createStatement().execute("fire schedule " + scheduleId);
        }

        // check if baseline is cleaned
        try (Connection c = getMetaConnection()) {
            ResultSet rs = c.createStatement().executeQuery(
                "select * from spm_baseline where schema_name in ('spm_test_clean_schema_name', 'spm_test_clean_schema_name1') "
                    + "AND id in (-9999, -9998, -9997, -9996)");
            Assert.assertTrue(!rs.next());
        }
    }

    private String explainStr(Connection c, String sql) throws SQLException {
        StringBuilder executionPlan = new StringBuilder();
        try (ResultSet rs = c.createStatement().executeQuery("explain " + sql)) {

            while (rs.next()) {
                executionPlan.append(rs.getString(1));
            }
        }
        return executionPlan.toString();
    }

}

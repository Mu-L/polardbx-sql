
package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.locality.DbConfig;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.collect.Lists;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DryRunAlterTableTest extends DDLBaseNewDBTestCase {

    //    List<String> tableNames = Lists.newArrayList("create_table", "drop_table", "add_column", "drop_column", "change_column", "modify_column"
//        , "rename_table", "truncate_table", "alter_column_default", "add_index", "drop_index");
    public String getDbName() {
        String dbName = "dry_run_auto_db";
        return dbName;
    }

    Map<String, String> db2dn = new HashMap<>();

    List<String> tableNames =
        Lists.newArrayList("dry_run_test_single", "dry_run_test_partition", "dry_run_test_broadcast", "dry_run_test_replicas");

    List<String> partitionSpecs = Lists.newArrayList("single", "partition by hash(b)", "broadcast", "replicas");

    List<String> storageInsts = new ArrayList<>();

    public DryRunAlterTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}});
    }

    @Before
    public void before() throws SQLException {
        storageInsts = getStorageInstIds(getDdlSchema());
        String dbLocalitySpec = getDbLocalitySpec(storageInsts, getDbName());
        String createDatabaseSql =
            String.format(
                "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ create database if not exists %s mode = auto locality = '%s'",
                getDbName(), dbLocalitySpec);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createDatabaseSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + getDbName());
        validateDbLocation(getDbName(), dbLocalitySpec);
    }

//    @After
//    public void after() throws SQLException {
//        String dropDatabaseSql = String.format(" drop database %s ", getDbName());
//        JdbcUtil.executeUpdateSuccess(tddlConnection, dropDatabaseSql);
//    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_01_create_table() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ create table " + tableNames.get(i)
                + "(id int, name varchar(20), b int, primary key(id)) ";
            String logicalSql = sql;
            List<Integer> dbIndexList = new ArrayList<>();
            List<String> distDns = new ArrayList<>();
            JdbcUtil.executeUpdate(tddlConnection, "use " + getDbName());
            JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + tableNames.get(i));
            if (partitionSpecs.get(i).equalsIgnoreCase("single")) {
                dbIndexList = generateDbIndexList(0);
                distDns = generateDbList(dbIndexList);
                String localitySpec = getTableLocalitySpec(distDns);
                logicalSql = sql + partitionSpecs.get(i) + " locality = '" + localitySpec + "'";
            } else if (partitionSpecs.get(i).equalsIgnoreCase("broadcast")) {
                dbIndexList = generateDbIndexList(i);
                distDns = generateDbList(dbIndexList);
                String localitySpec = getTableLocalitySpec(distDns);
                logicalSql = sql + partitionSpecs.get(i) + " locality = '" + localitySpec + "'";
            } else if (partitionSpecs.get(i).equalsIgnoreCase("replicas")) {
                dbIndexList = generateDbIndexList(i);
                distDns = generateDbList(dbIndexList);
                String localitySpec = getTableLocalitySpecForReplica(distDns);
                logicalSql = sql + partitionSpecs.get(i) + " locality = '" + localitySpec + "'";
            } else {
                dbIndexList = generateDbIndexList(i);
                distDns = generateDbList(dbIndexList);
                String localitySpec = getTableLocalitySpec(distDns);
                logicalSql =
                    sql + partitionSpecs.get(i) + " partitions " + distDns.size() + " locality = '" + localitySpec
                        + "'";
            }
            executePhyDdl(sql, dbIndexList);
            JdbcUtil.executeUpdate(tddlConnection, "use " + getDbName());
            JdbcUtil.executeUpdate(tddlConnection, logicalSql + " dryrun = true");
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_02_add_column() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table " + tableNames.get(i) + " add column x1 int";
            String logicalSql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/  alter table " + tableNames.get(i)
                + " add column x1 int dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_03_add_index() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table " + tableNames.get(i) + " add index i_x1(x1)";
            String logicalSql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/  alter table " + tableNames.get(i)
                + " add local index i_x1(x1) dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_04_drop_index() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table " + tableNames.get(i) + " drop index i_x1";
            String logicalSql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i)
                + " drop index i_x1 dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_05_modify_column() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table " + tableNames.get(i) + " modify column name varchar(32)";
            String logicalSql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i)
                + " modify column name varchar(32) dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_06_change_column() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i)
                + " change column name name1 varchar(32)";
            String logicalSql =
                "alter table " + tableNames.get(i) + " change column name name1 varchar(32) dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_07_drop_column() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table " + tableNames.get(i) + " drop column x1";
            String logicalSql = "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i)
                + " drop column x1 dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_08_alter_column_default() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table " + tableNames.get(i) + " modify column name1 varchar(32) default 'mine'";
            String logicalSql =
                " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i)
                    + " modify column name1 varchar(32) default 'mine' dryrun = true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_10_rename_table() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i) + " rename to "
                + tableNames.get(i) + "_bak";
            String logicalSql = sql + " dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i) + "_bak")) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i) + "_bak", tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "update " + tableNames.get(i) + "_bak" + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + "_bak" + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_11_rename_table_back() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql =
                " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table " + tableNames.get(i) + "_bak" + " rename to "
                    + tableNames.get(i);
            String logicalSql = sql + " dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i) + "_bak");
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_12_optimize_table() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "optimize table " + tableNames.get(i);
            String logicalSql =
                " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ optimize table " + tableNames.get(i) + "  dryrun = true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_13_truncate_table() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "truncate table " + tableNames.get(i);
            String logicalSql =
                " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/  truncate table " + tableNames.get(i) + "  dryrun = true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_14_create_index() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "create index i_b on " + tableNames.get(i) + " (b)";
            String logicalSql = " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ create index i_b on " + tableNames.get(i)
                + "(b)  dryrun = true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_15_rename_index() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "alter table  " + tableNames.get(i) + " rename index i_b to i_c";
            String logicalSql = " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ alter table  " + tableNames.get(i)
                + " rename index i_b to i_c" + " dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
            if (!checkTableOk(tddlConnection, tableNames.get(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " create table failed");
            }
            JdbcUtil.executeQuery("select * from " + tableNames.get(i), tddlConnection);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "update " + tableNames.get(i) + " set b = 2 where 1 = 1");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + tableNames.get(i) + " where 1 = 1");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void test_20_drop_table() {
        for (int i = 0; i < tableNames.size(); i++) {
            String sql = "drop table " + tableNames.get(i);
            String logicalSql =
                " /*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ drop table " + tableNames.get(i) + " dryrun=true";
            List<String> groupList = showTopologyForLogicalTable(tddlConnection, tableNames.get(i));
            executePhyDdlViaGroup(sql, groupList);
            JdbcUtil.executeUpdate(tddlConnection, logicalSql);
        }
    }

    public List<String> showTopologyForLogicalTable(Connection tddlConnection, String tableName) {
        String showTopologySql = "show topology " + tableName;
        ResultSet resultSet = JdbcUtil.executeQuery(showTopologySql, tddlConnection);
        List<String> groups = new ArrayList<>();
        try {
            while (resultSet.next()) {
                groups.add(resultSet.getString("GROUP_NAME"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return groups;
    }

    public void validateDbLocation(String dbName, String dbLocalitySpec) {
        String showDsSql = String.format("show ds where db = '%s' ", dbName);
        Map<String, String> groupAndStorageMap = new HashMap<>();
        try (ResultSet resultSet = JdbcUtil.executeQuery(showDsSql, tddlConnection)) {
            while (resultSet.next()) {
                groupAndStorageMap.put(resultSet.getString("GROUP"), resultSet.getString("STORAGE_INST_ID"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        LocalityDesc localityDesc = LocalityDesc.parse(dbLocalitySpec);
        Map<String, List<String>> proxyConfig = localityDesc.getProxyConfig();
        Boolean validate = true;
        for (String group : groupAndStorageMap.keySet()) {
            if (proxyConfig.containsKey(group)) {
                String storageInst = proxyConfig.get(group).get(0);
                if (!StringUtils.equalsIgnoreCase(storageInst, groupAndStorageMap.get(group))) {
                    validate = false;
                }
            } else {
                validate = false;
            }
        }
        if (!validate) {
            throw new RuntimeException(
                "invalid db location: " + JSON.toJSONString(groupAndStorageMap) + "; " + JSON.toJSONString(
                    proxyConfig));
        }
    }

    public String getDbLocalitySpec(List<String> storageInsts, String dbName) {
        String dbConfigPrefix = "dble_config=";
        Map<String, List<String>> elements = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            String phyDbName = String.format("%s_%d", getDbName(), i);
            String storageInst = storageInsts.get(i % storageInsts.size());
            db2dn.put(phyDbName, storageInst);
            String groupName = String.format("%s_%d_group", getDbName(), i);
            elements.put(groupName, Lists.newArrayList(storageInst, phyDbName));
//            elements.add(String.format("%s_%d:%s", getDbName(), i, storageInsts.get(i % storageInsts.size())));
        }
        DbConfig dbConfig = new DbConfig(elements);
        String localitySpec = dbConfigPrefix + JSON.toJSONString(dbConfig);
        return localitySpec;
    }

    public String getTableLocalitySpec(List<String> dbList) {
        String dbConfig = "db_pools=";
        String localitySpec = dbConfig + StringUtils.join(",", dbList);
        return localitySpec;
    }

    public String getTableLocalitySpecForReplica(List<String> dbList) {
        String dbConfig = "db_set=";
        String localitySpec = dbConfig + StringUtils.join(",", dbList);
        return localitySpec;
    }

    public List<Integer> generateDbIndexList(int i) {
        Random random = new Random();
        HashSet<Integer> sets = new HashSet<>();
        while (sets.size() <= i) {
            int rand = random.nextInt(db2dn.size());
            sets.add(rand);
        }
        return sets.stream().sorted(Comparator.comparingInt(o -> o)).collect(Collectors.toList());
    }

    public List<String> generateDbList(List<Integer> indexes) {
        return indexes.stream().map(o -> String.format("%s_%d_group", getDbName(), o)).collect(Collectors.toList());
    }

    public Boolean checkTableOk(Connection tddlConnection, String tableName) {
        List<List<Object>> results =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery("check table " + tableName, tddlConnection));
        return results.stream().allMatch(o -> o.get(o.size() - 1).toString().equalsIgnoreCase("ok"));

    }

    public void executePhyDdl(String sql, List<Integer> dbList) {
        List<Connection> connections = getMySQLPhysicalConnectionList(getDbName());
        for (Integer dbIndex : dbList) {
            Connection connection = connections.get(dbIndex);
            JdbcUtil.executeUpdateSuccess(connection, sql);
        }
    }

    public void executePhyDdlViaGroup(String sql, List<String> groupList) {
        for (String group : groupList) {
            Connection connection = getMySQLPhysicalConnectionByGroupName(getDbName(), group);
            JdbcUtil.executeUpdateSuccess(connection, sql);
        }
    }
}
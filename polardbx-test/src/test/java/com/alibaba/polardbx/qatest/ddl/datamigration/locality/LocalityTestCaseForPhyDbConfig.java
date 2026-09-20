package com.alibaba.polardbx.qatest.ddl.datamigration.locality;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.gms.locality.DbConfig;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestCaseTask;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;

@NotThreadSafe
public class LocalityTestCaseForPhyDbConfig extends LocalityTestBase {

    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException, SQLException {

        /**
         * Ignore this case for debug ddl qatest 
         */
        String resourceDir = "partition/env/LocalityTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        LocalityTestCaseTask localityTestCaseTask = new LocalityTestCaseTask(fileDir);
        localityTestCaseTask.execute(tddlConnection);
    }

    /*
     * for create partition table.
     * (hash_partition, range_partition, list_partition)
     * (full_part_spec, non_full_part_spec)
     * (int_partition_key, string_partition_key)
     * (with_gsi, without_gsi)
     * (table_level_locality, partition_level_localiy, table_and_partition_level_locality, no_locality)
     *
     * for create other table
     * (broadcast_table, single_table)
     * (with_gsi, without_gsi)
     *
     * for repartition
     * (broad->single, broad->partition, partition->single, partiton->broadcast, single->broad, single->partition)
     *
     * for modify partition
     * (move, add, split, merge, split_by_hot_value, extract)
     *
     * for set locality
     *
     * for rebalance
     */

    @Test
    public void testParse1() {
        String msg =
            "{\"d_you_k1\":[\"polardbx-storage-0-master\", \"d_you1\"],\"d_you_k2\":[\"polardbx-storage-2-master\", \"d_you2\"]}";
        DbConfig result = JSON.parseObject(msg, DbConfig.class);
        System.out.println(JSON.toJSONString(result));
    }

    @Test
    public void testParse2() {
        List<String> dns = getDatanodes(tddlConnection);
        String dn0 = dns.get(0);
        String dn1 = dns.get(1);
        String localitySpec = String.format(
            "LOCALITY = 'dble_config={\"group_config\":{\"d_you_k2\":[\"%s\",\"d_you2\"],\"d_you_k1\":[\"%s\",\"d_you1\"]}}'",
            dn0, dn1);
        String dbName = "d_you_k";
        String showCreateDatabaseSql =
            String.format("/*+TDDL:cmd_extra(ENABLE_OUTPUT_STORAGE_LABEL=false)*/SHOW CREATE DATABASE %s", dbName);
        String createDatabaseSql =
            String.format("CREATE DATABASE `%s` CHARSET = `utf8mb4` COLLATE = `utf8mb4_general_ci` MODE = 'auto' %s",
                dbName, localitySpec);
        String dropDatabaseSql = String.format("DROP DATABASE `%s`", dbName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createDatabaseSql);
        List<List<Object>> results =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showCreateDatabaseSql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropDatabaseSql);
        String result = results.get(0).get(1).toString();
        System.out.println(result);
        Assert.assertTrue(result.equalsIgnoreCase(createDatabaseSql));
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void testCreateSimpleTable() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("create_table_physical_config.test.yml");
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void testCreateSimpleTable2() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("create_table_physical_config2.test.yml");
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void testCreateSimpleTableNew() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("create_table_physical_config_new.test.yml");
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void testCreateSimpleTableNew2() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("create_table_physical_config_new2.test.yml");
    }

//    @Test
//    public void testDatabaseLocality() throws FileNotFoundException, InterruptedException, SQLException {
//        runTestCase("database_locality.test.yml");
//    }
}

package com.alibaba.polardbx.qatest.dql.auto.replicas;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@CdcIgnore(ignoreReason = "CDC不支持ReplicasTable相关用例")
public class ReplicasTableDqlTest extends AutoReadBaseTestCase {

    private final static String db1 = "dble_test_db1";

    private static int groupCountPerStorage = 2;
    private static int groupCount;


    @BeforeClass
    public static void beforeClass() throws Exception{
        Connection connection = getPolardbxConnection0();
        List<String> storageList = getStorage(connection);
        groupCount = storageList.size() * groupCountPerStorage;
        JSONObject groupConfig = new JSONObject();
        for (int i = 0; i < groupCount; i++){
            String groupName = getGroupName(db1, i);
            String partDbName = getPartDbName(db1, i);
            String storageName = storageList.get(i % storageList.size());
            JSONArray groupConfigItem = new JSONArray();
            groupConfigItem.add(storageName);
            groupConfigItem.add(partDbName);
            groupConfig.put(groupName, groupConfigItem);
        }
        JSONObject dbleConfig = new JSONObject();
        dbleConfig.put("group_config", groupConfig);

        String createDatabase = "CREATE DATABASE `" + db1 + "` CHARSET = `utf8` COLLATE = `utf8_general_ci` MODE = 'auto' " +
                "LOCALITY = 'dble_config=" + dbleConfig.toString() + "'";
        JdbcUtil.dropDatabase(connection, db1);
        JdbcUtil.executeSuccess(connection, createDatabase);
    }

    @AfterClass
    public static void afterClass() throws Exception{
        Connection connection = getPolardbxConnection0();
        JdbcUtil.dropDatabase(connection, db1);
    }

    private static String getGroupName(String db, int groupIndex){
        return db + "_g" + groupIndex;
    }

    private static String getPartDbName(String db, int groupIndex){
        return db + "_d" + groupIndex;
    }

    public static List<String> getStorage(Connection connection) throws SQLException {
        String sql = "show storage";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        List<String> storageList = new ArrayList<>();
        while (rs.next()){
            String instKind = rs.getString("INST_KIND");
            if (!instKind.equalsIgnoreCase("MASTER")){
                continue;
            }
            storageList.add(rs.getString("STORAGE_INST_ID"));
        }
        return storageList;
    }

    @Test
    public void testReplicasQuery() throws Exception{
        Connection connection = getPolardbxConnection(db1);
        for (int i = 1; i <= groupCount; i++){
            List<String> locality = new ArrayList<>();
            for (int j = groupCount - i; j < groupCount; j++){
                locality.add(getGroupName(db1, j));
            }
            String tb1 = "testReplicasQuery_tb1";
            JdbcUtil.dropTable(connection, tb1);
            String createTableSql = "create table " + tb1 + " (id int primary key, col1 int) replicas";
            createTableSql += " locality='db_set=" + String.join(",", locality) + "'";
            JdbcUtil.executeSuccess(connection, createTableSql);
            testReplicasQuery(connection, db1, tb1, locality);
            JdbcUtil.dropTable(connection, tb1);
        }
    }

    public void testReplicasQuery(Connection connection, String db, String tb, List<String> locality) throws Exception{
        ResultSet topologyRs = JdbcUtil.executeQuery("show topology from " + tb, connection);
        Map<String, String> phyTableNameMap = new HashMap<>();
        Map<String, String> partNameMap = new HashMap<>();
        Map<String, String> phyDbNameMap = new HashMap<>();

        while (topologyRs.next()){
            String groupName = topologyRs.getString("GROUP_NAME");
            String partitionName = topologyRs.getString("PARTITION_NAME");
            String physicalTableName = topologyRs.getString("TABLE_NAME");
            String physicalDbName = topologyRs.getString("PHY_DB_NAME");
            phyDbNameMap.put(groupName, physicalDbName);
            phyTableNameMap.put(groupName, physicalTableName);
            partNameMap.put(groupName, partitionName);
        }

        for (String groupName : locality){
            Connection groupConnection = getMySQLPhysicalConnectionByGroupName(db, groupName);
            String insertSql = "insert into " + phyTableNameMap.get(groupName) + " values (0,0),(1, 1),(2,2),(3,3)";
            JdbcUtil.executeUpdate(groupConnection, insertSql);
            groupConnection.close();
        }

        int tableRowCount = 4;

        testReplicasNormalQuery(connection, tb, tableRowCount);
        testReplicasScanQuery(connection, tb, locality, tableRowCount);
        testReplicasNodeQuery(connection, tb, locality.stream().map(partNameMap::get).collect(Collectors.toList()), tableRowCount);
    }

    public void testReplicasNormalQuery(Connection connection, String tb, int tableRowCount) throws Exception{
        String selectSql = "select * from " + tb + " order by id";
        List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(selectSql, connection));
        int resRowCount = tableRowCount;
        Assert.assertEquals(resRowCount, result.size());
        for (int i = 0; i < resRowCount; i++){
            for (Object res : result.get(i)){
                int intRes = ((JdbcUtil.MyNumber) res).getNumber().intValue();
                Assert.assertEquals(intRes, i);
            }
        }
    }

    public void testReplicasScanQuery(Connection connection, String tb, List<String> locality, int tableRowCount) throws Exception{
        String selectSql = "/*+TDDL:scan()*/select * from " + tb + " order by id";
        List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(selectSql, connection));
        int rowcount = tableRowCount * locality.size();
        Assert.assertEquals(rowcount, result.size());
        for (int i = 0; i < rowcount; i++){
            for (Object res : result.get(i)){
                int intRes = ((JdbcUtil.MyNumber) res).getNumber().intValue();
                Assert.assertEquals(intRes, i % tableRowCount);
            }
        }
    }

    public void testReplicasNodeQuery(Connection connection, String tb, List<String> partitionName, int tableRowCount) throws Exception{
        for (String partition : partitionName){
            String selectSql = "/*+TDDL:node(" + partition + ")*/select * from " + tb + " order by id";
            List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(selectSql, connection));
            int rowcount = tableRowCount;
            Assert.assertEquals(rowcount, result.size());
            for (int i = 0; i < rowcount; i++){
                for (Object res : result.get(i)){
                    int intRes = ((JdbcUtil.MyNumber) res).getNumber().intValue();
                    Assert.assertEquals(intRes, i % tableRowCount);
                }
            }
        }
    }




}

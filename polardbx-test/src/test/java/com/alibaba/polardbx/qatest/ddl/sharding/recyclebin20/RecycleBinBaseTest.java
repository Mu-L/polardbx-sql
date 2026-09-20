package com.alibaba.polardbx.qatest.ddl.sharding.recyclebin20;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;

import java.sql.Connection;

public class RecycleBinBaseTest extends DDLBaseNewDBTestCase {

    private static final String createTableName = "create table %s (a int, b vachar(20), c char(20), d datetime, e int ,f text %s) %s";
    private static final String autoPrimaryPartitionDefinition1 = "partition by key(a) partitionss 3";
    private static final String autoPrimaryPartitionDefinition2 = "partition by key(e) partitionss 3";
    private static final String drdsPrimaryPartitionDefinition1 = "dbpartition by hash(a) tbpartition by hash(a) tbpartitions 2";
    private static final String drdsPrimaryPartitionDefinition2 = "dbpartition by hash(e) tbpartition by hash(e) tbpartitions 2";

    public void createTable(Connection conn, String tableName, int numOfGsi) {
        if (usingNewPartDb()) {
            createAutoTable(conn, tableName, numOfGsi);
        } else {
            createDrdsTable(conn, tableName, numOfGsi);
        }
    }

    private void createAutoTable(Connection conn, String tableName, int numOfGsi) {
        String gsiInfo = "";
        for (int i = 0; i < numOfGsi; i++) {
            gsiInfo += String.format(", global index gid_%d(e) %s", i, autoPrimaryPartitionDefinition2);
        }
        String createTableSql = String.format(createTableName, tableName, gsiInfo, autoPrimaryPartitionDefinition1);
        JdbcUtil.executeUpdate(conn, createTableSql);
    }

    private void createDrdsTable(Connection conn, String tableName, int numOfGsi) {
        String gsiInfo = "";
        for (int i = 0; i < numOfGsi; i++) {
            gsiInfo += String.format(", global index %s(e) %s", RandomUtils.getStringBetween(4, 6), drdsPrimaryPartitionDefinition2);
        }
        String createTableSql = String.format(createTableName, tableName, gsiInfo, drdsPrimaryPartitionDefinition1);
        JdbcUtil.executeUpdate(conn, createTableSql);
    }
}

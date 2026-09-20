package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;

/**
 * Regression test for AONE-85493899: the CN port of the MySQL time parser only
 * recognizes '.', ',', '-' and ':' as separators, while MySQL accepts all ASCII
 * punctuation characters. As a result, slash-style date literals such as
 * '2021/08/11' are parsed to null, which breaks subpartition pruning, silently
 * filters out rows and misroutes DML. After the fix, slash dates must behave
 * exactly the same as dash dates.
 */
public class SlashDatePartitionPruningTest extends PartitionTestBase {

    private static final String TABLE_NAME = "tms_bill_flight_record_slash";

    @Before
    public void setUpEnv() {
        dropTbl("drop table if exists " + TABLE_NAME);
        String createSql = "create table " + TABLE_NAME + " (\n"
            + "  id bigint not null auto_increment,\n"
            + "  database_shard varchar(32) not null,\n"
            + "  flight_date date not null,\n"
            + "  primary key (id, database_shard, flight_date)\n"
            + ") partition by key(database_shard, id) partitions 16\n"
            + "  subpartition by range(month(flight_date)) (\n"
            + "    subpartition sp1 values less than (4),\n"
            + "    subpartition sp2 values less than (7),\n"
            + "    subpartition sp3 values less than (10),\n"
            + "    subpartition sp4 values less than (maxvalue)\n"
            + "  )";
        createTbl(createSql);
    }

    @After
    public void setDownEnv() {
        dropTbl("drop table if exists " + TABLE_NAME);
        JdbcUtil.updateDataTddl(tddlConnection, "set @auto_partition=1;", null);
    }

    private void createTbl(String createSql) {
        JdbcUtil.updateDataTddl(tddlConnection, "set @auto_partition=0;", null);
        JdbcUtil.executeSuccess(tddlConnection, createSql);
    }

    private void dropTbl(String dropSql) {
        JdbcUtil.updateDataTddl(tddlConnection, dropSql, null);
    }

    @Test
    public void testSlashDatePruningAndRouting() {
        JdbcUtil.executeSuccess(tddlConnection,
            "insert into " + TABLE_NAME + " (database_shard, flight_date) values ('s1', '2021-08-11')");
        JdbcUtil.executeSuccess(tddlConnection,
            "insert into " + TABLE_NAME + " (database_shard, flight_date) values ('s2', '2021/08/11')");

        long dashCount = countBySql("flight_date = '2021-08-11'");
        Assert.assertEquals("dash date literal should match both inserted rows", 2L, dashCount);

        long slashCount = countBySql("flight_date = '2021/08/11'");
        Assert.assertEquals("slash date literal must behave the same as dash date literal", 2L, slashCount);

        long multiCondSlashCount = countBySql("database_shard = 's2' and flight_date = '2021/08/11'");
        Assert.assertEquals("multi-condition query with slash date must not silently lose data",
            1L, multiCondSlashCount);

        int updateAffected = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection,
            "update " + TABLE_NAME + " set database_shard = database_shard where flight_date = '2021/08/11'");
        Assert.assertEquals("update with slash date should affect both rows", 2, updateAffected);

        int deleteAffected = JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection,
            "delete from " + TABLE_NAME + " where flight_date = '2021/08/11'");
        Assert.assertEquals("delete with slash date should affect both rows", 2, deleteAffected);
    }

    private long countBySql(String whereSql) {
        String sql = "select count(*) from " + TABLE_NAME + " where " + whereSql;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue("count query should return one row: " + sql, rs.next());
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }
}

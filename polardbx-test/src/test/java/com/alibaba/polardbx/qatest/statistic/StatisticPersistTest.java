package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableColumnStatistic;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableLogicalTableStatistic;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Test;

import java.sql.Connection;

public class StatisticPersistTest extends AutoReadBaseTestCase {

    public final String DB_NAME = PropertiesUtil.polardbXDBName1(usingNewPartDb());

    @Test
    public void testDuplicateEntryForPrimaryKey() throws Exception {
        if (isMySQL80()){
            return;
        }
        String tableName = "t_statistic_persist_test";
        String createTableSql = "create table " + tableName + " (id int primary key, col1 int, col2 int, col3 int, col4 int," +
                " key idx1(col1), key idx2(col2), key idx3(col3), key idx4(col4) ) partition by hash(id)";
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        for (int i = 0; i < 1000; i++){
            JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + tableName + " values (" + i + ", " + i + ", " + i + ", " + i + ", " + i + ")");
        }

        String collectStatisticSql = "/*+TDDL:ENABLE_COLLECT_HLL=true*/collect statistic " + DB_NAME + "." + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, collectStatisticSql);

        try (Connection metaConn = getMetaConnection()) {
            String updateSql;
            long columnStatisticMaxId = MetaDbUtil.getMaxId(metaConn, PolarDbXSystemTableColumnStatistic.TABLE_NAME);
            for (int i = 1; i <= 4; i++){
                //mysql57使用update语句更新auto_increment列，不会更新sequence
                updateSql = String.format("update " + PolarDbXSystemTableColumnStatistic.TABLE_NAME
                        + " set id=%s where schema_name='%s' and table_name='%s' and column_name='%s'", columnStatisticMaxId + i, DB_NAME, tableName, "col" + i);
                JdbcUtil.executeUpdateSuccess(metaConn, updateSql);
            }

            long tableStatisticMaxId = MetaDbUtil.getMaxId(metaConn, PolarDbXSystemTableLogicalTableStatistic.TABLE_NAME);
            updateSql = String.format("update " + PolarDbXSystemTableLogicalTableStatistic.TABLE_NAME
                    + " set id=%s where schema_name='%s' and table_name='%s'", tableStatisticMaxId + 1, DB_NAME, tableName);
            JdbcUtil.executeUpdateSuccess(metaConn, updateSql);

        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, collectStatisticSql);
        JdbcUtil.dropTable(tddlConnection, tableName);
    }

}

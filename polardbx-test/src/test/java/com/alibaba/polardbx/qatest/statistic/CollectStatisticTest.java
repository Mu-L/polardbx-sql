package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.handler.CollectStatisticHandler;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableColumnStatistic;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableLogicalTableStatistic;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author fangwu
 */
public class CollectStatisticTest extends BaseTestCase {
    static final String collectSql = "COLLECT STATISTIC";

    @Test
    public void testSampleSketchJob() throws SQLException, InterruptedException {
        long now = System.currentTimeMillis();

        // start collect statistic
        Connection c = this.getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(c, collectSql);

        // waiting job done
        boolean hasSample = false;
        boolean hasPersist = false;
        boolean hasSync = false;
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "' AND (trace_info like '%ServerExecutor%' or trace_info like '%"
                + CollectStatisticHandler.PARALLEL_STATISTIC_EXECUTOR_NAME + "%')";
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");

        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (!hasSample && event.contains("statistic sample started")) {
                hasSample = true;
                System.out.println(event);
                System.out.println("hasSample");
            } else if (!hasPersist && event.contains("persist tables statistic")) {
                hasPersist = true;
                System.out.println(event);
                System.out.println("hasPersist");
            } else if (!hasSync && event.contains("sync statistic info")) {
                hasSync = true;
                System.out.println(event);
                System.out.println("hasSync");
            }
            if (hasSample && hasSync && hasPersist) {
                break;
            }
        }
        Assert.assertTrue(hasSample && hasSync && hasPersist);
    }

    @Test
    public void testCancelCollectStatistic() throws Exception {
        try (Connection connection1 = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Connection connection2 = ConnectionManager.getInstance().getDruidPolardbxConnection();
        ) {
            int connectionId =
                Integer.parseInt(JdbcUtil.executeQueryAndGetFirstStringResult("select connection_id()", connection1));
            new Thread(() -> {
                try {
                    JdbcUtil.executeQuery("collect statistic", connection1);
                } catch (Throwable e) {
                    System.out.println("collect statistic failed : " + e.getMessage());
                }
            }).start();

            //30s内等待progress view创建
            long startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() - startTime < 30000) {
                    ResultSet rs = JdbcUtil.executeQuery(
                        "select * from information_schema.collect_statistic_progress where connection_id = "
                            + connectionId, connection2);
                    if (rs.next()) {
                        Assert.assertTrue(rs.getInt("CONNECTION_ID") == connectionId);
                        break;
                    }
                } else {
                    Assert.fail("information_schema.collect_statistic_progress create view failed");
                }
            }

            //中断collect statistic
            JdbcUtil.executeQuery("cancel collect statistic " + connectionId, connection2);

            //30s内等待等待progress view删除
            startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() - startTime < 30000) {
                    ResultSet rs = JdbcUtil.executeQuery(
                        "select * from information_schema.collect_statistic_progress where connection_id = "
                            + connectionId, connection2);
                    if (!rs.next()) {
                        break;
                    }
                } else {
                    Assert.fail("information_schema.collect_statistic_progress delete view failed");
                }

            }
        }
    }

    @Test
    public void testCollectStatisticProgressView() throws Exception {

        try (Connection connection1 = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Connection connection2 = ConnectionManager.getInstance().getDruidPolardbxConnection();
        ) {
            int connectionId =
                Integer.parseInt(JdbcUtil.executeQueryAndGetFirstStringResult("select connection_id()", connection1));
            new Thread(() -> {
                try {
                    JdbcUtil.executeQuery("collect statistic", connection1);
                } catch (Throwable e) {
                    System.out.println("collect statistic failed : " + e.getMessage());
                }
            }).start();

            //30s内等待progress view创建
            long startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() - startTime < 30000) {
                    ResultSet rs = JdbcUtil.executeQuery(
                        "select * from information_schema.collect_statistic_progress where connection_id = "
                            + connectionId, connection2);
                    if (rs.next()) {
                        Assert.assertTrue(rs.getInt("CONNECTION_ID") == connectionId);
                        break;
                    }
                } else {
                    Assert.fail("information_schema.collect_statistic_progress create view failed");
                }
            }

            //中断collect statistic
            JdbcUtil.executeQuery("kill " + connectionId, connection2);

            //30s内等待等待progress view删除
            startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() - startTime < 30000) {
                    ResultSet rs = JdbcUtil.executeQuery(
                        "select * from information_schema.collect_statistic_progress where connection_id = "
                            + connectionId, connection2);
                    if (!rs.next()) {
                        break;
                    }
                } else {
                    Assert.fail("information_schema.collect_statistic_progress delete view failed");
                }

            }
        }

    }

    public List<String> createDbAndTb(Connection connection, String dbname, int tableNum) throws Exception {
        JdbcUtil.useDb(connection, "polardbx");
        JdbcUtil.dropDatabase(connection, dbname);
        JdbcUtil.createPartDatabase(connection, dbname);
        JdbcUtil.useDb(connection, dbname);

        List<String> tableNames = new ArrayList<>();

        String tbNamePrefix = "tb_";
        String firstTableName = tbNamePrefix + 0;
        String localIndexName = "local_idx";
        String gsiName = "global_idx";

        //create table tb_0
        String createTable =
            "create table %s(id int primary key auto_increment, name varchar(20), val int, phone int, card int) partition by key(id)";
        String createLocalIndex = "create index " + localIndexName + " on %s(val, phone, card, name)";
        String createGSI = "create global index " + gsiName + " on %s(val, phone, card, name) partition by key(val)";

        JdbcUtil.executeSuccess(connection, String.format(createTable, firstTableName));
        JdbcUtil.executeSuccess(connection, String.format(createLocalIndex, firstTableName));
        JdbcUtil.executeSuccess(connection, String.format(createGSI, firstTableName));
        tableNames.add(firstTableName);

        //insert table tb_0
        String insertSql = String.format("insert into %s(name, val, phone, card) values(?, ?, ?, ?)", firstTableName);
        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
        for (int i = 0; i < 100; i++) {
            preparedStatement.setString(1, "name" + i);
            preparedStatement.setInt(2, i);
            preparedStatement.setInt(3, i + 1);
            preparedStatement.setInt(4, i + 2);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();

        String createTableLke = "create table %s like " + firstTableName;
        String insertIntoSelect = "insert into %s select * from " + firstTableName;
        for (int i = 1; i < tableNum; i++) {
            String tbName = tbNamePrefix + i;
            JdbcUtil.executeSuccess(connection, String.format(createTableLke, tbName));
            JdbcUtil.executeSuccess(connection, String.format(insertIntoSelect, tbName));
            tableNames.add(tbName);
        }

        return tableNames;
    }

    @Test
    public void testCollectStatistic() throws Exception {

        // start collect statistic
        Connection c = this.getPolardbxConnection();
        String dbName1 = "collect_statistics_db_test1";
        String dbName2 = "collect_statistics_db_test2";


        List<String> tableNames1 = createDbAndTb(c, dbName1, 5);
        List<String> tableNames2 = createDbAndTb(c, dbName2, 5);
        //每一次收集统计信息使用不同的库表，为了避免两次收集的统计信息一模一样导致meta表中的gmt_modified没有更新，从而影响verifyCollectStatisticUpdate

        //collect one table
        testCollectStatistic(false, 1, 1, Collections.singletonList(Pair.of(dbName1, tableNames1.get(0))));

        //collect multi table
        testCollectStatistic(false, 1, 5,
            Arrays.asList(Pair.of(dbName1, tableNames1.get(1)), Pair.of(dbName1, tableNames1.get(2))));

        //collect multi table and enable hll
        testCollectStatistic(true, 5, 5,
            Arrays.asList(Pair.of(dbName1, tableNames1.get(3)), Pair.of(dbName1, tableNames1.get(4))));

        //collect one db and enable hll
        testCollectStatistic(true, 5, 10, Collections.singletonList(Pair.of(dbName2, null)));

        JdbcUtil.dropDatabase(c, dbName1);
        JdbcUtil.dropDatabase(c, dbName2);
    }

    public void testCollectStatistic(boolean enableCollectHll, int dnHllParallelism, int statisticParallelism,
                                     List<Pair<String, String>> tableNames) throws Exception {

        String collectStatisticHint =
            String.format("/*+TDDL:ENABLE_COLLECT_HLL=%s DN_HLL_STATISTIC_PARALLELISM=%s STATISTIC_PARALLELISM=%s */",
                enableCollectHll ? "true" : "false", dnHllParallelism, statisticParallelism);
        String collectStatisticSql = collectStatisticHint + collectSql;
        if (tableNames != null) {
            StringJoiner sj = new StringJoiner(",");
            for (Pair<String, String> pair : tableNames) {
                if (pair.getValue() == null) {
                    sj.add(pair.getKey());
                } else {
                    sj.add(pair.getKey() + "." + pair.getValue());
                }
            }
            collectStatisticSql += " " + sj.toString();
        }

        Connection conn = this.getPolardbxConnection();

        Connection metaConn = this.getMetaConnection();
        long now = Long.parseLong(JdbcUtil.executeQueryAndGetFirstStringResult("select UNIX_TIMESTAMP()", metaConn));
        Thread.sleep(3000);
        System.out.println("start " + collectStatisticSql);
        JdbcUtil.executeUpdateSuccess(conn, collectStatisticSql);
        System.out.println("end " + collectStatisticSql);

        List<Pair<String, String>> verifyTableNames = new ArrayList<>();
        if (tableNames == null) {
            List<String> dbs = JdbcUtil.showDatabases(conn);
            for (String db : dbs) {
                JdbcUtil.useDb(conn, db);
                List<String> tbs = JdbcUtil.showTables(conn);
                for (String tb : tbs) {
                    verifyTableNames.add(Pair.of(db, tb));
                }
            }
        } else {
            for (Pair<String, String> pair : tableNames) {
                if (pair.getValue() == null) {
                    String db = pair.getKey();
                    JdbcUtil.useDb(conn, db);
                    List<String> tbs = JdbcUtil.showTables(conn);
                    for (String tb : tbs) {
                        verifyTableNames.add(Pair.of(db, tb));
                    }
                } else {
                    verifyTableNames.add(Pair.of(pair.getKey(), pair.getValue()));
                }
            }
        }

        for (Pair<String, String> pair : verifyTableNames) {
            verifyCollectStatisticUpdate(pair.getKey(), pair.getValue(), now, enableCollectHll);
        }

    }

    /**
     * @param time 以秒为单位
     */
    public void verifyCollectStatisticUpdate(String dbName, String tbName, long time, boolean enableHll)
        throws Exception {
        Connection connection = this.getMetaConnection();
        Collection<SystemTableTableStatistic.Row> tableStatisticRows =
            PolarDbXSystemTableLogicalTableStatistic.selectBySchemaAndTable(dbName, tbName, connection);
        Assert.assertTrue(!tableStatisticRows.isEmpty(), dbName + "-" + tbName);
        for (SystemTableTableStatistic.Row row : tableStatisticRows) {
            Assert.assertTrue(row.getUnixTime() >= time,
                    row.getSchema() + "-" + row.getTableName() + "-" + row.getUnixTime() + "-" + time);
        }

        Collection<SystemTableColumnStatistic.Row> columnStatisticRows =
            PolarDbXSystemTableColumnStatistic.selectBySchemaAndTable(dbName, tbName, connection);
        Assert.assertTrue(!columnStatisticRows.isEmpty(), dbName + "-" + tbName);
        for (SystemTableColumnStatistic.Row row : columnStatisticRows) {
            Assert.assertTrue(row.getUnixTime() >= time,
                    row.getSchema() + "-" + row.getTableName() + "-" + row.getColumnName() + "-" + row.getUnixTime() + "-" + time);
        }

        if (enableHll) {
            SystemTableNDVSketchStatistic.SketchRow[] sketchRows =
                PolarDbXSystemTableNDVSketchStatistic.loadByTableName(dbName, tbName, connection);
            Assert.assertTrue(sketchRows.length > 0,  dbName + "-" + tbName);
            int count = 0;
            for (SystemTableNDVSketchStatistic.SketchRow row : sketchRows) {
                if (row.getIndexName() != null && !"PRIMARY".equalsIgnoreCase(row.getIndexName())
                    && "HYPER_LOG_LOG".equalsIgnoreCase(row.getSketchType())) {
                    Assert.assertTrue(row.getGmtUpdate() >= time * 1000,
                        row.getSchemaName() + "-" + row.getTableName() + "-" + row.getColumnNames() + "-"
                            + row.getIndexName() + "-" + row.getSketchType() + "-" + row.getGmtUpdate() +"-" + time);
                    count++;
                }
            }
            Assert.assertTrue(count >= 1);
        }
    }

}

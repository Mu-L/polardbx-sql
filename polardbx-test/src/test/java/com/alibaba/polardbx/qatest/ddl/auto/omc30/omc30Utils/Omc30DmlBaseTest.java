package com.alibaba.polardbx.qatest.ddl.auto.omc30.omc30Utils;

import com.alibaba.polardbx.qatest.ddl.auto.omc.ConcurrentDMLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

public class Omc30DmlBaseTest extends ConcurrentDMLBaseTest {

    protected static final int FILL_COUNT = 1000;
    protected static final int FILL_BATCH_SIZE = 1000;

    public void concurrentTestInternal30WithoutGenerateCol(String tableName, String colDef, String alterSql,
                                                           String selectSql,
                                                           Function<Integer, String> generator1,
                                                           Function<Integer, String> generator2,
                                                           QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                           boolean fillData) throws Exception {
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            true, null, false);
    }

    public void concurrentTestInternal30(String tableName, String colDef, String alterSql, String selectSql,
                                         Function<Integer, String> generator1, Function<Integer, String> generator2,
                                         QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                         boolean fillData) throws Exception {
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            true, null, true);
    }

    public void concurrentTestInternal30WithNotStrict(String tableName, String colDef, String alterSql,
                                                      String selectSql,
                                                      Function<Integer, String> generator1,
                                                      Function<Integer, String> generator2,
                                                      QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                      boolean fillData) throws Exception {
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            false, null, true);
    }

    public void concurrentTestInternal30WithCreateTable(String tableName, String colDef, String alterSql,
                                                        String selectSql,
                                                        Function<Integer, String> generator1,
                                                        Function<Integer, String> generator2,
                                                        QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                        boolean fillData, String createTableSql) throws Exception {
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            true, createTableSql, false);
    }

    public void concurrentTestInternal30(String tableName, String colDef, String alterSql, String selectSql,
                                         Function<Integer, String> generator1, Function<Integer, String> generator2,
                                         QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                         boolean fillData, boolean isStrictMode, String createTableSql,
                                         boolean hasGeneratedColumns)
        throws Exception {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        Connection conn = getPolardbxConnection();
        String finalTableName = tableName;

        if (!isStrictMode) {
            String sql = "SET session sql_mode = ''";
            JdbcUtil.updateDataTddl(conn, sql, null);
        }

        try {
            String createSql = String.format(
                "create table %s ("
                    + "a int primary key, "
                    + "b %s,"
                    + "c varchar(10) default 'abc',"
                    + "d varchar(10) default 'abc'"
                    + ") partition by hash(`a`) partitions 3",
                tableName, colDef);
            if (createTableSql != null) {
                createSql = String.format(createTableSql, finalTableName);
            }

            // Retry in case of table group not exist
            int retryCnt = 0;
            while (true) {
                try {
                    JdbcUtil.executeUpdateSuccess(conn, createSql);
                    break;
                } catch (Throwable e) {
                    retryCnt++;
                    if (retryCnt > 5) {
                        throw e;
                    }
                    System.out.println("retry " + retryCnt + " " + e.getMessage());
                }
            }

            if (hasGeneratedColumns) {
                String alterGeneratedColumnSql = String.format(
                    "alter table %s add column tmp timestamp default current_timestamp", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);

                alterGeneratedColumnSql = String.format(
                    "alter table %s add column v_a bigint GENERATED ALWAYS AS (tmp) virtual", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);

                alterGeneratedColumnSql = String.format(
                    "alter table %s add column v_b bigint GENERATED ALWAYS AS (tmp) stored", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);

                alterGeneratedColumnSql = String.format(
                    "alter table %s add column v_c bigint GENERATED ALWAYS AS (tmp) logical", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);
            }

            if (fillData) {
                final String insert = String.format("insert into %s(a,b,c,d) values (?,?,?,?)", tableName);
                for (int i = 0; i < FILL_COUNT; i += FILL_BATCH_SIZE) {
                    List<List<Object>> params = new ArrayList<>();
                    for (int j = 0; j < FILL_BATCH_SIZE; j++) {
                        List<Object> param = new ArrayList<>();
                        param.add(j + i);
                        param.add(j + i);
                        param.add(j + i);
                        param.add(j + i);
                        params.add(param);
                    }
                    JdbcUtil.updateDataBatch(conn, insert, params);
                }
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> dmlFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        try {
                            if (!isStrictMode) {
                                String sql = "SET session sql_mode = ''";
                                JdbcUtil.updateDataTddl(connection, sql, null);
                            }
                            connection.setAutoCommit(false);
                            Function<Integer, String> generator = generator1;
                            boolean changed = false;
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                String sql = String.format(generator.apply(totalCount.get()), finalTableName);
                                try {
                                    JdbcUtil.executeUpdateSuccess(connection, sql);
                                    connection.commit();
                                } catch (AssertionError e) {
                                    connection.rollback();
                                    if (e.getMessage() == null) {
                                        throw e;
                                    }
                                    if (e.getMessage().contains("Lock wait timeout exceeded") || e.getMessage()
                                        .contains("Deadlock found")) {
                                        // ignore
                                        System.out.println(
                                            "threadId " + Thread.currentThread().getId() + ": " + e.getMessage());
                                        Thread.sleep(500);
                                        totalCount.getAndDecrement();
                                    } else if (e.getMessage().contains("Unknown target column") || e.getMessage()
                                        .contains("not found") || e.getMessage().contains("Unknown column")) {
                                        if (!changed) {
                                            changed = true;
                                            generator = generator2;
                                        } else {
                                            Thread.sleep(1000);
                                        }

                                        totalCount.getAndDecrement();
                                    } else if (e.getMessage().contains("The definition of the table")) {
                                        // ignore
                                        System.out.println(
                                            "threadId " + Thread.currentThread().getId() + ": " + e.getMessage());
                                        Thread.sleep(500);
                                        totalCount.getAndDecrement();
                                    } else {
                                        throw e;
                                    }
                                }

                                totalCount.incrementAndGet();
                            }
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                };

            BiFunction<AtomicBoolean, AtomicInteger, Void> alterFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        if (!isStrictMode) {
                            String sql = "SET session sql_mode = ''";
                            JdbcUtil.updateDataTddl(connection, sql, null);
                        }
                        Thread.sleep(1000);
                        String sql = String.format(alterSql, finalTableName) + USE_OMC_ALGORITHM;
                        try {
                            execDdlWithRetry(tddlDatabase1, finalTableName, sql, connection);
                            System.out.println("alter table done");
                        } finally {
                            shouldStop.set(true);
                            connection.close();
                        }
                        Thread.sleep(1000);
                        System.out.println(totalCount.get());
                        return null;
                    }
                };

            BiFunction<AtomicBoolean, AtomicInteger, Void> selectFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        if (!isStrictMode) {
                            String sql = "SET session sql_mode = ''";
                            JdbcUtil.updateDataTddl(connection, sql, null);
                        }
                        try {
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                int tot = totalCount.get();
                                String sql = String.format(selectSql, finalTableName);
                                try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql)) {
                                    while (rs.next() && tot > 0) {
                                        int colA = rs.getInt(1);
                                        Object tmpB = rs.getObject(2);
                                        int colB = 0;
                                        if (tmpB instanceof Integer) {
                                            colB = (Integer) tmpB;
                                        } else if (tmpB instanceof Long) {
                                            colB = ((Long) tmpB).intValue();
                                        } else if (tmpB instanceof String) {
                                            colB = (int) Float.parseFloat((String) tmpB);
                                        }
                                        String colC = rs.getObject(3) == null ? null : rs.getObject(3).toString();
                                        String colD = rs.getObject(4) == null ? null : rs.getObject(4).toString();
                                        if (!checker.apply(colA, colB, colC, colD)) {
                                            System.out.println(
                                                tot + ": " + colA + " " + colB + " " + colC + " " + colD);
                                        }
                                        assertTrue(checker.apply(colA, colB, colC, colD));
                                        tot--;
                                    }
                                } catch (AssertionError e) {
                                    if (e.getMessage() == null) {
                                        throw e;
                                    }
                                    if (e.getMessage().contains("Unknown target column") || e.getMessage()
                                        .contains("not found") || e.getMessage().contains("Unknown column")) {
                                        // ignore
                                        System.out.println(
                                            "threadId " + Thread.currentThread().getId() + ": " + e.getMessage());
                                        Thread.sleep(500);
                                    } else if (e.getMessage().contains("Communications link failure")) {
                                        System.out.println(
                                            "threadId " + Thread.currentThread().getId() + ": " + e.getMessage());
                                        connection = getPolardbxConnection();
                                    } else if (e.getMessage().contains("The definition of the table")) {
                                        // ignore
                                        System.out.println(
                                            "threadId " + Thread.currentThread().getId() + ": " + e.getMessage());
                                    } else {
                                        throw e;
                                    }
                                }
                                Thread.sleep(500);
                            }
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                };

            AtomicBoolean shouldStop = new AtomicBoolean(false);
            AtomicInteger totalCount = new AtomicInteger(0);

            final ExecutorService threadPool = Executors.newFixedThreadPool(3);
            Callable<Void> dmlTask = () -> {
                dmlFunc.apply(shouldStop, totalCount);
                return null;
            };
            Callable<Void> alterTask = () -> {
                alterFunc.apply(shouldStop, totalCount);
                return null;
            };
            Callable<Void> selectTask = () -> {
                selectFunc.apply(shouldStop, totalCount);
                return null;
            };

            ArrayList<Future<Void>> results = new ArrayList<>();
            results.add(threadPool.submit(dmlTask));
            results.add(threadPool.submit(alterTask));
            results.add(threadPool.submit(selectTask));

            try {
                for (Future<Void> result : results) {
                    result.get();
                }
            } catch (Throwable e) {
                e.printStackTrace();
                throw (e);
            } finally {
                //报错需设置退出信号,防止线程泄漏
                shouldStop.set(true);
                totalCount.set(FILL_COUNT);
                threadPool.shutdown();
            }
        } finally {
            conn.close();
        }
    }
}

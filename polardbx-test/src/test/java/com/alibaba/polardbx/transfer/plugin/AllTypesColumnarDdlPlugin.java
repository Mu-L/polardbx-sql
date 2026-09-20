package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.utils.AllTypesColumnarDdl;
import com.alibaba.polardbx.transfer.utils.AllTypesTestUtils;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.google.common.collect.ImmutableList;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class AllTypesColumnarDdlPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(AllTypesColumnarDdlPlugin.class);
    private static final List<Consumer<Statement>> actions = new ArrayList<>();
    private static final String normalCciName = AllTypesTestUtils.COLUMNAR_INDEX_NAME;
    private static final String snapshotCciName = AllTypesTestUtils.COLUMNAR_INDEX_NAME + "_snapshot";
    private static final String archiveCciName = AllTypesTestUtils.COLUMNAR_INDEX_NAME + "_archive";

    private final boolean runAllInOneRound;
    private boolean finished = false;
    private final boolean flushLogs;
    private final static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "flush-logs-thread");
                t.setDaemon(true);
                return t;
            }
        }
    );

    private volatile boolean failed = false;

    static {
        buildActions();
    }

    public AllTypesColumnarDdlPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("columnar_ddl");
        if (null == config) {
            enabled = false;
            runAllInOneRound = false;
            flushLogs = false;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = 1;
        runAllInOneRound = config.getBoolean("run_all_in_one_round", false);
        flushLogs = config.getBoolean("flush_logs", false);
        if (enabled) {
            if (runAllInOneRound) {
                Utils.getStopSignal().set(false);
            }
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement statement = conn.createStatement()) {
                    AllTypesColumnarDdl.createCci(statement, normalCciName, "id",
                        "PARTITION BY HASH(id)", null);
                } catch (Throwable t) {
                    if (t.getMessage().contains("Duplicate index name")) {
                        // ignore.
                    } else {
                        logger.error("create normal columnar index failed, skip AllTypesCheckColumnarPlugin.", t);
                        enabled = false;
                        if (runAllInOneRound) {
                            Utils.getStopSignal().set(true);
                        }
                        return;
                    }
                }

                try (Statement statement = conn.createStatement()) {
                    AllTypesColumnarDdl.createCci(statement, snapshotCciName, "id",
                        "PARTITION BY HASH(id)", "snapshot");
                } catch (Throwable t) {
                    if (t.getMessage().contains("Duplicate index name")) {
                        // ignore.
                    } else {
                        logger.error("create snapshot columnar index failed, skip AllTypesCheckColumnarPlugin.", t);
                        enabled = false;
                        if (runAllInOneRound) {
                            Utils.getStopSignal().set(true);
                        }
                        return;
                    }
                }

                try (Statement statement = conn.createStatement()) {
                    AllTypesColumnarDdl.createCci(statement, archiveCciName, "id",
                        "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                            + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                            + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                            + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                            + "    PARTITION p4 VALUES LESS THAN ('2026-01-01'),\n"
                            + "    PARTITION p_max VALUES LESS THAN (MAXVALUE)\n"
                            + ")",
                        "archive");
                } catch (Throwable t) {
                    if (t.getMessage().contains("Duplicate index name")) {
                        // ignore.
                    } else {
                        logger.error("create archive columnar index failed, skip AllTypesCheckColumnarPlugin.", t);
                        enabled = false;
                        if (runAllInOneRound) {
                            Utils.getStopSignal().set(true);
                        }
                        return;
                    }
                }
            });
            if (flushLogs) {
                scheduledExecutorService.scheduleAtFixedRate(() -> getConnectionAndExecute(dsn, (conn, error) -> {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute("flush logs");
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }), 10, 10, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    protected void runInternal() {
        if (runAllInOneRound) {
            // run all actions in one round, and stop the test
            try {
                if (!finished) {
                    AtomicReference<Throwable> exception = new AtomicReference<>(null);
                    for (Consumer<Statement> action : actions) {
                        if (exception.get() != null) {
                            return;
                        }
                        getConnectionAndExecute(dsn, (conn, error) -> {
                            try (Statement statement = conn.createStatement()) {
                                statement.execute("set sql_mode=''");
                                action.accept(statement);
                            } catch (Throwable t) {
                                exception.set(t);
                                errorAllTypesTest1(ImmutableList.of(), t);
                            }
                        });
                    }
                    // last check
                    getConnectionAndExecute(dsn, (conn, error) -> {
                        try (Statement statement = conn.createStatement()) {
                            checkAllCci(statement);
                        } catch (Throwable t) {
                            exception.set(t);
                            errorAllTypesTest1(ImmutableList.of(), t);
                        }
                    });
                } else {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                finished = true;
                AtomicBoolean stop = Utils.getStopSignal();
                synchronized (stop) {
                    stop.set(true);
                    stop.notifyAll();
                }
                scheduledExecutorService.shutdownNow();
            }
        } else {
            if (failed) {
                return;
            }
            // randomly choose an action to run
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement statement = conn.createStatement()) {
                    int random = ThreadLocalRandom.current().nextInt(actions.size());
                    statement.execute("set sql_mode=''");
                    actions.get(random).accept(statement);
                } catch (Throwable t) {
                    failed = true;
                    errorAllTypesTest1(ImmutableList.of(), t);
                    scheduledExecutorService.shutdownNow();
                }
            });
        }
    }

    private static void buildActions() {
        actions.add(statement -> {
            try {
                // rebuild normal cci using drop + create
                AllTypesColumnarDdl.dropCci(statement, normalCciName);
                Thread.sleep(1000);
                AllTypesColumnarDdl.createCci(statement, normalCciName, "id",
                    "PARTITION BY HASH(id)", null);
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // rebuild snapshot cci using drop + create
                AllTypesColumnarDdl.dropCci(statement, snapshotCciName);
                Thread.sleep(1000);
                AllTypesColumnarDdl.createCci(statement, snapshotCciName, "id",
                    "PARTITION BY HASH(id)", "snapshot");
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // rebuild archive cci using drop + create
                AllTypesColumnarDdl.dropCci(statement, archiveCciName);
                Thread.sleep(1000);
                AllTypesColumnarDdl.createCci(statement, archiveCciName, "id",
                    "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                        + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                        + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                        + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                        + "    PARTITION p4 VALUES LESS THAN (MAXVALUE)\n"
                        + ")",
                    "archive");
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // rebuild normal cci using drop + create
                AllTypesColumnarDdl.alterTableDropCci(statement, normalCciName);
                Thread.sleep(1000);
                AllTypesColumnarDdl.alterTableAddCci(statement, normalCciName, "id",
                    "PARTITION BY HASH(id)", null);
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // rebuild snapshot cci using drop + create
                AllTypesColumnarDdl.alterTableDropCci(statement, snapshotCciName);
                Thread.sleep(1000);
                AllTypesColumnarDdl.alterTableAddCci(statement, snapshotCciName, "id",
                    "PARTITION BY HASH(id)", "snapshot");
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // rebuild archive cci using drop + create
                AllTypesColumnarDdl.alterTableDropCci(statement, archiveCciName);
                Thread.sleep(1000);
                AllTypesColumnarDdl.alterTableAddCci(statement, archiveCciName, "id",
                    "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                        + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                        + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                        + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                        + "    PARTITION p4 VALUES LESS THAN (MAXVALUE)\n"
                        + ")",
                    "archive");
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            AllTypesWriteOnlyPlugin.getLock().writeLock().lock();
            try {
                // insert some data
                String sql = "insert into all_types (id) values (null)";
                for (int i = 0; i < 100; i++) {
                    statement.execute(sql);
                }

                // truncate table
                AllTypesColumnarDdl.truncateTable(statement);

                {
                    ResultSet rs = statement.executeQuery("call polardbx.columnar_flush()");
                    long tso = 0;
                    if (rs.next()) {
                        tso = rs.getLong(1);
                    } else {
                        throw new RuntimeException("call columnar flush empty results");
                    }
                    // wait for sync
                    while (true) {
                        rs = statement.executeQuery("show columnar offset");
                        boolean ok = false;
                        while (rs.next()) {
                            String type = rs.getString("TYPE");
                            if ("CN_MIN_LATENCY".equalsIgnoreCase(type)) {
                                if (rs.getLong("TSO") >= tso) {
                                    ok = true;
                                }
                                break;
                            }
                        }
                        if (ok) {
                            break;
                        } else {
                            Thread.sleep(1000);
                        }
                    }
                }

                Thread.sleep(10 * 1000);

                // archive table should still have data
                sql = "SELECT count(0) FROM all_types FORCE INDEX(" + archiveCciName + ")";
                try (ResultSet rs = statement.executeQuery(sql)) {
                    if (!rs.next()) {
                        throw new RuntimeException("query failed: " + sql);
                    }
                    if (rs.getLong(1) == 0) {
                        throw new RuntimeException("Truncate table delete data of archive cci.");
                    }
                }
                sql = "SELECT count(0) FROM all_types FORCE INDEX(" + normalCciName + ")";
                try (ResultSet rs = statement.executeQuery(sql)) {
                    if (!rs.next()) {
                        throw new RuntimeException("query failed: " + sql);
                    }
                    if (rs.getLong(1) != 0) {
                        throw new RuntimeException("Truncate table does not delete data of normal cci.");
                    }
                }
                sql = "SELECT count(0) FROM all_types FORCE INDEX(" + snapshotCciName + ")";
                try (ResultSet rs = statement.executeQuery(sql)) {
                    if (!rs.next()) {
                        throw new RuntimeException("query failed: " + sql);
                    }
                    if (rs.getLong(1) != 0) {
                        throw new RuntimeException("Truncate table does not delete data of snapshot cci.");
                    }
                }
                AllTypesColumnarDdl.alterTableDropCci(statement, archiveCciName);
                AllTypesColumnarDdl.alterTableAddCci(statement, archiveCciName, "id",
                    "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                        + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                        + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                        + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                        + "    PARTITION p4 VALUES LESS THAN (MAXVALUE)\n"
                        + ")",
                    "archive");
                // load data back
                logger.info("load data back after truncate table");
                // insert some data
                sql = "insert into all_types (id) values (null)";
                for (int i = 0; i < 100; i++) {
                    statement.execute(sql);
                }
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            } finally {
                AllTypesWriteOnlyPlugin.getLock().writeLock().unlock();
            }
        });

        actions.add(statement -> {
            try {
                // rename table
                // create a new table
                final String newTableName = "all_types_new";
                final String tmpTableName = "all_types_tmp";
                AllTypesColumnarDdl.dropTable(statement, newTableName);
                AllTypesColumnarDdl.dropTable(statement, tmpTableName);
                AllTypesColumnarDdl.createTableLike(statement, newTableName);
                // switch these two tables: all_types -> all_types_tmp, all_types_new -> all_types
                AllTypesColumnarDdl.renameTable(statement, tmpTableName, newTableName);
                // rename cci to avoid same cci names
                AllTypesColumnarDdl.renameCci(statement, tmpTableName, normalCciName, normalCciName + "_new");
                AllTypesColumnarDdl.renameCci(statement, tmpTableName, snapshotCciName, snapshotCciName + "_new");
                AllTypesColumnarDdl.renameCci(statement, tmpTableName, archiveCciName, archiveCciName + "_new");

                checkAllCci(statement);
                // rename back: all_types -> all_types_new, all_types_tmp -> all_types
                AllTypesColumnarDdl.renameTable(statement, newTableName, tmpTableName);
                AllTypesColumnarDdl.renameCci(statement, "all_types", normalCciName + "_new", normalCciName);
                AllTypesColumnarDdl.renameCci(statement, "all_types", snapshotCciName + "_new", snapshotCciName);
                AllTypesColumnarDdl.renameCci(statement, "all_types", archiveCciName + "_new", archiveCciName);
                // drop table
                AllTypesColumnarDdl.dropTable(statement, newTableName);
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // modify column
                final String columnName = "c_char";
//                final String originType = "char(10) DEFAULT NULL AFTER c_year_4";
//                final String newType = "char(255) DEFAULT NULL AFTER id";
                final String originType = "char(10) DEFAULT NULL";
                final String newType = "char(255) DEFAULT NULL";
                AllTypesColumnarDdl.alterTableModifyColumn(statement, columnName, newType, false);
                checkAllCci(statement);
                // 5419 not supported
//                final String sql = "SELECT * FROM all_types FORCE INDEX(" + normalCciName + ") LIMIT 1";
//                try (ResultSet rs = statement.executeQuery(sql)) {
//                    if (!rs.next()) {
//                        throw new RuntimeException("query failed: " + sql);
//                    }
//                    // the second column should be the new one
//                    if (!rs.getMetaData().getColumnName(2).equalsIgnoreCase(columnName)) {
//                        throw new RuntimeException(
//                            "the second column should be " + columnName + ", but was " + rs.getMetaData()
//                                .getColumnName(2));
//                    }
//                }

                // modify back to the origin type using omc
                AllTypesColumnarDdl.alterTableModifyColumn(statement, columnName, originType, true);
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // add/change/drop column, drop/set default, multi alter column
                // add column
                final String columnName = "c_new_int";
                AllTypesColumnarDdl.alterTableAddColumn(statement, columnName, "INT DEFAULT 100 AFTER id");
//                AllTypesColumnarDdl.alterTableAddColumn(statement, columnName, "INT DEFAULT 100");
                checkAllCci(statement);
                final String sql = "SELECT * FROM all_types FORCE INDEX(" + normalCciName + ") LIMIT 1";
                try (ResultSet rs = statement.executeQuery(sql)) {
                    if (!rs.next()) {
                        throw new RuntimeException("query failed: " + sql);
                    }
                    // the second column should be the new one
                    if (!rs.getMetaData().getColumnName(2).equalsIgnoreCase(columnName)) {
                        throw new RuntimeException(
                            "the second column should be " + columnName + ", but was " + rs.getMetaData()
                                .getColumnName(2));
                    }
                    if (rs.getLong(2) != 100) {
                        throw new RuntimeException("the second column should be 100, but was " + rs.getLong(2));
                    }
                }
                // change column
                final String newColumnName = "c_new_int_new";
                AllTypesColumnarDdl.alterTableChangeColumn(statement, columnName, newColumnName,
                    "INT DEFAULT 100");
                checkAllCci(statement);
                try (ResultSet rs = statement.executeQuery(sql)) {
                    if (!rs.next()) {
                        throw new RuntimeException("query failed: " + sql);
                    }
                    // the second column should be the new one
                    if (!rs.getMetaData().getColumnName(2).equalsIgnoreCase(newColumnName)) {
                        throw new RuntimeException(
                            "the second column should be " + newColumnName + ", but was " + rs.getMetaData()
                                .getColumnName(2));
                    }
                }
                // drop/set default
                final String columnName2 = "c_int_32";
                final String defaultValue = "10";
                AllTypesColumnarDdl.alterTableDropDefault(statement, columnName2);
                checkAllCci(statement);
                AllTypesColumnarDdl.alterTableSetDefault(statement, columnName2, defaultValue);
                checkAllCci(statement);

                // multi alter
                final String intType = "int(32) NOT NULL DEFAULT 0";
                AllTypesColumnarDdl.multiAlter(statement, columnName2, intType, newColumnName, columnName, intType);

                // drop the column
                AllTypesColumnarDdl.alterTableDropColumn(statement, columnName);
                checkAllCci(statement);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // alter cci partition/sort key
                final String cciName = AllTypesTestUtils.COLUMNAR_INDEX_NAME + "_new";
                AllTypesColumnarDdl.createCci(statement, cciName, "c_int_32", "PARTITION BY HASH(c_int_32)", null);
                checkAllCci(statement);
                List<String> checkResults = AllTypesColumnarDdl.check(statement, cciName);
                if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                    errorAllTypesTest1(checkResults, new RuntimeException("Cci naive checker failed for normal cci."));
                }
                AllTypesColumnarDdl.alterTableModifyColumn(statement, "c_int_32",
                    "bigint NOT NULL DEFAULT 0", true);
                checkAllCci(statement);
                checkResults = AllTypesColumnarDdl.check(statement, cciName);
                if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                    errorAllTypesTest1(checkResults, new RuntimeException("Cci naive checker failed for normal cci."));
                }
                AllTypesColumnarDdl.alterTableModifyColumn(statement, "c_int_32",
                    "int(32) NOT NULL DEFAULT 0", true);
                checkAllCci(statement);
                checkResults = AllTypesColumnarDdl.check(statement, cciName);
                if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                    errorAllTypesTest1(checkResults, new RuntimeException("Cci naive checker failed for normal cci."));
                }
                AllTypesColumnarDdl.alterTableDropCci(statement, cciName);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        actions.add(statement -> {
            try {
                // add/split cci partition, drop primary partition
                final String newArchiveCci = AllTypesTestUtils.COLUMNAR_INDEX_NAME + "_new_archive";
                final String newNormalCci = AllTypesTestUtils.COLUMNAR_INDEX_NAME + "_new_normal";
                AllTypesColumnarDdl.alterPrimaryPartition(statement,
                    "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                        + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                        + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                        + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                        + "    PARTITION p4 VALUES LESS THAN ('2026-01-01'),\n"
                        + "    PARTITION p5 VALUES LESS THAN ('2026-04-01')\n"
                        + ")");

                AllTypesWriteOnlyPlugin.getLock().writeLock().lock();
                try {
                    AllTypesColumnarDdl.createCci(statement, newArchiveCci, "id",
                        "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                            + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                            + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                            + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                            + "    PARTITION p4 VALUES LESS THAN ('2026-01-01')\n"
                            + ")",
                        "archive");
                    AllTypesColumnarDdl.createCci(statement, newNormalCci, "id",
                        "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                            + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                            + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                            + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                            + "    PARTITION p4 VALUES LESS THAN ('2026-01-01')\n"
                            + ")",
                        null);

                    String sql = "DROP TABLEGROUP IF EXISTS my_tg9998";
                    statement.execute(sql);
                    sql = "DROP TABLEGROUP IF EXISTS my_tg9999";
                    statement.execute(sql);
                    sql = "/*+TDDL:ENABLE_CHECK_DDL_FILE_STORAGE=FALSE*/ CREATE TABLEGROUP my_tg9998";
                    statement.execute(sql);
                    sql = "/*+TDDL:ENABLE_CHECK_DDL_FILE_STORAGE=FALSE*/ "
                        + "ALTER TABLE all_types." + newArchiveCci + " SET TABLEGROUP = my_tg9998";
                    statement.execute(sql);
                    sql = "/*+TDDL:ENABLE_CHECK_DDL_FILE_STORAGE=FALSE*/ CREATE TABLEGROUP my_tg9999";
                    statement.execute(sql);
                    sql = "/*+TDDL:ENABLE_CHECK_DDL_FILE_STORAGE=FALSE*/ "
                        + "ALTER TABLE all_types." + newNormalCci + " SET TABLEGROUP = my_tg9999";
                    statement.execute(sql);

                    List<String> checkResults = AllTypesColumnarDdl.check(statement, newArchiveCci);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for archive cci."));
                    }
                    checkResults = AllTypesColumnarDdl.check(statement, newNormalCci);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for normal cci."));
                    }

                    // add cci partition
                    AllTypesColumnarDdl.alterCciAddPartition(statement, newNormalCci,
                        "(PARTITION p5 VALUES LESS THAN ('2026-04-01'))");
                    AllTypesColumnarDdl.alterCciAddPartition(statement, newArchiveCci,
                        "(PARTITION p5 VALUES LESS THAN ('2026-04-01'))");
                    // insert some data from 2026-01-01 to 2026-03-31
                    sql = "INSERT INTO all_types(c_datetime) VALUES "
                        + "(FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2026-03-31 23:59:59') - UNIX_TIMESTAMP('2026-01-01 00:00:00')) + UNIX_TIMESTAMP('2026-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f'))";
                    int dataSize = 100;
                    for (int i = 0; i < dataSize; i++) {
                        statement.execute(sql);
                    }
                    checkAllCci(statement);

                    // split partition
                    AllTypesColumnarDdl.alterCciSplitPartition(statement, newNormalCci, "p5",
                        "(PARTITION p51 VALUES LESS THAN ('2026-02-01'),"
                            + "PARTITION p52 VALUES LESS THAN ('2026-03-01'),"
                            + "PARTITION p53 VALUES LESS THAN ('2026-04-01'))");
                    AllTypesColumnarDdl.alterCciSplitPartition(statement, newArchiveCci, "p5",
                        "(PARTITION p51 VALUES LESS THAN ('2026-02-01'),"
                            + "PARTITION p52 VALUES LESS THAN ('2026-03-01'),"
                            + "PARTITION p53 VALUES LESS THAN ('2026-04-01'))");
                    checkAllCci(statement);

                    // drop primary partition
                    AllTypesColumnarDdl.alterPrimaryDropPartition(statement, "p5");
                    checkResults = AllTypesColumnarDdl.check(statement, newNormalCci);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for " + newNormalCci));
                    }
                    checkResults = AllTypesColumnarDdl.check(statement, normalCciName);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for " + normalCciName));
                    }
                    checkResults = AllTypesColumnarDdl.check(statement, snapshotCciName);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for " + snapshotCciName));
                    }
                    // archive should contain 100 more data
                    sql = "SELECT count(0) FROM all_types FORCE INDEX(" + archiveCciName + ")";
                    logger.info("execute sql: {}", sql);
                    long archiveSize;
                    try (ResultSet rs = statement.executeQuery(sql)) {
                        if (!rs.next()) {
                            throw new RuntimeException("query failed: " + sql);
                        }
                        archiveSize = rs.getLong(1);
                    }
                    sql = "SELECT count(0) FROM all_types FORCE INDEX(primary)";
                    logger.info("execute sql: {}", sql);
                    long primarySize;
                    try (ResultSet rs = statement.executeQuery(sql)) {
                        if (!rs.next()) {
                            throw new RuntimeException("query failed: " + sql);
                        }
                        primarySize = rs.getLong(1);
                    }
                    sql = "SELECT count(0) FROM all_types FORCE INDEX(" + newArchiveCci + ")";
                    logger.info("execute sql: {}", sql);
                    long newArchiveSize;
                    try (ResultSet rs = statement.executeQuery(sql)) {
                        if (!rs.next()) {
                            throw new RuntimeException("query failed: " + sql);
                        }
                        newArchiveSize = rs.getLong(1);
                    }
                    logger.info("archiveSize: {}, primarySize: {}, newArchiveSize: {}", archiveSize,
                        primarySize,
                        newArchiveSize);
                    if (archiveSize != newArchiveSize) {
                        throw new RuntimeException("Archive size is not correct.");
                    }
                    if (archiveSize - primarySize != 100) {
                        throw new RuntimeException("Archive size is not correct.");
                    }
                } finally {
                    AllTypesWriteOnlyPlugin.getLock().writeLock().unlock();
                }

                // clean
                Toml config = TomlConfig.getConfig();
                String suffix = config.getString("create_table_suffix", "");
                AllTypesColumnarDdl.alterPrimaryPartition(statement, suffix);
                AllTypesColumnarDdl.dropCci(statement, newArchiveCci);
                AllTypesColumnarDdl.alterTableDropCci(statement, newNormalCci);
                AllTypesColumnarDdl.dropCci(statement, archiveCciName);
                AllTypesColumnarDdl.createCci(statement, archiveCciName, "id",
                    "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                        + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                        + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                        + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                        + "    PARTITION p4 VALUES LESS THAN ('2026-01-01'),\n"
                        + "    PARTITION p_max VALUES LESS THAN (MAXVALUE)\n"
                        + ")",
                    "archive");
            } catch (Throwable t) {
                throw new RuntimeException(t);
            } finally {
                try {
                    String sql = "DROP TABLEGROUP IF EXISTS my_tg9998";
                    statement.execute(sql);
                    sql = "DROP TABLEGROUP IF EXISTS my_tg9999";
                    statement.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        actions.add(statement -> {
            AllTypesWriteOnlyPlugin.getLock().writeLock().lock();
            try {
                // test black hole, archive trx
                final String insertBlackHoleTable = "`__#_all_types`";
                final String deleteBlackHoleTable = "`__$_all_types`";
                logger.info("create black hole table");
                final String dropTable = "DROP TABLE IF EXISTS %s";
                statement.execute(String.format(dropTable, insertBlackHoleTable));
                statement.execute(String.format(dropTable, deleteBlackHoleTable));
                final String createTable = "CREATE TABLE %s (id int primary key) ENGINE=BLACKHOLE";
                statement.execute(
                    String.format(AllTypesTestUtils.FULL_TYPE_TEMPLATE, insertBlackHoleTable) + "ENGINE=BLACKHOLE");
                statement.execute(String.format(createTable, deleteBlackHoleTable));
                final String insertSql = "insert into %s (id) values (%s)";
                final String getLastInsertId = "select last_insert_id()";
                final String selectSql = "select count(0) from all_types force index(%s) where id = %d";
                final String deleteSql = "delete from all_types where id = %d";
                Thread.sleep(1000);
                statement.execute(String.format(insertSql, "all_types", "null"));
                long id = 0;
                try (ResultSet rs = statement.executeQuery(getLastInsertId)) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                    } else {
                        throw new RuntimeException("Get last insert id failed.");
                    }
                }
                waitColumnarOffset(statement);
                // should exist in primary table
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, "primary", id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in primary table.");
                    }
                }
                // should exist in all cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, normalCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in cci.");
                    }
                }
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, snapshotCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in snapshot cci.");
                    }
                }
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, archiveCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in archive cci.");
                    }
                }

                // use black hole to delete in normal and snapshot cci
                statement.execute(String.format(insertSql, deleteBlackHoleTable, id));
                waitColumnarOffset(statement);
                // should exist in primary table
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, "primary", id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in primary table.");
                    }
                }
                // should exist in archive cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, archiveCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in archive cci.");
                    }
                }
                // should not exist in normal/snapshot cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, normalCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 0) {
                        throw new RuntimeException("Found unexpected record in cci.");
                    }
                }
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, snapshotCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 0) {
                        throw new RuntimeException("Found unexpected record in cci.");
                    }
                }

                // rebuild
                AllTypesColumnarDdl.dropCci(statement, normalCciName);
                AllTypesColumnarDdl.createCci(statement, normalCciName, "id",
                    "PARTITION BY HASH(id)", null);
                AllTypesColumnarDdl.dropCci(statement, snapshotCciName);
                AllTypesColumnarDdl.createCci(statement, snapshotCciName, "id",
                    "PARTITION BY HASH(id)", "snapshot");
                checkAllCci(statement);

                // use archive trx to delete
                statement.execute("set transaction_policy = archive");
                statement.execute("begin");
                statement.execute(String.format(deleteSql, id));
                statement.execute("commit");
                statement.execute("set transaction_policy = tso");
                waitColumnarOffset(statement);
                // should not exist in primary table
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, "primary", id))) {
                    if (!rs.next() || rs.getLong(1) != 0) {
                        throw new RuntimeException("Found unexpected record in primary table.");
                    }
                }
                // should not exist in normal/snapshot cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, normalCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 0) {
                        throw new RuntimeException("Found unexpected record in cci.");
                    }
                }
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, snapshotCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 0) {
                        throw new RuntimeException("Found unexpected record in snapshot cci.");
                    }
                }
                // should exist in archive cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, archiveCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in archive cci.");
                    }
                }

                // rebuild archive
                AllTypesColumnarDdl.dropCci(statement, archiveCciName);
                AllTypesColumnarDdl.createCci(statement, archiveCciName, "id",
                    "PARTITION BY RANGE COLUMNS(c_datetime) (\n"
                        + "    PARTITION p1 VALUES LESS THAN ('2025-04-01'),\n"
                        + "    PARTITION p2 VALUES LESS THAN ('2025-07-01'),\n"
                        + "    PARTITION p3 VALUES LESS THAN ('2025-10-01'),\n"
                        + "    PARTITION p4 VALUES LESS THAN ('2026-01-01'),\n"
                        + "    PARTITION p_max VALUES LESS THAN (MAXVALUE)\n"
                        + ")",
                    "archive");
                checkAllCci(statement);

                // use archive trx to insert
                statement.execute("set transaction_policy = archive");
                statement.execute("begin");
                statement.execute(String.format(insertSql, "all_types", "null"));
                try (ResultSet rs = statement.executeQuery(getLastInsertId)) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                    } else {
                        throw new RuntimeException("Get last insert id failed.");
                    }
                }
                statement.execute("commit");
                statement.execute("set transaction_policy = tso");
                waitColumnarOffset(statement);

                // should exist in primary table
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, "primary", id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in primary table.");
                    }
                }
                // should exist in normal/snapshot cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, normalCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in cci.");
                    }
                }
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, snapshotCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 1) {
                        throw new RuntimeException("Can not find record in snapshot cci.");
                    }
                }
                // should not exist in archive cci
                try (ResultSet rs = statement.executeQuery(String.format(selectSql, archiveCciName, id))) {
                    if (!rs.next() || rs.getLong(1) != 0) {
                        throw new RuntimeException("Found unexpected record in archive cci.");
                    }
                }

                // use insert black hole to insert in archive cci
                statement.execute("begin");
                statement.execute("insert into " + insertBlackHoleTable + " select * from all_types where id = " + id);
                statement.execute("commit");
                statement.execute("set transaction_policy = tso");
                checkAllCci(statement);

                statement.execute("DROP TABLE " + insertBlackHoleTable);
                statement.execute("DROP TABLE " + deleteBlackHoleTable);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            } finally {
                AllTypesWriteOnlyPlugin.getLock().writeLock().unlock();
            }
        });

        actions.add(statement -> {
            try {
                statement.execute("call polardbx.columnar_set_config('streaming_route_batch_size', 8)");
                Thread.sleep(5000);
                statement.execute("call polardbx.columnar_set_config('streaming_route_batch_size', 64)");
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        actions.add(statement -> {
            try {
                statement.execute("call polardbx.columnar_backup()");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private static void checkAllCci(Statement statement) {
        List<Runnable> checks = Arrays.asList(
            () -> {
                try {
                    List<String> checkResults = AllTypesColumnarDdl.check(statement, normalCciName);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for normal cci."));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            },
            () -> {
                try {
                    List<String> checkResults = AllTypesColumnarDdl.check(statement, snapshotCciName);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for snapshot cci."));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            },
            () -> {
                try {
                    List<String> checkResults = AllTypesColumnarDdl.check(statement, archiveCciName);
                    if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                        errorAllTypesTest1(checkResults,
                            new RuntimeException("Cci naive checker failed for archive cci."));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        );

        Collections.shuffle(checks);
        for (Runnable check : checks) {
            check.run();
        }
    }

    private static long[] getInnodbAndColumnarOffset(Statement statement) {
        long innodbOffset = 0, columnarOffset = -1;
        try (ResultSet rs = statement.executeQuery("SHOW COLUMNAR OFFSET")) {
            while (rs.next()) {
                String type = rs.getString("type");
                long position = rs.getLong("TSO");
                if (type.equalsIgnoreCase("cdc")) {
                    innodbOffset = position;
                } else if (type.equalsIgnoreCase("cn_min_latency")) {
                    columnarOffset = position;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new long[] {innodbOffset, columnarOffset};
    }

    private static long getColumnarOffset(Statement statement) {
        try (ResultSet rs = statement.executeQuery("SHOW COLUMNAR OFFSET")) {
            while (rs.next()) {
                String type = rs.getString("type");
                long position = rs.getLong("TSO");
                if (type.equalsIgnoreCase("cn_min_latency")) {
                    return position;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("get columnar offset failed");
    }

    public static void waitColumnarOffset(Statement statement) {
        try {
            statement.execute("call polardbx.columnar_flush()");
            Thread.sleep(1000);
            long[] offsets = getInnodbAndColumnarOffset(statement);
            long innodbOffset = offsets[0], columnarOffset = offsets[1];
            statement.execute("call polardbx.columnar_flush()");

            for (int retry = 0; retry <= 10 && columnarOffset < innodbOffset; retry++) {

                Thread.sleep(1000);

                columnarOffset = getColumnarOffset(statement);
            }
            if (columnarOffset < innodbOffset) {
                throw new RuntimeException(
                    String.format(
                        "wait columnar offset failed, retry time is %s, retry interval is %s, innodb offset is %s, columnar offset is %s",
                        10, 1000, innodbOffset, columnarOffset));
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

    }
}

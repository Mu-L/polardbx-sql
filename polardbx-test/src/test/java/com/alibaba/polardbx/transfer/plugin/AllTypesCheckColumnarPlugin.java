package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.utils.AllTypesTestUtils;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.transfer.utils.AllTypesTestUtils.COLUMNAR_INDEX_NAME;

/**
 * @author yaozhili
 */
public class AllTypesCheckColumnarPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(AllTypesWriteOnlyPlugin.class);

    private final List<String> allColumns = new ArrayList<>();
    private final Map<Long, Long> lastTsoMap = new ConcurrentHashMap<>();

    private boolean incCheck = false;
    private boolean snapshotCheck = false;
    private boolean simpleCheck = false;

    public AllTypesCheckColumnarPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("check_columnar");
        if (null == config) {
            enabled = false;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        incCheck = config.getBoolean("inc_check", false);
        snapshotCheck = config.getBoolean("snapshot_check", false);
        simpleCheck = config.getBoolean("simple_check", false);
        boolean bigColumn = TomlConfig.getConfig().getBoolean("big_column", false);
        allColumns.addAll(AllTypesTestUtils.getColumns());
        if (bigColumn) {
            allColumns.addAll(AllTypesTestUtils.getBigColumns());
        }
        if (enabled) {
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    // Create columnar index.
                    String createSql = AllTypesTestUtils.FULL_TYPE_TABLE_COLUMNAR_INDEX;
                    stmt.execute(createSql);
                } catch (Throwable t) {
                    if (t.getMessage().contains("Duplicate index name")) {
                        // ignore.
                    } else {
                        logger.error("create columnar index failed, skip AllTypesCheckColumnarPlugin.", t);
                        enabled = false;
                    }
                }
            });
        }
    }

    @Override
    protected void runInternal() {
        long lastTso = lastTsoMap.getOrDefault(Thread.currentThread().getId(), -1L);
        List<String> checkResults = new ArrayList<>();

        if (simpleCheck) {
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("call polardbx.columnar_flush()");
                    long tso = 0;
                    if (rs.next()) {
                        tso = rs.getLong(1);
                    } else {
                        throw new RuntimeException("call columnar flush empty results");
                    }
                    // wait for sync
                    while (true) {
                        rs = stmt.executeQuery("show columnar offset");
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
                    // column store
                    String sql = "select check_sum_v2(*) from " + AllTypesTestUtils.TABLE_NAME
                        + " as of tso " + tso + " force index (" + COLUMNAR_INDEX_NAME + ")";
                    rs = stmt.executeQuery(sql);
                    long columnChecksum = -1;
                    if (rs.next()) {
                        columnChecksum = rs.getLong(1);
                    }
                    // row store
                    rs = stmt.executeQuery("select check_sum_v2(*) from " + AllTypesTestUtils.TABLE_NAME
                        + " as of tso " + tso + " force index (primary)");
                    long rowChecksum = -1;
                    if (rs.next()) {
                        rowChecksum = rs.getLong(1);
                    }
                    if (columnChecksum != rowChecksum) {
                        checkResults.add(
                            "tso " + tso + ", column checksum " + columnChecksum + ", row checksum " + rowChecksum);
                        errorAllTypesTest1(checkResults, new RuntimeException("Cci simple checker failed."));
                    } else {
                        logger.info("simple check passed");
                    }
                } catch (SQLException e) {
                    logger.error("Check error.", e);
                    error.set(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            // Cci naive checker.
            checkResults.clear();
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    String checkSql = "/*+TDDL:ENABLE_CCI_FAST_CHECKER=false */ CHECK COLUMNAR INDEX "
                        + COLUMNAR_INDEX_NAME;
                    ResultSet rs = stmt.executeQuery(checkSql);
                    while (rs.next()) {
                        checkResults.add(rs.getString("DETAILS"));
                    }
                    stmt.execute("SELECT SLEEP(1)");
                } catch (SQLException e) {
                    logger.error("Check error.", e);
                    error.set(e);
                }
            });
            if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                errorAllTypesTest1(checkResults, new RuntimeException("Cci naive checker failed."));
            }
        }

        // Cci increment checker.
        if (incCheck && lastTso > 0) {
            checkResults.clear();
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    long currentTso = columnarFlushAndGetTso(stmt);
                    if (currentTso > lastTso) {
                        lastTsoMap.put(Thread.currentThread().getId(), currentTso);
                        String checkSql = "CHECK COLUMNAR INDEX "
                            + COLUMNAR_INDEX_NAME + " INCREMENT " + lastTso + " " + currentTso + " " + currentTso;
                        ResultSet rs = stmt.executeQuery(checkSql);
                        while (rs.next()) {
                            checkResults.add(rs.getString("DETAILS"));
                        }
                        stmt.execute("SELECT SLEEP(1)");
                    } else {
                        checkResults.add("Fail to get current tso.");
                    }
                } catch (SQLException e) {
                    logger.error("Write only error.", e);
                    error.set(e);
                }
            });
            if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                errorAllTypesTest1(checkResults, new RuntimeException("Cci increment checker failed."));
            }
        }

        // Cci snapshot naive checker.
        if (snapshotCheck && lastTso > 0) {
            checkResults.clear();
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    long currentTso = columnarFlushAndGetTso(stmt);
                    if (currentTso > lastTso) {
                        lastTsoMap.put(Thread.currentThread().getId(), currentTso);
                        String checkSql = "/*+TDDL:ENABLE_CCI_FAST_CHECKER=false */ CHECK COLUMNAR INDEX "
                            + COLUMNAR_INDEX_NAME + " SNAPSHOT " + lastTso + " " + lastTso;
                        ResultSet rs = stmt.executeQuery(checkSql);
                        while (rs.next()) {
                            checkResults.add(rs.getString("DETAILS"));
                        }
                        stmt.execute("SELECT SLEEP(1)");
                    } else {
                        checkResults.add("Fail to get current tso.");
                    }
                } catch (SQLException e) {
                    logger.error("Write only error.", e);
                    error.set(e);
                }
            });
            if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                errorAllTypesTest1(checkResults, new RuntimeException("Cci increment checker failed."));
            }
        }
    }

    static private long columnarFlushAndGetTso(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("call polardbx.columnar_flush()");
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            return -1;
        }
    }
}

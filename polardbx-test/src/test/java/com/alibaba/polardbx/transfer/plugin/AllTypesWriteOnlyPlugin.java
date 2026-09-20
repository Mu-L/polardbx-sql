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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alibaba.polardbx.transfer.utils.AllTypesTestUtils.TABLE_NAME;

/**
 * @author yaozhili
 */
public class AllTypesWriteOnlyPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(AllTypesWriteOnlyPlugin.class);
    private final boolean bigColumn;
    private final int skipTrx;

    private final List<String> allColumns = new ArrayList<>();
    private static final Object lock = new Object();
    private static long[] cachedIds = null;
    private static int size = 0;
    private static int start = 0;
    private final static int maxBatch = 8;
    private final static ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public AllTypesWriteOnlyPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("write_only");
        if (null == config) {
            enabled = false;
            bigColumn = false;
            skipTrx = 1;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        skipTrx = Math.toIntExact(config.getLong("skip_trx", 1L));
        bigColumn = TomlConfig.getConfig().getBoolean("big_column", false);
        allColumns.addAll(AllTypesTestUtils.getColumns());
        allColumns.addAll(AllTypesTestUtils.getBigColumns());
    }

    @Override
    protected void runInternal() {
        final long[] ids = new long[maxBatch];
        getConnectionAndExecute(dsn, (conn, error) -> {
            getLock().readLock().lock();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set sql_mode=''");
                stmt.execute("set transaction_isolation='repeatable-read'");
                stmt.execute("set transaction_policy=tso");

                List<String> sqls = new ArrayList<>();
                // generate dml sql
                try {
                    // Insert a new record.
                    sqls.add(AllTypesTestUtils.buildInsertSql(1, allColumns, bigColumn));

                    // Batch insert
                    int batch = ThreadLocalRandom.current().nextInt(maxBatch) + 1;
                    sqls.add(AllTypesTestUtils.buildInsertSql(batch, allColumns, bigColumn));

                    // Update a random row.
                    getIds(stmt, ids, 1);
                    sqls.add(AllTypesTestUtils.buildUpdateSql(ids[0], allColumns, bigColumn));

                    // Batch update
                    batch = ThreadLocalRandom.current().nextInt(maxBatch) + 1;
                    getIds(stmt, ids, batch);
                    sqls.add(AllTypesTestUtils.buildUpdateSql(ids, batch, allColumns, bigColumn));

                    // Delete a random row.
                    getDeleteIds(stmt, ids, 1);
                    sqls.add(AllTypesTestUtils.buildDeleteSql(ids[0]));

                    // Batch delete
                    batch = ThreadLocalRandom.current().nextInt(maxBatch) + 1;
                    getDeleteIds(stmt, ids, batch);
                    sqls.add(AllTypesTestUtils.buildDeleteSql(ids, batch));

                } catch (Throwable t) {
                    if (t.getMessage()
                        .contains("The definition of the table required by the flashback query has changed")) {
                        // ignore
                    } else {
                        logger.error("Write only error, prepare sql failed.", t);
                    }
                    return;
                }

                // shuffle
                Collections.shuffle(sqls);

                // execute sql
                boolean inTrx = false;
                if (skipTrx == 0 || (skipTrx == 1 && ThreadLocalRandom.current().nextInt(2) == 0)) {
                    stmt.execute("begin");
                    inTrx = true;
                }
                for (String s : sqls) {
                    stmt.execute(s);
                }
                // Commit.
                if (inTrx) {
                    stmt.execute("commit");
                }
            } catch (SQLException e) {
                if (e.getMessage().contains("ERR_MISS_SEQUENCE")
                    || e.getMessage().contains("ERR_SEQUENCE")
                    || e.getMessage().contains("Duplicate entry")
                    || e.getMessage()
                    .contains("The definition of the table required by the flashback query has changed")
                    || e.getMessage().contains("Deadlock found when trying to get lock")) {
                    // no log
                } else {
                    error.set(e);
                }
            } finally {
                getLock().readLock().unlock();
            }
        });
    }

    private static void getIds(Statement stmt, long[] ids, int batch) throws SQLException {
        synchronized (lock) {
            // random select a start index
            if (size < start + batch) {
                // rebuild cachedIds
                rebuildIds(stmt);
            }
            if (size < start + batch) {
                throw new RuntimeException("Not enough ids size " + size + ", start " + start + " batch " + batch);
            }
            int randomStart = ThreadLocalRandom.current().nextInt(size - start - batch + 1);
            System.arraycopy(cachedIds, start + randomStart, ids, 0, batch);
        }
    }

    private static void getDeleteIds(Statement stmt, long[] ids, int batch) throws SQLException {
        synchronized (lock) {
            if (size < start + batch) {
                // rebuild cachedIds
                rebuildIds(stmt);
            }
            if (size < start + batch) {
                throw new RuntimeException("Not enough ids size " + size + ", start " + start + " batch " + batch);
            }
            System.arraycopy(cachedIds, start, ids, 0, batch);
            start += batch;
        }
    }

    private static void rebuildIds(Statement stmt) throws SQLException {
        stmt.execute("set sql_mode=''");
        stmt.execute("set transaction_isolation='repeatable-read'");
        stmt.execute("set transaction_policy=tso");
        stmt.execute("begin");
        String sql = "SELECT count(0) FROM " + TABLE_NAME;
        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
            size = rs.getInt(1);
            cachedIds = new long[size];
            start = 0;
        }
        sql = "SELECT id FROM " + TABLE_NAME;
        rs = stmt.executeQuery(sql);
        int i = 0;
        while (rs.next()) {
            cachedIds[i++] = rs.getLong(1);
        }
        stmt.execute("rollback");
    }

    public static ReadWriteLock getLock() {
        return readWriteLock;
    }
}

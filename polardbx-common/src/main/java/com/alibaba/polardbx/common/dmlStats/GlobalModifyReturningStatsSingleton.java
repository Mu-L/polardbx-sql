package com.alibaba.polardbx.common.dmlStats;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global singleton for collecting delete returning optimization statistics.
 * Statistics are periodically flushed by LogSystemMetricsScheduledJob.
 */
public class GlobalModifyReturningStatsSingleton {
    private static final GlobalModifyReturningStatsSingleton INSTANCE = new GlobalModifyReturningStatsSingleton();

    private AtomicLong counter = new AtomicLong(0);

    private AtomicLong returningCounter = new AtomicLong(0);

    private AtomicLong totalRows = new AtomicLong(0);

    private Set<String> databaseNames = ConcurrentHashMap.newKeySet();

    private Set<String> tableNames = ConcurrentHashMap.newKeySet();

    private Instant lastLogTime = Instant.now();

    private GlobalModifyReturningStatsSingleton() {
    }

    public static GlobalModifyReturningStatsSingleton getInstance() {
        return INSTANCE;
    }

    public void increment() {
        counter.incrementAndGet();
    }

    public void incrementReturning() {
        returningCounter.incrementAndGet();
    }

    public void incrementTotalRows(long rows) {
        totalRows.addAndGet(rows);
    }

    public void addDatabaseName(String databaseName) {
        databaseNames.add(databaseName);
    }

    public void addTableName(String tableName) {
        tableNames.add(tableName);
    }

    public String log() {
        ZonedDateTime beijingTime = lastLogTime.atZone(ZoneId.of("Asia/Shanghai"));
        LocalDateTime localTime = beijingTime.toLocalDateTime();
        long counterVal = counter.getAndSet(0);
        long returningCounterVal = returningCounter.getAndSet(0);
        long totalRowsVal = totalRows.getAndSet(0);
        int dbCount = databaseNames.size();
        int tbCount = tableNames.size();
        lastLogTime = Instant.now();
        return "@@deleteReturningLastLogTime: " + localTime.toString()
            + " @@deleteCount: " + counterVal
            + " @@deleteReturningCount: " + returningCounterVal
            + " @@deleteReturningDatabaseCount: " + dbCount
            + " @@deleteReturningTableCount: " + tbCount
            + " @@totalRows: " + totalRowsVal;
    }
}

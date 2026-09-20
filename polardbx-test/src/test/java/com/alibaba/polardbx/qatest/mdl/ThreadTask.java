package com.alibaba.polardbx.qatest.mdl;

import org.apache.commons.logging.Log;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一个封装了数据库连接的线程任务类。
 * 它提供了同步和异步执行SQL语句的方法，并使用SLF4J进行日志记录。
 */
public class ThreadTask implements AutoCloseable {

    private final String name; // 任务的名称，用于日志标识
    private final Connection connection;
    private final Log logger; // 外部传入的logger实例
    private final ExecutorService executor;
    private Future<?> lastSubmittedFuture;

    /**
     * 构造函数
     * @param connection 数据库连接
     * @param name       此任务的唯一名称，用于日志区分
     * @param logger     用于记录日志的SLF4J Logger实例
     */
    public ThreadTask(Connection connection, String name, Log logger) {
        if (connection == null) {
            throw new IllegalArgumentException("Connection cannot be null.");
        }
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name cannot be null or empty.");
        }
        if (logger == null) {
            throw new IllegalArgumentException("Logger cannot be null.");
        }
        this.connection = connection;
        this.name = name;
        this.logger = logger;
        this.executor = Executors.newSingleThreadExecutor();
    }

    /**
     * 同步执行SQL语句。
     * 调用此方法的线程将被阻塞，直到SQL执行完成。
     * @param sql 要执行的SQL语句
     */
    public void execSql(String sql) {
        logger.info(String.format("[%s] SYNC executing: %s", name, sql));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logger.info(String.format("[%s] SYNC finished: %s", name, sql));
        } catch (SQLException e) {
            logger.error(String.format("[%s] SYNC SQL execution failed for: %s", name, sql, e));
            throw new RuntimeException(e);
        }
    }

    public void startTransaction() throws SQLException {
        logger.info(String.format("[%s] SYNC start transaction", name));
        connection.setAutoCommit(false);
    }

    /**
     * 异步提交SQL语句执行。
     * 此方法立即返回，SQL将在后台线程中执行。
     * @param sql 要执行的SQL语句
     */
    public void submitSql(String sql) {
        logger.info(String.format("[%s] ASYNC submitting: %s", name, sql));
        this.lastSubmittedFuture = executor.submit(() -> {
            logger.info(String.format("[%s] ASYNC executing in background: %s", name, sql));
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
                logger.info(String.format("[%s] ASYNC finished in background: %s", name, sql));
            } catch (SQLException e) {
                // 这个异常将在 Future.get() 时被捕获并记录
                logger.error(String.format(String.format("[%s] ASYNC SQL execution failed for: %s", name, sql, e)));
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 等待最后一次提交的异步SQL任务完成。
     * @param timeoutSecond 等待的超时时间（毫秒）
     * @return 如果任务在超时时间内成功完成，则返回true；否则返回false。
     */
    public boolean waitTimeout(long timeoutSecond) {
        if (lastSubmittedFuture == null) {
            logger.debug(String.format("[%s] No submitted task to wait for.", name));
            return true;
        }

        logger.info(String.format("[%s] Waiting for submitted task with timeout %s s...", name, timeoutSecond));
        try {
            lastSubmittedFuture.get(timeoutSecond * 1000, TimeUnit.MILLISECONDS);
            logger.info(String.format("[%s] Submitted task completed successfully within timeout.", name));
            return true;
        } catch (TimeoutException e) {
            logger.warn(String.format("[%s] Waiting for submitted task timed out after %s s.", name, timeoutSecond));
            return false;
        } catch (InterruptedException e) {
            logger.warn(String.format("[%s] Waiting thread was interrupted.", name, e));
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            // 任务执行期间抛出的异常已经被后台线程记录，这里再记录一次以表明等待失败的原因
            logger.error(String.format("[%s] Submitted task failed with an exception.", name, e.getCause()));
            return false;
        }
    }

    /**
     * 关闭内部的线程池。
     */
    @Override
    public void close() {
        logger.info(String.format("[%s] Shutting down executor...", name));
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn(String.format("[%s] Executor did not terminate in 5 seconds, forcing shutdown.", name));
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn(String.format("[%s] Interrupted while waiting for executor shutdown.", name, e));
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

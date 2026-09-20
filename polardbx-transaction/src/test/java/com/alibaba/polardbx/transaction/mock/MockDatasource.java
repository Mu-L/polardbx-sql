package com.alibaba.polardbx.transaction.mock;

import lombok.Setter;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Mock DataSource implementation for testing purposes.
 * Allows configuring SQL query results for different SQL statements.
 */
public class MockDatasource implements DataSource {

    final Map<String, MockResultSetData> sqlToResultMap = new ConcurrentHashMap<>();
    private final Map<String, Long> sqlToDelayMap = new ConcurrentHashMap<>();
    @Setter
    private long executionDelayMs;
    private PrintWriter logWriter;
    private int loginTimeout = 0;

    /**
     * Default constructor with no execution delay.
     */
    public MockDatasource() {
        this(0);
    }

    /**
     * Constructor with execution delay configuration.
     *
     * @param executionDelayMs Delay in milliseconds to simulate query execution time
     */
    public MockDatasource(long executionDelayMs) {
        this.executionDelayMs = executionDelayMs;
    }

    /**
     * Configure the result set data for a specific SQL query.
     *
     * @param sql The SQL query string
     * @param columnNames Array of column names
     * @param rows List of rows, where each row is an array of column values
     */
    public void configureSqlResult(String sql, String[] columnNames, List<Object[]> rows) {
        MockResultSetData resultData = new MockResultSetData(columnNames, rows);
        sqlToResultMap.put(sql.trim().toLowerCase(), resultData);
    }

    /**
     * Configure a simple result set with one row of data.
     *
     * @param sql The SQL query string
     * @param columnNames Array of column names
     * @param row Single row of data
     */
    public void configureSqlResult(String sql, String[] columnNames, Object[] row) {
        List<Object[]> rows = new ArrayList<>();
        rows.add(row);
        configureSqlResult(sql, columnNames, rows);
    }

    /**
     * Configure the execution delay for a specific SQL query.
     *
     * @param sql The SQL query string
     * @param delayMs Delay in milliseconds for this specific SQL
     */
    public void configureSqlDelay(String sql, long delayMs) {
        sqlToDelayMap.put(sql.trim().toLowerCase(), delayMs);
    }

    /**
     * Get the execution delay for a specific SQL query.
     * Returns the configured delay for the SQL, or the default executionDelayMs if not configured.
     *
     * @param sql The SQL query string
     * @return Delay in milliseconds
     */
    public long getSqlDelay(String sql) {
        Long delay = sqlToDelayMap.get(sql.trim().toLowerCase());
        return delay != null ? delay : executionDelayMs;
    }

    /**
     * Clear all configured SQL results.
     */
    public void clearSqlResults() {
        sqlToResultMap.clear();
    }

    /**
     * Clear all configured SQL delays.
     */
    public void clearSqlDelays() {
        sqlToDelayMap.clear();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new MockConnection(this);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

}

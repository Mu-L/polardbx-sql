package com.alibaba.polardbx.transaction.mock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * Mock Statement implementation for testing purposes.
 */
public class MockStatement implements Statement {

    protected final MockConnection connection;
    protected final MockDatasource mockDatasource;
    protected boolean closed = false;
    protected int maxRows = 0;
    protected int queryTimeout = 0;
    protected int fetchSize = 0;
    protected int fetchDirection = ResultSet.FETCH_FORWARD;
    protected ResultSet currentResultSet;

    public MockStatement(MockConnection connection, MockDatasource mockDatasource) {
        this.connection = connection;
        this.mockDatasource = mockDatasource;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();

        // Simulate execution delay if configured
        long delayMs = mockDatasource.getSqlDelay(sql);
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Query execution was interrupted", e);
            }
        }

        String normalizedSql = sql.trim().toLowerCase();
        MockResultSetData resultData = mockDatasource.sqlToResultMap.get(normalizedSql);

        if (resultData == null) {
            // Return empty result set if no configuration found
            resultData = new MockResultSetData(new String[0], java.util.Collections.emptyList());
        }

        currentResultSet = new MockResultSet(this, resultData);
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        // Mock implementation - return 1 for any update
        return 1;
    }

    @Override
    public void close() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        executeQuery(sql);
        return true; // Assume all statements return a result set
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return -1; // No update count for queries
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false; // Only one result set
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        return new int[0]; // Empty batch
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        // Return empty result set
        MockResultSetData emptyData = new MockResultSetData(new String[0], java.util.Collections.emptyList());
        return new MockResultSet(this, emptyData);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        // Mock implementation - do nothing
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
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

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }
}

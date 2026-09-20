package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Executes ordered statements on a direct physical connection without adding a round trip between them.
 *
 * <p>X Protocol pipelines the leading statement with an ignored result. A JDBC connection sends the same
 * statements as a multi-statement request and advances through the returned results explicitly.</p>
 */
public final class DirectConnectionUtils {

    private DirectConnectionUtils() {
    }

    @FunctionalInterface
    public interface PreparedStatementConfigurer {
        void configure(PreparedStatement statement) throws SQLException;
    }

    @FunctionalInterface
    public interface ResultSetExtractor<T> {
        T extract(ResultSet resultSet) throws SQLException;
    }

    /**
     * Execute {@code leadingSql} immediately before {@code updateSql} in one X Protocol pipeline or one JDBC
     * multi-statement request, and return the affected rows of {@code updateSql}.
     */
    public static int executeUpdateAfter(Connection connection,
                                         String leadingSql,
                                         String updateSql,
                                         byte[] hint,
                                         PreparedStatementConfigurer configurer) throws SQLException {
        validate(connection, leadingSql, updateSql, configurer);
        if (connection.isWrapperFor(XConnection.class)) {
            XConnection xConnection = connection.unwrap(XConnection.class);
            try (PreparedStatement statement = prepareXStatement(connection, xConnection, updateSql, hint)) {
                configurer.configure(statement);
                executeLater(xConnection, leadingSql, hint);
                return statement.executeUpdate();
            }
        }

        try (PreparedStatement statement = connection.prepareStatement(
            withHint(leadingSql, hint) + ";" + withHint(updateSql, hint))) {
            configurer.configure(statement);
            boolean leadingIsResultSet = statement.execute();
            requireUpdateCount(statement, leadingIsResultSet, "leading statement");

            boolean updateIsResultSet = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            int affectedRows = requireUpdateCount(statement, updateIsResultSet, "update statement");
            ensureNoMoreResults(statement, "leading and update statements");
            return affectedRows;
        }
    }

    /**
     * Execute {@code leadingSql} immediately before {@code querySql} in one X Protocol pipeline or one JDBC
     * multi-statement request, and extract the result of {@code querySql}.
     */
    public static <T> T executeQueryAfter(Connection connection,
                                          String leadingSql,
                                          String querySql,
                                          byte[] hint,
                                          PreparedStatementConfigurer configurer,
                                          ResultSetExtractor<T> extractor) throws SQLException {
        validate(connection, leadingSql, querySql, configurer);
        if (extractor == null) {
            throw new IllegalArgumentException("extractor is required");
        }

        if (connection.isWrapperFor(XConnection.class)) {
            XConnection xConnection = connection.unwrap(XConnection.class);
            try (PreparedStatement statement = prepareXStatement(connection, xConnection, querySql, hint)) {
                configurer.configure(statement);
                executeLater(xConnection, leadingSql, hint);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return extractor.extract(resultSet);
                }
            }
        }

        try (PreparedStatement statement = connection.prepareStatement(
            withHint(leadingSql, hint) + ";" + withHint(querySql, hint))) {
            configurer.configure(statement);
            boolean leadingIsResultSet = statement.execute();
            requireUpdateCount(statement, leadingIsResultSet, "leading statement");

            boolean queryIsResultSet = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            if (!queryIsResultSet) {
                throw new SQLException("Query statement returned no result set");
            }
            T result;
            try (ResultSet resultSet = statement.getResultSet()) {
                result = extractor.extract(resultSet);
            }
            ensureNoMoreResults(statement, "leading and query statements");
            return result;
        }
    }

    /**
     * Execute control statements in order. All but the last X Protocol statement are pipelined; JDBC sends all
     * statements in one request and consumes every result.
     */
    public static void executeControlStatements(Connection connection, byte[] hint, String... sqls)
        throws SQLException {
        if (connection == null || sqls == null || sqls.length == 0) {
            throw new IllegalArgumentException("connection and at least one SQL statement are required");
        }
        for (String sql : sqls) {
            if (sql == null || sql.isEmpty()) {
                throw new IllegalArgumentException("SQL statements must not be empty");
            }
        }

        if (connection.isWrapperFor(XConnection.class)) {
            XConnection xConnection = connection.unwrap(XConnection.class);
            for (int i = 0; i < sqls.length - 1; i++) {
                executeLater(xConnection, sqls[i], hint);
            }
            xConnection.execUpdate(BytesSql.getBytesSql(sqls[sqls.length - 1]), hint, null, false);
            return;
        }

        StringBuilder multiSql = new StringBuilder();
        for (String sql : sqls) {
            if (multiSql.length() > 0) {
                multiSql.append(';');
            }
            multiSql.append(withHint(sql, hint));
        }
        try (Statement statement = connection.createStatement()) {
            drainAllResults(statement, statement.execute(multiSql.toString()));
        }
    }

    private static PreparedStatement prepareXStatement(Connection connection,
                                                       XConnection xConnection,
                                                       String sql,
                                                       byte[] hint)
        throws SQLException {
        if (hint == null || hint.length == 0) {
            return connection.prepareStatement(sql);
        }
        return xConnection.prepareStatement(BytesSql.getBytesSql(sql), hint);
    }

    private static void executeLater(XConnection connection, String sql, byte[] hint) throws SQLException {
        connection.execUpdate(BytesSql.getBytesSql(sql), hint, null, true);
    }

    private static int requireUpdateCount(Statement statement, boolean isResultSet, String operation)
        throws SQLException {
        int updateCount = statement.getUpdateCount();
        if (isResultSet || updateCount == -1) {
            throw new SQLException(operation + " did not return an update count");
        }
        return updateCount;
    }

    private static void ensureNoMoreResults(Statement statement, String operation) throws SQLException {
        boolean hasResultSet = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        if (hasResultSet) {
            try (ResultSet ignored = statement.getResultSet()) {
                throw new SQLException("Unexpected result set after " + operation);
            }
        }
        if (statement.getUpdateCount() != -1) {
            throw new SQLException("Unexpected extra update count after " + operation);
        }
    }

    private static void drainAllResults(Statement statement, boolean hasResultSet) throws SQLException {
        while (hasResultSet || statement.getUpdateCount() != -1) {
            if (hasResultSet) {
                try (ResultSet ignored = statement.getResultSet()) {
                    // Control statements should not return rows, but consume them defensively.
                }
            }
            hasResultSet = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        }
    }

    private static String withHint(String sql, byte[] hint) {
        return hint == null ? sql : new String(hint, StandardCharsets.UTF_8) + sql;
    }

    private static void validate(Connection connection,
                                 String leadingSql,
                                 String followingSql,
                                 PreparedStatementConfigurer configurer) {
        if (connection == null || leadingSql == null || followingSql == null || configurer == null) {
            throw new IllegalArgumentException(
                "connection, leadingSql, followingSql and configurer are required");
        }
    }
}

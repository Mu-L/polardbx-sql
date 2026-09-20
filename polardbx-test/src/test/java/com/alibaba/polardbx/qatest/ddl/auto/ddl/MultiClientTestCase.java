package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MultiClientTestCase extends DDLBaseNewDBTestCase {
    public int DDL_ENGINE_POLLING_INTERVAL_MS = 200;
    public int DDL_ENGINE_TIMEOUT_MS = 30000; // 30 seconds timeout
    public Boolean debug = false;
    public Boolean copyMode = false;
    public String datasource;

    /**
     * Configuration flags for test environment setup.
     * - debug:       If true, introduces a 5000ms delay after each task execution for easier debugging;
     * if false, uses a shorter 500ms delay.
     * - copyMode:    If true, prepares test data by copying from a specified source table instead of executing initSQL.
     * - datasource:         Specifies the name of the source table to copy data from when copyMode is true.
     * Format: "schema.tableName".
     */
    public JobInfo waitForJobStart(String tableName) {
        final int maxRetries = !debug ? 100 : 500;

        for (int i = 0; i < maxRetries; i++) {
            try {
                JobInfo jobInfo = fetchCurrentJob(tableName);
                if (jobInfo != null && DdlState.valueOf(jobInfo.parentJob.state) == DdlState.RUNNING) {
                    return jobInfo;
                }
            } catch (Exception e) {
                logger.warn(
                    "Failed to fetch current job for table: " + tableName + ", retrying... Error: " + e.getMessage());
            }

            try {
                Thread.sleep(DDL_ENGINE_POLLING_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        logger.error("Failed to get job info after " + maxRetries + " retries for table: " + tableName);
        return null;
    }

    public void changeMode(String database, String mode) throws Exception {
        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + database);
        JdbcUtil.executeUpdate(tddlConnection, "use information_schema");
        JdbcUtil.executeUpdate(tddlConnection, "create database " + database + " mode = " + mode);
        JdbcUtil.executeUpdate(tddlConnection, "use " + database);
    }

    /**
     * Create and initialize the table according to the given SQL set for testing.
     */
    public void prepareTable(Connection connection, String database, String table,
                             List<Pair<String, Object[]>> initSqlSet) throws Exception {
        if (copyMode) {
            prepareTable(connection, database, table,
                Pair.of(String.format("create table %s LIKE %s", table, datasource), new Object[] {}));
            executeUpdate(connection, String.format("INSERT INTO %s SELECT * FROM %s", table, datasource));
        } else {
            prepareTable(connection, database, table, initSqlSet.get(0));
        }

        for (Pair<String, Object[]> sqlPair : initSqlSet.subList(1, initSqlSet.size())) {
            executeUpdate(connection, sqlPair.getKey(), sqlPair.getValue());
        }
    }

    public void prepareTable(Connection connection, String database, String table,
                             Pair<String, Object[]> tableCreateSQL) throws Exception {
        List<String> result;
        result = executeQueryWithoutShowResult(connection, "show databases");
        boolean databaseExists = result.stream().anyMatch(o -> o.equalsIgnoreCase(database));
        if (!databaseExists) {
            throw new Exception("database does not exist");
        }

        executeUpdate(connection, "drop table if exists " + table);
        executeUpdate(connection, tableCreateSQL.getKey(), tableCreateSQL.getValue());
        result = executeQueryWithoutShowResult(connection, "show tables");
        //executeQuery(connection, String.format("desc %s", table));
        boolean tableExists = result.stream().anyMatch(o -> o.equalsIgnoreCase(table));
        if (!tableExists) {
            throw new Exception("table does not exist");
        }
    }

    public static List<String> executeQuery(Connection tddlConnection, String sqlTemplate, Object... args)
        throws Exception {
        try (PreparedStatement ps = tddlConnection.prepareStatement(sqlTemplate)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            final ResultSet resultSet = ps.executeQuery();
            List<List<Object>> result = JdbcUtil.getAllResult(resultSet);
            List<String> resultStr = resultSetToStr(result);
            logger.info(sqlPairToString(new Pair<>(sqlTemplate, args)));
            resultStr.forEach(logger::info);
            return resultStr;
        }
    }

    public List<String> executeQueryWithoutShowResult(Connection tddlConnection, String sqlTemplate, Object... args)
        throws Exception {
        try (PreparedStatement ps = tddlConnection.prepareStatement(sqlTemplate)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            final ResultSet resultSet = ps.executeQuery();
            List<List<Object>> result = JdbcUtil.getAllResult(resultSet);
            return resultSetToStr(result);
        }
    }

    /*
     * Avoid results.toString error.
     */
    public static List<String> resultSetToStr(List<List<Object>> results) {
        try {
            List<String> resultStr = new ArrayList<>();
            for (List<Object> result : results) {
                List<String> convertedResult = new ArrayList<>();
                for (Object o : result) {
                    if (String.valueOf(o).equals("null") || Objects.equals(o.toString(), "")) {
                        continue;
                    }
                    if (o instanceof JdbcUtil.MyNumber) {
                        convertedResult.add(((JdbcUtil.MyNumber) o).getNumber().toString());
                    } else if (o instanceof JdbcUtil.MyDate) {
                        LocalDateTime dateTime = ((Timestamp) ((JdbcUtil.MyDate) o).getDate()).toLocalDateTime();
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        String dateTimeString = dateTime.format(formatter);
                        convertedResult.add(dateTimeString);
                    } else {
                        convertedResult.add(o.toString());
                    }
                }
                resultStr.add(String.join(" ", convertedResult));
            }
            return resultStr;
        } catch (Exception e) {
            logger.error("resultSetToStr error", e);
        }
        return null;
    }

    /**
     * Converts a Pair<String, Object[]> to a formatted SQL string by replacing
     * placeholders (?) with the corresponding values from the Object[] array.
     */
    public static String sqlPairToString(Pair<String, Object[]> sqlPair) {
        if (sqlPair == null) {
            return null;
        }

        String sqlTemplate = sqlPair.getKey();
        Object[] parameters = sqlPair.getValue();

        if (sqlTemplate == null) {
            return null;
        }

        if (parameters == null || parameters.length == 0) {
            return sqlTemplate;
        }

        String formattedSql = sqlTemplate;
        for (Object param : parameters) {
            if (param == null) {
                formattedSql = formattedSql.replaceFirst("\\?", "NULL");
            } else if (param instanceof String) {
                String paramStr = ((String) param).replace("'", "''");
                formattedSql = formattedSql.replaceFirst("\\?", "'" + paramStr + "'");
            } else if (param instanceof java.util.Date) {
                formattedSql = formattedSql.replaceFirst("\\?", "'" + param.toString() + "'");
            } else {
                formattedSql = formattedSql.replaceFirst("\\?", param.toString());
            }
        }

        return formattedSql;
    }

    public void executeUpdate(Connection tddlConnection, String sqlTemplate, Object... args) throws Exception {
        try (PreparedStatement ps = tddlConnection.prepareStatement(sqlTemplate)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            logger.info(sqlPairToString(new Pair<>(sqlTemplate, args)));
            ps.executeUpdate();
        }
    }

    public Boolean checkIfFinished(Connection tddlConnection, Long jobId, String killPolicy) throws Exception {
        int maxRetries = 200;
        int retryIntervalMs = 500;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            List<String> results = executeQuery(tddlConnection,
                "select state from metadb.ddl_engine_archive where job_id = ?", jobId);
            if (!results.isEmpty()) {
                String state = results.get(0);
                if (killPolicy.equals("beforeCommitPoint")) {
                    return state.equals("ROLLBACK_COMPLETED");
                } else if (killPolicy.equals("inCommitPoint")) {
                    return state.equals("COMPLETED") || state.equals("ROLLBACK_COMPLETED");
                } else {
                    return state.equals("COMPLETED");
                }
            }
            Thread.sleep(retryIntervalMs);
        }

        throw new Exception("job does not exist after " + maxRetries + " retries");
    }

    public static Boolean checkJobState(Connection coon, Long jobId, List<String> expected) throws SQLException {
        int maxRetries = 1000;
        int retryIntervalMs = 500;

        try {
            for (int attempt = 0; attempt < maxRetries; attempt++) {
                List<String> results = executeQuery(coon,
                    "select state from metadb.ddl_engine_archive where job_id = ?", jobId);
                if (!results.isEmpty()) {
                    String state = results.get(0);
                    return expected.contains(state);
                }
                Thread.sleep(retryIntervalMs);
            }
        } catch (Exception e) {
            logger.error("Failed to get job info after " + maxRetries + " retries");
        }

        throw new SQLException("job does not exist after " + maxRetries + " retries");
    }
}
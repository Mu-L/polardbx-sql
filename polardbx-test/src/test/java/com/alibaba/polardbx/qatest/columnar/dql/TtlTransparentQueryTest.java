package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TtlTransparentQueryTest extends ColumnarReadBaseTestCase {

    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @BeforeClass
    public static void closeTtlHybridSchedule() throws Exception {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, String.format("set global %s = '%s'",
                ConnectionProperties.ALLOW_TTL_HYBRID_SCHEDULE_WITHOUT_COLUMNAR_NODE, "true"));
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set global %s = '%s'", ConnectionProperties.MAX_CCI_COUNT, "100"));

        }
        Thread.sleep(1000);
    }

    @AfterClass
    public static void recoverTtlHybridSchedule() throws Exception {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, String.format("set global %s = '%s'",
                ConnectionProperties.ALLOW_TTL_HYBRID_SCHEDULE_WITHOUT_COLUMNAR_NODE, "false"));
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set global %s = '%s'", ConnectionProperties.MAX_CCI_COUNT, "1"));
        }
    }

    public static void createTtlTable(Connection conn, String tableName) {
        String createTableSQL = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `text_field` varchar(2048),\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` datetime NOT NULL,\n" +
            "      `float_field` float NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`date_field`)\n" +
            "        PARTITION BY RANGE COLUMNS(`date_field`)\n" +
            "        (PARTITION `p20200101` VALUES LESS THAN ('2020-01-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `p20200201` VALUES LESS THAN ('2020-02-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `p20200301` VALUES LESS THAN ('2020-03-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `p20200401` VALUES LESS THAN ('2020-04-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `p20200501` VALUES LESS THAN ('2020-05-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `p20200601` VALUES LESS THAN ('2020-06-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `p20200701` VALUES LESS THAN ('2020-07-01 00:00:00') ENGINE = Columnar,\n" +
            "        PARTITION `pmax` VALUES LESS THAN (MAXVALUE) ENGINE = Columnar) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        createTableSQL = String.format(createTableSQL, tableName);
        JdbcUtil.executeUpdateSuccess(conn, createTableSQL);

    }

    public static String getTtlQueryBoundary(Connection conn, String database, String tableName) {
        String queryBoundarySQL = "select TTL_QUERY_BOUNDARY('%s', '%s')";
        queryBoundarySQL = String.format(queryBoundarySQL, database, tableName);
        String res = JdbcUtil.executeQueryAndGetFirstStringResult(queryBoundarySQL, conn);
        return res;
    }

    @Test
    public void testCleanUp() throws SQLException {
        String tableName = "testCleanUp_tb";
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            createTtlTable(connection, tableName);
            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            now = LocalDateTime.of(2020, 6, 1, 0, 0, 0);
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            now = LocalDateTime.of(2020, 8, 1, 0, 0, 0);
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testTtlColType_timestamp() throws Exception {

        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `timestamp_field` timestamp NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_ts` (`timestamp_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = timestamp_field EXPIRE AFTER 2 DAY TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testTtlColType_timestamp_tb";
        createTableSql = String.format(createTableSql, tableName);
        System.out.println(createTableSql);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, timestamp_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);
            JdbcUtil.executeUpdateSuccess(connection, "set time_zone = '+08:00'");

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, ChronoUnit.DAYS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }

    }

    @Test
    public void testTtlColType_timestamp1() throws Exception {

        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `timestamp_field` timestamp NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_ts` (`timestamp_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = timestamp_field EXPIRE AFTER 2 DAY TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testTtlColType_timestamp_tb";
        createTableSql = String.format(createTableSql, tableName);
        System.out.println(createTableSql);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, timestamp_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);
            JdbcUtil.executeUpdateSuccess(connection, "set time_zone = '+00:00'");

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, ChronoUnit.DAYS);
            now = now.minus(8, ChronoUnit.HOURS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }

    }

    @Test
    public void testTtlColType_int_second() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `second_field` bigint NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`second_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = FROM_UNIXTIME(`second_field`) EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testTtlColType_int_second_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, second_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.atOffset(ZoneOffset.of("+08:00")).toEpochSecond()));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            long time = now.atOffset(ZoneOffset.of("+08:00")).toEpochSecond();
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), String.valueOf(time));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(String.valueOf(time)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testTtlColType_int_millisecond() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `millisecond_field` bigint NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`millisecond_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = FROM_UNIXTIME(`millisecond_field` / 1000) EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testTtlColType_int_millsecond_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, millisecond_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.atOffset(ZoneOffset.of("+08:00")).toEpochSecond() * 1000));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            long time = now.atOffset(ZoneOffset.of("+08:00")).toEpochSecond();
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), String.valueOf(time * 1000));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(String.valueOf(time * 1000)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testTtlColType_datetime() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `datetime_field` datetime NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `datetime_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testTtlColType_datetime_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, datetime_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testTtlColType_date() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` date NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testTtlColType_date_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, date_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testPartitionArchive() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` datetime NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'PARTITION', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY RANGE COLUMNS(`date_field`)\n" +
            "    (PARTITION `pmin` VALUES LESS THAN ('2020-01-01 00:00:00'),\n" +
            "    PARTITION `p1` VALUES LESS THAN ('2020-02-01 00:00:00'),\n" +
            "    PARTITION `p2` VALUES LESS THAN ('2020-03-01 00:00:00'),\n" +
            "    PARTITION `p3` VALUES LESS THAN ('2020-04-01 00:00:00'),\n" +
            "    PARTITION `p4` VALUES LESS THAN ('2020-05-01 00:00:00'),\n" +
            "    PARTITION `p5` VALUES LESS THAN ('2020-06-01 00:00:00'),\n" +
            "    PARTITION `p6` VALUES LESS THAN ('2020-07-01 00:00:00'),\n" +
            "    PARTITION `p7` VALUES LESS THAN ('2020-08-01 00:00:00'),\n" +
            "    PARTITION `p8` VALUES LESS THAN ('2024-08-01 00:00:00'),\n" +
            "    PARTITION `p9` VALUES LESS THAN ('2025-08-01 00:00:00'),\n" +
            "    PARTITION `p10` VALUES LESS THAN ('2026-08-01 00:00:00'),\n" +
            "    PARTITION `p11` VALUES LESS THAN (MAXVALUE));";
        String tableName = "testPartitionArchive_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, date_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testChangeQueryBoundary() throws SQLException {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` datetime NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`)\n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 DAY TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testChangeQueryBoundary_tb";
        createTableSql = String.format(createTableSql, tableName);

        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, date_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = 0; i < 10000; i++) {
                LocalDateTime time = now.plus(i, ChronoUnit.MINUTES);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);
        }

        // 新增的多线程测试逻辑
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        AtomicInteger counter = new AtomicInteger(0);
        LocalDateTime startDateTime = LocalDateTime.of(2020, 1, 4, 0, 0, 0);
        int cleanupCount = 10;

        // 启动cleanup线程
        Thread cleanupThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection(DB_NAME)) {
                while (counter.get() < cleanupCount) {
                    // 设置TTL_DEBUG_CURRENT_DATETIME并执行cleanup
                    LocalDateTime currentDateTime = startDateTime.plus(counter.get(), ChronoUnit.DAYS);
                    JdbcUtil.executeUpdateSuccess(conn,
                        String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';",
                            currentDateTime.format(dateTimeFormatter)));
                    JdbcUtil.executeUpdateSuccess(conn, cleanupSql);

                    // 增加计数器
                    counter.incrementAndGet();

                    // 休眠100毫秒，避免过于频繁的操作
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                stopFlag.set(true);
            }
        });

        // 启动查询线程池
        ExecutorService queryExecutor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            queryExecutor.submit(() -> {
                try (Connection conn = getPolardbxConnection(DB_NAME)) {
                    while (!stopFlag.get()) {
                        // 执行查询操作
                        LocalDateTime startTime = startDateTime;
                        LocalDateTime endTime = startTime.plus(2, ChronoUnit.DAYS);
                        String querySql = String.format(
                            "select count(*) from %s where date_field >= '%s' and date_field <= '%s'",
                            tableName, startTime.format(dateTimeFormatter), endTime.format(dateTimeFormatter));
                        Assert.assertEquals(2 * 24 * 60,
                            Integer.parseInt(JdbcUtil.executeQueryAndGetFirstStringResult(querySql, conn)));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // 启动线程
        cleanupThread.start();

        try {
            // 等待cleanup线程结束
            cleanupThread.join();

            // 关闭查询线程池
            queryExecutor.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testPlanManager() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` date NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testPlanCache_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, date_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            //wait cci sync
            while (true) {
                Thread.sleep(1000);
                String countSql = String.format("select count(*) from %s force index(arctmp_myarc_t1)", tableName);
                if (JdbcUtil.executeQueryAndGetFirstStringResult(countSql, connection).equals("400")) {
                    break;
                }
            }

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            //检查执行计划
            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("true"));

            //cleanup
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            //检查执行计划
            explain = JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            //再次clean up
            now = now.plus(4, ChronoUnit.MONTHS);
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            //检查plan cache中的执行计划
            explain = JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            //hint HOT_ONLY
            String sql =
                String.format("select * from %s where date_field < '%s'", tableName, now.format(dateTimeFormatter));
            String hint = "/*+TDDL:TTL_QUERY_TYPE=HOT_ONLY*/";
            String baselineFix = "baseline fix sql ";
            explain = JdbcUtil.getExplainResult(connection, hint + sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertFalse(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            //baseline fix HOT_ONLY
            String baselineId = JdbcUtil.executeQueryAndGetFirstStringResult(baselineFix + hint + sql,
                connection);
            explain = JdbcUtil.getExplainResult(connection, sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertFalse(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, sql);
            JdbcUtil.executeUpdateSuccess(connection, String.format("baseline delete %s", baselineId));

            //hint COLD_ONLY
            sql = String.format("select * from %s where date_field >= '%s'", tableName, now.format(dateTimeFormatter));
            hint = "/*+TDDL:TTL_QUERY_TYPE=COLD_ONLY*/";
            explain = JdbcUtil.getExplainResult(connection, hint + sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertFalse(explain.contains("logicalview"));
            Assert.assertTrue(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            //baseline fix COLD_ONLY
            baselineId = JdbcUtil.executeQueryAndGetFirstStringResult(baselineFix + hint + sql,
                connection);
            explain = JdbcUtil.getExplainResult(connection, sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertFalse(explain.contains("logicalview"));
            Assert.assertTrue(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, sql);
            JdbcUtil.executeUpdateSuccess(connection, String.format("baseline delete %s", baselineId));

            //hint HOT_COMMON
            sql = String.format("select * from %s where date_field < '%s'", tableName, now.format(dateTimeFormatter));
            hint = "/*+TDDL:TTL_QUERY_TYPE=HOT_COMMON*/";
            explain = JdbcUtil.getExplainResult(connection, hint + sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertFalse(explain.contains("osstablescan"));
            Assert.assertFalse(explain.contains(now.format(dateTimeFormatter)));

            //baseline fix HOT_COMMON
            baselineId = JdbcUtil.executeQueryAndGetFirstStringResult(baselineFix + hint + sql, connection);
            explain = JdbcUtil.getExplainResult(connection, sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertFalse(explain.contains("osstablescan"));
            Assert.assertFalse(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, sql);
            JdbcUtil.executeUpdateSuccess(connection, String.format("baseline delete %s", baselineId));

            //hint HOT_AND_COLD
            sql = String.format("select * from %s where date_field between '%s' and '%s'", tableName,
                now.plusDays(30).format(dateTimeFormatter), now.plusDays(60).format(dateTimeFormatter));
            hint = "/*+TDDL:TTL_QUERY_TYPE=HOT_AND_COLD*/";
            explain = JdbcUtil.getExplainResult(connection, hint + sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertFalse(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            //baseline fix HOT_AND_COLD
            baselineId = JdbcUtil.executeQueryAndGetFirstStringResult(baselineFix + hint + sql, connection);
            explain = JdbcUtil.getExplainResult(connection, sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertFalse(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, sql);

            sql = String.format("select * from %s where date_field between '%s' and '%s'", tableName,
                now.minusDays(60).format(dateTimeFormatter), now.minusDays(30).format(dateTimeFormatter));
            explain = JdbcUtil.getExplainResult(connection, sql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertFalse(explain.contains("logicalview"));
            Assert.assertTrue(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, sql);

            sql = String.format("select * from %s where date_field between '%s' and '%s'", tableName,
                now.minusDays(60).format(dateTimeFormatter), now.plusDays(30).format(dateTimeFormatter));
            explain = JdbcUtil.getExplainResult(connection, sql).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("logicalview"));
            Assert.assertTrue(explain.contains("osstablescan"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, sql);
            JdbcUtil.executeUpdateSuccess(connection, String.format("baseline delete %s", baselineId));

            explain = JdbcUtil.getExplainResult(connection,
                String.format("/*+TDDL:TTL_QUERY_TYPE=HOT_COMMON*/select * from %s force index(arctmp_myarc_t1)",
                    tableName)).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertFalse(explain.contains("logicalview"));
            Assert.assertTrue(explain.contains("osstablescan"));
            Assert.assertFalse(explain.contains(now.format(dateTimeFormatter)));

            Assert.assertEquals("400",
                JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s", tableName),
                    connection));

            JdbcUtil.dropTable(connection, tableName);
        }

    }

//    @Test
//    public void testTtlRefColCleanup() throws Exception {
//        String createTableSql = "CREATE TABLE `%s` (\n" +
//            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
//            "      `int_field` int NOT NULL,\n" +
//            "      `date_field` datetime NOT NULL,\n" +
//            "      PRIMARY KEY (`id`),\n" +
//            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
//            "        PARTITION BY HASH(`int_field`) \n" +
//            "        COLUMNAR_OPTIONS='{\n" +
//            "        \"TYPE\":\"ARCHIVE\",\n" +
//            "        }',\n" +
//            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
//            "      LOCAL KEY `idx_int` (`int_field`)\n" +
//            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
//            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
//            +
//            "    PARTITION BY KEY(`id`)\n" +
//            "    PARTITIONS 8;";
//        String tableName = "testTtlRefColCleanup_tb";
//        createTableSql = String.format(createTableSql, tableName);
//        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
//        String insertSql = String.format("insert into %s(int_field, date_field) values ", tableName);
//
//        try (Connection connection = getPolardbxConnection(DB_NAME)) {
//            JdbcUtil.dropTable(connection, tableName);
//            JdbcUtil.executeUpdateSuccess(connection, createTableSql);
//
//            //prepare data
//            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
//            StringJoiner sj = new StringJoiner(",");
//            for (int i = -200; i < 200; i++) {
//                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
//                sj.add(String.format("(%s, '%s')", -Math.abs(i), time.format(dateTimeFormatter)));
//            }
//            insertSql += sj.toString();
//            JdbcUtil.executeUpdateSuccess(connection, insertSql);
//
//            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
//            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));
//
//            JdbcUtil.executeUpdateSuccess(connection,
//                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
//            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
//            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
//            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));
//
//            now = LocalDateTime.of(2020, 6, 1, 0, 0, 0);
//            JdbcUtil.executeUpdateSuccess(connection,
//                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
//            JdbcUtil.executeFailed(connection, cleanupSql,
//                "the ttl ref col in archive is larger than the ttl ref col in online");
//
//            String ddlJobId = JdbcUtil.executeQueryAndGetFirstStringResult("show ddl", connection);
//            Assert.assertNotNull(ddlJobId);
//            JdbcUtil.executeUpdateSuccess(connection, String.format("rollback ddl %s", ddlJobId));
//
//            JdbcUtil.dropTable(connection, tableName);
//        }
//    }

    @Test
    public void testShowTtlQueryBoundary() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` date NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String databaseName = "testShowTtlQueryBoundary_db";
        String tableName = "testShowTtlQueryBoundary_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String showTtlQueryBoundarySql = String.format("show ttl query boundary");

        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.createPartDatabase(connection, databaseName);
            JdbcUtil.useDb(connection, databaseName);

            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);

            String queryBoundary = getTtlQueryBoundary(connection, databaseName, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, databaseName, tableName),
                now.format(dateTimeFormatter));

            ResultSet rs = JdbcUtil.executeQuery(showTtlQueryBoundarySql, connection);
            boolean showUp = false;
            while (rs.next()) {
                if (rs.getString("schema").equals(databaseName) && rs.getString("table").equals(tableName)) {
                    Assert.assertEquals(rs.getString("queryBoundary"), now.format(dateTimeFormatter));
                    showUp = true;
                }
            }
            Assert.assertTrue(showUp);

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testShowTtlQueryStat() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `date_field` date NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`date_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `date_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String databaseName = "testShowTtlQueryStat_db";
        String tableName = "testShowTtlQueryStat_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String showTtlQueryStatSql = String.format("show ttl query stat");
        String clearTtlQueryStatSql = String.format("clear ttl query stat");

        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.createPartDatabase(connection, databaseName);
            JdbcUtil.useDb(connection, databaseName);

            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            // 清除之前的统计信息
            JdbcUtil.executeUpdateSuccess(connection, clearTtlQueryStatSql);

            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);

            // 设置TTL_DEBUG_CURRENT_DATETIME并进行cleanup
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);

            // 执行不同类型的查询来统计访问次数
            // 1. HOT_ONLY查询 - 查询热数据范围
            String hotQuery =
                String.format("explain select count(*) from %s where date_field = '2020-01-01'", tableName);
            JdbcUtil.executeQuerySuccess(connection, hotQuery);

            // 2. COLD_ONLY查询 - 查询冷数据范围
            String coldQuery =
                String.format("explain select count(*) from %s where date_field = '2019-12-01'", tableName);
            JdbcUtil.executeQuerySuccess(connection, coldQuery);

            // 3. HOT_AND_COLD查询 - 查询跨越冷热数据范围
            String hotAndColdQuery = String.format(
                "explain select count(*) from %s where date_field >= '2019-12-01' and date_field <= '2020-01-01'",
                tableName);
            JdbcUtil.executeQuerySuccess(connection, hotAndColdQuery);

            // 使用show ttl query stat检测结果是否对应
            ResultSet rs = JdbcUtil.executeQuery(showTtlQueryStatSql, connection);
            boolean showUp = false;
            while (rs.next()) {
                if (rs.getString("schema").equals(databaseName) && rs.getString("table").equals(tableName)) {
                    // 验证统计结果
                    Assert.assertTrue(rs.getLong("ttlQueryCount") > 0);
                    Assert.assertTrue(rs.getLong("hitHotOnlyCount") > 0);
                    Assert.assertTrue(rs.getLong("hitColdOnlyCount") > 0);
                    Assert.assertTrue(rs.getLong("hitHotAndColdCount") > 0);
                    showUp = true;
                }
            }
            Assert.assertTrue(showUp);

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testNoTtlHybrid() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `datetime_field` datetime NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
            "        PARTITION BY HASH(`int_field`) \n" +
            "        COLUMNAR_OPTIONS='{\n" +
            "        \"TYPE\":\"ARCHIVE\",\n" +
            "        }',\n" +
            "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `datetime_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12)\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testNoTtlHybrid_tb";
        createTableSql = String.format(createTableSql, tableName);
        System.out.println(createTableSql);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            String explain = JdbcUtil.getExplainResult(connection,
                String.format("/*+TDDL:workload_type=xx*/select * from %s", tableName)).toLowerCase();
            System.out.println(explain);
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("gather"));

            explain = JdbcUtil.getExplainResult(connection,
                    String.format("/*+TDDL:workload_type=xx TTL_QUERY_TYPE=HOT_AND_COLD*/select * from %s", tableName))
                .toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("gather"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            explain = JdbcUtil.getExplainResult(connection,
                String.format("/*+TDDL:workload_type=xx*/select * from %s", tableName)).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            Assert.assertTrue(explain.contains("gather"));

            explain = JdbcUtil.getExplainResult(connection,
                    String.format("/*+TDDL:workload_type=xx TTL_QUERY_TYPE=HOT_AND_COLD*/select * from %s", tableName))
                .toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            Assert.assertFalse(explain.contains("gather"));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testHybridAutoCreateArchiveCci() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `datetime_field` datetime NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `datetime_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
            +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testHybridAutoCreateArchiveCci_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            String showCreateTable = JdbcUtil.showFullCreateTable(connection, tableName).toLowerCase();
            System.out.println(showCreateTable);
            Assert.assertTrue(showCreateTable.contains("clustered columnar index"));
            Assert.assertTrue(showCreateTable.contains("columnar_options"));
            Assert.assertTrue(showCreateTable.contains("archive"));

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testAlterAutoCreateArchiveCci() throws Exception {
        String createTableSql = "CREATE TABLE `%s` (\n" +
            "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "      `int_field` int NOT NULL,\n" +
            "      `datetime_field` datetime NOT NULL,\n" +
            "      PRIMARY KEY (`id`),\n" +
            "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
            "      LOCAL KEY `idx_int` (`int_field`)\n" +
            "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
            "    PARTITION BY KEY(`id`)\n" +
            "    PARTITIONS 8;";
        String tableName = "testAlterAutoCreateArchiveCci_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String alterModifyTtlSql = String.format("alter table %s modify ttl " +
            "SET ttl_enable = 'on', ttl_expr = `datetime_field` expire after 2 month timezone '+08:00'," +
            " ttl_job = cron '0 1 */1 * * ?', ttl_cleanup = 'on', ttl_part_interval = interval(1, month)," +
            " archive_type = 'row', archive_table_pre_allocate = 12, " +
            "archive_table_post_allocate = 12, ttl_ref_col_list = 'int_field', ttl_hybrid = 'true'", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            JdbcUtil.executeUpdateSuccess(connection, alterModifyTtlSql);

            String showCreateTable = JdbcUtil.showFullCreateTable(connection, tableName).toLowerCase();
            System.out.println(showCreateTable);
            Assert.assertTrue(showCreateTable.contains("clustered columnar index"));
            Assert.assertTrue(showCreateTable.contains("columnar_options"));
            Assert.assertTrue(showCreateTable.contains("archive"));

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testSPM() throws SQLException {
        String createTableSql =
            "CREATE TABLE `%s` (\n" +
                "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "      `int_field` int NOT NULL,\n" +
                "      `datetime_field` datetime NOT NULL,\n" +
                "      PRIMARY KEY (`id`),\n" +
                "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
                "      LOCAL KEY `idx_int` (`int_field`)\n" +
                "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `datetime_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_HYBRID = 'true')\n"
                +
                "    PARTITION BY KEY(`id`)\n" +
                "    PARTITIONS 8;";
        String tableName = "testSPM_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, datetime_field) values ", tableName);
        String tableName1 = "testSPM_tb1";

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName1);
            JdbcUtil.executeUpdateSuccess(connection, String.format("create table %s (" +
                "      `id` bigint NOT NULL AUTO_INCREMENT," +
                "      `int_field` int NOT NULL," +
                "      `datetime_field` datetime NOT NULL," +
                "      PRIMARY KEY (`id`)" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8" +
                " PARTITION BY KEY(`id`) PARTITIONS 8; ", tableName1));

            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", -Math.abs(i), time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            now = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName), now.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));

            String joinSql = String.format("select * from %s t1 join %s t2 on t1.id = t2.id", tableName, tableName1);
            explain =
                JdbcUtil.getExplainResult(connection, joinSql).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(now.format(dateTimeFormatter)));
            JdbcUtil.executeQuerySuccess(connection, joinSql);

            joinSql += String.format(" where t1.datetime_field > '%s'", now.format(dateTimeFormatter));
            explain =
                JdbcUtil.getExplainResult(connection, joinSql).toLowerCase();
            Assert.assertFalse(explain.contains("unionall"));
            JdbcUtil.executeQuerySuccess(connection, joinSql);

            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.dropTable(connection, tableName1);
        }
    }

    @Test
    public void testCleanUpRowInRealNeed() throws Exception {
        String createTableSql =
            "CREATE TABLE `%s` (\n" +
                "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "      `int_field` int NOT NULL,\n" +
                "      `datetime_field` datetime NOT NULL,\n" +
                "      PRIMARY KEY (`id`),\n" +
                "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
                "        PARTITION BY HASH(`int_field`) \n" +
                "        COLUMNAR_OPTIONS='{\n" +
                "        \"TYPE\":\"ARCHIVE\",\n" +
                "        }',\n" +
                "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
                "      LOCAL KEY `idx_int` (`int_field`)\n" +
                "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `datetime_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'ROW', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_HYBRID='true')\n"
                +
                "    PARTITION BY KEY(`id`)\n" +
                "    PARTITIONS 8;";
        String tableName = "testCleanUpRowInRealNeed_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, datetime_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -400; i < 0; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            LocalDateTime arcBound = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName),
                arcBound.format(dateTimeFormatter));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';",
                    now.plus(4, java.time.temporal.ChronoUnit.MONTHS).format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName),
                arcBound.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(arcBound.format(dateTimeFormatter)));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

    @Test
    public void testCleanUpPartitionInRealNeed() throws Exception {
        String createTableSql =
            "CREATE TABLE `%s` (\n" +
                "      `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "      `int_field` int NOT NULL,\n" +
                "      `datetime_field` datetime NOT NULL,\n" +
                "      PRIMARY KEY (`id`),\n" +
                "      CLUSTERED COLUMNAR INDEX `arctmp_myarc_t1` (`int_field`)\n" +
                "        PARTITION BY HASH(`int_field`) \n" +
                "        COLUMNAR_OPTIONS='{\n" +
                "        \"TYPE\":\"ARCHIVE\",\n" +
                "        }',\n" +
                "      LOCAL KEY `idx_df` (`datetime_field`)，\n" +
                "      LOCAL KEY `idx_int` (`int_field`)\n" +
                "    ) ENGINE = InnoDB DEFAULT CHARSET = utf8\n" +
                "    TTL = TTL_DEFINITION ( TTL_ENABLE = 'ON', TTL_EXPR = `datetime_field` EXPIRE AFTER 2 MONTH TIMEZONE '+08:00', TTL_JOB = CRON '0 1 */1 * * ?', TTL_CLEANUP = 'ON', TTL_PART_INTERVAL = INTERVAL(1, MONTH), ARCHIVE_TYPE = 'PARTITION', ARCHIVE_TABLE_PRE_ALLOCATE = 12, ARCHIVE_TABLE_POST_ALLOCATE = 12, TTL_REF_COL_LIST = 'int_field', TTL_HYBRID = 'true')\n"
                +
                "    PARTITION BY RANGE COLUMNS(`datetime_field`)\n" +
                "    (PARTITION `pmin` VALUES LESS THAN ('2020-01-01 00:00:00'),\n" +
                "    PARTITION `p7` VALUES LESS THAN ('2020-08-01 00:00:00'),\n" +
                "    PARTITION `p8` VALUES LESS THAN ('2024-08-01 00:00:00'),\n" +
                "    PARTITION `p9` VALUES LESS THAN ('2025-08-01 00:00:00'),\n" +
                "    PARTITION `p10` VALUES LESS THAN ('2026-08-01 00:00:00'),\n" +
                "    PARTITION `p11` VALUES LESS THAN (MAXVALUE));";
        String tableName = "testCleanUpPartitionInRealNeed_tb";
        createTableSql = String.format(createTableSql, tableName);
        String cleanupSql = String.format("alter table %s cleanup expired data;", tableName);
        String insertSql = String.format("insert into %s(int_field, datetime_field) values ", tableName);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeUpdateSuccess(connection, createTableSql);

            //prepare data
            LocalDateTime now = LocalDateTime.of(2020, 3, 1, 0, 0, 0);
            StringJoiner sj = new StringJoiner(",");
            for (int i = -200; i < 200; i++) {
                LocalDateTime time = now.plus(i, java.time.temporal.ChronoUnit.DAYS);
                sj.add(String.format("(%s, '%s')", i, time.format(dateTimeFormatter)));
            }
            insertSql += sj.toString();
            JdbcUtil.executeUpdateSuccess(connection, insertSql);

            String queryBoundary = getTtlQueryBoundary(connection, DB_NAME, tableName);
            Assert.assertTrue(queryBoundary == null || queryBoundary.equalsIgnoreCase("NULL"));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';", now.format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            LocalDateTime arcBound = now.minus(2, java.time.temporal.ChronoUnit.MONTHS);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName),
                arcBound.format(dateTimeFormatter));

            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set TTL_DEBUG_CURRENT_DATETIME='%s';",
                    now.plus(4, java.time.temporal.ChronoUnit.MONTHS).format(dateTimeFormatter)));
            JdbcUtil.executeUpdateSuccess(connection, cleanupSql);
            Assert.assertEquals(getTtlQueryBoundary(connection, DB_NAME, tableName),
                arcBound.format(dateTimeFormatter));

            String explain =
                JdbcUtil.getExplainResult(connection, String.format("select * from %s", tableName)).toLowerCase();
            Assert.assertTrue(explain.contains("unionall"));
            Assert.assertTrue(explain.contains(arcBound.format(dateTimeFormatter)));

            JdbcUtil.dropTable(connection, tableName);
        }
    }

}
package com.alibaba.polardbx.qatest.ddl.auto.inplacebackfill;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 分区分裂下推执行测试类
 * <p>
 * 原地分裂支持策略：
 * 1. 原地分裂只支持对hash/key策略的分区进行
 * 2. 一级分区是hash/key而子分区是range分区时，对一级分区进行分裂是可以支持原地分裂的
 * 3. 一级分区是range，二级分区是key/hash时，对二级分区的分裂支持原地分裂
 * 4. 对二级分区不支持原地分裂（除非二级分区是key/hash类型）
 * <p>
 * 支持的数据类型：字符串、整数、时间、二进制的部分类型
 * 支持的表结构：一级分区、二级分区（模板化和非模板化）
 */
public class AlterTableSplitPartitionTest extends DDLBaseNewDBTestCase {
    private static final String DB_NAME = "test_inplace_backfill";
    static AtomicLong globalPkGenerator = new AtomicLong(500);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(tmpConnection, "create database if not exists " + DB_NAME + " mode=auto");
        }
    }

    @Before
    public void setUp() throws Exception {
        // 启用分区分裂下推执行功能
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_INPLACE_BACKFILL=true");
        // 设置支持的数据类型：数字+字符串+日期时间+二进制 (1111)
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111");
        globalPkGenerator = new AtomicLong(101);
    }

    @Test
    public void testBigIntPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好BigIntPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用BIGINT分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key,id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            String changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1110";
            JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
            assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
            changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111";
            JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
            List<String> ddlSqlList = new ArrayList<>();
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            ddlSqlList.add(splitSql);
            splitSql = "ALTER TABLE " + tableName + " SPLIT into ht1_ partitions 3 by hot value(1)";
            ddlSqlList.add(splitSql);
            for (String sql : ddlSqlList) {
                assertInplaceBackfillUsed(tableName, true, "explain " + sql, conn);
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, sql, 2, 100_000_000);
                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testBigIntUnsignedPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好BigIntUnsignedPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用BIGINT UNSIGNED分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT UNSIGNED NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testIntegerPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好IntegerPartitionKey`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用整数分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key2 INT NOT NULL,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key,id,partition_key2)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, partition_key2, name) VALUES (" + i + "," + i
                        + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            List<String> ddlSqlList = new ArrayList<>();
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            ddlSqlList.add(splitSql);
            splitSql = "ALTER TABLE " + tableName + " SPLIT into ht1_ partitions 3 by hot value(1)";
            ddlSqlList.add(splitSql);
            for (String sql : ddlSqlList) {
                assertInplaceBackfillUsed(tableName, true, "explain " + sql, conn);
                executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, sql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testIntegerUnsignedPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好IntegerUnsignedPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用INTEGER UNSIGNED分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT UNSIGNED NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testSmallIntPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好SmallIntPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用SMALLINT分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key SMALLINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + (i % 32768) + ", 'name_" + i
                        + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testSmallIntUnsignedPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好SmallIntUnsignedPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用SMALLINT UNSIGNED分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key SMALLINT UNSIGNED NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + (i % 65536) + ", 'name_" + i
                        + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testTinyIntPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好TinyIntPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用TINYINT分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key TINYINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + (i % 128) + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行（因为TINYINT类型受支持）
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testTinyIntUnsignedPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "`test-/~``@#$%好TinyIntUnsignedPartitionKey`";
            dropTableIfExists(conn, tableName);
            // 创建使用TINYINT UNSIGNED分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key TINYINT UNSIGNED NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + (i % 256) + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testDateTimePartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好DateTimePartitionKey`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用日期时间分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key DATETIME NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES ('2023-01-01 10:00:" +
                        String.format("%02d", i % 60) + "', 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            String changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1011";
            JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
            assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
            changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111";
            JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testDatePartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好DatePartitionKey`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用DATE分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key DATE NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES ('2023-01-" + String.format("%02d",
                        (i % 28) + 1) + "', 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行（因为DATE类型受支持）
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testTimestampPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好TimestampPartitionKey`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用TIMESTAMP分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key TIMESTAMP NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES ('2023-01-" + String.format("%02d",
                        (i % 28) + 1) + " 10:30:00', 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行（因为TIMESTAMP类型受支持）
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testUtf8mb4DefaultCollationPartitionKey() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                String tableName = "`test-/~``@#$%好Utf8mb4DefaultCollationPartitionKey_" + tbIndex + "`";
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                dropTableIfExists(conn, tableName);
                // 创建使用KEY分区策略的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY KEY(partition_key,name)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                String changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1101";
                JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111";
                JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
                List<String> ddlSqlList = new ArrayList<>();
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                ddlSqlList.add(splitSql);
                splitSql = "ALTER TABLE " + tableName + " SPLIT into ht3_ partitions 3 by hot value('2')";
                ddlSqlList.add(splitSql);
                for (String sql : ddlSqlList) {
                    assertInplaceBackfillUsed(tableName, true, "explain " + sql, conn);
                    executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, sql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testUtf8mb4DefaultCollationPartitionHash() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                String tableName = "`test-/~``@#$%好Utf8mb4DefaultCollationPartitionHash_" + tbIndex + "`";
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                dropTableIfExists(conn, tableName);
                // 创建使用字符串分区键的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testUtf8mb4SupportedGeneralCiCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Utf8mb4SupportedGeneralCiCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用UTF8MB4字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);
                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testUtf8mb4Supported0900AiCiCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Utf8mb4Supported0900AiCiCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用UTF8MB4字符集但使用不受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n" +
                    "PARTITION BY KEY(partition_key, name, id)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                List<String> ddlSqlList = new ArrayList<>();
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                ddlSqlList.add(splitSql);
                splitSql = "ALTER TABLE " + tableName + " SPLIT into ht3_ partitions 3 by hot value('3','3')";
                ddlSqlList.add(splitSql);
                for (String sql : ddlSqlList) {
                    assertInplaceBackfillUsed(tableName, true, "explain " + sql, conn);
                    executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, sql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testLatin1SupportedLatin1GeneralCiCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Latin1SupportedLatin1GeneralCiCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用LATIN1字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testAsciiSupportedAsciiBinCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好AsciiSupportedAsciiBinCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用ASCII字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testGbkUnSupportedCharset() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好GbkUnsupportedCharset_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用不受支持的字符集的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=gbk\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否没有使用下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                // 执行分区分裂操作，不应该使用下推执行（因为使用了不受支持的charset）
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testUtf8UnSupportedCharset() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Utf8UnSupportedCharset_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用UTF8字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testUtf8mb4SupportedUtf8Mb4BinCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Utf8mb4SupportedUtf8Mb4BinCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用UTF8MB4字符集和受支持的bin collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testLatin1SupportedLatin1BinCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Latin1SupportedLatin1BinCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用LATIN1字符集和受支持的bin collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testUtf8mb4SupportedUtf8mb4UnicodeCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Utf8mb4SupportedUtf8mb4UnicodeCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用UTF8MB4字符集和受支持的unicode collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testGb18030CharsetUnSupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Gb18030CharsetUnSupportedDefaultCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用GB18030字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=gb18030\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                // 执行分区分裂操作，应该使用下推执行（因为GB18030 charset受支持）
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testUtf8mb4CharsetSupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Utf8mb4CharsetSupportedDefaultCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用UTF8MB4字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行（因为UTF8MB4 charset受支持）
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testLatin1CharsetUnSupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好Latin1CharsetSupportedCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用LATIN1字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=latin1\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                // 执行分区分裂操作，应该使用下推执行（因为LATIN1 charset受支持）
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testAsciiCharsetUnSupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName = "`test-/~``@#$%好AsciiCharsetUnSupportedDefaultCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用ASCII字符集和受支持的collation的一级分区表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=ascii\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('key_" + i + "', 'name_" + i
                            + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                // 执行分区分裂操作，应该使用下推执行（因为ASCII charset受支持）
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testVarbinarySupportDataType() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好VarbinarySupportDataType`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用VARBINARY分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key VARBINARY(50) NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (UNHEX('" + String.format("%04d", i)
                        + "'), 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            String changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=111";
            JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
            assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
            changeSupportType = "set SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL=1111";
            JdbcUtil.executeUpdateSuccess(conn, changeSupportType);
            // 执行分区分裂操作，应该使用下推执行（因为VARBINARY类型受支持）
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testBinarySupportDataType() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好BinarySupportDataType`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用二进制分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BINARY(50) NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (UNHEX(" + i + "), 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否使用了下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
            // 执行分区分裂操作，应该使用下推执行
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 into partitions 3";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testKeyKeySubPartitionUtf8mb4SupportedCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                String tableName = "`test-/~``@#$%好testKeyKeySubPartitionUtf8mb4SupportedCollation_" + tbIndex + "`";
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                dropTableIfExists(conn, tableName);
                // 创建使用KEY + KEY二级分区的表
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    (tbIndex == 0 ? "  partition_key2 VARCHAR(50) NOT NULL,\n" :
                        "  partition_key2 CHAR(50) NOT NULL,\n") +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY KEY(partition_key)\n" +
                    "PARTITIONS 2\n" +
                    "SUBPARTITION BY KEY(partition_key2)\n" +
                    "SUBPARTITIONS 2";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql = "INSERT INTO " + tableName
                        + " (partition_key, partition_key2, name) VALUES ('key_" +
                        (i % 10) + "', 'subkey_" + i + "', 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否使用了下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);
                // 执行分区分裂操作，应该使用下推执行
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1";
                executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testHashKeySubPartitionUtf8mb4SupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                String tableName =
                    "`test-/~``@#$%好HKSubPartUtf8mb4SupDefCollation_" + tbIndex + "`";
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                dropTableIfExists(conn, tableName);
                // 创建使用HASH + KEY二级分区的表（一级分区是hash，二级分区是key）
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    (tbIndex == 0 ? "  partition_key2 VARCHAR(50) NOT NULL,\n" :
                        "  partition_key2 CHAR(50) NOT NULL,\n") +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY HASH(partition_key,partition_key2)\n" +
                    "PARTITIONS 2\n" +
                    "SUBPARTITION BY KEY(partition_key2)\n" +
                    "SUBPARTITIONS 2";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, partition_key2, name) VALUES (" +
                            (i % 10) + ", 'subkey_" + i + "', 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                List<Boolean> splitLevels = new ArrayList<>();
                splitLevels.add(Boolean.TRUE);
                splitLevels.add(Boolean.FALSE);
                for (Boolean splitLevel : splitLevels) {
                    boolean isSubPartition = splitLevel;
                    String partName = isSubPartition ? "sp1" : "p1";
                    // 验证是否使用了下推执行（通过检查执行计划）
                    assertInplaceSplitBackfillUsed(tableName, partName, null, true, isSubPartition, conn);
                    // 支持原地分裂
                    String splitSql =
                        "ALTER TABLE " + tableName + (isSubPartition ? " SPLIT SUBPARTITION " : " SPLIT PARTITION ")
                            + partName;
                    executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testKeyHashSubPartitionUtf8mb4SupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName =
                    "`test-/~``@#$%好KHSubPartUtf8mb4SupDefCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用KEY + HASH二级分区的表（一级分区是key，二级分区是hash）
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ? "  partition_key VARCHAR(50) NOT NULL,\n" : "  partition_key CHAR(50) NOT NULL,\n")
                    +
                    "  partition_key2 INT NOT NULL,\n" +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY KEY(partition_key)\n" +
                    "PARTITIONS 2\n" +
                    "SUBPARTITION BY HASH(partition_key2, partition_key)\n" +
                    "SUBPARTITIONS 2";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, partition_key2, name) VALUES ('key_" +
                            (i % 10) + "', " + i + ", 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                List<Boolean> splitLevels = new ArrayList<>();
                splitLevels.add(Boolean.TRUE);
                splitLevels.add(Boolean.FALSE);
                for (Boolean splitLevel : splitLevels) {
                    boolean isSubPartition = splitLevel;
                    String partName = isSubPartition ? "sp1" : "p1";
                    // 验证是否使用了下推执行（通过检查执行计划）
                    assertInplaceSplitBackfillUsed(tableName, partName, null, true, isSubPartition, conn);
                    // 对一级分区进行分裂，应该支持原地分裂
                    String splitSql =
                        "ALTER TABLE " + tableName + (isSubPartition ? " SPLIT SUBPARTITION " : " SPLIT PARTITION ")
                            + partName;
                    executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testRangeHashSubPartitionUtf8mb4SupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName =
                    "`test-/~``@#$%好RHSubParUtf8mb4SupDefCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用RANGE + HASH二级分区的表（一级分区是range，二级分区是hash）
                // 对二级分区进行分裂应该支持原地分裂
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    "  partition_key INT NOT NULL,\n" +
                    (tbIndex == 0 ? "  partition_key2 VARCHAR(50) NOT NULL,\n" :
                        "  partition_key2 CHAR(50) NOT NULL,\n") +
                    "  name VARCHAR(100),\n" +
                    "  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY RANGE (partition_key)\n" +
                    "SUBPARTITION BY HASH (partition_key2, partition_key)\n" +
                    "SUBPARTITIONS 2 (\n" +
                    "  PARTITION p1 VALUES LESS THAN (100),\n" +
                    "  PARTITION p2 VALUES LESS THAN (200),\n" +
                    "  PARTITION p3 VALUES LESS THAN MAXVALUE\n" +
                    ")";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, partition_key2, name) VALUES (" +
                            (i * 2) + ", 'subkey_" + i + "', 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                List<Boolean> splitLevels = new ArrayList<>();
                splitLevels.add(Boolean.TRUE);
                splitLevels.add(Boolean.FALSE);
                for (Boolean splitLevel : splitLevels) {
                    boolean isSubPartition = splitLevel;
                    String partName = isSubPartition ? "sp1" : "p1";
                    // 验证是否使用了下推执行（通过检查执行计划）
                    // 对子分区进行分裂，支持原地分裂， 对一级range分区分裂，不支持
                    String splitSql =
                        "ALTER TABLE " + tableName + (isSubPartition ? " SPLIT SUBPARTITION " : " SPLIT PARTITION ")
                            + partName + (isSubPartition ? " " :
                            " into (partition p1_1 values less than(30), partition p1_2 values less than (100))");

                    assertInplaceSplitBackfillUsed(tableName, partName, (isSubPartition ? null :
                            " into (partition p1_1 values less than(30), partition p1_2 values less than (100))"),
                        isSubPartition,
                        isSubPartition, conn);

                    executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testRangeKeySubPartitionUtf8mb4SupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                String tableName =
                    "`test-/~``@#$%好RKSubPartUtf8mb4SupDefCollation_" + tbIndex + "`";
                dropTableIfExists(conn, tableName);
                // 创建使用RANGE + KEY二级分区的表（一级分区是range，二级分区是key）
                // 对二级分区进行分裂应该支持原地分裂
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    "  partition_key INT NOT NULL,\n" +
                    (tbIndex == 0 ? "  partition_key2 VARCHAR(50) NOT NULL,\n" :
                        "  partition_key2 CHAR(50) NOT NULL,\n") +
                    "  name VARCHAR(100),\n" +
                    "  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY RANGE (partition_key)\n" +
                    "SUBPARTITION BY KEY (partition_key2,partition_key)\n" +
                    "SUBPARTITIONS 2 (\n" +
                    "  PARTITION p1 VALUES LESS THAN (100),\n" +
                    "  PARTITION p2 VALUES LESS THAN (200),\n" +
                    "  PARTITION p3 VALUES LESS THAN MAXVALUE\n" +
                    ")";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, partition_key2, name) VALUES (" +
                            (i * 2) + ", 'subkey_" + i + "', 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                List<Boolean> splitLevels = new ArrayList<>();
                splitLevels.add(Boolean.TRUE);
                splitLevels.add(Boolean.FALSE);
                for (Boolean splitLevel : splitLevels) {
                    boolean isSubPartition = splitLevel;
                    String partName = isSubPartition ? "sp1" : "p1";
                    // 验证是否使用了下推执行（通过检查执行计划）
                    // 对子分区进行分裂，支持原地分裂， 对一级range分区分裂，不支持
                    String splitSql =
                        "ALTER TABLE " + tableName + (isSubPartition ? " SPLIT SUBPARTITION " : " SPLIT PARTITION ")
                            + partName + (isSubPartition ? " " :
                            " into (partition p1_1 values less than(30), partition p1_2 values less than (100))");

                    assertInplaceSplitBackfillUsed(tableName, partName, (isSubPartition ? null :
                            " into (partition p1_1 values less than(30), partition p1_2 values less than (100))"),
                        isSubPartition,
                        isSubPartition, conn);

                    executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testHashHashNoTempSubPartitionUtf8mb4SupportedDefaultCollation() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                String tableName =
                    "`test-/~``@#$%好HashHashNoTempSubPUtf8mb4SupDefCollation_" + tbIndex + "`";
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                dropTableIfExists(conn, tableName);
                // 创建使用非模板化二级分区的表（HASH + HASH）
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    "  partition_key INT NOT NULL,\n" +
                    (tbIndex == 0 ? "  partition_key2 VARCHAR(50) NOT NULL,\n" :
                        "  partition_key2 CHAR(50) NOT NULL,\n") +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY HASH(partition_key,partition_key2)\n" +
                    "PARTITIONS 2\n" +
                    "SUBPARTITION BY HASH(partition_key2,partition_key)(partition p1 subpartitions 2, partition p2 subpartitions 4)";

                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql = "INSERT INTO " + tableName
                        + " (partition_key, partition_key2, name) VALUES (" +
                        (i % 10) + ", 'subkey_" + i + "', 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                List<Boolean> splitLevels = new ArrayList<>();
                splitLevels.add(Boolean.TRUE);
                splitLevels.add(Boolean.FALSE);
                for (Boolean splitLevel : splitLevels) {
                    boolean isSubPartition = splitLevel;
                    String partName = isSubPartition ? "p1sp1" : "p1";
                    // 验证是否使用了下推执行（通过检查执行计划）
                    assertInplaceSplitBackfillUsed(tableName, partName, null, isSubPartition, isSubPartition, conn);
                    // 支持原地分裂
                    String splitSql =
                        "ALTER TABLE " + tableName + (isSubPartition ? " SPLIT SUBPARTITION " : " SPLIT PARTITION ")
                            + partName;
                    executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                    // 验证表结构和数据完整性
                    String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                    ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                    rs.next();
                    rs.close();
                }
            }
        }
    }

    @Test
    public void testHashRangeSubPartitionSupportedIntType() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好HashRangeSubPartitionSupportedIntType`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用HASH + RANGE二级分区的表（一级分区是hash/key而子分区是range）
            // 对一级分区进行分裂是可以支持原地分裂的
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  partition_key2 INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 2\n" +
                "SUBPARTITION BY RANGE(partition_key2) (\n" +
                "  SUBPARTITION sp1 VALUES LESS THAN (100),\n" +
                "  SUBPARTITION sp2 VALUES LESS THAN MAXVALUE\n" +
                ")";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql = "INSERT INTO " + tableName
                    + " (partition_key, partition_key2, name) VALUES (" +
                    (i % 10) + ", " + (i * 2) + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            List<Boolean> splitLevels = new ArrayList<>();
            splitLevels.add(Boolean.TRUE);
            splitLevels.add(Boolean.FALSE);
            for (Boolean splitLevel : splitLevels) {
                boolean isSubPartition = splitLevel;
                String partName = isSubPartition ? "sp1" : "p2";
                // 验证是否使用了下推执行（通过检查执行计划）
                // 对子分区进行分裂，支持原地分裂， 对一级range分区分裂，不支持
                String splitSql =
                    "ALTER TABLE " + tableName + (isSubPartition ? " SPLIT SUBPARTITION " : " SPLIT PARTITION ")
                        + partName + (isSubPartition ?
                        " into (subpartition sp1_1 values less than(30), subpartition sp1_2 values less than (100))" :
                        " ");

                String splitHotVal = "ALTER TABLE " + tableName + " SPLIT INTO sht_1 partitions 3 by hot value(5) ";

                if (!isSubPartition) {
                    assertInplaceBackfillUsed(tableName, true, "explain " + splitHotVal, conn);
                    JdbcUtil.executeUpdateSuccess(conn, splitHotVal);
                }
                assertInplaceSplitBackfillUsed(tableName, partName, (isSubPartition ?
                        " into (subpartition sp1_1 values less than(30), subpartition sp1_2 values less than (100))" :
                        null), !isSubPartition,
                    isSubPartition, conn);

                executeDmlConcurrentlyDuringDdl(conn, tableName, true, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testRangePartitionUnSupportedStrategy() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好RangePartitionUnSupportedStrategy`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用RANGE分区策略的一级分区表（不支持原地分裂）
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY RANGE(partition_key) (\n" +
                "  PARTITION p1 VALUES LESS THAN (100),\n" +
                "  PARTITION p2 VALUES LESS THAN (200),\n" +
                "  PARTITION p3 VALUES LESS THAN MAXVALUE\n" +
                ")";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i
                        + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否没有使用下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1", " at(50) into (partition p1_1, partition p1_2)", false,
                conn);
            // 执行分区分裂操作，不应该使用下推执行（因为RANGE分区不支持）
            String splitSql =
                "ALTER TABLE " + tableName + " SPLIT PARTITION p1 at(50) into (partition p1_1, partition p1_2)";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testListPartitionUnSupportedStrategy() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好ListPartitionUnSupportedStrategy`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用LIST分区策略的一级分区表（不支持原地分裂）
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY LIST(partition_key) (\n" +
                "  PARTITION p1 VALUES IN (1,2,3,4,5),\n" +
                "  PARTITION p2 VALUES IN (6,7,8,9,10),\n" +
                "  PARTITION p3 VALUES IN (11,12,13,14,15)\n" +
                ")";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            for (int i = 1; i <= 15; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i
                        + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            // 验证是否没有使用下推执行（通过检查执行计划）
            assertInplaceSplitBackfillUsed(tableName, "p1",
                " into(partition p1_1 values in(1,2), partition p1_2 values in (3,4,5)); ", false, conn);
            // 执行分区分裂操作，不应该使用下推执行（因为LIST分区不支持）
            String splitSql = "ALTER TABLE " + tableName
                + " SPLIT PARTITION p1 into(partition p1_1 values in(1,2), partition p1_2 values in (3,4,5))";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            // 验证表结构和数据完整性
            String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
            rs.next();
            rs.close();
        }
    }

    @Test
    public void testUnsupportedUtf8mb3() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            for (int tbIndex = 0; tbIndex < 2; tbIndex++) {
                // 切换到测试库
                String tableName = "`test-/~``@#$%好UnsupportedDataType_" + tbIndex + "`";
                JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
                dropTableIfExists(conn, tableName);
                // 创建使用不支持的数据类型分区键的一级分区表（如TEXT类型）
                String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                    "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                    (tbIndex == 0 ?
                        "  partition_key varchar(60) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL,\n" :
                        "  partition_key char(60) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL,\n") +
                    "  name VARCHAR(100),\n" +
                    "  PRIMARY KEY (id)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                    "PARTITION BY HASH(partition_key)\n" +
                    "PARTITIONS 4";
                JdbcUtil.executeUpdateSuccess(conn, createTableSql);
                String setTg = "alter table " + tableName + " set tablegroup = ''";
                JdbcUtil.executeUpdateSuccess(conn, setTg);
                // 插入测试数据
                for (int i = 1; i <= 100; i++) {
                    String insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES ('text_key_" + i
                            + "', 'name_" + i + "')";
                    JdbcUtil.executeUpdateSuccess(conn, insertSql);
                }
                // 验证是否没有使用下推执行（通过检查执行计划）
                assertInplaceSplitBackfillUsed(tableName, "p1", null, false, conn);
                // 执行分区分裂操作，应该不使用下推执行（因为TEXT类型不支持）
                String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1";
                executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

                // 验证表结构和数据完整性
                String checkDataSql = "SELECT COUNT(*) FROM " + tableName;
                ResultSet rs = JdbcUtil.executeQuery(checkDataSql, conn);
                rs.next();
                rs.close();
            }
        }
    }

    @Test
    public void testHotKeySplit() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            // 切换到测试库
            String tableName = "`test-/~``@#$%好MultPartitionKey`";
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            dropTableIfExists(conn, tableName);
            // 创建使用多列分区键的一级分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  a INT NOT NULL, primary key(id))\n" +
                "PARTITION BY key(a,id) PARTITIONS 5"
                + "(PARTITION `p1` VALUES LESS THAN (1,9223372036854775807) ENGINE = InnoDB,\n"
                + " PARTITION `p2` VALUES LESS THAN (5634770598966349852,9223372036854775807),\n"
                + " PARTITION `p3` VALUES LESS THAN (5634770598966349863,9223372036854775807),\n"
                + " PARTITION `p4` VALUES LESS THAN (5634770598966349864,9223372036854775807),\n"
                + " PARTITION `p5` VALUES LESS THAN (9223372036854775807,9223372036854775807))";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            String setTg = "alter table " + tableName + " set tablegroup = ''";
            JdbcUtil.executeUpdateSuccess(conn, setTg);
            // 插入测试数据
            String insertSql = "INSERT INTO " + tableName
                + " (a) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)";
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
            for (int i = 1; i <= 5; i++) {
                insertSql = "INSERT INTO " + tableName + " (a) select a+13 from " + tableName;
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
                insertSql = "INSERT INTO " + tableName + " (a) select a+119 from " + tableName;
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }
            insertSql = "INSERT INTO " + tableName + " (a) select 88 from " + tableName + " limit 4096";
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p3 into partitions 4";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

            splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p2 into partitions 4";
            executeDmlConcurrentlyDuringDdl(conn, tableName, false, false, splitSql, 2, 100_000_000);

        }
    }

    private void assertInplaceSplitBackfillUsed(String tableName, String partitionName, String newPartDef,
                                                boolean expectInplaceBackfill, boolean isSubPartitionSplit,
                                                Connection conn)
        throws SQLException {
        // 使用EXPLAIN语句检查分区分裂的执行计划
        String splitPartition = isSubPartitionSplit ? "SPLIT SUBPARTITION " : "SPLIT PARTITION ";
        String explainSql = String.format("EXPLAIN ALTER TABLE %s %s %s ", tableName, splitPartition, partitionName);
        explainSql = newPartDef != null ? explainSql + newPartDef : explainSql;
        assertInplaceBackfillUsed(tableName, expectInplaceBackfill, explainSql, conn);
    }

    private void assertInplaceBackfillUsed(String tableName, boolean expectInplaceBackfill, String explainSql,
                                           Connection conn) throws SQLException {
        ResultSet resultSet = JdbcUtil.executeQuery(explainSql, conn);
        // 收集所有执行计划信息
        List<String> taskInfos = new ArrayList<>();
        while (resultSet.next()) {
            String taskInfo = resultSet.getString(1);
            taskInfos.add(taskInfo);
        }
        // 检查是否包含AlterTableGroupInplaceBackFillTask
        boolean hasInplaceBackfillTask = false;
        for (String taskInfo : taskInfos) {
            if (taskInfo.contains("INPLACE_BACKFILL_TASK")) {
                hasInplaceBackfillTask = true;
                break;
            }
        }
        if (expectInplaceBackfill) {
            if (!hasInplaceBackfillTask) {
                throw new AssertionError("Expected inplace backfill to be used for table " + tableName +
                    ", but AlterTableGroupInplaceBackFillTask not found in execution plan. " +
                    "Execution plan: " + taskInfos.toString());
            }
        } else {
            if (hasInplaceBackfillTask) {
                throw new AssertionError("Expected inplace backfill NOT to be used for table " + tableName +
                    " (unsupported data type), but AlterTableGroupInplaceBackFillTask found in execution plan. " +
                    "Execution plan: " + taskInfos.toString());
            }
        }
    }

    /**
     * 通过EXPLAIN语句检查DDL执行计划来判断是否使用了下推执行
     *
     * @param tableName 表名
     * @param partitionName 分区名
     * @param expectInplaceBackfill 是否期望使用下推执行
     */
    private void assertInplaceSplitBackfillUsed(String tableName, String partitionName, String newPartDef,
                                                boolean expectInplaceBackfill, Connection conn) throws SQLException {
        assertInplaceSplitBackfillUsed(tableName, partitionName, newPartDef, expectInplaceBackfill, false, conn);
    }

    //todo add ntp test

    /**
     * 在DDL执行期间并行执行DML操作
     *
     * @param conn 数据库连接
     * @param tableName 表名
     * @param hasSubpartitionKey 是否有二级分区键（true: id+partition_key+partition_key2+name, false: id+partition_key+name）
     * @param isDatetimeType 分区键是否为datetime类型（默认false，如果为true则使用datetime值，否则使用数字字符串）
     * @param ddlSql DDL语句
     * @param threadCount DML并发线程数
     * @param dmlOpsPerThread 每个线程执行的DML操作数
     */
    public void executeDmlConcurrentlyDuringDdl(Connection conn, String tableName, boolean hasSubpartitionKey,
                                                boolean isDatetimeType, String ddlSql, int threadCount,
                                                int dmlOpsPerThread) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount + 1);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount + 1);
        AtomicBoolean ddlStarted = new AtomicBoolean(false);
        AtomicBoolean ddlCompleted = new AtomicBoolean(false);
        AtomicBoolean ddlFailed = new AtomicBoolean(false);
        AtomicReference<Throwable> ddlFailure = new AtomicReference<>();
        AtomicInteger successfulDmls = new AtomicInteger(0);
        AtomicInteger failedDmls = new AtomicInteger(0);
        ResultSet rs = JdbcUtil.executeQuery("select max(id) from " + tableName, conn);
        Long maxId = 0L;
        try {
            if (rs.next()) {
                maxId = rs.getLong(1);
                if (maxId > globalPkGenerator.incrementAndGet()) {
                    globalPkGenerator.set(maxId + 1);
                }
            }
        } catch (SQLException e) {
            //
        } finally {
            try {
                rs.close();
            } catch (Exception ex) {

            }
        }
        // DDL执行线程
        executor.submit(() -> {
            try {
                startLatch.await();
                ddlStarted.set(true);
                System.out.println("[DDL Thread] Starting DDL: " + ddlSql);
                JdbcUtil.executeUpdateSuccess(conn, ddlSql);
                System.out.println("[DDL Thread] DDL execution completed, setting flag...");
                ddlCompleted.set(true);
                System.out.println("[DDL Thread] DDL completed successfully, flag set to: " + ddlCompleted.get());
            } catch (Exception | AssertionError e) {
                System.err.println("[DDL Thread] DDL failed: " + e.getMessage());
                ddlFailed.set(true);
                ddlFailure.set(e);
                e.printStackTrace();
            } finally {
                ddlFailed.set(!ddlCompleted.get());
                completionLatch.countDown();
                System.out.println(
                    "[DDL Thread] CountDownLatch decremented, remaining count: " + completionLatch.getCount());
            }
        });

        // DML执行线程
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try (Connection dmlConn = getPolardbxConnection(DB_NAME)) {
                    startLatch.await();
                    Random random = new Random(threadId);

                    for (int opIdx = 0; opIdx < dmlOpsPerThread; opIdx++) {
                        if (ddlFailed.get()) {
                            throw new RuntimeException("DDL failed, cannot execute DML");
                        }
                        try {
                            int operationType = random.nextInt(4); // 0: INSERT, 1: REPLACE, 2: UPDATE, 3: DELETE
                            String dmlSql = generateDmlSql(tableName, hasSubpartitionKey, isDatetimeType,
                                operationType, threadId, opIdx, random);

                            if (dmlSql != null) {
                                JdbcUtil.executeUpdate(dmlConn, dmlSql);
                                successfulDmls.incrementAndGet();

                                if (opIdx % 100 == 0) {
                                    System.out.println(String.format("[DML Thread-%d] Executed %d operations, " +
                                            "DDL started: %b, DDL completed: %b",
                                        threadId, opIdx + 1, ddlStarted.get(), ddlCompleted.get()));
                                }
                            }

                            // 随机延迟，模拟真实负载
                            if (random.nextInt(10) < 2) {
                                Thread.sleep(random.nextInt(10));
                            }
                        } catch (Exception e) {
                            failedDmls.incrementAndGet();
                            // 忽略预期的错误（如表在重建中、死锁、锁超时、重复键、只读状态等）
                            if (shouldLogDmlError(e)) {
                                System.err.println(String.format("[DML Thread-%d] DML operation %d failed: %s",
                                    threadId, opIdx, e.getMessage()));
                                throw new RuntimeException(
                                    String.format("Unexpected DML error in thread %d at operation %d",
                                        threadId, opIdx), e);
                            }
                        }

                        // 如果DDL已完成且已执行足够的操作，提前退出
                        boolean isDdlCompleted = ddlCompleted.get();
                        if (isDdlCompleted && opIdx > 2000) {
                            System.out.println(String.format(
                                "[DML Thread-%d] Early exit: DDL completed at operation %d (ddlCompleted=%b)",
                                threadId, opIdx, isDdlCompleted));
                            break;
                        }
                    }

                    System.out.println(String.format("[DML Thread-%d] Completed all operations", threadId));
                } catch (Exception e) {
                    System.err.println(String.format("[DML Thread-%d] Thread failed: %s", threadId, e.getMessage()));
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // 启动所有线程
        startLatch.countDown();

        try {
            // 等待所有线程完成
            completionLatch.await();
        } catch (InterruptedException ex) {
            // 线程被中断，记录错误信息并终止测试
            System.err.println("[Main Thread] Thread interrupted while waiting for completion: " + ex.getMessage());
            Thread.currentThread().interrupt();
            Assert.fail("Thread was interrupted while waiting for DML operations to complete: " + ex.getMessage());
        } finally {
            // 确保线程池被正确关闭，释放资源
            executor.shutdown();
        }

        System.out.println(String.format("\n=== Execution Summary ==="));
        System.out.println(String.format("DDL completed: %b", ddlCompleted.get()));
        System.out.println(String.format("Successful DMLs: %d", successfulDmls.get()));
        System.out.println(String.format("Failed DMLs: %d", failedDmls.get()));
        System.out.println(String.format("Total DMLs: %d\n", successfulDmls.get() + failedDmls.get()));

        if (!ddlCompleted.get() && isRetriableDdlCancellation(ddlFailure.get())) {
            try {
                Thread.sleep(2000L);
                try (Connection retryConnection = getPolardbxConnection(DB_NAME)) {
                    JdbcUtil.executeUpdateSuccess(retryConnection, ddlSql);
                    ddlCompleted.set(true);
                    ddlFailed.set(false);
                    ddlFailure.set(null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("Interrupted while retrying cancelled DDL: " + ddlSql);
            } catch (Exception | AssertionError e) {
                ddlFailure.set(e);
            }
        }

        // 验证DDL成功完成
        Throwable failure = ddlFailure.get();
        Assert.assertTrue("DDL should complete successfully"
            + (failure == null ? "" : ": " + failure.getMessage()), ddlCompleted.get());
    }

    private boolean isRetriableDdlCancellation(Throwable failure) {
        String message = failure == null ? null : failure.getMessage();
        return message != null && message.contains("The DDL job has been cancelled or interrupted");
    }

    /**
     * 生成DML SQL语句
     *
     * @param tableName 表名
     * @param hasSubpartitionKey 是否有二级分区键
     * @param isDatetimeType 是否为datetime类型
     * @param operationType 操作类型 (0: INSERT, 1: REPLACE, 2: UPDATE, 3: DELETE)
     * @param threadId 线程ID
     * @param opIdx 操作索引
     * @param random 随机数生成器
     * @return DML SQL语句
     */
    private String generateDmlSql(String tableName, boolean hasSubpartitionKey, boolean isDatetimeType,
                                  int operationType, int threadId, int opIdx, Random random) {
        int recordId = threadId * 100000 + opIdx;
        String partitionKeyValue;
        String subpartitionKeyValue = null;
        String nameValue = "'name_" + recordId + "'";
        long currentMaxPk = globalPkGenerator.get();
        long targetId = 0;
        if (currentMaxPk > 1000) {
            targetId = random.nextInt((int) currentMaxPk) + 1;
        }
        long pk = globalPkGenerator.getAndIncrement();
        // 生成分区键值
        if (isDatetimeType) {
            // datetime类型：使用日期时间字符串
            int day = (recordId % 28) + 1;
            int hour = (recordId % 24);
            int minute = (recordId % 60);
            int second = (recordId % 60);
            partitionKeyValue = String.format("'2023-01-%02d %02d:%02d:%02d'", day, hour, minute, second);

            if (hasSubpartitionKey) {
                int day2 = ((recordId + 1) % 28) + 1;
                subpartitionKeyValue = String.format("'2023-01-%02d %02d:%02d:%02d'",
                    day2, (hour + 1) % 24, (minute + 1) % 60, (second + 1) % 60);
            }
        } else {
            // 非datetime类型：使用数字字符串
            partitionKeyValue = "'" + (recordId % 10000) + "'";

            if (hasSubpartitionKey) {
                subpartitionKeyValue = "'" + ((recordId + 1) % 10000) + "'";
            }
        }

        String sql = null;

        switch (operationType) {
        case 0: // INSERT
            if (hasSubpartitionKey) {
                sql = String.format("INSERT INTO %s (id, partition_key, partition_key2, name) VALUES (%d, %s, %s, %s)",
                    tableName, pk, partitionKeyValue, subpartitionKeyValue, nameValue);
            } else {
                sql = String.format("INSERT INTO %s (id, partition_key, name) VALUES (%d, %s, %s)",
                    tableName, pk, partitionKeyValue, nameValue);
            }
            break;

        case 1: // REPLACE
            if (hasSubpartitionKey) {
                sql = String.format("REPLACE INTO %s (id, partition_key, partition_key2, name) VALUES (%d, %s, %s, %s)",
                    tableName, pk, partitionKeyValue, subpartitionKeyValue, nameValue);
            } else {
                sql = String.format("REPLACE INTO %s (id, partition_key, name) VALUES (%d, %s, %s)",
                    tableName, pk, partitionKeyValue, nameValue);
            }
            break;

        case 2: // UPDATE
            long updateId = targetId > 0 ? targetId : random.nextInt(1000) + 1;
            String newName = "'updated_name_" + recordId + "'";
            sql = String.format("UPDATE %s SET name = %s WHERE id = %d",
                tableName, newName, updateId);
            break;

        case 3: // DELETE
            long deleteId = targetId > 0 ? targetId : random.nextInt(1000) + 1;
            sql = String.format("DELETE FROM %s WHERE id = %d", tableName, deleteId);
            break;
        }

        return sql;
    }

    /**
     * 判断是否应该记录DML错误日志
     * 忽略一些预期的错误，如死锁、锁超时、重复键、只读状态等
     *
     * @param e 异常对象
     * @return 是否应该记录错误日志
     */
    private boolean shouldLogDmlError(Exception e) {
        String errorMsg = e.getMessage();
        if (errorMsg == null) {
            return true;
        }

        return !errorMsg.contains("Deadlock found when trying to get lock") &&
            !errorMsg.contains("Lock wait timeout exceeded") &&
            !errorMsg.contains("Duplicate entry") &&
            !errorMsg.contains("Table is in readonly status") &&
            !errorMsg.contains("is ongoing") &&
            !errorMsg.contains("doesn't exist") &&
            !errorMsg.contains("Table has no partition for the values");
    }

    // ========== 新增：状态验证和边界条件测试 ==========

    /**
     * 测试：分裂完成后验证物理表readonly状态已清除
     * 必须通过DDL语句显式检查SECONDARY_ENGINE_ATTRIBUTE属性
     */
    @Test
    public void testReadonlyStatusClearedAfterSplit() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_readonly_cleared";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            for (int i = 1; i <= 50; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 执行分区分裂
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);

            // 验证readonly状态已清除
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证DML可以正常执行
            String insertAfterSplit = "INSERT INTO " + tableName + " (partition_key, name) VALUES (999, 'after_split')";
            JdbcUtil.executeUpdateSuccess(conn, insertAfterSplit);

            String updateAfterSplit = "UPDATE " + tableName + " SET name = 'updated' WHERE partition_key = 999";
            JdbcUtil.executeUpdateSuccess(conn, updateAfterSplit);

            String deleteAfterSplit = "DELETE FROM " + tableName + " WHERE partition_key = 999";
            JdbcUtil.executeUpdateSuccess(conn, deleteAfterSplit);
        }
    }

    /**
     * 测试：极大数据量分区的分裂
     */
    @Test
    public void testLargeDataPartitionSplit() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_large_data_split";
            dropTableIfExists(conn, tableName);

            // 创建测试表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key INT NOT NULL,\n" +
                "  data VARCHAR(500),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 批量插入大量数据
            StringBuilder batchInsert = new StringBuilder();
            batchInsert.append("INSERT INTO ").append(tableName).append(" (partition_key, data) VALUES ");
            for (int batch = 0; batch < 10; batch++) {
                StringBuilder values = new StringBuilder();
                for (int i = 0; i < 100; i++) {
                    if (values.length() > 0) {
                        values.append(",");
                    }
                    int pk = batch * 100 + i;
                    values.append("(").append(pk).append(", 'data_").append(pk).append("_")
                        .append(String.format("%0400d", pk)).append("')");
                }
                JdbcUtil.executeUpdateSuccess(conn, batchInsert.toString() + values.toString());
            }

            // 记录分裂前的数据量
            int beforeCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Should have 1000 rows before split", 1000, beforeCount);

            // 执行分区分裂
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 3";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);

            // 验证数据完整性
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged after split", beforeCount, afterCount);

            // 验证readonly状态已清除
            verifyReadonlyStatusCleared(conn, tableName);
        }
    }

    /**
     * 测试：多维度KEY分区的分裂
     */
    @Test
    public void testMultiDimensionKeySplit() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_multi_dim_key";
            dropTableIfExists(conn, tableName);

            // 创建多维度KEY分区表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  col1 BIGINT NOT NULL,\n" +
                "  col2 BIGINT NOT NULL,\n" +
                "  col3 BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(col1, col2, col3, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql = "INSERT INTO " + tableName + " (col1, col2, col3, name) VALUES ("
                    + i + ", " + (i * 2) + ", " + (i * 3) + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            int beforeCount = getTableRowCount(conn, tableName);

            // 验证使用inplace backfill
            assertInplaceSplitBackfillUsed(tableName, "p1", null, true, conn);

            // 执行分区分裂
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 3";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);

            // 验证数据完整性
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged", beforeCount, afterCount);

            // 验证readonly状态已清除
            verifyReadonlyStatusCleared(conn, tableName);
        }
    }

    /**
     * 测试：连续多次分裂同一个表
     */
    @Test
    public void testConsecutiveSplits() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_consecutive_splits_" + RandomStringUtils.randomNumeric(5);
            dropTableIfExists(conn, tableName);

            // 创建测试表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入测试数据
            for (int i = 1; i <= 100; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            int initialCount = getTableRowCount(conn, tableName);

            // 第一次分裂
            String splitSql1 = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql1);
            verifyReadonlyStatusCleared(conn, tableName);

            // 插入更多数据
            for (int i = 101; i <= 120; i++) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            // 第二次分裂
            String splitSql2 = "ALTER TABLE " + tableName + " SPLIT PARTITION p2 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql2);
            verifyReadonlyStatusCleared(conn, tableName);

            // 第三次分裂
            String splitSql3 = "ALTER TABLE " + tableName + " SPLIT PARTITION p3 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql3);
            verifyReadonlyStatusCleared(conn, tableName);

            // 验证数据完整性
            int finalCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should be 120", 120, finalCount);
        }
    }

    /**
     * 测试：NULL值处理
     */
    @Test
    public void testNullValueHandling() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_null_values_" + RandomStringUtils.randomNumeric(5);
            dropTableIfExists(conn, tableName);

            // 创建测试表（允许NULL值）
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY HASH(partition_key)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入包含NULL的数据
            for (int i = 1; i <= 50; i++) {
                String insertSql;
                if (i % 5 == 0) {
                    // 每5条插入一个NULL值
                    insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES (NULL, 'name_null_" + i + "')";
                } else {
                    insertSql =
                        "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + i + ", 'name_" + i + "')";
                }
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            int beforeCount = getTableRowCount(conn, tableName);

            // 执行分区分裂
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);

            // 验证数据完整性
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged", beforeCount, afterCount);

            // 验证NULL值数据仍然存在
            String checkNullSql = "SELECT COUNT(*) FROM " + tableName + " WHERE partition_key IS NULL";
            ResultSet rs = JdbcUtil.executeQuery(checkNullSql, conn);
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Should have 10 NULL rows", 10, rs.getInt(1));
            rs.close();
        }
    }

    /**
     * 测试：边界值数据的分裂
     */
    @Test
    public void testBoundaryValuesSplit() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdate(conn, "use " + DB_NAME);
            String tableName = "test_boundary_values_" + RandomStringUtils.randomNumeric(5);
            dropTableIfExists(conn, tableName);

            // 创建测试表
            String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
                "  partition_key BIGINT NOT NULL,\n" +
                "  name VARCHAR(100),\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY KEY(partition_key, id)\n" +
                "PARTITIONS 4";
            JdbcUtil.executeUpdateSuccess(conn, createTableSql);
            JdbcUtil.executeUpdateSuccess(conn, "alter table " + tableName + " set tablegroup = ''");

            // 插入边界值数据
            List<Long> boundaryValues = new ArrayList<>();
            boundaryValues.add(Long.MIN_VALUE);
            boundaryValues.add(Long.MIN_VALUE + 1);
            boundaryValues.add(-1L);
            boundaryValues.add(0L);
            boundaryValues.add(1L);
            boundaryValues.add(Long.MAX_VALUE - 1);
            boundaryValues.add(Long.MAX_VALUE);

            for (Long value : boundaryValues) {
                String insertSql =
                    "INSERT INTO " + tableName + " (partition_key, name) VALUES (" + value + ", 'boundary_" + value
                        + "')";
                JdbcUtil.executeUpdateSuccess(conn, insertSql);
            }

            int beforeCount = getTableRowCount(conn, tableName);

            // 执行分区分裂
            String splitSql = "ALTER TABLE " + tableName + " SPLIT PARTITION p1 INTO PARTITIONS 2";
            JdbcUtil.executeUpdateSuccess(conn, splitSql);

            // 验证数据完整性
            int afterCount = getTableRowCount(conn, tableName);
            Assert.assertEquals("Data count should remain unchanged", beforeCount, afterCount);

            // 验证边界值数据仍然可以查询
            for (Long value : boundaryValues) {
                String checkSql = "SELECT COUNT(*) FROM " + tableName + " WHERE partition_key = " + value;
                ResultSet rs = JdbcUtil.executeQuery(checkSql, conn);
                Assert.assertTrue(rs.next());
                Assert.assertEquals("Boundary value " + value + " should exist", 1, rs.getInt(1));
                rs.close();
            }
        }
    }

    // ========== Helper Methods ==========

    /**
     * 验证所有物理表的readonly状态已被清除
     * 必须通过DDL语句显式检查SECONDARY_ENGINE_ATTRIBUTE属性
     */
    private void verifyReadonlyStatusCleared(Connection conn, String tableName) throws SQLException {
        String showTopology = "SHOW TOPOLOGY FROM " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopology, conn);

        List<String[]> phyTables = new ArrayList<>();
        while (topologyRs.next()) {
            String groupName = topologyRs.getString("GROUP_NAME");
            String phyTableName = topologyRs.getString("TABLE_NAME");
            phyTables.add(new String[] {groupName, phyTableName});
        }
        topologyRs.close();

        List<String> stillReadOnlyTables = new ArrayList<>();
        for (String[] phyTable : phyTables) {
            String groupName = phyTable[0];
            String phyTableName = phyTable[1];

            String checkSql = "/*+TDDL:NODE('" + groupName + "')*/ " +
                "SELECT secondary_engine_attribute FROM information_schema.tables_extensions " +
                "WHERE table_name = '" + phyTableName + "'";

            try {
                ResultSet rs = JdbcUtil.executeQuery(checkSql, conn);
                if (rs.next()) {
                    String attribute = rs.getString(1);
                    if (attribute != null && attribute.contains("polarx.readonly") && attribute.contains("true")) {
                        stillReadOnlyTables.add(groupName + "." + phyTableName);
                    }
                }
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (!stillReadOnlyTables.isEmpty()) {
            Assert.fail("Following physical tables are still readonly after split: " + stillReadOnlyTables);
        }
    }

    /**
     * 获取表的行数
     */
    private int getTableRowCount(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        return count;
    }
}

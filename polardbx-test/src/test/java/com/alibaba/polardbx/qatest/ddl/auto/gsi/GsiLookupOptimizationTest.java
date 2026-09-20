package com.alibaba.polardbx.qatest.ddl.auto.gsi;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;

/**
 * 测试各种类型下GSI回表优化的正确性
 * 仿照GsiLookupTest，扩展更多的测试场景
 */
public class GsiLookupOptimizationTest extends BaseTestCase {

    @Test
    public void testGsiLookupWithDifferentDataTypes() throws SQLException {
        createDBAndTbForDataTypeTest();

        try (Connection c = getPolardbxConnection("gsi_lookup_datatype_test")) {
            c.createStatement().execute("set global GSI_LOOKUP_OPTIMIZE_THRESHOLD=0");

            // 插入不同数据类型的测试数据
            insertTestDataForDataTypes(c);

            // 测试VARCHAR类型GSI回表优化
            testVarcharTypeGsiLookup(c);

            // 测试INT类型GSI回表优化  
            testIntTypeGsiLookup(c);

            // 测试BIGINT类型GSI回表优化
            testBigintTypeGsiLookup(c);

            // 测试DATETIME类型GSI回表优化
            testDatetimeTypeGsiLookup(c);
        }
    }

    @Test
    public void testGsiLookupWithRangeQueries() throws SQLException {
        createDBAndTbForRangeQueryTest();

        try (Connection c = getPolardbxConnection("gsi_lookup_range_test")) {
            c.createStatement().execute("set global GSI_LOOKUP_OPTIMIZE_THRESHOLD=0");

            // 插入测试数据
            insertTestDataForRangeQueries(c);

            // 测试范围查询的GSI回表优化
            testRangeQueryGsiLookup(c);

            // 测试BETWEEN查询的GSI回表优化
            testBetweenQueryGsiLookup(c);

            // 测试比较运算符的GSI回表优化
            testComparisonGsiLookup(c);
        }
    }

    private void testVarcharTypeGsiLookup(Connection c) throws SQLException {
        // 测试VARCHAR类型索引的回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_datatype force index(g_i_varchar_col) where varchar_col = 'test_value_1'";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("VARCHAR GSI Lookup count: " + count);
        // 期望触发回表优化，执行计划应该较少
        assertThat(count).isEqualTo(17);

        // 验证查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql = "select * from t_datatype force index(g_i_varchar_col) where varchar_col = 'test_value_1'";
        verifyResultConsistency(c, baseSql, "VARCHAR GSI");
        verifyResultConsistencyForAP(c, baseSql, "VARCHAR GSI");
    }

    private void testIntTypeGsiLookup(Connection c) throws SQLException {
        // 测试INT类型索引的回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_datatype force index(g_i_int_col) where int_col = 100";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("INT GSI Lookup count: " + count);
        assertThat(count).isEqualTo(17);

        // 验证查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql = "select * from t_datatype force index(g_i_int_col) where int_col = 100";
        verifyResultConsistency(c, baseSql, "INT GSI");
        verifyResultConsistencyForAP(c, baseSql, "INT GSI");
    }

    private void testBigintTypeGsiLookup(Connection c) throws SQLException {
        // 测试BIGINT类型索引的回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_datatype force index(g_i_bigint_col) where bigint_col = 1000000";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("BIGINT GSI Lookup count: " + count);
        assertThat(count).isEqualTo(17);

        // 验证查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql = "select * from t_datatype force index(g_i_bigint_col) where bigint_col = 1000000";
        verifyResultConsistency(c, baseSql, "BIGINT GSI");
        verifyResultConsistencyForAP(c, baseSql, "BIGINT GSI");
    }

    private void testDatetimeTypeGsiLookup(Connection c) throws SQLException {
        // 测试DATETIME类型索引的回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_datatype force index(g_i_datetime_col) where datetime_col = '2023-01-01 00:00:00'";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("DATETIME GSI Lookup count: " + count);
        assertThat(count).isEqualTo(17);

        // 验证查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql =
            "select * from t_datatype force index(g_i_datetime_col) where datetime_col = '2023-01-01 00:00:00'";
        verifyResultConsistency(c, baseSql, "DATETIME GSI");
        verifyResultConsistencyForAP(c, baseSql, "DATETIME GSI");
    }

    private void testRangeQueryGsiLookup(Connection c) throws SQLException {
        // 测试范围查询的GSI回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_range force index(g_i_score) where score > 85 and score < 95";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("Range Query GSI Lookup count: " + count);
        assertThat(count).isEqualTo(23);

        // 验证范围查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql = "select * from t_range force index(g_i_score) where score > 85 and score < 95";
        verifyResultConsistency(c, baseSql, "Range Query GSI");
        verifyResultConsistencyForAP(c, baseSql, "Range Query GSI");
    }

    private void testBetweenQueryGsiLookup(Connection c) throws SQLException {
        // 测试BETWEEN查询的GSI回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_range force index(g_i_score) where score between 80 and 90";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("BETWEEN Query GSI Lookup count: " + count);
        assertThat(count).isEqualTo(25);

        // 验证BETWEEN查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql = "select * from t_range force index(g_i_score) where score between 80 and 90";
        verifyResultConsistency(c, baseSql, "BETWEEN Query GSI");
        verifyResultConsistencyForAP(c, baseSql, "BETWEEN Query GSI");
    }

    private void testComparisonGsiLookup(Connection c) throws SQLException {
        // 测试比较运算符的GSI回表优化
        String sql =
            "trace /*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ select * from t_range force index(g_i_score) where score >= 88";
        c.createStatement().execute(sql);
        ResultSet rs = c.createStatement().executeQuery("show trace");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        System.out.println("Comparison GSI Lookup count: " + count);
        assertThat(count).isEqualTo(32);

        // 验证比较查询结果的正确性 - 对比开启/关闭优化的结果集
        String baseSql = "select * from t_range force index(g_i_score) where score >= 88";
        verifyResultConsistency(c, baseSql, "Comparison GSI");
        verifyResultConsistencyForAP(c, baseSql, "Comparison GSI");
    }

    /**
     * 验证开启和关闭GSI回表优化的结果行数是否一致
     */
    private void verifyResultConsistency(Connection c, String baseSql, String testName) throws SQLException {
        // 执行开启优化的查询
        String sqlWithOptimize = "/*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true*/ " + baseSql;
        int rowCountWithOptimize = getRowCount(c, sqlWithOptimize);

        // 执行关闭优化的查询
        String sqlWithoutOptimize = "/*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=false*/ " + baseSql;
        int rowCountWithoutOptimize = getRowCount(c, sqlWithoutOptimize);

        // 对比行数
        assertThat(rowCountWithOptimize).isEqualTo(rowCountWithoutOptimize);

        System.out.println(testName + " row count consistency verified: " + rowCountWithOptimize + " rows");
    }

    private void verifyResultConsistencyForAP(Connection c, String baseSql, String testName) throws SQLException {
        // 执行开启优化的查询
        String sqlWithOptimize =
            "/*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=true enable_mpp=true workload_type=AP parallelism=2*/ " + baseSql;
        int rowCountWithOptimize = getRowCount(c, sqlWithOptimize);

        // 执行关闭优化的查询
        String sqlWithoutOptimize = "/*TDDL: ENABLE_GSI_LOOKUP_OPTIMIZE=false*/ " + baseSql;
        int rowCountWithoutOptimize = getRowCount(c, sqlWithoutOptimize);

        // 对比行数
        assertThat(rowCountWithOptimize).isEqualTo(rowCountWithoutOptimize);

        System.out.println(testName + " AP row count consistency verified: " + rowCountWithOptimize + " rows");
    }

    /**
     * 获取查询结果的行数
     */
    private int getRowCount(Connection c, String sql) throws SQLException {
        int count = 0;
        try (ResultSet rs = c.createStatement().executeQuery(sql)) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
    }

    private void insertTestDataForDataTypes(Connection c) throws SQLException {
        // 检查数据是否已存在
        String checkSql = "select count(*) from t_datatype";
        try (ResultSet rs = c.createStatement().executeQuery(checkSql)) {
            if (rs.next() && rs.getInt(1) >= 2000) {
                System.out.println("Data already exists in t_datatype, skipping insertion");
                return;
            }
        }

        System.out.println("Inserting test data into t_datatype...");
        String sql = "insert into t_datatype(varchar_col, varchar_col2, int_col, bigint_col, datetime_col, text_col) " +
            "values(?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < 2000; i++) {
                ps.setString(1, "test_value_" + (i % 10));
                ps.setString(2, "no_cover_" + i);
                ps.setInt(3, 100 + i % 100);
                ps.setLong(4, 1000000L + i);
                ps.setString(5, "2023-01-0" + (1 + i % 9) + " 00:00:00");
                ps.setString(6, "text_content_" + i);
                ps.addBatch();

                // 每500行执行一次批处理
                if ((i + 1) % 500 == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }

            // 执行剩余的数据
            ps.executeBatch();
        }
        System.out.println("Inserted 2000 rows into t_datatype");
    }

    private void insertTestDataForRangeQueries(Connection c) throws SQLException {
        // 检查数据是否已存在
        String checkSql = "select count(*) from t_range";
        try (ResultSet rs = c.createStatement().executeQuery(checkSql)) {
            if (rs.next() && rs.getInt(1) >= 2000) {
                System.out.println("Data already exists in t_range, skipping insertion");
                return;
            }
        }

        System.out.println("Inserting test data into t_range...");
        String sql = "insert into t_range(student_name, score, grade) values(?, ?, ?)";

        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < 2000; i++) {
                ps.setString(1, "student_" + i);
                ps.setInt(2, 60 + i % 40);
                ps.setString(3, "grade_" + (i % 5 + 1));
                ps.addBatch();

                // 每500行执行一次批处理
                if ((i + 1) % 500 == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }

            // 执行剩余的数据
            ps.executeBatch();
        }
        System.out.println("Inserted 2000 rows into t_range");
    }

    private void createDBAndTbForDataTypeTest() throws SQLException {
        String createDB = "create database if not exists gsi_lookup_datatype_test mode=auto";
        String createTb = "CREATE TABLE `t_datatype` (\n"
            + "\t`id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "\t`varchar_col` varchar(100) DEFAULT NULL,\n"
            + "\t`varchar_col2` varchar(100) DEFAULT NULL,\n"
            + "\t`int_col` int DEFAULT NULL,\n"
            + "\t`bigint_col` bigint DEFAULT NULL,\n"
            + "\t`datetime_col` datetime DEFAULT NULL,\n"
            + "\t`text_col` text,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX `g_i_varchar_col` (`varchar_col`) COVERING (`int_col`, `bigint_col`)\n"
            + "\t\tPARTITION BY KEY(`varchar_col`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tGLOBAL INDEX `g_i_int_col` (`int_col`) COVERING (`varchar_col`, `datetime_col`)\n"
            + "\t\tPARTITION BY KEY(`int_col`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tGLOBAL INDEX `g_i_bigint_col` (`bigint_col`) COVERING (`varchar_col`, `int_col`)\n"
            + "\t\tPARTITION BY KEY(`bigint_col`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tGLOBAL INDEX `g_i_datetime_col` (`datetime_col`) COVERING (`varchar_col`, `int_col`)\n"
            + "\t\tPARTITION BY KEY(`datetime_col`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tKEY `idx_varchar_col` (`varchar_col`),\n"
            + "\tKEY `idx_int_col` (`int_col`),\n"
            + "\tKEY `idx_bigint_col` (`bigint_col`),\n"
            + "\tKEY `idx_datetime_col` (`datetime_col`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb3\n"
            + "PARTITION BY KEY(`id`)\n"
            + "PARTITIONS 16";

        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists gsi_lookup_datatype_test");
            c.createStatement().execute(createDB);
            c.createStatement().execute("use gsi_lookup_datatype_test");
            c.createStatement().execute(createTb);
        }
    }

    private void createDBAndTbForRangeQueryTest() throws SQLException {
        String createDB = "create database if not exists gsi_lookup_range_test mode=auto";
        String createTb = "CREATE TABLE `t_range` (\n"
            + "\t`id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "\t`student_name` varchar(100) DEFAULT NULL,\n"
            + "\t`teacher_name` varchar(100) DEFAULT NULL,\n"
            + "\t`score` int DEFAULT NULL,\n"
            + "\t`grade` varchar(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX `g_i_score` (`score`) COVERING (`student_name`, `grade`)\n"
            + "\t\tPARTITION BY KEY(`score`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tGLOBAL INDEX `g_i_grade` (`grade`) COVERING (`student_name`, `score`)\n"
            + "\t\tPARTITION BY KEY(`grade`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tKEY `idx_score` (`score`),\n"
            + "\tKEY `idx_grade` (`grade`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb3\n"
            + "PARTITION BY KEY(`id`)\n"
            + "PARTITIONS 16";

        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists gsi_lookup_range_test");
            c.createStatement().execute(createDB);
            c.createStatement().execute("use gsi_lookup_range_test");
            c.createStatement().execute(createTb);
        }
    }
}

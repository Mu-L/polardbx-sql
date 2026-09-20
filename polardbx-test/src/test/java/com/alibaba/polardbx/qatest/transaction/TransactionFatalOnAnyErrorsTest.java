package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionFatalOnAnyErrorsTest extends BaseTestCase {
    private final static String DB_NAME = "transaction_fatal_on_any_errors_db";
    private final static String CREATE_DB = "create database if not exists " + DB_NAME + " mode=auto";
    private final static String DROP_DB = "drop database if exists " + DB_NAME;
    private final static String TABLE_NAME = "transaction_fatal_on_any_errors_tb";
    private final static String CREATE_TABLE = "create table if not exists " + TABLE_NAME + " ("
        + "id int primary key auto_increment, "
        + "a int"
        + ") partition by key (id) partitions 4";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, DROP_DB);
            JdbcUtil.executeUpdateSuccess(connection, CREATE_DB);
            JdbcUtil.executeUpdateSuccess(connection, "use " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, DROP_DB);
        }
    }

    @Before
    public void before() throws SQLException {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " where 1=1");
        }
    }

    @Test
    public void testDuplicateKey() throws Exception {
        String error;
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_TRX_FATAL_ON_ANY_ERROR = true");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into " + TABLE_NAME + " values(1, 1)");
            JdbcUtil.executeUpdateFailed(connection, "insert into " + TABLE_NAME + " values(1, 1)",
                "Duplicate entry");

            error = JdbcUtil.executeUpdateFailedReturn(connection, "select 1 from " + TABLE_NAME);
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "show tables");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            JdbcUtil.executeUpdateSuccess(connection, "rollback");
        }
    }

    @Test
    public void testShowTableError() throws Exception {
        String error;
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_TRX_FATAL_ON_ANY_ERROR = true");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into " + TABLE_NAME + " values(1, 1)");

            JdbcUtil.executeUpdateFailed(connection, "show create table " + TABLE_NAME + "x",
                "ERR_TABLE_NOT_EXIST");

            error = JdbcUtil.executeUpdateFailedReturn(connection, "select 1 from " + TABLE_NAME);
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "show tables");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            JdbcUtil.executeUpdateSuccess(connection, "rollback");
        }
    }

    @Test
    public void testParserError() throws Exception {
        String error;
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_TRX_FATAL_ON_ANY_ERROR = true");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into " + TABLE_NAME + " values(1, 1)");

            JdbcUtil.executeUpdateFailed(connection, "show tables " + TABLE_NAME + "x",
                "ERR_PARSER");

            error = JdbcUtil.executeUpdateFailedReturn(connection, "select 1 from " + TABLE_NAME);
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "show tables");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            JdbcUtil.executeUpdateSuccess(connection, "rollback");
        }
    }

    @Test
    public void testProcedureError() throws Exception {
        String sql = "create procedure test_procedure_error1() \n"
            + "begin\n"
            + "insert into " + TABLE_NAME + " values(1, 1);\n"
            + "end";
        String error;
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_TRX_FATAL_ON_ANY_ERROR = true");
            JdbcUtil.executeUpdateSuccess(connection, sql);
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into " + TABLE_NAME + " values(1, 1)");

            JdbcUtil.executeUpdateFailed(connection, "call test_procedure_error1()", "Duplicate entry");

            JdbcUtil.executeUpdateFailed(connection, "call test_procedure_error1()", "ERR_TRANS_FATAL_CANNOT_CONTINUE");

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            JdbcUtil.executeUpdateSuccess(connection, "rollback");
        }
    }

    @Test
    public void testProcedureError2() throws Exception {
        String sql = "create procedure test_procedure_error2() \n"
            + "begin\n"
            + "start transaction;\n"
            + "insert into " + TABLE_NAME + " values(1, 1);\n"
            + "commit;\n"
            + "end";
        String error;
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = false");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_TRX_FATAL_ON_ANY_ERROR = true");
            JdbcUtil.executeUpdateSuccess(connection, sql);
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into " + TABLE_NAME + " values(1, 1)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateFailed(connection, "call test_procedure_error2()", "Duplicate entry");

            JdbcUtil.executeUpdateFailed(connection, "call test_procedure_error2()", "ERR_TRANS_FATAL_CANNOT_CONTINUE");

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            error = JdbcUtil.executeUpdateFailedReturn(connection, "commit");
            System.out.println(error);
            Assert.assertEquals(1, contains(error, "ERR_TRANS_FATAL_CANNOT_CONTINUE"));

            JdbcUtil.executeUpdateSuccess(connection, "rollback");
        }
    }

    private static int contains(String str, String subStr) {
        Pattern pattern = Pattern.compile(Pattern.quote(subStr));
        Matcher matcher = pattern.matcher(str);

        int count = 0;
        while (matcher.find()) {
            count++;
        }

        return count;
    }
}

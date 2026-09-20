package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

@ReplicaIgnore(ignoreReason = "set session variables")
public class UncommonDataTypeTest extends DDLBaseNewDBTestCase {
    @Before
    public void beforeMethod() {
        JdbcUtil.executeSuccess(tddlConnection, "set ENABLE_OMC_30 = true");
        JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set sql_mode = ''");
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    // binary
    @Test
    public void testBinary() {
        String tableName = "omc_30_binary";
        dropTableIfExists(tableName);

        String sql = String.format(
            "create table %s (a binary(20) primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "insert into %s values(0x746573742DF09D9D85, 1),(0x746573742DCF80CD9D, 2), (0x746573742DF09D9CAB7F, 3), (0x746573742DF09D9EB9, 4), (0x746573742DCF80CC81, 5), (0x746573742DF09F9884, 6)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // varbinary
    @Test
    public void testVarbinary() {
        String tableName = "omc_30_varbinary";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varbinary(20) primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values(0x746573742DF09D9D85, 1),(0x746573742DCF80CD9D, 2), (0x746573742DF09D9CAB7F, 3)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // bit
    @Test
    public void testBit() {
        String tableName = "omc_30_bit";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a bit(64) primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values(0b1000000000000000000000000000000000000000000000000000000000000000, 1), (0b1000000000000000000000000000000000000000000000000000000000000001, 2)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // enum
    @Test
    public void testEnum() {
        String tableName = "omc_30_enum";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a enum('a', 'b', 'c') primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values('a', 1),('b', 2), ('c', 3)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // double
    @Test
    public void testDouble() {
        String tableName = "omc_30_double";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a double primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values(1.1, 1), (2.2, 2), (3.3, 3)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // time
    @Test
    public void testTime() {
        String tableName = "omc_30_time";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a time primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values('00:00:10', 1),('10:10:11', 2), ('10:10:12', 3)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // date
    @Test
    public void testDate() {
        String tableName = "omc_30_date";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a date primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values('0000-00-00', 0),('9999-12-31', 1),('0000-00-01', 2), ('1969-09-00', 3), ('2018-00-00', 4), ('2018-12-01', 5)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // year
    @Test
    public void testYear() {
        String tableName = "omc_30_year";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a year primary key, b int) single",
            tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values(9999, 1), (1969, 3), (2018, 4)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // datetime
    @Test
    public void testDatetime() {
        String tableName = "omc_30_datetime";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a datetime primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values('0000-00-00 00:00:00', 0),('9999-12-31 23:59:59', 1),('0000-00-00 01:01:01', 2), ('1969-09-00 23:59:59', 3), ('2018-00-00 00:00:00', 4)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // timestamp
    @Test
    public void testTimestamp() {
        String tableName = "omc_30_timestamp";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a timestamp primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values('0000-00-00 01:01:01', 2), ('2018-08-08 01:01:01', 4), ('2025-11-26 16:25:01', 6)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    // decimal
    @Test
    public void testDecimal() {
        String tableName = "omc_30_decimal";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a decimal(64, 10) primary key, b int) single",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            "insert into %s values(0.000000001, 1), (999999999999999999999.9999999999, 2), (12865867.438528965, 3)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("alter table %s engine=innodb, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}

package com.alibaba.polardbx.qatest.ddl.auto.localAutoInc;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

@CdcIgnore(ignoreReason = "本地自增列会出现重复主键，在预期内故忽略")
public class LocalAutoIncTraceTest extends DDLBaseNewDBTestCase {

    public boolean usingNewPartDb() {
        return true;
    }

    private String getPhySql(List<String> trace){
        String statement = trace.get(11);

        // 移除注释部分来提取SQL
        int sqlStartIndex = statement.indexOf("*/") + 2; // 找到注释关闭符号后的起始位置
        return statement.substring(sqlStartIndex).trim();
    }

    @Test
    public void testLocalAutoIncAsPkTrace() {
        String sql1 = "drop table if exists test_local_inc_as_pk";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "/*+TDDL:cmd_extra(ENABLE_PUSH_DOWN_AUTO_INCREMENT=true)*/CREATE TABLE `test_local_inc_as_pk` (\n"
            + "`a` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "`c` int(11) DEFAULT NULL,\n"
            + "PRIMARY KEY (`a`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`b`)\n"
            + "PARTITIONS 3;";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "trace insert into test_local_inc_as_pk(b,c) values(1,1)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        // column a should be empty
        List<List<String>> trace = getTrace(tddlConnection);
        checkTraceRowCount(1);
        assertEquals(getPhySql(trace.get(0)), "INSERT INTO ? (`b`, `c`) VALUES(?, ?)");
    }

    @Test
    public void testLocalAutoIncNotAsPkTrace() {
        String sql1 = "drop table if exists test_local_inc_not_as_pk";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "/*+TDDL:cmd_extra(ENABLE_PUSH_DOWN_AUTO_INCREMENT=true)*/CREATE TABLE `test_local_inc_not_as_pk` (\n"
            + "\t`a` int(11) NOT NULL,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`c` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (`a`),\n"
            + "  index(c)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`b`)\n"
            + "PARTITIONS 3";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "trace insert into test_local_inc_not_as_pk(a,b) values(1,1)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        List<List<String>> trace = getTrace(tddlConnection);
        checkTraceRowCount(1);
        assertEquals(getPhySql(trace.get(0)), "INSERT INTO ? (`a`, `b`) VALUES(?, ?)");
    }

    @Test
    public void testLocalAutoIncBroadcast() {
        String sql1 = "drop table if exists test_local_inc_as_pk_broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        sql1 = "/*+TDDL:cmd_extra(ENABLE_PUSH_DOWN_AUTO_INCREMENT=true)*/CREATE TABLE `test_local_inc_as_pk_broadcast` (\n"
            + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`c` int(11) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`a`)\n"
            + ") broadcast";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "trace insert into test_local_inc_as_pk_broadcast(b,c) values(1,1)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        List<List<String>> trace = getTrace(tddlConnection);
        checkTraceRowCount(2);
        assertEquals(getPhySql(trace.get(0)), "INSERT INTO ? (`a`, `b`, `c`) VALUES(NULL, ?, ?)");
        assertEquals(getPhySql(trace.get(1)), "INSERT INTO ? (`a`, `b`, `c`) VALUES(NULL, ?, ?)");
    }

    @Test
    public void testLocalAutoIncSingle() {
        String sql1 = "drop table if exists test_local_inc_as_pk_single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        sql1 = "/*+TDDL:cmd_extra(ENABLE_PUSH_DOWN_AUTO_INCREMENT=true)*/CREATE TABLE `test_local_inc_as_pk_single` (\n"
            + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`c` int(11) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`a`)\n"
            + ") single";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "trace insert into test_local_inc_as_pk_single(b,c) values(1,1)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        List<List<String>> trace = getTrace(tddlConnection);
        checkTraceRowCount(1);
        assertEquals(getPhySql(trace.get(0)), "INSERT INTO ? (`a`, `b`, `c`) VALUES(NULL, ?, ?)");
    }
}

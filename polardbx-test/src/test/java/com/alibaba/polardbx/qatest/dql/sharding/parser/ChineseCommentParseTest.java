package com.alibaba.polardbx.qatest.dql.sharding.parser;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class ChineseCommentParseTest extends ReadBaseTestCase {

    @Test
    public void testSelectWithChineseComment() {
        String sql = "/* 查询测试 */ select 1";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void testSelectWithLongChineseComment() {
        String sql = "/* 这是一条来自DMS的中文注释，用于测试多字节字符导致的偏移问题 */ select 1";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void testShowWithChineseComment() {
        String sql = "/* 显示数据库 */ show databases";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void testSetWithChineseComment() {
        String sql = "/* 设置变量 */ set @test_var = 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testMultipleChineseComments() {
        String sql = "/* 第一条注释 */ /* 第二条中文注释 */ select 1";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void testHashCommentWithChinese() {
        String sql = "# 中文注释\nselect 1";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }
}

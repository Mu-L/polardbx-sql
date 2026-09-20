package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class ServerParseByteStringTest {

    @Test
    public void testDropUser() {
        ByteString stmt = ByteString.from("drop user testuser");
        Assert.assertEquals(ServerParse.DROP_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testDropUserUpperCase() {
        ByteString stmt = ByteString.from("DROP USER testuser");
        Assert.assertEquals(ServerParse.DROP_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testDropRole() {
        ByteString stmt = ByteString.from("drop role testrole");
        Assert.assertEquals(ServerParse.DROP_ROLE, ServerParse.parse(stmt));
    }

    @Test
    public void testCreateUser() {
        ByteString stmt = ByteString.from("create user testuser");
        Assert.assertEquals(ServerParse.CREATE_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testCreateUserUpperCase() {
        ByteString stmt = ByteString.from("CREATE USER testuser");
        Assert.assertEquals(ServerParse.CREATE_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testCreateRole() {
        ByteString stmt = ByteString.from("create role testrole");
        Assert.assertEquals(ServerParse.CREATE_ROLE, ServerParse.parse(stmt));
    }

    @Test
    public void testDebugProcedureDebug() {
        ByteString stmt = ByteString.from("debug procedure debug myproc");
        Assert.assertEquals(ServerParse.DEBUG_PROCEDURE_DEBUG, ServerParse.parse(stmt));
    }

    @Test
    public void testDebugProcedureNext() {
        ByteString stmt = ByteString.from("debug procedure next myproc");
        Assert.assertEquals(ServerParse.DEBUG_PROCEDURE_NEXT, ServerParse.parse(stmt));
    }

    @Test
    public void testDropOther() {
        ByteString stmt = ByteString.from("drop something");
        Assert.assertEquals(ServerParse.OTHER, ServerParse.parse(stmt));
    }

    @Test
    public void testCreateOther() {
        ByteString stmt = ByteString.from("create something");
        Assert.assertEquals(ServerParse.OTHER, ServerParse.parse(stmt));
    }

    // --- Chinese comment prefix tests (aone #84018365) ---
    // DMS sends SQL with Chinese comments like "/* 中文注释 */ SQL".
    // Chinese chars are multi-byte in UTF-8, so byte offset != char offset.

    @Test
    public void testDropUserWithChineseComment() {
        ByteString stmt = ByteString.from("/* 删除用户 */ drop user testuser");
        Assert.assertEquals(ServerParse.DROP_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testDropRoleWithChineseComment() {
        ByteString stmt = ByteString.from("/* 删除角色 */ drop role testrole");
        Assert.assertEquals(ServerParse.DROP_ROLE, ServerParse.parse(stmt));
    }

    @Test
    public void testCreateUserWithChineseComment() {
        ByteString stmt = ByteString.from("/* 创建用户 */ create user testuser");
        Assert.assertEquals(ServerParse.CREATE_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testCreateRoleWithChineseComment() {
        ByteString stmt = ByteString.from("/* 创建角色 */ create role testrole");
        Assert.assertEquals(ServerParse.CREATE_ROLE, ServerParse.parse(stmt));
    }

    @Test
    public void testSetPasswordWithChineseComment() {
        ByteString stmt = ByteString.from("/* 修改密码 */ set password for testuser = password('123')");
        Assert.assertEquals(ServerParse.SET_PASSWORD, ServerParse.parse(stmt));
    }

    @Test
    public void testSetDefaultRoleWithChineseComment() {
        ByteString stmt = ByteString.from("/* 设置默认角色 */ set default role admin to testuser");
        Assert.assertEquals(ServerParse.OTHER, ServerParse.parse(stmt));
    }

    @Test
    public void testDebugProcedureWithChineseComment() {
        ByteString stmt = ByteString.from("/* 调试存储过程 */ debug procedure debug myproc");
        Assert.assertEquals(ServerParse.DEBUG_PROCEDURE_DEBUG, ServerParse.parse(stmt));
    }

    @Test
    public void testAoneFailingCase_longChineseComment() {
        // Original aone #84018365: long Chinese comment causes StringIndexOutOfBoundsException
        // because toString().substring(byteOffset) exceeds the character length
        ByteString stmt = ByteString.from(
            "/* 这是一条来自DMS的中文注释，用于测试多字节字符导致的偏移问题 */ drop user test_dms_user");
        Assert.assertEquals(ServerParse.DROP_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testChineseCommentWithHashPrefix() {
        ByteString stmt = ByteString.from("# 中文注释\ndrop user testuser");
        Assert.assertEquals(ServerParse.DROP_USER, ServerParse.parse(stmt));
    }

    @Test
    public void testMultipleChineseComments() {
        ByteString stmt = ByteString.from("/* 第一条注释 */ /* 第二条中文注释 */ create user testuser");
        Assert.assertEquals(ServerParse.CREATE_USER, ServerParse.parse(stmt));
    }
}

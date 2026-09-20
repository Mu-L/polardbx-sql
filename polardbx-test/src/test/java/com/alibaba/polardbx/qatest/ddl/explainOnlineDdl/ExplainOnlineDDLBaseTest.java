package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExplainOnlineDDLBaseTest extends DDLBaseNewDBTestCase {
    public enum DdlType {
        ONLINE_DDL,
        LOCK_TABLE,
        NONE
    }

    public enum DdlAlgorithm {
        INSTANT,
        INPLACE,
        META_ONLY,
        OMC20,
        OMC30,
        OSC,
        COPY,
        DEFAULT
    }

    protected static void assertOnlineDdlResult(Connection connection, String sql, DdlType expectDdlType,
                                                DdlAlgorithm expectAlgorithm) {
        DdlType ddlType = null;
        DdlAlgorithm algorithm = null;
        try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            if (rs.next()) {
                ddlType = DdlType.valueOf(rs.getString(1));
                algorithm = DdlAlgorithm.valueOf(rs.getString(2));
            }
        } catch (SQLException e) {
            Assert.fail();
        }
        Assert.assertSame(expectDdlType, ddlType);
        Assert.assertSame(expectAlgorithm, algorithm);
    }

    protected static void assertExplainAdvisorResult(Connection connection, String sql, DdlType expectDdlType,
                                                     String expectAdviseSql, DdlAlgorithm expectAlgorithm) {
        DdlType ddlType = null;
        DdlAlgorithm algorithm = null;
        String adviseSql = null;
        try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            if (rs.next()) {
                ddlType = DdlType.valueOf(rs.getString(1));
                adviseSql = rs.getString(2);
                algorithm = DdlAlgorithm.valueOf(rs.getString(3));
            }
        } catch (SQLException e) {
            Assert.fail();
        }
        // 检查结果
        Assert.assertSame(expectDdlType, ddlType);
        Assert.assertNotNull(adviseSql);
        Assert.assertEquals(expectAdviseSql.toLowerCase(), adviseSql.toLowerCase());
        Assert.assertSame(expectAlgorithm, algorithm);
        // 执行推荐的 DDL
        if (!expectAdviseSql.isEmpty()) {
            JdbcUtil.executeUpdateSuccess(connection, expectAdviseSql);
        }
    }

    protected static void assertExplainAdvisorResult(Connection connection, String sql, DdlType expectDdlType,
                                                     String expectAdviseSql, DdlAlgorithm expectAlgorithm,
                                                     String hint) {
        DdlType ddlType = null;
        DdlAlgorithm algorithm = null;
        String adviseSql = null;
        try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            if (rs.next()) {
                ddlType = DdlType.valueOf(rs.getString(1));
                adviseSql = rs.getString(2);
                algorithm = DdlAlgorithm.valueOf(rs.getString(3));
            }
        } catch (SQLException e) {
            Assert.fail();
        }
        // 检查结果
        Assert.assertSame(expectDdlType, ddlType);
        Assert.assertNotNull(adviseSql);
        Assert.assertEquals(expectAdviseSql.toLowerCase(), adviseSql.toLowerCase());
        Assert.assertSame(expectAlgorithm, algorithm);
        // 执行推荐的 DDL
        if (!expectAdviseSql.isEmpty()) {
            JdbcUtil.executeUpdateSuccess(connection, hint + expectAdviseSql);
        }
    }

    protected static void setSupportInstant(Connection tddlConnection) {
        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set global innodb_support_instant_add_column = true");
        }
    }
}

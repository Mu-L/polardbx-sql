package com.alibaba.polardbx.gms.privilege;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.gms.sqlaudit.SqlAuditInterceptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * 测试SqlAuditInterceptor类
 */
public class SqlAuditTest {
    final private Logger logger = LoggerFactory.getLogger(SqlAuditTest.class);

    @Before
    public void setUp() {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_SQL_AUDIT, "true");
    }

    @Test
    public void testUpdateAuditLogConfig() {

        final String CONFIG_JSON = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"user_1\"],\n" +
            "    \"schema_name\": [\"schema_1\", \"schema_2\"],\n" +
            "    \"operation\":[\"select\", \"create\", \"set_statement\", \"login_success\", \"logout\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": [\"schema_3\"],\n" +
            "    \"operation\": [\"drop\", \"insert\", \"create_user\", \"login_failed\", \"grant\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"user_3\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";
        // 假设SqlAuditAction.value和SqlAuditAction.convert方法已经正确实现
        // 直接调用updateAuditLogConfig
        SqlAuditInterceptor.updateAuditLogConfig(CONFIG_JSON);

        // 验证hasLoginSuccessPerm
        assertTrue(SqlAuditInterceptor.hasLoginSuccessPerm("user_1"));
        assertFalse(SqlAuditInterceptor.hasLoginSuccessPerm("user_2"));
        assertFalse(SqlAuditInterceptor.hasLoginSuccessPerm("user_3"));

        // 验证hasLoginFailedPerm
        assertFalse(SqlAuditInterceptor.hasLoginFailedPerm("user_1"));
        assertTrue(SqlAuditInterceptor.hasLoginFailedPerm("user_2"));
        assertFalse(SqlAuditInterceptor.hasLoginFailedPerm("user_3"));

        // 验证hasLogoutPerm
        assertTrue(SqlAuditInterceptor.hasLogoutPerm("user_1"));
        assertFalse(SqlAuditInterceptor.hasLogoutPerm("user_2"));
        assertFalse(SqlAuditInterceptor.hasLogoutPerm("user_3"));

        // 验证hasPermission
        // user_1(schema_1)允许select, create, set_statement
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.SET_STATEMENT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.DROP));

        // user_1(schema_2)允许select, create, set_statement
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.SET_STATEMENT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.DROP));

        // user_2(schema_3)允许drop, insert, create_user, grant
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.INSERT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.CREATE_USER));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.GRANT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.SELECT));

        // user_3(schema_4)允许所有操作
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.GRANT));
    }

    @Test
    public void testHasPermissionWithEmptyConfig() {
        // 在未设置配置的情况下，hasPermission应返回true
        SqlAuditInterceptor.updateAuditLogConfig("");
        assertTrue(SqlAuditInterceptor.hasPermission("any_user", "any_schema", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("any_user", "any_schema", SqlType.DROP));
    }

    @Test
    public void testHasPermissionWithWhiteAudit() {
        // 配置包含操作"*"，应允许所有操作
        String whiteConfig = "{\n" +
            "  \"audit_rule\": {\n" +
            "    \"user\":[\"white_user\"],\n" +
            "    \"schema_name\": [\"white_schema\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";

        SqlAuditInterceptor.updateAuditLogConfig(whiteConfig);

        // white_user在white_schema中应允许所有操作
        assertTrue(SqlAuditInterceptor.hasPermission("white_user", "white_schema", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("white_user", "white_schema", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("white_user", "white_schema", SqlType.CREATE));
    }

    @Test
    public void testWildcard() {
        final String CONFIG_JSON = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"*\"],\n" +
            "    \"schema_name\": [\"schema_1\", \"schema_2\"],\n" +
            "    \"operation\":[\"select\", \"create\", \"set_statement\", \"login_success\", \"logout\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": [\"*\"],\n" +
            "    \"operation\": [\"drop\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"*\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";
        SqlAuditInterceptor.updateAuditLogConfig(CONFIG_JSON);

        // 验证hasPermission
        // schema_1允许所有user的select, create, set_statement
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_1", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_1", SqlType.SET_STATEMENT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_4", "schema_1", SqlType.DROP));

        // user2对所有schema都有权限
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_1", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_2", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_4", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_5", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_6", SqlType.DROP));

        // schema_4允许所有user的所有操作
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_4", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_4", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.GRANT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.SET_STATEMENT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_4", "schema_4", SqlType.DROP));
    }

    @Test
    public void testAllWildcard() {
        final String CONFIG_JSON = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"*\"],\n" +
            "    \"schema_name\": [\"*\"],\n" +
            "    \"operation\":[\"*\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": [\"*\"],\n" +
            "    \"operation\": [\"drop\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"*\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";
        SqlAuditInterceptor.updateAuditLogConfig(CONFIG_JSON);

        // 验证hasPermission
        // schema_1允许所有user的select, create, set_statement
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_1", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_1", SqlType.SET_STATEMENT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_4", "schema_1", SqlType.DROP));

        // user2对所有schema都有权限
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_1", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_2", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_4", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_5", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_6", SqlType.DROP));

        // schema_4允许所有user的所有操作
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_4", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_4", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.GRANT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.SET_STATEMENT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_4", "schema_4", SqlType.DROP));
    }

    @Test
    public void testUserWhiteList() {
        final String CONFIG_JSON = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"user_1\"],\n" +
            "    \"schema_name\": [\"schema_1\", \"schema_2\"],\n" +
            "    \"operation\":[\"select\", \"create\", \"set_statement\", \"login_success\", \"logout\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": [\"schema_3\"],\n" +
            "    \"operation\": [\"drop\", \"insert\", \"create_user\", \"login_failed\", \"grant\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"user_3\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";
        // 假设SqlAuditAction.value和SqlAuditAction.convert方法已经正确实现
        // 直接调用updateAuditLogConfig
        SqlAuditInterceptor.updateAuditLogConfig(CONFIG_JSON);

        // 验证hasLoginSuccessPerm
        assertTrue(SqlAuditInterceptor.hasLoginSuccessPerm("user_1"));
        assertFalse(SqlAuditInterceptor.hasLoginSuccessPerm("user_2"));
        assertFalse(SqlAuditInterceptor.hasLoginSuccessPerm("user_3"));

        // 验证hasLoginFailedPerm
        assertFalse(SqlAuditInterceptor.hasLoginFailedPerm("user_1"));
        assertTrue(SqlAuditInterceptor.hasLoginFailedPerm("user_2"));
        assertFalse(SqlAuditInterceptor.hasLoginFailedPerm("user_3"));

        // 验证hasLogoutPerm
        assertTrue(SqlAuditInterceptor.hasLogoutPerm("user_1"));
        assertFalse(SqlAuditInterceptor.hasLogoutPerm("user_2"));
        assertFalse(SqlAuditInterceptor.hasLogoutPerm("user_3"));

        // 验证hasPermission
        // user_1(schema_1)允许select, create, set_statement
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.SET_STATEMENT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_1", "schema_1", SqlType.DROP));

        // user_1(schema_2)允许select, create, set_statement
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.SET_STATEMENT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_1", "schema_2", SqlType.DROP));

        // user_2(schema_3)允许drop, insert, create_user, grant
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.INSERT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.CREATE_USER));
        assertTrue(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.GRANT));
        assertFalse(SqlAuditInterceptor.hasPermission("user_2", "schema_3", SqlType.SELECT));

        // user_3(schema_4)允许所有操作
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.SELECT));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.DROP));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.CREATE));
        assertTrue(SqlAuditInterceptor.hasPermission("user_3", "schema_4", SqlType.GRANT));
    }

    @Test
    public void testInvalidConfig() {
        // 提供无效的JSON
        String invalidConfig = "{ invalid json }";

        try {
            // 调用updateAuditLogConfig，不应抛出异常
            SqlAuditInterceptor.updateAuditLogConfig(invalidConfig);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("expect ':' at 0, actual j"));
        }
    }

    @Test
    public void testSetConfig() {
        final String CONFIG_JSON = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"user_1\"],\n" +
            "    \"schema_name\": [\"schema_1\", \"schema_2\"],\n" +
            "    \"operation\":[\"select\", \"create\", \"set_statement\", \"login_success\", \"logout\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"operation\": [\"drop\", \"insert\", \"create_user\", \"login_failed\", \"grant\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"user_3\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";
        try {
            SqlAuditInterceptor.validateJsonValue(CONFIG_JSON);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("missing necessary field"));
        }

        final String CONFIG_JSON2 = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"user_1\"],\n" +
            "    \"schema_name\": [\"schema_1\", \"schema_2\"],\n" +
            "    \"operation\":[\"select\", \"create\", \"set_statement\", \"login_success\", \"logout\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": \"'test'\",\n" +
            "    \"operation\": [\"drop\", \"insert\", \"create_user\", \"login_failed\", \"grant\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"user_3\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";
        try {
            SqlAuditInterceptor.validateJsonValue(CONFIG_JSON2);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("must be JSONArray"));
        }

        final String CONFIG_JSON3 = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"user_1\"],\n" +
            "    \"schema_name\": [\"schema_1\", \"schema_2\"],\n" +
            "    \"operation\":[\"select\", \"create\", \"set_statement\", \"login_success\", \"logout\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": [\"schema_3\"],\n" +
            "    \"operation\": [\"drop\", \"insert\", \"create_user\", \"login_failed\", \"grant\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"user_3\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";

        try {
            SqlAuditInterceptor.validateJsonValue(CONFIG_JSON3);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("must be not empty"));
        }

        final String CONFIG_JSON4 = "{\n" +
            "  \"audit_rule1\": {\n" +
            "    \"user\":[\"*\"],\n" +
            "    \"schema_name\": [\"*\"],\n" +
            "    \"operation\":[\"*\"]\n" +
            "  },\n" +
            "  \"audit_rule2\": {\n" +
            "    \"user\":[\"user_2\"],\n" +
            "    \"schema_name\": [\"*\"],\n" +
            "    \"operation\": [\"drop2\"]\n" +
            "  },\n" +
            "  \"audit_rule3\": {\n" +
            "    \"user\":[\"*\"],\n" +
            "    \"schema_name\": [\"schema_4\"],\n" +
            "    \"operation\": [\"*\"]\n" +
            "  }\n" +
            "}";

        try {
            SqlAuditInterceptor.validateJsonValue(CONFIG_JSON4);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid operation"));
        }
    }

    @Test
    public void testDefaultLoginConfig() {
        try (MockedStatic<DynamicConfig> dynamicConfigMockedStatic = Mockito.mockStatic(DynamicConfig.class)) {
            DynamicConfig dynamicConfig = Mockito.mock(DynamicConfig.class);
            dynamicConfigMockedStatic.when(() -> DynamicConfig.getInstance()).thenReturn(dynamicConfig);
            Mockito.when(dynamicConfig.getEnableSqlAudit()).thenReturn(false);

            Assert.assertTrue(SqlAuditInterceptor.hasLoginSuccessPerm("test"));
            Assert.assertTrue(SqlAuditInterceptor.hasLoginFailedPerm("test"));
            Assert.assertTrue(SqlAuditInterceptor.hasLogoutPerm("test"));
        }

    }
}

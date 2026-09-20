package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDescribeExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRefreshExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowConnectorsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowExternalCatalogsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSecretsStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import org.junit.Test;

import static org.junit.Assert.*;

public class MySqlExternalCatalogParserTest extends MysqlTest {

    @Test
    public void testCreateExternalCatalog() {
        String sql =
            "CREATE EXTERNAL CATALOG IF NOT EXISTS hive_wh WITH ('connector'='hive', 'endpoint'='thrift://hms:9083')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateExternalCatalogStatement);
        MySqlCreateExternalCatalogStatement create = (MySqlCreateExternalCatalogStatement) stmt;
        assertTrue(create.isIfNotExists());
        assertEquals("hive_wh", create.getName().getSimpleName());
        assertFalse(create.getProperties().isEmpty());
    }

    @Test
    public void testDropExternalCatalog() {
        String sql = "DROP EXTERNAL CATALOG IF EXISTS hive_wh";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof SQLDropCatalogStatement);
        SQLDropCatalogStatement drop = (SQLDropCatalogStatement) stmt;
        assertTrue(drop.isIfExists());
        assertTrue(drop.isExternal());
    }

    @Test
    public void testDropExternalCatalogNoIfExists() {
        String sql = "DROP EXTERNAL CATALOG my_cat";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof SQLDropCatalogStatement);
        SQLDropCatalogStatement drop = (SQLDropCatalogStatement) stmt;
        assertFalse(drop.isIfExists());
    }

    @Test
    public void testShowExternalCatalogs() {
        String sql = "SHOW EXTERNAL CATALOGS";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlShowExternalCatalogsStatement);
    }

    @Test
    public void testShowSecrets() {
        String sql = "SHOW SECRETS";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlShowSecretsStatement);
    }

    @Test
    public void testShowSecretsLike() {
        String sql = "SHOW SECRETS LIKE 'oss%'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlShowSecretsStatement);
        assertNotNull(((MySqlShowSecretsStatement) stmt).getLike());
    }

    @Test
    public void testShowConnectorsLike() {
        String sql = "SHOW CONNECTORS LIKE 'hive%'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlShowConnectorsStatement);
        assertNotNull(((MySqlShowConnectorsStatement) stmt).getLike());
        assertFalse(((MySqlShowConnectorsStatement) stmt).isFull());
    }

    @Test
    public void testShowFullConnectorsLike() {
        String sql = "SHOW FULL CONNECTORS LIKE 'hive%'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlShowConnectorsStatement);
        assertNotNull(((MySqlShowConnectorsStatement) stmt).getLike());
        assertTrue(((MySqlShowConnectorsStatement) stmt).isFull());
    }

    @Test
    public void testShowExternalCatalogsLike() {
        String sql = "SHOW EXTERNAL CATALOGS LIKE 'hive%'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlShowExternalCatalogsStatement);
        assertNotNull(((MySqlShowExternalCatalogsStatement) stmt).getLike());
    }

    @Test
    public void testShowExternalNonCatalogsFallsThrough() {
        String sql = "SHOW EXTERNAL FOO";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            SQLStatement stmt = parser.parseStatement();
            assertFalse(stmt instanceof MySqlShowExternalCatalogsStatement);
        } catch (ParserException e) {
            assertFalse(String.valueOf(e.getMessage()).contains("Expected CATALOGS"));
        }
    }

    @Test
    public void testCreateSecret() {
        String sql =
            "CREATE SECRET IF NOT EXISTS oss_prod WITH ('type'='oss', 'access_key'='AK123', 'secret_key'='SK456')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateSecretStatement);
        MySqlCreateSecretStatement create = (MySqlCreateSecretStatement) stmt;
        assertTrue(create.isIfNotExists());
        assertEquals("oss_prod", create.getName().getSimpleName());
        assertEquals("oss", create.getProperties().get("type"));
        assertEquals("AK123", create.getProperties().get("access_key"));
    }

    @Test
    public void testCreateSecretNormalizesPropertyKeys() {
        String sql = "CREATE SECRET mixed_case WITH ('TYPE'='mock', 'User'='u', 'PASSWORD'='p')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateSecretStatement);
        MySqlCreateSecretStatement create = (MySqlCreateSecretStatement) stmt;
        assertEquals("mock", create.getProperties().get("type"));
        assertEquals("u", create.getProperties().get("user"));
        assertEquals("p", create.getProperties().get("password"));
        assertFalse(create.getProperties().containsKey("TYPE"));
        assertFalse(create.getProperties().containsKey("User"));
        assertFalse(create.getProperties().containsKey("PASSWORD"));
    }

    @Test
    public void testCreateSecretBacktickValueNormalized() {
        String sql = "CREATE SECRET s WITH (`type`=`mock`, `access_key`=`AK`)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateSecretStatement);
        MySqlCreateSecretStatement create = (MySqlCreateSecretStatement) stmt;
        assertEquals("backtick value `mock` should normalize to bare 'mock'",
            "mock", create.getProperties().get("type"));
        assertEquals("AK", create.getProperties().get("access_key"));
        assertFalse("backtick wrapper must not leak into value",
            create.getProperties().containsValue("`mock`"));
    }

    @Test
    public void testAlterSecretNormalizesPropertyKeys() {
        String sql = "ALTER SECRET mixed_case SET ('TYPE'='mock', 'User'='u', 'PASSWORD'='p')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterSecretStatement);
        MySqlAlterSecretStatement alter = (MySqlAlterSecretStatement) stmt;
        assertEquals("mock", alter.getSetProperties().get("type"));
        assertEquals("u", alter.getSetProperties().get("user"));
        assertEquals("p", alter.getSetProperties().get("password"));
        assertFalse(alter.getSetProperties().containsKey("TYPE"));
        assertFalse(alter.getSetProperties().containsKey("User"));
        assertFalse(alter.getSetProperties().containsKey("PASSWORD"));
    }

    @Test
    public void testCreateSecretOrReplaceNotSupported() {
        String sql = "CREATE OR REPLACE SECRET s WITH ('password' = 'pwd')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("CREATE OR REPLACE SECRET should not be supported");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testDropSecret() {
        String sql = "DROP SECRET IF EXISTS oss_prod";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlDropSecretStatement);
        MySqlDropSecretStatement drop = (MySqlDropSecretStatement) stmt;
        assertTrue(drop.isIfExists());
        assertEquals("oss_prod", drop.getName().getSimpleName());
    }

    @Test
    public void testRefreshExternalCatalog() {
        String sql = "REFRESH EXTERNAL CATALOG hive_wh";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlRefreshExternalCatalogStatement);
        MySqlRefreshExternalCatalogStatement refresh = (MySqlRefreshExternalCatalogStatement) stmt;
        assertFalse(refresh.isTable());
        assertEquals("hive_wh", refresh.getCatalogName().getSimpleName());
    }

    @Test
    public void testRefreshExternalTable() {
        String sql = "REFRESH EXTERNAL TABLE cat1.db1.t1";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlRefreshExternalCatalogStatement);
        MySqlRefreshExternalCatalogStatement refresh = (MySqlRefreshExternalCatalogStatement) stmt;
        assertTrue(refresh.isTable());
        assertEquals("cat1", refresh.getCatalogName().getSimpleName());
        assertEquals("db1", refresh.getDbName().getSimpleName());
        assertEquals("t1", refresh.getTableName().getSimpleName());
    }

    @Test
    public void testRefreshExternalRequiresSubcommand() {
        String sql = "REFRESH EXTERNAL";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("REFRESH EXTERNAL should require CATALOG or TABLE");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testAlterExternalCatalogSet() {
        String sql = "ALTER EXTERNAL CATALOG hive_wh SET ('endpoint'='new_endpoint')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertEquals("new_endpoint", alter.getSetProperties().get("endpoint"));
    }

    @Test
    public void testAlterExternalCatalogSetAndComment() {
        String sql = "ALTER EXTERNAL CATALOG hive_wh SET ('k1'='v1') COMMENT 'hello'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertEquals("v1", alter.getSetProperties().get("k1"));
        assertEquals("hello", alter.getComment());
    }

    @Test
    public void testAlterExternalCatalogCommentOnly() {
        String sql = "ALTER EXTERNAL CATALOG hive_wh COMMENT 'just comment'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertTrue(alter.getSetProperties().isEmpty());
        assertEquals("just comment", alter.getComment());
    }

    @Test
    public void testAlterExternalCatalogBacktickCommentNormalized() {
        String sql = "ALTER EXTERNAL CATALOG c COMMENT `my comment`";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertEquals("Backtick-wrapped comment should be normalized to bare text",
            "my comment", alter.getComment());
    }

    @Test
    public void testAlterExternalCatalogCommentWithEmbeddedQuotesPreserved() {
        // COMMENT '''hello''' → string literal value is 'hello' (with actual single quotes)
        // normalizeNoTrim should NOT strip these inner quotes
        String sql = "ALTER EXTERNAL CATALOG c COMMENT '''hello'''";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertEquals("Comment with embedded single quotes should be preserved",
            "'hello'", alter.getComment());
    }

    @Test
    public void testCreateExternalCatalogCommentWithEmbeddedQuotesPreserved() {
        String sql = "CREATE EXTERNAL CATALOG c COMMENT '''hello''' WITH ('connector'='hive')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateExternalCatalogStatement);
        MySqlCreateExternalCatalogStatement create = (MySqlCreateExternalCatalogStatement) stmt;
        assertEquals("Comment with embedded single quotes should be preserved",
            "'hello'", create.getComment());
    }

    @Test
    public void testAlterExternalCatalogResetNoLongerSupported() {
        String sql = "ALTER EXTERNAL CATALOG hive_wh RESET ('endpoint')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("RESET should no longer be supported");
        } catch (Exception e) {
            // expected - RESET removed
        }
    }

    @Test
    public void testDescribeExternalAsTableNameColumnName() {
        // DESC <table "external"> <column "catalog">: a previously-valid plain DESCRIBE
        // that was mis-routed to DESCRIBE EXTERNAL CATALOG and threw "illegal name" at EOF.
        MySqlStatementParser parser = new MySqlStatementParser("DESC external catalog");
        SQLStatement stmt = parser.parseStatement();
        assertNotNull(stmt);
    }

    @Test
    public void testDescribeExternalCatalogStillParses() {
        // DESC EXTERNAL CATALOG <name> must still route correctly after the guard.
        MySqlStatementParser parser = new MySqlStatementParser("DESC external catalog hive_cat");
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlDescribeExternalCatalogStatement);
        assertEquals("hive_cat",
            ((MySqlDescribeExternalCatalogStatement) stmt).getName().getSimpleName());
    }

    @Test
    public void testCreateExternalCatalogSecretInWith() {
        String sql = "CREATE EXTERNAL CATALOG my_cat WITH ('connector'='jdbc', 'secret'='my_s', 'url'='x')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateExternalCatalogStatement);
        MySqlCreateExternalCatalogStatement create = (MySqlCreateExternalCatalogStatement) stmt;
        assertTrue(create.getProperties().entrySet().stream()
            .anyMatch(e -> e.getKey().contains("secret")
                && e.getValue().contains("my_s")));
    }

    @Test
    public void testCreateExternalCatalogNormalizesPropertyKeys() {
        String sql = "CREATE EXTERNAL CATALOG c1 WITH ('Connector'='jdbc', 'SECRET'='s1', 'URL'='x')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateExternalCatalogStatement);
        MySqlCreateExternalCatalogStatement create = (MySqlCreateExternalCatalogStatement) stmt;
        assertTrue(create.getProperties().containsKey("connector"));
        assertTrue(create.getProperties().containsKey("secret"));
        assertTrue(create.getProperties().containsKey("url"));
        assertFalse(create.getProperties().containsKey("Connector"));
        assertFalse(create.getProperties().containsKey("SECRET"));
        assertFalse(create.getProperties().containsKey("URL"));
    }

    @Test
    public void testAlterExternalCatalogNormalizesPropertyKeys() {
        String sql = "ALTER EXTERNAL CATALOG c1 SET ('Endpoint'='ep1', 'PORT'=3306)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertEquals("ep1", alter.getSetProperties().get("endpoint"));
        assertEquals("3306", alter.getSetProperties().get("port"));
        assertFalse(alter.getSetProperties().containsKey("Endpoint"));
        assertFalse(alter.getSetProperties().containsKey("PORT"));
    }

    @Test
    public void testRefreshExternalTableRejectsStringLiteralNames() {
        String sql = "REFRESH EXTERNAL TABLE 'cat1'.'db1'.'tbl1'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("string literals are not valid identifiers in a three-part name");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testRefreshExternalTableWithBacktickNames() {
        String sql = "REFRESH EXTERNAL TABLE `my-cat`.`my-db`.`my-tbl`";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlRefreshExternalCatalogStatement);
        MySqlRefreshExternalCatalogStatement refresh = (MySqlRefreshExternalCatalogStatement) stmt;
        assertTrue(refresh.isTable());
        assertEquals("`my-cat`", refresh.getCatalogName().getSimpleName());
        assertEquals("`my-db`", refresh.getDbName().getSimpleName());
        assertEquals("`my-tbl`", refresh.getTableName().getSimpleName());
        assertEquals(sql, stmt.toString());
    }

    @Test
    public void testRefreshExternalTableWithKeywordNames() {
        String sql = "REFRESH EXTERNAL TABLE `order`.`select`.`key`";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlRefreshExternalCatalogStatement);
        MySqlRefreshExternalCatalogStatement refresh = (MySqlRefreshExternalCatalogStatement) stmt;
        assertTrue(refresh.isTable());
        assertEquals("`order`", refresh.getCatalogName().getSimpleName());
        assertEquals("`select`", refresh.getDbName().getSimpleName());
        assertEquals("`key`", refresh.getTableName().getSimpleName());
        assertEquals(sql, stmt.toString());
    }

    @Test
    public void testRefreshExternalTableRejectsIncompleteName() {
        String sql = "REFRESH EXTERNAL TABLE db1.tbl1";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("REFRESH EXTERNAL TABLE should require catalog.database.table");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testSoftKeywordNoConflict() {
        String sql = "SELECT catalog, secret FROM t";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertNotNull(stmt);
    }

    @Test
    public void testCreateSecretNumericValue() {
        String sql = "CREATE SECRET s1 WITH ('password'=123, 'timeout'=1.5)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateSecretStatement);
        MySqlCreateSecretStatement create = (MySqlCreateSecretStatement) stmt;
        assertEquals("123", create.getProperties().get("password"));
        assertEquals("1.5", create.getProperties().get("timeout"));
    }

    @Test
    public void testCreateSecretBooleanAndIdentifierValue() {
        String sql = "CREATE SECRET s1 WITH ('cache'=true, 'level'=fast)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateSecretStatement);
        MySqlCreateSecretStatement create = (MySqlCreateSecretStatement) stmt;
        assertEquals("true", create.getProperties().get("cache"));
        assertEquals("fast", create.getProperties().get("level"));
    }

    @Test
    public void testCreateSecretRejectsMissingComma() {
        String sql = "CREATE SECRET s1 WITH ('a'='1' 'b'='2')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("missing comma between properties should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testCreateSecretRejectsNonScalarValue() {
        String sql = "CREATE SECRET s1 WITH ('k'=(1))";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("non-scalar property value should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testCreateSecretRejectsNumericKey() {
        String sql = "CREATE SECRET s1 WITH (123='v')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("numeric property key should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testAlterSecretNumericValue() {
        String sql = "ALTER SECRET s1 SET ('password'=456)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterSecretStatement);
        MySqlAlterSecretStatement alter = (MySqlAlterSecretStatement) stmt;
        assertEquals("456", alter.getSetProperties().get("password"));
    }

    @Test
    public void testAlterSecretRejectsMissingComma() {
        String sql = "ALTER SECRET s1 SET ('a'='1' 'b'='2')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("missing comma between properties should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testAlterExternalCatalogNumericValue() {
        String sql = "ALTER EXTERNAL CATALOG hive_wh SET ('port'=3306)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) stmt;
        assertEquals("3306", alter.getSetProperties().get("port"));
    }

    @Test
    public void testAlterExternalCatalogRejectsMissingComma() {
        String sql = "ALTER EXTERNAL CATALOG hive_wh SET ('a'='1' 'b'='2')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("missing comma between properties should be rejected");
        } catch (ParserException e) {
            // expected
        }
    }

    /**
     * The legacy PROPERTIES form is whitespace-separated, so CREATE EXTERNAL CATALOG
     * must keep accepting entries without commas.
     */
    @Test
    public void testCreateExternalCatalogAcceptsWhitespaceSeparatedProperties() {
        String sql = "CREATE EXTERNAL CATALOG c1 PROPERTIES ('a'='1' 'b'='2')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateExternalCatalogStatement);
        MySqlCreateExternalCatalogStatement create = (MySqlCreateExternalCatalogStatement) stmt;
        assertEquals(2, create.getProperties().size());
    }

    @Test
    public void testCreateExternalCatalogWithRejectsMissingComma() {
        String sql = "CREATE EXTERNAL CATALOG c1 WITH ('a'='1' 'b'='2')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("WITH clause should require a comma between properties");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testCreateExternalCatalogWithAcceptsCommaSeparated() {
        String sql = "CREATE EXTERNAL CATALOG c1 WITH ('a'='1', 'b'='2')";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        assertTrue(stmt instanceof MySqlCreateExternalCatalogStatement);
        MySqlCreateExternalCatalogStatement create = (MySqlCreateExternalCatalogStatement) stmt;
        assertEquals(2, create.getProperties().size());
    }

    @Test
    public void testCreateExternalCatalogRejectsMissingWith() {
        String sql = "CREATE EXTERNAL CATALOG c1";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("WITH clause is mandatory");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testCreateExternalCatalogRejectsCommentOnlyWithoutWith() {
        String sql = "CREATE EXTERNAL CATALOG c1 COMMENT 'x'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatement();
            fail("WITH clause is mandatory even when COMMENT is present");
        } catch (ParserException e) {
            // expected
        }
    }

    @Test
    public void testCreateExternalCatalogEmptyPropertiesOmitsWithOnOutput() {
        MySqlCreateExternalCatalogStatement stmt = new MySqlCreateExternalCatalogStatement();
        stmt.setName(new SQLIdentifierExpr("c1"));
        stmt.setComment("x");
        String out = SQLUtils.toSQLString(stmt, DbType.mysql);
        assertFalse(out.contains("WITH"));
    }
}

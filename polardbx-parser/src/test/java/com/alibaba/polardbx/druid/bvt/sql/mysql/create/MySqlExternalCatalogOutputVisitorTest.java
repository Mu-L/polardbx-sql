package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRefreshExternalCatalogStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

public class MySqlExternalCatalogOutputVisitorTest extends MysqlTest {

    private static SQLStatement parseSql(String sql) {
        return new MySqlStatementParser(sql).parseStatement();
    }

    private static void assertRoundTrip(String sql) {
        SQLStatement stmt = parseSql(sql);
        assertEquals(sql, stmt.toString());
        SQLStatement reparsed = parseSql(stmt.toString());
        assertEquals(stmt.getClass(), reparsed.getClass());
        assertEquals(stmt.toString(), reparsed.toString());
    }

    public void testDropSecretOutput() {
        assertRoundTrip("DROP SECRET IF EXISTS s1");
        assertRoundTrip("DROP SECRET s1");
    }

    public void testShowSecretsOutput() {
        assertRoundTrip("SHOW SECRETS");
        assertRoundTrip("SHOW SECRETS LIKE 'oss%'");
    }

    public void testRefreshExternalTableOutput() {
        assertRoundTrip("REFRESH EXTERNAL TABLE c1.d1.t1");
    }

    public void testRefreshExternalCatalogOutput() {
        assertRoundTrip("REFRESH EXTERNAL CATALOG hive_wh");
    }

    public void testShowExternalCatalogsOutput() {
        assertRoundTrip("SHOW EXTERNAL CATALOGS");
        assertRoundTrip("SHOW EXTERNAL CATALOGS LIKE 'hive%'");
    }

    public void testShowConnectorsOutput() {
        assertRoundTrip("SHOW CONNECTORS");
        assertRoundTrip("SHOW FULL CONNECTORS");
        assertRoundTrip("SHOW CONNECTORS LIKE 'hive%'");
        assertRoundTrip("SHOW FULL CONNECTORS LIKE 'hive%'");
    }

    public void testShowCreateExternalCatalogOutput() {
        assertRoundTrip("SHOW CREATE EXTERNAL CATALOG hive_wh");
    }

    public void testShowCreateSecretOutput() {
        assertRoundTrip("SHOW CREATE SECRET s1");
    }

    public void testDescribeExternalCatalogOutput() {
        SQLStatement stmt = parseSql("DESCRIBE EXTERNAL CATALOG hive_wh");
        assertEquals("DESC EXTERNAL CATALOG hive_wh", stmt.toString());
        assertRoundTrip("DESC EXTERNAL CATALOG hive_wh");
    }

    public void testCreateSecretOutput() {
        SQLStatement stmt = parseSql("CREATE SECRET IF NOT EXISTS s1 WITH ('type'='oss')");
        assertEquals("CREATE SECRET IF NOT EXISTS s1 WITH ('type'='oss')", stmt.toString());
        SQLStatement reparsed = parseSql(stmt.toString());
        assertTrue(reparsed instanceof MySqlCreateSecretStatement);
        assertEquals("oss", ((MySqlCreateSecretStatement) reparsed).getProperties().get("type"));
    }

    public void testCreateSecretOutputEscapesQuote() {
        SQLStatement stmt = parseSql("CREATE SECRET s1 WITH ('password'='a''b')");
        SQLStatement reparsed = parseSql(stmt.toString());
        assertTrue(reparsed instanceof MySqlCreateSecretStatement);
        assertEquals("a'b", ((MySqlCreateSecretStatement) reparsed).getProperties().get("password"));
    }

    public void testCreateSecretOutputEscapesBackslash() {
        SQLStatement stmt = parseSql("CREATE SECRET s1 WITH (password='a\\\\b')");
        SQLStatement reparsed = parseSql(stmt.toString());
        assertTrue(reparsed instanceof MySqlCreateSecretStatement);
        assertEquals("a\\b", ((MySqlCreateSecretStatement) reparsed).getProperties().get("password"));
    }

    public void testCreateSecretPropertiesPreserveInsertionOrder() {
        SQLStatement stmt = parseSql("CREATE SECRET s1 WITH (access_key='ak', secret_key='sk')");
        String s = stmt.toString();
        int ak = s.indexOf("access_key");
        int sk = s.indexOf("secret_key");
        assertTrue("access_key should precede secret_key in: " + s, ak >= 0 && sk > ak);
    }

    public void testAlterSecretOutput() {
        SQLStatement stmt = parseSql("ALTER SECRET s1 SET ('password'='p2')");
        assertEquals("ALTER SECRET s1 SET ('password'='p2')", stmt.toString());
        SQLStatement reparsed = parseSql(stmt.toString());
        assertTrue(reparsed instanceof MySqlAlterSecretStatement);
        assertEquals("p2", ((MySqlAlterSecretStatement) reparsed).getSetProperties().get("password"));
    }

    public void testAlterExternalCatalogOutput() {
        SQLStatement stmt = parseSql("ALTER EXTERNAL CATALOG hive_wh SET ('endpoint'='ep1') COMMENT 'c1'");
        assertEquals("ALTER EXTERNAL CATALOG hive_wh SET ('endpoint'='ep1') COMMENT 'c1'", stmt.toString());
        SQLStatement reparsed = parseSql(stmt.toString());
        assertTrue(reparsed instanceof MySqlAlterExternalCatalogStatement);
        MySqlAlterExternalCatalogStatement alter = (MySqlAlterExternalCatalogStatement) reparsed;
        assertEquals("ep1", alter.getSetProperties().get("endpoint"));
        assertEquals("c1", alter.getComment());
    }

    public void testDropExternalCatalogOutput() {
        assertRoundTrip("DROP EXTERNAL CATALOG IF EXISTS hive_wh");
        assertRoundTrip("DROP EXTERNAL CATALOG hive_wh");
    }

    public void testAlterExternalCatalogCommentOnlyOutput() {
        SQLStatement stmt = parseSql("ALTER EXTERNAL CATALOG hive_wh COMMENT 'only'");
        assertEquals("ALTER EXTERNAL CATALOG hive_wh COMMENT 'only'", stmt.toString());
    }

    public void testRefreshExternalCatalogStatementFieldsPreserved() {
        SQLStatement stmt = parseSql("REFRESH EXTERNAL TABLE c1.d1.t1");
        SQLStatement reparsed = parseSql(stmt.toString());
        assertTrue(reparsed instanceof MySqlRefreshExternalCatalogStatement);
        MySqlRefreshExternalCatalogStatement refresh = (MySqlRefreshExternalCatalogStatement) reparsed;
        assertTrue(refresh.isTable());
        assertEquals("c1", refresh.getCatalogName().getSimpleName());
        assertEquals("d1", refresh.getDbName().getSimpleName());
        assertEquals("t1", refresh.getTableName().getSimpleName());
    }

    public void testShowDatabasesFromCatalogOutput() {
        assertRoundTrip("SHOW DATABASES FROM cat1");
        assertRoundTrip("SHOW DATABASES FROM cat1 LIKE 'db%'");
        assertRoundTrip("SHOW DATABASES");
    }

    public void testCreateExternalCatalogOutput() {
        SQLStatement stmt = parseSql(
            "CREATE EXTERNAL CATALOG IF NOT EXISTS hive_wh WITH ('connector'='hive', 'endpoint'='thrift://hms:9083')");
        String output = stmt.toString();
        assertTrue(output, output.contains("CREATE EXTERNAL CATALOG"));
        assertTrue(output, output.contains("hive_wh"));
        assertTrue(output, output.contains("connector"));
        assertTrue(output, output.contains("hive"));
        SQLStatement reparsed = parseSql(output);
        assertTrue(reparsed instanceof MySqlCreateExternalCatalogStatement);
    }
}

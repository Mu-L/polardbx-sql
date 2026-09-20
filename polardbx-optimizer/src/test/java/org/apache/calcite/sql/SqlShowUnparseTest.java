package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SqlShowUnparseTest {

    private static String sql(SqlNode node) {
        return node.toString();
    }

    @Test
    public void testShowCreateSecretUnparseKeepsName() {
        SqlShowCreateSecret node = new SqlShowCreateSecret(SqlParserPos.ZERO, "s1");
        String s = sql(node);
        assertTrue("expected 'SHOW CREATE SECRET' in: " + s, s.toUpperCase().contains("SHOW CREATE SECRET"));
        assertTrue("expected secret name 's1' in: " + s, s.contains("s1"));
    }

    @Test
    public void testShowCreateExternalCatalogUnparseKeepsName() {
        SqlShowCreateExternalCatalog node = new SqlShowCreateExternalCatalog(SqlParserPos.ZERO, "hive_wh");
        String s = sql(node);
        assertTrue("expected 'SHOW CREATE EXTERNAL CATALOG' in: " + s,
            s.toUpperCase().contains("SHOW CREATE EXTERNAL CATALOG"));
        assertTrue("expected catalog name 'hive_wh' in: " + s, s.contains("hive_wh"));
    }

    @Test
    public void testDescribeExternalCatalogUnparseKeepsName() {
        SqlDescribeExternalCatalog node = new SqlDescribeExternalCatalog(SqlParserPos.ZERO, "hive_wh");
        String s = sql(node);
        assertTrue("expected 'EXTERNAL CATALOG' in: " + s, s.toUpperCase().contains("EXTERNAL CATALOG"));
        assertTrue("expected catalog name 'hive_wh' in: " + s, s.contains("hive_wh"));
    }

    @Test
    public void testDescribeExternalTableUnparseKeepsThreePartName() {
        SqlDescribeExternalTable node = new SqlDescribeExternalTable(SqlParserPos.ZERO, "cat", "db", "t");
        String s = sql(node);
        assertTrue("expected 'cat' in: " + s, s.contains("cat"));
        assertTrue("expected 'db' in: " + s, s.contains("db"));
        assertTrue("expected 't' in: " + s, s.contains("t"));
    }

    @Test
    public void testShowTablesFromCatalogUnparseKeepsNames() {
        SqlShowTablesFromCatalog node =
            new SqlShowTablesFromCatalog(SqlParserPos.ZERO, "cat", "db", null, null, false);
        String s = sql(node);
        assertTrue("expected 'SHOW TABLES FROM' in: " + s, s.toUpperCase().contains("SHOW TABLES FROM"));
        assertTrue("expected 'cat' in: " + s, s.contains("cat"));
        assertTrue("expected 'db' in: " + s, s.contains("db"));
    }

    @Test
    public void testShowTablesFromCatalogFullUnparseKeepsNames() {
        SqlShowTablesFromCatalog node =
            new SqlShowTablesFromCatalog(SqlParserPos.ZERO, "cat", "db", null, null, true);
        String s = sql(node);
        assertTrue("expected 'SHOW FULL TABLES FROM' in: " + s,
            s.toUpperCase().contains("SHOW FULL TABLES FROM"));
        assertTrue("expected 'cat' in: " + s, s.contains("cat"));
    }

    @Test
    public void testShowDatabasesFromCatalogUnparseKeepsName() {
        SqlShowDatabasesFromCatalog node =
            new SqlShowDatabasesFromCatalog(SqlParserPos.ZERO, "cat1", null, null);
        String s = sql(node);
        assertTrue("expected 'SHOW DATABASES FROM' in: " + s, s.toUpperCase().contains("SHOW DATABASES FROM"));
        assertTrue("expected 'cat1' in: " + s, s.contains("cat1"));
    }

    @Test
    public void testShowDatabasesFromCatalogUnparseKeepsLike() {
        SqlNode like = SqlLiteral.createCharString("db%", SqlParserPos.ZERO);
        SqlShowDatabasesFromCatalog node =
            new SqlShowDatabasesFromCatalog(SqlParserPos.ZERO, "cat1", like, null);
        String s = sql(node);
        assertTrue("expected 'LIKE' in: " + s, s.toUpperCase().contains("LIKE"));
        assertTrue("expected 'db%' in: " + s, s.contains("db%"));
    }
}

package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlVectorIndexOptionsTest {

    @Test
    public void testCreateIndexCloneAndUnparsePreserveOptions() {
        SqlCreateIndex createIndex = SqlCreateIndex.createLocalIndex(
            identifier("vi"),
            identifier("t"),
            Collections.singletonList(column("v")),
            null,
            false,
            SqlCreateIndex.SqlIndexConstraintType.VECTOR,
            false,
            null,
            Collections.<SqlIndexOption>emptyList(),
            null,
            null,
            "CREATE VECTOR INDEX vi ON t(v)",
            SqlParserPos.ZERO,
            allOptions());

        assertEquals("INNER_PRODUCT", createIndex.getDistance());
        assertEquals("16", createIndex.getM());
        assertEquals("80", createIndex.getEfConstruction());

        SqlCreateIndex clone = (SqlCreateIndex) createIndex.clone(SqlParserPos.ZERO);
        assertEquals(createIndex.getDistance(), clone.getDistance());
        assertEquals(createIndex.getM(), clone.getM());
        assertEquals(createIndex.getEfConstruction(), clone.getEfConstruction());

        String sql = unparse(createIndex);
        assertTrue(sql, sql.contains("DISTANCE=INNER_PRODUCT"));
        assertTrue(sql, sql.contains("M=16"));
        assertTrue(sql, sql.contains("EF_CONSTRUCTION=80"));
    }

    @Test
    public void testInlineDefinitionUnparsePreservesOptions() {
        SqlIndexDefinition definition = SqlIndexDefinition.vectorIndex(
            SqlParserPos.ZERO,
            false,
            null,
            false,
            "VECTOR",
            null,
            identifier("vi"),
            identifier("t"),
            Collections.singletonList(column("v")),
            Collections.<SqlIndexOption>emptyList(),
            null,
            false,
            allOptions());

        String sql = unparse(definition);
        assertTrue(sql, sql.contains("DISTANCE=INNER_PRODUCT"));
        assertTrue(sql, sql.contains("M=16"));
        assertTrue(sql, sql.contains("EF_CONSTRUCTION=80"));
    }

    @Test
    public void testValidation() {
        Map<String, String> invalid = new HashMap<>();
        invalid.put("distance", "MANHATTAN");
        assertInvalid(invalid);

        invalid.clear();
        invalid.put("m", "2");
        assertInvalid(invalid);

        invalid.clear();
        invalid.put("ef_construction", "1001");
        assertInvalid(invalid);

        invalid.clear();
        invalid.put("unknown", "1");
        assertInvalid(invalid);
    }

    private static Map<String, String> allOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("distance", "inner_product");
        options.put("m", "16");
        options.put("ef_construction", "80");
        return options;
    }

    private static SqlIdentifier identifier(String name) {
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }

    private static SqlIndexColumnName column(String name) {
        return new SqlIndexColumnName(SqlParserPos.ZERO, identifier(name), null, null);
    }

    private static String unparse(SqlNode node) {
        SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
        writer.setIndentation(0);
        node.unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }

    private static void assertInvalid(Map<String, String> options) {
        try {
            SqlVectorIndexOptions.from(options);
            fail("Expected invalid vector index options: " + options);
        } catch (IllegalArgumentException expected) {
            // Expected.
        }
    }
}

package com.alibaba.polardbx.qatest.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for native_query() function.
 * Uses mock connector with declared native_query schema and data.
 */
public class NativeQueryTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "nq_mock_secret";
    private static final String CATALOG_NAME = "nq_mock_cat";
    private static final String MOCK_PROPS =
        "'mock.databases' = 'wumu', "
            + "'mock.wumu.tables' = 'ext_demo', "
            + "'mock.wumu.ext_demo.columns' = 'id:int,name:varchar,score:int', "
            + "'mock.wumu.ext_demo.data' = '1,Alice,95|2,Bob,88|3,Carol,72|4,Dave,91|5,Eve,65|6,Frank,83|7,Grace,77|8,Hank,99|9,Ivy,54|10,Jack,86', "
            + "'mock.native_query.columns' = 'id:int,name:varchar,score:int', "
            + "'mock.native_query.data' = '1,Alice,95|2,Bob,88|3,Carol,72'";

    private Connection conn;

    @Before
    public void setUp() {
        conn = getPolardbxConnection();
        setupMockCatalog(conn, CATALOG_NAME, SECRET_NAME, MOCK_PROPS);
    }

    @After
    public void cleanup() {
        dropCatalog(conn, CATALOG_NAME);
        dropSecret(conn, SECRET_NAME);
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
            conn = null;
        }
    }

    @Test
    public void testNativeQuerySelectAll() throws SQLException {
        String sql = "SELECT * FROM TABLE ("
            + CATALOG_NAME + ".native_query('SELECT id, name, score FROM wumu.ext_demo LIMIT 3')) nq";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(3, meta.getColumnCount());
        Assert.assertEquals("id", meta.getColumnName(1));
        Assert.assertEquals("name", meta.getColumnName(2));
        Assert.assertEquals("score", meta.getColumnName(3));

        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        Assert.assertEquals(3, count);
    }

    @Test
    public void testNativeQueryProjection() throws SQLException {
        String sql = "SELECT nq.name FROM TABLE ("
            + CATALOG_NAME + ".native_query('SELECT name FROM wumu.ext_demo LIMIT 2')) nq";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        Assert.assertEquals(1, rs.getMetaData().getColumnCount());
        Assert.assertEquals("name", rs.getMetaData().getColumnName(1));

        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString("name"));
        }
        rs.close();
        Assert.assertEquals(3, names.size());
        Assert.assertEquals("Alice", names.get(0));
        Assert.assertEquals("Bob", names.get(1));
        Assert.assertEquals("Carol", names.get(2));
    }

    @Test
    public void testExternalCatalogSelectAll() throws SQLException {
        String sql = "SELECT * FROM " + CATALOG_NAME + ".wumu.ext_demo";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        Assert.assertEquals(10, count);
    }

    @Test
    public void testNativeQueryWithoutAlias() throws SQLException {
        // Verify no-alias syntax works
        String sql = "SELECT * FROM TABLE ("
            + CATALOG_NAME + ".native_query('SELECT id, name, score FROM wumu.ext_demo LIMIT 3'))";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        Assert.assertEquals(3, count);
    }

    @Test
    public void testNativeQueryWithAsAlias() throws SQLException {
        // Verify AS alias syntax
        String sql = "SELECT q.id FROM TABLE ("
            + CATALOG_NAME + ".native_query('SELECT id, name, score FROM wumu.ext_demo LIMIT 3')) AS q";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        Assert.assertEquals(3, count);
    }

    @Test
    public void testNativeQueryCatalogNotFound() {
        // Verify error when catalog does not exist
        String sql = "SELECT * FROM TABLE (nonexistent_cat.native_query('SELECT 1')) nq";
        try {
            JdbcUtil.executeQuery(sql, conn);
            Assert.fail("Expected exception for nonexistent catalog");
        } catch (Throwable e) {
            String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            Assert.assertTrue("Should mention catalog not found, got: " + e.getMessage(),
                msg.contains("not found") || msg.contains("catalog"));
        }
    }
}

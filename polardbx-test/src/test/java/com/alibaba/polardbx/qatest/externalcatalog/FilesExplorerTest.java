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
 * Integration tests for FILES() function.
 * Uses mock connector with declared columns and data.
 */
public class FilesExplorerTest extends BaseExternalCatalogTest {

    private static final String MOCK_DATA = "1,alice,hz,95|2,bob,bj,88|3,carol,sh,72|4,dave,sz,91|5,eve,gz,65";

    private Connection conn;

    @Before
    public void setUp() {
        conn = getPolardbxConnection();
    }

    @After
    public void cleanup() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
            conn = null;
        }
    }

    private String filesSql(String extraProps) {
        String sql = "SELECT * FROM FILES("
            + "'connector' = 'mock', "
            + "'mock.columns' = 'id:int,name:varchar,city:varchar,score:int', "
            + "'mock.data' = '" + MOCK_DATA + "'";
        if (extraProps != null && !extraProps.isEmpty()) {
            sql += ", " + extraProps;
        }
        sql += ") f";
        return sql;
    }

    @Test
    public void testSelectAllFromMockFiles() throws SQLException {
        String sql = filesSql(null);
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(4, meta.getColumnCount());
        Assert.assertEquals("id", meta.getColumnName(1));
        Assert.assertEquals("name", meta.getColumnName(2));
        Assert.assertEquals("city", meta.getColumnName(3));
        Assert.assertEquals("score", meta.getColumnName(4));

        List<String> ids = new ArrayList<>();
        List<String> names = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getString("id"));
            names.add(rs.getString("name"));
        }
        rs.close();

        Assert.assertEquals(5, ids.size());
        Assert.assertEquals("alice", names.get(0));
        Assert.assertEquals("eve", names.get(4));
    }

    @Test
    public void testSelectWithLimit() throws SQLException {
        String sql = filesSql(null) + " LIMIT 2";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();

        Assert.assertEquals(2, count);
    }

    @Test
    public void testSelectSpecificColumns() throws SQLException {
        String sql = "SELECT f.name, f.score FROM FILES("
            + "'connector' = 'mock', "
            + "'mock.columns' = 'id:int,name:varchar,city:varchar,score:int', "
            + "'mock.data' = '" + MOCK_DATA + "') f";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(2, meta.getColumnCount());

        Assert.assertTrue(rs.next());
        Assert.assertEquals("alice", rs.getString("name"));
        rs.close();
    }

    @Test
    public void testWhereOrderByLimit() throws SQLException {
        String sql = "SELECT * FROM FILES("
            + "'connector' = 'mock', "
            + "'mock.columns' = 'id:int,name:varchar,city:varchar,score:int', "
            + "'mock.data' = '" + MOCK_DATA + "') f"
            + " WHERE f.id < 3 ORDER BY f.id DESC LIMIT 5";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt("id"));
        }
        rs.close();

        Assert.assertEquals(2, ids.size());
        Assert.assertEquals(Integer.valueOf(2), ids.get(0));
        Assert.assertEquals(Integer.valueOf(1), ids.get(1));
    }

    @Test
    public void testCaseInsensitiveKeys() throws SQLException {
        // All option keys use mixed/upper case — should still work
        String sql = "SELECT * FROM FILES("
            + "'CONNECTOR' = 'mock', "
            + "'Mock.Columns' = 'id:int,name:varchar', "
            + "'MOCK.DATA' = '1,alice|2,bob') f";
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);

        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(2, meta.getColumnCount());

        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString("name"));
        }
        rs.close();

        Assert.assertEquals(2, names.size());
        Assert.assertEquals("alice", names.get(0));
        Assert.assertEquals("bob", names.get(1));
    }
}

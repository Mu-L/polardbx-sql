package com.alibaba.polardbx.qatest.externalcatalog;

import com.alibaba.polardbx.qatest.BaseExternalCatalogTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Integration tests for external catalog query execution with mock data.
 */
public class ExternalCatalogPushDownTest extends BaseExternalCatalogTest {

    private static final String SECRET_NAME = "pd_secret";
    private static final String CATALOG_NAME = "pd_cat";
    private static final String MOCK_PROPS =
        "'mock.databases'='pd_db',"
            + "'mock.pd_db.tables'='t_score',"
            + "'mock.pd_db.t_score.columns'='id:int,name:varchar,city:varchar,score:int',"
            + "'mock.pd_db.t_score.data'="
            + "'1,a,hz,95|2,b,bj,88|3,c,sh,72|4,d,sz,91|5,e,gz,65|6,f,cd,83|7,g,nj,77|8,h,wh,99|9,i,xa,54|10,j,xm,86',"
            + "'mock.capabilities'='PROJECT,FILTER,SORT,AGG'";

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

    private String t() {
        return CATALOG_NAME + ".pd_db.t_score";
    }

    @Test
    public void testOrderByLimit() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("SELECT id, name FROM " + t() + " ORDER BY id LIMIT 3", conn);
        List<Integer> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getInt("id"));
        }
        rs.close();
        Assert.assertEquals(Arrays.asList(1, 2, 3), ids);
    }

    @Test
    public void testCountAll() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("SELECT COUNT(*) AS cnt FROM " + t(), conn);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(10L, rs.getLong("cnt"));
        rs.close();
    }

    @Test
    public void testFilterAndSort() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(
            "SELECT name, score FROM " + t() + " WHERE score >= 85 ORDER BY score DESC", conn);
        List<Integer> scores = new ArrayList<>();
        while (rs.next()) {
            scores.add(rs.getInt("score"));
        }
        rs.close();
        Assert.assertEquals(5, scores.size());
        for (int i = 1; i < scores.size(); i++) {
            Assert.assertTrue(scores.get(i - 1) >= scores.get(i));
        }
    }

}

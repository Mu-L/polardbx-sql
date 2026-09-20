package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcTest extends ReadBaseTestCase {
    static final String schema = "jdbc_test";

    @BeforeClass
    public static void setUp() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            conn.createStatement().execute("create database if not exists " + schema + " mode=auto");
        }
    }

    @AfterClass
    public static void cleanUp() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            conn.createStatement().execute("drop database if  exists " + schema);
        }
    }

    @Test
    public void getColumnNameTest() throws SQLException {
        String tbl = "column_name_test";
        String creatTbl = String.format("CREATE TABLE %s (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8", tbl);
        try (Connection conn = getPolardbxConnection(schema)) {
            conn.createStatement().execute(creatTbl);

            String testSql = "select Seller_id seLLer_id, seller_id as buyer_id, iD, oRder_id xx from " + tbl;
            ResultSet rs = conn.createStatement().executeQuery(testSql);

            String col1 = rs.getMetaData().getColumnName(1);
            String col1Label = rs.getMetaData().getColumnLabel(1);
            String col2 = rs.getMetaData().getColumnName(2);
            String col2Label = rs.getMetaData().getColumnLabel(2);
            String col3 = rs.getMetaData().getColumnName(3);
            String col3Label = rs.getMetaData().getColumnLabel(3);
            String col4 = rs.getMetaData().getColumnName(4);
            String col4Label = rs.getMetaData().getColumnLabel(4);

            assert col1.equals("seller_id");
            assert col1Label.equals("seLLer_id");

            assert col2.equals("seller_id");
            assert col2Label.equals("buyer_id");

            assert col3.equals("id");
            assert col3Label.equals("iD");

            assert col4.equals("order_id");
            assert col4Label.equals("xx");
        }
    }
}

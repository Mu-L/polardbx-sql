package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CTECrossDBTest extends BaseTestCase {

    private static final String db1 = "CTE_CROSS_DB1";
    private static final String db2 = "CTE_CROSS_DB2";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("create database if not exists " + db1 + " mode=auto");
            c.createStatement().execute("create database if not exists " + db2 + " mode=auto");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + db1);
            c.createStatement().execute("drop database if exists " + db2);
        }
    }

    @Test
    public void testCTECrossDB() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("create table if not exists " + db1 + ".fin_expense_head ("
                + "id bigint, `status` varchar(32), source_document varchar(64), expense_type varchar(64)) single");
            c.createStatement().execute("create table if not exists " + db1 + ".fin_expense_hkont ("
                + "expense_id bigint, bqfse decimal(18,2), cate_id bigint) single");
            c.createStatement().execute("create table if not exists " + db2 + ".mdm_sap_subject_cate ("
                + "id bigint, cate_num varchar(64)) single");

            c.createStatement().execute("truncate table " + db1 + ".fin_expense_head");
            c.createStatement().execute("truncate table " + db1 + ".fin_expense_hkont");
            c.createStatement().execute("truncate table " + db2 + ".mdm_sap_subject_cate");

            c.createStatement()
                .execute("insert into " + db1 + ".fin_expense_head(id, `status`, source_document, expense_type) "
                    + "values (1, 'audited', 'GZJL20251028000001', 'manufacturing_overhead')");
            c.createStatement().execute("insert into " + db1 + ".fin_expense_hkont(expense_id, bqfse, cate_id) "
                + "values (1, 100.00, 10)");
            c.createStatement()
                .execute("insert into " + db2 + ".mdm_sap_subject_cate(id, cate_num) values (10, '0123')");

            String sql = "WITH "
                + "labor_cost_temp AS ("
                + " SELECT COALESCE(SUM(feht.bqfse) * (10557 / NULL), 0) AS laborCost"
                + " FROM " + db1 + ".fin_expense_head feh"
                + " INNER JOIN " + db1 + ".fin_expense_hkont feht ON feht.expense_id = feh.id"
                + " WHERE feh.`status` IN ('audited','finished')"
                + " AND feh.source_document = 'GZJL20251028000001'"
                + " AND feh.expense_type = 'manufacturing_overhead'"
                + " AND EXISTS ("
                + "   SELECT 1 FROM " + db2 + ".mdm_sap_subject_cate m"
                + "   WHERE m.id = feht.cate_id AND m.cate_num LIKE '01%')"
                + " ) "
                + " SELECT ROUND(IF(laborCost * 0 / 10557 IS NULL, 0.00, laborCost * 0 / 10557), 2) AS increaseEmbryoTotalLaborCost"
                + " FROM labor_cost_temp";

            try (java.sql.Statement stmt = c.createStatement()) {
                c.createStatement().execute("clear plancache");
                ResultSet rs = stmt.executeQuery(sql);
                org.junit.Assert.assertTrue(rs.next());
            }
        }
    }

}
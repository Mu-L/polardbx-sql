package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Materialize optimize test for IN subquery
 * CorrelateApply rel node (transformed by in subquery) should push down and looks like `xx in (?)`
 */

public class SubqueryJoinOnTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog(SubqueryJoinOnTest.class);
    private static final String testSchemaDrds = "subquery_join_on_test_db";
    private static final String testSchemaDrds1 = "subquery_join_on_test_db1";
    private static final String testSchemaAuto = "subquery_join_on_test_db_auto";
    private static final String testSchemaAuto1 = "subquery_join_on_test_db_auto1";

    @Test
    public void testSubqueryJoinOnSameDB() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            Statement stmt = c.createStatement();

            // test in drds db
            stmt.execute("use " + testSchemaDrds);
            stmt.execute(
                "select * from tbl_single t1 join tbl_single1 t2 on t1.id = t2.id and t1.QHDM in (select SJBM from tbl_broadcast)");

            // test in auto db
            stmt.execute("use " + testSchemaAuto);
            stmt.execute(
                "select * from single_tbl t1 join single_tbl1 t2 on t1.bid = t2.bid and t1.name in (select name from single_tbl2)");
        }
    }

    @Test
    public void testSubqueryJoinOnCrossDB() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            Statement stmt = c.createStatement();

            // test in drds db(not support yet)
            stmt.execute("use " + testSchemaDrds);
            try {
                stmt.execute(
                    "select * from tbl_single t1 join tbl_single1 t2 on t1.id = t2.id and t1.QHDM in (select SJBM from "
                        + testSchemaDrds1 + ".tbl_broadcast)");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("subquery in join not support yet"));
            }
            // test in auto db
            stmt.execute("use " + testSchemaAuto);
            stmt.execute(
                "select * from single_tbl t1 join single_tbl1 t2 on t1.bid = t2.bid and t1.name in (select name from "
                    + testSchemaAuto1 + ".single_tbl2)");
        }
    }

    /**
     * prepare db and table for test
     */
    @BeforeClass
    public static void prepareCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            prepareDrdsDB(c, testSchemaDrds);
            prepareDrdsDB(c, testSchemaDrds1);
            prepareAutoDB(c, testSchemaAuto);
            prepareAutoDB(c, testSchemaAuto1);
        } finally {
            log.info(testSchemaDrds + " catalog prepared");
            log.info(testSchemaAuto + " catalog prepared");
        }
    }

    private static void prepareAutoDB(Connection c, String db) throws SQLException {
        c.createStatement().execute("drop database if exists " + db);
        c.createStatement().execute("create database " + db + " mode='auto'");
        c.createStatement().execute("use " + db);

        c.createStatement().execute(
            "CREATE TABLE single_tbl(\n"
                + " id bigint not null auto_increment, \n"
                + " bid int, \n"
                + " name varchar(30), \n"
                + " primary key(id)\n"
                + ") SINGLE;"
        );
        c.createStatement().execute(
            "CREATE TABLE single_tbl1(\n"
                + " id bigint not null auto_increment, \n"
                + " bid int, \n"
                + " name varchar(30), \n"
                + " primary key(id)\n"
                + ") SINGLE;"
        );
        c.createStatement().execute(
            "CREATE TABLE single_tbl2(\n"
                + " id bigint not null auto_increment, \n"
                + " bid int, \n"
                + " name varchar(30), \n"
                + " primary key(id)\n"
                + ") SINGLE;"
        );
    }

    private static void prepareDrdsDB(Connection c, String db) throws SQLException {
        c.createStatement().execute("drop database if exists " + db);
        c.createStatement().execute("create database " + db + " mode=drds");
        c.createStatement().execute("use " + db);
        c.createStatement().execute(
            "CREATE TABLE `tbl_mult` (\n"
                + "    `ID` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
                + "    `YLJGDM` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "    `LASTUPDATETIME` datetime DEFAULT NULL COMMENT '最后一次更新时间',\n"
                + "    PRIMARY KEY (`ID`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_general_ci dbpartition by YYYYMM(`LASTUPDATETIME`)");
        c.createStatement().execute(
            "CREATE TABLE `tbl_single` (\n"
                + "    `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
                + "    `QHDM` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "    `PDM` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "    PRIMARY KEY (`ID`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_general_ci;\n");
        c.createStatement().execute(
            "CREATE TABLE `tbl_single1` (\n"
                + "    `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
                + "    `QHDM` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "    `PDM` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "    PRIMARY KEY (`ID`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_general_ci;\n");
        c.createStatement().execute(
            "CREATE TABLE `tbl_broadcast` (\n"
                + "    `YLJGDM` varchar(30) NOT NULL ,\n"
                + "    `SJBM` varchar(64) DEFAULT NULL ,\n"
                + "    `QXBM` varchar(64) DEFAULT NULL ,\n"
                + "    PRIMARY KEY (`YLJGDM`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_general_ci broadcast;"
        );
    }

    /**
     * clear db after test
     */
    @AfterClass
    public static void clearCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + testSchemaDrds);
            c.createStatement().execute("drop database if exists " + testSchemaAuto);
        } finally {
            log.info(testSchemaDrds + " catalog was dropped");
            log.info(testSchemaAuto + " catalog was dropped");
        }
    }
}

package com.alibaba.polardbx.qatest.ddl.auto.gsi;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * AONE 81632339: GSI 回表 binary(16)/varbinary 主键类型被误判为 StringType，查询返回空。
 * 走 GSI 回表（需要 Lookup Join 关联主表）的结果应与不走 GSI（直接主表扫描）的结果一致。
 */
public class GsiLookupBinaryTypeTest extends BaseTestCase {

    @After
    public void dropTestDatabases() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists gsi_lookup_binary_test");
            c.createStatement().execute("drop database if exists gsi_lookup_varbinary_test");
        }
    }

    @Test
    public void testGsiLookupJoinWithBinaryPrimaryKey() throws SQLException {
        String dbName = "gsi_lookup_binary_test";
        createDBAndTbForBinaryPk(dbName);

        try (Connection c = getPolardbxConnection(dbName)) {
            c.createStatement()
                .execute("insert into order_mapping(external_order_no, uuid, company_id, check_date) values "
                    + "('96010011120001022604241613001369', x'019dcdb3fc87791d9c038a1285a90126', 3, '2026-04-27')");

            List<String> viaGsi = queryUuidHexList(c,
                "/*+TDDL:INDEX(order_mapping, gsi_order_mapping_order_no_company)*/"
                    + " SELECT hex(uuid) FROM order_mapping WHERE external_order_no = "
                    + "'96010011120001022604241613001369' AND company_id = 3 ORDER BY check_date DESC LIMIT 1");

            List<String> baseline = queryUuidHexList(c,
                "/*+TDDL:CMD_EXTRA(ENABLE_INDEX_SELECTION=false)*/"
                    + " SELECT hex(uuid) FROM order_mapping WHERE external_order_no = "
                    + "'96010011120001022604241613001369' AND company_id = 3 ORDER BY check_date DESC LIMIT 1");

            assertThat(baseline).isNotEmpty();
            assertThat(viaGsi).isEqualTo(baseline);
        }
    }

    @Test
    public void testGsiLookupJoinWithVarbinaryPrimaryKey() throws SQLException {
        String dbName = "gsi_lookup_varbinary_test";
        createDBAndTbForVarbinaryPk(dbName);

        try (Connection c = getPolardbxConnection(dbName)) {
            c.createStatement()
                .execute("insert into order_mapping_vb(external_order_no, uid, company_id, check_date) values "
                    + "('96010011120001022604241613001370', x'019dcdb3fc87791d9c038a1285a90126', 5, '2026-04-27')");

            List<String> viaGsi = queryUuidHexList(c,
                "/*+TDDL:INDEX(order_mapping_vb, gsi_order_mapping_vb_order_no_company)*/"
                    + " SELECT hex(uid) FROM order_mapping_vb WHERE external_order_no = "
                    + "'96010011120001022604241613001370' AND company_id = 5 ORDER BY check_date DESC LIMIT 1",
                "uid");

            List<String> baseline = queryUuidHexList(c,
                "/*+TDDL:CMD_EXTRA(ENABLE_INDEX_SELECTION=false)*/"
                    + " SELECT hex(uid) FROM order_mapping_vb WHERE external_order_no = "
                    + "'96010011120001022604241613001370' AND company_id = 5 ORDER BY check_date DESC LIMIT 1",
                "uid");

            assertThat(baseline).isNotEmpty();
            assertThat(viaGsi).isEqualTo(baseline);
        }
    }

    private List<String> queryUuidHexList(Connection c, String sql) throws SQLException {
        return queryUuidHexList(c, sql, "uuid");
    }

    private List<String> queryUuidHexList(Connection c, String sql, String columnLabel) throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = c.createStatement().executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    private void createDBAndTbForBinaryPk(String dbName) throws SQLException {
        String createDB = "create database if not exists " + dbName + " mode=auto";
        String createTb = "CREATE TABLE order_mapping (\n"
            + "  external_order_no varchar(32) NOT NULL,\n"
            + "  uuid binary(16) NOT NULL,\n"
            + "  company_id bigint NOT NULL,\n"
            + "  check_date date NOT NULL,\n"
            + "  PRIMARY KEY (uuid),\n"
            + "  GLOBAL INDEX gsi_order_mapping_order_no_company (external_order_no, company_id)\n"
            + "    PARTITION BY KEY(external_order_no, company_id) PARTITIONS 4\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
            + "PARTITION BY KEY(uuid) PARTITIONS 4";
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + dbName);
            c.createStatement().execute(createDB);
            c.createStatement().execute("use " + dbName);
            c.createStatement().execute(createTb);
        }
    }

    private void createDBAndTbForVarbinaryPk(String dbName) throws SQLException {
        String createDB = "create database if not exists " + dbName + " mode=auto";
        String createTb = "CREATE TABLE order_mapping_vb (\n"
            + "  external_order_no varchar(32) NOT NULL,\n"
            + "  uid varbinary(32) NOT NULL,\n"
            + "  company_id bigint NOT NULL,\n"
            + "  check_date date NOT NULL,\n"
            + "  PRIMARY KEY (uid),\n"
            + "  GLOBAL INDEX gsi_order_mapping_vb_order_no_company (external_order_no, company_id)\n"
            + "    PARTITION BY KEY(external_order_no, company_id) PARTITIONS 4\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
            + "PARTITION BY KEY(uid) PARTITIONS 4";
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + dbName);
            c.createStatement().execute(createDB);
            c.createStatement().execute("use " + dbName);
            c.createStatement().execute(createTb);
        }
    }
}

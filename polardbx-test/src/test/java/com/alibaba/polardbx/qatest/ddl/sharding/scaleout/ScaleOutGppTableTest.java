package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class ScaleOutGppTableTest extends DDLBaseNewDBTestCase {
    public static String DB_NAME = "test_gpp_move_database";
    private static final String ENABLE_PHYSICAL_BACKFILL_HINT =
        "/*+TDDL:CMD_EXTRA(physical_backfill_enable=TRUE, TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=32768, SHARE_STORAGE_MODE=true)*/";
    private static final String TABLE_NAME1 = "TABLE_NAME1_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME2 = "TABLE_NAME2_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME3 = "TABLE_NAME3_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME4 = "TABLE_NAME4_" + RandomUtils.getStringBetween(4, 6);

    final static String PART_DEF =
        "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 3;";

    @BeforeClass
    public static void setUpClass() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=drds");
            createTable();
            initData(100_000);
        }
    }

    @Test
    public void testMoveDatabase() throws SQLException {
        moveDatabase(false, false);
        moveDatabase(true, false);
        moveDatabase(false, true);
        moveDatabase(true, true);
    }

    private void moveDatabase(boolean addColumn, boolean addIndex) throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        Map<String, List<String>> storageInstToGroup = getInstIdGroupMap(DB_NAME, tddlConnection);
        if (storageInstToGroup.size() <= 1) {
            return;
        }
        if (addColumn) {
            alterTableAddDropColumn();
        }
        if (addIndex) {
            alterTableAddIndex();
        }
        List<String> storages = new ArrayList<>();
        List<List<String>> partitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : storageInstToGroup.entrySet()) {
            String storage = entry.getKey();
            storages.add(storage);
            partitions.add(entry.getValue());
        }
        try (Statement stmt = tddlConnection.createStatement()) {
            for (int i = 0; i < storages.size(); i++) {
                int j = ((i == storages.size() - 1) ? 0 : i + 1);
                String movePgSql =
                    String.format(
                        "move database %s to '%s'",
                        ENABLE_PHYSICAL_BACKFILL_HINT + "(" + String.join(",", partitions.get(i)) + ")",
                        storages.get(j));
                System.out.println(movePgSql);
                stmt.executeUpdate(movePgSql);
                insertData(100);
                JdbcUtil.executeQuery("select count(1) from " + TABLE_NAME1, tddlConnection);
                JdbcUtil.executeQuery("show create table " + TABLE_NAME1 + (isMySQL80() ? " for export" : ""),
                    tddlConnection);
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        try {
            JdbcUtil.useDb(tddlConnection, DB_NAME);
            JdbcUtil.executeUpdate(tddlConnection, "rebalance database explain=true");
        } catch (Exception exception) {
            //pass
        }
    }

    private static void createTable() throws SQLException {
        String sql = "CREATE TABLE " + TABLE_NAME1 + " (" +
            "id INT AUTO_INCREMENT PRIMARY KEY," +
            "tinyint_col TINYINT," +
            "smallint_col SMALLINT," +
            "mediumint_col MEDIUMINT," +
            "int_col INT," +
            "bigint_col BIGINT," +
            "decimal_col DECIMAL(10, 2)," +
            "float_col FLOAT," +
            "double_col DOUBLE," +
            "bit_col BIT(8)," +
            "char_col CHAR(10)," +
            "varchar_col VARCHAR(128)," +
            "binary_col BINARY(10)," +
            "varbinary_col VARBINARY(255)," +
            "tinyblob_col TINYBLOB," +
            "blob_col BLOB," +
            "mediumblob_col MEDIUMBLOB," +
            "longblob_col LONGBLOB," +
            "date_col DATE," +
            "datetime_col DATETIME," +
            "timestamp_col TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
            "time_col TIME," +
            "year_col YEAR," +
            "enum_col ENUM('A', 'B', 'C')," +
            "set_col SET('X', 'Y', 'Z'),"
            + " index idx_int_col(int_col),"
            + " index idx_char_col(char_col),"
            + " index idx_varchar_col(varchar_col(100)),"
            + " index idx_datetime_col(datetime_col),"
            + " index idx_timestamp_col(timestamp_col))";

        sql = sql + PART_DEF;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            if (isMySQL80()) {
                stmt.executeQuery("set opt_index_format_gpp_enabled=false;");
            }
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME2, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME3, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME4, TABLE_NAME1));
        }
    }

    private void alterTableAddIndex() throws SQLException {
        List<String> tableNames = ImmutableList.of(TABLE_NAME1, TABLE_NAME2, TABLE_NAME3, TABLE_NAME4);
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, DB_NAME);
            for (String tableName : tableNames) {
                if (isMySQL80()) {
                    JdbcUtil.executeUpdate(conn, "set opt_index_format_gpp_enabled=" + RandomUtils.getBoolean());
                    JdbcUtil.executeUpdate(conn, "set opt_index_format_panda_enabled=" + RandomUtils.getBoolean());
                }
                if (tableName.equalsIgnoreCase(TABLE_NAME1)) {
                    JdbcUtil.executeUpdate(conn,
                        "alter table " + tableName + " add index idx2_int_col(int_col)");
                } else {
                    JdbcUtil.executeUpdate(conn,
                        "alter table " + tableName + " add unique index idx2_int_col(int_col)");
                }
                if (isMySQL80()) {
                    JdbcUtil.executeUpdate(conn, "set opt_index_format_gpp_enabled=" + RandomUtils.getBoolean());
                    JdbcUtil.executeUpdate(conn, "set opt_index_format_panda_enabled=" + RandomUtils.getBoolean());
                }
                int randLen = RandomUtils.getIntegerBetween(30, 50);
                String addVarIndex = String.format("alter table %s add index idx2_var_col(varchar_col(%d))",
                    tableName, randLen);
                JdbcUtil.executeUpdate(conn, addVarIndex);
                if (isMySQL80()) {
                    JdbcUtil.executeUpdate(conn, "set opt_index_format_gpp_enabled=" + RandomUtils.getBoolean());
                    JdbcUtil.executeUpdate(conn, "set opt_index_format_panda_enabled=" + RandomUtils.getBoolean());
                }
                JdbcUtil.executeUpdate(conn, "alter table " + tableName + " drop index idx2_char_col", true, true);
                JdbcUtil.executeUpdate(conn, "alter table " + tableName + " add index idx2_char_col(char_col)");
                List<Pair<String, String>> topoList = showTopologyWithGroup(tddlConnection, TABLE_NAME1);

                for (Pair<String, String> topoPair : topoList) {
                    if (isMySQL80()) {
                        JdbcUtil.executeUpdate(conn, "set opt_index_format_gpp_enabled=" + RandomUtils.getBoolean());
                        JdbcUtil.executeUpdate(conn, "set opt_index_format_panda_enabled=" + RandomUtils.getBoolean());
                    }
                    JdbcUtil.executeUpdateSuccess(conn, String.format(
                        "/*+TDDL:node='%s'*/alter table %s drop index idx2_var_col", topoPair.getKey(),
                        topoPair.getValue()));
                    JdbcUtil.executeUpdateSuccess(conn, String.format(
                        "/*+TDDL:node='%s'*/alter table %s add index idx2_var_col (varchar_col(%d))", topoPair.getKey(),
                        topoPair.getValue(), RandomUtils.getIntegerBetween(30, 50)));
                }
            }

            if (isMySQL80()) {
                JdbcUtil.executeUpdate(conn, "set opt_index_format_gpp_enabled=" + true);
                JdbcUtil.executeUpdate(conn, "set opt_index_format_panda_enabled=" + false);
            }
        }
    }

    private void alterTableAddDropColumn() throws SQLException {
        List<String> tableNames = ImmutableList.of(TABLE_NAME1, TABLE_NAME2, TABLE_NAME3, TABLE_NAME4);
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, DB_NAME);
            for (String tableName : tableNames) {
                JdbcUtil.executeUpdate(conn, "alter table " + tableName + " drop column varchar_col");
                JdbcUtil.executeUpdate(conn, "alter table " + tableName + " add column varchar_col varchar(255)");
                JdbcUtil.executeUpdate(conn, "alter table " + tableName + " drop column int_col");
                JdbcUtil.executeUpdate(conn, "alter table " + tableName + " add column int_col int");
            }
        }
    }

    private static void initData(int count) {
        String sql = "INSERT INTO %s "
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        int log2N = (int) Math.ceil(Math.log(count) / Math.log(2));
        String insertSelect = "INSERT INTO " + TABLE_NAME1
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, "
            + "float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, "
            + "timestamp_col, time_col, year_col, enum_col, set_col) "
            + "SELECT tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col,"
            + " varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col FROM "
            + TABLE_NAME1;
        List<String> tableNames = ImmutableList.of(TABLE_NAME1, TABLE_NAME2, TABLE_NAME3, TABLE_NAME4);
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            for (String tableName : tableNames) {
                int maxI = tableName.equalsIgnoreCase(TABLE_NAME1) ? 1 : 100;
                try {
                    for (int i = 0; i < maxI; i++) {
                        PreparedStatement pstmt = connection.prepareStatement(String.format(sql, tableName));
                        pstmt.setByte(1, (byte) (Math.random() * 256));
                        pstmt.setShort(2, (short) (Math.random() * Short.MAX_VALUE));
                        pstmt.setInt(3, ((int) (Math.random() * Integer.MAX_VALUE)) & 0x007FFFFF);
                        pstmt.setInt(4, i++);
                        pstmt.setLong(5, Math.abs(new Random().nextLong()));
                        pstmt.setBigDecimal(6, BigDecimal.valueOf(RandomUtils.getIntegerBetween(1, 199) * 1000, 2));
                        pstmt.setFloat(7, (float) (Math.random() * 1000));
                        pstmt.setDouble(8, Math.random() * 1000);
                        pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(0, 127)));
                        pstmt.setString(10, "char" + (int) (Math.random() * 10));
                        pstmt.setString(11, "varchar" + (int) (Math.random() * 100));
                        pstmt.setBytes(12, ("binary" + (int) (Math.random() * 10)).getBytes());
                        pstmt.setBytes(13, ("varbinary" + (int) (Math.random() * 100)).getBytes());
                        pstmt.setDate(14, Date.valueOf(LocalDate.now()));
                        pstmt.setTimestamp(15, Timestamp.valueOf(LocalDateTime.now()));
                        pstmt.setTimestamp(16, Timestamp.valueOf(LocalDateTime.now()));
                        pstmt.setTime(17, Time.valueOf(LocalTime.now()));
                        pstmt.setInt(18, Year.now().getValue());
                        pstmt.setString(19, "A");
                        pstmt.setString(20, "X,Y");

                        pstmt.executeUpdate();
                    }
                    if (tableName.equalsIgnoreCase(TABLE_NAME1)) {
                        do {
                            JdbcUtil.executeUpdateSuccess(connection, insertSelect);
                            log2N--;
                        } while (log2N > 0);
                    }
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertData(int count) {
        String sql = "INSERT INTO " + TABLE_NAME1
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            try {
                JdbcUtil.useDb(connection, DB_NAME);
                for (int i = 0; i < count; i++) {
                    PreparedStatement pstmt = connection.prepareStatement(sql);
                    pstmt.setByte(1, (byte) (Math.random() * 256));
                    pstmt.setShort(2, (short) (Math.random() * Short.MAX_VALUE));
                    pstmt.setInt(3, ((int) (Math.random() * Integer.MAX_VALUE)) & 0x007FFFFF);
                    pstmt.setInt(4, (int) (Math.random() * Integer.MAX_VALUE));
                    pstmt.setLong(5, Math.abs(new Random().nextLong()));
                    pstmt.setBigDecimal(6, BigDecimal.valueOf(RandomUtils.getIntegerBetween(1, 199) * 1000, 2));
                    pstmt.setFloat(7, (float) (Math.random() * 1000));
                    pstmt.setDouble(8, Math.random() * 1000);
                    pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(0, 127)));
                    pstmt.setString(10, "char" + (int) (Math.random() * 10));
                    pstmt.setString(11, "varchar" + (int) (Math.random() * 100));
                    pstmt.setBytes(12, ("binary" + (int) (Math.random() * 10)).getBytes());
                    pstmt.setBytes(13, ("varbinary" + (int) (Math.random() * 100)).getBytes());
                    pstmt.setDate(14, Date.valueOf(LocalDate.now()));
                    pstmt.setTimestamp(15, Timestamp.valueOf(LocalDateTime.now()));
                    pstmt.setTimestamp(16, Timestamp.valueOf(LocalDateTime.now()));
                    pstmt.setTime(17, Time.valueOf(LocalTime.now()));
                    pstmt.setInt(18, Year.now().getValue());
                    pstmt.setString(19, "A");
                    pstmt.setString(20, "X,Y");

                    pstmt.executeUpdate();
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected Map<String, List<String>> getInstIdGroupMap(String schemaName, Connection conn) {
        Map<String, List<String>> storageInstToGroup = new TreeMap<>(String::compareToIgnoreCase);
        String sql = String.format("show ds where db='%s'", schemaName);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            while (rs.next()) {
                String groupName = rs.getString("GROUP");
                if (groupName.toLowerCase().contains("single_group")) {
                    continue;
                }
                String dnId = rs.getString("STORAGE_INST_ID");
                List<String> groups = storageInstToGroup.computeIfAbsent(dnId, k -> new ArrayList<>());
                groups.add(groupName);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        return storageInstToGroup;
    }
}

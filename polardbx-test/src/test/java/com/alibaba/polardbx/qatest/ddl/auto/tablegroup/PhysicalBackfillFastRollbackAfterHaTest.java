package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.misc.Sleep;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

public class PhysicalBackfillFastRollbackAfterHaTest extends DDLBaseNewDBTestCase {
    public static String DB_NAME = "test_fast_rollback_after_ha_" + RandomUtils.getStringBetween(4, 6);
    private static final String ENABLE_PHYSICAL_BACKFILL_HINT =
        "/*+TDDL:CMD_EXTRA(physical_backfill_enable=TRUE, TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=1, FP_PHYSICAL_BACKFILL_TASK_RANDOM_FAIL='100')*/";
    private static final String TABLE_NAME1 = "TABLE_NAME1_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME2 = "TABLE_NAME2_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME3 = "TABLE_NAME3_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME4 = "TABLE_NAME4_" + RandomUtils.getStringBetween(4, 6);

    final static String KEY_KEY_PART =
        "partition by key(id) partitions 3 subpartition by key(bigint_col) subpartitions 1;";

    @BeforeClass
    public static void setUpClass() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            createTable();
            initData(1000);
        }
    }

    @Test
    public void testMovePartitionGroup() throws SQLException, InterruptedException {
        Map<Pair<String, Integer>, Pair<String, Integer>> changeMap = new HashMap<>();
        movePartition();
        List<Map<String, String>> fullDDL = showFullDDL();
        if (fullDDL.isEmpty()) {
            return;
        }
        Long jobId = Long.valueOf(fullDDL.get(0).get("JOB_ID"));
        changeTargetDnLocalition(changeMap, jobId);
        if (!changeMap.isEmpty()) {
            JdbcUtil.executeUpdateFailed(tddlConnection, "rollback ddl " + jobId, "ERR");
            checkXDataSourceExistence(true, changeMap.values().iterator().next());
            insertCorrectionAddr(changeMap, jobId);
            JdbcUtil.executeUpdate(tddlConnection, "continue ddl " + jobId, true, true);
            checkXDataSourceExistence(false, changeMap.values().iterator().next());
        }
        JdbcUtil.executeSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(PHYSICAL_BACKFILL_SPEED_TEST=true, physical_backfill_enable=true)*/rebalance database explain=true;");
        JdbcUtil.executeSuccess(tddlConnection,
            "/*+TDDL:cmd_extra(physical_backfill_enable=true)*/rebalance database explain=true;");
        List<com.alibaba.polardbx.common.utils.Pair<String, Boolean>> storageInsts =
            DdlStateCheckUtil.getRealStorageList();
        if (storageInsts.size() <= 1 && !storageInsts.get(0).getValue()) {
            return;
        }
        String dainNodeSql = String.format(" rebalance database drain_node = '%s' ", storageInsts.get(0).getKey());
        JdbcUtil.executeSuccess(tddlConnection, dainNodeSql);
        jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, dainNodeSql);
        DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, "somerandom_badstring_forthis");
        //do nothing but not raise error
        JdbcUtil.executeSuccess(tddlConnection, dainNodeSql);

    }

    private void insertCorrectionAddr(Map<Pair<String, Integer>, Pair<String, Integer>> changeMap, Long jobId)
        throws SQLException {
        String sql =
            META_DB_HINT + " insert into rollback_task_config(root_job_id, old_value, new_value) values ( ?, ?, ?)";

        // 遍历changeMap，将每个条目插入到rollback_task_config表中
        for (Map.Entry<Pair<String, Integer>, Pair<String, Integer>> entry : changeMap.entrySet()) {
            Pair<String, Integer> newAddr = entry.getKey();
            Pair<String, Integer> oldAddr = entry.getValue();

            // 构造old_value和new_value字符串
            String oldValue = oldAddr.getKey() + ":" + oldAddr.getValue();
            String newValue = newAddr.getKey() + ":" + newAddr.getValue();

            try (PreparedStatement pstmt = tddlConnection.prepareStatement(sql)) {
                pstmt.setLong(1, jobId);
                pstmt.setString(2, oldValue);
                pstmt.setString(3, newValue);
                pstmt.executeUpdate();
            }
        }
    }

    private void changeTargetDnLocalition(Map<Pair<String, Integer>, Pair<String, Integer>> changeMap, Long jobId)
        throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);

        String sql = META_DB_HINT + " select task_id from ddl_engine_task where job_id = ?";
        List<Long> taskIds = new ArrayList<>();
        try (PreparedStatement pstmt = tddlConnection.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    taskIds.add(rs.getLong("task_id"));
                }
            }
        }

// 使用task_id查询Physical_backfill_objects表
        if (!taskIds.isEmpty()) {
            for (Long taskId : taskIds) {
                StringBuilder physicalBackfillSql = new StringBuilder(META_DB_HINT);
                physicalBackfillSql.append(" select detail_info from physical_backfill_objects where job_id = ?");
                // detail_info {"sourceHostAndPort":{"key":"11.167.35.44","value":31306},"targetHostAndPorts":[{"key":"11.167.35.45","value":31306}]}

                try (PreparedStatement pstmt = tddlConnection.prepareStatement(physicalBackfillSql.toString())) {
                    pstmt.setLong(1, taskId);
                    try (ResultSet physicalBackfillRs = pstmt.executeQuery()) {
                        // 处理查询结果
                        if (physicalBackfillRs.next()) {
                            // 根据需要处理physical_backfill_objects表的数据
                            String detailInfo = physicalBackfillRs.getString("detail_info");
                            // 解析JSON并修改targetHostAndPorts中的一个元素
                            String modifiedDetailInfo = modifyTargetHostAndPorts(detailInfo, changeMap);
                            // 更新修改后的detail_info回数据库
                            updateDetailInfoInDatabase(modifiedDetailInfo, taskId);
                        }
                    }
                }
            }
        }
    }

    /**
     * 将修改后的detail_info更新回physical_backfill_objects表
     *
     * @param modifiedDetailInfo 修改后的detail_info
     * @param taskId 任务ID
     */
    private void updateDetailInfoInDatabase(String modifiedDetailInfo, Long taskId) throws SQLException {
        // 构建更新SQL
        StringBuilder updateSql = new StringBuilder(META_DB_HINT);
        updateSql.append("UPDATE physical_backfill_objects SET detail_info = ? WHERE job_id =? ");

        // 执行更新
        try (PreparedStatement pstmt = tddlConnection.prepareStatement(updateSql.toString())) {
            pstmt.setString(1, modifiedDetailInfo);
            pstmt.setLong(2, taskId);
            pstmt.executeUpdate();
        }
    }

    /**
     * 解析JSON并修改targetHostAndPorts中的一个元素
     *
     * @param detailInfo 原始的detail_info JSON字符串
     * @return 修改后的detail_info JSON字符串
     */
    private String modifyTargetHostAndPorts(String detailInfo,
                                            Map<Pair<String, Integer>, Pair<String, Integer>> changeMap) {
        // 解析JSON字符串
        JSONObject jsonObject = JSON.parseObject(detailInfo);

        // 获取targetHostAndPorts数组
        JSONArray targetHostAndPorts = jsonObject.getJSONArray("targetHostAndPorts");

        // 修改targetHostAndPorts中的第一个元素（可以根据需要修改其他元素）
        if (targetHostAndPorts != null && !targetHostAndPorts.isEmpty()) {
            JSONObject targetHost = targetHostAndPorts.getJSONObject(0);
            // 修改value值，例如增加1000
            int originalValue = targetHost.getInteger("value");
            String originalKey = targetHost.getString("key");
            targetHost.put("value", 10086);
            targetHost.put("key", "192.168.99.99");
            changeMap.put(Pair.of(originalKey, originalValue),
                Pair.of(targetHost.getString("key"), targetHost.getInteger("value")));
            // 更新targetHostAndPorts数组
            targetHostAndPorts.set(0, targetHost);
            jsonObject.put("targetHostAndPorts", targetHostAndPorts);
        }

        // 返回修改后的JSON字符串
        return jsonObject.toJSONString();
    }

    private void movePartition() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        Map<String, List<String>> storageAndPartitions = showTopologyByStorage(tddlConnection, TABLE_NAME1);
        if (storageAndPartitions.size() <= 1) {
            return;
        }
        List<String> storages = new ArrayList<>();
        List<List<String>> partitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : storageAndPartitions.entrySet()) {
            String storage = entry.getKey();
            storages.add(storage);
            partitions.add(entry.getValue());
        }
        try (Statement stmt = tddlConnection.createStatement()) {
            for (int i = 0; i < storages.size(); i++) {
                int j = ((i == storages.size() - 1) ? 0 : i + 1);
                String movePgSql =
                    String.format(
                        "%s alter tablegroup by table " + TABLE_NAME1
                            + " move subpartitions %s to '%s'",
                        ENABLE_PHYSICAL_BACKFILL_HINT, String.join(",", partitions.get(i)), storages.get(j));
                System.out.println(movePgSql);
                stmt.executeUpdate(movePgSql);
                Assert.fail("expect move partition group failed, but not");
            }
        } catch (SQLException e) {
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

        sql = sql + KEY_KEY_PART;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            if (isMySQL80()) {
                stmt.executeQuery("set opt_index_format_gpp_enabled=false;");
                stmt.executeQuery("set opt_index_format_panda_enabled=false;");
            }
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME2, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME3, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME4, TABLE_NAME1));
        }
    }

    private static void initData(int count) {
        String sql = "INSERT INTO %s "
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        int log2N = (int) Math.ceil(Math.log(count) / Math.log(2));
        String insertSelect = "INSERT INTO " + TABLE_NAME1
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col,"
            + " varchar_col, binary_col, varbinary_col, date_col, datetime_col, "
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
                        JdbcUtil.useDb(connection, DB_NAME);
                        PreparedStatement pstmt = connection.prepareStatement(String.format(sql, tableName));
                        pstmt.setByte(1, (byte) (Math.random() * 256));
                        pstmt.setShort(2, (short) (Math.random() * Short.MAX_VALUE));
                        pstmt.setInt(3, ((int) (Math.random() * Integer.MAX_VALUE)) & 0x007FFFFF);
                        pstmt.setInt(4, i);
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

    private void cleanPending(String obj, boolean expectSuccess, String errorMsg) {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        List<Map<String, String>> fullDDL = showFullDDL();
        Optional<Map<String, String>> jobOp = fullDDL.stream()
            .filter(m -> m.get("OBJECT_NAME").equals(obj))
            .findFirst();
        if (!jobOp.isPresent()) {
            return;
        }
        if (expectSuccess) {
            jobOp.ifPresent(stringStringMap -> JdbcUtil
                .executeUpdateSuccess(tddlConnection, "rollback ddl " + stringStringMap.get("JOB_ID")));
        } else {
            jobOp.ifPresent(stringStringMap -> JdbcUtil
                .executeUpdateFailed(tddlConnection, "rollback ddl " + stringStringMap.get("JOB_ID"), errorMsg));
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

    public Map<String, List<String>> showTopologyByStorage(Connection conn, String tbName) {
        Map<String, List<String>> storageAndPartitions = new HashMap<>();
        String sql = "show topology " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            while (rs.next()) {
                String partitionName = rs.getString("PARTITION_NAME");
                String subpartitionName = rs.getString("SUBPARTITION_NAME");
                String storageId = rs.getString("DN_ID");
                if (StringUtils.isEmpty(subpartitionName)) {
                    storageAndPartitions.computeIfAbsent(storageId, k -> new ArrayList<>()).add(partitionName);
                } else {
                    storageAndPartitions.computeIfAbsent(storageId, k -> new ArrayList<>()).add(subpartitionName);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return storageAndPartitions;
    }

    private void checkXDataSourceExistence(boolean expectExist, Pair<String, Integer> hostIpAndPort) {
        try {
            Thread.sleep(5000);
        } catch (Exception ex) {
            //
        }
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            String host = hostIpAndPort.getKey() + ":" + hostIpAndPort.getValue();
            String sql = "select dn,REF_COUNT from information_schema.dn_perf";
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
            boolean actualExist = false;
            try {
                while (rs.next()) {
                    String dn = rs.getString("dn").toLowerCase();
                    Long refCount = rs.getLong("REF_COUNT");
                    if (dn.indexOf(host.toLowerCase()) != -1 && refCount > 0) {
                        actualExist = true;
                        break;
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            } finally {
                JdbcUtil.close(rs);
            }
            Assert.assertTrue("expect XDataSource of host:" + host + (expectExist ? "exists" : "not exists"),
                expectExist == actualExist);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
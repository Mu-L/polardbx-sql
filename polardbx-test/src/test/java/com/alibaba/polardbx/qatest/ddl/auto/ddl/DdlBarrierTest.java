package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockAccessor;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class DdlBarrierTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME1 = "TABLE_NAME1_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME2 = "TABLE_NAME2_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME3 = "TABLE_NAME3_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME4 = "TABLE_NAME4_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME5 = "TABLE_NAME5_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME6 = "TABLE_NAME6_" + RandomUtils.getStringBetween(4, 6);
    private static final String DB_NAME_ = "DdlBarrierTest";
    static String DB_NAME = DB_NAME_;
    final static AtomicBoolean forceStop = new AtomicBoolean(false);
    final static String DEFAULT_TG_NAME = "mytg123";
    final static String KEY_RANG_PARTITION_DEF =
        "partition by key(id) partitions 3 subpartition by range(b1) (subpartition sp1 values less than(10), subpartition sp2 values less than(20))";
    final static String RANG_RANG_PARTITION_DEF =
        "partition by range(id) subpartition by range(b1) (subpartition sp1 values less than(10), subpartition sp2 values less than(20)) (partition p1 values less than(10), partition p2 values less than(20)) ";
    final static String RANG_RANG_NOT_TEMP_PARTITION_DEF =
        "partition by range(id) subpartition by range(b1) (partition p1 values less than(10) (subpartition p1sp1 values less than(10),subpartition p1sp2 values less than(20)), partition p2 values less than(20) (subpartition p2sp1 values less\n"
            + "than(10),subpartition p2sp2 values less than(20)))";
    final static String KEY_2_COL =
        "partition by key(b1,id) partitions 3";
    final static String CREATE_TABLE_TEMPLATE =
        "create table %s (id int auto_increment,a int,b1 int, primary key(id)) %s;";
    final static String RANG_LIST_PARTITION_DEF =
        "PARTITION BY RANGE(id) SUBPARTITION BY LIST(b1) (   PARTITION p1 VALUES LESS THAN (10)     ( SUBPARTITION sp1 VALUES IN (10),       SUBPARTITION sp2 VALUES IN (20) ),   PARTITION p2 VALUES LESS THAN (20)     ( SUBPARTITION sp3 VALUES IN (10),       SUBPARTITION sp4 VALUES IN (20) ) )";
    final static String RANG__DEF =
        "PARTITION BY RANGE(id) (   PARTITION p1 VALUES LESS THAN (10) ,   PARTITION p2 VALUES LESS THAN (20))";
    final static String AYSNC_AND_SUSPEND_HINT =
        "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=TRUE, fp_random_suspend='100,1000')*/";
    private final boolean forceDownGradeRWForTableGroup;
    /*+TDDL:cmd_extra(fp_random_suspend='100,1000')*/

    public DdlBarrierTest(boolean forceDownGradeRWForTableGroup) {
        this.forceDownGradeRWForTableGroup = forceDownGradeRWForTableGroup;
    }

    @Parameterized.Parameters(name = "{index}: forceDownGradeRWForTableGroup={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(
            new Object[][] {
                {false},
                {true}
            }
        );
    }

    @Before
    public void setUp() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            DB_NAME = DB_NAME_ + RandomUtils.getStringBetween(4, 6);
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
        }
    }

    @After
    public void tearDown() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void testConcurrentCreateTable() throws SQLException {
        concurrentCreateTable(false);
    }

    @Test
    public void testConcurrentCreateTableWithTg() throws SQLException {
        concurrentCreateTable(true);
    }

    @Test
    public void movePartitionGroupConcurrently() {
        if (forceDownGradeRWForTableGroup) {
            return;
        }
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            String sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME1, KEY_RANG_PARTITION_DEF);
            stmt.executeUpdate(sql1);
            sql1 = "create global index g10 on " + TABLE_NAME1 + "(a) partition by key(a) partitions 2";
            stmt.executeUpdate(sql1);
            sql1 = "create global index g9 on " + TABLE_NAME1 + "(id,b1) " + KEY_RANG_PARTITION_DEF;
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME2, KEY_RANG_PARTITION_DEF);
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME3, KEY_RANG_PARTITION_DEF);
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME4, KEY_RANG_PARTITION_DEF);
            stmt.executeUpdate(sql1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        JdbcUtil.useDb(tddlConnection, DB_NAME);

        String refresh = "refresh topology";
        JdbcUtil.executeSuccess(tddlConnection, refresh);
        String hint = forceDownGradeRWForTableGroup ?
            "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='DELETE_ONLY', FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true)*/" :
            "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='DELETE_ONLY')*/ ";

        String partStr = "subpartitions";
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
                        "%s alter tablegroup by table " + TABLE_NAME1 + " move " + partStr + " %s to '%s' async=true",
                        hint, String.join(",", partitions.get(i)), storages.get(j));
                System.out.println(movePgSql);
                if (i == 0 || forceDownGradeRWForTableGroup) {
                    stmt.executeUpdate(movePgSql);
                } else {
                    JdbcUtil.executeUpdateFailed(tddlConnection, movePgSql, "ERR_PAUSED_DDL_JOB_EXISTS");
                }
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        Set<String> jobIds = new HashSet<>();
        List<Map<String, String>> fullDDL = showFullDDL();
        for (Map<String, String> map : fullDDL) {
            jobIds.add(map.get("JOB_ID"));
        }
        try (Connection metadbConn = getMetaConnection()) {
            ReadWriteLockAccessor readWriteLockAccessor = new ReadWriteLockAccessor();
            readWriteLockAccessor.setConnection(metadbConn);
            /*
            int tsl_count = (forceDownGradeRWForTableGroup ? 0 : 1);
            String lastResource = null;
            for (String jobId : jobIds) {
                String owner = "DDL_" + jobId;
                List<ReadWriteLockRecord> records = readWriteLockAccessor.query(owner);
                for (ReadWriteLockRecord record : GeneralUtil.emptyIfNull(records)) {

                    if (!record.type.startsWith(PersistentReadWriteLock.TYPE_SHARE_LOCK)) {
                        continue;
                    }
                    tsl_count++;
                    Assert.assertTrue(record.type.endsWith(PersistentReadWriteLock.MOVE_PARTITION_GROUP_TYPE));
                    if (lastResource == null) {
                        lastResource = record.resource;
                    } else {
                        Assert.assertTrue(lastResource.equals(record.resource));
                    }
                }
            }
            Assert.assertEquals(jobIds.size(), tsl_count);
            */
            String sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME6, KEY_RANG_PARTITION_DEF);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter table " + TABLE_NAME2 + " add column d1 int";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter table " + TABLE_NAME2 + " partition by key(id) partitions 2";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter table " + TABLE_NAME2 + " drop column a";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter table " + TABLE_NAME2 + " modify column a bigint";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "create global index g1 on " + TABLE_NAME2 + "(a) partition by key(a) partitions 2";
            if (!forceDownGradeRWForTableGroup) {
                JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
                sql1 = "create index index2 on " + TABLE_NAME2 + "(a)";
                JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");

                sql1 = "alter table " + TABLE_NAME1 + " drop index g10";
                JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            }
            sql1 = "alter table " + TABLE_NAME1 + " drop index g9";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter tablegroup by table " + TABLE_NAME1 + " split partition p1";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter tablegroup by table " + TABLE_NAME1 + " merge partitions p1,p2 to p12";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 =
                "alter tablegroup by table " + TABLE_NAME1 + " add subpartition(subpartition sp3 values less than(30))";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
            sql1 = "alter table " + TABLE_NAME2 + " drop subpartition sp1";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "ERR_PAUSED_DDL_JOB_EXISTS");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void explainDDL() {
        if (forceDownGradeRWForTableGroup) {
            return;
        }
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            String sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME1, RANG_RANG_PARTITION_DEF);
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME2, RANG_RANG_NOT_TEMP_PARTITION_DEF);
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME3, KEY_2_COL);
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME4, RANG_LIST_PARTITION_DEF);
            stmt.executeUpdate(sql1);
            sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME5, RANG__DEF);
            stmt.executeUpdate(sql1);

            sql1 = "explain alter tablegroup by table " + TABLE_NAME1 + " drop partition p1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME1 + " drop partition p1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter tablegroup by table " + TABLE_NAME1 + " drop subpartition sp1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME1 + " drop subpartition sp1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter tablegroup by table " + TABLE_NAME2 + " drop partition p1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME2 + " drop partition p1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter tablegroup by table " + TABLE_NAME2 + " drop subpartition p1sp1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME2 + " drop subpartition p1sp1";
            stmt.executeUpdate(sql1);

            sql1 = "explain alter table " + TABLE_NAME1 + " truncate subpartition sp1";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter tablegroup by table " + TABLE_NAME1 + " truncate subpartition sp1";
            stmt.executeUpdate(sql1);

            sql1 = "explain alter tablegroup by table " + TABLE_NAME3 + " SPLIT INTO pp PARTITIONS 3 BY HOT VALUE(88)";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME3 + " SPLIT INTO pp PARTITIONS 3 BY HOT VALUE(88)";
            stmt.executeUpdate(sql1);

            sql1 = "explain alter tablegroup by table " + TABLE_NAME3 + " extract to partition hp by hot value(80)";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME3 + " extract to partition hp by hot value(80)";
            stmt.executeUpdate(sql1);

            sql1 = "explain alter tablegroup by table " + TABLE_NAME4
                + " modify subpartition sp1 add values(10001, 10002)";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME4 + " modify subpartition sp1 add values(10001, 10002)";
            stmt.executeUpdate(sql1);

            sql1 = "explain alter tablegroup by table " + TABLE_NAME5
                + " reorganize partition p2 into(partition p21 values less than (15), partition p22 values less than (20))";
            stmt.executeUpdate(sql1);
            sql1 = "explain alter table " + TABLE_NAME5
                + " reorganize partition p2 into(partition p21 values less than (15), partition p22 values less than (20))";
            stmt.executeUpdate(sql1);
            Map<String, List<String>> storageAndPartitions = showTopologyByStorage(connection, TABLE_NAME1);
            sql1 = "explain  /*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=false)*/ alter tablegroup by table " + TABLE_NAME1
                + " move partitions p1,p2 to '" + storageAndPartitions.keySet().iterator().next() + "'";
            stmt.executeUpdate(sql1);
            sql1 =
                "explain  /*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=false, ENABLE_MOVE_PARTITIONGROUP_CONCURRENTLY=true)*/ alter tablegroup by table "
                    + TABLE_NAME1
                    + " move partitions p1,p2 to '" + storageAndPartitions.keySet().iterator().next() + "'";
            stmt.executeUpdate(sql1);
            sql1 =
                "explain  /*+TDDL:CMD_EXTRA(ENABLE_MOVE_PARTITIONGROUP_CONCURRENTLY=true)*/ alter tablegroup by table "
                    + TABLE_NAME1
                    + " move partitions p1,p2 to '" + storageAndPartitions.keySet().iterator().next() + "'";
            stmt.executeUpdate(sql1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void concurrentCreateTable(boolean withTg) throws SQLException {
        Set<String> jobIds = new HashSet<>();
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            if (withTg) {
                stmt.executeUpdate("create tablegroup " + DEFAULT_TG_NAME);
            }
            String partition_def = KEY_RANG_PARTITION_DEF + (withTg ? " tablegroup=" + DEFAULT_TG_NAME : "");
            String sql1 = String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME1, partition_def);
            stmt.executeUpdate(sql1);
            Map<String, List<String>> storageAndPartitions = showTopologyByStorage(connection, TABLE_NAME1);
            sql1 = AYSNC_AND_SUSPEND_HINT + String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME2, partition_def);
            stmt.executeUpdate(sql1);
            sql1 = AYSNC_AND_SUSPEND_HINT + String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME3, partition_def);
            stmt.executeUpdate(sql1);
            sql1 = AYSNC_AND_SUSPEND_HINT + String.format(CREATE_TABLE_TEMPLATE, TABLE_NAME4, partition_def);
            stmt.executeUpdate(sql1);
            JdbcUtil.useDb(tddlConnection, DB_NAME);
            List<Map<String, String>> fullDDL = showFullDDL();

            for (Map<String, String> map : fullDDL) {
                jobIds.add(map.get("JOB_ID"));
            }
            Assert.assertTrue(jobIds.toString(), jobIds.size() == 3);
            /*
            String sql = META_DB_HINT + " select owner,resource,type from read_write_lock where schema_name = ?";
            String lastResource = null;
            int tsl_count = 0;
            try (PreparedStatement pstmt = tddlConnection.prepareStatement(sql)) {
                pstmt.setString(1, DB_NAME);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String type = rs.getString("type");
                        if (!type.startsWith(PersistentReadWriteLock.TYPE_SHARE_LOCK)) {
                            continue;
                        }
                        tsl_count++;
                        Assert.assertTrue(type.endsWith(PersistentReadWriteLock.CREATE_TABLE_TYPE));
                        String resource = rs.getString("resource");
                        if (lastResource == null) {
                            lastResource = resource;
                        } else {
                            Assert.assertTrue(lastResource.equals(resource));
                        }
                    }
                }
            }
            Assert.assertEquals(jobIds.size(), tsl_count);
            */
            if (storageAndPartitions.size() > 1) {
                List<String> storages = new ArrayList<>();
                List<List<String>> partitions = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : storageAndPartitions.entrySet()) {
                    String storage = entry.getKey();
                    storages.add(storage);
                    partitions.add(entry.getValue());
                }
                String hint = forceDownGradeRWForTableGroup ?
                    "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false, FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true)*/" :
                    "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false)*/";

                String movePgSql =
                    String.format(
                        "%s alter tablegroup by table " + TABLE_NAME1 + " move subpartitions %s to '%s'",
                        hint, String.join(",", partitions.get(0)),
                        storages.get(1));
                if (!forceDownGradeRWForTableGroup) {
                    JdbcUtil.executeUpdateFailed(tddlConnection, movePgSql, "TOO_OLD");
                }
                System.out.println(movePgSql);
            }

        }
    }

}

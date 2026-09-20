package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class ForbidModifyShardingKeyTest extends AutoCrudBasedLockTestCase {
    private final String forbidHint =
        "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=false)*/";
    private final String allowHint =
        "/*+TDDL:cmd_extra(ENABLE_MODIFY_SHARDING_COLUMN=true)*/";

    private Connection tddlConnection;
    private static final String TABLE_NAME = "forbid_modify_sharding_key_table";
    private static final String SECOND_TABLE_NAME = "forbid_modify_sharding_key_table_2";

    @Before
    public void setUp() throws Exception {
        tddlConnection = getPolardbxConnection();
        useDb(tddlConnection, polardbxOneDB);

        // 创建测试表
        String dropTableSql = "DROP TABLE IF EXISTS " + TABLE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);

        String createTableSql = "CREATE TABLE `" + TABLE_NAME + "` (\n" +
            "  `id` int(11) NOT NULL,\n" +
            "  `k1` int(11) DEFAULT NULL,\n" +
            "  `k2` int(11) DEFAULT NULL,\n" +
            "  `k3` int(11) DEFAULT NULL,\n" +
            "  `c` char(120) NOT NULL DEFAULT '',\n" +
            "  `pad` char(60) NOT NULL DEFAULT '',\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  GLOBAL INDEX `gsi` (`k2`) covering (`k1`,`k3`,`c`,`pad`)\n" +
            "  PARTITION BY KEY(`k2`)\n" +
            "  PARTITIONS 4\n" +
            ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
            "PARTITION BY KEY(`k1`)\n" +
            "PARTITIONS 4";

        JdbcUtil.executeUpdate(tddlConnection, createTableSql);

        // 创建第二个测试表
        String dropSecondTableSql = "DROP TABLE IF EXISTS " + SECOND_TABLE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, dropSecondTableSql);

        String createSecondTableSql = "CREATE TABLE `" + SECOND_TABLE_NAME + "` (\n" +
            "  `id` int(11) NOT NULL,\n" +
            "  `k1` int(11) DEFAULT NULL,\n" +
            "  `k2` int(11) DEFAULT NULL,\n" +
            "  `k3` int(11) DEFAULT NULL,\n" +
            "  `c` char(120) NOT NULL DEFAULT '',\n" +
            "  `pad` char(60) NOT NULL DEFAULT '',\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  GLOBAL INDEX `gsi2` (`k2`) covering (`k1`,`k3`,`c`,`pad`)\n" +
            "  PARTITION BY KEY(`k2`)\n" +
            "  PARTITIONS 4\n" +
            ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
            "PARTITION BY KEY(`k1`)\n" +
            "PARTITIONS 4";

        JdbcUtil.executeUpdate(tddlConnection, createSecondTableSql);

        // 插入测试数据
        String insertSql =
            "INSERT INTO " + TABLE_NAME + " (id, k1, k2, k3, c, pad) VALUES (1, 10, 20, 30, 'test', 'pad')";
        JdbcUtil.executeUpdate(tddlConnection, insertSql);

        // 插入第二个表的测试数据
        String insertSecondSql =
            "INSERT INTO " + SECOND_TABLE_NAME + " (id, k1, k2, k3, c, pad) VALUES (1, 10, 20, 30, 'test2', 'pad2')";
        JdbcUtil.executeUpdate(tddlConnection, insertSecondSql);
    }

    @After
    public void tearDown() throws SQLException {
        // 清理测试表
        String dropTableSql = "DROP TABLE IF EXISTS " + TABLE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, dropTableSql);

        // 清理第二个测试表
        String dropSecondTableSql = "DROP TABLE IF EXISTS " + SECOND_TABLE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, dropSecondTableSql);

        // 关闭连接
        if (tddlConnection != null && !tddlConnection.isClosed()) {
            tddlConnection.close();
        }
    }

    @Test
    public void testForbidModifyPrimaryTableShardingKey() {
        // 先允许修改分区键，确保更新成功
        String updateSql = " UPDATE " + TABLE_NAME + " SET k1 = 15 WHERE id = 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, allowHint + updateSql);
        // 测试修改主表分区键是否被禁止
        JdbcUtil.executeUpdateFailed(tddlConnection, forbidHint + updateSql, "ERR_MODIFY_SHARD_COLUMN");
    }

    @Test
    public void testForbidModifyGsiShardingKey() {
        // 先允许修改分区键，确保更新成功
        String updateSql = " UPDATE " + TABLE_NAME + " SET k2 = 25 WHERE id = 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, allowHint + updateSql);
        // 测试修改GSI分区键是否被禁止
        JdbcUtil.executeUpdateFailed(tddlConnection, forbidHint + updateSql, "ERR_MODIFY_SHARD_COLUMN");
    }

    @Test
    public void testForbidInsertOnDuplicateKeyModifyShardingKey() throws SQLException {
        // 先允许修改分区键，确保更新成功
        String insertSql = " INSERT INTO " + TABLE_NAME
            + " (id, k1, k2, k3, c, pad) VALUES (1, 15, 20, 30, 'test', 'pad') ON DUPLICATE KEY UPDATE k1 = 25";
        JdbcUtil.executeUpdateSuccess(tddlConnection, allowHint + insertSql);
        // 测试INSERT ON DUPLICATE KEY UPDATE修改分区键是否被禁止
        JdbcUtil.executeUpdateFailed(tddlConnection, forbidHint + insertSql, "ERR_MODIFY_SHARD_COLUMN");
    }

    @Test
    public void testForbidInsertOnDuplicateKeyModifyGsiShardingKey() throws SQLException {
        // 先允许修改分区键，确保更新成功
        String insertSql = " INSERT INTO " + TABLE_NAME
            + " (id, k1, k2, k3, c, pad) VALUES (1, 10, 25, 30, 'test', 'pad') ON DUPLICATE KEY UPDATE k2 = 35";
        JdbcUtil.executeUpdateSuccess(tddlConnection, allowHint + insertSql);
        // 测试INSERT ON DUPLICATE KEY UPDATE修改GSI分区键是否被禁止
        JdbcUtil.executeUpdateFailed(tddlConnection, forbidHint + insertSql, "ERR_MODIFY_SHARD_COLUMN");
    }

    @Test
    public void testAllowModifyNonShardingKey() throws SQLException {
        // 测试允许修改非分区键列
        String updateSql = forbidHint + " UPDATE " + TABLE_NAME + " SET k3 = 35, c = 'updated' WHERE id = 1";
        JdbcUtil.executeUpdate(tddlConnection, updateSql);

        // 验证更新是否成功
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT k3, c FROM " + TABLE_NAME + " WHERE id = 1");
        if (rs.next()) {
            assertEquals(35, rs.getInt("k3"));
            assertEquals("updated", rs.getString("c"));
        }
        rs.close();
        stmt.close();
    }

    @Test
    public void testForbidMultiTableUpdatePrimaryTableShardingKey() throws SQLException {
        // 多表不允许修改分区键，因此不加forbidHint也应该报错
        String updateSql = " UPDATE " + TABLE_NAME + " t1, " + SECOND_TABLE_NAME + " t2"
            + " SET t1.k1 = 15, t2.k1 = 25 WHERE t1.id = 1 AND t2.id = 1";
        JdbcUtil.executeUpdateFailed(tddlConnection, allowHint + updateSql, "ERR_MODIFY_SHARD_COLUMN");

        // 测试多表UPDATE修改主表分区键是否被禁止
        JdbcUtil.executeUpdateFailed(tddlConnection, forbidHint + updateSql, "ERR_MODIFY_SHARD_COLUMN");
    }

    @Test
    public void testForbidMultiTableUpdateGsiShardingKey() throws SQLException {
        // 多表不允许修改分区键，因此不加forbidHint也应该报错
        String updateSql = " UPDATE " + TABLE_NAME + " t1, " + SECOND_TABLE_NAME + " t2"
            + " SET t1.k2 = 25, t2.k2 = 35 WHERE t1.id = 1 AND t2.id = 1";
        JdbcUtil.executeUpdateFailed(tddlConnection, allowHint + updateSql, "ERR_MODIFY_SHARD_COLUMN");

        // 测试多表UPDATE修改GSI分区键是否被禁止
        JdbcUtil.executeUpdateFailed(tddlConnection, forbidHint + updateSql, "ERR_MODIFY_SHARD_COLUMN");
    }

    @Test
    public void testAllowMultiTableUpdateNonShardingKey() throws SQLException {
        // 测试允许多表UPDATE修改非分区键列
        String updateSql = forbidHint + " UPDATE " + TABLE_NAME + " t1, " + SECOND_TABLE_NAME + " t2"
            + " SET t1.k3 = 35, t2.k3 = 45, t1.c = 'updated1', t2.c = 'updated2' WHERE t1.id = 1 AND t2.id = 1";
        JdbcUtil.executeUpdate(tddlConnection, updateSql);

        // 验证第一个表的更新是否成功
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT k3, c FROM " + TABLE_NAME + " WHERE id = 1");
        if (rs.next()) {
            assertEquals(35, rs.getInt("k3"));
            assertEquals("updated1", rs.getString("c"));
        }
        rs.close();

        // 验证第二个表的更新是否成功
        rs = stmt.executeQuery("SELECT k3, c FROM " + SECOND_TABLE_NAME + " WHERE id = 1");
        if (rs.next()) {
            assertEquals(45, rs.getInt("k3"));
            assertEquals("updated2", rs.getString("c"));
        }
        rs.close();
        stmt.close();
    }
}
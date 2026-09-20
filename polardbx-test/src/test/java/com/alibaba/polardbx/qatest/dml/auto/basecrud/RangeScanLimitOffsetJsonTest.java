package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 复现: RangeScan + LIMIT offset 在 SELECT 含 JSON 列时漏跳一行的缺陷。
 * <p>
 * 触发链路:
 * 1. KEY(user_id) + RANGE(create_time) 二级分区, 查询固定 user_id + create_time 范围 + ORDER BY + LIMIT,
 * 优化器走 RangeScanSortExec (ENABLE_RANGE_SCAN 默认 true)。
 * 2. SELECT 含 JSON 列 -> 无对应 BlockDecoder -> X-Protocol 回退 Row Layout。
 * 3. RangeScanSortExec.fetchChunk() 的 skip 逻辑: 当 skipped 恰好减到 0 时不 continue, fall through 后
 * 再次读取 xResult.current() 拿到同一行 -> 被跳过的行又被输出。
 * <p>
 * 校验方式 (PolarDB-X 自洽, 不依赖 MySQL, 避开 JSON 格式化差异):
 * 以 "LIMIT 0, N" 全量有序结果为基线, 任意 "LIMIT k, m" 的 key 列必须等于 baseline.subList(k, k+m)。
 * 缺陷下 "LIMIT 1, m" 实际等于 "LIMIT 0, m", 与期望的 subList(1, 1+m) 不符。
 * <p>
 * 该表使用 KEY+RANGE 子分区, MySQL 原生不支持同样语法, 故仅在 PolarDB-X 上建表与校验。
 */
public class RangeScanLimitOffsetJsonTest extends AutoCrudBasedLockTestCase {

    private static final String TABLE_NAME = "tab_range_scan_limit_offset_json";

    private static final String CREATE_TABLE = "CREATE TABLE `" + TABLE_NAME + "` (\n"
        + "    `order_no` varchar(100) NOT NULL,\n"
        + "    `user_id` bigint NOT NULL,\n"
        + "    `create_time` bigint NOT NULL,\n"
        + "    `tenant_id` int NOT NULL,\n"
        + "    `reward_config` json DEFAULT NULL,\n"
        + "    PRIMARY KEY (`user_id`, `create_time`, `order_no`),\n"
        + "    KEY `auto_shard_key_createtime` USING BTREE (`create_time`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
        + "PARTITION BY KEY(`user_id`) PARTITIONS 4\n"
        + "SUBPARTITION BY RANGE(`create_time`)\n"
        + "(SUBPARTITION `sp1` VALUES LESS THAN (1777500000000),\n"
        + " SUBPARTITION `sp2` VALUES LESS THAN (1778000000000),\n"
        + " SUBPARTITION `sp3` VALUES LESS THAN (1778500000000),\n"
        + " SUBPARTITION `sp4` VALUES LESS THAN (1779000000000),\n"
        + " SUBPARTITION `sp5` VALUES LESS THAN (1779500000000),\n"
        + " SUBPARTITION `sp6` VALUES LESS THAN (1780000000000));";

    private static final long USER_ID = 30060000110511L;
    private static final int TENANT_ID = 3006;
    private static final long LOW = 1777292750901L;
    private static final long HIGH = 1779971150901L;

    /**
     * 12 行数据, create_time 跨越多个子分区 (产生多个物理 split, 触发 RangeScan 归并);
     * 含两组相同 create_time + 不同 order_no, 以校验 ORDER BY 第二排序键。
     */
    private static final long[] CREATE_TIMES = {
        1777300000000L, 1777600000000L, 1777600000000L, 1778100000000L,
        1778200000000L, 1778600000000L, 1778900000000L, 1779100000000L,
        1779100000000L, 1779300000000L, 1779600000000L, 1779800000000L
    };

    private static final String SELECT_TMPL =
        "%s SELECT tenant_id, create_time, order_no, user_id, reward_config\n"
            + "FROM `" + TABLE_NAME + "`\n"
            + "WHERE user_id = " + USER_ID + "\n"
            + "  AND create_time >= " + LOW + "\n"
            + "  AND create_time <= " + HIGH + "\n"
            + "  AND tenant_id = " + TENANT_ID + "\n"
            + "ORDER BY create_time DESC, order_no DESC\n"
            + "LIMIT %d, %d";

    private static final String RANGE_SCAN_ON = "/*+TDDL:CMD_EXTRA(ENABLE_RANGE_SCAN=true)*/";
    private static final String RANGE_SCAN_OFF = "/*+TDDL:CMD_EXTRA(ENABLE_RANGE_SCAN=false)*/";

    @Before
    public void prepare() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists `" + TABLE_NAME + "`");
        JdbcUtil.executeSuccess(tddlConnection, CREATE_TABLE);

        StringBuilder insert = new StringBuilder("INSERT INTO `" + TABLE_NAME
            + "` (order_no, user_id, create_time, tenant_id, reward_config) VALUES ");
        for (int i = 0; i < CREATE_TIMES.length; i++) {
            if (i > 0) {
                insert.append(",");
            }
            // order_no 用零填充保证字符串排序与插入顺序一致, 便于推理
            String orderNo = String.format("ON%04d", i);
            insert.append(String.format("('%s', %d, %d, %d, '{\"k\": %d}')",
                orderNo, USER_ID, CREATE_TIMES[i], TENANT_ID, i));
        }
        JdbcUtil.executeSuccess(tddlConnection, insert.toString());
    }

    @Test
    public void testRangeScanLimitOffsetWithJsonColumn() {
        // 基线: 全量有序结果 (offset=0)
        List<String> baseline = queryKeys(RANGE_SCAN_ON, 0, 100);
        Assert.assertTrue(
            "数据不足, 无法验证分页, 实际行数=" + baseline.size(),
            baseline.size() >= 10);

        final int limit = 5;

        // 核心断言: 任意 offset 的分页结果必须等于基线的对应子区间。
        // 缺陷下 offset=1 会漏跳第一行, 退化为 offset=0 的结果。
        for (int offset = 1; offset <= 5; offset++) {
            List<String> page = queryKeys(RANGE_SCAN_ON, offset, limit);
            List<String> expected = baseline.subList(offset, offset + limit);
            Assert.assertEquals(
                "RangeScan + JSON 列下 LIMIT " + offset + "," + limit + " 漏跳行 (缺陷)。"
                    + "\n期望(基线 subList): " + expected
                    + "\n实际(分页查询):   " + page,
                expected, page);
        }

        // 文档原始症状: LIMIT 1,5 不应与 LIMIT 0,5 返回相同首行。
        List<String> page0 = queryKeys(RANGE_SCAN_ON, 0, limit);
        List<String> page1 = queryKeys(RANGE_SCAN_ON, 1, limit);
        Assert.assertNotEquals(
            "LIMIT 1," + limit + " 的首行与 LIMIT 0," + limit + " 相同, 第一行未被跳过 (缺陷)",
            page0.get(0), page1.get(0));

        // 对照组: 关闭 RangeScan 走 TableScanSortExec, 分页应正确 (即绕过方法一)。
        for (int offset = 1; offset <= 5; offset++) {
            List<String> page = queryKeys(RANGE_SCAN_OFF, offset, limit);
            List<String> expected = baseline.subList(offset, offset + limit);
            Assert.assertEquals(
                "关闭 RangeScan 后 LIMIT " + offset + "," + limit + " 仍不正确",
                expected, page);
        }
    }

    /**
     * 执行分页查询, 返回每行的唯一 key (create_time|order_no)。
     * SELECT 中保留 reward_config (JSON) 以触发 Row Layout, 但仅比较 key 列, 规避 JSON 格式化差异。
     */
    private List<String> queryKeys(String hint, int offset, int limit) {
        String sql = String.format(SELECT_TMPL, hint, offset, limit);
        List<String> keys = new ArrayList<>();
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuery(sql, tddlConnection);
            while (rs.next()) {
                // 列顺序: tenant_id(1), create_time(2), order_no(3), user_id(4), reward_config(5)
                keys.add(rs.getString(2) + "|" + rs.getString(3));
            }
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        } finally {
            JdbcUtil.close(rs);
        }
        return keys;
    }

    @After
    public void clean() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists `" + TABLE_NAME + "`");
    }
}

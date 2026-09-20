package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 验证 ENABLE_OMC_30_MODIFY_PARTITION_KEY 开关：
 * 开关开启后，分区键 / GSI 分区键 / GSI 覆盖列的类型修改不再被强制 fallback
 * 到 OMC 2.0，而是直接走 OMC 3.0 路径。
 * 用例统一使用字符串扩长（varchar(10) → varchar(20)）作为类型变更场景。
 * <p>
 * 注意：
 * 1. 表带有 GSI，主表分区键也属于 existsInGsi（隐式 PK + 分区键），
 * 需要 ALLOW_ALTER_GSI_INDIRECTLY=true 才能通过 buildAlterTableJob 中的 GSI 守卫；
 * 2. 开关 ON 时不再强制 OMC 路径，需要 SQL 显式 `algorithm = omc` 让普通路径选择 OMC30，
 * 否则会被选成 INPLACE/INSTANT；
 * 3. 5.7 DN 不支持 changeset 协议，supportOmc30() 默认不成立，需要 FORCE_USING_OMC_30=true
 * 强制走 OMC 3.0；8.0 加该参数行为不变；
 * 4. 在 EXPLAIN ONLINE_DDL 语句中，hint 必须放在 `explain online_ddl` 之后、`alter` 之前
 * （附在内层 DDL 上）才能透传至 ec.paramManager。放在 SQL 最前面的 hint 会被当作
 * EXPLAIN 自身的 hint，不会传到内层 alter 的处理上下文。
 */
public class ExplainOnlineDDLOmc30PartitionKeyTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_omc30_pk";
    String localIndexName = "idx_b";
    String globalIndexName = "gsi_b";

    /**
     * 开启 OMC 3.0 修改分区键开关的 hint（同时放行 GSI 间接修改守卫，以及 5.7 强制 OMC30）
     */
    String hintOn =
        "/*+TDDL:CMD_EXTRA(ALLOW_ALTER_GSI_INDIRECTLY=true,FORCE_USING_OMC_30=true,"
            + "ENABLE_OMC_30_MODIFY_PARTITION_KEY=true)*/";

    /**
     * 关闭 OMC 3.0 修改分区键开关的 hint（仅放行 GSI 间接修改守卫，作为 baseline）
     */
    String hintBaseline = "/*+TDDL:CMD_EXTRA(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    @Before
    public void prepare() {
        // 主表分区键 a 与 GSI 分区键 b 均为 varchar(10)，便于做字符串扩长用例
        String createTableSql = String.format(
            "create table %s ("
                + "a varchar(10),"
                + "b varchar(10),"
                + "c int,"
                + "d varchar(20),"
                + "e int,"
                + "index %s(b),"
                + "global index %s(b) covering(c, d) partition by key(b) partitions 3"
                + ") partition by key(a) partitions 3",
            tableName, localIndexName, globalIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
    }

    @After
    public void clean() {
        String dropTableSql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    // -------- 对照基线：开关 OFF（不带 ENABLE_OMC_30_MODIFY_PARTITION_KEY），强制走 OMC 2.0 --------

    @Test
    public void testDefaultOmc20PrimaryPartitionColumn() {
        String sql = String.format(
            "explain online_ddl %salter table %s modify column a varchar(20)", hintBaseline, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC20);
    }

    @Test
    public void testDefaultOmc20GsiPartitionColumn() {
        String sql = String.format(
            "explain online_ddl %salter table %s modify column b varchar(20)", hintBaseline, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC20);
    }

    // -------- 开关 ON（hint）：分区键 / GSI 分区键 / GSI 覆盖列改类型走 OMC 3.0 --------
    // 开关 ON 后 OMC 不再强制，需 SQL 显式 algorithm=omc 让普通路径选 OMC30。

    @Test
    public void testOmc30PrimaryPartitionColumn() {
        String sql = String.format(
            "explain online_ddl %salter table %s modify column a varchar(20), algorithm = omc",
            hintOn, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC30);
    }

    @Test
    public void testOmc30GsiPartitionColumn() {
        String sql = String.format(
            "explain online_ddl %salter table %s modify column b varchar(20), algorithm = omc",
            hintOn, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC30);
    }

    @Test
    public void testOmc30GsiCoveringColumn() {
        // d 是 GSI 的 covering 列，原本会被 L2507 强制走 OMC 2.0；方案 A 同步放行
        String sql = String.format(
            "explain online_ddl %salter table %s modify column d varchar(40), algorithm = omc",
            hintOn, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC30);
    }

    // -------- 通过 CHANGE COLUMN 同名形式同样适用（路径同 MODIFY） --------

    @Test
    public void testOmc30ChangeColumnSameNamePrimaryPk() {
        String sql = String.format(
            "explain online_ddl %salter table %s change column a a varchar(20), algorithm = omc",
            hintOn, tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC30);
    }

    // -------- 路由正确性验证：执行真实 OMC30 alter，校验路由稳定性 --------
    //
    // 路由稳定性的本质：alter 前/后用相同的「分区键值」做等值查询，CN 路由计算应得到相同的
    // 物理分片，且能查到原数据。这要求经过 CN 路由路径（partition pruning）才有意义，
    // 直接 `select * from t partition(pX)` 只是跳过路由读物理表，无法证明路由没坏。
    //
    // 这里采用三类断言相互印证：
    //   1) 主表分区键 a 等值查询：每个 key 在 alter 前后能查到、内容一致 → 主表路由稳定；
    //   2) GSI 分区键 b 等值查询（强制 force index 走 GSI）：alter 前后 GSI 路径仍能命中
    //      → GSI 路由 + GSI 物理数据稳定；
    //   3) partition(pX) 物理快照：alter 前后逐分区数据集合不变 → OMC 没有把数据错迁到
    //      其它分区（与等值查询互为印证：物理位置 + CN 路由两端都对得上才算路由正确）。

    @Test
    public void testOmc30RouteUnchangedPrimaryPartitionColumn() {
        verifyRouteUnchangedAfterAlter("modify column a varchar(20)");
    }

    @Test
    public void testOmc30RouteUnchangedGsiPartitionColumn() {
        verifyRouteUnchangedAfterAlter("modify column b varchar(20)");
    }

    @Test
    public void testOmc30RouteUnchangedGsiCoveringColumn() {
        verifyRouteUnchangedAfterAlter("modify column d varchar(40)");
    }

    /**
     * 通用流程：插入 6 行数据 → 记录 alter 前的「等值查询路由+物理快照」→ 执行真实 alter
     * （OMC30 路径）→ 用相同断言验证 alter 后路由不变。
     *
     * @param alterClause alter table 的 spec 部分（不含 algorithm 子句和分号）
     */
    private void verifyRouteUnchangedAfterAlter(String alterClause) {
        // 1) 插入 6 行覆盖 3 个 KEY 分区
        String insertSql = String.format(
            "insert into %s values"
                + "('k1','k1',1,'d1',1),"
                + "('k2','k2',2,'d2',2),"
                + "('k3','k3',3,'d3',3),"
                + "('k4','k4',4,'d4',4),"
                + "('k5','k5',5,'d5',5),"
                + "('k6','k6',6,'d6',6)", tableName);
        JdbcUtil.executeSuccess(tddlConnection, insertSql);

        String[] keys = {"k1", "k2", "k3", "k4", "k5", "k6"};
        String[] partitions = {"p1", "p2", "p3"};

        // 2) alter 前：路由路径快照（主表 a 等值 + GSI b 等值）+ 物理分区快照
        Map<String, String> primaryEqualBefore = fingerprintByEqualLookup("a", keys, false);
        Map<String, String> gsiEqualBefore = fingerprintByEqualLookup("b", keys, true);
        Map<String, List<String>> partitionBefore = new LinkedHashMap<>();
        int totalBefore = 0;
        for (String p : partitions) {
            List<String> rows = readPartitionRows(p);
            partitionBefore.put(p, rows);
            totalBefore += rows.size();
        }
        Assert.assertEquals("插入数据应全部命中 p1/p2/p3", 6, totalBefore);
        for (String k : keys) {
            Assert.assertNotEquals("alter 前主表等值查询应命中 " + k, "", primaryEqualBefore.get(k));
            Assert.assertNotEquals("alter 前 GSI 等值查询应命中 " + k, "", gsiEqualBefore.get(k));
        }

        // 3) 真实 alter（OMC30 路径）
        String alterSql = String.format(
            "%salter table %s %s, algorithm = omc", hintOn, tableName, alterClause);
        JdbcUtil.executeSuccess(tddlConnection, alterSql);

        // 4) alter 后：三类快照应与 alter 前完全一致
        Map<String, String> primaryEqualAfter = fingerprintByEqualLookup("a", keys, false);
        Map<String, String> gsiEqualAfter = fingerprintByEqualLookup("b", keys, true);
        for (String k : keys) {
            Assert.assertEquals(
                "alter [" + alterClause + "] 后主表分区键 a 路由错乱：key=" + k,
                primaryEqualBefore.get(k), primaryEqualAfter.get(k));
            Assert.assertEquals(
                "alter [" + alterClause + "] 后 GSI 分区键 b 路由错乱：key=" + k,
                gsiEqualBefore.get(k), gsiEqualAfter.get(k));
        }
        for (String p : partitions) {
            Assert.assertEquals(
                "alter [" + alterClause + "] 后分区 " + p + " 物理数据偏移",
                partitionBefore.get(p), readPartitionRows(p));
        }
    }

    /**
     * 对每个 key 执行「按列等值查询」（走 CN 路由 + 单分片下推），把命中行序列化为指纹。
     * 路由若被破坏，等值查询会落到错误的物理分片，结果指纹必然变化。
     *
     * @param column 等值查询使用的列（必须是主表/GSI 的分区键）
     * @param keys 待查询的 key 集合
     * @param forceGsi true 时通过 hint 强制走 GSI，验证 GSI 路由
     */
    private Map<String, String> fingerprintByEqualLookup(String column, String[] keys, boolean forceGsi) {
        String hint = forceGsi ? "/*+TDDL:INDEX(" + tableName + "," + globalIndexName + ")*/" : "";
        String sql = String.format(
            "%sselect a,b,c,d,e from %s where %s = ?", hint, tableName, column);
        Map<String, String> result = new LinkedHashMap<>();
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            for (String k : keys) {
                ps.setString(1, k);
                try (ResultSet rs = ps.executeQuery()) {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        if (sb.length() > 0) {
                            sb.append(";");
                        }
                        sb.append(rs.getString(1)).append("|").append(rs.getString(2)).append("|")
                            .append(rs.getInt(3)).append("|").append(rs.getString(4)).append("|")
                            .append(rs.getInt(5));
                    }
                    result.put(k, sb.toString());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("等值查询失败 column=" + column + " forceGsi=" + forceGsi, e);
        }
        return result;
    }

    /**
     * 读取指定分区上的全部行，按 (a,b,c) 排序，序列化为字符串列表，便于 equals 比较。
     * 用于检测物理数据是否被错迁到其它分区。
     */
    private List<String> readPartitionRows(String partition) {
        String sql = String.format(
            "select a,b,c,d,e from %s partition(%s) order by a,b,c", tableName, partition);
        List<String> rows = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(rs.getString(1) + "|" + rs.getString(2) + "|" + rs.getInt(3)
                    + "|" + rs.getString(4) + "|" + rs.getInt(5));
            }
        } catch (SQLException e) {
            throw new RuntimeException("读取分区 " + partition + " 失败", e);
        }
        return rows;
    }
}

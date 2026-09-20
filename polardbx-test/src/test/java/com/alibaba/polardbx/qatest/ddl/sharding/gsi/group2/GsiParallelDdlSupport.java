package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.collect.ImmutableSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * GSI 参数化测试类在 {@link ParallelGsiRunner} 下并发执行时共用的 DDL 安全防护:
 * 超时执行、先查存在再 DROP、遗留 DDL job 清理。
 * 行为与 GsiBackfillTypeTest 中的同名防护一致, 供多个类型测试类复用。
 */
public final class GsiParallelDdlSupport {

    /**
     * DDL 超时: 单条 DDL/清理语句最长等待 60 秒, 防止服务端锁等待(默认 1 小时)拖死整个测试类
     */
    public static final int DDL_QUERY_TIMEOUT_SECONDS = 60;

    /**
     * DDL 引擎 job 的终态集合, 处于终态的 job 无需清理
     */
    private static final ImmutableSet<String> DDL_JOB_TERMINAL_STATES =
        ImmutableSet.of("COMPLETED", "ROLLBACK_COMPLETED", "CANCELLED");

    /**
     * CANCEL 遗留 job 后等待其到达终态(或被引擎清理)的最长时间
     */
    private static final long CANCEL_WAIT_TIMEOUT_MILLIS = 15_000L;

    private static final Logger logger = LoggerFactory.getLogger(GsiParallelDdlSupport.class);

    private GsiParallelDdlSupport() {
    }

    public static void executeUpdateWithTimeout(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(DDL_QUERY_TIMEOUT_SECONDS);
            stmt.execute(sql);
        }
    }

    /**
     * 带超时的容错执行, 语义与 {@link com.alibaba.polardbx.qatest.util.JdbcUtil#executeUpdateSuccessIgnoreErr}
     * 对齐: 返回 true 表示命中预期错误集合, false 表示执行成功
     */
    public static boolean executeUpdateIgnoreErrWithTimeout(Connection conn, String sql,
                                                            ImmutableSet<String> errIgnored) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(DDL_QUERY_TIMEOUT_SECONDS);
            stmt.execute(sql);
        } catch (SQLException e) {
            for (String err : errIgnored) {
                if (e.getMessage().contains(err)) {
                    return true;
                }
            }
            throw e;
        }
        return false;
    }

    /**
     * 先查存在性再 DROP: CN 的 CdcDropTableIfExistsMarkTask 对不存在的表执行
     * DROP TABLE IF EXISTS 会抛 IndexOutOfBoundsException 并残留 PAUSED job,
     * 因此仅在表确实存在时才下发 DROP
     */
    public static void dropTableIfPresent(Connection conn, String table) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(DDL_QUERY_TIMEOUT_SECONDS);
            try (ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + table + "'")) {
                if (!rs.next()) {
                    return;
                }
            }
            stmt.execute("DROP TABLE " + table);
        }
    }

    /**
     * 清理指定表遗留的非终态 DDL job。若表存在残留的 backfill/锁等待 job,
     * 后续任何针对该表的 DDL 都会卡在其排他锁上, 这里前置将它们取消掉。
     * <p>
     * 直查 metadb.ddl_engine 而非 SHOW FULL DDL: 后者不返回 PAUSED/ROLLBACK_PAUSED
     * 状态的 job(实测盲区), 而旧版本 CN 的 CDC mark 缺陷会让 DROP job 停留在 PAUSED,
     * 其持有的 schema 级锁会拒绝同库后续 DDL(ERR_PAUSED_DDL_JOB_EXISTS)。
     * <p>
     * 表名按前缀匹配: 参数化测试类的表共享模板前缀, 前一用例遗留的 PAUSED job
     * 可能挂在别的方法变体表上, 同样会锁阻塞当前用例, 需要一并取消;
     * 无关表的 job 不在处理范围, 避免逐个 cancel 拖慢用例节奏。
     */
    public static void cancelLegacyDdlJobs(Connection tddlConnection, String schema, Set<String> tablePrefixes) {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.setQueryTimeout(DDL_QUERY_TIMEOUT_SECONDS);
            final String query = "SELECT job_id, object_name, state FROM metadb.ddl_engine "
                + "WHERE state NOT IN ('COMPLETED','ROLLBACK_COMPLETED','CANCELLED') AND schema_name = '" + schema
                + "'";
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    final String objectName = rs.getString("object_name");
                    if (!matchesAnyPrefix(objectName, tablePrefixes)) {
                        continue;
                    }
                    final long jobId = rs.getLong("job_id");
                    logger.warn("Cancel legacy ddl job before test, jobId=" + jobId + ", state=" + rs.getString(
                        "state") + ", object=" + objectName);
                    cancelAndAwaitTerminal(tddlConnection, jobId);
                }
            }
        } catch (Throwable t) {
            logger.warn("cancelLegacyDdlJobs failed, ignore and continue", t);
        }
    }

    private static boolean matchesAnyPrefix(String objectName, Set<String> tablePrefixes) {
        if (objectName == null) {
            return false;
        }
        for (String prefix : tablePrefixes) {
            if (objectName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * CANCEL 是异步指令, 轮询等待 job 到达终态或被引擎清理(实测 CANCELLED job 会从
     * ddl_engine 删除), 确保其持有的锁释放后再继续, 避免后续 DDL 撞上锁阻塞
     */
    private static void cancelAndAwaitTerminal(Connection tddlConnection, long jobId) {
        try {
            executeUpdateWithTimeout(tddlConnection, "CANCEL DDL " + jobId);
        } catch (Throwable cancelErr) {
            logger.warn("Cancel legacy ddl job failed, jobId=" + jobId, cancelErr);
        }
        final long deadline = System.currentTimeMillis() + CANCEL_WAIT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.setQueryTimeout(5);
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT state FROM metadb.ddl_engine WHERE job_id = " + jobId)) {
                    if (!rs.next() || DDL_JOB_TERMINAL_STATES.contains(rs.getString(1).toUpperCase())) {
                        return;
                    }
                }
            } catch (Throwable t) {
                return;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        logger.warn("Legacy ddl job not terminal after cancel wait, jobId=" + jobId);
    }
}

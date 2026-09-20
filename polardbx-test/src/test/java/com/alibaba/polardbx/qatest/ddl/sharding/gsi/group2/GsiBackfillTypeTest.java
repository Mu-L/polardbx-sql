/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIGINT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author chenmo.cm
 */

@RunWith(ParallelGsiRunner.class)
public class GsiBackfillTypeTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "full_gsi_primary";
    private static final ImmutableMap<String, List<String>> GSI_FULL_TYPE_TEST_INSERTS = GsiConstant
        .buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME);

    /**
     * DDL 超时: 单条 DDL/清理语句最长等待 60 秒, 防止服务端锁等待(默认 1 小时)拖死整个测试类
     */
    private static final int DDL_QUERY_TIMEOUT_SECONDS = 60;

    /**
     * DDL 引擎 job 的终态集合, 处于终态的 job 无需清理
     */
    private static final ImmutableSet<String> DDL_JOB_TERMINAL_STATES =
        ImmutableSet.of("COMPLETED", "ROLLBACK_COMPLETED", "CANCELLED");

    private static final String FULL_TYPE_TABLE = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        "");

    private String dataColumn = null;

    private boolean supportXA = false;

    // testName rule 复用基类 BaseTestCase#testName, 子类重复声明会遮蔽基类字段导致其失效

    public GsiBackfillTypeTest(String indexSk) {
        this.dataColumn = indexSk;
    }

    @Parameters(name = "{index}:indexSk={0}")
    public static List<String[]> prepareDate() {
        final List<String> columns = filterColumns(FULL_TYPE_TABLE_COLUMNS);
        return columns.stream().map(c -> new String[] {c}).collect(Collectors.toList());
    }

    /**
     * 支持 -Dgsi.test.columns=c_datetime;c_timestamp 只跑列子集(本地冒烟验证), 分号分隔、按子串匹配;
     * 不设置时全量执行, 保持线上行为不变。全类 276 用例约 3 小时, 无法在分钟级完成,
     * 快速回归时用该参数把范围缩小到重点类型。
     */
    private static List<String> filterColumns(List<String> allColumns) {
        final String filter = System.getProperty("gsi.test.columns", "").trim();
        if (filter.isEmpty()) {
            return allColumns;
        }
        final List<String> patterns = Arrays.stream(filter.split(";"))
            .map(String::trim).filter(p -> !p.isEmpty()).collect(Collectors.toList());
        final List<String> selected = allColumns.stream()
            .filter(c -> patterns.stream().anyMatch(c::contains))
            .collect(Collectors.toList());
        assertWithMessage("gsi.test.columns=" + filter + " matched no column").that(selected).isNotEmpty();
        return selected;
    }

    @Before
    public void before() throws SQLException {
        // JDBC handles zero-date differently in prepared statement and statement, so ignore this case in cursor fetch
        org.junit.Assume.assumeTrue(!PropertiesUtil.useCursorFetch());

        supportXA = JdbcUtil.supportXA(tddlConnection);

        // 每个用例(参数x方法)使用独立表名, 避免共享表导致的用例间锁级联
        final String primary = primaryTableName();
        final String indexBigInt = indexTableNameOf(C_BIGINT_64);
        final String indexDataColumn = indexTableNameOf(dataColumn);

        // 先清理本库遗留的非终态 DDL job, 避免后续 DROP TABLE 卡在服务端锁等待上
        cancelLegacyDdlJobs();

        // 所有 DDL 均带超时执行, 即使服务端挂死也最多阻塞 60 秒
        dropTableIfPresent(mysqlConnection, primary);
        executeUpdateWithTimeout(mysqlConnection, FULL_TYPE_TABLE_MYSQL.replace(PRIMARY_TABLE_NAME, primary));

        dropTableIfPresent(tddlConnection, primary);
        dropTableIfPresent(tddlConnection, indexBigInt);
        dropTableIfPresent(tddlConnection, indexDataColumn);
        executeUpdateWithTimeout(tddlConnection, FULL_TYPE_TABLE.replace(PRIMARY_TABLE_NAME, primary));
    }

    @After
    public void after() {
        // 尽力清理本用例的表; 若清理本身失败, 交给下一轮 before 的超时 DROP 与 job 清理兜底
        final String primary = primaryTableName();
        try {
            dropTableIfPresent(mysqlConnection, primary);
        } catch (Throwable ignored) {
        }
        try {
            dropTableIfPresent(tddlConnection, primary);
            dropTableIfPresent(tddlConnection, indexTableNameOf(C_BIGINT_64));
            dropTableIfPresent(tddlConnection, indexTableNameOf(dataColumn));
        } catch (Throwable ignored) {
        }
    }

    private void initData(List<String> inserts) throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Prepare data
        for (String insert : inserts) {
            gsiExecuteUpdate(tddlConnection, mysqlConnection, insert.replace(PRIMARY_TABLE_NAME, primaryTableName()),
                failedList, true, !C_BIT_64.equals(dataColumn));
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(p -> System.out.println(p.left));

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + primaryTableName(),
            tddlConnection);
        assertWithMessage("查询测试数据集大小失败").that(resultSet.next()).isTrue();
        assertWithMessage("测试数据集为空").that(resultSet.getLong(1)).isGreaterThan(0L);
    }

    private void gsiIntegrityCheck(String primary, String index) {
        final String columnList = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .collect(Collectors.joining(", "));

        final String columnList1 = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(C_ID))
            .collect(Collectors.joining(", "));

        gsiIntegrityCheck(primary, index, columnList, columnList1, !C_BIT_64.equals(this.dataColumn));
    }

    /**
     * 每个用例独立的主表名: fgsi_<参数列>_<方法缩写>
     */
    private String primaryTableName() {
        return "fgsi_" + methodSuffix();
    }

    /**
     * 每个用例独立的索引表名: g_i_<索引列>_<参数列>_<方法缩写>
     */
    private String indexTableNameOf(String columnName) {
        return MessageFormat.format("g_i_{0}_" + methodSuffix(), columnName);
    }

    private String methodSuffix() {
        return dataColumn + "_" + methodTag();
    }

    /**
     * 方法名缩写, 保证表名总长度不超过 MySQL 64 字符上限;
     * 参数化测试的方法名带 "[index:param]" 后缀, 因此用前缀/包含匹配
     */
    private String methodTag() {
        final String m = testName.getMethodName();
        if (m.startsWith("testCreateGsi")) {
            return m.contains("OnColumn") ? "coc" : "cd";
        }
        return m.contains("OnColumn") ? "aoc" : "ad";
    }

    private static void executeUpdateWithTimeout(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(DDL_QUERY_TIMEOUT_SECONDS);
            stmt.execute(sql);
        }
    }

    /**
     * 先查存在性再 DROP: CN 的 CdcDropTableIfExistsMarkTask 对不存在的表执行
     * DROP TABLE IF EXISTS 会抛 IndexOutOfBoundsException 并残留 PAUSED job,
     * 因此仅在表确实存在时才下发 DROP
     */
    private static void dropTableIfPresent(Connection conn, String table) throws SQLException {
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
     * 带超时的容错执行, 语义与 {@link JdbcUtil#executeUpdateSuccessIgnoreErr} 对齐:
     * 返回 true 表示命中预期错误集合, false 表示执行成功
     */
    private boolean executeUpdateIgnoreErrWithTimeout(String sql, ImmutableSet<String> errIgnored)
        throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
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
     * 清理本用例相关表遗留的非终态 DDL job。若表存在残留的 backfill/锁等待 job,
     * 后续任何针对该表的 DDL 都会卡在其排他锁上, 这里前置将它们取消掉;
     * 无关表的 job 不在本用例处理范围, 避免逐个 cancel 拖慢用例节奏。
     */
    private void cancelLegacyDdlJobs() {
        final ImmutableSet<String> myTables = ImmutableSet.of(primaryTableName(),
            indexTableNameOf(C_BIGINT_64), indexTableNameOf(dataColumn));
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.setQueryTimeout(DDL_QUERY_TIMEOUT_SECONDS);
            try (ResultSet rs = stmt.executeQuery("SHOW FULL DDL")) {
                while (rs.next()) {
                    final String state = rs.getString("STATE");
                    if (state == null || DDL_JOB_TERMINAL_STATES.contains(state.toUpperCase())) {
                        continue;
                    }
                    final String schema = rs.getString("OBJECT_SCHEMA");
                    if (tddlDatabase1 == null || !tddlDatabase1.equalsIgnoreCase(schema)) {
                        continue;
                    }
                    if (!myTables.contains(rs.getString("OBJECT_NAME"))) {
                        continue;
                    }
                    final long jobId = rs.getLong("JOB_ID");
                    logger.warn("Cancel legacy ddl job before test, jobId=" + jobId + ", state=" + state
                        + ", object=" + rs.getString("OBJECT_NAME"));
                    try {
                        executeUpdateWithTimeout(tddlConnection, "CANCEL DDL " + jobId);
                    } catch (Throwable cancelErr) {
                        logger.warn("Cancel legacy ddl job failed, jobId=" + jobId, cancelErr);
                    }
                }
            }
        } catch (Throwable t) {
            logger.warn("cancelLegacyDdlJobs failed, ignore and continue", t);
        }
    }

    @Test
    public void testCreateGsiDefault() throws SQLException {
        final String indexSk = C_BIGINT_64;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = primaryTableName();
        final String index = indexTableNameOf(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        executeUpdateWithTimeout(tddlConnection, GSI_ALLOW_ADD_HINT + createGsi);

        gsiIntegrityCheck(primary, index);
    }

    @Test
    public void testCreateGsiOnColumn() throws SQLException {
        final String indexSk = dataColumn;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = primaryTableName();
        final String index = indexTableNameOf(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.partitioning(indexSk);

        final String indexSkWithLen = indexSk.contains("c_text") ? indexSk + "(63)" : indexSk;
        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSkWithLen, covering, partitioning);

        /**
         * 某些列不允许作为拆分键, 因此索引表只能使用默认拆分键
         */
        if (!executeUpdateIgnoreErrWithTimeout(GSI_ALLOW_ADD_HINT + createGsi,
            ImmutableSet.of("Rule generator dataType is not supported!",
                "Invalid type for a sharding key",
                "Unsupported index table structure"))) {
            gsiIntegrityCheck(primary, index);
        }
    }

    @Test
    public void testAddGsiDefault() throws SQLException {
        final String indexSk = C_BIGINT_64;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = primaryTableName();
        final String index = indexTableNameOf(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String addGsi = GsiConstant.getAddGsi(primary, index, indexSk, covering, partitioning);

        executeUpdateWithTimeout(tddlConnection, GSI_ALLOW_ADD_HINT + addGsi);

        gsiIntegrityCheck(primary, index);
    }

    @Test
    public void testAddGsiOnColumn() throws SQLException {
        final String indexSk = dataColumn;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = primaryTableName();
        final String index = indexTableNameOf(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.partitioning(indexSk);

        final String indexSkWithLen = indexSk.contains("c_text") ? indexSk + "(63)" : indexSk;
        final String addGsi = GsiConstant.getAddGsi(primary, index, indexSkWithLen, covering, partitioning);

        /**
         * 某些列不允许作为拆分键, 因此索引表只能使用默认拆分键
         */
        if (!executeUpdateIgnoreErrWithTimeout(GSI_ALLOW_ADD_HINT + addGsi,
            ImmutableSet.of("Rule generator dataType is not supported!",
                "Invalid type for a sharding key",
                "Unsupported index table structure"))) {
            gsiIntegrityCheck(primary, index);
        }
    }
}

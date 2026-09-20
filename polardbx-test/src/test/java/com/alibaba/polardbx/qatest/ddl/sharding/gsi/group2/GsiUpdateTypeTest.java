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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BINARY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_LONG;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_MEDIUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BLOB_TINY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DATETIME_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_DECIMAL;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ENUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_GEOMETORY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_JSON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_LINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTILINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SET;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_LONG;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_MEDIUM;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TEXT_TINY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIME_6;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.dateType;
import static com.alibaba.polardbx.qatest.constant.TableConstant.floatType;
import static com.alibaba.polardbx.qatest.constant.TableConstant.timeType;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.google.common.truth.Truth.assertWithMessage;

@RunWith(ParallelGsiRunner.class)
public class GsiUpdateTypeTest extends DDLBaseNewDBTestCase {

    /**
     * 模板表名, 仅用于静态初始化 SQL 模板; 运行时按用例替换为 {@link #PRIMARY_TABLE_NAME}
     */
    private static final String PRIMARY_TABLE_TEMPLATE = "gsi_update_type_test_primary";
    private static final String INDEX_TABLE_TEMPLATE = "gsi_update_type_test_gsi";
    private static final ImmutableMap<String, List<String>> GSI_FULL_TYPE_TEST_INSERTS =
        GsiConstant.buildGsiFullTypeTestInserts(PRIMARY_TABLE_TEMPLATE);
    private static final String UPSERT_INIT_DATA_TEMPLATE =
        "insert into " + PRIMARY_TABLE_TEMPLATE + "(`id`) values (1)";
    private static final String DELETE_DATA_TEMPLATE = "delete from " + PRIMARY_TABLE_TEMPLATE;
    private static final String LOCAL_UK_FULL_SCAN_HINT =
        "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ ";

    private static final String FULL_TYPE_TABLE =
        ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_TEMPLATE, DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL =
        ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_TEMPLATE, "");

    private static final Set<String> CN_UNSUPPORTED_FUNC_TYPE = new HashSet<>();
    private static final Set<String> UK_WITH_LENGTH_TYPE = new HashSet<>();

    static {
        // CN does not support geo functions like ST_PointFromText
        CN_UNSUPPORTED_FUNC_TYPE.add(C_GEOMETORY);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_POINT);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_LINESTRING);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_POLYGON);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_MULTIPOINT);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_MULTILINESTRING);
        CN_UNSUPPORTED_FUNC_TYPE.add(C_MULTIPOLYGON);

        UK_WITH_LENGTH_TYPE.add(C_TEXT_TINY);
        UK_WITH_LENGTH_TYPE.add(C_TEXT);
        UK_WITH_LENGTH_TYPE.add(C_TEXT_MEDIUM);
        UK_WITH_LENGTH_TYPE.add(C_TEXT_LONG);
        UK_WITH_LENGTH_TYPE.add(C_BLOB_TINY);
        UK_WITH_LENGTH_TYPE.add(C_BLOB);
        UK_WITH_LENGTH_TYPE.add(C_BLOB_MEDIUM);
        UK_WITH_LENGTH_TYPE.add(C_BLOB_LONG);
    }

    private String dataColumn = null;

    /**
     * 每用例(参数x方法)独立的表名, initTables() 按当前方法名延迟赋值, 避免共享表导致并发互踩
     */
    private String PRIMARY_TABLE_NAME;
    private String INDEX_TABLE_NAME;
    private String UPSERT_INIT_DATA;
    private String DELETE_DATA;

    public GsiUpdateTypeTest(String indexSk) {
        this.dataColumn = indexSk;
    }

    @Parameters(name = "{index}:indexSk={0}")
    public static List<String[]> prepareDate() {
        final List<String> columns = filterColumns(FULL_TYPE_TABLE_COLUMNS);
        return columns.stream().map(c -> new String[] {c}).collect(Collectors.toList());
    }

    /**
     * 支持 -Dgsi.test.columns=c_datetime;c_timestamp 只跑列子集(本地冒烟验证), 分号分隔、按子串匹配;
     * 不设置时全量执行, 保持线上行为不变。
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

    public void initTables() throws SQLException {
        // JDBC handles zero-date differently in prepared statement and statement, so ignore this case in cursor fetch
        org.junit.Assume.assumeFalse(
            PropertiesUtil.useCursorFetch() && (dataColumn.contains("time") || dataColumn.contains("year")
                || dataColumn.contains("date")));
        // just do not test primary key
        org.junit.Assume.assumeFalse(dataColumn.equalsIgnoreCase(C_ID));
        // out of range for BIT_64 in JDBC
        org.junit.Assume.assumeFalse(!useXproto() && dataColumn.equalsIgnoreCase(C_BIT_64));

        // 每个用例(参数x方法)使用独立表名, 避免共享表导致的用例间锁级联
        PRIMARY_TABLE_NAME = PRIMARY_TABLE_TEMPLATE + "_" + dataColumn + "_" + methodTag();
        INDEX_TABLE_NAME = INDEX_TABLE_TEMPLATE + "_" + dataColumn + "_" + methodTag();
        // 保持模板原文, 由 initData/clearData 在运行时统一 replace, 避免二次替换叠加后缀
        UPSERT_INIT_DATA = UPSERT_INIT_DATA_TEMPLATE;
        DELETE_DATA = DELETE_DATA_TEMPLATE;

        // 先清理本类表遗留的非终态 DDL job, 按模板前缀匹配覆盖所有方法变体表,
        // 避免遗留 job 的 schema 级锁阻塞后续 DDL
        GsiParallelDdlSupport.cancelLegacyDdlJobs(tddlConnection, tddlDatabase1,
            ImmutableSet.of(PRIMARY_TABLE_TEMPLATE + "_", INDEX_TABLE_TEMPLATE + "_"));

        // 所有 DDL 均带超时执行, 即使服务端挂死也最多阻塞 60 秒
        GsiParallelDdlSupport.dropTableIfPresent(mysqlConnection, PRIMARY_TABLE_NAME);
        GsiParallelDdlSupport.executeUpdateWithTimeout(mysqlConnection,
            FULL_TYPE_TABLE_MYSQL.replace(PRIMARY_TABLE_TEMPLATE, PRIMARY_TABLE_NAME));

        GsiParallelDdlSupport.dropTableIfPresent(tddlConnection, PRIMARY_TABLE_NAME);
        GsiParallelDdlSupport.executeUpdateWithTimeout(tddlConnection,
            FULL_TYPE_TABLE.replace(PRIMARY_TABLE_TEMPLATE, PRIMARY_TABLE_NAME));
    }

    @After
    public void after() {
        // 尽力清理本用例的表; 若清理本身失败, 交给下一轮 before 的超时 DROP 与 job 清理兜底
        if (PRIMARY_TABLE_NAME == null) {
            return;
        }
        try {
            GsiParallelDdlSupport.dropTableIfPresent(mysqlConnection, PRIMARY_TABLE_NAME);
        } catch (Throwable ignored) {
        }
        try {
            GsiParallelDdlSupport.dropTableIfPresent(tddlConnection, PRIMARY_TABLE_NAME);
            GsiParallelDdlSupport.dropTableIfPresent(tddlConnection, INDEX_TABLE_NAME);
        } catch (Throwable ignored) {
        }
    }

    /**
     * 方法名缩写, 保证表名总长度不超过 MySQL 64 字符上限;
     * 参数化测试的方法名带 "[index:param]" 后缀, 因此用前缀匹配
     */
    private String methodTag() {
        final String m = testName.getMethodName();
        if (m.startsWith("testPushDownDML")) {
            return "pd";
        }
        if (m.startsWith("testLogicalDMLWithGsiUk")) {
            return "luk";
        }
        if (m.startsWith("testLogicalUpdateWithGsiSK")) {
            return "lsk";
        }
        return "ld";
    }

    private void initData(List<String> inserts) throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Prepare data
        for (String insert : inserts) {
            gsiExecuteUpdate(tddlConnection, mysqlConnection, insert.replace(PRIMARY_TABLE_TEMPLATE,
                PRIMARY_TABLE_NAME), failedList, true, true);
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(p -> System.out.println(p.left));

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + PRIMARY_TABLE_NAME, tddlConnection);
        assertWithMessage("查询测试数据集大小失败").that(resultSet.next()).isTrue();
        assertWithMessage("测试数据集为空").that(resultSet.getLong(1)).isGreaterThan(0L);
    }

    private void clearData() throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Delete data
        gsiExecuteUpdate(tddlConnection, mysqlConnection, DELETE_DATA.replace(PRIMARY_TABLE_TEMPLATE,
            PRIMARY_TABLE_NAME), failedList, true, true);

        System.out.println("Failed delete: ");
        failedList.forEach(p -> System.out.println(p.left));
    }

    /*
     * Update / Upsert / Replace 下推执行
     */
    @Test
    public void testPushDownDML() throws SQLException {
        initTables();

        // Update
        clearData();
        initData(GSI_FULL_TYPE_TEST_INSERTS.get(C_ID));
        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));
        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
        }

        // Upsert
        clearData();
        initData(ImmutableList.of(UPSERT_INIT_DATA));

        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}={2}",
                PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
        }

        // Replace
        clearData();
        initData(ImmutableList.of(UPSERT_INIT_DATA));

        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update =
                MessageFormat.format("REPLACE INTO {0}(`id`,{1}) VALUES (1,{2})", PRIMARY_TABLE_NAME, dataColumn,
                    value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
        }
    }

    /*
     * Update / Upsert / Replace 逻辑执行
     */
    @Test
    public void testLogicalDMLWithGsi() throws SQLException {
        initTables();

        // Update
        clearData();
        initData(GSI_FULL_TYPE_TEST_INSERTS.get(C_ID));

        // Create a GSI to use logical execution
        String covering = MessageFormat.format("COVERING (`{0}`)", dataColumn);
        String createGsi = MessageFormat.format(
            "CREATE GLOBAL INDEX {0} ON {1}(`id`) {2} DBPARTITION BY HASH(`id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 7",
            INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, covering);
        GsiParallelDdlSupport.executeUpdateWithTimeout(tddlConnection, createGsi);

        // Update
        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));
        for (String value : values) {
            // Update with value pushdown
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

            // Update without value pushdown
            if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
                failedList = new ArrayList<>();
                update = MessageFormat.format("/*+TDDL:ENABLE_PUSH_PROJECT=false*/ UPDATE {0} SET {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }

            // Update with DN select value
            update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, dataColumn);
            if (dataColumn.contains(C_ENUM) && value.contains("0")) {
                // 插入非法 enum 值的时候，在 c_enum=c_enum 时，读出来的是 ""，update 的时候会报 Data truncated
                JdbcUtil.executeUpdateFailed(tddlConnection, update, "Data truncated for column 'c_enum'");
            } else {
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }
        }

        // Upsert
        if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
            for (String value : values) {
                clearData();
                initData(ImmutableList.of(UPSERT_INIT_DATA));

                // set from specified value
                List<Pair<String, Exception>> failedList = new ArrayList<>();
                String update = MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from insert value
                update =
                    MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}=values({2})",
                        PRIMARY_TABLE_NAME, dataColumn, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from select value
                update = MessageFormat.format("INSERT INTO {0}(`id`) VALUES (1) ON DUPLICATE KEY UPDATE {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }
        }

        // Replace
        for (String value : values) {
            clearData();
            initData(ImmutableList.of(UPSERT_INIT_DATA));

            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update =
                MessageFormat.format("REPLACE INTO {0}(`id`,{1}) VALUES (1,{2})", PRIMARY_TABLE_NAME, dataColumn,
                    value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
        }
    }

    /*
     * Update SK
     */
    @Test
    public void testLogicalUpdateWithGsiSK() throws SQLException {
        if (dataColumn.equalsIgnoreCase(C_BINARY)) {
            // In relocation, DELETE WRITER will use old 0x31310000000000000000 and INSERT writer will use '11', which
            // are in different shards
            return;
        }

        initTables();

        clearData();
        initData(GSI_FULL_TYPE_TEST_INSERTS.get(C_ID));

        // Create a GSI on data column to test modifying sharding key
        String createGsi;

        if (dateType.contains(dataColumn)) {
            createGsi = MessageFormat.format(
                "CREATE GLOBAL INDEX {0} ON {1}({2}) DBPARTITION BY YYYYMM({3}) TBPARTITION BY YYYYMM({4}) TBPARTITIONS 7",
                INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn, dataColumn, dataColumn);
        } else {
            createGsi = MessageFormat.format(
                "CREATE GLOBAL INDEX {0} ON {1}({2}) DBPARTITION BY HASH({3}) TBPARTITION BY HASH({4}) TBPARTITIONS 7",
                INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn, dataColumn, dataColumn);
        }
        boolean unsupportedSkType = GsiParallelDdlSupport.executeUpdateIgnoreErrWithTimeout(tddlConnection, createGsi,
            ImmutableSet.of("Rule generator dataType is not supported!", "Invalid type for a sharding key"));

        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));
        for (String value : values) {
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            // Update with value pushdown
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            if (!unsupportedSkType) {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            } else {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
            }

            // Update without value pushdown
            if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
                update = MessageFormat.format("/*+TDDL:ENABLE_PUSH_PROJECT=false*/ UPDATE {0} SET {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                if (!unsupportedSkType) {
                    gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
                } else {
                    gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
                }
            }

            // Update with DN select value
            update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, dataColumn);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            if (!unsupportedSkType) {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            } else {
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, PRIMARY_TABLE_NAME, dataColumn);
            }
        }
    }

    /*
     * Upsert / Replace 逻辑执行，含有 UK
     */
    @Test
    public void testLogicalDMLWithGsiUk() throws SQLException {
        if (CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {
            // This case needs to eval value in CN, so if data contains unsupported function, it will fail this case
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_JSON)) {
            // DN do not support JSON to be unique key
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_SET)) {
            // SELECT used for SET must be in specified order, otherwise it will not select duplicated value
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_BINARY)) {
            // SELECT used for BINARY must be in hex string, otherwise it will not select duplicated value
            return;
        }

        if (dateType.contains(dataColumn) && !(dataColumn.equalsIgnoreCase(C_TIMESTAMP_6)
            || dataColumn.equalsIgnoreCase(C_DATETIME_6))) {
            // Some data of date type will be truncated after insert, so it will not select duplicated value
            return;
        }

        if (timeType.contains(dataColumn) && !(dataColumn.equalsIgnoreCase(C_TIME_6))) {
            // Some data of date type will be truncated after insert, so it will not select duplicated value
            return;
        }

        if (floatType.contains(dataColumn)) {
            // Some data of float type will be truncated after insert, so it will not select duplicated value
            return;
        }

        if (dataColumn.equalsIgnoreCase(C_DECIMAL)) {
            // Some data of decimal type will be truncated after insert, so it will not select duplicated value
            return;
        }

        initTables();

        // Create a GSI to use logical execution
        String covering = MessageFormat.format("COVERING (`{0}`)", dataColumn);
        String createGsi = MessageFormat.format(
            "CREATE GLOBAL INDEX {0} ON {1}(`id`) {2} DBPARTITION BY HASH(`id`) TBPARTITION BY HASH(`id`) TBPARTITIONS 7",
            INDEX_TABLE_NAME, PRIMARY_TABLE_NAME, covering);
        GsiParallelDdlSupport.executeUpdateWithTimeout(tddlConnection, createGsi);

        // Create a local unique index
        String localUk = "local_uk";
        String ukLength = UK_WITH_LENGTH_TYPE.contains(dataColumn) ? "(60)" : "";

        String createUk =
            MessageFormat.format("CREATE LOCAL UNIQUE INDEX {0} on {1}({2}{3})", localUk, PRIMARY_TABLE_NAME,
                dataColumn, ukLength);
        GsiParallelDdlSupport.executeUpdateWithTimeout(tddlConnection, createUk);
        GsiParallelDdlSupport.executeUpdateWithTimeout(mysqlConnection, createUk.replace("LOCAL", ""));

        // Update
        List<String> values = new ArrayList<>(GsiConstant.FULL_TYPE_TEST_VALUES.get(dataColumn));

        // Upsert
        if (!CN_UNSUPPORTED_FUNC_TYPE.contains(dataColumn)) {

            for (String value : values) {
                // ignore bad convert on enum
                if (dataColumn.contains(C_ENUM) && value.contains("0")) {
                    // 在处理 duplicate 的时候，依赖查询，因为 enum 拆入非法值会变成 ""，导致查询的时候查不到，从而使 update 变成了
                    // insert，同时由于 id 自增，且按 id 拆分，又会以非法值插入一条新数据，导致和 MySQL 行为不一致
                    continue;
                }

                clearData();
                initData(ImmutableList.of(UPSERT_INIT_DATA));

                // init data
                List<Pair<String, Exception>> failedList = new ArrayList<>();
                String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true,
                    false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from specified value
                failedList = new ArrayList<>();
                update = MessageFormat.format("INSERT INTO {0}(`{1}`) VALUES ({2}) ON DUPLICATE KEY UPDATE {1}={2}",
                    PRIMARY_TABLE_NAME, dataColumn, value);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update,
                    "TRACE " + LOCAL_UK_FULL_SCAN_HINT + update, failedList, true, true, false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from insert value
                update =
                    MessageFormat.format("INSERT INTO {0}(`{1}`) VALUES ({2}) ON DUPLICATE KEY UPDATE {1}=values({3})",
                        PRIMARY_TABLE_NAME, dataColumn, value, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update,
                    "TRACE " + LOCAL_UK_FULL_SCAN_HINT + update, failedList, true, true, false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

                // set from select value
                update = MessageFormat.format("INSERT INTO {0}(`{1}`) VALUES ({2}) ON DUPLICATE KEY UPDATE {1}={3}",
                    PRIMARY_TABLE_NAME, dataColumn, value, dataColumn);
                gsiExecuteUpdate(tddlConnection, mysqlConnection, update,
                    "TRACE " + LOCAL_UK_FULL_SCAN_HINT + update, failedList, true, true, false);
                gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
            }
        }

        // Replace
        for (String value : values) {
            // ignore bad convert on enum
            if (dataColumn.contains(C_ENUM) && value.contains("0")) {
                // 在处理 duplicate 的时候，依赖查询，因为 enum 拆入非法值会变成 ""，导致查询的时候查不到，从而使 update 变成了
                // insert，同时由于 id 自增，且按 id 拆分，又会以非法值插入一条新数据，导致和 MySQL 行为不一致
                continue;
            }

            clearData();
            initData(ImmutableList.of(UPSERT_INIT_DATA));

            // init data
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            String update = MessageFormat.format("UPDATE {0} SET {1}={2}", PRIMARY_TABLE_NAME, dataColumn, value);
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, "TRACE " + update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);

            failedList = new ArrayList<>();
            update =
                MessageFormat.format("REPLACE INTO {0}(`id`,{1}) VALUES (NULL,{2})", PRIMARY_TABLE_NAME, dataColumn,
                    value);
            // 这里和mysql对比校验固化了local索引的全局唯一性，加hint防止走returning路径
            update = LOCAL_UK_FULL_SCAN_HINT + update;
            gsiExecuteUpdate(tddlConnection, mysqlConnection, update, update, failedList, true, true, false);
            gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, dataColumn);
        }
    }

    private void gsiIntegrityCheck(String primary, String index, String dataColumn) throws SQLException {
        if (primary.equalsIgnoreCase(index)) {
            return;
        }
        gsiIntegrityCheck(primary, index, dataColumn, dataColumn, true);
        checkGsi(tddlConnection, index);
    }
}

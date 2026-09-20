package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;

public class OuterJoinPartitionPruningTest extends ColumnarReadBaseTestCase {
    private static final String TABLE_1 = "test_outer_join_pruning_1";
    private static final String TABLE_2 = "test_outer_join_pruning_2";

    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 int) ";

    private static final String PARTITION_INFO = "partition by key(c1)";

    private static final String DISABLE_BROADCAST_JOIN = "/*+TDDL:cmd_extra(PARTITION_WISE_THRESHOLD=4 ENABLE_PARTITION_WISE_GROUP_OPT=false ENABLE_BROADCAST_JOIN=false)*/";

    private static final String TEMPLATE_SQL =
        DISABLE_BROADCAST_JOIN + "select * from %s a %s join %s b on a.%s = b.%s %s";

    private String table1;

    private String table2;

    private String joinType;

    private String joinColumn;

    private String filter;

    public OuterJoinPartitionPruningTest(Object table1, Object table2, Object joinType, Object joinColumn,
                                         Object filter) {
        this.table1 = (String) table1;
        this.table2 = (String) table2;
        this.joinType = (String) joinType;
        this.joinColumn = (String) joinColumn;
        this.filter = (String) filter;
    }

    @Before
    public void useDb() {
        JdbcUtil.useDb(tddlConnection, PropertiesUtil.polardbXAutoDBName1());
        JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
    }

    @Test
    public void checkPairWiseJoin() {
        String sql = String.format(TEMPLATE_SQL, table1, joinType, table2, joinColumn, joinColumn, filter);
        if (!table1.equals(table2)) {
            // plan is partition wise join
            String explain = JdbcUtil.getExplainResult(tddlConnection, sql).toLowerCase();
            // explain should contains partition exchange and remote pairwise
            Assert.assertTrue(explain.contains("distribution=hash") && explain.contains("remote")
                && StringUtils.countMatches(explain, "exchange") == 1, "but was " + explain);
        }

        // result is equal
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Parameterized.Parameters(name = "{index}:{0},{1},{2},{3},{4}")
    public static List<Object[]> getParameters() {
        List<Object[]> params = new ArrayList<>();
        String[] condition1List = new String[] {
            "",
            "where b.c1 = 1",
            "where b.c1 = 0",
            "where b.c1 = 10",
            "where b.c1 in (1, 2)",
        };
        String[] condition2List = new String[] {
            "",
            "where b.c2 = 1",
            "where b.c2 = 0",
            "where b.c2 = 10",
            "where b.c2 in (1, 2)"
        };
        for (Object joinType : joinTypes()) {
            for (String condition1 : condition1List) {
                params.add(new Object[] { TABLE_1, TABLE_2, joinType, "c2", condition1 });
            }
            for (String condition2 : condition2List) {
                params.add(new Object[] { TABLE_1, TABLE_2, joinType, "c1", condition2 });
            }
        }
        return params;
    }

    public static Object[] joinTypes() {
        return new Object[] {
            "inner",
            "left",
            "right"
        };
    }

    @BeforeClass
    public static void prepare() throws SQLException {
        dropTables();
        prepareData();
    }

    @AfterClass
    public static void dropTables() throws SQLException {
        try (Connection connection = getPolardbxConnection0(PropertiesUtil.polardbXAutoDBName1())) {
            JdbcUtil.dropTable(connection, TABLE_1);
            JdbcUtil.dropTable(connection, TABLE_2);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.dropTable(mysqlConnection, TABLE_1);
            JdbcUtil.dropTable(mysqlConnection, TABLE_2);
        }
    }

    private static void prepareData() throws SQLException {
        try (Connection connection = getPolardbxConnection0(PropertiesUtil.polardbXAutoDBName1())) {
            JdbcUtil.executeSuccess(connection, String.format(CREATE_TABLE + PARTITION_INFO, TABLE_1));
            JdbcUtil.executeSuccess(connection, String.format(CREATE_TABLE + PARTITION_INFO, TABLE_2));
            JdbcUtil.executeSuccess(connection,
                String.format("insert into %s values (1, 1), (2,2), (3,3), (4,4)", TABLE_1));
            JdbcUtil.executeSuccess(connection, String.format("insert into %s values (1, 1)", TABLE_2));
            ColumnarUtils.createColumnarIndex(connection, "col_" + TABLE_1, TABLE_1, "c1", "c1", 4);
            ColumnarUtils.createColumnarIndex(connection, "col_" + TABLE_2, TABLE_2, "c2", "c2", 4);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, TABLE_1));
            JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, TABLE_2));
            JdbcUtil.executeSuccess(mysqlConnection,
                String.format("insert into %s values (1, 1), (2,2), (3,3), (4,4)", TABLE_1));
            JdbcUtil.executeSuccess(mysqlConnection, String.format("insert into %s values (1, 1)", TABLE_2));
        }
    }
}

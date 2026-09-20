package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.validation.constraints.AssertTrue;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class CheckPhysicalTableTest extends DDLBaseNewDBTestCase {

    private final String testTableName = "check_physical_table";

    private String tableName1 = schemaPrefix + testTableName + "_1";

    public CheckPhysicalTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}});
    }

    @Before
    public void before() throws SQLException {
        dropTableIfExists(tableName1);
    }

    @Test
    public void testCheckPhysicalTable() {
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "check physical table " + tableName1;
        String fullTableName = getDdlSchema() + "." + tableName1;
        ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql1);
        List<List<Object>> results = JdbcUtil.getAllResult(resultSet);
        Assert.assertEquals(1, results.size());
        List<Object> result = results.get(0);
        Assert.assertTrue(result.get(0).toString().equalsIgnoreCase(fullTableName));
    }
}

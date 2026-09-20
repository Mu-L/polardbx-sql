
package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.locality.DbConfig;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.collect.Lists;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DryRunAlterTableTest2 extends DryRunAlterTableTest {

    //    List<String> tableNames = Lists.newArrayList("create_table", "drop_table", "add_column", "drop_column", "change_column", "modify_column"
//        , "rename_table", "truncate_table", "alter_column_default", "add_index", "drop_index");

    @Override
    public String getDbName() {
        String dbName = "dry_run_auto_db2";
        return dbName;
    }

    List<String> storageInsts = new ArrayList<>();

    public DryRunAlterTableTest2(boolean schema) {
        super(schema);
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
        storageInsts = getStorageInstIds(getDdlSchema());
        Collections.reverse(storageInsts);
        String dbLocalitySpec = getDbLocalitySpec(storageInsts, getDbName());
        String createDatabaseSql =
            String.format(
                "/*+TDDL:cmd_extra(BLOCK_LOGICAL_DDL=false)*/ create database if not exists %s mode = auto locality = '%s'",
                getDbName(), dbLocalitySpec);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createDatabaseSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + getDbName());
        validateDbLocation(getDbName(), dbLocalitySpec);
    }
}
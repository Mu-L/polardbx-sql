package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * created by ziyang.lb
 **/
public class CdcGdnSqlIdTest extends BaseTestCase {

    private static final String CREATE_SQL = "CREATE TABLE `%s`(a int not null, b int not null, primary key(a))";
    private static final String DROP_IF_EXISTS_SQL = "DROP TABLE IF EXISTS `%s`";

    private static final String ALTER_TABLE_SQL = "ALTER TABLE `%s` MODIFY COLUMN `b` bigint";

    private static final String TABLE_NAME = "cdc_gdn_sql_id";

    private static final String ERR_MSG =
        "this ddl sql id %d is before cdc ddl load checkpoint %d, maybe repeatedly applied!";

    @Test
    public void testGdnSqlId() throws SQLException {
        String createSql = String.format(CREATE_SQL, TABLE_NAME);
        String alterTableSql = String.format(ALTER_TABLE_SQL, TABLE_NAME);
        String dropIfExistsSql = String.format(DROP_IF_EXISTS_SQL, TABLE_NAME);
        try (Connection connection = getPolardbxConnection("drds_polarx1_part_qatest_app")) {
            JdbcUtil.executeUpdateSuccess(connection, dropIfExistsSql);
            JdbcUtil.executeUpdateSuccess(connection, createSql);
            updateDdlLoadCheckpoint(0L);
            // sqlId null, checkpont null;
            JdbcUtil.executeUpdateSuccess(connection, alterTableSql);

            updateDdlLoadCheckpoint(100L);

            // sqlId null, checkpoint 100
            JdbcUtil.executeUpdateSuccess(connection, alterTableSql);
            // sqlId 100, checkpoint 100
            String hint1 = "/*+TDDL:cmd_extra(ASYNC_LOAD_GDN_DDL_SQL_ID=100)*/";
            String errMsg1 = String.format(ERR_MSG, 100, 100);
            JdbcUtil.executeUpdateFailed(connection, hint1 + alterTableSql, errMsg1);

//            // sqlId 101, checkpoint 100 => checkpint 101
//            String hint2 = "/*+TDDL:cmd_extra(ASYNC_LOAD_GDN_DDL_SQL_ID=101)*/";
//            JdbcUtil.executeUpdateSuccess(connection, hint2 + alterTableSql);
//
//            // sqlId 100, checkpoint 101
//            String errMsg2 = String.format(ERR_MSG, 100, 101);
//            JdbcUtil.executeUpdateFailed(connection, hint1 + alterTableSql, errMsg2);

            // sqlId null, checkpoint 101
            JdbcUtil.executeUpdateSuccess(connection, alterTableSql);

            // set checkpoint null.
            updateDdlLoadCheckpoint(0L);
        }
    }

    private void updateDdlLoadCheckpoint(long checkPoint) throws SQLException {
        try (Connection connection = getMetaConnection()) {
            MetaDbUtil.upsertDdlLoadCheckPoint(connection, checkPoint);
        }
    }
}

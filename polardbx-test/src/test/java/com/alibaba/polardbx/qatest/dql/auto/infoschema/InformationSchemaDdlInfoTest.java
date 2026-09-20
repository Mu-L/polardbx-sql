package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static com.google.common.truth.Truth.assertWithMessage;

public class InformationSchemaDdlInfoTest extends DDLBaseNewDBTestCase {
    private static final String DDL_INFO =
        "select job_id from information_schema.ddl_info where job_id=%s";
    private static final String DDL_INFO_ALL =
        "select count(1) from information_schema.ddl_info";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testInformationSchemaDdlInfo() throws SQLException {
        String createSql = "create table test_ddl_info(a int, b int)";
        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s)*/", jobId);
        JdbcUtil.executeSuccess(tddlConnection, myHint + createSql);

        checkDdlInfo(jobId, String.format(DDL_INFO, jobId), tddlConnection);
    }

    @Test
    public void testInformationSchemaDdlInfo2() {
        ResultSet rs = JdbcUtil.executeQuery(DDL_INFO_ALL, tddlConnection);
        List<List<Object>> result = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(!result.isEmpty());
    }

    private void checkDdlInfo(Long jobId, String sql, Connection connection) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        boolean found =
            JdbcUtil.getAllResult(rs).stream()
                .anyMatch(x -> Objects.equals(jobId, Long.valueOf(String.valueOf(x.get(0)))));
        assertWithMessage("find ddl job " + jobId + " in 'information_schema.ddl_info'")
            .that(found).isTrue();
        rs.close();
    }
}

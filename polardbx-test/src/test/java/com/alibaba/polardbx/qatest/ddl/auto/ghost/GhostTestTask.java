package com.alibaba.polardbx.qatest.ddl.auto.ghost;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.qatest.BaseTestCase.isMySQL80;

public class GhostTestTask {
    private static final String hint = "/*+TDDL:datanode='%s'*/";
    public GhostTestBean ghostTestBean;

    public GhostTestTask(String fileDir) throws FileNotFoundException {
        this.ghostTestBean = readTestCase(fileDir);
    }

    public GhostTestBean readTestCase(String fileDir) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(fileDir);
        return yaml.loadAs(inputStream, GhostTestBean.class);
    }

    public void execute(Connection tddlConnection, Connection mysqlConnection, String dnId)
        throws InterruptedException, SQLException {
        System.out.println(ghostTestBean);

        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }

        try {
            // prepareDb
            for (String prepareDb : ghostTestBean.prepareDb) {
                JdbcUtil.executeSuccess(mysqlConnection, prepareDb);
            }

            // set sqlMode
            JdbcUtil.executeSuccess(mysqlConnection, "set sql_mode=''");

            // prepareTables
            for (String prepareTable : ghostTestBean.prepareTables) {
                JdbcUtil.executeSuccess(mysqlConnection, prepareTable);
            }

            // prepareEvents
            if (GeneralUtil.isNotEmpty(ghostTestBean.prepareEvents)) {
                for (String prepareEvent : ghostTestBean.prepareEvents) {
                    JdbcUtil.executeSuccess(mysqlConnection, prepareEvent);
                }
            }

            // sleep 1s
            Thread.sleep(1000);

            // set sqlMode
            if (ghostTestBean.sqlMode != null) {
                JdbcUtil.executeSuccess(tddlConnection, "set sql_mode='" + ghostTestBean.sqlMode + "'");
            }

            // alterTableCommand
            String alter = String.format(hint, dnId) + ghostTestBean.alterTableCommand + ",algorithm=omc";
            if (StringUtils.isEmpty(ghostTestBean.expectFailure)) {
                JdbcUtil.executeSuccess(tddlConnection, alter);
            } else {
                JdbcUtil.executeFailed(tddlConnection, alter, ghostTestBean.expectFailure);
            }

            // expectTableStructure
            if (GeneralUtil.isNotEmpty(ghostTestBean.expectTableStructure)) {
                String sql = "show create table " + ghostTestBean.dbName + "." + ghostTestBean.tableName;
                List<String> result = JdbcUtil.executeQueryAndGetColumnResult(sql, mysqlConnection, 2);
                System.out.println(result);
                String createTable = result.get(0);
                for (String expectTableStructure : ghostTestBean.expectTableStructure) {
                    Assert.assertTrue(createTable.contains(expectTableStructure));
                }
            }
        } finally {
            executeCleanupDDLs(mysqlConnection, ghostTestBean.cleanupDDLs);
        }
    }

    public void executeCleanupDDLs(Connection mysqlConnection, List<String> cleanupDDls) {
        for (String cleanupDDl : cleanupDDls) {
            JdbcUtil.executeSuccess(mysqlConnection, cleanupDDl);
        }
    }
}

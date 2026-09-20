package com.alibaba.polardbx.qatest.ddl.auto.omc30.omc30Utils;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Data;
import org.junit.Assert;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.qatest.BaseTestCase.isMySQL80;

public class Omc30TestTask {
    @Data
    public static class RejectDdl {
        public String ddl;
        public String message;
    }

    @Data
    public static class SuccessDdl {
        public String ddl;
        public String expectTableStructure;
    }

    @Data
    public static class Omc30TestBean {
        public String dbName;
        public String tableName;
        public List<String> prepareSqlList;
        public List<SuccessDdl> successDDLs;
        public List<RejectDdl> rejectDDLs;
        public List<String> cleanupDDLs;
    }

    public Omc30TestBean omc30TestBean;

    public Omc30TestTask(String fileDir) throws FileNotFoundException {
        this.omc30TestBean = readTestCase(fileDir);
    }

    public Omc30TestBean readTestCase(String fileDir) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(fileDir);
        return yaml.loadAs(inputStream, Omc30TestBean.class);
    }

    public void execute(Connection tddlConnection) throws InterruptedException, SQLException {
        System.out.println(omc30TestBean);

        try {
            // prepare
            for (String prepareSql : omc30TestBean.prepareSqlList) {
                JdbcUtil.executeSuccess(tddlConnection, prepareSql);
            }

            // get physical table names
            Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, omc30TestBean.tableName);

            // success alter
            if (GeneralUtil.isNotEmpty(omc30TestBean.successDDLs)) {
                for (SuccessDdl successDdl : omc30TestBean.successDDLs) {
                    JdbcUtil.executeSuccess(tddlConnection, successDdl.ddl);
                    // check table structure
                    if (!StringUtils.isEmpty(successDdl.expectTableStructure)) {
                        String sql = "show create table " + omc30TestBean.dbName + "." + omc30TestBean.tableName;
                        List<String> result = JdbcUtil.executeQueryAndGetColumnResult(sql, tddlConnection, 2);
                        System.out.println(result);
                        String createTable = result.get(0);
                        Assert.assertTrue(createTable.contains(successDdl.expectTableStructure));
                    }
                }
            }

            // reject alter
            if (GeneralUtil.isNotEmpty(omc30TestBean.rejectDDLs)) {
                for (RejectDdl rejectDdl : omc30TestBean.rejectDDLs) {
                    System.out.println(rejectDdl);
                    JdbcUtil.executeUpdateFailed(tddlConnection, rejectDdl.ddl, rejectDdl.message);
                    // check ddl not exist
                    String ddlState =
                        DDLBaseNewDBTestCase.findDdlStateByTable(omc30TestBean.dbName, omc30TestBean.tableName,
                            tddlConnection);
                    if (!ddlState.equalsIgnoreCase("ROLLBACK_COMPLETED") && !ddlState.equalsIgnoreCase("")) {
                        Assert.fail("ddl state is not ROLLBACK_COMPLETED, ddl state is " + ddlState + ", ddl is "
                            + rejectDdl.ddl);
                    }
                }
            }

            // check table
            Assert.assertTrue(DdlStateCheckUtil.checkTableStatus(tddlConnection, null, omc30TestBean.tableName));
            // check physical table
            Set<String> afterDdlPhysicalTableNames = getPhysicalTableNames(tddlConnection, omc30TestBean.tableName);
            Assert.assertEquals(physicalTableNames, afterDdlPhysicalTableNames);
        } finally {
            executeCleanupDDLs(tddlConnection, omc30TestBean.cleanupDDLs);
        }
    }

    public void executeFailed(Connection tddlConnection) throws InterruptedException, SQLException {
        System.out.println(omc30TestBean);

        // reject alter
        if (GeneralUtil.isNotEmpty(omc30TestBean.rejectDDLs)) {
            for (RejectDdl rejectDdl : omc30TestBean.rejectDDLs) {
                System.out.println(rejectDdl);
                try {
                    // prepare
                    for (String prepareSql : omc30TestBean.prepareSqlList) {
                        JdbcUtil.executeSuccess(tddlConnection, prepareSql);
                    }

                    // get physical table names
                    Set<String> physicalTableNames = getPhysicalTableNames(tddlConnection, omc30TestBean.tableName);

                    JdbcUtil.executeUpdateFailed(tddlConnection, rejectDdl.ddl, rejectDdl.message);
                    // check ddl not exist
                    String ddlState =
                        DDLBaseNewDBTestCase.findDdlStateByTable(omc30TestBean.dbName, omc30TestBean.tableName,
                            tddlConnection);
                    if (!ddlState.equalsIgnoreCase("PAUSED")) {
                        Assert.fail("ddl state is not PAUSED, ddl state is " + ddlState + ", ddl is " + rejectDdl.ddl);
                    }

                    // check table
//                    Assert.assertFalse(
//                        DdlStateCheckUtil.checkTableStatus(tddlConnection, null, omc30TestBean.tableName));
                    // check physical table
                    Set<String> afterDdlPhysicalTableNames =
                        getPhysicalTableNames(tddlConnection, omc30TestBean.tableName);
                    Assert.assertEquals(physicalTableNames, afterDdlPhysicalTableNames);
                } finally {
                    executeCleanupDDLs(tddlConnection, omc30TestBean.cleanupDDLs);
                }
            }
        }
    }

    public void executeCleanupDDLs(Connection mysqlConnection, List<String> cleanupDDls) {
        for (String cleanupDDl : cleanupDDls) {
            JdbcUtil.executeSuccess(mysqlConnection, cleanupDDl);
        }
    }

    public Set<String> getPhysicalTableNames(Connection tddlConnection, String tableName) throws SQLException {
        String showTopology = "show topology from " + tableName;
        ResultSet rs = JdbcUtil.executeQuery(showTopology, tddlConnection);
        Set<String> physicalTableNames = new TreeSet<>();
        while (rs.next()) {
            String physicalTableName = rs.getString("TABLE_NAME");
            physicalTableNames.add(physicalTableName);
        }
        return physicalTableNames;
    }
}

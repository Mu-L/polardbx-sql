package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.auto.omc30.omc30Utils.Omc30TestTask;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.SQLException;

@ReplicaIgnore(ignoreReason = "set session variables")
public class FailedPointTest extends DDLBaseNewDBTestCase {
    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException, SQLException {

        /**
         * Ignore this case for debug ddl qatest
         */
        String resourceDir = "partition/env/Omc30Test/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        Omc30TestTask omc30TestTask = new Omc30TestTask(fileDir);
        omc30TestTask.execute(tddlConnection);
    }

    @Before
    public void beforeMethod() {
        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
    }

    /**
     * 全部失败
     */
    @Test
    public void testAllFailed() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("all_physical_tb_failed.test.yml");
    }

    @Test
    public void testAllFailed2() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("all_physical_tb_failed_2.test.yml");
    }

    /**
     * 执行期间出现报错，但最后全部执行成功(rename 成功)
     */
    @Test
    public void testErrorButSuccess() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("all_physical_tb_success.test.yml");
    }

    /**
     * 部分成功部分失败，并自动回滚
     */
    @Test
    public void testPartFailed() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("part_physical_tb_failed.test.yml");
    }

    /**
     * 全部成功后失败，并自动回滚
     */
    @Test
    public void testSuccessButFailed() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("all_physical_tb_success_but_failed.test.yml");
    }
}

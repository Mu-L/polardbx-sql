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
public class FailedPointPausedTest extends DDLBaseNewDBTestCase {
    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException, SQLException {

        /**
         * Ignore this case for debug ddl qatest
         */
        String resourceDir = "partition/env/Omc30Test/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        Omc30TestTask omc30TestTask = new Omc30TestTask(fileDir);
        omc30TestTask.executeFailed(tddlConnection);
    }

    @Before
    public void beforeMethod() {
        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
    }

    @Test
    public void testPartPhysicalTableFailed() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("part_physical_tb_failed_not_rollback.test.yml");
    }
}

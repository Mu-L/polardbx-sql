package com.alibaba.polardbx.qatest.ddl.datamigration.locality;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.gms.locality.DbConfig;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestCaseTask;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;

@NotThreadSafe
public class LocalityTestCaseForRepartitionWithPhyDbConfig extends LocalityTestBase {

    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException, SQLException {

        /**
         * Ignore this case for debug ddl qatest 
         */
        String resourceDir = "partition/env/LocalityTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        LocalityTestCaseTask localityTestCaseTask = new LocalityTestCaseTask(fileDir);
        localityTestCaseTask.execute(tddlConnection);
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void testRepartitionWithPhyDbConfig() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("repartition_table_with_physical_config.test.yml");
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void testRepartitionWithPhyDbConfig2() throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("repartition_table_with_physical_config2.test.yml");
    }
}

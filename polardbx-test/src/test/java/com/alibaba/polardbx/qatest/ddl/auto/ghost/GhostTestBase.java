package com.alibaba.polardbx.qatest.ddl.auto.ghost;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GhostTestBase extends DDLBaseNewDBTestCase {
    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException, SQLException {
        // get dn id
        List<String> dnList = getStorageInstIds(tddlDatabase1);

        /**
         * Ignore this case for debug ddl qatest
         */
        String resourceDir = "partition/env/GhostTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        GhostTestTask ghostTestTask = new GhostTestTask(fileDir);
        ghostTestTask.execute(tddlConnection, mysqlConnection, dnList.get(0));
    }

    protected static List<String> getParameters(Class cls) {
        BufferedReader bufferedReader = null;
        List<String> testCases = new ArrayList<>();
        try {
            String resourceDir = "partition/env/GhostTest/testcase.config.yml";
            InputStream in = cls.getClassLoader().getResourceAsStream(resourceDir);
            InputStreamReader inputStreamReader = new InputStreamReader(in, "utf8");
            bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                testCases.add(line.trim() + ".test.yml");
            }
            return testCases;
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return testCases;
    }
}

/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.qatest.dal.ai;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class Nl2sqlAgentTest extends Nl2sqlAgentTestBase {

    @Test
    public void testNl2sql_basicQuery() throws Exception {
        String output = executeNlQuery("查询所有员工姓名");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        Assert.assertTrue("Output should contain SQL or data",
            output.toLowerCase().contains("select") || output.contains("Alice")
                || output.contains("name") || output.contains("员工"));
    }

    @Test
    public void testNl2sql_englishQuery() throws Exception {
        String output = executeNlQuery("how many employees are there");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_aggregationQuery() throws Exception {
        String output = executeNlQuery("每个部门有多少员工");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_filterQuery() throws Exception {
        String output = executeNlQuery("查询employees表中salary大于80000的员工");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }
}

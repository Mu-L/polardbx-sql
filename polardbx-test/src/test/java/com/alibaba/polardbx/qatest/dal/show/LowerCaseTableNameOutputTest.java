/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

public class LowerCaseTableNameOutputTest extends DDLBaseNewDBTestCase {

    private static final String ENABLE_HINT =
        "/*+TDDL:cmd_extra(ENABLE_LOWER_CASE_TABLE_NAME_OUTPUT=true)*/";
    private static final String DISABLE_HINT =
        "/*+TDDL:cmd_extra(ENABLE_LOWER_CASE_TABLE_NAME_OUTPUT=false)*/";

    private final String tableName = randomTableName("LowerCaseTableNameOutputTest", 12);
    private final String lowerCaseTableName = tableName.toLowerCase(Locale.ROOT);

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void prepareTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists `" + tableName + "`");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table `" + tableName + "` (id bigint primary key) partition by key(id)");
    }

    @After
    public void dropTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists `" + tableName + "`");
    }

    @Test
    public void testMetadataCommandsLowerCaseTableNameWhenSwitchOn() throws SQLException {
        assertShowCreateTable(ENABLE_HINT, lowerCaseTableName);

        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            ENABLE_HINT + "show rule from `" + tableName + "`")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(lowerCaseTableName, resultSet.getString("TABLE_NAME"));
        }
    }

    @Test
    public void testMetadataCommandsPreserveTableNameWhenSwitchOff() throws SQLException {
        assertShowCreateTable(DISABLE_HINT, tableName);
    }

    private void assertShowCreateTable(String hint, String expectedTableName) throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            hint + "show create table `" + tableName + "`")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(expectedTableName, resultSet.getString(1));
            Assert.assertTrue(resultSet.getString(2).contains("`" + expectedTableName + "`"));
        }
    }

}

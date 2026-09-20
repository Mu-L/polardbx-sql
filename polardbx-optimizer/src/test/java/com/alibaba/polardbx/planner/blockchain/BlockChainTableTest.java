/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.planner.blockchain;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lijiu.lzw
 */
public class BlockChainTableTest extends BasePlannerTest {
    public BlockChainTableTest() {
        super("blockchain");
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    @Test
    public void createTest() {
        String sql = "CREATE TABLE `account` (\n"
            + "  `id` BIGINT NOT NULL,\n"
            + "  `balance` DECIMAL(10, 2) NOT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") PARTITION BY KEY(`id`) COMMENT '__POLARDBX_BLOCK_CHAIN__';";

        ExecutionContext executionContext = new ExecutionContext(appName);
        SqlNode ast = new FastsqlParser().parse(sql, executionContext).get(0);

        String trueSql = "CREATE TABLE `account` (\n"
            + "\t`id` BIGINT NOT NULL,\n"
            + "\t`balance` DECIMAL(10, 2) NOT NULL,\n"
            + "\t`_polardbx_hash_digest_` CHAR(64) GENERATED ALWAYS AS (SHA2(CONCAT_WS('|', `id`, `balance`), 256)) STORED,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tCLUSTERED COLUMNAR INDEX `_cci_account`(`id`)\n"
            + ") COMMENT '__POLARDBX_BLOCK_CHAIN__'\n"
            + "PARTITION BY KEY (`id`);";
        Assert.assertEquals(trueSql, ast.toString());

        String sql2 = "CREATE TABLE `account` (\n"
            + "\t`id` BIGINT NOT NULL,\n"
            + "\t`balance` DECIMAL(10, 2) NOT NULL,\n"
            + "\t`_polardbx_hash_digest_` CHAR(64) GENERATED ALWAYS AS (SHA2(CONCAT_WS('|', `id`, `balance`), 256)) STORED,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tCLUSTERED COLUMNAR INDEX `_cci_account`(`id`) COLUMNAR_OPTIONS='{ \"TYPE\":\"HISTORY\"}'\n"
            + ") COMMENT '__POLARDBX_BLOCK_CHAIN__'\n"
            + "PARTITION BY KEY (`id`);";

        String trueSql2 = "CREATE TABLE `account` (\n"
            + "\t`id` BIGINT NOT NULL,\n"
            + "\t`balance` DECIMAL(10, 2) NOT NULL,\n"
            + "\t`_polardbx_hash_digest_` CHAR(64) GENERATED ALWAYS AS (SHA2(CONCAT_WS('|', `id`, `balance`), 256)) STORED,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tCLUSTERED COLUMNAR INDEX `_cci_account`(`id`)\n"
            + ") COMMENT '__POLARDBX_BLOCK_CHAIN__'\n"
            + "PARTITION BY KEY (`id`);";

        SqlNode ast2 = new FastsqlParser().parse(sql2, executionContext).get(0);

        Assert.assertEquals(trueSql2, ast2.toString());

    }

}

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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTableNoCdcMark;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropBlockChainTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionTable;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lijiu.lzw
 */
public class BlockChainJobTest {

    @Test
    public void testBlockChainJob() {
        ExecutableDdlJob4CreatePartitionTableNoCdcMark task =
            ExecutableDdlJob4CreatePartitionTableNoCdcMark.buildFrom(new ExecutableDdlJob4CreatePartitionTable());

        ExecutableDdlJob4DropBlockChainTable task2 = ExecutableDdlJob4DropBlockChainTable.buildFrom(
            new ExecutableDdlJob4DropPartitionTable(), "testSchema", "testTable", "");

        Assert.assertEquals("testSchema", task2.getDeleteGlobalChainDataTask().getSchemaName());
        Assert.assertEquals("testTable", task2.getDeleteGlobalChainDataTask().getTableName());
    }
}

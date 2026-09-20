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

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.server.handler.pl.inner.ChainHistArchiveProcedure;
import com.alibaba.polardbx.server.util.GlobalChainUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class ChainHistArchiveProcedureTest {
    @Test
    public void test() {
        try (MockedStatic<ChainHistArchiveProcedure> ignored = mockStatic(ChainHistArchiveProcedure.class);
            MockedStatic<GlobalChainUtils> globalChainUtilsMockedStatic = mockStatic(GlobalChainUtils.class)) {
            globalChainUtilsMockedStatic.when(() -> GlobalChainUtils.checkPrivilege(Mockito.any()))
                .thenAnswer(invocation -> null);

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.chain_hist_archive(10)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ChainHistArchiveProcedure procedure = new ChainHistArchiveProcedure();
            ArrayResultCursor cursor = new ArrayResultCursor("test");
            try {
                procedure.execute(null, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Expects two parameters"));
            }
            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.chain_hist_archive(10, 10)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Expects two string parameters"));
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.chain_hist_archive('db', 'tb')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            when(ChainHistArchiveProcedure.chainHistArchive(Mockito.any(), Mockito.any())).thenReturn(null);
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());

            when(ChainHistArchiveProcedure.chainHistArchive(Mockito.any(), Mockito.any())).thenReturn(
                new BlockChainHistoryTableRecord());

            cursor = new ArrayResultCursor("test");
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
        }
    }

    @Test
    public void test2() throws Exception {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<DynamicConfig> dynamicConfigMockedStatic = mockStatic(DynamicConfig.class);
            MockedStatic<InnerConnectionManager> innerConnectionManagerMockedStatic = mockStatic(
                InnerConnectionManager.class)) {
            InnerConnection innerConnection = Mockito.mock(InnerConnection.class);
            InnerConnectionManager innerConnectionManager = Mockito.mock(InnerConnectionManager.class);
            innerConnectionManagerMockedStatic.when(InnerConnectionManager::getInstance)
                .thenReturn(innerConnectionManager);
            when(innerConnectionManager.getConnection()).thenReturn(innerConnection);

            DynamicConfig dynamicConfig = Mockito.mock(DynamicConfig.class);
            when(DynamicConfig.getInstance()).thenReturn(dynamicConfig);
            when(dynamicConfig.isIgnoreCheckGlobalWhenArchiveChain()).thenReturn(true);

            List<BlockChainHistoryTableRecord> records = new ArrayList<>();
            metaDbUtilMockedStatic.when(
                () -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(), Mockito.any(),
                    Mockito.any())).thenReturn(records);

            BlockChainHistoryTableRecord rs = ChainHistArchiveProcedure.chainHistArchive("schemaName", "tableName");
            Assert.assertNull(rs);
            BlockChainHistoryTableRecord record = new BlockChainHistoryTableRecord();
            records.add(record);
            rs = ChainHistArchiveProcedure.chainHistArchive("schemaName", "tableName");
            Assert.assertNotNull(rs);
        }
    }

}

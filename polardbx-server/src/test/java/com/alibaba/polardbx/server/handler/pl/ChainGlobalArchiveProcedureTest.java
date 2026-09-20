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
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainSimpleRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.ChainGlobalArchiveProcedure;
import com.alibaba.polardbx.server.util.GlobalChainUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class ChainGlobalArchiveProcedureTest {
    @Test
    public void test() {
        try (MockedStatic<ChainGlobalArchiveProcedure> ignored = mockStatic(ChainGlobalArchiveProcedure.class);
            MockedStatic<GlobalChainUtils> globalChainUtilsMockedStatic = mockStatic(GlobalChainUtils.class)) {
            globalChainUtilsMockedStatic.when(() -> GlobalChainUtils.checkPrivilege(Mockito.any()))
                .thenAnswer(invocation -> null);

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.chain_global_archive(10)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            ChainGlobalArchiveProcedure procedure = new ChainGlobalArchiveProcedure();
            ArrayResultCursor cursor = new ArrayResultCursor("test");
            try {
                procedure.execute(null, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Expects No Parameters"));
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.chain_global_archive()",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            when(ChainGlobalArchiveProcedure.globalChainArchive()).thenReturn(-1L);
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());

            when(ChainGlobalArchiveProcedure.globalChainArchive()).thenReturn(10L);

            cursor = new ArrayResultCursor("test");
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
        }
    }

    @Test
    public void test2() throws Exception {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<DynamicConfig> dynamicConfigMockedStatic = Mockito.mockStatic(DynamicConfig.class);
            MockedStatic<GlobalChainUtils> globalChainUtilsMockedStatic = mockStatic(GlobalChainUtils.class)) {
            Connection conn = Mockito.mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(conn);

            List<GlobalChainSimpleRecord> records = new ArrayList<>();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(records);
            long rs = ChainGlobalArchiveProcedure.globalChainArchive();
            Assert.assertEquals(-1, rs);
            GlobalChainSimpleRecord record = new GlobalChainSimpleRecord();
            records.add(record);

            DynamicConfig config = Mockito.mock(DynamicConfig.class);
            when(DynamicConfig.getInstance()).thenReturn(config);
            when(config.isIgnoreCheckGlobalWhenArchiveChain()).thenReturn(true);

            ChainGlobalArchiveProcedure.globalChainArchive();

            record.schemaName = "schemaName";

            globalChainUtilsMockedStatic.when(
                    () -> GlobalChainUtils.accumulateOpHash(Mockito.any(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.anyLong()))
                .thenReturn("abc");

            ChainGlobalArchiveProcedure.globalChainArchive();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenThrow(new RuntimeException("mock exception"));

            try {
                ChainGlobalArchiveProcedure.globalChainArchive();
                Assert.fail();
            } catch (Exception ignored) {
            }

        }
    }
}

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

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.CheckChainValidProcedure;
import com.alibaba.polardbx.server.util.GlobalChainUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class CheckChainValidProcedureTest {
    @Test
    public void test() {
        try (MockedStatic<GlobalChainUtils> ignored = mockStatic(GlobalChainUtils.class)) {
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.check_chain_valid(10)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            CheckChainValidProcedure procedure = new CheckChainValidProcedure();
            ArrayResultCursor cursor = new ArrayResultCursor("test");
            try {
                procedure.execute(null, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("Expects two parameters"));
            }
            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.check_chain_valid(db, table)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            when(GlobalChainUtils.checkChain(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals("OK", cursor.getRows().get(0).getObject(0));
            when(GlobalChainUtils.checkChain(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
            cursor = new ArrayResultCursor("test");
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals("FAIL", cursor.getRows().get(0).getObject(0));
        }
    }
}

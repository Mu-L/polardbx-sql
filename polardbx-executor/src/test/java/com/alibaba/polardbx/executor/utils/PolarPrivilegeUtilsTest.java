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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.ImmutableMap;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * @author lijiu.lzw
 */
public class PolarPrivilegeUtilsTest {

    @Test
    public void test() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setPrivilegeMode(true);

        SchemaManager sm = Mockito.mock(SchemaManager.class);
        when(sm.getSchemaName()).thenReturn("information_schema");
        executionContext.setSchemaManagers(ImmutableMap.of("information_schema", sm));

        PrivilegeContext pc = new PrivilegeContext();
        executionContext.setPrivilegeContext(pc);
        pc.setPolarUserInfo(new PolarAccountInfo(PolarAccount.newBuilder().setAccountType(AccountType.GOD).build()));

        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        when(sm.getTable(Mockito.anyString())).thenReturn(tableMeta);

        PolarPrivilegeUtils.checkPrivilege("information_schema", "polardbx_global_chain", PrivilegePoint.SELECT,
            executionContext);

        pc.setPolarUserInfo(new PolarAccountInfo(PolarAccount.newBuilder().setAccountType(AccountType.USER).build()));
        try {
            PolarPrivilegeUtils.checkPrivilege("information_schema", "polardbx_global_chain", PrivilegePoint.SELECT,
                executionContext);
            Assert.fail();
        } catch (Exception ignored) {
        }

        when(tableMeta.isBlockChainHistory()).thenReturn(true);
        pc.setPolarUserInfo(new PolarAccountInfo(PolarAccount.newBuilder().setAccountType(AccountType.GOD).build()));
        PolarPrivilegeUtils.checkPrivilege("information_schema", "hist", PrivilegePoint.UPDATE,
            executionContext);

        pc.setPolarUserInfo(new PolarAccountInfo(PolarAccount.newBuilder().setAccountType(AccountType.USER).build()));
        try {
            PolarPrivilegeUtils.checkPrivilege("information_schema", "hist", PrivilegePoint.UPDATE,
                executionContext);
            Assert.fail();
        } catch (Exception ignored) {
        }

        try {
            PolarPrivilegeUtils.checkPrivilege("information_schema", "hist", PrivilegePoint.SELECT,
                executionContext);
            Assert.fail();
        } catch (Exception ignored) {
        }

        when(tableMeta.isBlockChainHistory()).thenReturn(false);
        when(tableMeta.isPolardbxBlockChain()).thenReturn(true);

        try {
            PolarPrivilegeUtils.checkPrivilege("information_schema", "hist", PrivilegePoint.ALTER,
                executionContext);
            Assert.fail();
        } catch (Exception ignored) {
        }

    }
}

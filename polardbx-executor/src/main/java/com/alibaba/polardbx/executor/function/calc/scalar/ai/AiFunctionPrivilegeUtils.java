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

package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;

final class AiFunctionPrivilegeUtils {

    private static final String UNKNOWN_ACCOUNT = "unknown";

    private AiFunctionPrivilegeUtils() {
    }

    static void checkPrivilege(ExecutionContext executionContext, String functionName) {
        PrivilegeContext privilegeContext =
            executionContext == null ? null : executionContext.getPrivilegeContext();
        if (privilegeContext != null
            && privilegeContext.getPolarUserInfo() != null
            && executionContext.isSuperUser()) {
            return;
        }

        String user = privilegeContext == null || privilegeContext.getUser() == null
            ? UNKNOWN_ACCOUNT : privilegeContext.getUser();
        String host = privilegeContext == null || privilegeContext.getHost() == null
            ? UNKNOWN_ACCOUNT : privilegeContext.getHost();
        throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED, functionName, user, host);
    }
}

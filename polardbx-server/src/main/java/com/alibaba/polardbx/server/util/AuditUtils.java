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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AuditUtils {
    private static volatile boolean enableLogAudit = Boolean.parseBoolean(
        ConnectionParams.ENABLE_LOGIN_AUDIT_CONFIG.getDefault());

    public static void logAuditInfo(ServerConnection c, String message, AuditAction action) {
        SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
        LogUtils.recordSql(c, ByteString.from(f.format(new Date()) + ',' + message + ',' + action), true);
    }

    public static void setEnableLogAudit(boolean enableLogAudit) {
        AuditUtils.enableLogAudit = enableLogAudit;
    }

    public static boolean isEnableLogAudit() {
        return enableLogAudit;
    }
}

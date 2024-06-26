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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.archive.reader.BufferPoolManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;

/**
 * @author chenzilin
 */
public class InvalidateBufferPoolSyncAction implements ISyncAction {

    private String schemaName = null;

    private String logicalTableName = null;

    public InvalidateBufferPoolSyncAction() {
    }

    public InvalidateBufferPoolSyncAction(String schemaName, String logicalTableName) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public void setLogicalTableName(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    @Override
    public ResultCursor sync() {

        if (schemaName == null && logicalTableName == null) {
            BufferPoolManager.getInstance().clear();
        } else if (schemaName != null) {
            BufferPoolManager.getInstance().invalidate(schemaName, null);
        } else if (schemaName != null && logicalTableName != null) {
            BufferPoolManager.getInstance().invalidate(schemaName, logicalTableName);
        }
        return null;
    }
}


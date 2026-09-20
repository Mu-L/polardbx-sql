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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class TableGroupSyncAction implements ISyncAction {
    private String schemaName;
    private String tableGroupName;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public TableGroupSyncAction(String schemaName, String tableGroupName) {
        this.schemaName = schemaName;
        this.tableGroupName = tableGroupName;
    }

    /*
     * Change context:
     * - Before: sync() dereferenced OptimizerContext.getContext(schemaName) directly, assuming the
     *   context of the target schema is always registered when the sync is broadcast
     *   (historical rationale not confirmed, but the code path had no null branch). During schema
     *   reload or when the schema is temporarily unregistered, getContext() returns null and a
     *   message-less NPE surfaced as "Failed to execute the DDL task. Caused by: null".
     * - Path impact: every caller of SyncManagerHelper broadcasting a TableGroupSyncAction
     *   (TableGroupSyncTask and other table-group DDL tasks) now receives an explicit
     *   TddlRuntimeException instead of an NPE only when the context is unavailable; the normal
     *   path with a registered context is unchanged, and TableGroupSyncTask still catches the
     *   exception and wraps it, so DDL retry/PAUSED semantics are preserved.
     * - Capability regression: None. The added branch only fires when the previous code already
     *   failed with NPE, and the exception type remains a RuntimeException subclass handled by the
     *   same catch block.
     */
    @Override
    public ResultCursor sync() {
        FailPoint.inject(FailPointKey.FP_TABLE_GROUP_SYNC_CONTEXT_NULL, () -> {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "[failpoint] optimizer context of schema [" + schemaName
                    + "] is unavailable, cannot sync table group [" + tableGroupName + "]");
        });
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        TableGroupInfoManager tableGroupInfoManager =
            optimizerContext == null ? null : optimizerContext.getTableGroupInfoManager();
        if (tableGroupInfoManager == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "optimizer context of schema [" + schemaName
                    + "] is unavailable, cannot sync table group [" + tableGroupName + "]");
        }
        tableGroupInfoManager.reloadTableGroupByGroupName(schemaName, tableGroupName);
        return null;
    }
}

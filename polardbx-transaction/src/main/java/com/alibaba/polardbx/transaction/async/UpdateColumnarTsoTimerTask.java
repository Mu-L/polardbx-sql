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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.sync.ColumnarSnapshotUpdateSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.config.SqlEngineAlert;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SyncUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class UpdateColumnarTsoTimerTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger("mpp_log");

    @Override
    public void run() {
        try {
            if (!SyncUtil.isNodeWithSmallestId()) {
                return;
            }
            Long latestTso;
            int tsoUpdateDelay = InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY);
            if (tsoUpdateDelay > 0) {
                latestTso = ColumnarTransactionUtils.getLatestTsoFromGmsWithDelay(
                    1000L * tsoUpdateDelay // convert milliseconds to microseconds
                );
            } else {
                latestTso = ColumnarTransactionUtils.getLatestTsoFromGms();
            }
            long lastTsoMs = ColumnarManager.getInstance().latestTso() >> 22;
            long delayThreshold = InstConfUtil.getInt(ConnectionParams.COLUMNAR_DELAY_WARNING_THRESHOLD);
            long currentDelay = System.currentTimeMillis() - lastTsoMs;
            if (currentDelay > delayThreshold && latestTso != null && latestTso != Long.MIN_VALUE) {
                if (cciExists()) {
                    String alertMsg =
                        String.format("Current columnar read delay is %d ms, which is beyond the threshold %d ms",
                            currentDelay, delayThreshold);
                    EventLogger.log(EventType.COLUMNAR_READ_ALERT, alertMsg);
                    SqlEngineAlert.getInstance().putColumnarRead(alertMsg);
                }
            }

            logger.warn("update the columnar tso: " + latestTso);

            if (latestTso != null) {
                try {
                    ColumnarManager.getInstance().setLatestTso(latestTso);
                    SyncManagerHelper.syncThrowExceptions(new ColumnarSnapshotUpdateSyncAction(latestTso),
                        SystemDbHelper.DEFAULT_DB_NAME, SyncScope.CURRENT_ONLY);
                } catch (Throwable t) {
                    logger.error(String.format("Failed to update columnar tso: %d", latestTso), t);
                }
            }
        } catch (Throwable t) {
            logger.error("Columnar tso update task failed unexpectedly!", t);
        }
    }

    private static boolean cciExists() throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            accessor.setConnection(connection);
            List<ColumnarTableMappingRecord> records = accessor.queryLimitOne();
            return !records.isEmpty();
        }
    }
}

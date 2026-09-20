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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsReloadTableStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.sync.ConnectorSyncAction;
import com.alibaba.polardbx.executor.sync.ReloadSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.ReloadUtils;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReloadHandler {

    public static boolean handle(ByteString sqlBytes, ServerConnection c) {
        final String stmt = sqlBytes.toString();
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            // 取得SCHEMA
            String db = c.getSchema();
            if (db == null) {
                c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return false;
            }

            SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(db);
            if (schema == null) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
                return false;
            }

            TDataSource ds = schema.getDataSource();
            if (!ds.isInited()) {
                try {
                    ds.init();
                } catch (Throwable e) {
                    c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                    return false;
                }
            }

            OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

            String pattern;
            Pattern r;
            Matcher m;

            pattern = "RELOAD[\\s]+DATASOURCES";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.DATASOURCES, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+SCHEMA";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.SCHEMA, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+USER";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.USERS, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+PROCEDURES";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.PROCEDURES, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+FUNCTIONS";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.FUNCTIONS, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+JAVA[\\s]+FUNCTIONS";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.JAVA_FUNCTIONS, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+LOCAL[\\s]+JAVA[\\s]+FUNCTIONS";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                JavaFunctionManager.getInstance().reload();
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+FILESTORAGE";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(new ReloadSyncAction(ReloadUtils.ReloadType.FILESTORAGE, c.getSchema()),
                        c.getSchema(),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+CONNECTORS[;]*";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                if (!c.isSuperUserOrAllPrivileges()) {
                    c.writeErrMessage(ErrorCode.ER_SPECIFIC_ACCESS_DENIED_ERROR,
                        "Access denied; you need a high-privilege account for this operation");
                    return false;
                }
                SyncManagerHelper.syncThrowExceptions(new ConnectorSyncAction(), SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "[\\s]*RELOAD[\\s]+STATISTICS[;]*";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                SyncManagerHelper
                    .syncThrowExceptions(
                        new ReloadSyncAction(ReloadUtils.ReloadType.STATISTICS, SystemDbHelper.DEFAULT_DB_NAME),
                        SyncScope.ALL);
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            pattern = "RELOAD[\\s]+COLUMNARMANAGER.*";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                String subPattern;
                Pattern subR;
                Matcher subM;

                subPattern = "RELOAD[\\s]+COLUMNARMANAGER[\\s]+CACHE[;]*";
                subR = Pattern.compile(subPattern, Pattern.CASE_INSENSITIVE);
                subM = subR.matcher(stmt);
                if (subM.matches()) {
                    SyncManagerHelper
                        .syncThrowExceptions(
                            new ReloadSyncAction(ReloadUtils.ReloadType.COLUMNARMANAGER_CACHE, c.getSchema()),
                            c.getSchema(),
                            SyncScope.ALL);
                    PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                    return true;
                }

                subPattern = "RELOAD[\\s]+COLUMNARMANAGER[\\s]+SNAPSHOT[;]*";
                subR = Pattern.compile(subPattern, Pattern.CASE_INSENSITIVE);
                subM = subR.matcher(stmt);
                if (subM.matches()) {
                    SyncManagerHelper
                        .syncThrowExceptions(
                            new ReloadSyncAction(ReloadUtils.ReloadType.COLUMNARMANAGER_SNAPSHOT, c.getSchema()),
                            c.getSchema(),
                            SyncScope.ALL);
                    PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                    return true;
                }

                subPattern = "RELOAD[\\s]+COLUMNARMANAGER[\\s]+SCHEMA[;]*";
                subR = Pattern.compile(subPattern, Pattern.CASE_INSENSITIVE);
                subM = subR.matcher(stmt);
                if (subM.matches()) {
                    SyncManagerHelper
                        .syncThrowExceptions(
                            new ReloadSyncAction(ReloadUtils.ReloadType.COLUMNARMANAGER_SCHEMA, c.getSchema()),
                            c.getSchema(),
                            SyncScope.ALL);
                    PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                    return true;
                }

                subPattern = "RELOAD[\\s]+COLUMNARMANAGER[;]*";
                subR = Pattern.compile(subPattern, Pattern.CASE_INSENSITIVE);
                subM = subR.matcher(stmt);
                if (subM.matches()) {
                    SyncManagerHelper
                        .syncThrowExceptions(
                            new ReloadSyncAction(ReloadUtils.ReloadType.COLUMNARMANAGER, c.getSchema()),
                            c.getSchema(),
                            SyncScope.ALL);
                    PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                    return true;
                }
            }

            pattern = "RELOAD[\\s]+TABLE.*";
            r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
            m = r.matcher(stmt);
            if (m.matches()) {
                List<SQLStatement> stmtList = FastsqlUtils.parseSql(stmt);
                if (stmtList.size() != 1) {
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORT, "dose not support multi statement in reload table");
                    return false;
                }

                if (!(stmtList.get(0) instanceof DrdsReloadTableStatement)) {
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORT, "illegal flush statement");
                    return false;
                }

                DrdsReloadTableStatement drdsReloadTableStatement = (DrdsReloadTableStatement) stmtList.get(0);
                String schemaName = drdsReloadTableStatement.getTableSchemaName() == null ? c.getSchema() :
                    drdsReloadTableStatement.getTableSchemaName();
                String tableName = drdsReloadTableStatement.getTableNameStr();

                if (StringUtils.isEmpty(tableName)) {
                    c.writeErrMessage(ErrorCode.ERR_NOT_SUPPORT, "table name can not be empty");
                    return false;
                }

                // update version
                long tableVersionFromMetaDb = -1L;
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableVersionFromMetaDb = TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e,
                        "Failed to update table version before reload table");
                }

                // do sync
                TableMeta tableMetaBefore =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(tableName);
                boolean preemptive = drdsReloadTableStatement.getPreemptive();
                ParamManager paramManager = OptimizerContext.getContext(schemaName).getParamManager();
                boolean enablePreemptiveMdl = paramManager.getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
                PreemptiveTime preemptiveTime =
                    PreemptiveTime.getPreemptiveTimeFromExecutionContext(paramManager,
                        ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
                if (!preemptive || !enablePreemptiveMdl) {
                    SyncManagerHelper.syncThrowExceptions(
                        new TableMetaChangeSyncAction(schemaName, tableName),
                        SyncScope.ALL);
                } else {
                    SyncManagerHelper.syncThrowExceptions(
                        new TableMetaChangePreemptiveSyncAction(schemaName, tableName, preemptiveTime, false),
                        SyncScope.ALL);
                }

                TableMeta tableMetaAfter =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(tableName);

                StringBuilder builder = new StringBuilder();
                builder.append("[Reload table] schema: ").append(schemaName);
                builder.append(" table: ").append(tableName);
                if (tableMetaBefore != null) {
                    builder.append(" before version: ").append(tableMetaBefore.getVersion());
                }
                if (tableMetaAfter != null) {
                    builder.append(" after version: ").append(tableMetaAfter.getVersion());
                }
                if (tableVersionFromMetaDb != -1L) {
                    builder.append(" table version from metadb: ").append(tableVersionFromMetaDb);
                }
                builder.append("reload sql :").append(drdsReloadTableStatement);
                SQLRecorderLogger.ddlMetaLogger.info(builder.toString());

                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                return true;
            }

            recordSql = false;
            return c.execute(sqlBytes, false);
        } catch (Throwable ex) {
            sqlEx = ex;
            throw ex;
        } finally {
            if (recordSql) {
                LogUtils.recordSql(c, sqlBytes, sqlEx);
            }
        }
    }
}

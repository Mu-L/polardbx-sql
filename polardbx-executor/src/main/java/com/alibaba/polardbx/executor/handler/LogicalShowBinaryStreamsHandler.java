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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.FullMasterStatus;
import com.alibaba.polardbx.rpc.cdc.Request;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinaryStreams;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LogicalShowBinaryStreamsHandler extends HandlerCommon {
    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalShowBinaryStreamsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShowBinaryStreams sqlShowBinaryStreams =
            (SqlShowBinaryStreams) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlShowBinaryStreams.getWith();
        String groupName = with == null ? null : RelUtils.lastStringValue(with);
        ArrayResultCursor result = null;

        BinlogStreamAccessor binlogStreamAccessor = new BinlogStreamAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            binlogStreamAccessor.setConnection(metaDbConn);
            List<BinlogStreamRecord> streams;
            if (groupName == null) {
                streams = binlogStreamAccessor.listAllStream();
            } else {
                streams = binlogStreamAccessor.listStreamInGroup(groupName);
            }
            if (streams == null) {
                throw new TddlNestableRuntimeException("binlog multi stream is not support...");
            }

            if (sqlShowBinaryStreams.isFull()) {
                result = new ArrayResultCursor("SHOW FULL BINARY STREAMS");
                result.addColumn("Group", DataTypes.StringType);
                result.addColumn("Stream", DataTypes.StringType);
                result.addColumn("File", DataTypes.StringType);
                result.addColumn("Position", DataTypes.LongType);
                result.addColumn("Status", DataTypes.LongType);
                result.addColumn("LastTso", DataTypes.StringType);
                result.addColumn("DelayTimeMs", DataTypes.LongType);
                result.addColumn("AvgRevEps", DataTypes.LongType);
                result.addColumn("AvgRevBps", DataTypes.LongType);
                result.addColumn("AvgWriteEps", DataTypes.LongType);
                result.addColumn("AvgWriteBps", DataTypes.LongType);
                result.addColumn("AvgWriteTps", DataTypes.LongType);
                result.addColumn("AvgUploadBps", DataTypes.LongType);
                result.addColumn("AvgDumpBps", DataTypes.LongType);
                result.addColumn("ExtInfo", DataTypes.StringType);
                result.initMeta();

                for (BinlogStreamRecord stream : streams) {
                    CdcServiceGrpc.CdcServiceBlockingStub cdcServiceBlockingStub =
                        CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub(stream.getStreamName());
                    try {
                        FullMasterStatus fullMasterStatus =
                            cdcServiceBlockingStub.showFullMasterStatus(
                                Request.newBuilder().setStreamName(stream.getStreamName()).build());

                        result.addRow(new Object[] {
                            stream.getGroupName(),
                            stream.getStreamName(),
                            fullMasterStatus.getFile(),
                            fullMasterStatus.getPosition(),
                            getStatusDesc(stream.getStatus()),
                            fullMasterStatus.getLastTso(),
                            fullMasterStatus.getDelayTime(),
                            fullMasterStatus.getAvgRevEps(),
                            fullMasterStatus.getAvgRevBps(),
                            fullMasterStatus.getAvgWriteEps(),
                            fullMasterStatus.getAvgWriteBps(),
                            fullMasterStatus.getAvgWriteTps(),
                            fullMasterStatus.getAvgUploadBps(),
                            fullMasterStatus.getAvgDumpBps(),
                            fullMasterStatus.getExtInfo()
                        });
                    } finally {
                        Channel channel = cdcServiceBlockingStub.getChannel();
                        if (channel instanceof ManagedChannel) {
                            ((ManagedChannel) channel).shutdown();
                        }
                    }
                }
            } else {
                result = new ArrayResultCursor("SHOW BINARY STREAMS");
                result.addColumn("Group", DataTypes.StringType);
                result.addColumn("Stream", DataTypes.StringType);
                result.addColumn("File", DataTypes.StringType);
                result.addColumn("Position", DataTypes.LongType);
                result.addColumn("Status", DataTypes.LongType);
                result.initMeta();

                for (BinlogStreamRecord stream : streams) {
                    result.addRow(new Object[] {
                        stream.getGroupName(), stream.getStreamName(), stream.getFileName(),
                        stream.getPosition(), getStatusDesc(stream.getStatus())});
                }
            }
        } catch (SQLException e) {
            cdcLogger.error("get binlog x stream fail", e);
        }
        return result;
    }

    private String getStatusDesc(int status) {
        return status == 0 ? "Normal" : "Pending";
    }
}

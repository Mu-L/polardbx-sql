package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlPurgeBinaryStream;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LogicalPurgeBinaryStreamHandler extends HandlerCommon {
    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalPurgeBinaryStreamHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlPurgeBinaryStream sqlPurgeBinaryStream =
            (SqlPurgeBinaryStream) ((LogicalDal) logicalPlan).getNativeSqlNode();
        BinlogStreamAccessor binlogStreamAccessor = new BinlogStreamAccessor();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            String streamName = sqlPurgeBinaryStream.getStreamName().toString();
            binlogStreamAccessor.setConnection(metaDbConn);
            List<BinlogStreamRecord> recordList =
                binlogStreamAccessor.getStream(streamName);
            if (recordList.isEmpty()) {
                throw new RuntimeException("binlog stream is not found");
            } else {
                if (recordList.get(0).getStatus() != 1) {
                    throw new RuntimeException("binlog stream status is not in Pending, can`t purge");
                } else {
                    binlogStreamAccessor.purgeStream(streamName);
                    cdcLogger.warn("binlog stream is purged, stream name is " + streamName);
                }
            }
        } catch (SQLException e) {
            cdcLogger.error("purge binary stream error", e);
            throw new RuntimeException("purge binary stream error", e);
        }
        return new AffectRowCursor(0);
    }
}

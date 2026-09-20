package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryUtil;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * show ttl query boundary实现
 */
public final class ShowTtlQueryBoundary {

    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("schema", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("table", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("queryBoundary", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("refColQueryBoundary", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

    }

    public static boolean execute(ServerConnection c, boolean hasMore) {
        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return false;
        }

        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return false;
        }

        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        byte tmpPacketId = packetId;
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        ExecutionContext ec = new ExecutionContext();
        Map<String, SchemaManager> schemaManagers = new ConcurrentHashMap<>();
        SchemaManager schemaManager = OptimizerContext.getContext(db).getLatestSchemaManager();
        schemaManagers.put(db, schemaManager);
        ec.setSchemaManagers(schemaManagers);
        ec.setTimeZone(c.getTimeZone());
        // write rows
        for (TableMeta tableMeta : schemaManager.getAllTables()) {
            if (tableMeta.getTtlDefinitionInfo() == null) {
                continue;
            }
            RowDataPacket row = getRow(db, tableMeta.getTableName(),
                TtlQueryUtil.getTtlQueryBoundary(ec, tableMeta.getSchemaName(), tableMeta.getTableName()),
                TtlQueryUtil.getRefColQueryBoundary(tableMeta), c.getResultSetCharset());
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }

    private static RowDataPacket getRow(String schema, String table, String curQueryBoundary,
                                        Map<String, String> curRefColQueryBoundary, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode(table, charset));

        // curQueryBoundary
        row.add(curQueryBoundary != null ? StringUtil.encode(curQueryBoundary, charset) : null);

        // curRefColQueryBoundary
        String curRefColStr = curRefColQueryBoundary != null ? curRefColQueryBoundary.toString() : null;
        row.add(curRefColStr != null ? StringUtil.encode(curRefColStr, charset) : null);

        return row;
    }
}
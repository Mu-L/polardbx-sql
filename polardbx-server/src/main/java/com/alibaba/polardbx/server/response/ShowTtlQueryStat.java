package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryStat;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryStatManager;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.Map;

/**
 * show ttl query stat实现
 */
public final class ShowTtlQueryStat {

    private static final int FIELD_COUNT = 6;
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

        fields[i] = PacketUtil.getField("ttlQueryCount", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("hitHotOnlyCount", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("hitColdOnlyCount", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("hitHotAndColdCount", Fields.FIELD_TYPE_LONG);
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

        // 获取TTL查询统计数据
        Map<Pair<String, String>, TtlQueryStat> ttlQueryStatMap =
            TtlQueryStatManager.getInstance().getTtlQueryStatMap();

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

        // write rows
        for (Map.Entry<Pair<String, String>, TtlQueryStat> entry : ttlQueryStatMap.entrySet()) {
            Pair<String, String> key = entry.getKey();
            TtlQueryStat stat = entry.getValue();
            RowDataPacket row = getRow(key.getKey(), key.getValue(), stat, c.getResultSetCharset());
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

    private static RowDataPacket getRow(String schema, String table, TtlQueryStat stat, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(schema, charset));
        row.add(StringUtil.encode(table, charset));
        row.add(LongUtil.toBytes(stat.getTtlQueryCount()));
        row.add(LongUtil.toBytes(stat.getHitHotOnlyCount()));
        row.add(LongUtil.toBytes(stat.getHitColdOnlyCount()));
        row.add(LongUtil.toBytes(stat.getHitHotAndColdCount()));
        return row;
    }
}
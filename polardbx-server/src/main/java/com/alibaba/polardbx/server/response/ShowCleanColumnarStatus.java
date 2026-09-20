/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowCleanColumnarStatus {
    private static final int FIELD_COUNT = 10;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;
    private static final int CLEAN_COLUMNAR_STATUS_FIELDS = 10;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("SCHEMA_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("TABLE_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("JOB_ID", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("DELETE_PROGRESS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("CURRENT_SPEED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("AVERAGE_SPEED", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("USED_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("REMAINING_TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("DELETE_PARTITIONS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("FINISHED_PHY_PARTITIONS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : FIELDS) {
            proxy = field.write(proxy);
        }

        byte tmpPacketId = packetId;
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        String schema = c.getSchema();

        try {
            List<byte[][]> resultList = generateStatusPacket(schema);
            for (byte[][] results : resultList) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                for (byte[] result : results) {
                    row.add(result);
                }
                row.packetId = ++tmpPacketId;
                proxy = row.write(proxy);
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "fail to fetch clean columnar status. ", e);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }

    public static List<byte[][]> generateStatusPacket(String logicalSchema)
        throws JsonProcessingException {
        List<byte[][]> resultsList = new ArrayList<>();

        Map<Long, String> jobState = new HashMap<>();
        // 获取 ddl_engine_task 中 DropPrimaryTblPartitionCleanColumnarDataTask 的进度
        List<DdlEngineTaskRecord> ddlRecords = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
            ddlEngineTaskAccessor.setConnection(metaDbConn);
            DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(metaDbConn);
            ddlEngineAccessor.setConnection(metaDbConn);

            ddlRecords = ddlEngineTaskAccessor.queryAllTaskByNames(false,
                Collections.singletonList("DropPrimaryTblPartitionCleanColumnarDataTask"));

            List<Long> jobIds = ddlRecords.stream().map(DdlEngineTaskRecord::getJobId).collect(Collectors.toList());
            for (Long jobId : jobIds) {
                DdlEngineRecord ddlEngineRecord = ddlEngineAccessor.query(jobId);
                jobState.put(jobId, ddlEngineRecord.state);
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "fail to fetch ddl task: " + logicalSchema, e);
        }

        for (DdlEngineTaskRecord record : ddlRecords) {
            String value = record.value;

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            TaskState task = mapper.readValue(value, TaskState.class);

            long partitionRows = queryPartitionRows(logicalSchema, task.getTableName(), task.getPartitionNames(),
                task.getIsSubPartition());
            double speed = task.currentTotalRows / (task.currentTotalTime / 1_000_000_000.0);

            double currentSpeed = 0;
            if (jobState.get(record.jobId) != null) {
                currentSpeed =
                    DdlState.RUNNING == DdlState.valueOf(jobState.get(record.jobId)) ? task.currentSpeed : 0;
            }

            byte[][] results = new byte[CLEAN_COLUMNAR_STATUS_FIELDS][];
            results[0] = record.schemaName.getBytes();
            results[1] = task.getTableName().getBytes();
            results[2] = String.valueOf(record.jobId).getBytes();
            results[3] = (task.currentTotalRows + "(Delete)/" + partitionRows + "(Total)").getBytes();
            results[4] = String.format("%.2f rows/s", currentSpeed).getBytes();
            results[5] = String.format("%.2f rows/s", speed).getBytes();
            results[6] = String.format("%.2f s", (double) task.currentTotalTime / 1_000_000_000.0).getBytes();
            results[7] = String.format("%.2f s",
                partitionRows > 0 ? (double) (partitionRows - task.currentTotalRows) / speed : 0.0).getBytes();
            results[8] = String.join(",", task.getPartitionNames()).getBytes();
            results[9] = task.getDnCleanedPhyPartSetMap().values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.joining(","))
                .getBytes();
            resultsList.add(results);
        }

        return resultsList;
    }

    private static Long queryPartitionRows(String schema, String table, List<String> partitions,
                                           Boolean isSubPartition) {
        if (partitions == null || partitions.isEmpty()) {
            return 0L;
        }

        String partitionField = (isSubPartition != null && isSubPartition) ? "SUBPARTITION_NAME" : "PARTITION_NAME";

        // 一次查询所有分区
        StringBuilder partitionConditions = new StringBuilder();
        for (int i = 0; i < partitions.size(); i++) {
            if (i > 0) {
                partitionConditions.append(" OR ");
            }
            // 二级分区名有一级分区前缀，一级分区无前缀
            if (isSubPartition != null && isSubPartition) {
                partitionConditions.append(partitionField).append(" like '%").append(partitions.get(i)).append("'");
            } else {
                partitionConditions.append(partitionField).append(" = '").append(partitions.get(i)).append("'");
            }
        }

        // 优化后的SQL：一次查询获取所有分区的行数总和
        final String querySql = String.format(
            "select sum(table_rows) as rows from information_schema.PARTITIONS where table_schema = '%s' and table_name = '%s' and (%s)",
            schema, table, partitionConditions
        );

        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(querySql, schema, null);

        if (!result.isEmpty() && result.get(0) != null && result.get(0).get("rows") != null) {
            return ((Decimal) result.get(0).get("rows")).longValue();
        }

        return 0L;
    }

    @Getter
    @Setter
    public static class TaskState {
        private Map<String, Set<String>> dnCleanedPhyPartSetMap = new HashMap<>();
        private Map<String, Map<String, List<Object>>> dnPartitionToLastPKMap = new HashMap<>();
        private Integer currentTotalRows;
        private Long currentTotalTime;
        private Double currentSpeed;
        private String tableName;
        private List<String> partitionNames = new ArrayList<>();
        private Long batchInterval;
        private Long batchSize;
        private String description;
        private String shadowTableName;
        private Boolean isSubPartition;
        private String name;
        private String rankHint = "";
        public String state;
    }
}
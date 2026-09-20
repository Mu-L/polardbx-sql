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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.columnar.ColumnarDataSourceMetrics;
import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.memory.GlobalMemoryTrackerManager;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.orc.PreheatMetaManager;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheStats;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.columnar.CSVFileStatistics;
import com.alibaba.polardbx.executor.columnar.pruning.ColumnarPruneManager;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.MppTaskMetrics;
import com.alibaba.polardbx.executor.mpp.execution.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.common.trx.ITimestampOracle;
import com.alibaba.polardbx.server.util.IntegerUtil;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * show @@columnar_r
 * View columnar read related metrics
 */
public final class ShowColumnarRead {

    private static final int FIELD_COUNT = 114;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("QUERY_LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOGICAL_MEMORY_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOGICAL_MEMORY_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_CACHE_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_CACHE_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_USED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_CSV_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_DEL_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PRUNE_CACHE_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PREHEAT_META_USED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_USED_SIZE_ON_DISK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_MAX_SIZE_ON_DISK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_CACHE_MISS_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("BLOCK_CACHE_HIT_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_MISS_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("VERSION_CACHE_HIT_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_MISS_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_HIT_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INCREMENT_OPEN_FD", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SNAPSHOT_OPEN_FD", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOADED_VERSION_FILES", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOADED_VERSION_NUM", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INCREMENT_FILE_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SNAPSHOT_FILE_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PURGE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_READ_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_READ_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("IO_THREAD_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("IO_THREAD_ACTIVE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCAN_THREAD_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCAN_THREAD_ACTIVE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TP_EXEC_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("AP_EXEC_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXEC_BLOCK_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_TRANSFER_ACTIVE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_TRANSFER_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // Add new fields for additional metrics
        fields[i] = PacketUtil.getField("BYTES_READ", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REQUEST_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REQUEST_HIT_COLUMNAR", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REQUEST_HIT_OSS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CSV_RT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("COLUMNAR_CSV_RT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_CSV_RT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // MPP Thread Pool Metrics - Exchange (3 fields: active + max_threads + queue)
        fields[i] = PacketUtil.getField("HTTP_CLIENT_EXCHANGE_ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_EXCHANGE_MAX_THREADS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_EXCHANGE_QUEUE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        // MPP Thread Pool Metrics - Scheduler (3 fields: active + max_threads + queue)
        fields[i] = PacketUtil.getField("HTTP_CLIENT_SCHEDULER_ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_SCHEDULER_MAX_THREADS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_SCHEDULER_QUEUE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        // MPP Client Connection Metrics (4 fields: active + idle + max_connections + max_per_server)
        fields[i] = PacketUtil.getField("HTTP_CLIENT_ACTIVE_CONNECTION_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_IDLE_CONNECTION_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_MAX_CONNECTIONS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        // MPP Client Request Metrics (3 fields: request_count + queue + max_requests_per_destination)
        fields[i] = PacketUtil.getField("HTTP_CLIENT_REQUEST_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_QUEUE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_MAX_REQUESTS_PER_DESTINATION", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        // MPP Client Data Flow Metrics (5 fields: rows + pages + bytes + throughput + timing)
        fields[i] = PacketUtil.getField("HTTP_CLIENT_INPUT_ROW_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_INPUT_PAGE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_INPUT_BYTES", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_THROUGHPUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_RESPONSE_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_CLIENT_WAIT_CONNECTION_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // MPP Server Metrics (5 fields: active + max_threads + active_conn + queue + idle)
        fields[i] = PacketUtil.getField("HTTP_WORKER_ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_WORKER_MAX_THREADS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_WORKER_ACTIVE_CONNECTION_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_WORKER_QUEUE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HTTP_WORKER_IDLE_CONNECTION_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        // Scan Monitoring
        fields[i] = PacketUtil.getField("TOTAL_SCAN_BYTES", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_SCAN_ROWS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_SCAN_FILTERED_ROWS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // Columnar Data Source Latency Metrics
        fields[i] = PacketUtil.getField("ORC_QUERY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ORC_QUERY_LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CSV_QUERY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CSV_QUERY_LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DEL_QUERY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DEL_QUERY_LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("GMS_QUERY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("GMS_QUERY_LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // MPP Task Metrics
        fields[i] = PacketUtil.getField("EXEC_PENDING_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_PRE_PREPROCESSOR_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_SCAN_IO_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_PIPELINE_DEPENDENCY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_DRIVER_CONSUMER_FINISHED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_MEMORY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_EXCHANGE_CLIENT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_NO_MORE_SPLIT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_LOCAL_BUFFER_NOT_EMPTY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_LOCAL_BUFFER_NOT_FULL_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_PRODUCER_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_MEMORY_REVOKE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_BLOOM_FILTER_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_SPILL_WRITE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_SPILL_READ_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_WAIT_FOR_PARALLEL_BUILD_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // V3 Metrics - Version Cache Details
        fields[i] = PacketUtil.getField("VERSION_CACHE_CSV_USED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_DEL_USED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILE_SNAPSHOT_META_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILE_SNAPSHOT_META_USED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // V3 Additional Metrics - Memory Management
        fields[i] = PacketUtil.getField("PREHEAT_META_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_MEMORY_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_MEMORY_USED_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        // V4 Additional Executor Memory Metrics
        fields[i] = PacketUtil.getField("EXECUTOR_MEMORY_QUOTA_USAGE_RATIO", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_MEMORY_AVAILABLE_QUOTA", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_MAX_QUERY_MEMORY_PEAK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_AVG_QUERY_MEMORY_USAGE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_ACTIVE_PIPELINE_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_ACTIVE_DRIVER_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_ACTIVE_OPERATOR_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_TOTAL_ALLOCATED_MEMORY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTOR_TOTAL_FREE_MEMORY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        // write rows
        byte packetId = eof.packetId;
        RowDataPacket row = getStats(c.getResultSetCharset());
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getStats(String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);

        FileVersionStorage fileVersionStorage = DynamicColumnarManager.getInstance().getVersionStorage();
        OSSFileSystem ossFs = getOssFileSystem();
        CacheManager ossCacheManager = getOssCacheManager();

        // Add Metrics (V1)
        addLatency(row);
        addLogicalMemory(row);
        addBlockCache(row);
        addVersionCache(row, fileVersionStorage);
        addPruneCache(row);
        addPreheatMeta(row);
        addFileSystemOnDisk(row, ossCacheManager);
        addFileSystemInMemory(row, ossCacheManager);
        addBlockMissCount(row, charset);
        addVersionMissCount(row, charset, fileVersionStorage);
        addFsMissCount(row, charset, ossCacheManager);
        addFileDescriptor(row, fileVersionStorage);
        addLoadedVersion(row);
        addFileReq(row);
        addPurgeCount(row);
        addOssRead(row, ossFs);
        addColumnarScanPool(row);
        addExecPool(row);
        addOssTransferPool(row);

        // Add Metrics (V2)
        addBytesRead(row);
        addRequestCount(row);
        addRequestHitColumnar(row);
        addRequestHitOSS(row);
        addCsvRt(row);
        addColumnarCsvRt(row);
        addOssCsvRt(row);

        // Add Metrics (V3)

        // Memory Management:
        // CSV file cache count: version_cache_csv_used_count (count)
        // DEL file cache count: version_cache_del_used_count (count)
        // Preheat meta cache memory usage: preheat_meta_used_size (bytes)
        // Executor runtime memory usage: executor_memory_used_size (bytes)
        // Executor runtime query count: executor_memory_used_count (count)
        // File snapshot cache memory usage: file_snapshot_meta_used_size (bytes)
        // File snapshot cache count: file_snapshot_meta_used_count (count)

        // Columnar Data Source Latency:
        // ORC access total latency: orc_query_latency_ms (milliseconds)
        // ORC access total count: orc_query_count (count)
        // CSV access total latency: csv_query_latency_ms (milliseconds)
        // CSV access total count: csv_query_count (count)
        // DEL access total latency: del_query_latency_ms (milliseconds)
        // DEL access total count: del_query_count (count)
        // GMS metadata query total latency: gms_query_latency_ms (milliseconds)
        // GMS metadata query total count: gms_query_count (count)

        // Scan Monitoring:
        // Total scan bytes: total_scan_bytes (bytes)
        // Total scan rows: total_scan_rows (rows)
        // Total filtered rows: total_scan_filtered_rows (rows)

        // MPP Thread Monitoring:
        // http-client-exchange active thread count: http_client_exchange_active_count (count)
        // http-client-exchange queued task count: http_client_exchange_queue_count (count)
        // http-client-scheduler active thread count: http_client_scheduler_active_count (count)
        // http-client-scheduler queued task count: http_client_scheduler_queue_count (count)

        // Cluster Inter-node Network Communication (Client Side) - Data Flow Monitoring:
        // http-client active connection count: http_client_active_connection_count (count)
        // http-client idle connection count: http_client_idle_connection_count (count)
        // http-client total request count: http_client_request_count (count)
        // http-client queued request count: http_client_queue_count (count)
        // http-client input row count: http_client_input_row_count (count)
        // http-client input page count: http_client_input_page_count (count)
        // http-client input bytes: http_client_input_bytes (bytes)
        // http-client total bandwidth accumulated: http_client_throughput (bytes)
        // http-client request response time: http_client_response_time (milliseconds)
        // http-client wait connection time: http_client_wait_connection_time (milliseconds)

        // Cluster Inter-node Network Communication (Server Side) - Control Flow Monitoring:
        // http-worker active thread count: http_worker_active_count (count)
        // http-worker concurrent request count: http_worker_active_connection_count (count)
        // http-worker queue count: http_worker_queue_count (count)
        // http-worker idle thread count: http_worker_idle_connection_count (count)

        // MPP Task Metrics:
        // Ready task count: exec_pending_queue (count)
        // wait_for_pre_preprocessor blocked task count: block_wait_for_pre_preprocessor_count (count)
        // wait_for_scan_io blocked task count: block_wait_for_scan_io_count (count)
        // wait_pipeline_dependency blocked task count: block_wait_pipeline_dependency_count (count)
        // wait_driver_consumer_finished blocked task count: block_wait_driver_consumer_finished_count (count)
        // wait_for_memory blocked task count: block_wait_for_memory_count (count)
        // wait_for_exchange_client blocked task count: block_wait_for_exchange_client_count (count)
        // wait_for_no_more_split blocked task count: block_wait_for_no_more_split_count (count)
        // local_buffer_not_empty blocked task count: block_local_buffer_not_empty_count (count)
        // local_buffer_not_full blocked task count: block_local_buffer_not_full_count (count)
        // wait_for_producer blocked task count: block_wait_for_producer_count (count)
        // local_buffer_not_full类型阻塞任务数	block_local_buffer_not_full_count	个	内核直接提供
        // wait_for_producer类型阻塞任务数	block_wait_for_producer_count	个	内核直接提供

        // Add MPP Metrics (with thresholds interleaved)
        addMppMetricsWithThresholds(row);

        // Add Scan Monitoring Metrics
        addColumnarScanMetrics(row);

        // Add Columnar Data Source Latency Metrics
        addColumnarDataSourceMetrics(row);

        // Add MPP Task Metrics
        addMppTaskMetrics(row);

        // Add V3 Metrics - Version Cache Details
        addVersionCacheDetailsMetrics(row);

        // Add V3 Additional Metrics - Memory Management
        addMemoryManagementMetrics(row);

        // Add V4 Additional Executor Memory Metrics
        addExecutorMemoryMetrics(row);

        return row;
    }

    /**
     * LATENCY_MS
     * QUERY_LATENCY_MS
     */
    private static void addLatency(RowDataPacket row) {
        // columnar node will write heartbeat tso
        Long columnarTso = ColumnarTransactionUtils.getLatestTsoFromGms();
        long latestTso = ColumnarManager.getInstance().latestTso();
        long curTimeMillis = System.currentTimeMillis();
        if (columnarTso == null) {
            row.add(null);
        } else {
            long columnarTsoMillis = ITimestampOracle.getTimeMillis(columnarTso);
            row.add(LongUtil.toBytes(curTimeMillis - columnarTsoMillis));
        }

        if (latestTso <= 0) {
            row.add(null);
        } else {
            long queryTsoMillis = ITimestampOracle.getTimeMillis(latestTso);
            row.add(LongUtil.toBytes(curTimeMillis - queryTsoMillis));
        }
    }

    /**
     * Stats from global memory pool:
     * LOGICAL_MEMORY_USED_SIZE
     * LOGICAL_MEMORY_MAX_SIZE
     */
    private static void addLogicalMemory(RowDataPacket row) {
        MemoryPool globalMemoryPool = MemoryManager.getInstance().getGlobalMemoryPool();
        long limit = globalMemoryPool.getMaxLimit();
        if (limit == MemorySetting.UNLIMITED_SIZE) {
            // -1 represents unlimited
            limit = -1;
        }
        row.add(LongUtil.toBytes(globalMemoryPool.getMemoryUsage()));
        row.add(LongUtil.toBytes(limit));
    }

    /**
     * Cache directory on disk
     * FILESYSTEM_USED_SIZE_ON_DISK
     * FILESYSTEM_MAX_SIZE_ON_DISK
     */
    private static void addFileSystemOnDisk(RowDataPacket row, CacheManager cacheManager) {
        if (cacheManager instanceof FileMergeCacheManager) {
            FileMergeCacheManager fmCacheManager = (FileMergeCacheManager) cacheManager;
            row.add(LongUtil.toBytes(
                fmCacheManager.calcCacheSize().longValue()));
            row.add(LongUtil.toBytes(
                FileConfig.getInstance().getMergeCacheConfig().getMaxInDiskCacheSize().toBytes()));
            return;
        }

        row.add(null);
        row.add(null);
    }

    /**
     * Cache in memory, size in bytes
     * FILESYSTEM_CACHE_USED_SIZE
     * FILESYSTEM_CACHE_MAX_SIZE
     */
    private static void addFileSystemInMemory(RowDataPacket row, CacheManager cacheManager) {
        if (cacheManager instanceof FileMergeCacheManager) {
            FileMergeCacheManager fmCacheManager = (FileMergeCacheManager) cacheManager;
            row.add(LongUtil.toBytes(
                fmCacheManager.getStats().getInMemoryRetainedBytes()));
            row.add(LongUtil.toBytes(
                FileConfig.getInstance().getMergeCacheConfig().getMaxInMemoryCacheSize().toBytes()));
            return;
        }

        row.add(null);
        row.add(null);
    }

    /**
     * BLOCK_CACHE_USED_SIZE
     * BLOCK_CACHE_MAX_SIZE
     */
    private static void addBlockCache(RowDataPacket row) {
        BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();
        row.add(LongUtil.toBytes(blockCacheManager.getMemorySize()));
        row.add(LongUtil.toBytes(blockCacheManager.getMaximumMemorySize()));
    }

    /**
     * VERSION_CACHE_USED_COUNT
     * VERSION_CACHE_MAX_SIZE
     * VERSION_CACHE_CSV_USED_SIZE
     * VERSION_CACHE_DEL_USED_SIZE
     */
    private static void addVersionCache(RowDataPacket row, FileVersionStorage fileVersionStorage) {
        row.add(LongUtil.toBytes(fileVersionStorage.getUsedCacheSize()));
        row.add(LongUtil.toBytes(fileVersionStorage.getMaxCacheSize()));
        row.add(LongUtil.toBytes(fileVersionStorage.getCsvCacheSizeInBytes()));
        row.add(LongUtil.toBytes(fileVersionStorage.getDelCacheSizeInBytes()));
    }

    /**
     * PRUNE_CACHE_USED_SIZE
     */
    private static void addPruneCache(RowDataPacket row) {
        row.add(LongUtil.toBytes(ColumnarPruneManager.getPruneCacheUsedSize()));
    }

    /**
     * PREHEAT_META_USED_COUNT
     */
    private static void addPreheatMeta(RowDataPacket row) {
        row.add(LongUtil.toBytes(PreheatMetaManager.getInstance().entries()));
    }

    /**
     * BLOCK_CACHE_MISS_COUNT
     */
    private static void addBlockMissCount(RowDataPacket row, String charset) {
        BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();
        long missCount = blockCacheManager.getMissCount();
        row.add(LongUtil.toBytes(missCount));
        long hitCount = blockCacheManager.getHitCount();
        row.add(LongUtil.toBytes(hitCount));
    }

    /**
     * VERSION_CACHE_MISS_COUNT
     */
    private static void addVersionMissCount(RowDataPacket row, String charset, FileVersionStorage fileVersionStorage) {
        long missCount = fileVersionStorage.getMissCount();
        row.add(LongUtil.toBytes(missCount));
        long hitCount = fileVersionStorage.getHitCount();
        row.add(LongUtil.toBytes(hitCount));
    }

    /**
     * FILESYSTEM_CACHE_MISS_COUNT
     */
    private static void addFsMissCount(RowDataPacket row, String charset, CacheManager ossCacheManager) {
        if (ossCacheManager != null) {
            CacheStats cacheStats = ((FileMergeCacheManager) ossCacheManager).getStats();
            long missCount = cacheStats.getCacheMiss();
            row.add(LongUtil.toBytes(missCount));
            long hitCount = cacheStats.getCacheHit();
            row.add(LongUtil.toBytes(hitCount));
            return;
        }

        row.add(null);
        row.add(null);
    }

    /**
     * INCREMENT_OPEN_FD
     * SNAPSHOT_OPEN_FD
     */
    private static void addFileDescriptor(RowDataPacket row, FileVersionStorage fileVersionStorage) {
        row.add(LongUtil.toBytes(fileVersionStorage.getOpenedIncrementFileCount()));
        row.add(LongUtil.toBytes(getSnapshotFd()));
    }

    /**
     * LOADED_VERSION_FILES
     * LOADED_VERSION_NUM
     */
    private static void addLoadedVersion(RowDataPacket row) {
        DynamicColumnarManager columnarManager = DynamicColumnarManager.getInstance();
        row.add(LongUtil.toBytes(columnarManager.getLoadedAppendFileCount()));
        row.add(LongUtil.toBytes(columnarManager.getLoadedVersionCount()));
    }

    /**
     * INCREMENT_FILE_REQUEST
     * SNAPSHOT_FILE_REQUEST
     */
    private static void addFileReq(RowDataPacket row) {
        DynamicColumnarManager columnarManager = DynamicColumnarManager.getInstance();
        row.add(LongUtil.toBytes(columnarManager.getAppendFileAccessCount()));
        row.add(LongUtil.toBytes(ColumnarScanExec.getSnapshotFileAccessCount()));
    }

    /**
     * PURGE_COUNT
     */
    private static void addPurgeCount(RowDataPacket row) {
        DynamicColumnarManager columnarManager = DynamicColumnarManager.getInstance();
        row.add(LongUtil.toBytes(columnarManager.getPurgeCount()));
    }

    /**
     * OSS_READ_REQUEST
     * OSS_READ_SIZE
     */
    private static void addOssRead(RowDataPacket row, OSSFileSystem ossFileSystem) {
        if (ossFileSystem != null) {
            FileSystem.Statistics statistics = ossFileSystem.getStore().getStatistics();
            if (statistics != null) {
                row.add(LongUtil.toBytes(statistics.getReadOps()));
                row.add(LongUtil.toBytes(statistics.getBytesRead()));
                return;
            }
        }

        // OSS statistics does not exist
        row.add(null);
        row.add(null);
    }

    /**
     * IO_THREAD_QUEUE
     * IO_THREAD_ACTIVE
     * SCAN_THREAD_QUEUE
     * SCAN_THREAD_ACTIVE
     */
    private static void addColumnarScanPool(RowDataPacket row) {
        ExecutorService ioExecutor = ColumnarScanExec.getIoExecutor();
        if (ioExecutor instanceof ThreadPoolExecutor) {
            long queueSize = ((ThreadPoolExecutor) ioExecutor).getQueue().size();
            row.add(LongUtil.toBytes(queueSize));

            long activeCount = ((ThreadPoolExecutor) ioExecutor).getActiveCount();
            row.add(LongUtil.toBytes(activeCount));
        } else {
            row.add(null);
            row.add(null);
        }

        ExecutorService scanExecutor = ColumnarScanExec.getScanExecutor();
        if (scanExecutor instanceof ThreadPoolExecutor) {
            long queueSize = ((ThreadPoolExecutor) scanExecutor).getQueue().size();
            row.add(LongUtil.toBytes(queueSize));

            long activeCount = ((ThreadPoolExecutor) scanExecutor).getActiveCount();
            row.add(LongUtil.toBytes(activeCount));
        } else {
            row.add(null);
            row.add(null);
        }
    }

    /**
     * TP_EXEC_QUEUE
     * AP_EXEC_QUEUE
     * EXEC_BLOCK_QUEUE
     */
    private static void addExecPool(RowDataPacket row) {
        if (ServiceProvider.getInstance().getServer() != null) {
            TaskExecutor priorityExecutor = ServiceProvider.getInstance().getServer().getTaskExecutor();
            PriorityExecutorInfo tpInfo = priorityExecutor.getHighPriorityInfo();
            PriorityExecutorInfo apInfo = priorityExecutor.getLowPriorityInfo();

            row.add(LongUtil.toBytes(tpInfo.getActiveCount()));
            row.add(LongUtil.toBytes(apInfo.getActiveCount()));
            row.add(LongUtil.toBytes(tpInfo.getBlockedSplitSize() + apInfo.getBlockedSplitSize()));
            return;
        }
        // fill default values
        row.add(LongUtil.toBytes(0));
        row.add(LongUtil.toBytes(0));
        row.add(LongUtil.toBytes(0));
    }

    private static void addOssTransferPool(RowDataPacket row) {
        OSSFileSystem ossFs = getOssFileSystem();
        if (ossFs != null) {
            BlockingThreadPoolExecutorService service = ossFs.getBoundedThreadPool();
            if (service != null) {
                row.add(IntegerUtil.toBytes(service.getPermitCount() - service.getAvailablePermits()));
                row.add(IntegerUtil.toBytes(service.getWaitingCount()));
                return;
            }
        }
        // fill default values
        row.add(LongUtil.toBytes(0));
        row.add(LongUtil.toBytes(0));
    }

    private static long getSnapshotFd() {
        return 0;
    }

    private static long getIncrementFd() {
        return 0;
    }

    private static CacheManager getOssCacheManager() {
        FileSystemGroup group = FileSystemManager.getFileSystemGroup(Engine.OSS, false);
        if (group != null) {
            FileSystem masterFs = group.getMaster();
            if (masterFs instanceof FileMergeCachingFileSystem) {
                return ((FileMergeCachingFileSystem) masterFs).getCacheManager();
            }
        }
        return null;
    }

    public static OSSFileSystem getOssFileSystem() {
        FileSystemGroup group = FileSystemManager.getFileSystemGroup(Engine.OSS, false);
        if (group != null) {
            FileSystem masterFs = group.getMaster();
            if (masterFs instanceof OSSFileSystem) {
                return (OSSFileSystem) masterFs;
            } else if (masterFs instanceof CachingFileSystem) {
                FileSystem dataFileSystem = ((CachingFileSystem) masterFs).getDataTier();
                if (dataFileSystem instanceof OSSFileSystem) {
                    return (OSSFileSystem) dataFileSystem;
                }
            }
        }
        return null;
    }

    /**
     * BYTES_READ
     */
    private static void addBytesRead(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getTotalBytesRead()));
    }

    /**
     * REQUEST_COUNT
     */
    private static void addRequestCount(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getTotalAccessCount()));
    }

    /**
     * REQUEST_HIT_COLUMNAR
     */
    private static void addRequestHitColumnar(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getColumnarAccessCount()));
    }

    /**
     * REQUEST_HIT_OSS
     */
    private static void addRequestHitOSS(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getFileSystemAccessCount()));
    }

    /**
     * CSV_RT
     */
    private static void addCsvRt(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getTotalReadTimeMs()));
    }

    /**
     * COLUMNAR_CSV_RT
     */
    private static void addColumnarCsvRt(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getColumnarReadTimeMs()));
    }

    /**
     * OSS_CSV_RT
     */
    private static void addOssCsvRt(RowDataPacket row) {
        row.add(LongUtil.toBytes(CSVFileStatistics.getInstance().getFileSystemReadTimeMs()));
    }

    /**
     * MPP Metrics with Thresholds Interleaved
     * Combines all MPP metrics with their corresponding thresholds in adjacent positions
     * This makes it easier to calculate usage ratios and monitor resource utilization
     */
    private static void addMppMetricsWithThresholds(RowDataPacket row) {
        // Get MppConfig for threshold values
        com.alibaba.polardbx.common.properties.MppConfig mppConfig =
            com.alibaba.polardbx.common.properties.MppConfig.getInstance();

        // ========== Exchange HttpClient Thread Pool ==========
        addExchangeThreadPoolMetrics(row, mppConfig);

        // ========== Scheduler HttpClient Thread Pool ==========
        addSchedulerThreadPoolMetrics(row, mppConfig);

        // ========== Client Connection Metrics ==========
        addClientConnectionMetrics(row, mppConfig);

        // ========== Client Request Metrics ==========
        addClientRequestMetrics(row, mppConfig);

        // ========== Client Data Flow Metrics ==========
        addClientDataFlowMetrics(row);

        // ========== Server Metrics ==========
        addServerMetrics(row, mppConfig);
    }

    /**
     * Exchange HttpClient Thread Pool Metrics
     * HTTP_CLIENT_EXCHANGE_ACTIVE_COUNT
     * HTTP_CLIENT_EXCHANGE_MAX_THREADS
     * HTTP_CLIENT_EXCHANGE_QUEUE_COUNT
     */
    private static void addExchangeThreadPoolMetrics(RowDataPacket row,
                                                     com.alibaba.polardbx.common.properties.MppConfig mppConfig) {
        int exchangeActiveCount = 0;
        int exchangeQueueSize = 0;

        try {
            if (ServiceProvider.getInstance().getServer() != null
                && ServiceProvider.getInstance()
                .getServer() instanceof com.alibaba.polardbx.executor.mpp.deploy.MppServer) {
                com.alibaba.polardbx.executor.mpp.deploy.MppServer mppServer =
                    (com.alibaba.polardbx.executor.mpp.deploy.MppServer) ServiceProvider.getInstance().getServer();

                com.google.inject.Injector injector = mppServer.getInjector();
                if (injector != null) {
                    io.airlift.http.client.HttpClient exchangeHttpClient =
                        injector.getInstance(com.google.inject.Key.get(
                            io.airlift.http.client.HttpClient.class,
                            com.alibaba.polardbx.executor.mpp.operator.ForExchange.class));

                    if (exchangeHttpClient != null) {
                        try {
                            java.lang.reflect.Field httpClientField =
                                exchangeHttpClient.getClass().getDeclaredField("httpClient");
                            httpClientField.setAccessible(true);
                            Object jettyHttpClient = httpClientField.get(exchangeHttpClient);

                            if (jettyHttpClient != null) {
                                java.lang.reflect.Method getExecutorMethod =
                                    jettyHttpClient.getClass().getMethod("getExecutor");
                                Object executor = getExecutorMethod.invoke(jettyHttpClient);

                                if (executor instanceof org.eclipse.jetty.util.thread.QueuedThreadPool) {
                                    org.eclipse.jetty.util.thread.QueuedThreadPool queuedThreadPool =
                                        (org.eclipse.jetty.util.thread.QueuedThreadPool) executor;
                                    exchangeActiveCount = queuedThreadPool.getBusyThreads();
                                    exchangeQueueSize = queuedThreadPool.getQueueSize();
                                }
                            }
                        } catch (Exception e) {
                            // Reflection failed
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Error getting metrics
        }

        // Add Exchange metrics with thresholds
        row.add(IntegerUtil.toBytes(exchangeActiveCount));
        row.add(IntegerUtil.toBytes(mppConfig.getHttpClientMaxThreads()));
        row.add(IntegerUtil.toBytes(exchangeQueueSize));
    }

    /**
     * Scheduler HttpClient Thread Pool Metrics
     * HTTP_CLIENT_SCHEDULER_ACTIVE_COUNT
     * HTTP_CLIENT_SCHEDULER_MAX_THREADS
     * HTTP_CLIENT_SCHEDULER_QUEUE_COUNT
     */
    private static void addSchedulerThreadPoolMetrics(RowDataPacket row,
                                                      com.alibaba.polardbx.common.properties.MppConfig mppConfig) {
        int schedulerActiveCount = 0;
        int schedulerQueueSize = 0;

        try {
            if (ServiceProvider.getInstance().getServer() != null
                && ServiceProvider.getInstance()
                .getServer() instanceof com.alibaba.polardbx.executor.mpp.deploy.MppServer) {
                com.alibaba.polardbx.executor.mpp.deploy.MppServer mppServer =
                    (com.alibaba.polardbx.executor.mpp.deploy.MppServer) ServiceProvider.getInstance().getServer();

                com.google.inject.Injector injector = mppServer.getInjector();
                if (injector != null) {
                    io.airlift.http.client.HttpClient schedulerHttpClient =
                        injector.getInstance(com.google.inject.Key.get(
                            io.airlift.http.client.HttpClient.class,
                            com.alibaba.polardbx.executor.mpp.operator.ForScheduler.class));

                    if (schedulerHttpClient != null) {
                        try {
                            java.lang.reflect.Field httpClientField =
                                schedulerHttpClient.getClass().getDeclaredField("httpClient");
                            httpClientField.setAccessible(true);
                            Object jettyHttpClient = httpClientField.get(schedulerHttpClient);

                            if (jettyHttpClient != null) {
                                java.lang.reflect.Method getExecutorMethod =
                                    jettyHttpClient.getClass().getMethod("getExecutor");
                                Object executor = getExecutorMethod.invoke(jettyHttpClient);

                                if (executor instanceof org.eclipse.jetty.util.thread.QueuedThreadPool) {
                                    org.eclipse.jetty.util.thread.QueuedThreadPool queuedThreadPool =
                                        (org.eclipse.jetty.util.thread.QueuedThreadPool) executor;
                                    schedulerActiveCount = queuedThreadPool.getBusyThreads();
                                    schedulerQueueSize = queuedThreadPool.getQueueSize();
                                }
                            }
                        } catch (Exception e) {
                            // Reflection failed
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Error getting metrics
        }

        // Add Scheduler metrics with thresholds
        row.add(IntegerUtil.toBytes(schedulerActiveCount));
        row.add(IntegerUtil.toBytes(mppConfig.getHttpClientMaxThreads()));
        row.add(IntegerUtil.toBytes(schedulerQueueSize));
    }

    /**
     * Client Connection Metrics
     * HTTP_CLIENT_ACTIVE_CONNECTION_COUNT
     * HTTP_CLIENT_IDLE_CONNECTION_COUNT
     * HTTP_CLIENT_MAX_CONNECTIONS
     * HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER
     */
    private static void addClientConnectionMetrics(RowDataPacket row,
                                                   com.alibaba.polardbx.common.properties.MppConfig mppConfig) {
        try {
            io.airlift.http.client.HttpClient httpClient =
                com.alibaba.polardbx.executor.mpp.execution.HttpClientUtil.getExchangeHttpClient();

            com.alibaba.polardbx.executor.mpp.execution.MppMetricsCollector.MppClientMetrics metrics =
                com.alibaba.polardbx.executor.mpp.execution.MppMetricsCollector.getInstance()
                    .getMppMetrics(httpClient);

            row.add(IntegerUtil.toBytes(metrics.getActiveConnectionCount()));
            row.add(IntegerUtil.toBytes(metrics.getIdleConnectionCount()));
            row.add(IntegerUtil.toBytes(mppConfig.getHttpClientMaxConnections()));
            row.add(IntegerUtil.toBytes(mppConfig.getDefaultMppHttpClientMaxConnectionsPerServer()));
        } catch (Exception e) {
            row.add(IntegerUtil.toBytes(0));
            row.add(IntegerUtil.toBytes(0));
            row.add(IntegerUtil.toBytes(mppConfig.getHttpClientMaxConnections()));
            row.add(IntegerUtil.toBytes(mppConfig.getDefaultMppHttpClientMaxConnectionsPerServer()));
        }
    }

    /**
     * Client Request Metrics
     * HTTP_CLIENT_REQUEST_COUNT
     * HTTP_CLIENT_QUEUE_COUNT
     * HTTP_CLIENT_MAX_REQUESTS_PER_DESTINATION
     */
    private static void addClientRequestMetrics(RowDataPacket row,
                                                com.alibaba.polardbx.common.properties.MppConfig mppConfig) {
        try {
            io.airlift.http.client.HttpClient httpClient =
                com.alibaba.polardbx.executor.mpp.execution.HttpClientUtil.getExchangeHttpClient();

            com.alibaba.polardbx.executor.mpp.execution.MppMetricsCollector.MppClientMetrics metrics =
                com.alibaba.polardbx.executor.mpp.execution.MppMetricsCollector.getInstance()
                    .getMppMetrics(httpClient);

            row.add(LongUtil.toBytes(metrics.getRequestCount()));
            row.add(IntegerUtil.toBytes(metrics.getQueueCount()));
            row.add(IntegerUtil.toBytes(mppConfig.getHttpMaxRequestsPerDestination()));
        } catch (Exception e) {
            row.add(LongUtil.toBytes(0L));
            row.add(IntegerUtil.toBytes(0));
            row.add(IntegerUtil.toBytes(mppConfig.getHttpMaxRequestsPerDestination()));
        }
    }

    /**
     * Client Data Flow Metrics (no thresholds needed)
     * HTTP_CLIENT_INPUT_ROW_COUNT
     * HTTP_CLIENT_INPUT_PAGE_COUNT
     * HTTP_CLIENT_INPUT_BYTES
     * HTTP_CLIENT_THROUGHPUT
     * HTTP_CLIENT_RESPONSE_TIME
     * HTTP_CLIENT_WAIT_CONNECTION_TIME
     */
    private static void addClientDataFlowMetrics(RowDataPacket row) {
        try {
            io.airlift.http.client.HttpClient httpClient =
                com.alibaba.polardbx.executor.mpp.execution.HttpClientUtil.getExchangeHttpClient();

            com.alibaba.polardbx.executor.mpp.execution.MppMetricsCollector.MppClientMetrics metrics =
                com.alibaba.polardbx.executor.mpp.execution.MppMetricsCollector.getInstance()
                    .getMppMetrics(httpClient);

            row.add(LongUtil.toBytes(metrics.getInputRowCount()));
            row.add(LongUtil.toBytes(metrics.getInputPageCount()));
            row.add(LongUtil.toBytes(metrics.getInputBytes()));
            row.add(LongUtil.toBytes(metrics.getThroughput()));
            row.add(LongUtil.toBytes(metrics.getAvgResponseTimeMs()));
            row.add(LongUtil.toBytes(metrics.getAvgWaitConnectionTimeMs()));
        } catch (Exception e) {
            for (int i = 0; i < 6; i++) {
                row.add(LongUtil.toBytes(0L));
            }
        }
    }

    /**
     * Server Metrics
     * HTTP_WORKER_ACTIVE_COUNT
     * HTTP_WORKER_MAX_THREADS
     * HTTP_WORKER_ACTIVE_CONNECTION_COUNT
     * HTTP_WORKER_QUEUE_COUNT
     * HTTP_WORKER_IDLE_CONNECTION_COUNT
     */
    private static void addServerMetrics(RowDataPacket row,
                                         com.alibaba.polardbx.common.properties.MppConfig mppConfig) {
        int busyThreads = 0;
        int idleThreads = 0;
        int queueSize = 0;
        int activeConnections = 0;

        try {
            if (ServiceProvider.getInstance().getServer() != null
                && ServiceProvider.getInstance()
                .getServer() instanceof com.alibaba.polardbx.executor.mpp.deploy.MppServer) {
                com.alibaba.polardbx.executor.mpp.deploy.MppServer mppServer =
                    (com.alibaba.polardbx.executor.mpp.deploy.MppServer) ServiceProvider.getInstance().getServer();

                com.google.inject.Injector injector = mppServer.getInjector();
                if (injector != null) {
                    io.airlift.http.server.HttpServer httpServer =
                        injector.getInstance(io.airlift.http.server.HttpServer.class);

                    if (httpServer != null) {
                        try {
                            java.lang.reflect.Field serverField =
                                io.airlift.http.server.HttpServer.class.getDeclaredField("server");
                            serverField.setAccessible(true);
                            Object jettyServer = serverField.get(httpServer);

                            if (jettyServer instanceof org.eclipse.jetty.server.Server) {
                                org.eclipse.jetty.server.Server server = (org.eclipse.jetty.server.Server) jettyServer;
                                org.eclipse.jetty.util.thread.ThreadPool threadPool = server.getThreadPool();

                                if (threadPool instanceof org.eclipse.jetty.util.thread.QueuedThreadPool) {
                                    org.eclipse.jetty.util.thread.QueuedThreadPool queuedThreadPool =
                                        (org.eclipse.jetty.util.thread.QueuedThreadPool) threadPool;

                                    busyThreads = queuedThreadPool.getBusyThreads();
                                    idleThreads = queuedThreadPool.getIdleThreads();
                                    queueSize = queuedThreadPool.getQueueSize();

                                    try {
                                        org.eclipse.jetty.server.Connector[] connectors = server.getConnectors();
                                        if (connectors != null) {
                                            for (org.eclipse.jetty.server.Connector connector : connectors) {
                                                if (connector instanceof org.eclipse.jetty.server.ServerConnector) {
                                                    org.eclipse.jetty.server.ServerConnector serverConnector =
                                                        (org.eclipse.jetty.server.ServerConnector) connector;
                                                    try {
                                                        activeConnections +=
                                                            serverConnector.getConnectedEndPoints().size();
                                                    } catch (Exception e) {
                                                        // Ignore
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        activeConnections = busyThreads;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            // Reflection failed
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Error getting metrics
        }

        // Add Server metrics with thresholds
        row.add(IntegerUtil.toBytes(busyThreads));
        row.add(IntegerUtil.toBytes(mppConfig.getHttpServerMaxThreads()));
        row.add(IntegerUtil.toBytes(activeConnections));
        row.add(IntegerUtil.toBytes(queueSize));
        row.add(IntegerUtil.toBytes(idleThreads));
    }

    /**
     * Columnar Scan Metrics
     * TOTAL_SCAN_BYTES - Total bytes scanned from columnar storage
     * TOTAL_SCAN_ROWS - Total rows scanned from columnar storage
     * TOTAL_SCAN_FILTERED_ROWS - Total rows after filtering
     */
    private static void addColumnarScanMetrics(RowDataPacket row) {
        try {
            // Directly get global accumulated values using static methods
            row.add(LongUtil.toBytes(ColumnarScanMetrics.getGlobalTotalScanBytes()));
            row.add(LongUtil.toBytes(ColumnarScanMetrics.getGlobalTotalScanRows()));
            row.add(LongUtil.toBytes(ColumnarScanMetrics.getGlobalTotalFilteredRows()));
        } catch (Exception e) {
            // Return zeros on error
            row.add(LongUtil.toBytes(0L));
            row.add(LongUtil.toBytes(0L));
            row.add(LongUtil.toBytes(0L));
        }
    }

    /**
     * Columnar Data Source Latency Metrics
     * ORC_QUERY_COUNT - Total ORC file access count
     * ORC_QUERY_LATENCY_MS - Total ORC file access latency in milliseconds
     * CSV_QUERY_COUNT - Total CSV file access count
     * CSV_QUERY_LATENCY_MS - Total CSV file access latency in milliseconds
     * DEL_QUERY_COUNT - Total deletion bitmap access count
     * DEL_QUERY_LATENCY_MS - Total deletion bitmap access latency in milliseconds
     * GMS_QUERY_COUNT - Total GMS metadata query count
     * GMS_QUERY_LATENCY_MS - Total GMS metadata query latency in milliseconds
     */
    private static void addColumnarDataSourceMetrics(RowDataPacket row) {
        try {
            // Directly get global accumulated values using static methods
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalOrcQueryCount()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalOrcQueryLatencyMs()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalCsvQueryCount()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalCsvQueryLatencyMs()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalDelQueryCount()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalDelQueryLatencyMs()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalGmsQueryCount()));
            row.add(LongUtil.toBytes(ColumnarDataSourceMetrics.getGlobalGmsQueryLatencyMs()));
        } catch (Exception e) {
            // Return zeros on error
            for (int i = 0; i < 8; i++) {
                row.add(LongUtil.toBytes(0L));
            }
        }
    }

    /**
     * MPP Task Metrics
     * EXEC_PENDING_QUEUE - Ready task count in pending queue
     * BLOCK_WAIT_FOR_PRE_PREPROCESSOR_COUNT - Tasks blocked waiting for preprocessor
     * BLOCK_WAIT_FOR_SCAN_IO_COUNT - Tasks blocked waiting for scan I/O
     * BLOCK_WAIT_PIPELINE_DEPENDENCY_COUNT - Tasks blocked by pipeline dependency
     * BLOCK_WAIT_DRIVER_CONSUMER_FINISHED_COUNT - Tasks blocked waiting for consumer
     * BLOCK_WAIT_FOR_MEMORY_COUNT - Tasks blocked waiting for memory
     * BLOCK_WAIT_FOR_EXCHANGE_CLIENT_COUNT - Tasks blocked waiting for exchange client
     * BLOCK_WAIT_FOR_NO_MORE_SPLIT_COUNT - Tasks blocked waiting for no more splits
     * BLOCK_LOCAL_BUFFER_NOT_EMPTY_COUNT - Tasks blocked by non-empty local buffer
     * BLOCK_LOCAL_BUFFER_NOT_FULL_COUNT - Tasks blocked by full local buffer
     * BLOCK_WAIT_FOR_PRODUCER_COUNT - Tasks blocked waiting for producer
     * BLOCK_WAIT_FOR_MEMORY_REVOKE_COUNT - Tasks blocked waiting for memory revoke
     * BLOCK_WAIT_FOR_BLOOM_FILTER_COUNT - Tasks blocked waiting for bloom filter
     * BLOCK_WAIT_FOR_SPILL_WRITE_COUNT - Tasks blocked waiting for spill write
     * BLOCK_WAIT_FOR_SPILL_READ_COUNT - Tasks blocked waiting for spill read
     * BLOCK_WAIT_FOR_PARALLEL_BUILD_COUNT - Tasks blocked waiting for parallel build
     */
    private static void addMppTaskMetrics(RowDataPacket row) {
        try {
            // Directly get global accumulated values using static methods
            row.add(LongUtil.toBytes(MppTaskMetrics.getPendingQueueCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForPreProcessorCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForScanIoCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitPipelineDependencyCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitDriverConsumerFinishedCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForMemoryCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForExchangeClientCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForNoMoreSplitCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getLocalBufferNotEmptyCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getLocalBufferNotFullCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForProducerCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForMemoryRevokeCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForBloomFilterCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForSpillWriteCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForSpillReadCount()));
            row.add(LongUtil.toBytes(MppTaskMetrics.getWaitForParallelBuildCount()));
        } catch (Exception e) {
            // Return zeros on error
            for (int i = 0; i < 16; i++) {
                row.add(LongUtil.toBytes(0L));
            }
        }
    }

    /**
     * Version Cache Details Metrics (V3)
     * VERSION_CACHE_CSV_USED_COUNT - CSV file cache entry count
     * VERSION_CACHE_DEL_USED_COUNT - DEL file cache entry count
     * FILE_SNAPSHOT_META_USED_SIZE - File metadata cache memory usage in bytes
     * FILE_SNAPSHOT_META_USED_COUNT - File metadata cache entry count
     */
    private static void addVersionCacheDetailsMetrics(RowDataPacket row) {
        try {
            DynamicColumnarManager columnarManager = DynamicColumnarManager.getInstance();
            FileVersionStorage versionStorage = columnarManager.getVersionStorage();

            // 1. CSV file cache count
            long csvCacheCount = versionStorage.getCsvCacheSize();
            row.add(LongUtil.toBytes(csvCacheCount));

            // 2. DEL file cache count
            long delCacheCount = versionStorage.getDelCacheSize();
            row.add(LongUtil.toBytes(delCacheCount));

            // 3. File snapshot cache memory usage (bytes)
            long fileMetaMemory = columnarManager.getFileMetaCacheMemorySize();
            row.add(LongUtil.toBytes(fileMetaMemory));

            // 4. File snapshot cache count
            long fileMetaCount = columnarManager.getFileMetaCacheCount();
            row.add(LongUtil.toBytes(fileMetaCount));
        } catch (Exception e) {
            // Return zeros on error
            for (int i = 0; i < 4; i++) {
                row.add(LongUtil.toBytes(0L));
            }
        }
    }

    /**
     * Additional Memory Management Metrics (V3)
     * PREHEAT_META_USED_SIZE - Preheat meta cache memory usage in bytes
     * EXECUTOR_MEMORY_USED_SIZE - Executor runtime memory usage in bytes
     * EXECUTOR_MEMORY_USED_COUNT - Executor runtime query count
     */
    private static void addMemoryManagementMetrics(RowDataPacket row) {
        try {
            // 1. Preheat meta cache memory usage (bytes)
            long preheatMetaMemory = PreheatMetaManager.getInstance().memorySize();
            row.add(LongUtil.toBytes(preheatMetaMemory));

            // 2. Executor runtime memory usage (bytes)
            GlobalMemoryTrackerManager globalMemoryTrackerManager =
                MemoryTrackerManager.getGlobalMemoryTrackerManager();
            long executorMemoryUsed = globalMemoryTrackerManager.getExecutorMemoryUsedSize();
            row.add(LongUtil.toBytes(executorMemoryUsed));

            // 3. Executor runtime query count
            int executorQueryCount = globalMemoryTrackerManager.getExecutorMemoryUsedCount();
            row.add(LongUtil.toBytes(executorQueryCount));
        } catch (Exception e) {
            // Return zeros on error
            for (int i = 0; i < 3; i++) {
                row.add(LongUtil.toBytes(0L));
            }
        }
    }

    /**
     * Additional Executor Memory Metrics (V4) - Optimized Version
     * Uses single snapshot to collect all metrics in one iteration for thread-safety and performance
     * <p>
     * EXECUTOR_MEMORY_QUOTA_USAGE_RATIO - Memory quota usage percentage (0-100)
     * EXECUTOR_MEMORY_AVAILABLE_QUOTA - Remaining available memory quota in bytes
     * EXECUTOR_MAX_QUERY_MEMORY_PEAK - Maximum query memory peak in bytes
     * EXECUTOR_AVG_QUERY_MEMORY_USAGE - Average query memory usage in bytes
     * EXECUTOR_ACTIVE_PIPELINE_COUNT - Number of active pipelines
     * EXECUTOR_ACTIVE_DRIVER_COUNT - Number of active drivers
     * EXECUTOR_ACTIVE_OPERATOR_COUNT - Number of active operators
     * EXECUTOR_TOTAL_ALLOCATED_MEMORY - Total allocated memory in bytes
     * EXECUTOR_TOTAL_FREE_MEMORY - Total free memory (buffer) in bytes
     */
    private static void addExecutorMemoryMetrics(RowDataPacket row) {
        try {
            GlobalMemoryTrackerManager globalMemoryTrackerManager =
                MemoryTrackerManager.getGlobalMemoryTrackerManager();

            // Get all metrics in a single snapshot (one iteration over queryMemoryTrackers)
            GlobalMemoryTrackerManager.ExecutorMemoryMetrics metrics =
                globalMemoryTrackerManager.getExecutorMemoryMetricsSnapshot();

            // 1. Memory quota usage ratio (percentage)
            row.add(StringUtil.encode(String.format("%.2f", metrics.getExecutorMemoryQuotaUsageRatio()), "utf-8"));

            // 2. Available memory quota (bytes)
            row.add(LongUtil.toBytes(metrics.getExecutorMemoryAvailableQuota()));

            // 3. Maximum query memory peak (bytes)
            row.add(LongUtil.toBytes(metrics.getMaxQueryMemoryPeak()));

            // 4. Average query memory usage (bytes)
            row.add(LongUtil.toBytes(metrics.getAvgQueryMemoryUsage()));

            // 5. Active pipeline count
            row.add(IntegerUtil.toBytes(metrics.getActivePipelineCount()));

            // 6. Active driver count
            row.add(IntegerUtil.toBytes(metrics.getActiveDriverCount()));

            // 7. Active operator count
            row.add(IntegerUtil.toBytes(metrics.getActiveOperatorCount()));

            // 8. Total allocated memory (bytes)
            row.add(LongUtil.toBytes(metrics.getTotalAllocatedMemory()));

            // 9. Total free memory (bytes)
            row.add(LongUtil.toBytes(metrics.getTotalFreeMemory()));
        } catch (Exception e) {
            // Return zeros on error
            row.add(StringUtil.encode("0.00", "utf-8")); // quota usage ratio
            for (int i = 0; i < 8; i++) {
                if (i < 2) {
                    row.add(LongUtil.toBytes(0L)); // long values
                } else if (i < 5) {
                    row.add(IntegerUtil.toBytes(0)); // int values
                } else {
                    row.add(LongUtil.toBytes(0L)); // long values
                }
            }
        }
    }
}
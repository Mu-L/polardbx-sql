package com.alibaba.polardbx.common;

public class ColumnarOptions {
    public static final String DICTIONARY_COLUMNS = "DICTIONARY_COLUMNS";
    public static final String TYPE = "TYPE";
    public static final String SNAPSHOT_RETENTION_DAYS = "SNAPSHOT_RETENTION_DAYS";
    public static final String AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL = "AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL";
    public static final String COLUMNAR_PURGE_SAVE_MS = "COLUMNAR_PURGE_SAVE_MS";
    public static final String COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION =
        "COLUMNAR_HEARTBEAT_INTERVAL_MS_SELF_ADAPTION";
    public static final String COLUMNAR_BACKUP_ENABLE = "COLUMNAR_BACKUP_ENABLE";
    public static final String COLUMNAR_IGNORE = "COLUMNAR_IGNORE";
    public static final String ENABLE_COLUMNAR_CDC_CLIENT = "ENABLE_COLUMNAR_CDC_CLIENT";
    public static final String COLUMNAR_CDC_CLIENT_PARALLELISM = "COLUMNAR_CDC_CLIENT_PARALLELISM";
    public static final String ENABLE_OPTIMIZED_STREAMING = "ENABLE_OPTIMIZED_STREAMING";
    public static final String OPTIMIZE_CDC_CLIENT_FILTER = "OPTIMIZE_CDC_CLIENT_FILTER";
    public static final String COLUMNAR_CDC_CLIENT_RING_BUFFER_SIZE = "COLUMNAR_CDC_CLIENT_RING_BUFFER_SIZE";
    public static final String CDC_BINLOG_DOWNLOAD_PARALLELISM_PER_FILE = "CDC_BINLOG_DOWNLOAD_PARALLELISM_PER_FILE";
    public static final String CDC_BINLOG_DOWNLOAD_PART_SIZE = "CDC_BINLOG_DOWNLOAD_PART_SIZE";
    public static final String CDC_BINLOG_DOWNLOAD_WINDOW_SIZE = "CDC_BINLOG_DOWNLOAD_WINDOW_SIZE";
    public static final String COLUMNAR_ORC_TAIL_WRITE_TO_METADB = "columnar_orc_tail_write_to_metadb";
    public static final String COLUMNAR_ROLLBACK_TSO = "columnar_rollback_tso";

}

package com.alibaba.polardbx.common.properties;

/**
 * 用于执行的ExtraCmd
 *
 * @author Dreamond
 */
public class ConnectionProperties {
    public static final String SQL_MODE = "SQL_MODE";

    /**
     * Generate columnar snapshots automatically.
     */
    public static final String ENABLE_AUTO_GEN_COLUMNAR_SNAPSHOT = "ENABLE_AUTO_GEN_COLUMNAR_SNAPSHOT";
    public static final String AUTO_GEN_COLUMNAR_SNAPSHOT_PARALLELISM = "AUTO_GEN_COLUMNAR_SNAPSHOT_PARALLELISM";

    public static final String USE_SHA2_PASSWORD_FOR_BACKEND = "USE_SHA2_PASSWORD_FOR_BACKEND";
    /**
     * CoronaDB PlanCache
     */
    public static final String PLAN_CACHE = "PLAN_CACHE";
    public static final String PHY_SQL_TEMPLATE_CACHE = "PHY_SQL_TEMPLATE_CACHE";
    public static final String PREPARE_OPTIMIZE = "PREPARE_OPTIMIZE";

    /**
     * enable recyclebin
     */
    public static final String ENABLE_RECYCLEBIN = "ENABLE_RECYCLEBIN";

    /**
     * 是否开启show tables结果cache,默认关闭,针对ruby on rails的优化
     */
    public static final String SHOW_TABLES_CACHE = "SHOW_TABLES_CACHE";

    /**
     * 是否强制设置Merge并行执行，默认为空值,也就是会选择自适应模式
     */
    public final static String MERGE_CONCURRENT = "MERGE_CONCURRENT";

    /**
     * 是否强制优化成merge union，默认为true
     */
    public final static String MERGE_UNION = "MERGE_UNION";

    /**
     * UNION 优化,将多少个 SQL UNION 在一起
     */
    public final static String MERGE_UNION_SIZE = "MERGE_UNION_SIZE";

    /**
     * minimum count of union physical sqls in a query
     */
    public final static String MIN_MERGE_UNION_SIZE = "MIN_MERGE_UNION_SIZE";
    /**
     * maximum count of union physical sqls in a query
     */
    public final static String MAX_MERGE_UNION_SIZE = "MAX_MERGE_UNION_SIZE";

    /**
     * 表的meta超时时间，单位毫秒，默认5分钟
     */
    public static final String TABLE_META_CACHE_EXPIRE_TIME = "TABLE_META_CACHE_EXPIRE_TIME";

    /**
     * 优化器和parser结果超时时间，单位毫秒，默认5分钟
     */
    public static final String OPTIMIZER_CACHE_EXPIRE_TIME = "OPTIMIZER_CACHE_EXPIRE_TIME";

    /**
     * 优化器和parser可缓存的数量，默认1000
     */
    public static final String OPTIMIZER_CACHE_SIZE = "OPTIMIZER_CACHE_SIZE";

    /**
     * 在 SHOW CREATE TABLE 结果中输出与 MySQL 兼容的缩进格式（两个空格）
     */
    public static final String OUTPUT_MYSQL_INDENT = "OUTPUT_MYSQL_INDENT";

    /**
     * 在 SHOW CREATE TABLE 结果中输出不带注释的 locality
     */
    public static final String OUTPUT_LOCALITY_WITHOUT_COMMENT = "OUTPUT_LOCALITY_WITHOUT_COMMENT";

    /**
     * 是否允许全表扫描查询,默认为false
     */
    public static final String ALLOW_FULL_TABLE_SCAN = "ALLOW_FULL_TABLE_SCAN";

    /**
     * 是否强制开启/关闭streaming模式,默认为空值,也就是会选择自适应模式
     */
    public static final String CHOOSE_STREAMING = "CHOOSE_STREAMING";

    public static final String COLUMNAR_FLUSH_USING_SYNC_POINT = "COLUMNAR_FLUSH_USING_SYNC_POINT";

    /**
     * 广播表是否开启多写
     */
    public static final String CHOOSE_BROADCAST_WRITE = "CHOOSE_BROADCAST_WRITE";

    /**
     * hbase的faimly/rowkey和表结构的映射配置文件
     */
    public static final String HBASE_MAPPING_FILE = "HBASE_MAPPING_FILE";

    /**
     * 执行jdbc fetch size
     */
    public static final String FETCH_SIZE = "FETCH_SIZE";

    public static final String IGNORE_CHECK_GLOBAL_WHEN_ARCHIVE_CHAIN = "IGNORE_CHECK_GLOBAL_WHEN_ARCHIVE_CHAIN";

    /**
     * 为每个连接都初始化一个线程池，用来做并行查询，默认为true
     */
    public static final String INIT_CONCURRENT_POOL_EVERY_CONNECTION = "INIT_CONCURRENT_POOL_EVERY_CONNECTION";

    /**
     * 并行查询线程池大小
     */
    public static final String CONCURRENT_THREAD_SIZE = "CONCURRENT_THREAD_SIZE";

    /**
     * 并行查询最大线程数,在INIT_CONCURRENT_POOL_EVERY_CONNECTION模式下，避免产生过多的线程
     */
    public static final String MAX_CONCURRENT_THREAD_SIZE = "MAX_CONCURRENT_THREAD_SIZE";

    /**
     * 是否使用sequence替换分布式表的auto_increment,默认为false
     */
    public static final String PROCESS_AUTO_INCREMENT_BY_SEQUENCE = "PROCESS_AUTO_INCREMENT_BY_SEQUENCE";

    /**
     * column label大小写是否不敏感，默认为true. 如果设置为false，强制返回大写保持兼容
     */
    public static final String COLUMN_LABEL_INSENSITIVE = "COLUMN_LABEL_INSENSITIVE";

    /**
     * 记录所有sql,默认值为TRUE
     */
    public static final String RECORD_SQL = "RECORD_SQL";

    /**
     * 指定设置socketTimeout,默认为null,使用系统默认值
     */
    public static final String SOCKET_TIMEOUT = "SOCKET_TIMEOUT";

    public static final String MERGE_DDL_TIMEOUT = "MERGE_DDL_TIMEOUT";

    /**
     * Index usage query timeout in seconds (for physical_index_usage and logical_index_usage views)
     * Default: 120 seconds
     */
    public static final String INDEX_USAGE_QUERY_TIMEOUT = "INDEX_USAGE_QUERY_TIMEOUT";

    /**
     * 事务策略
     */
    public static final String TRANSACTION_POLICY = "TRANSACTION_POLICY";

    /**
     * 共享ReadView
     */
    public static final String SHARE_READ_VIEW = "SHARE_READ_VIEW";

    /**
     * 事务单分片优化
     */
    public static final String ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION = "ENABLE_TRX_SINGLE_SHARD_OPTIMIZATION";

    /**
     * Enable aggressive read connection reuse under RC-level XA/2PC transactions
     */
    public static final String ENABLE_TRX_READ_CONN_REUSE = "ENABLE_TRX_READ_CONN_REUSE";

    /**
     * Get TSO timeout time.
     */
    public static final String GET_TSO_TIMEOUT = "GET_TSO_TIMEOUT";

    public static final String PURGE_HISTORY_MS = "PURGE_HISTORY_MS";

    /**
     * Max single TSO/XA/2PC transaction time
     */
    public static final String MAX_TRX_DURATION = "MAX_TRX_DURATION";

    /**
     * Show X-Plan in explain.
     */
    public static final String EXPLAIN_X_PLAN = "EXPLAIN_X_PLAN";

    public static final String EXPLAIN_SHOW_PHYSICAL_PLAN = "EXPLAIN_SHOW_PHYSICAL_PLAN";

    /**
     * Transaction Isolation Level
     */
    public static final String TRANSACTION_ISOLATION = "TRANSACTION_ISOLATION";

    public static final String TX_ISOLATION = "TX_ISOLATION";

    /**
     * 并行模式是否等所有节点返回数据后再返回，默认false
     */
    public static final String BLOCK_CONCURRENT = "BLOCK_CONCURRENT";

    /**
     * 是否开启GROUP级BLOCK并行默认true
     */
    public static final String GROUP_CONCURRENT_BLOCK = "GROUP_CONCURRENT_BLOCK";

    public static final String OSS_FILE_CONCURRENT = "OSS_FILE_CONCURRENT";

    /**
     * 是否强制串行执行，默认false
     */
    public static final String SEQUENTIAL_CONCURRENT_POLICY = "SEQUENTIAL_CONCURRENT_POLICY";

    /**
     * 是否强制首个subNode串行，其他并行，默认false
     */
    public static final String FIRST_THEN_CONCURRENT_POLICY = "FIRST_THEN_CONCURRENT_POLICY";

    public static final String ENABLE_ZERO_GROUP_AS_BROADCAST_FIRST_GROUP =
        "ENABLE_ZERO_GROUP_AS_BROADCAST_FIRST_GROUP";

    /**
     * 在事务时，DML忽略串行执行策略规则，详情请见ExecUtils.getQueryConcurrencyPolicy
     */
    public static final String ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION =
        "ENABLE_DML_GROUP_CONCURRENT_IN_TRANSACTION";

    /**
     * 指定 DML 执行策略, PUSHDOWN, DETERMINISTIC_PUSHDOWN, LOGICAL
     */
    public static final String DML_EXECUTION_STRATEGY = "DML_EXECUTION_STRATEGY";

    /**
     * 是否下推 WHERE 条件中包含目标表子查询的 DELETE/UPDATE 语句
     */
    public final static String DML_PUSH_MODIFY_WITH_SUBQUERY_CONDITION_OF_TARGET =
        "DML_PUSH_MODIFY_WITH_SUBQUERY_CONDITION_OF_TARGET";

    /**
     * 是否禁止 SET 子句中包含子查询的 UPDATE 语句
     */
    public final static String DML_FORBID_UPDATE_WITH_SUBQUERY_IN_SET = "DML_FORBID_UPDATE_WITH_SUBQUERY_IN_SET";

    public final static String DML_FORBID_PUSH_DOWN_UPDATE_WITH_SUBQUERY_IN_SET =
        "DML_FORBID_PUSH_DOWN_UPDATE_WITH_SUBQUERY_IN_SET";

    /**
     * 是否下推 INSERT IGNORE / REPLACE / UPSERT
     */
    public static final String DML_PUSH_DUPLICATE_CHECK = "DML_PUSH_DUPLICATE_CHECK";

    /**
     * UPSERT 语句中 ON DUPLICATE KEY UPDATE 的目标列中存在 timestamp 类型列时，是否强制逻辑执行
     */
    public static final String DML_CHECK_UPSERT_DYNAMIC_IMPLICIT_WITH_COLUMN_REF =
        "DML_CHECK_UPSERT_DYNAMIC_IMPLICIT_WITH_COLUMN_REF";

    /**
     * 是否忽略不改变实际值的 UPDATE
     */
    public static final String DML_SKIP_TRIVIAL_UPDATE = "DML_SKIP_TRIVIAL_UPDATE";

    /**
     * 是否跳过主键的冲突检查
     */
    public static final String DML_SKIP_DUPLICATE_CHECK_FOR_PK = "DML_SKIP_DUPLICATE_CHECK_FOR_PK";

    /**
     * 是否允许有DML报错的事务继续提交
     */
    public static final String DML_SKIP_CRUCIAL_ERR_CHECK = "DML_SKIP_CRUCIAL_ERR_CHECK";

    /**
     * 是否允许在 RC 的隔离级别下下推 REPLACE
     */
    public static final String DML_FORCE_PUSHDOWN_RC_REPLACE = "DML_FORCE_PUSHDOWN_RC_REPLACE";
    /**
     * 是否使用 returning 优化
     */
    public static final String DML_USE_RETURNING = "DML_USE_RETURNING";

    /**
     * 是否使用 returning 优化包含 order by limit 的 update / delete
     */
    public static final String OPTIMIZE_MODIFY_TOP_N_BY_RETURNING = "OPTIMIZE_MODIFY_TOP_N_BY_RETURNING";

    /**
     * 是否使用 returning 优化需要逻辑执行的 DELETE
     */
    public static final String OPTIMIZE_DELETE_BY_RETURNING = "OPTIMIZE_DELETE_BY_RETURNING";

    /**
     * 是否为局部 UK 使用全表扫描检查冲突的插入值
     */
    public static final String DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN =
        "DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN";

    /**
     * 是否将带 GSI 的单行复杂 DML 的 LOCAL UK 查重裁剪到目标分区
     */
    public static final String DML_PARTITION_LOCAL_UK_DUP_CHECK = "DML_PARTITION_LOCAL_UK_DUP_CHECK";

    /**
     * 是否将带 GSI 的单行复杂 DML 的 LOCAL PK 查重裁剪到目标分区
     */
    public static final String DML_PARTITION_LOCAL_PK_DUP_CHECK = "DML_PARTITION_LOCAL_PK_DUP_CHECK";

    /**
     * 是否使用 returning 优化需要逻辑执行的 relocate
     */
    public static final String OPTIMIZE_RELOCATE_BY_RETURNING = "OPTIMIZE_RELOCATE_BY_RETURNING";

    /**
     * 是否使用 returning 优化需要逻辑执行的 REPLACE
     */
    public static final String OPTIMIZE_REPLACE_BY_RETURNING = "OPTIMIZE_REPLACE_BY_RETURNING";

    /**
     * 使用 returning 优化逻辑执行的 replace 前是否开启自冲突检测
     * 默认开启，关闭自冲突检测可能导致下游消费报错，仅供可忽略下游场景使用
     */
    public static final String OPTIMIZE_REPLACE_BY_RETURNING_CHECK_SELF_CONFLICT =
        "OPTIMIZE_REPLACE_BY_RETURNING_CHECK_SELF_CONFLICT";

    /**
     * 是否只允许使用 主表 检查主键冲突，false 代表个可以使用 按照主键分区的 gsi 来检查主键冲突
     * 对新购实例默认为 false
     */
    public static final String DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY =
        "DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY";

    /**
     * 是否使用 GSI 检查冲突的插入值
     */
    public static final String DML_GET_DUP_USING_GSI = "DML_GET_DUP_USING_GSI";

    /**
     * DML 检查冲突列时下发 DN 的一条 SQL 所能包含的最大 UNION 数量，<= 0 表示无限制
     */
    public static final String DML_GET_DUP_UNION_SIZE = "DML_GET_DUP_UNION_SIZE";

    /**
     * DML 检查冲突列时，在允许时是否使用 IN 来代替 UNION；会增加死锁概率
     */
    public static final String DML_GET_DUP_USING_IN = "DML_GET_DUP_USING_IN";

    /**
     * DML 检查冲突列时，对于多列UK，是否是用 key1=value1 and key2=value2 代替 (key1,key2) in ((value1,value2))
     * 后者在 8.0 DN 上会因为类型转化而查不到结果（比如 key1 是数字类型，value1 是字符串）
     */
    public static final String DML_GET_DUP_USING_UNION_EQUAL = "DML_GET_DUP_USING_UNION_EQUAL";

    /**
     * DML 检查冲突列时下发 DN 的一条 SQL 所能包含的最大 IN 数量，<= 0 表示无限制
     */
    public static final String DML_GET_DUP_IN_SIZE = "DML_GET_DUP_IN_SIZE";

    /**
     * 是否使用 duplicated row count 作为 INSERT IGNORE 的 affected rows
     */
    public static final String DML_RETURN_IGNORED_COUNT = "DML_RETURN_IGNORED_COUNT";

    /**
     * 在 Logical Relocate 时是否跳过没有发生变化的行，不下发任何物理 SQL
     */
    public static final String DML_RELOCATE_SKIP_UNCHANGED_ROW = "DML_RELOCATE_SKIP_UNCHANGED_ROW";

    /**
     * DML 执行时是否检查主键冲突
     */
    public static final String PRIMARY_KEY_CHECK = "PRIMARY_KEY_CHECK";

    /**
     * Rebalance组装任务时生成的单个DDL job迁移对最大数据量，单位为MB.
     */
    public static final String REBALANCE_MAX_UNIT_SIZE = "REBALANCE_MAX_UNIT_SIZE";

    public static final String REBALANCE_MAX_TABLEGROUP_SOLVED_BY_LP = "REBALANCE_MAX_TABLEGROUP_SOLVED_BY_LP";

    public static final String REBALANCE_MAX_UNIT_PARTITION_COUNT = "REBALANCE_MAX_UNIT_PARTITION_COUNT";

    /**
     * When enabled, drain_node rebalance defaults to DRAIN_ONLY solve level,
     * which only moves data off the draining node without global rebalancing.
     */
    public static final String ENABLE_FAST_DRAIN_MODE = "ENABLE_FAST_DRAIN_MODE";

    /**
     * 是否开启 Foreign Key
     */
    public static final String ENABLE_FOREIGN_KEY = "ENABLE_FOREIGN_KEY";

    /**
     * 是否开启 Foreign Constraint Check
     */
    public static final String FOREIGN_KEY_CHECKS = "FOREIGN_KEY_CHECKS";

    /**
     * CN 是否开启 Foreign Constraint Check, 优先级最高
     * 0 -> 关闭
     * 1 -> 开启
     * 2 -> 未设置
     */
    public static final String CN_FOREIGN_KEY_CHECKS = "CN_FOREIGN_KEY_CHECKS";

    /**
     * 是否开启 UPDATE/DELETE 语句的 Foreign Constraint Check
     */
    public static final String FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE = "FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE";

    /**
     * 是否允许开启check约束
     */
    public static final String ENABLE_CHECK_CONSTRAINT = "ENABLE_CHECK_CONSTRAINT";

    /**
     * 是否允许在包含 CCI 的表上执行 DDL
     */
    public static final String FORBID_DDL_WITH_CCI = "FORBID_DDL_WITH_CCI";

    /**
     * 是否禁止在纯列存表上执行 DDL
     */
    public static final String FORBID_DDL_WITH_PURE_COLUMNAR = "FORBID_DDL_WITH_PURE_COLUMNAR";

    public static final String FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR = "FORBID_COMPLEX_DML_WITH_PURE_COLUMNAR";

    /**
     * 是否允许在包含 归档CCI 的表上执行 DDL
     */
    public static final String FORBID_TRUNCATE_WITH_ARCHIVE_CCI = "FORBID_TRUNCATE_WITH_ARCHIVE_CCI";

    /**
     * 是否允许删除表上的最后一个 CCI
     */
    public static final String ALLOW_DROP_LAST_CCI = "ALLOW_DROP_LAST_CCI";

    /**
     * 是否在执行repartition的时候重建CCI
     */
    public static final String REBUILD_CCI_WHEN_REPARTITION = "REBUILD_CCI_WHEN_REPARTITION";

    /**
     * 是否允许在 CCI 上执行 DROP/TRUNCATE PARTITION 删除/清空数据，会造成主表和分区表数据不一致
     */
    public static final String ENABLE_DROP_TRUNCATE_CCI_PARTITION = "ENABLE_DROP_TRUNCATE_CCI_PARTITION";

    /**
     * 是否允许主表DROP PARTITION时，向影子表中插入数据，生成BINLOG（用于CCI删除）
     */
    public static final String ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = "ENABLE_SHADOW_INSERT_ON_DROP_PARTITION";

    /**
     * INSERT SELECT 到影子表的批次文件总量（B)
     */
    public static final String SHADOW_INSERT_BATCH_FILE_SIZE = "SHADOW_INSERT_BATCH_FILE_SIZE";
    /**
     * INSERT SELECT 到影子表的批次大小
     */
    public static final String SHADOW_INSERT_BATCH_SIZE = "SHADOW_INSERT_BATCH_SIZE";

    /**
     * INSERT SELECT 到影子表的批次间隔时间（ms）
     */
    public static final String SHADOW_INSERT_BATCH_INTERVAL = "SHADOW_INSERT_BATCH_INTERVAL";
    /**
     * 生成影子表时忽略CCI
     */
    public static final String IGNORE_CCI_WHEN_CREATE_SHADOW_TABLE = "IGNORE_CCI_WHEN_CREATE_SHADOW_TABLE";
    /**
     * 生成影子表的引擎
     */
    public static final String CREATE_SHADOW_TABLE_ENGINE = "CREATE_SHADOW_TABLE_ENGINE";

    /**
     * 是否将CCI设置到独立的表组
     */
    public static final String SET_CCI_TO_SEPARATE_TG = "SET_CCI_TO_SEPARATE_TG";

    /**
     * 在 RelocateWriter 中是否通过 PartitionField 判断拆分键是否变化
     */
    public static final String DML_USE_NEW_SK_CHECKER = "DML_USE_NEW_SK_CHECKER";

    public static final String DML_PRINT_CHECKER_ERROR = "DML_PRINT_CHECKER_ERROR";

    /**
     * 在 INSERT IGNORE、REPLACE、UPSERT 的时候使用 PartitionField 判断重复值
     */
    public static final String DML_USE_NEW_DUP_CHECKER = "DML_USE_NEW_DUP_CHECKER";

    /**
     * 在 REPLACE、UPSERT 的时候是否跳过相同行比较（将导致 affected rows 不正确）
     */
    public static final String DML_SKIP_IDENTICAL_ROW_CHECK = "DML_SKIP_IDENTICAL_ROW_CHECK";

    /**
     * 在 REPLACE、UPSERT 的时候是否跳过含有 JSON 的相同行比较（因为 CN 不支持 JSON 比较）
     */
    public static final String DML_SKIP_IDENTICAL_JSON_ROW_CHECK = "DML_SKIP_IDENTICAL_JSON_ROW_CHECK";

    public static final String DML_SELECT_SAME_ROW_ONLY_COMPARE_PK_UK_SK = "DML_SELECT_SAME_ROW_ONLY_COMPARE_PK_UK_SK";

    /**
     * 在 DML 的时候是否使用简单的字符串比较 JSON
     */
    public static final String DML_CHECK_JSON_BY_STRING_COMPARE = "DML_CHECK_JSON_BY_STRING_COMPARE";

    /**
     * INSERT 中的 VALUES 出现列名时是否替换为插入值而不是默认值，以兼容 MySQL 行为；会对 INSERT 的 INPUT 按 VALUES 顺序排序
     */
    public static final String DML_REF_PRIOR_COL_IN_VALUE = "DML_REF_PRIOR_COL_IN_VALUE";

    /**
     * 是否检查向包含 implicit default 值的列(比如类型为 bigint not null的列)写入数据的表达式 rex,
     * 并可能为 NULL 的 rex， 替换为 IFNULL(rex, defaultLiteral)
     */
    public static final String DML_REPLACE_IMPLICIT_DEFAULT =
        "DML_REPLACE_IMPLICIT_DEFAULT";

    /**
     * 是否将向包含 dynamic implicit default 值的列(比如类型为 timestamp not null的列)写入数据的可能为 NULL 的表达式 rex，
     * 替换为使用 IFNULL(rex, defaultRex)
     */
    public static final String DML_REPLACE_DYNAMIC_IMPLICIT_DEFAULT =
        "DML_REPLACE_DYNAMIC_IMPLICIT_DEFAULT";

    /**
     * 将向包含 dynamic implicit default 值的列(比如类型为 timestamp not null的列)写入数据的表达式，
     * 使用 IFNULL(rex, defaultRex) 表达式包裹后，替换为 RexCallParam
     */
    public static final String DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM =
        "DML_FORCE_REPLACE_DYNAMIC_IMPLICIT_DEFAULT_WITH_PARAM";

    /**
     * 一次性完成全部 dynamic implicit default 求值，
     * 保持与 MySQL 相同行为(batch insert 中，所有使用 current_timestamp 作为隐式 default 值的列，最终写入的值相同)
     */
    public static final String DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO =
        "DML_COMPUTE_ALL_DYNAMIC_IMPLICIT_DEFAULT_REF_IN_ONE_GO";

    /**
     * 一次性完成全部 dynamic implicit default 求值的同时，将显示指定的 REX_CALL 替换为 相同的值, 保持与 MySQL 相同行为:
     * <pre>
     * batch insert 中，
     * 所有使用 current_timestamp 作为隐式 default 值的列
     * 和 所有通过 default 或者 CURRENT_TIMESTAMP() 函数 赋值的列，
     * 最终写入的值相同
     * </pre>
     */
    public static final String DML_REPLACE_EXPLICIT_REX_CALL_WITH_COMPUTED_DYNAMIC_IMPLICIT_DEFAULT =
        "DML_REPLACE_EXPLICIT_REX_CALL_WITH_COMPUTED_DYNAMIC_IMPLICIT_DEFAULT";

    public static String DML_INSERT_PUSH_DOWN_WITH_COLUMN_LIST = "DML_INSERT_PUSH_DOWN_WITH_COLUMN_LIST";

    /**
     * 在逻辑DDL中校验建表语句时，主动在物理连接上等待的时间，仅用于测试
     */
    public static final String GET_PHY_TABLE_INFO_DELAY = "GET_PHY_TABLE_INFO_DELAY";

    /**
     * WAIT_PREPARED的延时时间
     */
    public static final String MULTI_PHASE_WAIT_PREPARED_DELAY = "MULTI_PHASE_WAIT_PREPARED_DELAY";

    /**
     * WAIT_COMMIT的延时时间
     */
    public static final String MULTI_PHASE_WAIT_COMMIT_DELAY = "MULTI_PHASE_WAIT_COMMIT_DELAY";

    /**
     * WAIT_COMMIT的延时时间
     */
    public static final String MULTI_PHASE_COMMIT_DELAY = "MULTI_PHASE_COMMIT_DELAY";

    /**
     * WAIT_COMMIT的延时时间
     */
    public static final String MULTI_PHASE_PREPARE_DELAY = "MULTI_PHASE_PREPARE_DELAY";
    /**
     * 在逻辑DDL中发起物理DDL操作时，主动延迟的时间，仅用于测试
     */
    public static final String EMIT_PHY_TABLE_DDL_DELAY = "EMIT_PHY_TABLE_DDL_DELAY";

    /**
     * 在建表语句中跳过CDC
     */
    public static final String CREATE_TABLE_SKIP_CDC = "CREATE_TABLE_SKIP_CDC";

    /**
     * 校验逻辑列顺序
     */
    public static final String CHECK_LOGICAL_COLUMN_ORDER = "CHECK_LOGICAL_COLUMN_ORDER";

    /**
     * DMS 无锁变更
     */
    public static final String ENABLE_DMS_OMC_V1 = "ENABLE_DMS_OMC_V1";

    /**
     * 是否强制使用 Online Modify Column，即使列类型没有改变，或者不是支持的类型
     */
    public static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION";

    /**
     * Online Modify Column 回填时是否使用 returning 优化
     */
    public static final String OMC_BACK_FILL_USE_RETURNING = "OMC_BACK_FILL_USE_RETURNING";

    /**
     * 是否自动采用 Online Modify Column
     */
    public static final String ENABLE_AUTO_OMC = "ENABLE_AUTO_OMC";

    /**
     * 是否强制采用 Online Modify Column 2.0
     */
    public static final String FORCE_USING_OMC = "FORCE_USING_OMC";

    /**
     * 是否开启 Online Modify Column 3.0
     */
    public static final String ENABLE_OMC_30 = "ENABLE_OMC_30";

    /**
     * 是否强制采用 Online Modify Column 3.0
     */
    public static final String FORCE_USING_OMC_30 = "FORCE_USING_OMC_30";

    /**
     * 是否允许 OMC 3.0 直接修改分区键列类型，开启后不再强制 fallback 到 OMC 2.0 重建表
     */
    public static final String ENABLE_OMC_30_MODIFY_PARTITION_KEY = "ENABLE_OMC_30_MODIFY_PARTITION_KEY";

    /**
     * 是否开启 Ghost ddl 能力
     */
    public static final String ENABLE_GHOST_DDL = "ENABLE_GHOST_DDL";

    public static final String ENABLE_CHANGESET_FOR_OMC = "ENABLE_CHANGESET_FOR_OMC";

    public static final String ENABLE_BACKFILL_OPT_FOR_OMC = "ENABLE_BACKFILL_OPT_FOR_OMC";

    public static final String ENABLE_INSERT_IGNORE_FOR_OMC = "ENABLE_INSERT_IGNORE_FOR_OMC";

    public static final String ENABLE_OMC_CHECK_TX_ISOLATION = "ENABLE_OMC_CHECK_TX_ISOLATION";

    /**
     * enable backpressure for catchup procedure, dynamic config
     */
    public static final String ENABLE_CHANGESET_BACKPRESSURE = "ENABLE_CHANGESET_BACKPRESSURE";

    public static final String OMC_CATCHUP_LOOP_COUNT_BEFORE_BP = "OMC_CATCHUP_LOOP_COUNT_BEFORE_BP";

    public static final String OMC_BACKPRESSURE_MIN_RATE_LIMIT = "OMC_BACKPRESSURE_MIN_RATE_LIMIT";

    public static final String OMC_BACKPRESSURE_DEFAULT_BUCKET_CAPACITY = "OMC_BACKPRESSURE_DEFAULT_BUCKET_CAPACITY";

    public static final String OMC_BACKPRESSURE_CUTOVER_THRESHOLD = "OMC_BACKPRESSURE_CUTOVER_THRESHOLD";

    public static final String OMC_BACKPRESSURE_MAX_THRESHOLD = "OMC_BACKPRESSURE_MAX_THRESHOLD";

    public static final String OMC_BACKPRESSURE_VALID_SPEED = "OMC_BACKPRESSURE_VALID_SPEED";

    public static final String OMC_BACKPRESSURE_THRESHOLD_FILE_SIZE = "OMC_BACKPRESSURE_THRESHOLD_FILE_SIZE";

    public static final String OMC_BACKPRESSURE_ADAPTIVE_LEVEL = "OMC_BACKPRESSURE_ADAPTIVE_LEVEL";

    public static final String OMC_BACKPRESSURE_ADJUSTMENT_FACTOR = "OMC_BACKPRESSURE_ADJUSTMENT_FACTOR";

    public static final String OMC_BACKPRESSURE_THRESHOLD_FACTOR = "OMC_BACKPRESSURE_THRESHOLD_FACTOR";

    public static final String OMC_CHANGESET_FILESIZE_LIMIT = "OMC_CHANGESET_FILESIZE_LIMIT";

    public static final String OMC_BACKFILL_BATCH_SIZE_MAX = "OMC_BACKFILL_BATCH_SIZE_MAX";

    public static final String OMC_BACKFILL_BATCH_FILE_SIZE = "OMC_BACKFILL_BATCH_FILE_SIZE";

    public static final String OMC_BACKFILL_SPEED_LIMITATION = "OMC_BACKFILL_SPEED_LIMITATION";

    public static final String OMC_BACKFILL_SPEED_MIN = "OMC_BACKFILL_SPEED_MIN";

    public static final String OMC_BACKFILL_PARALLELISM = "OMC_BACKFILL_PARALLELISM";

    public static final String OMC_CHECKER_PARALLELISM = "OMC_CHECKER_PARALLELISM";

    public static final String OMC_MAX_RETRY_COUNT = "OMC_MAX_RETRY_COUNT";

    public static final String OMC_MAX_CHECK_RETRY_COUNT = "OMC_MAX_CHECK_RETRY_COUNT";

    public static final String OMC_CHECKER_BATCH_SIZE = "OMC_CHECKER_BATCH_SIZE";

    public static final String OMC_CHANGESET_ACQUIRE_LOCK = "OMC_CHANGESET_ACQUIRE_LOCK";

    public static final String OMC_LOCK_WAIT_TIMEOUT = "OMC_LOCK_WAIT_TIMEOUT";

    public static final String OMC_CUTOVER_TIMEOUT = "OMC_CUTOVER_TIMEOUT";

    public static final String OMC_SEQUENTIAL_POLICY = "OMC_SEQUENTIAL_POLICY";

    public static final String OMC_FULL_CONCURRENT_POLICY = "OMC_FULL_CONCURRENT_POLICY";

    public static final String OMC_PREFETCH_SHARDS = "OMC_PREFETCH_SHARDS";

    public static final String OMC_ENABLE_TRACE = "OMC_ENABLE_TRACE";

    /**
     * Add Generated Column 是否强制 CN 计算表达式
     */
    public static final String GEN_COL_FORCE_CN_EVAL = "COL_FORCE_CN_EVAL";

    /**
     * 是否将条件中的表达式替换为 Generated Column
     */
    public static final String GEN_COL_SUBSTITUTION = "GEN_COL_SUBSTITUTION";

    /**
     * 是否在进行 Generated Column 表达式替换的时候进行类型判断
     */
    public static final String GEN_COL_SUBSTITUTION_CHECK_TYPE = "GEN_COL_SUBSTITUTION_CHECK_TYPE";

    /**
     * 是否允许在 DN Generated Column 上创建 Unique Key
     */
    public static final String ENABLE_UNIQUE_KEY_ON_GEN_COL = "ENABLE_UNIQUE_KEY_ON_GEN_COL";

    /**
     * 是否允许使用表达式索引的语法创建索引（创建生成列 + 创建索引）
     */
    public static final String ENABLE_CREATE_EXPRESSION_INDEX = "ENABLE_CREATE_EXPRESSION_INDEX";

    /**
     * Online Modify Column Checker 并行策略
     */
    public static final String OMC_CHECKER_CONCURRENT_POLICY = "OMC_CHECKER_CONCURRENT_POLICY";

    /**
     * 是否开启DDL
     */
    public static final String ENABLE_DDL = "ENABLE_DDL";

    /*
     * 是否开启两阶段DDL
     */
    public static final String ENABLE_DRDS_MULTI_PHASE_DDL = "ENABLE_DRDS_MULTI_PHASE_DDL";

    /*
     * 强制关闭多阶段DDL，优先级高于ENABLE_DRDS_MULTI_PHASE_DDL
     */
    public static final String FORCE_DISABLE_MULTI_PHASE_DDL = "FORCE_DISABLE_MULTI_PHASE_DDL";

    /*
     * 是否在DDL之前执行check table
     */
    public static final String CHECK_TABLE_BEFORE_PHY_DDL = "CHECK_TABLE_BEFORE_PHY_DDL";

    /*
     * 是否检查连接状态
     */
    public static final String CHECK_PHY_CONN_NUM = "CHECK_PHY_CONN_NUM";
    /*
     * 两阶段DDL最终状态，仅用于调试
     */

    public static final String TWO_PHASE_DDL_FINAL_STATUS = "TWO_PHASE_DDL_FINAL_STATUS";
    /**
     * 是否开启DDL
     */
    public static final String ENABLE_ALTER_DDL = "ENABLE_ALTER_DDL";

    /**
     * 是否允许执行self-join的跨库join,默认为false. 避免用户以前的sql出现性能问题
     */
    public static final String ENABLE_SELF_CROSS_JOIN = "ENABLE_SELF_CROSS_JOIN";

    /**
     * 是否开启兼容datetime类型的round down,避免四舍五入,默认为false
     */
    public static final String ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN = "ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN";

    /**
     * 是否开启兼容timestamp类型的round down,避免四舍五入,默认为false
     */
    public static final String ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN = "ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN";

    /**
     * 广播表的写入，不走事务
     */
    public static final String BROADCAST_DML = "BROADCAST_DML";

    public static final String USE_READ_CONN_FOR_XA_BROADCAST_DML = "USE_READ_CONN_FOR_XA_BROADCAST_DML";

    public static final String FORBID_CROSS_GROUP_WRITE_FOR_EXPLICIT_TRX =
        "FORBID_CROSS_GROUP_WRITE_FOR_EXPLICIT_TRX";

    public static final String OPTIMIZE_FORBID_CROSS_GROUP_CHECK_FOR_PUSH_DOWN_PLAN =
        "OPTIMIZE_FORBID_CROSS_GROUP_CHECK_FOR_PUSH_DOWN_PLAN";

    public static final String OPTIMIZE_FORBID_CROSS_GROUP_CHECK_FOR_NON_PUSH_DOWN_PLAN =
        "OPTIMIZE_FORBID_CROSS_GROUP_CHECK_FOR_NON_PUSH_DOWN_PLAN";

    public static final String FORBID_TRX_CONTINUE_AFTER_CROSS_GROUP =
        "FORBID_TRX_CONTINUE_AFTER_CROSS_GROUP";

    /**
     * Enable/Disable New Sequence cache on CN
     */
    public static final String ENABLE_NEW_SEQ_CACHE_ON_CN = "ENABLE_NEW_SEQ_CACHE_ON_CN";

    /**
     * Cache size for New Sequence on CN. Only valid when ENABLE_NEW_SEQ_CACHE_ON_CN is true.
     */
    public static final String NEW_SEQ_CACHE_SIZE_ON_CN = "NEW_SEQ_CACHE_SIZE_ON_CN";

    /**
     * Cache size for New Sequence on DN
     */
    public static final String NEW_SEQ_CACHE_SIZE = "NEW_SEQ_CACHE_SIZE";

    /**
     * Enable grouping for New Sequence
     */
    public static final String ENABLE_NEW_SEQ_GROUPING = "ENABLE_NEW_SEQ_GROUPING";

    /**
     * Enable batch for New Sequence
     */
    public static final String ENABLE_NEW_SEQ_BATCH = "ENABLE_NEW_SEQ_BATCH";

    /**
     * Grouping timeout for New Sequence
     */
    public static final String NEW_SEQ_GROUPING_TIMEOUT = "NEW_SEQ_GROUPING_TIMEOUT";

    /**
     * The number of task queues shared by all New Sequence objects in the same database
     */
    public static final String NEW_SEQ_TASK_QUEUE_NUM_PER_DB = "NEW_SEQ_TASK_QUEUE_NUM_PER_DB";

    /**
     * Check if New Sequence value handler should merge requests from different sequences.
     */
    public static final String ENABLE_NEW_SEQ_REQUEST_MERGING = "ENABLE_NEW_SEQ_REQUEST_MERGING";

    public static final String SKIP_RELOAD_VALUE_CHECK = "SKIP_RELOAD_VALUE_CHECK";

    /**
     * Idle time before a value handler is terminated
     */
    public static final String NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME = "NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME";

    /**
     * 2.0 only: check if simple sequence is allowed to be created. False by default.
     */
    public static final String ALLOW_SIMPLE_SEQUENCE = "ALLOW_SIMPLE_SEQUENCE";

    /**
     * Step for Group Sequence
     */
    public static final String SEQUENCE_STEP = "SEQUENCE_STEP";

    /**
     * Unit Count for Group Sequence
     */
    public static final String SEQUENCE_UNIT_COUNT = "SEQUENCE_UNIT_COUNT";

    /**
     * Unit Index for Group Sequence
     */
    public static final String SEQUENCE_UNIT_INDEX = "SEQUENCE_UNIT_INDEX";

    /**
     * Check if Group Sequence Catcher is enabled
     */
    public static final String ENABLE_GROUP_SEQ_CATCHER = "ENABLE_GROUP_SEQ_CATCHER";

    /**
     * Check Interval for Group Sequence Catcher
     */
    public static final String GROUP_SEQ_CHECK_INTERVAL = "GROUP_SEQ_CHECK_INTERVAL";

    /**
     * merge ddl是否采用全并行模式,设置为false,默认为库间并行
     */
    public static final String MERGE_DDL_CONCURRENT = "MERGE_DDL_CONCURRENT";

    /**
     * 慢SQL阈值,默认为1000
     */
    public static final String SLOW_SQL_TIME = "SLOW_SQL_TIME";

    /**
     * load data 每次 batch insert的记录条数，默认为1024条
     */
    public static final String LOAD_DATA_BATCH_INSERT_SIZE = "LOAD_DATA_BATCH_INSERT_SIZE";

    /**
     * load data 缓存的buffer阈值，默认为60M
     */
    public static final String LOAD_DATA_CACHE_BUFFER_SIZE = "LOAD_DATA_CACHE_BUFFER_SIZE";

    /**
     * select into outfile 缓存的buffer阈值，默认为32M
     */
    public static final String SELECT_INTO_OUTFILE_BUFFER_SIZE = "SELECT_INTO_OUTFILE_BUFFER_SIZE";

    public static final String LOAD_DATA_USE_BATCH_MODE = "LOAD_DATA_USE_BATCH_MODE";

    /**
     * 主备延迟切断值,超过切断备库, 默认不开启 为-1
     */
    public static final String SQL_DELAY_CUTOFF = "SQL_DELAY_CUTOFF";

    public static final String DB_PRIV = "DB_PRIV";

    /**
     * Whether enable version check, default is true.
     */
    public static final String ENABLE_VERSION_CHECK = "ENABLE_VERSION_CHECK";

    /**
     * server模式下最大允许客户端传递的packet大小,默认为1MB
     */
    public static final String MAX_ALLOWED_PACKET = "MAX_ALLOWED_PACKET";

    /**
     * mysql回报最大超时时间,默认设置为8小时
     */
    public static final String NET_WRITE_TIMEOUT = "NET_WRITE_TIMEOUT";

    /**
     * 用kill指令来关闭流式结果集，默认关闭
     */
    public static final String KILL_CLOSE_STREAM = "KILL_CLOSE_STREAM";

    /**
     * show tables使用规则数据
     */
    public static final String SHOW_TABLES_FROM_RULE_ONLY = "SHOW_TABLES_FROM_RULE_ONLY";

    public static final String BLOCK_LOGICAL_DDL = "BLOCK_LOGICAL_DDL";

    public static final String DDL_TASK_ERROR_RETRY_WAIT_TIME = "DDL_TASK_ERROR_RETRY_WAIT_TIME";

    /**
     * rule 的兼容性配置
     */
    public static final String IS_CROSS_RULE = "IS_CROSS_RULE";

    /**
     * Check if logical information_schema query is supported. The default value
     * is TRUE.
     */
    public static final String ENABLE_LOGICAL_INFO_SCHEMA_QUERY = "ENABLE_LOGICAL_INFO_SCHEMA_QUERY";

    /**
     * Execute the query of statistics collection by group. FALSE by default for
     * "by instance".
     */
    public static final String INFO_SCHEMA_QUERY_STAT_BY_GROUP = "INFO_SCHEMA_QUERY_STAT_BY_GROUP";

    /**
     * 后端建库时写入diamond,标识后端数据库类型,目前有histore
     */
    public static final String DB_INSTANCE_TYPE = "DB_INSTANCE_TYPE";

    public static final String STRICT_COLUMN_META = "STRICT_COLUMN_META";

    /**
     * 是否需要将在Calcite上执行异常的SQL在老Server的逻辑上进行重试，默认是打开，在随机SQL测试时要关闭
     */
    public static final String RETRY_ERROR_SQL_ON_OLD_SERVER = "RETRY_ERROR_SQL_ON_OLD_SERVER";

    /**
     * 收集SQL在DRDS的新或旧引擎上的执行错误，包括完整堆栈、行号、方法名、文件名和IpPort
     */
    public static final String COLLECT_SQL_ERROR_INFO = "COLLECT_SQL_ERROR_INFO";

    /**
     * 是否开启参数化SQL的日志及其参数化后的SQL_ID的向物理SQL的透传
     */
    public static final String ENABLE_PARAMETERIZED_SQL_LOG = "ENABLE_PARAMETERIZED_SQL_LOG";

    /**
     * 参与参数化SQL日志打印的最大长度
     */
    public static final String MAX_PARAMETERIZED_SQL_LOG_LENGTH = "MAX_PARAMETERIZED_SQL_LOG_LENGTH";

    /**
     * Hint 的识别是否采用新的 Hint parser
     */
    public static final String HINT_PARSER_FLAG = "HINT_PARSER_FLAG";

    /**
     * 禁止全表删除或者全表更新
     */
    public static final String FORBID_EXECUTE_DML_ALL = "FORBID_EXECUTE_DML_ALL";

    /**
     * 是否启用规则的数据库存储（弱依赖Diamond），默认为TRUE（启用）
     */
    public static final String ENABLE_RULE_DB_STORE = "ENABLE_RULE_DB_STORE";

    /**
     * 规则定时任务启动时间（整点时钟 0 ~ 23）
     */
    public static final String SCHEDULED_RULE_TASK_CLOCK = "SCHEDULED_RULE_TASK_CLOCK";

    /**
     * 规则检查与同步周期/间隔
     */
    public static final String RULE_CHECK_INTERVAL = "RULE_CHECK_INTERVAL";

    /**
     * 版本前缀中的 MySQL 版本号
     */
    public static final String VERSION_PREFIX = "VERSION_PREFIX";

    /**
     * BlockIndexNLJoin : block size
     */
    public static final String JOIN_BLOCK_SIZE = "JOIN_BLOCK_SIZE";

    public static final String LOOKUP_JOIN_MAX_BATCH_SIZE = "LOOKUP_JOIN_MAX_BATCH_SIZE";

    public static final String LOOKUP_JOIN_MIN_BATCH_SIZE = "LOOKUP_JOIN_MIN_BATCH_SIZE";

    /**
     * COLD_HOT 模式下，LIMIT_COUNT指定update/delete语句在在线库发生的最少的影响行数，
     * 当行数少于COLD_HOT_LIMIT_COUNT时，则需要在历史库也要执行一遍
     */
    public static final String COLD_HOT_LIMIT_COUNT = "COLD_HOT_LIMIT_COUNT";

    /**
     * 当后端事务连接被占用时，允许再开一条读连接（仅在柔性事务中有效）
     */
    public static final String ALLOW_EXTRA_READ_CONN = "ALLOW_EXTRA_READ_CONN";

    /**
     * 注入错误（仅供事务测试使用）
     */
    public static final String FAILURE_INJECTION = "FAILURE_INJECTION";

    /**
     * 事务日志清理间隔
     */
    public static final String PURGE_TRANS_INTERVAL = "PURGE_TRANS_INTERVAL";

    /**
     * 清理多久之前的事务日志
     */
    public static final String PURGE_TRANS_BEFORE = "PURGE_TRANS_BEFORE";

    /**
     * Enable deadlock detection for distributed transactions
     */
    public static final String ENABLE_DEADLOCK_DETECTION = "ENABLE_DEADLOCK_DETECTION";

    /**
     * deadlock detection interval
     */
    public static final String DEADLOCK_DETECTION_INTERVAL = "DEADLOCK_DETECTION_INTERVAL";

    public static final String LOCAL_DEADLOCK_SCAN_INTERVAL = "LOCAL_DEADLOCK_SCAN_INTERVAL";

    public static final String CCL_EXECUTION_TIME_DETECT_INTERVAL = "CCL_EXECUTION_TIME_DETECT_INTERVAL";
    public static final String ENABLE_CCL_EXECUTION_TIME_TASK = "ENABLE_CCL_EXECUTION_TIME_TASK";
    public static final String MAX_TRX_AFFECT_ROWS = "MAX_TRX_AFFECT_ROWS";

    /**
     * 是否允许进行跨DB进行查询
     */
    public static final String ALLOW_CROSS_DB_QUERY = "ALLOW_CROSS_DB_QUERY";

    /**
     * XA RECOVER 扫描间隔
     */
    public static final String XA_RECOVER_INTERVAL = "XA_RECOVER_INTERVAL";

    public static final String AC_RECOVER_PARALLELISM = "AC_RECOVER_PARALLELISM";

    /**
     * TSO HEARTBEAT 扫描间隔
     */
    public static final String TSO_HEARTBEAT_INTERVAL = "TSO_HEARTBEAT_INTERVAL";

    /**
     * 列存 purge 间隔
     */
    public static final String COLUMNAR_TSO_PURGE_INTERVAL = "COLUMNAR_TSO_PURGE_INTERVAL";

    /**
     * 列存 tso 加载间隔
     */
    public static final String COLUMNAR_TSO_UPDATE_INTERVAL = "COLUMNAR_TSO_UPDATE_INTERVAL";

    /**
     * 主动为主实例增加一个 tso 加载延迟，通过读一个更早的 tso 来规避加载增量数据导致的延迟，单位为毫秒
     */
    public static final String COLUMNAR_TSO_UPDATE_DELAY = "COLUMNAR_TSO_UPDATE_DELAY";

    public static final String COLUMNAR_DELAY_WARNING_THRESHOLD = "COLUMNAR_DELAY_WARNING_THRESHOLD";

    /**
     * 强制 CN purge 延迟相对于最新版本超过一定时间的版本链版本，单位为毫秒。-1 表示关闭，默认 1 小时
     */
    public static final String FORCE_COLUMNAR_PURGE_DURATION_MS = "FORCE_COLUMNAR_PURGE_DURATION_MS";

    /**
     * 支持在非主实例上执行列存 purge 操作，默认为 false
     */
    public static final String COLUMNAR_SLAVE_SUPPORT_PURGE = "COLUMNAR_SLAVE_SUPPORT_PURGE";

    /**
     * 强制本次列存查询走最新的 TSO，供预热脚本使用
     */
    public static final String USE_LATEST_COLUMNAR_TSO = "USE_LATEST_COLUMNAR_TSO";

    /**
     * 列存 RPC 调用的消息大小上限
     */
    public static final String COLUMNAR_RPC_MAX_MESSAGE_SIZE = "COLUMNAR_RPC_MAX_MESSAGE_SIZE";

    /**
     * 列存 RPC 流式调用超时时间，单位为 ms
     */
    public static final String COLUMNAR_RPC_READ_TIMEOUT = "COLUMNAR_RPC_READ_TIMEOUT";

    /**
     * 列存 RPC 流式调用反压超时时间，单位为 ms
     */
    public static final String COLUMNAR_RPC_BACK_PRESSURE_TIMEOUT = "COLUMNAR_RPC_BACK_PRESSURE_TIMEOUT";

    /**
     * 默认事务清理开始时间（在该时间段内随机）
     */
    public static final String PURGE_TRANS_START_TIME = "PURGE_TRANS_START_TIME";

    /**
     * 每次清理事务日志的批量
     */
    public static final String PURGE_TRANS_BATCH_SIZE = "PURGE_TRANS_BATCH_SIZE";

    /**
     * 每次清理事务日志的批量操作间隔时间
     */
    public static final String PURGE_TRANS_BATCH_PERIOD = "PURGE_TRANS_BATCH_PERIOD";

    /**
     * GROUP_CONCAT 展示最大长度
     */
    public static final String GROUP_CONCAT_MAX_LEN = "GROUP_CONCAT_MAX_LEN";

    /**
     * 2 policies for batch insert: NONE, SPLIT.
     */
    public static final String BATCH_INSERT_POLICY = "BATCH_INSERT_POLICY";

    /**
     * The threshold of sql length for checking batch insert policy.
     */
    public static final String MAX_BATCH_INSERT_SQL_LENGTH = "MAX_BATCH_INSERT_SQL_LENGTH";

    /**
     * Each split sql size in SPLIT batch insert policy. For instance, when the
     * value is 200, original sql of 50000 values may split into 250 sqls.
     */
    public static final String BATCH_INSERT_CHUNK_SIZE = "BATCH_INSERT_CHUNK_SIZE";

    /**
     * For test only. Simulate a switchover reschedule when the physical sql id reaches this value.
     */
    public static final String SIMULATE_SWITCHOVER_RESCHEDULE_PHY_SQL_ID_FOR_TEST =
        "SIMULATE_SWITCHOVER_RESCHEDULE_PHY_SQL_ID_FOR_TEST";

    /**
     * For test only. Disable this guard to reproduce InsertSplitter switchover reschedule duplication.
     */
    public static final String ENABLE_SWITCHOVER_RESCHEDULE_INTERNAL_SUB_EXECUTION_GUARD_FOR_TEST =
        "ENABLE_SWITCHOVER_RESCHEDULE_INTERNAL_SUB_EXECUTION_GUARD_FOR_TEST";

    /**
     * Batch size for insert select.
     */
    public static final String INSERT_SELECT_BATCH_SIZE = "INSERT_SELECT_BATCH_SIZE";

    /**
     * Limit of select size for insert select in distributed transaction.
     */
    public static final String INSERT_SELECT_LIMIT = "INSERT_SELECT_LIMIT";

    /**
     * Insert/update/delete select执行策略为Insert/update/delete多线程执行
     */
    public static final String MODIFY_SELECT_MULTI = "MODIFY_SELECT_MULTI";

    /**
     * Insert/update/delete select执行策略为select 和 Insert/update/delete 并行
     */
    public static final String MODIFY_WHILE_SELECT = "MODIFY_WHILE_SELECT";

    /**
     * Insert select执行策略为MPP执行
     */
    public static final String INSERT_SELECT_MPP = "INSERT_SELECT_MPP";

    public static final String INSERT_SELECT_MPP_BY_PARALLEL = "INSERT_SELECT_MPP_BY_PARALLEL";

    /**
     * Insert select self_table; insert 和 select 操作同一个表时,非事务下可能会导致数据 > 2倍，默认自身表时，先select 再insert
     */
    public static final String INSERT_SELECT_SELF_BY_PARALLEL = "INSERT_SELECT_SELF_BY_PARALLEL";

    public static final String ENABLE_INSERT_SELECT_WITH_FLASHBACK_PUSH_DOWN =
        "ENABLE_INSERT_SELECT_WITH_FLASHBACK_PUSH_DOWN";

    /**
     * 是否允许 Insert 列重复
     */
    public static final String INSERT_DUPLICATE_COLUMN = "INSERT_DUPLICATE_COLUMN";

    /**
     * MODIFY_SELECT_MULTI策略时 逻辑任务执行 的线程个数
     */
    public static final String MODIFY_SELECT_LOGICAL_THREADS = "MODIFY_SELECT_LOGICAL_THREADS";

    /**
     * MODIFY_SELECT_MULTI策略时 物理任务执行 的线程个数
     */
    public static final String MODIFY_SELECT_PHYSICAL_THREADS = "MODIFY_SELECT_PHYSICAL_THREADS";

    /**
     * MODIFY_SELECT_MULTI策略时 内存buffer大小，
     */
    public static final String MODIFY_SELECT_BUFFER_SIZE = "MODIFY_SELECT_BUFFER_SIZE";

    public static final String SQL_SELECT_LIMIT = "SQL_SELECT_LIMIT";

    /**
     * Batch size for select size for update or delete.
     */
    public static final String UPDATE_DELETE_SELECT_BATCH_SIZE = "UPDATE_DELETE_SELECT_BATCH_SIZE";

    /**
     * Limit of select size for update or delete in distributed transaction.
     */
    public static final String UPDATE_DELETE_SELECT_LIMIT = "UPDATE_DELETE_SELECT_LIMIT";

    public static final String PL_MEMORY_LIMIT = "PL_MEMORY_LIMIT";

    public static final String PL_CURSOR_MEMORY_LIMIT = "PL_CURSOR_MEMORY_LIMIT";

    public static final String ENABLE_UDF = "ENABLE_UDF";

    public static final String MAX_JAVA_UDF_NUM = "MAX_JAVA_UDF_NUM";

    public static final String FORCE_DROP_JAVA_UDF = "FORCE_DROP_JAVA_UDF";

    public static final String PL_INTERNAL_CACHE_SIZE = "PL_INTERNAL_CACHE_SIZE";

    public static final String MAX_PL_DEPTH = "MAX_PL_DEPTH";

    public static final String ORIGIN_CONTENT_IN_ROUTINES = "ORIGIN_CONTENT_IN_ROUTINES";

    public static final String FORCE_DROP_PROCEDURE = "FORCE_DROP_PROCEDURE";

    public static final String FORCE_DROP_SQL_UDF = "FORCE_DROP_SQL_UDF";

    public static final String READONLY_DN_LIST = "READONLY_DN_LIST";

    public static final String ALLOW_BROADCAST_WRITE_FOR_READONLY_DN = "ALLOW_BROADCAST_WRITE_FOR_READONLY_DN";

    public static final String FORBID_TRX_CONTINUE_AFTER_WRITE_READONLY = "FORBID_TRX_CONTINUE_AFTER_WRITE_READONLY";

    /**
     * recyclebin table retain hours
     */
    public static final String RECYCLEBIN_RETAIN_HOURS = "RECYCLEBIN_RETAIN_HOURS";

    /**
     * max update num in global secondary index
     */
    public static final String MAX_UPDATE_NUM_IN_GSI = "MAX_UPDATE_NUM_IN_GSI";

    /**
     * enable the optimizer rules: LogicalJoinToBushyJoinRule, BushyJoinClusteringRule, BushyJoinToLogicalJoinRule
     */
    public static final String ENABLE_JOIN_CLUSTERING = "ENABLE_JOIN_CLUSTERING";

    public static final String MOCK_SQL_ENGINE_ALERT = "MOCK_SQL_ENGINE_ALERT";

    public static final String ENABLE_SQL_ENGINE_ALERT = "ENABLE_SQL_ENGINE_ALERT";

    public static final String ENABLE_SQL_ENGINE_ALERT_NORMAL = "ENABLE_SQL_ENGINE_ALERT_NORMAL";

    public static final String ENABLE_SQL_ENGINE_ALERT_MAJOR = "ENABLE_SQL_ENGINE_ALERT_MAJOR";

    public static final String ENABLE_SQL_ENGINE_ALERT_CRITICAL = "ENABLE_SQL_ENGINE_ALERT_CRITICAL";

    public static final String ENABLE_SQL_ENGINE_ALERT_COLUMNAR_READ = "ENABLE_SQL_ENGINE_ALERT_COLUMNAR_READ";

    public static final String ENABLE_SQL_ENGINE_ALERT_COLUMNAR_WARMUP = "ENABLE_SQL_ENGINE_ALERT_COLUMNAR_WARMUP";

    public static final String JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT =
        "JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT";

    /**
     * enable join clustering aovid cross join
     */
    public static final String ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN = "ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN";

    /**
     * enable background collect table staistic
     */
    public static final String ENABLE_BACKGROUND_STATISTIC_COLLECTION = "ENABLE_BACKGROUND_STATISTIC_COLLECTION";

    /**
     * background collect table staistic start time default 02:00
     */
    public static final String BACKGROUND_STATISTIC_COLLECTION_START_TIME =
        "BACKGROUND_STATISTIC_COLLECTION_START_TIME";

    /**
     * background collect table staistic end time default 03:00
     */
    public static final String BACKGROUND_STATISTIC_COLLECTION_END_TIME = "BACKGROUND_STATISTIC_COLLECTION_END_TIME";

    public static final String STATISTIC_CORRECTIONS = "STATISTIC_CORRECTIONS";

    public static final String STATISTIC_IN_DEGRADATION_NUMBER = "STATISTIC_IN_DEGRADATION_NUMBER";

    /**
     * statistic collect rowcount timeout
     * background ttl expire end time default 05:00
     */
    public static final String BACKGROUND_TTL_EXPIRE_END_TIME = "BACKGROUND_TTL_EXPIRE_END_TIME";

    /**
     * background collect table staistic period default 12h = 12 * 60 min = 720 min
     */
    public static final String STATISTIC_VISIT_DN_TIMEOUT = "STATISTIC_VISIT_DN_TIMEOUT";

    /**
     * background collect table staistic expire time default 12h = 12 * 60 min = 720 min
     */
    public static final String BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME =
        "BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME";

    public static final String SKIP_PHYSICAL_ANALYZE = "SKIP_PHYSICAL_ANALYZE";

    /**
     * statistic info expire time
     */
    public static final String STATISTIC_EXPIRE_TIME = "STATISTIC_EXPIRE_TIME";

    public static final String CACHELINE_INDICATE_UPDATE_TIME = "CACHELINE_INDICATE_UPDATE_TIME";

    public static final String COMPENSATION_REDUNDANCY_TIME = "COMPENSATION_REDUNDANCY_TIME";

    public static final String ENABLE_CACHELINE_COMPENSATION = "ENABLE_CACHELINE_COMPENSATION";

    public static final String CACHELINE_COMPENSATION_BLACKLIST = "CACHELINE_COMPENSATION_BLACKLIST";

    /**
     * statistic sample rate
     */
    public static final String STATISTIC_SAMPLE_RATE = "STATISTIC_SAMPLE_RATE";

    /**
     * mysql sample_percentage
     */
    public static final String SAMPLE_PERCENTAGE = "SAMPLE_PERCENTAGE";

    public static final String DN_HINT = "DN_HINT";

    /**
     * backfill sample_percentage
     */
    public static final String BACKFILL_MAX_SAMPLE_PERCENTAGE = "BACKFILL_MAX_SAMPLE_PERCENTAGE";

    /**
     * enable innodb btree sampling
     */
    public static final String ENABLE_INNODB_BTREE_SAMPLING = "ENABLE_INNODB_BTREE_SAMPLING";

    /**
     * histogram max sample size
     */
    public static final String HISTOGRAM_MAX_SAMPLE_SIZE = "HISTOGRAM_MAX_SAMPLE_SIZE";

    /**
     * when table number more than AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT, only analyze column with index
     */
    public static final String AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT = "AUTO_ANALYZE_ALL_COLUMN_TABLE_LIMIT";

    /**
     * the time between each time analyze
     */
    public static final String AUTO_ANALYZE_TABLE_SLEEP_MILLS = "AUTO_ANALYZE_TABLE_SLEEP_MILLS";

    /**
     * the time between each auto analyze in hours
     */
    public static final String AUTO_ANALYZE_PERIOD_IN_HOURS = "AUTO_ANALYZE_PERIOD_IN_HOURS";

    /**
     * histogram max sample size
     */
    public static final String HISTOGRAM_BUCKET_SIZE = "HISTOGRAM_BUCKET_SIZE";

    /**
     * enable sort merge join default true
     */
    public static final String ENABLE_SORT_MERGE_JOIN = "ENABLE_SORT_MERGE_JOIN";

    public static final String ENABLE_IN_TO_UNION_ALL = "ENABLE_IN_TO_UNION_ALL";

    public static final String IN_TO_UNION_STRICT_MODE = "IN_TO_UNION_STRICT_MODE";

    public static final String ENABLE_IN_TO_EXPAND_IN = "ENABLE_IN_TO_EXPAND_IN";

    public static final String ENABLE_SPLIT_MERGE_SORT = "ENABLE_SPLIT_MERGE_SORT";

    public static final String IN_TO_UNION_ALL_THRESHOLD = "IN_TO_UNION_ALL_THRESHOLD";

    public static final String ENABLE_DYNAMIC_PRUNE_MERGE_SORT = "ENABLE_DYNAMIC_PRUNE_MERGE_SORT";

    public static final String ENABLE_DYNAMIC_MERGE_SORT_LOOKUP = "ENABLE_DYNAMIC_MERGE_SORT_LOOKUP";

    public static final String DYNAMIC_MERGE_SORT_WHITE_LIST = "DYNAMIC_MERGE_SORT_WHITE_LIST";

    public static final String DYNAMIC_MERGE_SORT_THRESHOLD = "DYNAMIC_MERGE_SORT_THRESHOLD";

    public static final String DYNAMIC_MERGE_SORT_DETECT_PREFETCH = "DYNAMIC_MERGE_SORT_DETECT_PREFETCH";

    /**
     * enable bka join default true
     */
    public static final String ENABLE_BKA_JOIN = "ENABLE_BKA_JOIN";

    /**
     * enable remove join condition default true
     */
    public static final String ENABLE_REMOVE_JOIN_CONDITION = "ENABLE_REMOVE_JOIN_CONDITION";

    public static final String ENABLE_SIMPLIFY_LOOKUP_JOIN = "ENABLE_SIMPLIFY_LOOKUP_JOIN";

    /**
     * enable dynamic pruning in bka join default false
     */
    public static final String ENABLE_BKA_PRUNING = "ENABLE_BKA_PRUNING";

    /**
     * enable dynamic in values pruning in bka join default true
     */
    public static final String ENABLE_BKA_IN_VALUES_PRUNING = "ENABLE_BKA_IN_VALUES_PRUNING";

    public static final String ENABLE_SINGLE_JOIN_EST = "ENABLE_SINGLE_JOIN_EST";

    /**
     * enable hash join default true
     */
    public static final String ENABLE_HASH_JOIN = "ENABLE_HASH_JOIN";

    /**
     * enable left driver hash join default true
     */
    public static final String FORCE_OUTER_DRIVER_HASH_JOIN = "FORCE_OUTER_DRIVER_HASH_JOIN";
    public static final String FORBID_OUTER_DRIVER_HASH_JOIN = "FORBID_OUTER_DRIVER_HASH_JOIN";

    /**
     * enable nl join default true
     */
    public static final String ENABLE_NL_JOIN = "ENABLE_NL_JOIN";

    /**
     * enable semi nl join default true
     */
    public static final String ENABLE_SEMI_NL_JOIN = "ENABLE_SEMI_NL_JOIN";

    /**
     * enable semi hash join default true
     */
    public static final String ENABLE_SEMI_HASH_JOIN = "ENABLE_SEMI_HASH_JOIN";

    public static final String ENABLE_REVERSE_HASH_JOIN = "ENABLE_REVERSE_HASH_JOIN";

    public static final String ENABLE_REVERSE_BROADCAST_SEMI_HASH_JOIN = "ENABLE_REVERSE_BROADCAST_SEMI_HASH_JOIN";

    public static final String ENABLE_REVERSE_SEMI_HASH_JOIN = "ENABLE_REVERSE_SEMI_HASH_JOIN";

    public static final String ENABLE_REVERSE_ANTI_HASH_JOIN = "ENABLE_REVERSE_ANTI_HASH_JOIN";

    public static final String EARLY_MATCH_MARKED_TABLE = "EARLY_MATCH_MARKED_TABLE";

    public static final String BLOOM_FILTER_IN_REVERSE_SEMI_JOIN = "BLOOM_FILTER_IN_REVERSE_SEMI_JOIN";

    /**
     * enable semi bka join default true
     */
    public static final String ENABLE_SEMI_BKA_JOIN = "ENABLE_SEMI_BKA_JOIN";

    /**
     * enable semi sort merge join default true
     */
    public static final String ENABLE_SEMI_SORT_MERGE_JOIN = "ENABLE_SEMI_SORT_MERGE_JOIN";

    public static final String MATERIALIZED_ITEMS_LIMIT = "MATERIALIZED_ITEMS_LIMIT";

    /**
     * enable materialized semi join default true
     */
    public static final String ENABLE_MATERIALIZED_SEMI_JOIN = "ENABLE_MATERIALIZED_SEMI_JOIN";

    public static final String ENABLE_MYSQL_HASH_JOIN = "ENABLE_MYSQL_HASH_JOIN";

    public static final String ENABLE_MYSQL_SEMI_HASH_JOIN = "ENABLE_MYSQL_SEMI_HASH_JOIN";

    /**
     * cbo search join commute rule only when CBO_LEFT_DEEP_TREE_JOIN_LIMIT < join size <= CBO_TOO_MANY_JOIN_LIMIT
     */
    public static final String CBO_TOO_MANY_JOIN_LIMIT = "CBO_TOO_MANY_JOIN_LIMIT";

    public static final String COLUMNAR_CBO_TOO_MANY_JOIN_LIMIT = "COLUMNAR_CBO_TOO_MANY_JOIN_LIMIT";

    /**
     * cbo search in left deep tree search space only when CBO_ZIG_ZAG_TREE_JOIN_LIMIT < join size <= CBO_LEFT_DEEP_TREE_JOIN_LIMIT
     */
    public static final String CBO_LEFT_DEEP_TREE_JOIN_LIMIT = "CBO_LEFT_DEEP_TREE_JOIN_LIMIT";

    /**
     * cbo search in zig-zag tree search space only when CBO_BUSHY_TREE_JOIN_LIMIT < join size <= CBO_ZIG_ZAG_TREE_JOIN_LIMIT
     */
    public static final String CBO_ZIG_ZAG_TREE_JOIN_LIMIT = "CBO_ZIG_ZAG_TREE_JOIN_LIMIT";

    /**
     * cbo search in bushy tree search space only when join size <= CBO_BUSHY_TREE_JOIN_LIMIT
     */
    public static final String CBO_BUSHY_TREE_JOIN_LIMIT = "CBO_BUSHY_TREE_JOIN_LIMIT";

    public static final String ENABLE_JOINAGG_TO_JOINAGGSEMIJOIN = "ENABLE_JOINAGG_TO_JOINAGGSEMIJOIN";

    /**
     * enable the heuristic algorithm to reorder join when join size <= RBO_HEURISTIC_JOIN_REORDER_LIMIT
     */
    public static final String RBO_HEURISTIC_JOIN_REORDER_LIMIT = "RBO_HEURISTIC_JOIN_REORDER_LIMIT";

    public static final String MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH = "MYSQL_JOIN_REORDER_EXHAUSTIVE_DEPTH";

    public static final String ENABLE_LV_SUBQUERY_UNWRAP = "ENABLE_LV_SUBQUERY_UNWRAP";

    public static final String ENABLE_PAGING_FORCE_TO_JOIN = "ENABLE_PAGING_FORCE_TO_JOIN";

    public static final String ENABLE_PRE_FILTER_LOOKUP = "ENABLE_PRE_FILTER_LOOKUP";

    public static final String ENABLE_PAGING_FORCE_LOOKUP = "ENABLE_PAGING_FORCE_LOOKUP";

    public static final String ENABLE_SUBQUERY_IGNORE_LIMIT = "ENABLE_SUBQUERY_IGNORE_LIMIT";

    public static final String FORBID_DUPLICATE_PUSH = "FORBID_DUPLICATE_PUSH";

    public static final String ENABLE_AUTO_FORCE_INDEX = "ENABLE_AUTO_FORCE_INDEX";

    public static final String ENABLE_AUTO_PAGINATION_INDEX = "ENABLE_AUTO_PAGINATION_INDEX";

    public static final String ENABLE_AUTO_PAGINATION_UNION = "ENABLE_AUTO_PAGINATION_UNION";

    public static final String AUTO_PAGINATION_OR_THRESHOLD = "AUTO_PAGINATION_OR_THRESHOLD";

    public static final String ENABLE_AUTO_PAGINATION_IGNORE_INDEX = "ENABLE_AUTO_PAGINATION_IGNORE_INDEX";

    public static final String ENABLE_AUTO_PAGINATION_PAGING_FORCE = "ENABLE_AUTO_PAGINATION_PAGING_FORCE";

    public static final String ENABLE_AUTO_PAGINATION_IN_SCALAR = "ENABLE_AUTO_PAGINATION_IN_SCALAR";

    public static final String SKIP_SORT_EQ_PRE_COL = "SKIP_SORT_EQ_PRE_COL";

    public static final String SORT_EQ_PRE_COL = "SORT_EQ_PRE_COL";

    public static final String PAGINATION_UNCOVER_COL = "PAGINATION_UNCOVER_COL";

    public static final String PUSHDOWN_RANGE_LIMIT = "PUSHDOWN_RANGE_LIMIT";

    public static final String EXPLAIN_PRUNING_DETAIL = "EXPLAIN_PRUNING_DETAIL";

    public static final String ENABLE_FILTER_REORDER = "ENABLE_FILTER_REORDER";

    public static final String PREFILTER_COLUMNS = "PREFILTER_COLUMNS";

    public static final String ENABLE_PREFILTER_TO_SUBQUERY = "ENABLE_PREFILTER_TO_SUBQUERY";

    public static final String ENABLE_PREFILTER_LOWER_BOUND = "ENABLE_PREFILTER_LOWER_BOUND";

    public static final String ENABLE_PREFILTER_UPPER_BOUND = "ENABLE_PREFILTER_UPPER_BOUND";

    public static final String ENABLE_PLANNER_TIMEOUT = "ENABLE_PLANNER_TIMEOUT";

    public static final String PLANNER_MAX_TIME = "PLANNER_MAX_TIME";

    public static final String ENABLE_CONSTANT_FOLD = "ENABLE_CONSTANT_FOLD";

    /**
     * enable semi join reorder default true
     */
    public static final String ENABLE_SEMI_JOIN_REORDER = "ENABLE_SEMI_JOIN_REORDER";

    /**
     * enable outer join reorder default true
     */
    public static final String ENABLE_OUTER_JOIN_REORDER = "ENABLE_OUTER_JOIN_REORDER";

    /**
     * enable statistic feedback default true
     */
    public static final String ENABLE_STATISTIC_FEEDBACK = "ENABLE_STATISTIC_FEEDBACK";

    /**
     * enable hash agg default true
     */
    public static final String ENABLE_HASH_AGG = "ENABLE_HASH_AGG";

    /**
     * enable sort agg default true
     */
    public static final String ENABLE_SORT_AGG = "ENABLE_SORT_AGG";

    public static final String PREFER_PUSH_AGG = "PREFER_PUSH_AGG";

    public static final String PREFER_PARTIAL_AGG = "PREFER_PARTIAL_AGG";

    public static final String PARTIAL_AGG_SHARD = "PARTIAL_AGG_SHARD";

    public static final String PRE_AGG_STREAM_BATCH_THRESHOLD = "PRE_AGG_STREAM_BATCH_THRESHOLD";

    public static final String ENABLE_STREAM_PARTIAL_AGG = "ENABLE_STREAM_PARTIAL_AGG";

    public static final String ENABLE_TRANSPARENT_PARTIAL_AGG = "ENABLE_TRANSPARENT_PARTIAL_AGG";

    public static final String TRANSPARENT_PRE_AGG_JUDGE_RATE = "TRANSPARENT_PRE_AGG_JUDGE_RATE";

    public static final String DYNAMIC_PRE_AGG_JUDGE_RATE = "DYNAMIC_PRE_AGG_JUDGE_RATE";

    /**
     * enable partial agg default true
     */
    public static final String ENABLE_PARTIAL_AGG = "ENABLE_PARTIAL_AGG";

    public static final String ENABLE_PARTIAL_GROUP_TOPN = "ENABLE_PARTIAL_GROUP_TOPN";

    public static final String PREFER_PARTIAL_GROUP_TOPN = "PREFER_PARTIAL_GROUP_TOPN";

    public static final String ENABLE_TOPN = "ENABLE_TOPN";

    public static final String ENABLE_LIMIT = "ENABLE_LIMIT";

    public static final String ENABLE_PARTIAL_LIMIT = "ENABLE_PARTIAL_LIMIT";

    public static final String PARTIAL_AGG_SELECTIVITY_THRESHOLD = "PARTIAL_AGG_SELECTIVITY_THRESHOLD";

    public static final String PARTIAL_AGG_BUCKET_THRESHOLD = "PARTIAL_AGG_BUCKET_THRESHOLD";

    public static final String AGG_MAX_HASH_TABLE_FACTOR = "AGG_MAX_HASH_TABLE_FACTOR";

    public static final String AGG_MIN_HASH_TABLE_FACTOR = "AGG_MIN_HASH_TABLE_FACTOR";

    public static final String AGG_MAX_HASH_TABLE_INITIAL_SIZE = "AGG_MAX_HASH_TABLE_INITIAL_SIZE";

    public static final String ENABLE_HASH_WINDOW = "ENABLE_HASH_WINDOW";

    public static final String ENABLE_SORT_WINDOW = "ENABLE_SORT_WINDOW";

    public static final String ENABLE_GROUP_TOPN = "ENABLE_GROUP_TOPN";

    /**
     * enable push join default true
     */
    public static final String ENABLE_PUSH_JOIN = "ENABLE_PUSH_JOIN";

    public static final String ENABLE_PUSH_SINGLE_GROUP_JOIN = "ENABLE_PUSH_SINGLE_GROUP_JOIN";

    public static final String ENABLE_PUSH_CORRELATE = "ENABLE_PUSH_CORRELATE";

    public static final String ENABLE_CHECK_PUSH_CORRELATE = "ENABLE_CHECK_PUSH_CORRELATE";

    /**
     * ignore un pushable function when join
     */
    public static final String IGNORE_UN_PUSHABLE_FUNC_IN_JOIN = "IGNORE_UN_PUSHABLE_FUNC_IN_JOIN";

    /**
     * enable push project default true
     */
    public static final String ENABLE_PUSH_PROJECT = "ENABLE_PUSH_PROJECT";

    /**
     * enable cbo push join default true
     */
    public static final String ENABLE_CBO_PUSH_JOIN = "ENABLE_CBO_PUSH_JOIN";

    /**
     * cbo restrict push join, enable when join in cn is >= CBO_RESTRICT_PUSH_JOIN_LIMIT
     */
    public static final String CBO_RESTRICT_PUSH_JOIN_LIMIT = "CBO_RESTRICT_PUSH_JOIN_LIMIT";

    /**
     * cbo restrict push join rule counter, restrict the rule if it has been invoked CBO_RESTRICT_PUSH_JOIN_LIMIT times
     */
    public static final String CBO_RESTRICT_PUSH_JOIN_COUNT = "CBO_RESTRICT_PUSH_JOIN_COUNT";

    /**
     * enable rbo push agg default true
     */
    public static final String ENABLE_PUSH_AGG = "ENABLE_PUSH_AGG";

    /**
     * enable rbo push agg default true
     */
    public static final String PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD = "PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD";

    /**
     * enable cbo push agg default true
     */
    public static final String ENABLE_CBO_PUSH_AGG = "ENABLE_CBO_PUSH_AGG";

    /**
     * enable rbo push sort default true
     */
    public static final String ENABLE_PUSH_SORT = "ENABLE_PUSH_SORT";

    public static final String ENABLE_EXTERNAL_PUSH_PROJECT = "ENABLE_EXTERNAL_PUSH_PROJECT";
    public static final String ENABLE_EXTERNAL_PUSH_FILTER = "ENABLE_EXTERNAL_PUSH_FILTER";
    public static final String ENABLE_EXTERNAL_PUSH_SORT = "ENABLE_EXTERNAL_PUSH_SORT";
    public static final String ENABLE_EXTERNAL_PUSH_AGG = "ENABLE_EXTERNAL_PUSH_AGG";

    /**
     * Idle minutes after which an external catalog schema releases its connector
     * metadata and cached table metas. 0 disables reclaiming.
     */
    public static final String EXTERNAL_CATALOG_METADATA_IDLE_TTL_MINUTES =
        "EXTERNAL_CATALOG_METADATA_IDLE_TTL_MINUTES";

    /**
     * enable rbo group join default true
     */
    public static final String ENABLE_CBO_GROUP_JOIN = "ENABLE_CBO_GROUP_JOIN";

    /**
     * the max time of agg join transpose
     */
    public static final String CBO_AGG_JOIN_TRANSPOSE_LIMIT = "CBO_AGG_JOIN_TRANSPOSE_LIMIT";

    public static final String ENABLE_EXPAND_DISTINCTAGG = "ENABLE_EXPAND_DISTINCTAGG";
    public static final String ENABLE_SIMPLIFY_GROUP_BY_RULE = "ENABLE_SIMPLIFY_GROUP_BY_RULE";

    public static final String ENABLE_SORT_JOIN_TRANSPOSE = "ENABLE_SORT_JOIN_TRANSPOSE";

    public static final String ENABLE_SORT_OUTERJOIN_TRANSPOSE = "ENABLE_SORT_OUTERJOIN_TRANSPOSE";

    public static final String CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT = "CBO_JOIN_TABLELOOKUP_TRANSPOSE_LIMIT";

    public static final String CBO_START_UP_COST_JOIN_LIMIT = "CBO_START_UP_COST_JOIN_LIMIT";

    /**
     * volcano planner use startup cost
     */
    public static final String ENABLE_START_UP_COST = "ENABLE_START_UP_COST";

    public static final String ENABLE_MQ_CACHE_COST_BY_THREAD = "ENABLE_MQ_CACHE_COST_BY_THREAD";

    /**
     * join hint
     */
    public static final String JOIN_HINT = "JOIN_HINT";

    /**
     * async secondary index config
     */
    public static final String ASI_CONF = "ASI_CONF";

    /**
     * max length of sqlSimple which is show in processlist
     */
    public static final String SQL_SIMPLE_MAX_LENGTH = "SQL_SIMPLE_MAX_LENGTH";

    /**
     * 强制走主库
     */
    public static final String MASTER = "MASTER";

    /**
     * 强制走备库
     */
    public static final String SLAVE = "SLAVE";

    /**
     * 强制走follower备库
     */
    public static final String FOLLOWER = "FOLLOWER";

    /**
     * allow DDL on Global Secondary Index
     */
    public static final String DDL_ON_GSI = "DDL_ON_GSI";

    /**
     * allow DML on Global Secondary Index
     */
    public static final String DML_ON_GSI = "DML_ON_GSI";

    /**
     * allow NODE/SCAN on Global Secondary Index
     */
    public static final String PUSHDOWN_HINT_ON_GSI = "PUSHDOWN_HINT_ON_GSI";

    /**
     * allow NODE/SCAN of dml on broadcast table
     */
    public static final String PUSHDOWN_HINT_ON_BROADCAST = "PUSHDOWN_HINT_ON_BROADCAST";

    /**
     * copy modify with HINT NODE(0) TO SINGLE GROUP
     */
    public static final String COPY_MODIFY_NODE0_TO_SINGLE = "COPY_MODIFY_NODE0_TO_SINGLE";

    /**
     * allow Global Secondary Index DDL or DML on MySQL 5.6
     */
    public static final String STORAGE_CHECK_ON_GSI = "STORAGE_CHECK_ON_GSI";

    /**
     * Check distributed transaction, for debug use
     */
    public static final String DISTRIBUTED_TRX_REQUIRED = "DISTRIBUTED_TRX_REQUIRED";

    /**
     * Check transaction type, for debug use
     */
    public static final String TRX_CLASS_REQUIRED = "TRX_CLASS_REQUIRED";

    public static final String TSO_OMIT_GLOBAL_TX_LOG = "TSO_OMIT_GLOBAL_TX_LOG";

    /**
     * allow TRUNCATE table with Global Secondary Index
     */
    public static final String TRUNCATE_TABLE_WITH_GSI = "TRUNCATE_TABLE_WITH_GSI";

    /**
     * allow ADD Global Secondary Index after primary table created
     */
    public static final String ALLOW_ADD_GSI = "ALLOW_ADD_GSI";

    /**
     * debug mode on Global Secondary Index, which makes GSI status change slower etc.
     */
    public static final String GSI_DEBUG = "GSI_DEBUG";

    /**
     * debug mode on column, including hidden column, column multi-write, etc.
     */
    public static final String COLUMN_DEBUG = "COLUMN_DEBUG";

    /**
     * allow gsi stop at a specific status
     */
    public static final String GSI_FINAL_STATUS_DEBUG = "GSI_FINAL_STATUS_DEBUG";

    /**
     * skip the cutover stage
     */
    public static final String REPARTITION_SKIP_CUTOVER = "REPARTITION_SKIP_CUTOVER";

    /**
     * skip the repartition unchanged check
     */
    public static final String REPARTITION_SKIP_CHECK = "REPARTITION_SKIP_CHECK";

    /**
     * enable rebuild gsi for repartition
     */
    public static final String REPARTITION_ENABLE_REBUILD_GSI = "REPARTITION_ENABLE_REBUILD_GSI";

    /**
     * skip the cleanup stage
     */
    public static final String REPARTITION_SKIP_CLEANUP = "REPARTITION_SKIP_CLEANUP";

    /**
     * force gsi name for repartition
     */
    public static final String REPARTITION_FORCE_GSI_NAME = "REPARTITION_FORCE_GSI_NAME";

    /**
     * batch size for scaleout backfill procedure
     */
    public static final String SCALEOUT_BACKFILL_BATCH_SIZE = "SCALEOUT_BACKFILL_BATCH_SIZE";

    /**
     * speed limit for scaleout backfill procedure
     */
    public static final String SCALEOUT_BACKFILL_SPEED_LIMITATION = "SCALEOUT_BACKFILL_SPEED_LIMITATION";

    public static final String SCALEOUT_BACKFILL_SPEED_MIN = "SCALEOUT_BACKFILL_SPEED_MIN";

    /**
     * parallelism of scaleout backfill procedure
     */
    public static final String SCALEOUT_BACKFILL_PARALLELISM = "SCALEOUT_BACKFILL_PARALLELISM";

    /**
     * parallelism tasks of logical table for scaleout
     */
    public static final String SCALEOUT_TASK_PARALLELISM = "SCALEOUT_TASK_PARALLELISM";

    /**
     * parallelism tasks of logical table for tablegroup
     */
    public static final String TABLEGROUP_TASK_PARALLELISM = "TABLEGROUP_TASK_PARALLELISM";
    /**
     * max number of parallelism tasks of logical table for tablegroup
     */
    public static final String TABLEGROUP_TASK_MAX_PARALLELISM = "TABLEGROUP_TASK_MAX_PARALLELISM";

    /**
     * batch size for scaleout check procedure
     */
    public static final String SCALEOUT_CHECK_BATCH_SIZE = "SCALEOUT_CHECK_BATCH_SIZE";

    /**
     * speed limit for scaleout check procedure
     */
    public static final String SCALEOUT_CHECK_SPEED_LIMITATION = "SCALEOUT_CHECK_SPEED_LIMITATION";

    /**
     * speed limit for scaleout check procedure
     */
    public static final String SCALEOUT_CHECK_SPEED_MIN = "SCALEOUT_CHECK_SPEED_MIN";

    /**
     * parallelism of scaleout check procedure
     */
    public static final String SCALEOUT_CHECK_PARALLELISM = "SCALEOUT_CHECK_PARALLELISM";

    /**
     * number of error for check early fail.
     */
    public static final String SCALEOUT_EARLY_FAIL_NUMBER = "SCALEOUT_EARLY_FAIL_NUMBER";

    /**
     * for scaleout backfill test
     */
    public static final String SCALEOUT_BACKFILL_POSITION_MARK = "GSI_BACKFILL_POSITION_MARK";

    /**
     * debug mode on scale out, which makes scale out status change slower etc.
     */
    public static final String SCALE_OUT_DEBUG = "SCALE_OUT_DEBUG";

    /**
     * The wait time (unit: ms) before the status of each table convert
     * from write_only/write_reorg to public, this config is used to test scale out
     */
    public static final String SCALE_OUT_DEBUG_WAIT_TIME_IN_WO = "SCALE_OUT_DEBUG_WAIT_TIME_IN_WO";

    /**
     * set the table's final status for scaleout debug purpose.
     */
    public static final String SCALE_OUT_FINAL_TABLE_STATUS_DEBUG = "SCALE_OUT_FINAL_TABLE_STATUS_DEBUG";

    /**
     * set the database's final status for scaleout debug purpose.
     */
    public static final String SCALE_OUT_FINAL_DB_STATUS_DEBUG = "SCALE_OUT_FINAL_DB_STATUS_DEBUG";

    /**
     * to split physical table for backfill
     */
    public static final String PHYSICAL_TABLE_START_SPLIT_SIZE = "PHYSICAL_TABLE_START_SPLIT_SIZE";

    /**
     * the parallelism for backfill
     */
    public static final String BACKFILL_PARALLELISM = "BACKFILL_PARALLELISM";

    /**
     * max sample size of backfill physical table
     */
    public static final String BACKFILL_MAX_SAMPLE_ROWS = "BACKFILL_MAX_SAMPLE_ROWS";

    public static final String BACKFILL_MAX_SAMPLE_ROWS_FOR_PK_RANGE = "BACKFILL_MAX_SAMPLE_ROWS_FOR_PK_RANGE";

    public static final String BACKFILL_MAX_PK_RANGE_SIZE = "BACKFILL_MAX_PK_RANGE_SIZE";

    public static final String BACKFILL_MAX_TASK_PK_RANGE_SIZE = "BACKFILL_MAX_TASK_PK_RANGE_SIZE";

    public static final String BACKFILL_USE_RETURNING = "BACKFILL_USE_RETURNING";

    /**
     * enable split physical table for backfill
     */
    public static final String ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL = "ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL";

    public static final String PHYSICAL_TABLE_BACKFILL_PARALLELISM = "PHYSICAL_TABLE_BACKFILL_PARALLELISM";

    public static final String ENABLE_SLIDE_WINDOW_BACKFILL = "ENABLE_SLIDE_WINDOW_BACKFILL";

    public static final String SLIDE_WINDOW_TIME_INTERVAL = "SLIDE_WINDOW_TIME_INTERVAL";

    public static final String SLIDE_WINDOW_SPLIT_SIZE = "SLIDE_WINDOW_SPLIT_SIZE";

    /**
     * check target table after scaleout's backfill
     */
    public static final String SCALEOUT_CHECK_AFTER_BACKFILL = "SCALEOUT_CHECK_AFTER_BACKFILL";

    /**
     * SCALEOUT_BACKFILL_USE_FASTCHECKER
     */
    public static final String SCALEOUT_BACKFILL_USE_FASTCHECKER = "SCALEOUT_BACKFILL_USE_FASTCHECKER";

    /**
     * use fastchecker to check table
     */
    public static final String USE_FASTCHECKER = "USE_FASTCHECKER";

    /**
     * GSI_BACKFILL_USE_FASTCHECKER
     */
    public static final String GSI_BACKFILL_USE_FASTCHECKER = "GSI_BACKFILL_USE_FASTCHECKER";

    public static final String GSI_BACKFILL_ONLY_USE_FASTCHECKER = "GSI_BACKFILL_ONLY_USE_FASTCHECKER";

    public static final String GSI_BACKFILL_OVERRIDE_DDL_PARAMS = "GSI_BACKFILL_OVERRIDE_DDL_PARAMS";

    public static final String GSI_BUILD_LOCAL_INDEX_LATER = "GSI_BUILD_LOCAL_INDEX_LATER";

    public static final String GSI_BACKFILL_BY_PK_RANGE = "GSI_BACKFILL_BY_PK_RANGE";

    public static final String GSI_BACKFILL_BY_PARTITION = "GSI_BACKFILL_BY_PARTITION";

    public static final String GSI_JOB_MAX_PARALLELISM = "GSI_JOB_MAX_PARALLELISM";

    public static final String GSI_PK_RANGE_CPU_ACQUIRE = "GSI_PK_RANGE_CPU_ACQUIRE";

    public static final String GSI_PK_RANGE_LOCK_READ = "GSI_PK_RANGE_LOCK_READ";

    public static final String FP_FAILED_TABLE_SYNC = "FP_FAILED_TABLE_SYNC";

    public static final String FP_RANDOM_SUSPEND = "FP_RANDOM_SUSPEND";

    public static final String FP_DDL_INTERNAL_MAX_PARALLELISM = "FP_DDL_INTERNAL_MAX_PARALLELISM";
    /**
     * fastChecker use thread pool to control parallelism
     * each thread pool corresponds to a storage inst node
     */
    public static final String FASTCHECKER_THREAD_POOL_SIZE = "FASTCHECKER_THREAD_POOL_SIZE";

    /**
     * when fastchecker failed to calculate hash value because of timeout,
     * we will decrease the batch size and retry
     */
    public static final String FASTCHECKER_BATCH_TIMEOUT_RETRY_TIMES = "FASTCHECKER_BATCH_TIMEOUT_RETRY_TIMES";

    /**
     * if a physical table's row count exceed FASTCHECKER_BATCH_SIZE, we will start to check by batch
     */
    public static final String FASTCHECKER_BATCH_SIZE = "FASTCHECKER_BATCH_SIZE";

    /**
     * fastchecker max batch file size (bytes)
     */
    public static final String FASTCHECKER_BATCH_FILE_SIZE = "FASTCHECKER_BATCH_FILE_SIZE";
    public static final String FASTCHECKER_BATCH_PARALLEL = "FASTCHECKER_BATCH_PARALLEL";
    public static final String FASTCHECKER_ERROR_REPORT = "FASTCHECKER_ERROR_REPORT";

    /**
     * when fastchecker check table by batch, we limit the max sample percentage
     */
    public static final String FASTCHECKER_MAX_SAMPLE_PERCENTAGE = "FASTCHECKER_MAX_SAMPLE_PERCENTAGE";

    /**
     * enable resend snapshot seq for snapshot too old
     */
    public static final String FASTCHECKER_RESEND_SNAPSHOT = "FASTCHECKER_RESEND_SNAPSHOT";

    /**
     * when fastchecker check table by batch, we limit the max sample size
     */
    public static final String FASTCHECKER_MAX_SAMPLE_SIZE = "FASTCHECKER_MAX_SAMPLE_SIZE";

    public static final String FASTCHECKER_MAX_RECHECK_BATCH = "FASTCHECKER_MAX_RECHECK_BATCH";

    /**
     * allow to push down dml for the non-gsi and non-broadcast table
     * when shard groups has no scale-out group
     */
    public static final String SCALEOUT_DML_PUSHDOWN_OPTIMIZATION = "SCALEOUT_DML_PUSHDOWN_OPTIMIZATION";

    /**
     * allow to push down dml for the non-gsi and non-broadcast table
     * when the batch size is less than SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT
     */
    public static final String SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT = "SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT";

    /**
     * 当前实例是否支持scaleout这个功能，默认是关闭的
     */
    public static final String ENABLE_SCALE_OUT_FEATURE = "ENABLE_SCALE_OUT_FEATURE";

    /**
     * import table
     */
    public static final String IMPORT_TABLE = "IMPORT_TABLE";

    public static final String IMPORT_TABLE_PARALLELISM = "IMPORT_TABLE_PARALLELISM";

    /**
     * reimport table
     */
    public static final String REIMPORT_TABLE = "REIMPORT_TABLE";

    /**
     * import database
     */
    public static final String IMPORT_DATABASE = "IMPORT_DATABASE";

    /**
     * check whether enable all phy dml log during doing scale out
     */
    public static final String ENABLE_SCALE_OUT_ALL_PHY_DML_LOG = "ENABLE_SCALE_OUT_ALL_PHY_DML_LOG";

    /**
     * check whether enable scaleout dml log of one group during doing scale out
     */
    public static final String ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG = "ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG";

    /**
     * debug mode on scale out write for dml. When SCALE_OUT_WRITE_DEBUG=true,
     * all the dml will doing by scale out write
     */
    public static final String SCALE_OUT_WRITE_DEBUG = "SCALE_OUT_WRITE_DEBUG";

    /**
     * test mode on scale out write performance testing. When SCALE_OUT_WRITE_PERFORMANCE_TEST=true,
     * the scaleout status will not change anymore when reach FINISH_DB_MIG status
     */
    public static final String SCALE_OUT_WRITE_PERFORMANCE_TEST = "SCALE_OUT_WRITE_PERFORMANCE_TEST";

    /**
     * true: drop database directly after switch datasource
     * false: instead of drop database, rename all of its tables to a new database schema
     */
    public static final String SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE =
        "SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE";

    public static final String SCALE_OUT_GENERATE_MULTIPLE_TO_GROUP_JOB =
        "SCALE_OUT_GENERATE_MULTIPLE_TO_GROUP_JOB";
    /**
     * retry time when scaleout task fail
     */
    public static final String SCALEOUT_TASK_RETRY_TIME = "SCALEOUT_TASK_RETRY_TIME";

    /**
     * allow to execution the drop database ddl even though the scaleout task in not complete
     */
    public static final String ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE = "ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE";

    /**
     * force execute the drop database when the drop lock of the database is already fetched
     */

    public static final String ALLOW_DROP_DATABASE_FORCE = "ALLOW_DROP_DATABASE_FORCE";

    /**
     * reload the database/tables status from metadb for debug purpose.
     */
    public static final String RELOAD_SCALE_OUT_STATUS_DEBUG = "RELOAD_SCALE_OUT_STATUS_DEBUG";

    /**
     * allow Alter Global Secondary Index indirectly(rename GSI in primary table, alter drop covering column)
     */
    public static final String ALLOW_ALTER_GSI_INDIRECTLY = "ALLOW_ALTER_GSI_INDIRECTLY";

    public static final String ALLOW_ALTER_MODIFY_SK = "ALLOW_ALTER_MODIFY_SK";

    /**
     * allow drop part unique constrain(drop some not all columns in composite unique constrain) in primary table or UGSI.
     */
    public static final String ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI = "ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI";

    public static final String UNIQUE_GSI_WITH_PRIMARY_KEY = "UNIQUE_GSI_WITH_PRIMARY_KEY";

    /**
     * allow change/modify columns in which is also in GSI.
     */
    public static final String ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI = "ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI";

    /**
     * the default partition mode
     */
    public static final String DEFAULT_PARTITION_MODE = "DEFAULT_PARTITION_MODE";

    /**
     * allow auto partition.
     */
    public static final String AUTO_PARTITION = "AUTO_PARTITION";

    /**
     * Auto partition partitions.
     */
    public static final String AUTO_PARTITION_PARTITIONS = "AUTO_PARTITION_PARTITIONS";

    /**
     * Columnar default partitions
     */
    public static final String COLUMNAR_DEFAULT_PARTITIONS = "COLUMNAR_DEFAULT_PARTITIONS";

    /**
     * Specify the 'before status' of ALTER INDEX VISIBLE,
     * so that we can change cci status from CREATING to PUBLIC
     */
    public static final String ALTER_CCI_STATUS_BEFORE = "ALTER_CCI_STATUS_BEFORE";

    /**
     * Specify the 'after status' of ALTER INDEX VISIBLE,
     * so that we can change cci status from CREATING to PUBLIC
     */
    public static final String ALTER_CCI_STATUS_AFTER = "ALTER_CCI_STATUS_AFTER";

    /**
     * Enable change cci status with ALTER INDEX VISIBLE
     */
    public static final String ALTER_CCI_STATUS = "ALTER_CCI_STATUS";

    /**
     * allow create table gsi on table with column default current_timestamp
     */
    public static final String GSI_DEFAULT_CURRENT_TIMESTAMP = "GSI_DEFAULT_CURRENT_TIMESTAMP";

    /**
     * allow create table gsi on table with column on update current_timestamp
     */
    public static final String GSI_ON_UPDATE_CURRENT_TIMESTAMP = "GSI_ON_UPDATE_CURRENT_TIMESTAMP";

    /**
     * allow to ignore Global Secondary Index restriction(PK auto increment check)
     */
    public static final String GSI_IGNORE_RESTRICTION = "GSI_IGNORE_RESTRICTION";

    /**
     * check Global Secondary Index after creation
     */
    public static final String GSI_CHECK_AFTER_CREATION = "GSI_CHECK_AFTER_CREATION";

    /**
     * speed limit for all read/write procedure(backfill, check, insert select, modify)
     */
    public static final String GENERAL_DYNAMIC_SPEED_LIMITATION = "GENERAL_DYNAMIC_SPEED_LIMITATION";

    /**
     * batch size for check oss data procedure
     */
    public static final String CHECK_OSS_BATCH_SIZE = "CHECK_OSS_BATCH_SIZE";

    /**
     * batch size for backfill procedure
     */
    public static final String GSI_BACKFILL_BATCH_SIZE = "GSI_BACKFILL_BATCH_SIZE";

    /**
     * speed limit for backfill procedure
     */
    public static final String GSI_BACKFILL_SPEED_LIMITATION = "GSI_BACKFILL_SPEED_LIMITATION";

    public static final String GSI_BACKFILL_SPEED_MIN = "GSI_BACKFILL_SPEED_MIN";

    /**
     * parallelism of backfill procedure
     */
    public static final String GSI_BACKFILL_PARALLELISM = "GSI_BACKFILL_PARALLELISM";

    /**
     * batch size for check procedure
     */
    public static final String GSI_CHECK_BATCH_SIZE = "GSI_CHECK_BATCH_SIZE";

    /**
     * speed limit for check procedure
     */
    public static final String GSI_CHECK_SPEED_LIMITATION = "GSI_CHECK_SPEED_LIMITATION";
    /**
     * speed limit for check procedure
     */
    public static final String GSI_CHECK_SPEED_MIN = "GSI_CHECK_SPEED_MIN";
    /**
     * parallelism of check procedure
     */
    public static final String GSI_CHECK_PARALLELISM = "GSI_CHECK_PARALLELISM";

    /**
     * number of error for check early fail.
     */
    public static final String GSI_EARLY_FAIL_NUMBER = "GSI_EARLY_FAIL_NUMBER";

    /**
     * for gsi backfill test
     */
    public static final String GSI_BACKFILL_POSITION_MARK = "GSI_BACKFILL_POSITION_MARK";

    /**
     * Write primary and gsi concurrently
     */
    public static final String GSI_CONCURRENT_WRITE_OPTIMIZE = "GSI_CONCURRENT_WRITE_OPTIMIZE";

    /**
     * batch size for create database as
     */
    public static final String CREATE_DATABASE_AS_BATCH_SIZE = "CREATE_DATABASE_AS_BATCH_SIZE";

    /**
     * speed limit for CTAS
     */
    public static final String CREATE_DATABASE_AS_BACKFILL_SPEED_LIMITATION =
        "CREATE_DATABASE_AS_BACKFILL_SPEED_LIMITATION";

    /**
     * min speed for CTAS
     */
    public static final String CREATE_DATABASE_AS_BACKFILL_SPEED_MIN = "CREATE_DATABASE_AS_BACKFILL_SPEED_MIN";

    /**
     * backfill parallelism for CDAS
     */
    public static final String CREATE_DATABASE_AS_BACKFILL_PARALLELISM = "CREATE_DATABASE_AS_BACKFILL_PARALLELISM";

    /**
     * DDL Tasks parallelism for CTAS
     */
    public static final String CREATE_DATABASE_AS_TASKS_PARALLELISM = "CREATE_DATABASE_AS_TASKS_PARALLELISM";
    /**
     * whether use fastchecker for CTAS
     */
    public static final String CREATE_DATABASE_AS_USE_FASTCHECKER = "CREATE_DATABASE_AS_USE_FASTCHECKER";

    public static final String CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG = "CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG";
    /**
     * Write primary and gsi concurrently for load data
     */
    public static final String LOAD_DATA_IGNORE_IS_SIMPLE_INSERT = "LOAD_DATA_IGNORE_IS_SIMPLE_INSERT";

    public static final String ENABLE_LOAD_DATA_TRACE = "ENABLE_LOAD_DATA_TRACE";

    public static final String LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN = "LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN";

    public static final String LOAD_DATA_PURE_INSERT_MODE = "LOAD_DATA_PURE_INSERT_MODE";

    /**
     * handle empty char for load data
     */
    public static final String LOAD_DATA_HANDLE_EMPTY_CHAR = "LOAD_DATA_HANDLE_EMPTY_CHAR";

    /**
     * Write primary and gsi concurrently
     */
    public static final String GSI_CONCURRENT_WRITE = "GSI_CONCURRENT_WRITE";

    /**
     * the read/write parallelism of one phy group
     */
    public static final String GROUP_PARALLELISM = "GROUP_PARALLELISM";

    public static final String GSI_STATISTICS_COLLECTION = "GSI_STATISTICS_COLLECTION";

    /**
     * the switch of the read/write parallelism of one phy group
     */
    public static final String ENABLE_GROUP_PARALLELISM = "ENABLE_GROUP_PARALLELISM";

    /**
     * the switch of log the computing group connection key of all phyTableOperation
     */
    public static final String ENABLE_LOG_GROUP_CONN_KEY = "ENABLE_LOG_GROUP_CONN_KEY";

    /**
     * allow use group parallelism for select-query on autocommit=true trans when shareReadView is closed
     */
    public static final String ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW =
        "ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW";

    /**
     * for table lookup replicate all filter from index table to primary table
     */
    public static final String REPLICATE_FILTER_TO_PRIMARY = "REPLICATE_FILTER_TO_PRIMARY";

    /**
     * Reschedule failed DDL job caused by exception after certain minutes
     */
    public static final String PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES = "PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES";

    /**
     * enable MDL
     */
    public static final String ENABLE_MDL = "ENABLE_MDL";

    /**
     * Always rebuild plan
     */
    public static final String ALWAYS_REBUILD_PLAN = "ALWAYS_REBUILD_PLAN";

    /**
     * Number of workers to run parallel query
     */
    public static final String PARALLELISM = "PARALLELISM";

    /**
     * Number of shards to prefetch (only take effect under parallel query)
     */
    public static final String OSS_EXPORT_MAX_ROWS_PER_FILE = "OSS_EXPORT_MAX_ROWS_PER_FILE";

    public static final String ENCDB_ENCRYPTION_ALGORITHM = "ENCDB_ENCRYPTION_ALGORITHM";

    public static final String ENCDB_DEFAULT_ROLE_PRIVILEGE = "ENCDB_DEFAULT_ROLE_PRIVILEGE";

    public static final String PREFETCH_SHARDS = "PREFETCH_SHARDS";

    public static final String PHYSICAL_DDL_PARALLELISM = "PHYSICAL_DDL_PARALLELISM";

    public static final String MAX_CACHE_PARAMS = "MAX_CACHE_PARAMS";

    public static final String MAX_EXECUTE_MEMORY = "MAX_EXECUTE_MEMORY";

    public static final String CHUNK_SIZE = "CHUNK_SIZE";

    public static final String INDEX_ADVISOR_BROADCAST_THRESHOLD = "INDEX_ADVISOR_BROADCAST_THRESHOLD";

    public static final String SHARDING_ADVISOR_MAX_NODE_NUM = "SHARDING_ADVISOR_MAX_NODE_NUM";

    public static final String SHARDING_ADVISOR_APPRO_THRESHOLD = "SHARDING_ADVISOR_APPRO_THRESHOLD";

    public static final String SHARDING_ADVISOR_BROADCAST_THRESHOLD = "SHARDING_ADVISOR_BROADCAST_THRESHOLD";

    public static final String SHARDING_ADVISOR_SHARD = "SHARDING_ADVISOR_SHARD";

    public static final String SHARDING_ADVISOR_RECORD_PLAN = "SHARDING_ADVISOR_RECORD_PLAN";

    public static final String SHARDING_ADVISOR_RETURN_ANSWER = "SHARDING_ADVISOR_RETURN_ANSWER";

    /**
     * VECTORIZATION
     */
    public static final String ENABLE_EXPRESSION_VECTORIZATION = "ENABLE_EXPRESSION_VECTORIZATION";

    /**
     * record expression exec times
     */
    public static final String ENABLE_EXPRESSION_STATS = "ENABLE_RECORD_EXPRESSION";

    public static final String EXPRESSION_STATS_THRESHOLD = "EXPRESSION_RECORD_THRESHOLD";

    public static final String ENABLE_OPTIMIZE_RANDOM_EXCHANGE = "ENABLE_OPTIMIZE_RANDOM_EXCHANGE";

    /**
     * Allow constant fold when binding the vectorized expression.
     */
    public static final String ENABLE_EXPRESSION_CONSTANT_FOLD = "ENABLE_EXPRESSION_CONSTANT_FOLD";

    /**
     * SPM
     */
    public static final String PLAN_EXTERNALIZE_TEST = "PLAN_EXTERNALIZE_TEST";

    public static final String ENABLE_SPM = "ENABLE_SPM";

    public static final String ENABLE_MODULE_CHECK = "ENABLE_MODULE_CHECK";

    public static final String ENABLE_SPM_EVOLUTION_BY_TIME = "ENABLE_SPM_EVOLUTION_BY_TIME";

    public static final String ENABLE_HOT_GSI_EVOLUTION = "ENABLE_HOT_GSI_EVOLUTION";

    public static final String HOT_GSI_EVOLUTION_THRESHOLD = "HOT_GSI_EVOLUTION_THRESHOLD";

    public static final String ENABLE_SPM_BACKGROUND_TASK = "ENABLE_SPM_BACKGROUND_TASK";

    public static final String SPM_MAX_BASELINE_SIZE = "SPM_MAX_BASELINE_SIZE";

    public static final String SPM_DIFF_ESTIMATE_TIME = "SPM_DIFF_ESTIMATE_TIME";

    public static final String SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE = "SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE";

    public static final String SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE = "SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE";

    public static final String SPM_EVOLUTION_RATE = "SPM_EVOLUTION_RATE";

    public static final String SPM_PQO_STEADY_CHOOSE_TIME = "SPM_PQO_STEADY_CHOOSE_TIME";

    public static final String SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES = "SPM_MAX_UNACCEPTED_PLAN_EVOLUTION_TIMES";

    public static final String SPM_MAX_BASELINE_INFO_SQL_LENGTH = "SPM_MAX_BASELINE_INFO_SQL_LENGTH";

    public static final String SPM_MAX_PLAN_INFO_PLAN_LENGTH = "SPM_MAX_PLAN_INFO_PLAN_LENGTH";

    public static final String SPM_MAX_PLAN_INFO_ERROR_COUNT = "SPM_MAX_PLAN_INFO_ERROR_COUNT";

    public static final String SPM_RECENTLY_EXECUTED_PERIOD = "SPM_RECENTLY_EXECUTED_PERIOD";

    public static final String EXPLAIN_OUTPUT_FORMAT = "EXPLAIN_OUTPUT_FORMAT";

    public static final String SPM_MAX_PQO_PARAMS_SIZE = "SPM_MAX_PQO_PARAMS_SIZE";

    public static final String SPM_ENABLE_PQO = "SPM_ENABLE_PQO";

    /**
     * max length of sql text in sql.log, default is 4096
     */
    public static final String SQL_LOG_MAX_LENGTH = "SQL_LOG_MAX_LENGTH";

    /**
     * the max count limit of rex node when doing toDnf
     */
    public static final String DNF_REX_NODE_LIMIT = "DNF_REX_NODE_LIMIT";

    /**
     * the max count limit of rex node when doing toCnf
     */
    public static final String CNF_REX_NODE_LIMIT = "CNF_REX_NODE_LIMIT";

    /**
     * enable the memory limit for rex node during toCnf/toDnf
     */
    public static final String REX_MEMORY_LIMIT = "REX_MEMORY_LIMIT";

    /**
     * enable alter shard key, default is false
     */
    public static final String ENABLE_ALTER_SHARD_KEY = "ENABLE_ALTER_SHARD_KEY";

    /**
     * using SET @@rds_result_skip_counter = x
     */
    public static final String USING_RDS_RESULT_SKIP = "USING_RDS_RESULT_SKIP";

    /**
     * the connection time_zone that is defined by user, e.g. set time_zone="+09:00`;
     */
    public static final String CONN_TIME_ZONE = "CONN_TIME_ZONE";

    /**
     * AES block encryption mode
     */
    public static final String BLOCK_ENCRYPTION_MODE = "block_encryption_mode";

    /**
     * Check if random physical table name is enabled to support logical rename table.
     */
    public static final String ENABLE_RANDOM_PHY_TABLE_NAME = "ENABLE_RANDOM_PHY_TABLE_NAME";

    public static final String CHECK_PHYSICAL_TABLE = "CHECK_PHYSICAL_TABLE";

    /**
     * Check if asynchronous DDL is supported. It's FALSE by default.
     */
    public static final String ENABLE_ASYNC_DDL = "ENABLE_ASYNC_DDL";

    /**
     * Force DDLs to run on the legacy DDL engine (Async DDL).
     */
    public static final String FORCE_DDL_ON_LEGACY_ENGINE = "FORCE_DDL_ON_LEGACY_ENGINE";

    public static final String DDL_ENGINE_DEBUG = "DDL_ENGINE_DEBUG";

    public static final String DDL_ENGINE_RESOURCE_LIST = "DDL_ENGINE_RESOURCE_LIST";

    public static final String SKIP_DDL_FIXED_RESOURCE_CHECK = "SKIP_DDL_FIXED_RESOURCE_CHECK";

    public static final String ENABLE_DDL_TWO_PHASE_LOCK = "ENABLE_DDL_TWO_PHASE_LOCK";

    public static final String ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE = "ENABLE_DDL_RW_LOCK_FIFO_WAITING_QUEUE";

    public static final String DDL_SHARD_CHANGE_DEBUG = "DDL_SHARD_CHANGE_DEBUG";

    /**
     * Check if asynchronous DDL is pure, i.e. A DDL execution returns
     * immediately.
     */
    public static final String PURE_ASYNC_DDL_MODE = "PURE_ASYNC_DDL_MODE";

    public static final String EXPLAIN_SHOW_PERF_PARAMS = "EXPLAIN_SHOW_PERF_PARAMS";

    public static final String EXPLAIN_SHOW_DB_INDEX_MODE = "EXPLAIN_SHOW_DB_INDEX_MODE";

    public static final String SHOW_COMMAND_RAND_DISPATCH = "SHOW_COMMAND_RAND_DISPATCH";

    public static final String PERF_DDL_MODE = "PERF_DDL_MODE";

    public static final String BATCH_FILE_SIZE = "BATCH_FILE_SIZE";

    public static final String MAX_BATCH_FILE_SIZE_SPEED = "MAX_BATCH_FILE_SIZE_SPEED";

    /**
     * Label if return job_id on async_ddl_mode when submit ddl
     */
    public static final String RETURN_JOB_ID_ON_ASYNC_DDL_MODE = "RETURN_JOB_ID_ON_ASYNC_DDL_MODE";

    public static final String ENABLE_OPERATE_SUBJOB = "ENABLE_OPERATE_SUBJOB";

    public static final String SKIP_VALIDATE_STORAGE_INST_IDLE = "SKIP_VALIDATE_STORAGE_INST_IDLE";

    public static final String CANCEL_SUBJOB = "CANCEL_SUBJOB";

    public static final String EXPLAIN_DDL_PHYSICAL_OPERATION = "EXPLAIN_DDL_PHYSICAL_OPERATION";
    public static final String ENABLE_CONTINUE_RUNNING_SUBJOB = "ENABLE_CONTINUE_RUNNING_SUBJOB";
    /**
     * Check if the "INSTANT ADD COLUMN" feature is supported.
     */
    public static final String SUPPORT_INSTANT_ADD_COLUMN = "SUPPORT_INSTANT_ADD_COLUMN";

    /**
     * DDL job request timeout.
     */
    public static final String DDL_JOB_REQUEST_TIMEOUT = "DDL_JOB_REQUEST_TIMEOUT";

    /**
     * Indicate that how many logical DDLs are allowed to execute concurrently.
     */
    public static final String LOGICAL_DDL_PARALLELISM = "LOGICAL_DDL_PARALLELISM";

    /**
     * The number of async ddl job schedulers
     */
    public static final String NUM_OF_JOB_SCHEDULERS = "NUM_OF_JOB_SCHEDULERS";

    /**
     * Waiting time when no job is being handled (i.e. idle)
     */
    public static final String DDL_JOB_IDLE_WAITING_TIME = "DDL_JOB_IDLE_WAITING_TIME";

    public static final String ENABLE_ASYNC_PHY_OBJ_RECORDING = "ENABLE_ASYNC_PHY_OBJ_RECORDING";

    /**
     * Physical DDL MDL WAITING TIMEOUT
     */
    public static final String PHYSICAL_DDL_MDL_WAITING_TIMEOUT = "PHYSICAL_DDL_MDL_WAITING_TIMEOUT";

    /**
     * The timeout (in minutes) for acquiring DDL engine resource locks.
     */
    public static final String DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES = "DDL_ACQUIRE_LOCK_TIMEOUT_MINUTES";

    public static final String BACKFILL_MPP_CN_KEYS = "BACKFILL_MPP_CN_KEYS";

    /**
     * Check if server should automatically recover left jobs during initialization.
     */
    public static final String AUTOMATIC_DDL_JOB_RECOVERY = "AUTOMATIC_DDL_JOB_RECOVERY";

    /**
     * Comma separated string(.e.g "TASK1,TASK2"), using for skip execution some ddl task;
     * Only works for ddl tasks that handled this flag explicitly
     */
    public static final String SKIP_DDL_TASKS = "SKIP_DDL_TASKS";
    public static final String SKIP_DDL_TASKS_EXECUTE = "SKIP_DDL_TASKS_EXECUTE";
    public static final String SKIP_DDL_TASKS_ROLLBACK = "SKIP_DDL_TASKS_ROLLBACK";

    /**
     * Maximum number of table partitions per database.
     */
    public static final String MAX_TABLE_PARTITIONS_PER_DB = "MAX_TABLE_PARTITIONS_PER_DB";

    /**
     * Global time zone of logical db, will affect sql routing and evaluation of time related function.
     */
    public static final String LOGICAL_DB_TIME_ZONE = "LOGICAL_DB_TIME_ZONE";
    /**
     * The default time zone a of shard router
     * (e.g. dbpartition by YYYYDD(timestamp_cold), the default timezone of sharding is controlled by this variable )
     */
    public static final String SHARD_ROUTER_TIME_ZONE = "SHARD_ROUTER_TIME_ZONE";

    /**
     * The default collation of database if creating database without specifying any collation
     */
    public static final String COLLATION_SERVER = "COLLATION_SERVER";

    /**
     * Allow sharding with constant expression
     */
    public static final String ENABLE_SHARD_CONST_EXPR = "ENABLE_SHARD_CONST_EXPR";

    /**
     * FORBID_APPLY_CACHE
     */
    public static final String FORBID_APPLY_CACHE = "FORBID_APPLY_CACHE";

    /**
     * FORCE_APPLY_CACHE
     */
    public static final String FORCE_APPLY_CACHE = "FORCE_APPLY_CACHE";

    /**
     * Skip readonly check, Manager may do DDL(rename tables) after the servers
     * were set readonly
     */
    public static final String SKIP_READONLY_CHECK = "SKIP_READONLY_CHECK";

    /**
     * flag for window func optimize
     */
    public static final String WINDOW_FUNC_OPTIMIZE = "WINDOW_FUNC_OPTIMIZE";

    public static final String COL_HOLISTIC_SUBQUERY_UNNEST = "COL_HOLISTIC_SUBQUERY_UNNEST";

    public static final String FORCE_HOLISTIC_SUBQUERY_UNNEST = "FORCE_HOLISTIC_SUBQUERY_UNNEST";

    /**
     * Master switch to enable auto-detection of join ON sub-queries and trigger holistic sub-query unnest.
     * Defaults to false so the detection is disabled unless explicitly enabled.
     */
    public static final String ENABLE_JOIN_SUBQUERY_UNNEST = "ENABLE_JOIN_SUBQUERY_UNNEST";

    /**
     * Whether holistic sub-query unnest is applied to DML statements (INSERT/UPDATE/DELETE ...).
     * Defaults to true.
     */
    public static final String HOLISTIC_SUBQUERY_UNNEST_DML = "HOLISTIC_SUBQUERY_UNNEST_DML";

    /**
     * window func flag for condition judge
     */
    public static final String WINDOW_FUNC_SUBQUERY_CONDITION = "WINDOW_FUNC_SUBQUERY_CONDITION";

    /**
     * the num limit for correlate materialized judgement
     */
    public static final String PUSH_CORRELATE_MATERIALIZED_LIMIT = "PUSH_CORRELATE_MATERIALIZED_LIMIT";

    public static final String ENABLE_PUSH_CORRELATE_DOWN = "ENABLE_PUSH_CORRELATE_DOWN";

    /**
     * When enabled, SubQueryToSemiJoinRule skips the SemiJoin/Join rewrite and
     * forces correlated subqueries to be rewritten via buildCorrelateNode (CorrelateApply).
     */
    public static final String DISABLE_SUBQUERY_TO_SEMI_JOIN = "DISABLE_SUBQUERY_TO_SEMI_JOIN";

    public static final String ENABLE_TRANS_CORRELATE_TO_VALUES = "ENABLE_TRANS_CORRELATE_TO_VALUES";

    public static final String STATISTIC_COLLECTOR_FROM_RULE = "STATISTIC_COLLECTOR_FROM_RULE";

    public static final String ENABLE_MPP = "ENABLE_MPP";

    /**
     * Switch for the ParallelHashJoin passNothing fast-exit optimization on the INNER JOIN
     * empty-build branch. NOTE: the name semantic is inverted for historical reasons.
     * <p>
     * {@code true}  (default) = KEEP the passNothing optimization: when the build side is empty,
     * return immediately WITHOUT consuming the probe side.
     * {@code false}           = DISABLE the optimization: drain the probe side to EOF silently
     * (no hash lookup, no output rows) to keep the upstream Stage
     * from hanging under peer2peer schedule mode.
     * <p>
     * Session / global scope, runtime mutable. See Aone #79665023.
     */
    public static final String ENABLE_PASS_NOTHING_CONSUME_PROBE = "ENABLE_PASS_NOTHING_CONSUME_PROBE";

    public static final String SLAVE_SOCKET_TIMEOUT = "SLAVE_SOCKET_TIMEOUT";

    /**
     * Maximum num for data that could be broadcast.
     */
    public static final String MPP_JOIN_BROADCAST_NUM = "MPP_JOIN_BROADCAST_NUM";

    public static final String MPP_QUERY_MANAGER_THREAD_SIZE = "MPP_QUERY_MANAGER_THREAD_SIZE";

    public static final String MPP_QUERY_EXECUTION_THREAD_SIZE = "MPP_QUERY_EXECUTION_THREAD_SIZE";

    public static final String MPP_REMOTE_TASK_CALLBACK_THREAD_SIZE = "MPP_REMOTE_TASK_CALLBACK_THREAD_SIZE";

    public static final String MPP_TASK_NOTIFICATION_THREAD_SIZE = "MPP_TASK_NOTIFICATION_THREAD_SIZE";

    public static final String MPP_TASK_YIELD_THREAD_SIZE = "MPP_TASK_YIELD_THREAD_SIZE";

    public static final String MPP_MAX_WORKER_THREAD_SIZE = "MPP_MAX_WORKER_THREAD_SIZE";

    public static final String MPP_TASK_FUTURE_CALLBACK_THREAD_SIZE = "MPP_TASK_FUTURE_CALLBACK_THREAD_SIZE";

    public static final String MPP_EXCHANGE_CLIENT_THREAD_SIZE = "MPP_EXCHANGE_CLIENT_THREAD_SIZE";

    public static final String MPP_HTTP_DATA_RESPONSE_THREAD_SIZE = "MPP_HTTP_DATA_RESPONSE_THREAD_SIZE";

    public static final String MPP_HTTP_CONTROL_RESPONSE_THREAD_SIZE = "MPP_HTTP_CONTROL_RESPONSE_THREAD_SIZE";

    public static final String MPP_HTTP_TIMEOUT_THREAD_SIZE = "MPP_HTTP_TIMEOUT_THREAD_SIZE";

    public static final String MPP_PARALLELISM = "MPP_PARALLELISM";

    public static final String MPP_SCAN_PARALLELISM = "MPP_SCAN_PARALLELISM";

    public static final String MPP_NODE_SIZE = "MPP_NODE_SIZE";

    public static final String MPP_NODE_RANDOM_MODE = "MPP_NODE_RANDOM_MODE";

    public static final String MPP_PREFER_LOCAL_NODE = "MPP_PREFER_LOCAL_NODE";

    public static final String SCHEDULE_BY_PARTITION = "SCHEDULE_BY_PARTITION";

    //-------------------------------------------- http rpc thread -----------------------------------------

    public static final String MPP_HTTP_IDLE_TIMEOUT = "MPP_HTTP_IDLE_TIMEOUT"; //1h
    public static final String MPP_HTTP_REQUEST_TIMEOUT = "MPP_HTTP_REQUEST_TIMEOUT"; //25s
    public static final String MPP_HTTP_CONNECT_TIMEOUT = "MPP_HTTP_CONNECT_TIMEOUT"; //3s
    public static final String MPP_HTTP_MAX_CONTENT_LENGTH = "MPP_HTTP_MAX_CONTENT_LENGTH"; //128MB

    public static final String MPP_HTTP_SERVER_MAX_THREADS = "MPP_HTTP_SERVER_MAX_THREADS"; //200
    public static final String MPP_HTTP_SERVER_MIN_THREADS = "MPP_HTTP_SERVER_MIN_THREADS"; //2
    public static final String MPP_HTTP_CLIENT_MAX_THREADS = "MPP_HTTP_CLIENT_MAX_THREADS"; //200
    public static final String MPP_HTTP_CLIENT_MIN_THREADS = "MPP_HTTP_CLIENT_MIN_THREADS"; //8
    public static final String MPP_HTTP_MAX_REQUESTS_PER_DESTINATION = "MPP_HTTP_MAX_REQUESTS_PER_DESTINATION"; //5000
    public static final String MPP_HTTP_CLIENT_MAX_CONNECTIONS = "MPP_HTTP_CLIENT_MAX_CONNECTIONS"; //250
    public static final String MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER =
        "MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER"; //20

    /**
     * 一个查询在单数据库实例上允许的最大并发度
     */
    public static final String DATABASE_PARALLELISM = "DATABASE_PARALLELISM";

    /**
     * 一个查询在单数据库PolarX节点上允许的最大并发度
     */
    public static final String POLARDBX_PARALLELISM = "POLARDBX_PARALLELISM";

    public static final String ALLOW_COLUMNAR_BIND_MASTER = "ALLOW_COLUMNAR_BIND_MASTER";

    /**
     * 是否对单表进行拆分
     */
    public static final String SEGMENTED = "SEGMENTED";

    /**
     * 对单表的拆分数量
     */
    public static final String SEGMENTED_COUNT = "SEGMENTED_COUNT";

    /**
     * push policy : ALL,BROADCAST,NOTHING
     */
    public static final String PUSH_POLICY = "PUSH_POLICY";

    public static final String SUPPORT_PUSH_AMONG_DIFFERENT_DB = "SUPPORT_PUSH_AMONG_DIFFERENT_DB";

    public static final String SIMPLIFY_MULTI_DB_SINGLE_TB_PLAN = "SIMPLIFY_MULTI_DB_SINGLE_TB_PLAN";

    public static final String MPP_QUERY_MAX_RUN_TIME = "MPP_QUERY_MAX_RUN_TIME";

    /**
     * Query生成到这个Query的driver第一次被调用的时间差阈值上限，用于回退
     */
    public static final String MPP_QUERY_MAX_DELAY_TIME = "MPP_QUERY_MAX_DELAY_TIME";

    /**
     * Query生成到这个Query的driver第一次被调用的时间差阈值下限，用于恢复local mpp
     */
    public static final String MPP_QUERY_MIN_DELAY_TIME = "MPP_QUERY_MIN_DELAY_TIME";

    /**
     * Query生成到这个Query的driver第一次被调用的时间差达到阈值的次数，回退
     */
    public static final String MPP_QUERY_DELAY_COUNT = "MPP_QUERY_DELAY_COUNT";

    /**
     * TaskExecutor的PendingSplitSize阈值上限，用于回退
     */
    public static final String MPP_QUERY_MAX_DELAY_PENDING_RATIO = "MPP_QUERY_MAX_DELAY_PENDING_RATIO";

    /**
     * TaskExecutor的PendingSplitSize阈值下限，用于恢复local mpp
     */
    public static final String MPP_QUERY_MIN_DELAY_PENDING_RATIO = "MPP_QUERY_MIN_DELAY_PENDING_RATIO";

    public static final String MPP_TASK_MAX_RUN_TIME = "MPP_TASK_MAX_RUN_TIME";

    public static final String MPP_CPU_CFS_PERIOD_US = "MPP_CPU_CFS_PERIOD_US";

    public static final String MPP_CPU_CFS_QUOTA = "MPP_CPU_CFS_QUOTA";

    public static final String MPP_CPU_CFS_MIN_QUOTA = "MPP_CPU_CFS_MIN_QUOTA";

    public static final String MPP_CPU_CFS_MAX_QUOTA = "MPP_CPU_CFS_MAX_QUOTA";

    public static final String MPP_AP_PRIORITY = "MPP_AP_PRIORITY";

    /**
     * sqlQuery FIFO队列的清理判断界限，单位ms
     */
    public static final String MPP_MIN_QUERY_EXPIRE_TIME = "MPP_MIN_QUERY_EXPIRE_TIME";

    public static final String MPP_RESERVED_SLOW_QUERY_TIME = "MPP_RESERVED_SLOW_QUERY_TIME";

    public static final String MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME = "MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME";

    public static final String MPP_MAX_QUERY_HISTORY = "MPP_MAX_QUERY_HISTORY";

    public static final String MPP_QUERY_CLIENT_TIMEOUT = "MPP_QUERY_CLIENT_TIMEOUT";

    public static final String MPP_QUERY_REMOTE_TASK_MIN_ERROR = "MPP_QUERY_REMOTETASK_MIN_ERROR_DURATION";

    public static final String MPP_QUERY_REMOTE_TASK_MAX_ERROR = "MPP_QUERY_REMOTETASK_MAX_ERROR_DURATION";

    public static final String MPP_QUERY_MAX_CPU_TIME = "MPP_QUERY_MAX_CPU_TIME";

    public static final String MPP_MAX_PARALLELISM = "MPP_MAX_PARALLELISM";

    public static final String PARALLELISM_FOR_EMPTY_TABLE = "PARALLELISM_FOR_EMPTY_TABLE";

    public static final String MPP_MIN_PARALLELISM = "MPP_MIN_PARALLELISM";

    public static final String MPP_QUERY_ROWS_PER_PARTITION = "MPP_QUERY_ROWS_PER_PARTITION";

    public static final String MPP_QUERY_IO_PER_PARTITION = "MPP_QUERY_IO_PER_PARTITION";

    public static final String LOOKUP_JOIN_PARALLELISM_FACTOR = "LOOKUP_JOIN_PARALLELISM_FACTOR";

    public static final String MPP_PARALLELISM_AUTO_ENABLE = "MPP_PARALLELISM_AUTO_ENABLE";

    public static final String SHOW_PIPELINE_INFO_UNDER_MPP = "SHOW_PIPELINE_INFO_UNDER_MPP";

    public static final String ENABLE_TWO_CHOICE_SCHEDULE = "ENABLE_TWO_CHOICE_SCHEDULE";

    public static final String ENABLE_COLUMNAR_SCHEDULE = "ENABLE_COLUMNAR_SCHEDULE";

    public static final String MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE = "MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE";
    public static final String MPP_SCHEDULE_MAX_SPLITS_PER_NODE = "MPP_SCHEDULE_MAX_SPLITS_PER_NODE";

    public static final String MPP_SCHEMA_MAX_MEM = "MPP_SCHEMA_MAX_MEMORY";

    public static final String MPP_TP_TASK_WORKER_THREADS_RATIO = "MPP_TP_WORKER_THREADS_RATIO";

    public static final String MPP_TASK_WORKER_THREADS_RATIO = "MPP_WORKER_THREADS_RATIO";

    public static final String MPP_SPLIT_RUN_QUANTA = "MPP_SPLIT_RUN_QUANTA";

    public static final String MPP_STATUS_REFRESH_MAX_WAIT = "MPP_STATUS_REFRESH_MAX_WAIT";

    public static final String MPP_INFO_UPDATE_INTERVAL = "MPP_INFO_UPDATE_INTERVAL";

    public static final String MPP_OUTPUT_MAX_BUFFER_SIZE = "MPP_OUTPUT_MAX_BUFFER_SIZE";

    public static final String MPP_TASK_CLIENT_TIMEOUT = "MPP_TASK_CLIENT_TIMEOUT";

    public static final String MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS = "MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS";

    public static final String MPP_LOW_PRIORITY_ENABLED = "MPP_TASK_LOW_PRIORITY_ENABLED";

    public static final String MPP_TASK_LOCAL_MAX_BUFFER_SIZE = "MPP_TASK_LOCAL_MAX_BUFFER_SIZE";

    public static final String MPP_TASK_LOCAL_BUFFER_ENABLED = "MPP_TASK_LOCAL_BUFFER_ENABLED";

    /**
     * mpp配置了这个属性，如果atom连接池的最大连接数小于MPP_TABLESCAN_DS_MAX_SIZE，则使用MPP_TABLESCAN_DS_MAX_SIZE
     * 主要用于集团内corona mpp环境
     */
    public static final String MPP_TABLESCAN_DS_MAX_SIZE = "MPP_TABLESCAN_DS_MAX_SIZE";

    public static final String MPP_TABLESCAN_CONNECTION_STRATEGY = "MPP_TABLESCAN_CONNECTION_STRATEGY";

    public static final String MPP_EXCHANGE_MAX_BUFFER_SIZE = "MPP_EXCHANGE_MAX_BUFFER_SIZE";

    public static final String MPP_EXCHANGE_CONCURRENT_MULTIPLIER = "MPP_EXCHANGE_CONCURRENT_REQUEST_MULTIPLIER";

    public static final String MPP_EXCHANGE_MIN_ERROR_DURATION = "MPP_EXCHANGE_MIN_ERROR_DURATION";

    public static final String MPP_EXCHANGE_MAX_ERROR_DURATION = "MPP_EXCHANGE_MAX_ERROR_DURATION";

    public static final String MPP_EXCHANGE_MAX_RESPONSE_SIZE = "MPP_EXCHANGE_MAX_RESPONSE_SIZE";

    /**
     * exchange 的input如果在本进程的话，是否直接走内存获取，不走RPC接口
     * 当前打开选项，不需要经过序列化和反序列化，且不走网络传输
     */
    public static final String MPP_RPC_LOCAL_ENABLED = "MPP_RPC_LOCAL_ENABLED";

    public static final String MPP_PRINT_ELAPSED_LONG_QUERY_ENABLED = "MPP_PRINT_ELAPSED_QUERY_ENABLED";

    public static final String MPP_ELAPSED_QUERY_THRESHOLD_MILLS = "MPP_ELAPSED_QUERY_THRESHOLD_MILLS";

    public static final String MPP_ENABLE_UI = "MPP_ENABLE_UI";

    public static final String MPP_METRIC_LEVEL = "MPP_METRIC_LEVEL";

    public static final String MPP_QUERY_NEED_RESERVE = "MPP_QUERY_NEED_RESERVE";

    /**
     * Allow update sharding column
     */
    public static final String ENABLE_MODIFY_SHARDING_COLUMN = "ENABLE_MODIFY_SHARDING_COLUMN";

    public static final String NDV_ALIKE_PRECENTAGE_THRESHOLD = "NDV_ALIKE_PRECENTAGE_THRESHOLD";

    public static final String ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO = "ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO";
    /**
     * Allow multi update/delete cross db
     */
    public static final String ENABLE_COMPLEX_DML_CROSS_DB = "ENABLE_COMPLEX_DML_CROSS_DB";
    /**
     * Multi update/delete cross db with transaction
     */
    public static final String COMPLEX_DML_WITH_TRX = "COMPLEX_DML_WITH_TRX";

    public static final String ENABLE_PUSHDOWN_DISTINCT = "ENABLE_PUSHDOWN_DISTINCT";
    /**
     * Enable index selection
     */
    public static final String ENABLE_INDEX_SELECTION = "ENABLE_INDEX_SELECTION";

    public static final String ENABLE_INDEX_SELECTION_PRUNE = "ENABLE_INDEX_SELECTION_PRUNE";

    public static final String GSI_SHARD_DIFFERENCE_THRESHOLD = "GSI_SHARD_DIFFERENCE_THRESHOLD";

    public static final String ENABLE_INDEX_SKYLINE = "ENABLE_INDEX_SKYLINE";
    public static final String ENABLE_MERGE_INDEX = "ENABLE_MERGE_INDEX";
    public static final String ENABLE_OSS_INDEX_SELECTION = "ENABLE_OSS_INDEX_SELECTION";

    public static final String ENABLE_PROJECT_TO_WINDOW_OPT = "ENABLE_PROJECT_TO_WINDOW_OPT";

    /**
     * whether to use plan cache for columnar plan
     */
    public static final String ENABLE_COLUMNAR_PLAN_CACHE = "ENABLE_COLUMNAR_PLAN_CACHE";
    public static final String ENABLE_COLUMNAR_PULL_UP_PROJECT = "ENABLE_COLUMNAR_PULL_UP_PROJECT";
    /**
     * Enable index selection
     */
    public static final String SWITCH_GROUP_ONLY = "SWITCH_GROUP_ONLY";
    /**
     * externalize plan
     */
    public static final String PLAN = "PLAN";
    /**
     * Enable print all profile info into sql.log
     */
    public static final String ENABLE_SQL_PROFILE_LOG = "ENABLE_SQL_PROFILE_LOG";
    /**
     * Enable the spl profile function
     */
    public static final String ENABLE_CPU_PROFILE = "ENABLE_CPU_PROFILE";
    /**
     * Enable the using of memory pool
     */
    public static final String ENABLE_MEMORY_POOL = "ENABLE_MEMORY_POOL";
    /**
     * 临时表超过最大条数时，是否截断数据。 单个查询能使用的最大内存大小
     */
    public static final String PER_QUERY_MEMORY_LIMIT = "PER_QUERY_MEMORY_LIMIT";
    /**
     * 单个 Schema 使用的最大内存大小
     */
    public static final String SCHEMA_MEMORY_LIMIT = "SCHEMA_MEMORY_LIMIT";
    /**
     * 整个机器节点的使用的最大内存大小
     */
    public static final String GLOBAL_MEMORY_LIMIT = "GLOBAL_MEMORY_LIMIT";
    /**
     * 是否允许进行内存资源限制
     */
    public static final String ENABLE_MEMORY_LIMITATION = "ENABLE_MEMORY_LIMITATION";
    /**
     * enable post planner
     */
    public static final String ENABLE_POST_PLANNER = "ENABLE_POST_PLANNER";
    /**
     * enable direct plan
     */
    public static final String ENABLE_DIRECT_PLAN = "ENABLE_DIRECT_PLAN";
    public static final String MPP_MEMORY_REVOKING_THRESHOLD = "MPP_MEMORY_REVOKING_THRESHOLD";
    public static final String MPP_MEMORY_REVOKING_TARGET = "MPP_MEMORY_REVOKING_TARGET";
    public static final String MPP_NOTIFY_BLOCKED_QUERY_MEMORY = "MPP_NOTIFY_BLOCKED_QUERY_MEMORY";
    public static final String TP_LOW_MEMORY_PROPORTION = "TP_LOW_MEMORY_PROPORTION";
    public static final String TP_HIGH_MEMORY_PROPORTION = "TP_HIGH_MEMORY_PROPORTION";
    public static final String AP_LOW_MEMORY_PROPORTION = "AP_LOW_MEMORY_PROPORTION";
    public static final String AP_HIGH_MEMORY_PROPORTION = "AP_HIGH_MEMORY_PROPORTION";
    /**
     * 支持spill
     */
    public static final String ENABLE_SPILL = "ENABLE_SPILL";
    public static final String MPP_MAX_SPILL_THREADS = "MPP_MAX_SPILL_THREADS";
    public static final String MPP_SPILL_PATHS = "MPP_SPILL_PATHS";
    public static final String MPP_LOG_PATHS = "MPP_LOG_PATHS";

    public static final String MAX_SPILL_SPACE_THRESHOLD = "MAX_SPILL_SPACE_THRESHOLD";

    public static final String MPP_AVAILABLE_SPILL_SPACE_THRESHOLD = "MPP_AVAILABLE_SPILL_SPACE_THRESHOLD";

    public static final String MAX_QUERY_SPILL_SPACE_THRESHOLD = "MAX_QUERY_SPILL_SPACE_THRESHOLD";

    public static final String MPP_MAX_SPILL_FD_THRESHOLD = "MPP_MAX_SPILL_FD_THRESHOLD";
    public static final String HYBRID_HASH_JOIN_BUCKET_NUM = "HYBRID_HASH_JOIN_BUCKET_NUM";
    public static final String HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM = "HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM";
    public static final String HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH = "HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH";
    public static final String MPP_LESS_REVOKE_BYTES = "MPP_LESS_REVOKE_BYTES";
    public static final String MPP_ALLOCATOR_SIZE = "MPP_ALLOCATOR_SIZE";
    public static final String MPP_CLUSTER_NAME = "MPP_CLUSTER_NAME";
    /**
     * enable parameter plan, for view plan
     */
    public static final String ENABLE_PARAMETER_PLAN = "ENABLE_PARAMETER_PLAN";
    /**
     * enable folding table-source VALUES into typed RawString parameters
     */
    public static final String ENABLE_DYNAMIC_VALUES_OPTIMIZATION = "ENABLE_DYNAMIC_VALUES_OPTIMIZATION";
    /**
     * enable cross view optimize
     */
    public static final String ENABLE_CROSS_VIEW_OPTIMIZE = "ENABLE_CROSS_VIEW_OPTIMIZE";
    public static final String MPP_GLOBAL_MEMORY_LIMIT_RATIO = "MPP_GLOBAL_MEMORY_LIMIT_RATIO";
    /**
     * conn pool properties
     */
    public static final String CONN_POOL_PROPERTIES = "CONN_POOL_PROPERTIES";
    public static final String CONN_POOL_MIN_POOL_SIZE = "CONN_POOL_MIN_POOL_SIZE";
    public static final String CONN_POOL_MAX_POOL_SIZE = "CONN_POOL_MAX_POOL_SIZE";
    public static final String CONN_POOL_MAX_WAIT_THREAD_COUNT = "CONN_POOL_MAX_WAIT_THREAD_COUNT";
    public static final String CONN_POOL_IDLE_TIMEOUT = "CONN_POOL_IDLE_TIMEOUT";
    public static final String CONN_POOL_BLOCK_TIMEOUT = "CONN_POOL_BLOCK_TIMEOUT";
    public static final String CONN_POOL_XPROTO_CONFIG = "CONN_POOL_XPROTO_CONFIG";
    public static final String CONN_POOL_XPROTO_FLAG = "CONN_POOL_XPROTO_FLAG";
    public static final String CONN_POOL_XPROTO_META_DB_PORT = "CONN_POOL_XPROTO_META_DB_PORT";
    public static final String CONN_POOL_XPROTO_STORAGE_DB_PORT = "CONN_POOL_XPROTO_STORAGE_DB_PORT";
    public static final String CONN_POOL_XPROTO_MAX_CLIENT_PER_INST = "CONN_POOL_XPROTO_MAX_CLIENT_PER_INST";
    public static final String CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT = "CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT";
    public static final String CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST =
        "CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST";
    public static final String CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST =
        "CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST";
    public static final String CONN_POOL_XPROTO_SESSION_AGING_TIME = "CONN_POOL_XPROTO_SESSION_AGING_TIME";
    public static final String CONN_POOL_XPROTO_SLOW_THRESH = "CONN_POOL_XPROTO_SLOW_THRESH";
    public static final String CONN_POOL_XPROTO_AUTH = "CONN_POOL_XPROTO_AUTH";
    public static final String CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE = "CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE";
    public static final String CONN_POOL_XPROTO_XPLAN = "CONN_POOL_XPROTO_XPLAN";
    public static final String XPLAN_MAX_SCAN_ROWS = "XPLAN_MAX_SCAN_ROWS";
    public static final String CONN_POOL_XPROTO_XPLAN_EXPEND_STAR = "CONN_POOL_XPROTO_XPLAN_EXPEND_STAR";
    public static final String CONN_POOL_XPROTO_XPLAN_TABLE_SCAN = "CONN_POOL_XPROTO_XPLAN_TABLE_SCAN";
    public static final String CONN_POOL_XPROTO_TRX_LEAK_CHECK = "CONN_POOL_XPROTO_TRX_LEAK_CHECK";
    public static final String CONN_POOL_XPROTO_MESSAGE_TIMESTAMP = "CONN_POOL_XPROTO_MESSAGE_TIMESTAMP";
    public static final String CONN_POOL_XPROTO_PLAN_CACHE = "CONN_POOL_XPROTO_PLAN_CACHE";
    public static final String CONN_POOL_XPROTO_CHUNK_RESULT = "CONN_POOL_XPROTO_CHUNK_RESULT";
    public static final String CONN_POOL_XPROTO_PURE_ASYNC_MPP = "CONN_POOL_XPROTO_PURE_ASYNC_MPP";
    public static final String CONN_POOL_XPROTO_CHECKER = "CONN_POOL_XPROTO_CHECKER";
    public static final String CONN_POOL_XPROTO_DIRECT_WRITE = "CONN_POOL_XPROTO_DIRECT_WRITE";
    public static final String CONN_POOL_XPROTO_FEEDBACK = "CONN_POOL_XPROTO_FEEDBACK";
    public static final String CONN_POOL_XPROTO_MAX_PACKET_SIZE = "CONN_POOL_XPROTO_MAX_PACKET_SIZE";
    public static final String CONN_POOL_XPROTO_QUERY_TOKEN = "CONN_POOL_XPROTO_QUERY_TOKEN";
    public static final String CONN_POOL_XPROTO_PIPE_BUFFER_SIZE = "CONN_POOL_XPROTO_PIPE_BUFFER_SIZE";
    /**
     * Dynamic X-Protocol config.
     */
    public static final String XPROTO_MAX_DN_CONCURRENT = "XPROTO_MAX_DN_CONCURRENT";
    /**
     * Dynamic X-Protocol config.
     */
    public static final String XPROTO_MAX_DN_WAIT_CONNECTION = "XPROTO_MAX_DN_WAIT_CONNECTION";
    /**
     * Retry times when establishing a GMS sync connection to a CN manager port.
     */
    public static final String GMS_SYNC_CONNECTION_RETRY_TIMES = "GMS_SYNC_CONNECTION_RETRY_TIMES";
    /**
     * Retry interval in milliseconds when establishing a GMS sync connection to a CN manager port.
     */
    public static final String GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS = "GMS_SYNC_CONNECTION_RETRY_INTERVAL_MS";
    /**
     * X-Protocol always keep upper filter when use XPlan
     */
    public static final String XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET = "XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET";
    /**
     * x-protocol probe timeout.
     */
    public static final String XPROTO_PROBE_TIMEOUT = "XPROTO_PROBE_TIMEOUT";
    /**
     * Galaxy prepare config.
     */
    public static final String XPROTO_GALAXY_PREPARE = "XPROTO_GALAXY_PREPARE";
    /**
     * X-Protocol / XRPC flow control pipe max size(in KB, 1024 means 1MB).
     */
    public static final String XPROTO_FLOW_CONTROL_SIZE_KB = "XPROTO_FLOW_CONTROL_SIZE_KB";
    /**
     * X-Protocol / XRPC TCP aging time in seconds.
     */
    public static final String XPROTO_TCP_AGING = "XPROTO_TCP_AGING";

    /**
     * Enable smooth switchover.
     */
    public static final String ENABLE_SMOOTH_SWITCHOVER = "ENABLE_SMOOTH_SWITCHOVER";

    public static final String ENABLE_STATISTIC_TRACE = "ENABLE_STATISTIC_TRACE";

    public static final String ENABLE_LOG_PLAN_BUILD = "ENABLE_LOG_PLAN_BUILD";

    /**
     * For test only. Force the switchover-check path (PartitionGather over the plan) to run
     * regardless of the real DN leader-changing state, and rethrow any error raised during
     * gathering instead of swallowing it. Used to reproduce the switchover upsert NPE via SQL.
     */
    public static final String FORCE_SWITCHOVER_CHECK_FOR_TEST = "FORCE_SWITCHOVER_CHECK_FOR_TEST";

    /**
     * Timeout of switchover wait in millis.
     */
    public static final String SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS = "SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS";
    /**
     * Switchover check interval in millis.
     */
    public static final String SWITCHOVER_CHECK_INTERVAL_IN_MILLIS = "SWITCHOVER_CHECK_INTERVAL_IN_MILLIS";
    /**
     * Release dirty read connection when switchover.
     */
    public static final String RELEASE_DIRTY_READ_CONNECTION_WHEN_SWITCHOVER =
        "RELEASE_DIRTY_READ_CONNECTION_WHEN_SWITCHOVER";

    /**
     * The storage inst list of all single groups when creating new database
     */
    public static final String SINGLE_GROUP_STORAGE_INST_LIST = "SINGLE_GROUP_STORAGE_INST_LIST";
    /**
     * The count of shard phy db on each storage inst
     */
    public static final String SHARD_DB_COUNT_EACH_STORAGE_INST = "SHARD_DB_COUNT_EACH_STORAGE_INST";
    /**
     * The count of shard phy db on each storage inst for one create db stmt
     */
    public static final String SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT = "SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT";
    public static final String MAX_LOGICAL_DB_COUNT = "MAX_LOGICAL_DB_COUNT";
    /**
     * Password rule config
     */
    public static final String PASSWORD_RULE_CONFIG = "PASSWORD_RULE_CONFIG";
    public static final String MAX_AUDIT_LOG_CLEAN_KEEP_DAYS = "MAX_AUDIT_LOG_CLEAN_KEEP_DAYS";
    public static final String MAX_AUDIT_LOG_CLEAN_DELAY_DAYS = "MAX_AUDIT_LOG_CLEAN_DELAY_DAYS";
    public static final String LOGIN_ERROR_MAX_COUNT_CONFIG = "LOGIN_ERROR_MAX_COUNT_CONFIG";
    public static final String ENABLE_LOGIN_AUDIT_CONFIG = "ENABLE_LOGIN_AUDIT_CONFIG";
    /**
     * enable the forbid of all the pushed dml with hint
     */
    public static final String ENABLE_FORBID_PUSH_DML_WITH_HINT = "ENABLE_FORBID_PUSH_DML_WITH_HINT";
    public static final String VARIABLE_EXPIRE_TIME = "VARIABLE_EXPIRE_TIME";
    public static final String MERGE_SORT_BUFFER_SIZE = "MERGE_SORT_BUFFER_SIZE";
    public static final String ENABLE_AGG_PRUNING = "ENABLE_AGG_PRUNING";
    public static final String CONVERTER_IN_ONE_RELSET = "CONVERTER_IN_ONE_RELSET";
    public static final String WORKLOAD_CPU_THRESHOLD = "WORKLOAD_CPU_THRESHOLD";
    public static final String WORKLOAD_MEMORY_THRESHOLD = "WORKLOAD_MEMORY_THRESHOLD";
    public static final String WORKLOAD_IO_THRESHOLD = "WORKLOAD_IO_THRESHOLD";

    public static final String WORKLOAD_OSS_NET_THRESHOLD = "WORKLOAD_OSS_NET_THRESHOLD";
    public static final String WORKLOAD_COLUMNAR_ROW_THRESHOLD = "WORKLOAD_COLUMNAR_ROW_THRESHOLD";
    public static final String WORKLOAD_TYPE = "WORKLOAD_TYPE";

    public static final String ENABLE_OSS_MOCK_COLUMNAR = "ENABLE_OSS_MOCK_COLUMNAR";
    public static final String SUB_INST_ROLE_TYPE = "SUB_INST_ROLE_TYPE";
    public static final String ENABLE_COLUMNAR_OPTIMIZER = "ENABLE_COLUMNAR_OPTIMIZER";
    public static final String ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR = "ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR";
    public static final String EXECUTOR_MODE = "EXECUTOR_MODE";
    public static final String ENABLE_MASTER_MPP = "ENABLE_MASTER_MPP";
    public static final String ENABLE_TEMP_TABLE_JOIN = "ENABLE_TEMP_TABLE_JOIN";
    public static final String ENABLE_CTE_REUSE = "ENABLE_CTE_REUSE";
    public static final String CTE_MODE = "CTE_MODE";
    public static final String CTE_REUSE_THRESHOLD = "CTE_REUSE_THRESHOLD";
    public static final String CTE_PARSER_THRESHOLD = "CTE_PARSER_THRESHOLD";
    public static final String CTE_MAX_NESTING_DEPTH = "CTE_MAX_NESTING_DEPTH";
    public static final String LOOKUP_IN_VALUE_LIMIT = "LOOKUP_IN_VALUE_LIMIT";
    public static final String LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD = "LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD";
    public static final String ENABLE_CONSISTENT_REPLICA_READ = "ENABLE_CONSISTENT_REPLICA_READ";
    public static final String SEND_TSO_FOR_NON_CONSISTENT_REPLICA_READ = "SEND_TSO_FOR_NON_CONSISTENT_REPLICA_READ";
    public static final String EXPLAIN_LOGICALVIEW = "EXPLAIN_LOGICALVIEW";
    public static final String EXPLAIN_CTE_CONSUMER = "EXPLAIN_CTE_CONSUMER";
    public static final String ENABLE_CREATE_VIEW = "ENABLE_CREATE_VIEW";
    public static final String ENABLE_USE_VIEW = "ENABLE_USE_VIEW";
    public static final String RETURN_REAL_ACTIVE_CONNNUM = "RETURN_REAL_ACTIVE_CONNNUM";
    public static final String ENABLE_HTAP = "ENABLE_HTAP";
    public static final String IN_SUB_QUERY_THRESHOLD = "IN_SUB_QUERY_THRESHOLD";
    public static final String ENABLE_OR_OPT = "ENABLE_OR_OPT";

    public static final String ENABLE_XPLAN_FEEDBACK = "ENABLE_XPLAN_FEEDBACK";

    public static final String ENABLE_IN_SUB_QUERY_FOR_DML = "ENABLE_IN_SUB_QUERY_FOR_DML";
    public static final String ENABLE_RUNTIME_FILTER = "ENABLE_RUNTIME_FILTER";
    public static final String ENABLE_LOCAL_RUNTIME_FILTER = "ENABLE_LOCAL_RUNTIME_FILTER";
    public static final String CHECK_RUNTIME_FILTER_SAME_FRAGMENT = "CHECK_RUNTIME_FILTER_SAME_FRAGMENT";
    public static final String FORCE_ENABLE_RUNTIME_FILTER_COLUMNS = "FORCE_ENABLE_RUNTIME_FILTER_COLUMNS";
    public static final String FORCE_DISABLE_RUNTIME_FILTER_COLUMNS = "FORCE_DISABLE_RUNTIME_FILTER_COLUMNS";
    public static final String BLOOM_FILTER_BROADCAST_NUM = "BLOOM_FILTER_BROADCAST_NUM";
    public static final String BLOOM_FILTER_MAX_SIZE = "BLOOM_FILTER_MAX_SIZE";
    public static final String BLOOM_FILTER_RATIO = "BLOOM_FILTER_RATIO";
    public static final String RUNTIME_FILTER_PROBE_MIN_ROW_COUNT = "RUNTIME_FILTER_PROBE_MIN_ROW_COUNT";
    public static final String BLOOM_FILTER_GUESS_SIZE = "BLOOM_FILTER_GUESS_SIZE";
    public static final String BLOOM_FILTER_MIN_SIZE = "BLOOM_FILTER_MIN_SIZE";
    public static final String ENABLE_PUSH_RUNTIME_FILTER_SCAN = "ENABLE_PUSH_RUNTIME_FILTER_SCAN";
    public static final String WAIT_RUNTIME_FILTER_FOR_SCAN = "WAIT_RUNTIME_FILTER_FOR_SCAN";
    public static final String ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE = "ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE";
    public static final String ENABLE_RUNTIME_FILTER_XXHASH = "ENABLE_RUNTIME_FILTER_XXHASH";
    public static final String ENABLE_SPLIT_RUNTIME_FILTER = "ENABLE_SPLIT_RUNTIME_FILTER";
    public static final String ENABLE_OPTIMIZE_SCAN_WITH_RUNTIME_FILTER = "ENABLE_OPTIMIZE_SCAN_WITH_RUNTIME_FILTER";
    public static final String RUNTIME_FILTER_FPP = "RUNTIME_FILTER_FPP";
    public static final String STORAGE_SUPPORTS_BLOOM_FILTER = "STORAGE_SUPPORTS_BLOOM_FILTER";
    public static final String WAIT_BLOOM_FILTER_TIMEOUT_MS = "WAIT_BLOOM_FILTER_TIMEOUT_MS";
    public static final String RESUME_SCAN_STEP_SIZE = "RESUME_SCAN_STEP_SIZE";
    public static final String ENABLE_SPILL_OUTPUT = "ENABLE_SPILL_OUTPUT";
    public static final String SPILL_OUTPUT_MAX_BUFFER_SIZE = "SPILL_OUTPUT_MAX_BUFFER_SIZE";
    public static final String SUPPORT_READ_FOLLOWER_STRATEGY = "SUPPORT_READ_FOLLOWER_STRATEGY";
    public static final String ENABLE_BROADCAST_RANDOM_READ = "ENABLE_BROADCAST_RANDOM_READ";
    public static final String ENABLE_REPLICAS_RANDOM_READ = "ENABLE_REPLICAS_RANDOM_READ";
    public static final String BROADCAST_RANDOM_READ_IN_LOGICALVIEW =
        "BROADCAST_RANDOM_READ_IN_LOGICALVIEW";

    public static final String ENABLE_LOCAL_PARTITION_WISE_JOIN = "ENABLE_LOCAL_PARTITION_WISE_JOIN";

    public static final String LOCAL_PAIRWISE_PROBE_SEPARATE = "LOCAL_PAIRWISE_PROBE_SEPARATE";

    public static final String JOIN_KEEP_PARTITION = "JOIN_KEEP_PARTITION";

    /**
     * debug mode on alter tablegroup, which makes alter tablegroup status change slower etc.
     */
    public static final String TABLEGROUP_DEBUG = "TABLEGROUP_DEBUG";
    public static final String DDL_ON_PRIMARY_GSI_TYPE = "DDL_ON_PRIMARY_GSI_TYPE";
    // cdc进行ddl打标专用,提示系统打标前，是否进行Sleep以及Sleep的时间，单位：s
    public static final String SLEEP_TIME_BEFORE_NOTIFY_DDL = "SLEEP_TIME_BEFORE_NOTIFY_DDL";
    public static final String SHOW_IMPLICIT_ID = "SHOW_IMPLICIT_ID";
    public static final String SHOW_IMPLICIT_TABLE_GROUP = "SHOW_IMPLICIT_TABLE_GROUP";
    public static final String ENABLE_DRIVING_STREAM_SCAN = "ENABLE_DRIVING_STREAM_SCAN";
    public static final String ENABLE_SIMPLIFY_TRACE_SQL = "ENABLE_SIMPLIFY_TRACE_SQL";
    public static final String CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST = "CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST";
    public static final String PARAMETRIC_SIMILARITY_ALGO = "PARAMETRIC_SIMILARITY_ALGO";
    public static final String FEEDBACK_WORKLOAD_AP_THRESHOLD = "FEEDBACK_WORKLOAD_AP_THRESHOLD";
    //HTAP FEEDBACK
    public static final String FEEDBACK_WORKLOAD_TP_THRESHOLD = "FEEDBACK_WORKLOAD_TP_THRESHOLD";
    public static final String MASTER_READ_WEIGHT = "MASTER_READ_WEIGHT";
    public static final String FOLLOWER_READ_WEIGHT = "FOLLOWER_READ_WEIGHT";

    //HTAP ROUTE
    public static final String STORAGE_DELAY_THRESHOLD = "STORAGE_DELAY_THRESHOLD";
    public static final String STORAGE_BUSY_THRESHOLD = "STORAGE_BUSY_THRESHOLD";
    /**
     * set the operation strategy when the slave delay
     * <0 means nothing, =1 change master, =2 throw exception
     */
    public static final String DELAY_EXECUTION_STRATEGY = "DELAY_EXECUTION_STRATEGY";
    public static final String KEEP_DELAY_EXECUTION_STRATEGY = "KEEP_DELAY_EXECUTION_STRATEGY";
    public static final String USE_CDC_CON = "USE_CDC_CON";

    public static final String NEW_TOPN = "NEW_TOPN";

    /**
     * top record size
     */
    public static final String TOPN_SIZE = "TOPN_SIZE";

    public static final String NEW_TOPN_SIZE = "NEW_TOPN_SIZE";

    /**
     * topn min num, only record the topn info if its count > TOPN_MIN_NUM
     */
    public static final String TOPN_MIN_NUM = "TOPN_MIN_NUM";

    public static final String NEW_TOPN_MIN_NUM = "NEW_TOPN_MIN_NUM";

    /**
     * Whether return the result of SELECT INTO OUTFILE STATISTICS
     */
    public static final String SELECT_INTO_OUTFILE_STATISTICS_DUMP = "SELECT_INTO_OUTFILE_STATISTICS_DUMP";
    /**
     * Whether ignore histogram of string column
     */
    public static final String STATISTICS_DUMP_IGNORE_STRING = "STATISTICS_DUMP_IGNORE_STRING";

    public static final String STATISTICS_COLLECT_HISTOGRAM_STRING = "STATISTICS_COLLECT_HISTOGRAM_STRING";
    /**
     * Use range-format to show hash/key partitioned table
     */
    public static final String SHOW_HASH_PARTITIONS_BY_RANGE = "SHOW_HASH_PARTITIONS_BY_RANGE";
    /**
     * Show table group name in SHOW CREATE TABLE stmt
     */
    public static final String SHOW_TABLE_GROUP_NAME = "SHOW_TABLE_GROUP_NAME";
    /**
     * The max physical partitions (including both partitions and subpartitions) of one logical table
     */
    public static final String MAX_PHYSICAL_PARTITION_COUNT = "MAX_PHYSICAL_PARTITION_COUNT";
    /**
     * The max count for the partition columns or subpartition columns
     */
    public static final String MAX_PARTITION_COLUMN_COUNT = "MAX_PARTITION_COLUMN_COUNT";

    /**
     * The max length of  partition name(included the name of subpartition template)
     */
    public static final String MAX_PARTITION_NAME_LENGTH = "MAX_PARTITION_NAME_LENGTH";

    /**
     * Label if auto use range-key subpart for index of auto-part table, default is true
     */
    public static final String ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX = "ENABLE_AUTO_USE_RANGE_FOR_TIME_INDEX";
    /**
     * Label if auto use key syntax for all local index on show create table
     */
    public static final String ENABLE_USE_KEY_FOR_ALL_LOCAL_INDEX = "ENABLE_USE_KEY_FOR_ALL_LOCAL_INDEX";

    /**
     * Label if auto create local index (auto_shard_key_xxx) for partition keys on CREATE TABLE, default true
     */
    public static final String ENABLE_AUTO_SHARD_KEY_INDEX = "ENABLE_AUTO_SHARD_KEY_INDEX";

    /**
     * Label if auto use range/list columns partitions for "part by range/list", default is true
     */
    public static final String ENABLE_AUTO_USE_COLUMNS_PARTITION = "ENABLE_AUTO_USE_COLUMNS_PARTITION";

    /**
     * Label if show storage partitions in show create table
     */
    public static final String ENABLE_SHOW_STORAGE_PARTITIONS = "ENABLE_SHOW_STORAGE_PARTITIONS";

    /**
     * Balancer parameters
     */
    public static final String ENABLE_BALANCER = "ENABLE_BALANCER";
    public static final String BALANCER_MAX_PARTITION_SIZE = "BALANCER_MAX_PARTITION_SIZE";
    public static final String BALANCER_WINDOW = "BALANCER_WINDOW";

    public static final String SHOW_DDL_ENGINE_RESOURCES = "SHOW_DDL_ENGINE_RESOURCES";

    /**
     * Allow move the single table with locality='balance_single_table=on' during scale-out/scale-in
     */
    public static final String ALLOW_MOVING_BALANCED_SINGLE_TABLE = "ALLOW_MOVING_BALANCED_SINGLE_TABLE";
    /**
     * The default value of default_single when create auto-db without specify default_single option
     */
    public static final String DATABASE_DEFAULT_SINGLE = "DATABASE_DEFAULT_SINGLE";
    /**
     * switch for partition pruning, only use for qatest and debug
     */
    public static final String ENABLE_PARTITION_PRUNING = "ENABLE_PARTITION_PRUNING";
    /**
     * Allow use auto merge intervals during dynamic pruning
     */
    public static final String ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING = "ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING";
    /**
     * Allow enumerate the intervals and convert to range Query to In Query( 1<pk<5 => pk in (2,3,4) )
     */
    public static final String ENABLE_INTERVAL_ENUMERATION_IN_PRUNING = "ENABLE_INTERVAL_ENUMERATION_IN_PRUNING";
    /**
     * the prune step count limit that support doing dynamic pruning
     */
    public static final String PARTITION_PRUNING_STEP_COUNT_LIMIT = "PARTITION_PRUNING_STEP_COUNT_LIMIT";
    /**
     * <pre>
     * Label if use fast single-point interval merging during interval merging.
     * if it is true(default value),
     *  then the intervalMerger of AND will directly use first single-point interval
     * (pk1=c1) as the result of interval merging:
     *  And
     *      pk1=c1
     *      pk1>=c2
     *      pk1<=c3
     *      pk1=c4
     * (the merge result of the above andExpr cannot be Always-False expr)
     *
     * if it is false,
     * then the intervalMerger of AND will change all single-point intervals
     * into range intervals and compute the final result of interval merging:
     *  And
     *      pk1=c1
     *      pk1>=c2
     *      pk1<=c3
     *      pk1=c4
     * =>
     *  And
     *      pk1>=c1
     *      pk1<=c1
     *      pk1>=c2
     *      pk1<=c3
     *      pk1>=c4
     *      pk1<=c4
     *  (the merge result of the above andExpr can be Always-False/Always-True expr  )
     * </pre>
     */
    public static final String USE_FAST_SINGLE_POINT_INTERVAL_MERGING = "USE_FAST_SINGLE_POINT_INTERVAL_MERGING";

    /**
     * Label if allow fast routing for point-select of prefix part cols
     * <pre>
     *    if ENABLE_FAST_PREFIX_PART_COL_SINGLE_POINT_ROUTING = true,
     *    that mean
     *    for part cols: (a,b,c) and b and c is not using:
     *    a=? == > tuple(?,any,any) and do tuple routing
     * </pre>
     */
    public static final String ENABLE_FAST_PREFIX_PART_COL_EQ_COND_ROUTING =
        "ENABLE_FAST_PREFIX_PART_COL_EQ_COND_ROUTING";

    /**
     * Allow to cache the result of const expressions during pruning
     */
    public static final String ENABLE_CONST_EXPR_EVAL_CACHE = "ENABLE_CONST_EXPR_EVAL_CACHE";

    /**
     * The max length of the enumerable interval in pruning
     */
    public static final String MAX_ENUMERABLE_INTERVAL_LENGTH = "MAX_ENUMERABLE_INTERVAL_LENGTH";
    /**
     * The max size of in value from the InSubQuery pruning
     */
    public static final String MAX_IN_SUBQUERY_PRUNING_SIZE = "MAX_IN_SUBQUERY_PRUNING_SIZE";

    public static final String ENABLE_PARSE_ORIGINAL_TABLE = "ENABLE_PARSE_ORIGINAL_TABLE";
    /**
     * Enable do pruning log in pruner.log
     */
    public static final String ENABLE_LOG_PART_PRUNING = "ENABLE_LOG_PART_PRUNING";
    public static final String ENABLE_OPTIMIZER_ALERT = "ENABLE_OPTIMIZER_ALERT";
    public static final String ENABLE_OPTIMIZER_ALERT_LOG = "ENABLE_OPTIMIZER_ALERT_LOG";
    public static final String ENABLE_OPTIMIZER_ALERT_BKA = "ENABLE_OPTIMIZER_ALERT_BKA";
    public static final String OPTIMIZER_ALERT_LOG_INTERVAL = "OPTIMIZER_ALERT_LOG_INTERVAL";

    public static final String ALERT_BKA_BASE = "ALERT_BKA_BASE";
    public static final String ALERT_TP_BASE = "ALERT_TP_BASE";

    public static final String ENABLE_TP_SLOW_ALERT = "ENABLE_TP_SLOW_ALERT";

    public static final String ENABLE_TP_SLOW_ALERT_THRESHOLD = "ENABLE_TP_SLOW_ALERT_THRESHOLD";

    public static final String ENABLE_ALERT_TEST_DEFAULT = "ENABLE_ALERT_TEST_DEFAULT";

    public static final String ENABLE_ALERT_TEST = "ENABLE_ALERT_TEST";

    public static final String ALERT_STATISTIC_INTERRUPT = "ALERT_STATISTIC_INTERRUPT";

    public static final String ALERT_STATISTIC_INCONSISTENT = "ALERT_STATISTIC_INCONSISTENT";

    public static final String MOCK_ROUTING_USER = "MOCK_ROUTING_USER";

    public static final String ENABLE_MANUAL_ROUTING = "ENABLE_MANUAL_ROUTING";

    public static final String FOLLOWER_ROUTING_EXPIRE_INTERVAL = "FOLLOWER_ROUTING_EXPIRE_INTERVAL";

    public static final String ENABLE_BRANCH_AND_BOUND_OPTIMIZATION = "ENABLE_BRANCH_AND_BOUND_OPTIMIZATION";
    public static final String ENABLE_BROADCAST_JOIN = "ENABLE_BROADCAST_JOIN";

    public static final String ENABLE_PARTITION_WISE_GROUP_OPT = "ENABLE_PARTITION_WISE_GROUP_OPT";
    public static final String PARTITION_WISE_GROUP_THRESHOLD = "PARTITION_WISE_GROUP_THRESHOLD";
    public static final String ENABLE_PARTITION_WISE_JOIN = "ENABLE_PARTITION_WISE_JOIN";
    public static final String ENABLE_BROADCAST_LEFT = "ENABLE_BROADCAST_LEFT";

    public static final String ENABLE_PARTITION_WISE_AGG = "ENABLE_PARTITION_WISE_AGG";

    public static final String ENABLE_PARTITION_WISE_WINDOW = "ENABLE_PARTITION_WISE_WINDOW";

    public static final String ENABLE_PARTITION_WISE = "ENABLE_PARTITION_WISE";

    public static final String PARTITION_WISE_THRESHOLD = "PARTITION_WISE_THRESHOLD";

    public static final String COL_IN_SEMIJOIN_THRESHOLD = "COL_IN_SEMIJOIN_THRESHOLD";

    public static final String ENABLE_COL_IN_SEMIJOIN_STRING = "ENABLE_COL_IN_SEMIJOIN_STRING";

    public static final String ENABLE_COL_MULTI_IN_SEMIJOIN = "ENABLE_COL_MULTI_IN_SEMIJOIN";

    public static final String BROADCAST_SHUFFLE_PARALLELISM = "BROADCAST_SHUFFLE_PARALLELISM";
    public static final String ENABLE_PASS_THROUGH_TRAIT = "ENABLE_PASS_THROUGH_TRAIT";
    public static final String ENABLE_DERIVE_TRAIT = "ENABLE_DERIVE_TRAIT";
    public static final String ENABLE_SHUFFLE_BY_PARTIAL_KEY = "ENABLE_SHUFFLE_BY_PARTIAL_KEY";
    public static final String ADVISE_TYPE = "ADVISE_TYPE";
    /**
     * statistic use hyperloglog
     */
    public static final String ENABLE_HLL = "ENABLE_HLL";

    public static final String ENABLE_COLLECT_HLL = "ENABLE_COLLECT_HLL";
    public static final String HLL_PARALLELISM = "HLL_PARALLELISM";

    public static final String STRICT_ENUM_CONVERT = "STRICT_ENUM_CONVERT";
    public static final String STRICT_YEAR_CONVERT = "STRICT_YEAR_CONVERT";
    /**
     * feedback minor tolerance value
     */
    public static final String MINOR_TOLERANCE_VALUE = "MINOR_TOLERANCE_VALUE";
    /**
     * upper bound for baseline sync
     */
    public static final String MAX_BASELINE_SYNC_PLAN_SIZE = "MAX_BASELINE_SYNC_PLAN_SIZE";
    public static final String SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL = "SPM_OLD_PLAN_CHOOSE_COUNT_LEVEL";
    /**
     * bytes upper bound for baseline sync
     */
    public static final String MAX_BASELINE_SYNC_BYTE_SIZE = "MAX_BASELINE_SYNC_BYTE_SIZE";

    public static final String ENABLE_BASELINE_CLEAN_JOB = "ENABLE_BASELINE_CLEAN_JOB";

    /**
     * the period of storage ha task of each dn, unit:ms
     */
    public static final String STORAGE_HA_TASK_PERIOD = "STORAGE_HA_TASK_PERIOD";
    /**
     * the socket timeout of mysql jdbc url of storage ha task, unit:ms
     */
    public static final String STORAGE_HA_SOCKET_TIMEOUT = "STORAGE_HA_SOCKET_TIMEOUT";
    /**
     * the connect timeout of mysql jdbc url of storage ha task, unit:ms
     */
    public static final String STORAGE_HA_CONNECT_TIMEOUT = "STORAGE_HA_CONNECT_TIMEOUT";
    /**
     * the switch of print storage ha task log
     */
    public static final String ENABLE_HA_CHECK_TASK_LOG = "ENABLE_HA_CHECK_TASK_LOG";

    public static final String ANALYZE_TEST_UPDATE = "ANALYZE_TEST_UPDATE";

    public static final String ENABLE_MPP_NDV_USE_COLUMNAR = "ENABLE_MPP_NDV_USE_COLUMNAR";

    public static final String MPP_NDV_USE_COLUMNAR_LIMIT = "MPP_NDV_USE_COLUMNAR_LIMIT";
    /**
     * ndv sketch expire time
     */
    public static final String STATISTIC_NDV_SKETCH_EXPIRE_TIME = "STATISTIC_NDV_SKETCH_EXPIRE_TIME";
    public static final String STATISTIC_NDV_SKETCH_QUERY_TIMEOUT = "STATISTIC_NDV_SKETCH_QUERY_TIMEOUT";

    public static final String STATISTIC_NDV_SKETCH_QUERY_TIMEOUT_ON_CCI = "STATISTIC_NDV_SKETCH_QUERY_TIMEOUT_ON_CCI";
    public static final String STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE = "STATISTIC_NDV_SKETCH_MAX_DIFFERENT_VALUE";
    public static final String STATISTIC_NDV_SKETCH_MAX_DIFFERENT_RATIO = "STATISTIC_NDV_SKETCH_MAX_DIFFERENT_RATIO";
    public static final String STATISTIC_NDV_SKETCH_SAMPLE_RATE = "STATISTIC_NDV_SKETCH_SAMPLE_RATE";
    public static final String ENABLE_CHECK_STATISTICS_EXPIRE = "ENABLE_CHECK_STATISTICS_EXPIRE";
    public static final String INDEX_ADVISOR_CARDINALITY_BASE = "INDEX_ADVISOR_CARDINALITY_BASE";
    /**
     * the time between each ndv collect
     */
    public static final String AUTO_COLLECT_NDV_SKETCH = "AUTO_COLLECT_NDV_SKETCH";
    /*
     * CDC模块的启动方式，0不启动，1同步启动，2异步启动。默认同步启动
     */
    public static final String CDC_STARTUP_MODE = "CDC_STARTUP_MODE";
    /**
     * CDC模块是否开启metadata snapshot 能力
     */
    public static final String ENABLE_CDC_META_BUILD_SNAPSHOT = "ENABLE_CDC_META_BUILD_SNAPSHOT";
    public static final String SHARE_STORAGE_MODE = "SHARE_STORAGE_MODE";
    public static final String SHOW_ALL_PARAMS = "SHOW_ALL_PARAMS";
    public static final String ENABLE_SET_GLOBAL = "ENABLE_SET_GLOBAL";
    public static final String ENABLE_SAME_DB_SWITCH_NOOP = "ENABLE_SAME_DB_SWITCH_NOOP";
    public static final String COMPATIBLE_CHARSET_VARIABLES = "COMPATIBLE_CHARSET_VARIABLES";
    public static final String ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY =
        "ENABLE_JSON_RESULT_CHARSET_COMPATIBILITY";
    public static final String ENABLE_PREEMPTIVE_MDL = "ENABLE_PREEMPTIVE_MDL";
    public static final String SHOW_STORAGE_POOL = "SHOW_STORAGE_POOL";
    public static final String SHOW_FULL_LOCALITY = "SHOW_FULL_LOCALITY";
    public static final String PREEMPTIVE_MDL_INITWAIT = "PREEMPTIVE_MDL_INITWAIT";
    public static final String PREEMPTIVE_MDL_INTERVAL = "PREEMPTIVE_MDL_INTERVAL";
    public static final String RENAME_PREEMPTIVE_MDL_INITWAIT = "RENAME_PREEMPTIVE_MDL_INITWAIT";
    public static final String RENAME_PREEMPTIVE_MDL_INTERVAL = "RENAME_PREEMPTIVE_MDL_INTERVAL";
    public static final String TG_PREEMPTIVE_MDL_INITWAIT = "TG_PREEMPTIVE_MDL_INITWAIT";
    public static final String TG_PREEMPTIVE_MDL_INTERVAL = "TG_PREEMPTIVE_MDL_INTERVAL";
    public static final String FORCE_READ_OUTSIDE_TX = "FORCE_READ_OUTSIDE_TX";
    public static final String SCHEDULER_SCAN_INTERVAL_SECONDS = "SCHEDULER_SCAN_INTERVAL_SECONDS";
    public static final String SCHEDULER_CLEAN_UP_INTERVAL_HOURS = "SCHEDULER_CLEAN_UP_INTERVAL_HOURS";
    public static final String SCHEDULER_RECORD_KEEP_HOURS = "SCHEDULER_RECORD_KEEP_HOURS";
    public static final String SCHEDULER_MIN_WORKER_COUNT = "SCHEDULER_MIN_WORKER_COUNT";
    public static final String SCHEDULER_MAX_WORKER_COUNT = "SCHEDULER_MAX_WORKER_COUNT";
    public static final String DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR =
        "DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR";

    public static final String DEFAULT_TTL_SCHEDULE_CRON_EXPR =
        "DEFAULT_TTL_SCHEDULE_CRON_EXPR";

    /**
     * check target table after alter tablegroup's backfill
     */
    public static final String TABLEGROUP_REORG_CHECK_AFTER_BACKFILL = "TABLEGROUP_REORG_CHECK_AFTER_BACKFILL";
    /**
     * TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER
     */
    public static final String TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER = "TABLEGROUP_REORG_BACKFILL_USE_FASTCHECKER";
    public static final String TABLEGROUP_REORG_CHECK_BATCH_SIZE = "TABLEGROUP_REORG_CHECK_BATCH_SIZE";
    public static final String TABLEGROUP_REORG_CHECK_SPEED_LIMITATION = "TABLEGROUP_REORG_CHECK_SPEED_LIMITATION";
    public static final String TABLEGROUP_REORG_CHECK_SPEED_MIN = "TABLEGROUP_REORG_CHECK_SPEED_MIN";
    public static final String TABLEGROUP_REORG_CHECK_PARALLELISM = "TABLEGROUP_REORG_CHECK_PARALLELISM";

    /**
     * number of error for check early fail.
     */
    public static final String TABLEGROUP_REORG_EARLY_FAIL_NUMBER = "TABLEGROUP_REORG_EARLY_FAIL_NUMBER";
    /**
     * set the table's final status for alter tablegroup debug purpose.
     */
    public static final String TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG = "TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG";
    public static final String INTERRUPT_DDL_WHILE_LOSING_LEADER = "INTERRUPT_DDL_WHILE_LOSING_LEADER";
    public static final String RECORD_SQL_COST = "RECORD_SQL_COST";
    public static final String ENABLE_LOGICALVIEW_COST = "ENABLE_LOGICALVIEW_COST";
    public static final String FORCE_RECREATE_GROUP_DATASOURCE = "FORCE_RECREATE_GROUP_DATASOURCE";
    public static final String ENABLE_PLAN_TYPE_DIGEST = "ENABLE_PLAN_TYPE_DIGEST";
    public static final String ENABLE_PLAN_TYPE_DIGEST_STRICT_MODE = "ENABLE_PLAN_TYPE_DIGEST_STRICT_MODE";
    /**
     * flag that if auto warming logical db
     */
    public static final String ENABLE_LOGICAL_DB_WARMMING_UP = "ENABLE_LOGICAL_DB_WARMMING_UP";
    /**
     * pool size of auto-warming-logical-db-executor
     */
    public static final String LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE = "LOGICAL_DB_WARMMING_UP_EXECUTOR_POOL_SIZE";
    public static final String FLASHBACK_RENAME = "FLASHBACK_RENAME";
    public static final String PURGE_FILE_STORAGE_TABLE = "PURGE_FILE_STORAGE_TABLE";
    public static final String OSS_BACKFILL_PARALLELISM = "OSS_BACKFILL_PARALLELISM";
    /* ================ For OSS Table ORC File ================ */
    public static final String OSS_ORC_INDEX_STRIDE = "OSS_ORC_INDEX_STRIDE";
    public static final String OSS_BLOOM_FILTER_FPP = "OSS_BLOOM_FILTER_FPP";
    public static final String OSS_MAX_ROWS_PER_FILE = "OSS_MAX_ROWS_PER_FILE";
    public static final String OSS_REMOVE_TMP_FILES = "OSS_REMOVE_TMP_FILES";
    public static final String OSS_ORC_COMPRESSION = "OSS_ORC_COMPRESSION";
    public static final String OSS_FS_MAX_READ_RATE = "OSS_FS_MAX_READ_RATE";

    /* ================ For OSS Table File System ================ */
    public static final String OSS_FS_MAX_WRITE_RATE = "OSS_FS_MAX_WRITE_RATE";
    public static final String OSS_FS_VALIDATION_ENABLE = "OSS_FS_VALIDATION_ENABLE";
    public static final String OSS_FS_CACHE_TTL = "OSS_FS_CACHE_TTL";
    public static final String OSS_FS_MAX_CACHED_ENTRIES = "OSS_FS_MAX_CACHED_ENTRIES";

    public static final String OSS_FS_ENABLE_CACHED = "OSS_FS_ENABLE_CACHED";

    public static final String OSS_FS_CACHED_FLUSH_THREAD_NUM = "OSS_FS_CACHED_FLUSH_THREAD_NUM";

    public static final String OSS_FS_MAX_CACHED_GB = "OSS_FS_MAX_CACHED_GB";

    public static final String OSS_FS_USE_BYTES_CACHE = "OSS_FS_USE_BYTES_CACHE";

    public static final String OSS_FS_MEMORY_RATIO_OF_BYTES_CACHE = "OSS_FS_MEMORY_RATIO_OF_BYTES_CACHE";

    /**
     * Global dynamic switch for the GeneralCache system.
     * When false, all OSS reads fall back to OSSInputStream (bypass cache).
     */
    public static final String ENABLE_OSS_GENERAL_CACHE = "ENABLE_OSS_GENERAL_CACHE";

    /**
     * Global switch for blob (externalized column) read cache.
     */
    public static final String ENABLE_BLOB_CACHE = "ENABLE_BLOB_CACHE";

    /**
     * When true, a high-watermark FETCH_BLOB read (seqId &gt; flushedWatermark) probes the
     * local/RPC blob cache and the DN staging table in parallel and uses whichever returns first.
     * When false (default), it falls back to the legacy sequential path (staging first, then OSS).
     */
    public static final String EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED = "EXT_BLOB_HIGH_WATERMARK_RACE_ENABLED";

    /**
     * Emergency escape hatch. FETCH_BLOB fails close when a valid non-NULL BlobRef resolves to no
     * content anywhere (transaction-local, staging, cache, Page/OSS). Enabling this restores the
     * legacy warn-and-return-NULL behavior so surviving rows can be salvaged after a confirmed
     * permanent object loss; read-modify-write DML may then persist NULL for the lost values.
     */
    public static final String EXT_FETCH_BLOB_MISS_RETURN_NULL = "EXT_FETCH_BLOB_MISS_RETURN_NULL";

    /**
     * Emergency escape hatch. Externalized value encoding fails close on runtime types whose
     * textual form is ambiguous (Boolean, Float/Double, temporal, unknown objects). Enabling this
     * restores the legacy silent toString() encoding while a missing legitimate type is being
     * added to the explicit whitelist.
     */
    public static final String EXT_BLOB_UNKNOWN_TYPE_TO_STRING = "EXT_BLOB_UNKNOWN_TYPE_TO_STRING";

    /**
     * Emergency escape hatch. TRUNCATE on a table with externalized columns is refused by default;
     * enabling this force-truncates the physical shards and leaves every staged/published blob of
     * the table as an orphan until purge reclaims it. The recycle-bin truncate path stays
     * hard-rejected regardless (it renames the table and breaks address resolution by name).
     */
    public static final String ALLOW_TRUNCATE_EXTERNALIZED_TABLE = "ALLOW_TRUNCATE_EXTERNALIZED_TABLE";

    /**
     * Emergency salvage-read escape hatch. Blob Page read verification (header/metadata CRC,
     * chunk CRC32C, value rawMd5) fails close by default; enabling this instance-level mode logs
     * each mismatch and keeps reading so intact payload bytes can be rescued from partially
     * corrupted Pages. Structural and identity checks (magic, version, bounds, slot rawLength)
     * always stay enforced.
     */
    public static final String EXT_BLOB_PAGE_SALVAGE_READ = "EXT_BLOB_PAGE_SALVAGE_READ";

    /**
     * Maximum number of high-watermark speculative cache probes admitted across one CN. The
     * effective limit is also capped by GeneralCache query concurrency and one quarter of a
     * ServerExecutor bucket. Default 64.
     */
    public static final String EXT_BLOB_HIGH_WATERMARK_RACE_CONCURRENCY =
        "EXT_BLOB_HIGH_WATERMARK_RACE_CONCURRENCY";

    /**
     * @deprecated Use {@link #EXT_BLOB_IO_TIMEOUT_MS}. Retained only so old inst_config rows can
     * be parsed during rolling upgrade.
     */
    @Deprecated
    public static final String EXT_BLOB_READ_TIMEOUT_MS = "EXT_BLOB_READ_TIMEOUT_MS";

    /**
     * Canonical timeout in milliseconds for one external-column data I/O operation. Cache, DN
     * staging and OSS paths share the same request deadline. Long-running MCE DDL and staging
     * flush lifecycles are not bounded by this value; each blocking I/O inside them is. Default
     * 60000 (60 seconds).
     */
    public static final String EXT_BLOB_IO_TIMEOUT_MS = "EXT_BLOB_IO_TIMEOUT_MS";

    /**
     * Whether to append per-SQL external column statistics (extc=...) to sql.log.
     * Default true.
     */
    public static final String ENABLE_EXT_COLUMN_STATISTICS_LOG = "ENABLE_EXT_COLUMN_STATISTICS_LOG";

    /**
     * Require externalized-column writes to remain recoverable from physical binlog branches.
     * When true, every new BlobRef is backed by transactional staging and a primary DELETE + INSERT
     * caused by a routing-key change rematerializes unchanged externalized values.
     * Default true.
     * <p>
     * When false, writes fall back to the size-threshold policy (values under
     * EXT_STAGING_THRESHOLD_BYTES still go through transactional staging, larger ones upload to
     * OSS directly) — it does NOT restore any pre-v5 non-transactional write channel. Turning this
     * off on a GDN instance breaks binlog consumability of externalized values and must never be
     * done there.
     */
    public static final String ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY =
        "ENABLE_EXTERNALIZED_BINLOG_COMPATIBILITY";

    /**
     * Master switch: enable the staging-buffer write path for externalized columns.
     * When binlog compatibility is disabled, this switch enables the legacy size/backpressure
     * staging policy. Binlog compatibility takes precedence and forces transactional staging.
     * Default true.
     */
    public static final String EXT_STAGING_BUFFER_ENABLED = "EXT_STAGING_BUFFER_ENABLED";
    /**
     * Values smaller than this threshold use staging when binlog compatibility is disabled.
     * Values greater than or equal to it use direct OSS. Default 102400 (100 KiB).
     */
    public static final String EXT_STAGING_THRESHOLD_BYTES = "EXT_STAGING_THRESHOLD_BYTES";
    /**
     * Validate that a generated staging physical plan resolves to the same group connection id as its primary
     * business owner plan. Intended for tests and diagnostics; disabled by default to avoid duplicate partition
     * metadata lookups on the production DML path.
     */
    public static final String EXT_STAGING_VALIDATE_GROUP_CONN_ID = "EXT_STAGING_VALIDATE_GROUP_CONN_ID";
    /**
     * Max rows accumulated in the current ACTIVE staging table before triggering a rotate.
     * After rotate, a new staging table becomes ACTIVE and the old one stays DRAINING until its transactions finish.
     * Default 10000.
     */
    public static final String EXT_STAGING_ROTATE_MAX_ROWS = "EXT_STAGING_ROTATE_MAX_ROWS";
    /**
     * Flush scheduler polling interval (ms). Controls how often the flush task checks
     * for SEALED staging tables ready to be flushed to OSS.
     * Default 2000 (2 s).
     */
    public static final String EXT_STAGING_FLUSH_INTERVAL_MS = "EXT_STAGING_FLUSH_INTERVAL_MS";
    /**
     * Flush lease claim timeout (ms). If a flush task holds a lease longer than this,
     * other nodes may steal it to avoid stuck flushes.
     * Default 300000 (5 min).
     */
    public static final String EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS = "EXT_STAGING_FLUSH_CLAIM_TIMEOUT_MS";
    /**
     * Number of concurrent OSS upload threads used during a single flush operation.
     * Default 256.
     */
    public static final String EXT_STAGING_FLUSH_UPLOAD_CONCURRENCY = "EXT_STAGING_FLUSH_UPLOAD_CONCURRENCY";
    /**
     * Backpressure threshold ratio: ratio multiplied by the eligible master-CN count. When the
     * active staging-table count reaches the threshold, compatibility-disabled writes use direct
     * OSS. Default 50.
     */
    public static final String EXT_STAGING_BACKPRESSURE_RATIO = "EXT_STAGING_BACKPRESSURE_RATIO";
    /**
     * Manual rotation trigger. Setting this to true forces an immediate rotate
     * (without row threshold). The trigger is edge-based: set it back to false
     * before setting true again.
     */
    public static final String EXT_STAGING_FORCE_ROTATE = "EXT_STAGING_FORCE_ROTATE";
    /**
     * Drain wait timeout in ms. Drain wait task PAUSEs (waits for ops) once
     * exceeded so a stuck DN does not block rebalance forever.
     */
    public static final String EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS = "EXT_STAGING_DRAIN_WAIT_TIMEOUT_MS";
    /**
     * Drain wait poll interval in ms.
     */
    public static final String EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS = "EXT_STAGING_DRAIN_WAIT_POLL_INTERVAL_MS";
    /**
     * Force-takeover delay in ms. After this timeout, DrainWait seals stuck ACTIVE seqs.
     */
    public static final String EXT_STAGING_DRAIN_FORCE_TAKEOVER_MS = "EXT_STAGING_DRAIN_FORCE_TAKEOVER_MS";

    /**
     * Post-execution sleep for ExtStagingDrainStartTask (idempotency testing).
     */
    public static final String EXT_STAGING_DRAIN_START_SLEEP_MS = "EXT_STAGING_DRAIN_START_SLEEP_MS";
    /**
     * Post-execution sleep for ExtStagingDrainWaitTask (idempotency testing).
     */
    public static final String EXT_STAGING_DRAIN_WAIT_SLEEP_MS = "EXT_STAGING_DRAIN_WAIT_SLEEP_MS";

    /**
     * Slow-log threshold (ms) for a single blob write (staging INSERT).
     * Default 500.
     */
    public static final String BLOB_WRITE_SLOW_THRESHOLD_MS = "BLOB_WRITE_SLOW_THRESHOLD_MS";
    /**
     * Slow-log threshold (ms) for a single blob read (from cache or OSS).
     * Default 200.
     */
    public static final String BLOB_READ_SLOW_THRESHOLD_MS = "BLOB_READ_SLOW_THRESHOLD_MS";
    /**
     * Slow-log threshold (ms) for a complete flush operation (read + upload + cleanup).
     * Default 1000.
     */
    public static final String BLOB_FLUSH_SLOW_THRESHOLD_MS = "BLOB_FLUSH_SLOW_THRESHOLD_MS";
    /**
     * Warning threshold (ms) for individual blob flush sub-steps.
     * Default 5.
     */
    public static final String BLOB_FLUSH_WARN_THRESHOLD_MS = "BLOB_FLUSH_WARN_THRESHOLD_MS";

    /**
     * Slow-log threshold (us) for staging queue wait time (enqueue to dequeue).
     * Default 5000 (5 ms).
     */
    public static final String EXT_STAGING_SLOW_QUEUE_US = "EXT_STAGING_SLOW_QUEUE_US";
    /**
     * Slow-log threshold (us) for staging execution time (dequeue to DN ack).
     * Default 10000 (10 ms).
     */
    public static final String EXT_STAGING_SLOW_EXEC_US = "EXT_STAGING_SLOW_EXEC_US";
    /**
     * Slow-log threshold (ms) for a single blob upload to OSS during flush.
     * Default 200.
     */
    public static final String EXT_BLOB_UPLOAD_SLOW_MS = "EXT_BLOB_UPLOAD_SLOW_MS";

    /**
     * Externalized column storage version.
     * 0 = no compression, 1 = ZSTD compression, 2 = ZSTD compression with raw MD5.
     */
    public static final String EXT_COLUMN_VERSION = "EXT_COLUMN_VERSION";

    /**
     * MCE (MODIFY COLUMN EXTERNALIZE) physical ADD COLUMN algorithm control.
     * When adding the addr column, MCE first tries ALGORITHM=INSTANT, then falls back to
     * ALGORITHM=INPLACE, LOCK=NONE. If neither online algorithm is supported by the DN:
     * false (default) = throw an error to the user (never silently lock the table);
     * true = drop the ALGORITHM/LOCK clause and let the DN pick its default (may lock the table).
     */
    public static final String MCE_ADD_COLUMN_ALLOW_LOCK = "MCE_ADD_COLUMN_ALLOW_LOCK";

    /**
     * Pause an EXTERNALIZE MCE job after backfill/checker succeeds and immediately before the
     * READ_ADDR cutover. Statement hint and session values are captured with the DDL job; when
     * neither is specified, the gate reads the latest global value at execution time.
     */
    public static final String MCE_PAUSE_BEFORE_READ_CUTOVER = "MCE_PAUSE_BEFORE_READ_CUTOVER";

    /**
     * EXTERNALIZE backfill rows per physical streaming SELECT window. Page aggregation may span
     * multiple windows and is sealed independently by Page entry/raw-byte limits.
     */
    public static final String MCE_BACKFILL_BATCH_ROWS = "MCE_BACKFILL_BATCH_ROWS";

    /**
     * MCE externalize backfill rows per autocommit CAS UPDATE after one Page is durable.
     */
    public static final String MCE_BACKFILL_UPDATE_BATCH_ROWS = "MCE_BACKFILL_UPDATE_BATCH_ROWS";

    /**
     * MCE checker rows per physical verification batch.
     */
    public static final String MCE_CHECKER_BATCH_ROWS = "MCE_CHECKER_BATCH_ROWS";

    /**
     * Maximum number of physical-table checker pipelines running concurrently for one MCE task.
     */
    public static final String MCE_CHECKER_PARALLELISM = "MCE_CHECKER_PARALLELISM";

    /**
     * EXTERNALIZE backfill raw content bytes admitted to one Page aggregation batch.
     */
    public static final String MCE_BACKFILL_BATCH_BYTES = "MCE_BACKFILL_BATCH_BYTES";

    /**
     * INTERNALIZE backfill rows per physical SELECT/CAS batch. Kept independent from the much
     * larger EXTERNALIZE Page aggregation window to bound searched-CASE SQL and payload heap.
     */
    public static final String MCE_INTERNALIZE_BACKFILL_BATCH_ROWS = "MCE_INTERNALIZE_BACKFILL_BATCH_ROWS";

    /**
     * INTERNALIZE raw content bytes admitted to one physical SELECT/CAS batch.
     */
    public static final String MCE_INTERNALIZE_BACKFILL_BATCH_BYTES = "MCE_INTERNALIZE_BACKFILL_BATCH_BYTES";

    /**
     * Maximum number of MCE physical ADD/DROP COLUMN statements running concurrently.
     */
    public static final String MCE_PHYSICAL_DDL_PARALLELISM = "MCE_PHYSICAL_DDL_PARALLELISM";

    /**
     * Maximum number of physical-table backfill pipelines running concurrently for one MCE task.
     */
    public static final String MCE_BACKFILL_PARALLELISM = "MCE_BACKFILL_PARALLELISM";

    /**
     * Maximum aggregate raw logical-value bytes retained between streaming reads and remote
     * durability across all physical-table pipelines of one MCE backfill task.
     */
    public static final String MCE_BACKFILL_MAX_INFLIGHT_BYTES = "MCE_BACKFILL_MAX_INFLIGHT_BYTES";

    /**
     * Max rows per second shared by all physical-table pipelines of one MCE backfill task.
     */
    public static final String MCE_BACKFILL_SPEED_LIMITATION = "MCE_BACKFILL_SPEED_LIMITATION";

    /**
     * Min rows per second the adaptive MCE backfill throttle may decay to.
     */
    public static final String MCE_BACKFILL_SPEED_MIN = "MCE_BACKFILL_SPEED_MIN";

    /**
     * Cache RPC timeout in milliseconds.
     */
    public static final String CACHE_RPC_TIMEOUT_MS = "CACHE_RPC_TIMEOUT_MS";

    /**
     * Batch size (number of ids per batch) for cache_file_mapping orphan cleanup scan.
     */
    public static final String CACHE_FILE_MAPPING_CLEAN_BATCH_SIZE = "CACHE_FILE_MAPPING_CLEAN_BATCH_SIZE";

    /**
     * Sleep time in milliseconds between each cleanup batch to reduce MetaDB load.
     */
    public static final String CACHE_FILE_MAPPING_CLEAN_SLEEP_MS = "CACHE_FILE_MAPPING_CLEAN_SLEEP_MS";

    /**
     * Hard cap of bytes pinned by a single {@code cache.get(...)} call in
     * {@code CachedInputStream}. A larger request is split into chunks of at
     * most this many bytes so that the returned BP refer never pins more than
     * this amount of BP pages at once. Default is 1MB.
     */
    public static final String CACHE_MAX_PIN_BYTES_PER_GET = "CACHE_MAX_PIN_BYTES_PER_GET";

    /**
     * Dynamic override for the OSS read rate limit (bytes/sec) used by
     * PrefixRoutingRemoteStorageService. A value <= 0 means "keep the static
     * configuration (server.properties#ossRateLimit)". When set via SET GLOBAL,
     * the running RateLimiter is replaced atomically.
     */
    public static final String OSS_GENERAL_CACHE_RATE_LIMIT = "OSS_GENERAL_CACHE_RATE_LIMIT";

    public static final String OSS_ORC_MAX_MERGE_DISTANCE = "OSS_ORC_MAX_MERGE_DISTANCE";
    public static final String FILE_LIST = "FILE_LIST";
    public static final String FILE_PATTERN = "FILE_PATTERN";
    public static final String ENABLE_EXPIRE_FILE_STORAGE_PAUSE = "ENABLE_EXPIRE_FILE_STORAGE_PAUSE";
    public static final String ENABLE_CHECK_DDL_FILE_STORAGE = "ENABLE_CHECK_DDL_FILE_STORAGE";
    public static final String ENABLE_CHECK_DDL_BINDING_FILE_STORAGE = "ENABLE_CHECK_DDL_BINDING_FILE_STORAGE";
    public static final String ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE = "ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE";
    public static final String FILE_STORAGE_TASK_PARALLELISM = "FILE_STORAGE_TASK_PARALLELISM";
    public static final String ENABLE_FILE_STORE_CHECK_TABLE = "ENABLE_FILE_STORE_CHECK_TABLE";
    public static final String ENABLE_CHECK_GPP_FOR_LOCAL_INDEX = "ENABLE_CHECK_GPP_FOR_LOCAL_INDEX";
    public static final String ENABLE_OSS_BUFFER_POOL = "ENABLE_OSS_BUFFER_POOL";
    public static final String ENABLE_OSS_DELAY_MATERIALIZATION = "ENABLE_OSS_DELAY_MATERIALIZATION";
    public static final String ENABLE_OSS_ZERO_COPY = "ENABLE_OSS_ZERO_COPY";
    public static final String ENABLE_OSS_COMPATIBLE = "ENABLE_OSS_COMPATIBLE";
    public static final String OSS_STREAM_BUFFER_SIZE = "OSS_STREAM_BUFFER_SIZE";
    public static final String OSS_MAX_READ_AHEAD_PART_NUMBER = "OSS_MAX_READ_AHEAD_PART_NUMBER";

    public static final String ENABLE_PAIRWISE_SHUFFLE_COMPATIBLE = "ENABLE_PAIRWISE_SHUFFLE_COMPATIBLE";

    public static final String COLD_DATA_STATUS = "COLD_DATA_STATUS";

    public static final String ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE =
        "ENABLE_OSS_DELAY_MATERIALIZATION_ON_EXCHANGE";
    public static final String ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN =
        "ENABLE_OSS_FILE_CONCURRENT_SPLIT_ROUND_ROBIN";
    public static final String ENABLE_REUSE_VECTOR = "ENABLE_REUSE_VECTOR";
    public static final String ENABLE_DECIMAL_FAST_VEC = "ENABLE_DECIMAL_FAST_VEC";
    public static final String ENABLE_AND_FAST_VEC = "ENABLE_AND_FAST_VEC";
    public static final String ENABLE_OR_FAST_VEC = "ENABLE_OR_FAST_VEC";
    public static final String ENABLE_UNIQUE_HASH_KEY = "ENABLE_UNIQUE_HASH_KEY";
    public static final String ENABLE_PRUNE_EXCHANGE_PARTITION = "ENABLE_PRUNE_EXCHANGE_PARTITION";
    public static final String BLOCK_BUILDER_CAPACITY = "BLOCK_BUILDER_CAPACITY";
    public static final String ENABLE_HASH_TABLE_BLOOM_FILTER = "ENABLE_HASH_TABLE_BLOOM_FILTER";
    public static final String ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE =
        "ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE";
    public static final String OSS_FILE_ORDER = "OSS_FILE_ORDER";
    public static final String MAX_SESSION_PREPARED_STMT_COUNT = "MAX_SESSION_PREPARED_STMT_COUNT";
    public static final String ALLOW_REPLACE_ARCHIVE_TABLE = "ALLOW_REPLACE_ARCHIVE_TABLE";

    public static final String CHECK_ARCHIVE_PARTITION_READY = "CHECK_ARCHIVE_PARTITION_READY";

    public static final String ALLOW_CREATE_TABLE_LIKE_FILE_STORE = "ALLOW_CREATE_TABLE_LIKE_FILE_STORE";

    public static final String ALLOW_CREATE_TABLE_LIKE_IGNORE_ARCHIVE_CCI =
        "ALLOW_CREATE_TABLE_LIKE_IGNORE_ARCHIVE_CCI";

    /**
     * is enable collect partitions heatmap, dynamic, default:true
     */
    public static final String ENABLE_PARTITIONS_HEATMAP_COLLECTION = "ENABLE_PARTITIONS_HEATMAP_COLLECTION";
    /**
     * set schemas and tables of partitions heatmap collect
     * exp: 'schema_01#table1&table12,schema_02#table1' or  'schema_01,schema_02' or ''
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_ONLY = "PARTITIONS_HEATMAP_COLLECTION_ONLY";
    /**
     * if partitions numbers that has been collected more than PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN, then do not collect others.
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN = "PARTITIONS_HEATMAP_COLLECTION_MAX_SCAN";
    /**
     * if single logic schema count more than PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT, then do not collect it.
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT =
        "PARTITIONS_HEATMAP_COLLECTION_MAX_SINGLE_LOGIC_SCHEMA_COUNT";
    /**
     * if partitions numbers more than PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM, then merge this.
     */
    public static final String PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM =
        "PARTITIONS_HEATMAP_COLLECTION_MAX_MERGE_NUM";
    /**
     * extreme performance mode
     */
    public static final String ENABLE_EXTREME_PERFORMANCE = "ENABLE_EXTREME_PERFORMANCE";
    public static final String ENABLE_CLEAN_FAILED_PLAN = "ENABLE_CLEAN_FAILED_PLAN";

    public static final String ENABLE_LOG_SYSTEM_METRICS = "ENABLE_LOG_SYSTEM_METRICS";

    /**
     * the min size of IN expr that would be pruned
     */
    public static final String IN_PRUNE_SIZE = "IN_PRUNE_SIZE";
    /**
     * the batch size of IN expr being pruned
     */
    public static final String IN_PRUNE_STEP_SIZE = "IN_PRUNE_STEP_SIZE";
    public static final String IN_PRUNE_MAX_TIME = "IN_PRUNE_MAX_TIME";
    public static final String PRUNING_TIME_WARNING_THRESHOLD = "PRUNING_TIME_WARNING_THRESHOLD";

    public static final String ENABLE_PRUNING_IN = "ENABLE_PRUNING_IN";

    public static final String ENABLE_PRUNING_IN_DML = "ENABLE_PRUNING_IN_DML";

    public static final String ENABLE_STATISTIC_BUILD_SKEW = "ENABLE_STATISTIC_BUILD_SKEW";

    /**
     * the max num of pruning info cache by logical view
     */
    public static final String MAX_IN_PRUNE_CACHE_SIZE = "MAX_IN_PRUNE_CACHE_SIZE";
    /**
     * the max table num of cache pruning info for logical view
     */
    public static final String MAX_IN_PRUNE_CACHE_TABLE_SIZE = "MAX_IN_PRUNE_CACHE_TABLE_SIZE";
    public static final String REBALANCE_DB_PARALLELISM = "REBALANCE_DB_PARALLELISM";
    public static final String REBALANCE_CLUSTER_PARALLELISM = "REBALANCE_CLUSTER_PARALLELISM";
    public static final String REBALANCE_DB_LIST_WHEN_REBALANCE_CLUSTER_ONLY_DEBUG =
        "REBALANCE_DB_LIST_WHEN_REBALANCE_CLUSTER_ONLY_DEBUG";
    public static final String ALLOW_SCHEDULE_CONCURRENT_MOVE_PARTITION_INSIDE_TABLEGROUP =
        "ALLOW_SCHEDULE_CONCURRENT_MOVE_PARTITION_INSIDE_TABLEGROUP";
    /**
     * params for statement summary
     */
    public static final String ENABLE_STATEMENTS_SUMMARY = "ENABLE_STATEMENTS_SUMMARY";
    /**
     * the interval of flush the current statement summary set to the history set.  unit: seconds
     */
    public static final String STATEMENTS_SUMMARY_PERIOD_SEC = "STATEMENTS_SUMMARY_PERIOD_SEC";
    /**
     * the period count which the history contains
     */
    public static final String STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM = "STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM";
    /**
     * the max statement template count which statement summary support.
     */
    public static final String STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT = "STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT";
    public static final String STATEMENTS_SUMMARY_RECORD_INTERNAL = "STATEMENTS_SUMMARY_RECORD_INTERNAL";
    /**
     * only collect local data when it is false.
     */
    public static final String ENABLE_REMOTE_SYNC_ACTION = "ENABLE_REMOTE_SYNC_ACTION";
    /**
     * the max length of sql sample stored in statement summary.
     */
    public static final String STATEMENTS_SUMMARY_MAX_SQL_LENGTH = "STATEMENTS_SUMMARY_MAX_SQL_LENGTH";
    /**
     * the percent of queries being summarized.
     * when the percent is 0, only slow sql is summarized.
     */
    public static final String STATEMENTS_SUMMARY_PERCENT = "STATEMENTS_SUMMARY_PERCENT";
    public static final String ENABLE_STORAGE_TRIGGER = "enable_storage_trigger";
    public static final String ENABLE_TRANS_LOG = "ENABLE_TRANS_LOG";
    public static final String PLAN_CACHE_EXPIRE_TIME = "PLAN_CACHE_EXPIRE_TIME";
    public static final String SKIP_MOVE_DATABASE_VALIDATOR = "SKIP_MOVE_DATABASE_VALIDATOR";
    public static final String USE_MOVE_TABLEGROUP_VALIDATOR = "USE_MOVE_TABLEGROUP_VALIDATOR";
    public static final String ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP = "ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP";
    public static final String ENABLE_MPP_FILE_STORE_BACKFILL = "ENABLE_MPP_FILE_STORE_BACKFILL";
    public static final String PARTITION_NAME = "PARTITION_NAME";
    public static final String FORBID_REMOTE_DDL_TASK = "FORBID_REMOTE_DDL_TASK";
    public static final String DISABLE_REBALANCE_MPP = "DISABLE_REBALANCE_MPP";
    public static final String ENABLE_STANDBY_BACKFILL = "ENABLE_STANDBY_BACKFILL";
    public static final String FORCE_STANDBY_BACKFILL = "FORCE_STANDBY_BACKFILL";
    public static final String PHYSICAL_DDL_IGNORED_ERROR_CODE = "PHYSICAL_DDL_IGNORED_ERROR_CODE";
    public static final String DDL_PAUSE_DURING_EXCEPTION = "DDL_PAUSE_DURING_EXCEPTION";
    public static final String OUTPUT_MYSQL_ERROR_CODE = "OUTPUT_MYSQL_ERROR_CODE";

    public static final String MAPPING_TO_MYSQL_ERROR_CODE = "MAPPING_TO_MYSQL_ERROR_CODE";
    public static final String ENABLE_CONSISTENT_ERRORCODE = "ENABLE_CONSISTENT_ERRORCODE";

    public static final String ROLLBACK_ON_CHECKER = "ROLLBACK_ON_CHECKER";

    public static final String CHANGE_SET_REPLAY_TIMES = "CHANGE_SET_REPLAY_TIMES";
    public static final String CHANGE_SET_APPLY_BATCH = "CHANGE_SET_APPLY_BATCH";
    public static final String CHANGE_SET_APPLY_LOCK_BATCH = "CHANGE_SET_APPLY_LOCK_BATCH";
    public static final String CHANGE_SET_MEMORY_LIMIT = "CHANGE_SET_MEMORY_LIMIT";
    public static final String CHANGE_SET_APPLY_BATCH_FILE_SIZE = "CHANGE_SET_APPLY_BATCH_FILE_SIZE";
    public static final String ENABLE_CHANGESET = "ENABLE_CHANGESET";
    public static final String CN_ENABLE_CHANGESET = "CN_ENABLE_CHANGESET";
    public static final String CHANGE_SET_APPLY_SPEED_LIMITATION = "CHANGE_SET_APPLY_SPEED_LIMITATION";
    public static final String CHANGE_SET_APPLY_SPEED_MIN = "CHANGE_SET_APPLY_SPEED_MIN";
    public static final String CHANGE_SET_THREAD_POOL_SIZE = "CHANGE_SET_THREAD_POOL_SIZE";
    public static final String CHANGE_SET_APPLY_PHY_PARALLELISM = "CHANGE_SET_APPLY_PHY_PARALLELISM";
    public static final String CHANGE_SET_APPLY_OPTIMIZATION = "CHANGE_SET_APPLY_OPTIMIZATION";
    /**
     * for change set debug
     */
    public static final String SKIP_CHANGE_SET_CHECKER = "SKIP_CHANGE_SET_CHECKER";
    public static final String CHANGE_SET_CHECK_TWICE = "CHANGE_SET_CHECK_TWICE";
    public static final String CHANGE_SET_DEBUG_MODE = "CHANGE_SET_DEBUG_MODE";
    public static final String SKIP_CHANGE_SET_APPLY = "SKIP_CHANGE_SET_APPLY";
    public static final String SKIP_CHANGE_SET_FETCH = "SKIP_CHANGE_SET_FETCH";
    public static final String PURGE_OSS_FILE_CRON_EXPR = "PURGE_OSS_FILE_CRON_EXPR";
    public static final String PURGE_OSS_FILE_BEFORE_DAY = "PURGE_OSS_FILE_BEFORE_DAY";
    public static final String BACKUP_OSS_PERIOD = "BACKUP_OSS_PERIOD";
    public static final String FILE_STORAGE_FILES_META_QUERY_PARALLELISM = "FILE_STORAGE_FILES_META_QUERY_PARALLELISM";
    public static final String ENBALE_BIND_PARAM_TYPE = "ENBALE_BIND_PARAM_TYPE";
    public static final String ENABLE_GSI_LOOKUP_OPTIMIZE = "ENABLE_GSI_LOOKUP_OPTIMIZE";
    public static final String ENBALE_BIND_COLLATE = "ENBALE_BIND_COLLATE";
    public static final String SKIP_TABLEGROUP_VALIDATOR = "SKIP_TABLEGROUP_VALIDATOR";
    /**
     * Enable auto savepoint. If it is TRUE, failed DML statements will be rollbacked automatically.
     */
    public static final String ENABLE_AUTO_SAVEPOINT = "ENABLE_AUTO_SAVEPOINT";
    public static final String CURSOR_FETCH_CONN_MEMORY_LIMIT = "CURSOR_FETCH_CONN_MEMORY_LIMIT";
    public static final String FORCE_RESHARD = "FORCE_RESHARD";
    public static final String REMOVE_DDL_JOB_REDUNDANCY_RELATIONS = "REMOVE_DDL_JOB_REDUNDANCY_RELATIONS";
    public static final String TG_MDL_SEGMENT_SIZE = "TG_MDL_SEGMENT_SIZE";
    public static final String DB_MDL_SEGMENT_SIZE = "DB_MDL_SEGMENT_SIZE";
    public static final String ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY =
        "ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY";
    public static final String ENABLE_LOWER_CASE_TABLE_NAMES = "ENABLE_LOWER_CASE_TABLE_NAMES";
    public static final String ENABLE_LOWER_CASE_TABLE_NAME_OUTPUT = "ENABLE_LOWER_CASE_TABLE_NAME_OUTPUT";
    public static final String COST_MODEL_VERSION = "COST_MODEL_VERSION";
    /**
     * second when ddl plan scheduler wait for polling ddl plan record.
     */
    public static final String DDL_PLAN_SCHEDULER_DELAY = "DDL_PLAN_SCHEDULER_DELAY";
    public static final String USE_PARAMETER_DELEGATE = "USE_PARAMETER_DELEGATE";
    public static final String ENABLE_NODE_HINT_REPLACE = "ENABLE_NODE_HINT_REPLACE";
    public static final String USE_JDK_DEFAULT_SER = "USE_JDK_DEFAULT_SER";
    public static final String OPTIMIZE_TABLE_PARALLELISM = "OPTIMIZE_TABLE_PARALLELISM";
    public static final String OPTIMIZE_TABLE_USE_DAL = "OPTIMIZE_TABLE_USE_DAL";
    /**
     * module conf
     */
    public static final String MAINTENANCE_TIME_START = "MAINTENANCE_TIME_START";
    public static final String MAINTENANCE_TIME_END = "MAINTENANCE_TIME_END";
    public static final String ENABLE_MODULE_LOG = "ENABLE_MODULE_LOG";
    public static final String ENABLE_COLUMNAR_DECIMAL64 = "ENABLE_COLUMNAR_DECIMAL64";
    public static final String ENABLE_XPROTO_RESULT_DECIMAL64 = "ENABLE_XPROTO_RESULT_DECIMAL64";
    public static final String MAX_MODULE_LOG_PARAMS_SIZE = "MAX_MODULE_LOG_PARAMS_SIZE";
    public static final String MAX_MODULE_LOG_PARAM_SIZE = "MAX_MODULE_LOG_PARAM_SIZE";
    /**
     * speed limit for oss backfill procedure
     */
    public static final String OSS_BACKFILL_SPEED_LIMITATION = "OSS_BACKFILL_SPEED_LIMITATION";
    /**
     * speed lower bound for oss backfill procedure
     */
    public static final String OSS_BACKFILL_SPEED_MIN = "OSS_BACKFILL_SPEED_MIN";
    public static final String ONLY_MANUAL_TABLEGROUP_ALLOW = "ONLY_MANUAL_TABLEGROUP_ALLOW";
    public static final String MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH = "MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH";
    public static final String ACQUIRE_CREATE_TABLE_GROUP_LOCK = "ACQUIRE_CREATE_TABLE_GROUP_LOCK";
    public static final String DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL = "DDL_RW_LOCK_DEADLOCK_DETECTION_INTERVAL";
    public static final String PASSWORD_CHECK_PATTERN = "PASSWORD_CHECK_PATTERN";
    public static final String DEPRECATE_EOF = "DEPRECATE_EOF";
    public static final String ENABLE_AUTO_SPLIT_PARTITION = "ENABLE_AUTO_SPLIT_PARTITION";
    public static final String ENABLE_FORCE_PRIMARY_FOR_TSO = "ENABLE_FORCE_PRIMARY_FOR_TSO";
    public static final String ENABLE_FORCE_PRIMARY_FOR_FILTER = "ENABLE_FORCE_PRIMARY_FOR_FILTER";
    public static final String ENABLE_FORCE_PRIMARY_FOR_GROUP_BY = "ENABLE_FORCE_PRIMARY_FOR_GROUP_BY";
    /**
     * Whether rollback a branch of XA trx if its primary group is unknown.
     */
    public static final String ROLLBACK_UNKNOWN_PRIMARY_GROUP_XA_TRX = "ROLLBACK_UNKNOWN_PRIMARY_GROUP_XA_TRX";
    public static final String PREFETCH_EXECUTE_POLICY = "PREFETCH_EXECUTE_POLICY";

    public static final String ENABLE_RANGE_SCAN = "ENABLE_RANGE_SCAN";
    public static final String ENABLE_RANGE_SCAN_FOR_DML = "ENABLE_RANGE_SCAN_FOR_DML";
    public static final String RANGE_SCAN_MODE = "RANGE_SCAN_MODE";
    public static final String RANGE_SCAN_ADAPTIVE_POLICY = "RANGE_SCAN_ADAPTIVE_POLICY";
    public static final String RANGE_SCAN_SERIALIZE_LIMIT = "RANGE_SCAN_SERIALIZE_LIMIT";
    public static final String MAX_RECURSIVE_COUNT = "MAX_RECURSIVE_COUNT";
    public static final String MAX_RECURSIVE_CTE_MEM_BYTES = "MAX_RECURSIVE_CTE_MEM_BYTES";
    public static final String ENABLE_REPLICA = "ENABLE_REPLICA";
    public static final String GROUPING_LSN_THREAD_NUM = "GROUPING_LSN_THREAD_NUM";
    public static final String GROUPING_LSN_TIMEOUT = "GROUPING_LSN_TIMEOUT";
    public static final String ENABLE_ASYNC_COMMIT_80 = "ENABLE_ASYNC_COMMIT_80";
    public static final String ENABLE_ASYNC_COMMIT_57 = "ENABLE_ASYNC_COMMIT_57";
    public static final String ENABLE_TRANSACTION_RECOVER_TASK = "ENABLE_TRANSACTION_RECOVER_TASK";
    public static final String ASYNC_COMMIT_TASK_LIMIT = "ASYNC_COMMIT_TASK_LIMIT";
    public static final String ASYNC_COMMIT_PUSH_MAX_SEQ_ONLY_LEADER = "ASYNC_COMMIT_PUSH_MAX_SEQ_ONLY_LEADER";
    public static final String ASYNC_COMMIT_OMIT_PREPARE_TS = "ASYNC_COMMIT_OMIT_PREPARE_TS";
    public static final String ENABLE_FOLLOWER_READ = "ENABLE_FOLLOWER_READ";
    public static final String MIN_THRESHOLD_FOR_FOLLOWER = "MIN_THRESHOLD_FOR_FOLLOWER";
    public static final String ENABLE_ROLLBACK_MASTER_FOR_FOLLOWER_READ = "ENABLE_ROLLBACK_MASTER_FOR_FOLLOWER_READ";
    public static final String ENABLE_FOLLOWER_READ_IN_TRANS = "ENABLE_FOLLOWER_READ_IN_TRANS";
    public static final String ENABLE_FOLLOWER_READ_TIMEOUT = "ENABLE_FOLLOWER_READ_TIMEOUT";
    public static final String FOLLOWER_READ_ACCOUNT_TIMEOUT = "FOLLOWER_READ_ACCOUNT_TIMEOUT";

    public static final String CREATE_TABLE_WITH_CHARSET_COLLATE = "CREATE_TABLE_WITH_CHARSET_COLLATE";
    public static final String ENABLE_SIMPLIFY_SUBQUERY_SQL = "ENABLE_SIMPLIFY_SUBQUERY_SQL";
    public static final String ENABLE_SIMPLIFY_SHARDING_SQL = "ENABLE_SIMPLIFY_SHARDING_SQL";
    public static final String MAX_PHYSICAL_SLOW_SQL_PARAMS_TO_PRINT = "MAX_PHYSICAL_SLOW_SQL_PARAMS_TO_PRINT";
    public static final String MAX_CCI_COUNT = "MAX_CCI_COUNT";
    public static final String REBUILD_CCI_STRATEGY = "REBUILD_CCI_STRATEGY";
    public static final String ENABLE_MODIFY_CCI_CRITICAL_COLUMN = "ENABLE_MODIFY_CCI_CRITICAL_COLUMN";
    public static final String ENABLE_CCI_ON_TABLE_WITH_IMPLICIT_PK = "ENABLE_CCI_ON_TABLE_WITH_IMPLICIT_PK";
    public static final String ENABLE_COLUMNAR_IGNORE = "ENABLE_COLUMNAR_IGNORE";
    public static final String ENABLE_REBUILD_SNAPSHOT_CCI = "ENABLE_REBUILD_SNAPSHOT_CCI";
    public static final String ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE = "ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE";

    /**
     * max show length of group concat, only work for cn
     */
    @Deprecated
    public static final String CN_GROUP_CONCAT_MAX_LEN = "CN_GROUP_CONCAT_MAX_LEN";
    public static final String SERVER_ID = "SERVER_ID";
    public static final String ENABLE_REMOTE_CONSUME_LOG = "ENABLE_REMOTE_CONSUME_LOG";
    public static final String REMOTE_CONSUME_LOG_BATCH_SIZE = "REMOTE_CONSUME_LOG_BATCH_SIZE";
    public static final String ENABLE_TRANSACTION_STATISTICS = "ENABLE_TRANSACTION_STATISTICS";
    public static final String SLOW_TRANS_THRESHOLD = "SLOW_TRANS_THRESHOLD";
    public static final String TRANSACTION_STATISTICS_TASK_INTERVAL = "TRANSACTION_STATISTICS_TASK_INTERVAL";
    public static final String IDLE_TRANSACTION_TIMEOUT = "IDLE_TRANSACTION_TIMEOUT";
    public static final String IDLE_WRITE_TRANSACTION_TIMEOUT = "IDLE_WRITE_TRANSACTION_TIMEOUT";
    public static final String IDLE_READONLY_TRANSACTION_TIMEOUT = "IDLE_READONLY_TRANSACTION_TIMEOUT";
    public static final String MAX_CACHED_SLOW_TRANS_STATS = "MAX_CACHED_SLOW_TRANS_STATS";
    public static final String ENABLE_TRX_IDLE_TIMEOUT_TASK = "ENABLE_TRX_IDLE_TIMEOUT_TASK";
    public static final String TRX_IDLE_TIMEOUT_TASK_INTERVAL = "TRX_IDLE_TIMEOUT_TASK_INTERVAL";
    public static final String BACKFILL_USING_BINARY = "BACKFILL_USING_BINARY";
    /**
     * -1 mean the learner only allow read, this is the default value;
     */
    public static final String LEARNER_LEVEL = "LEARNER_LEVEL";
    public static final String PLAN_CACHE_SIZE = "PLAN_CACHE_SIZE";
    public static final String ENABLE_X_PROTO_OPT_FOR_AUTO_SP = "ENABLE_X_PROTO_OPT_FOR_AUTO_SP";
    public static final String SIM_CDC_FAILED = "SIM_CDC_FAILED";
    public static final String SKIP_DDL_RESPONSE = "SKIP_DDL_RESPONSE";
    public static final String ENABLE_ROLLBACK_TO_READY = "ENABLE_ROLLBACK_TO_READY";
    public static final String TRX_LOG_CLEAN_PARALLELISM = "TRX_LOG_CLEAN_PARALLELISM";
    public static final String CHECK_RESPONSE_IN_MEM = "CHECK_RESPONSE_IN_MEM";
    public static final String ASYNC_PAUSE = "ASYNC_PAUSE";
    public static final String DRY_RUN_PHYSICAL_DDL = "DRY_RUN_PHYSICAL_DDL";
    public static final String PHYSICAL_DDL_TASK_RETRY = "PHYSICAL_DDL_TASK_RETRY";

    public static final String SKIP_COLUMNAR_WAIT_TASK = "SKIP_COLUMNAR_WAIT_TASK";
    // columnar index
    public static final String COLUMNAR_BITMAP_INDEX_MAX_SCAN_SIZE_FOR_PRUNING =
        "COLUMNAR_BITMAP_INDEX_MAX_SCAN_SIZE_FOR_PRUNING";
    /**
     * To enable the columnar scan exec.
     */
    public static final String ENABLE_COLUMNAR_SCAN_EXEC = "ENABLE_COLUMNAR_SCAN_EXEC";
    /**
     * The count of maximum groups in a scan work.
     */
    public static final String COLUMNAR_WORK_UNIT = "COLUMNAR_WORK_UNIT";
    /**
     * To enable the multi-version partition for columnar index partition pruning.
     */
    public static final String ENABLE_COLUMNAR_MULTI_VERSION_PARTITION = "ENABLE_COLUMNAR_MULTI_VERSION_PARTITION";
    /**
     * The policy of table scan: IO_PRIORITY, FILTER_PRIORITY, IO_ON_DEMAND.
     */
    public static final String SCAN_POLICY = "SCAN_POLICY";

    /**
     * Enable the random split for columnar scan to avoid hang on the same file
     */
    public static final String ENABLE_COLUMNAR_SCAN_RANDOM_SPLIT = "ENABLE_COLUMNAR_SCAN_RANDOM_SPLIT";

    /**
     * To enable the oss client crc check.
     * polardbx.file.storage.info += 1 after modify this param
     */
    public static final String ENABLE_OSS_CLIENT_CRC_CHECK = "ENABLE_OSS_CLIENT_CRC_CHECK";

    /**
     * To enable the block cache.
     */
    public static final String ENABLE_BLOCK_CACHE = "ENABLE_BLOCK_CACHE";

    public static final String ENABLE_COLUMNAR_CSV_CACHE = "ENABLE_COLUMNAR_CSV_CACHE";

    public static final String ENABLE_COLUMNAR_DEL_CACHE = "ENABLE_COLUMNAR_DEL_CACHE";

    public static final String ENABLE_COLUMNAR_SNAPSHOT_CACHE = "ENABLE_COLUMNAR_SNAPSHOT_CACHE";

    public static final String COLUMNAR_SNAPSHOT_CACHE_TTL_MS = "COLUMNAR_SNAPSHOT_CACHE_TTL_MS";

    public static final String CSV_CACHE_SIZE = "CSV_CACHE_SIZE";

    public static final String ENABLE_USE_IN_FLIGHT_BLOCK_CACHE = "ENABLE_USE_IN_FLIGHT_BLOCK_CACHE";

    /**
     * To enable the verbose metrics report.
     */
    public static final String ENABLE_VERBOSE_METRICS_REPORT = "ENABLE_VERBOSE_METRICS_REPORT";
    /**
     * To enable the columnar metrics.
     */
    public static final String ENABLE_COLUMNAR_METRICS = "ENABLE_COLUMNAR_METRICS";
    /**
     * To enable the index pruning on orc.
     */
    public static final String ENABLE_INDEX_PRUNING = "ENABLE_INDEX_PRUNING";
    /**
     * To enable canceling the loading processing of stripe-loader.
     */
    public static final String ENABLE_CANCEL_STRIPE_LOADING = "ENABLE_CANCEL_STRIPE_LOADING";

    public static final String ENABLE_COLUMNAR_SLICE_DICT = "ENABLE_COLUMNAR_SLICE_DICT";

    public static final String ENABLE_LAZY_BLOCK_ACTIVE_LOADING = "ENABLE_LAZY_BLOCK_ACTIVE_LOADING";
    public static final String ENABLE_COLUMN_READER_LOCK = "ENABLE_COLUMN_READER_LOCK";
    public static final String ENABLE_VEC_ACCUMULATOR = "ENABLE_VEC_ACCUMULATOR";
    public static final String ENABLE_LOCAL_EXCHANGE_BATCH = "ENABLE_LOCAL_EXCHANGE_BATCH";
    public static final String ENABLE_VEC_BUILD_JOIN_ROW = "ENABLE_VEC_BUILD_JOIN_ROW";
    public static final String ENABLE_VEC_JOIN = "ENABLE_VEC_JOIN";
    public static final String ENABLE_JOIN_CONDITION_PRUNING = "ENABLE_JOIN_CONDITION_PRUNING";
    public static final String ENABLE_EXCHANGE_PARTITION_OPTIMIZATION = "ENABLE_EXCHANGE_PARTITION_OPTIMIZATION";
    public static final String ENABLE_DRIVER_OBJECT_POOL = "ENABLE_DRIVER_OBJECT_POOL";
    public static final String ENABLE_COLUMNAR_SCAN_SELECTION = "ENABLE_COLUMNAR_SCAN_SELECTION";
    public static final String BLOCK_CACHE_MEMORY_SIZE_FACTOR = "BLOCK_CACHE_MEMORY_SIZE_FACTOR";
    public static final String PREHEATED_CACHE_MAX_MEMORY_SIZE = "PREHEATED_CACHE_MAX_MEMORY_SIZE";
    public static final String ENABLE_BLOCK_BUILDER_BATCH_WRITING = "ENABLE_BLOCK_BUILDER_BATCH_WRITING";
    public static final String ENABLE_SCAN_RANDOM_SHUFFLE = "ENABLE_SCAN_RANDOM_SHUFFLE";

    public static final String SCAN_RANDOM_SHUFFLE_THRESHOLD = "SCAN_RANDOM_SHUFFLE_THRESHOLD";

    public static final String ENABLE_AUTOMATIC_COLUMNAR_PARAMS = "ENABLE_AUTOMATIC_COLUMNAR_PARAMS";

    public static final String ENABLE_FILE_STORAGE_DELTA_STATISTIC = "ENABLE_FILE_STORAGE_DELTA_STATISTIC";

    public static final String ZONEMAP_MAX_GROUP_SIZE = "ZONEMAP_MAX_GROUP_SIZE";
    public static final String PHYSICAL_BACKFILL_BATCH_SIZE = "PHYSICAL_BACKFILL_BATCH_SIZE";
    public static final String PHYSICAL_BACKFILL_MIN_SUCCESS_BATCH_UPDATE =
        "PHYSICAL_BACKFILL_MIN_SUCCESS_BATCH_UPDATE";
    public static final String PHYSICAL_BACKFILL_MIN_WRITE_BATCH_PER_THREAD =
        "PHYSICAL_BACKFILL_MIN_WRITE_BATCH_PER_THREAD";
    public static final String PHYSICAL_BACKFILL_PARALLELISM = "PHYSICAL_BACKFILL_PARALLELISM";
    public static final String PHYSICAL_BACKFILL_ENABLE = "PHYSICAL_BACKFILL_ENABLE";
    public static final String PHYSICAL_BACKFILL_FROM_FOLLOWER = "PHYSICAL_BACKFILL_FROM_FOLLOWER";
    public static final String PHYSICAL_BACKFILL_MAX_RETRY_WAIT_FOLLOWER_TO_LSN =
        "PHYSICAL_BACKFILL_MAX_RETRY_WAIT_FOLLOWER_TO_LSN";
    public static final String PHYSICAL_BACKFILL_MAX_SLAVE_LATENCY = "PHYSICAL_BACKFILL_MAX_SLAVE_LATENCY";
    public static final String PHYSICAL_BACKFILL_NET_SPEED_TEST_TIME = "PHYSICAL_BACKFILL_NET_SPEED_TEST_TIME";
    public static final String IMPORT_TABLESPACE_TASK_EXEC_SERIALLY = "IMPORT_TABLESPACE_TASK_EXEC_SERIALLY";
    //this option is just for test only
    public static final String PHYSICAL_BACKFILL_IGNORE_CFG = "PHYSICAL_BACKFILL_IGNORE_CFG";
    public static final String PHYSICAL_BACKFILL_SPEED_LIMIT = "PHYSICAL_BACKFILL_SPEED_LIMIT";
    public static final String PHYSICAL_BACKFILL_WAIT_LSN_WHEN_ROLLBACK = "PHYSICAL_BACKFILL_WAIT_LSN_WHEN_ROLLBACK";
    public static final String PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK = "PHYSICAL_BACKFILL_STORAGE_HEALTHY_CHECK";

    public static final String PHYSICAL_BACKFILL_IMPORT_TABLESPACE_BY_LEADER =
        "PHYSICAL_BACKFILL_IMPORT_TABLESPACE_BY_LEADER";

    public static final String PHYSICAL_BACKFILL_IMPORT_TABLESPACE_IO_ADVISE =
        "PHYSICAL_BACKFILL_IMPORT_TABLESPACE_IO_ADVISE";
    public static final String PHYSICAL_BACKFILL_PIPELINE_SIZE = "PHYSICAL_BACKFILL_PIPELINE_SIZE";

    public static final String FLUSH_TABLE_TIMEOUT_FOR_DDL_ON_XPROTO_CONN =
        "FLUSH_TABLE_TIMEOUT_FOR_DDL_ON_XPROTO_CONN";

    public static final String DISCARD_TABLESPACE_USE_GROUP_CONCURRENT_BLOCK =
        "DISCARD_TABLESPACE_USE_GROUP_CONCURRENT_BLOCK";

    public static final String PHYSICAL_BACKFILL_SPEED_TEST =
        "PHYSICAL_BACKFILL_SPEED_TEST";

    public static final String PHYSICAL_BACKFILL_CLONE_DATA_FROM_LEADER =
        "PHYSICAL_BACKFILL_CLONE_DATA_FROM_LEADER";

    public static final String FETCH_TABLE_SIZE_FROM_TABLESPACE =
        "FETCH_TABLE_SIZE_FROM_TABLESPACE";

    public static final String TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL =
        "TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL";

    public static final String ANALYZE_TABLE_AFTER_IMPORT_TABLESPACE =
        "ANALYZE_TABLE_AFTER_IMPORT_TABLESPACE";

    public static final String CHECK_TABLE_DISCARD_STATE =
        "CHECK_TABLE_DISCARD_STATE";

    public static final String REBALANCE_MAINTENANCE_ENABLE = "REBALANCE_MAINTENANCE_ENABLE";
    public static final String REBALANCE_MAINTENANCE_TIME_START = "REBALANCE_MAINTENANCE_TIME_START";

    public static final String REBALANCE_MAINTENANCE_TIME_END = "REBALANCE_MAINTENANCE_TIME_END";

    public static final String CANCEL_REBALANCE_JOB_DUE_MAINTENANCE = "CANCEL_REBALANCE_JOB_DUE_MAINTENANCE";

    public static final String ENABLE_DEADLOCK_DETECTION_80 = "ENABLE_DEADLOCK_DETECTION_80";

    public static final String DEADLOCK_DETECTION_80_FETCH_TRX_ROWS = "DEADLOCK_DETECTION_80_FETCH_TRX_ROWS";

    public static final String DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD =
        "DEADLOCK_DETECTION_DATA_LOCK_WAITS_THRESHOLD";

    public static final String MAX_KEEP_DEADLOCK_LOGS = "MAX_KEEP_DEADLOCK_LOGS";

    public static final String ENABLE_FLASHBACK_AREA = "ENABLE_FLASHBACK_AREA";

    public static final String ENABLE_AS_OF_CROSS_DDL = "ENABLE_AS_OF_CROSS_DDL";

    // In milliseconds.
    public static final String MIN_SNAPSHOT_KEEP_TIME = "MIN_SNAPSHOT_KEEP_TIME";

    public static final String COLUMNAR_VERSION_CHAIN_PRUNER = "COLUMNAR_VERSION_CHAIN_PRUNER";

    public static final String MOCK_COLUMNAR_INDEX = "MOCK_COLUMNAR_INDEX";
    public static final String MCI_FORMAT = "MCI_FORMAT";
    public static final String ENABLE_LOGICAL_TABLE_META = "ENABLE_LOGICAL_TABLE_META";
    public static final String OPTIMIZER_TYPE = "OPTIMIZER_TYPE";
    public static final String ENABLE_COLUMNAR_AFTER_CBO_PLANNER = "ENABLE_COLUMNAR_AFTER_CBO_PLANNER";
    public static final String PUSH_PROJECT_INPUT_REF_THRESHOLD = "PUSH_PROJECT_INPUT_REF_THRESHOLD";

    /**
     * 0: legacy method
     * 1: new method (A/B table)
     */
    public static final String TRX_LOG_METHOD = "TRX_LOG_METHOD";

    /**
     * A/B table clean interval time, in minute.
     * default: 30 min
     */
    public static final String TRX_LOG_CLEAN_INTERVAL = "TRX_LOG_CLEAN_INTERVAL";

    public static final String SKIP_LEGACY_LOG_TABLE_CLEAN = "SKIP_LEGACY_LOG_TABLE_CLEAN";

    public static final String WARM_UP_DB_PARALLELISM = "WARM_UP_DB_PARALLELISM";

    public static final String GDN_TRX_POLICY_STATUS = "GDN_TRX_POLICY_STATUS";

    /**
     * In seconds.
     */
    public static final String WARM_UP_DB_INTERVAL = "WARM_UP_DB_INTERVAL";

    public static final String ENABLE_XA_TSO = "ENABLE_XA_TSO";

    public static final String ENABLE_AUTO_COMMIT_TSO = "ENABLE_AUTO_COMMIT_TSO";

    public static final String MAX_CONNECTIONS = "MAX_CONNECTIONS";

    public static final String MAX_SHOW_DDL_RESULT_STMT_LENGTH = "MAX_SHOW_DDL_RESULT_STMT_LENGTH";

    public static final String ENABLE_ENCDB = "ENABLE_ENCDB";

    public static final String ENABLE_TRX_EVENT_LOG = "ENABLE_TRX_EVENT_LOG";

    public static final String ENABLE_TRX_DEBUG_MODE = "ENABLE_TRX_DEBUG_MODE";

    public static final String IGNORE_TRANSACTION_POLICY_NO_TRANSACTION = "IGNORE_TRANSACTION_POLICY_NO_TRANSACTION";

    public static final String ENABLE_XXHASH_RF_IN_BUILD = "ENABLE_XXHASH_RF_IN_BUILD";

    public static final String ENABLE_XXHASH_RF_IN_FILTER = "ENABLE_XXHASH_RF_IN_FILTER";

    public static final String ENABLE_NEW_RF = "ENABLE_NEW_RF";

    public static final String GLOBAL_RF_ROWS_UPPER_BOUND = "GLOBAL_RF_ROWS_UPPER_BOUND";

    public static final String GLOBAL_RF_ROWS_LOWER_BOUND = "GLOBAL_RF_ROWS_LOWER_BOUND";

    public static final String ENABLE_SKIP_COMPRESSION_IN_ORC = "ENABLE_SKIP_COMPRESSION_IN_ORC";

    public static final String ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE = "ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE";

    public static final String NEW_RF_SAMPLE_COUNT = "NEW_RF_SAMPLE_COUNT";

    public static final String NEW_RF_FILTER_RATIO_THRESHOLD = "NEW_RF_FILTER_RATIO_THRESHOLD";

    public static final String GSI_LOOKUP_OPTIMIZE_THRESHOLD = "GSI_LOOKUP_OPTIMIZE_THRESHOLD";

    public static final String ENABLE_DRDS_TRACE_FOR_XA = "ENABLE_DRDS_TRACE_FOR_XA";

    public static final String ENABLE_LBAC = "ENABLE_LBAC";

    public static final String ENABLE_VALUES_PUSHDOWN = "ENABLE_VALUES_PUSHDOWN";

    public static final String ENABLE_SET_GLOBAL_SERVER_ID = "ENABLE_SET_GLOBAL_SERVER_ID";
    public static final String CDC_RANDOM_DDL_TOKEN = "CDC_RANDOM_DDL_TOKEN";
    public static final String ENABLE_IMPLICIT_TABLE_GROUP = "ENABLE_IMPLICIT_TABLE_GROUP";
    public static final String ALLOW_AUTO_CREATE_TABLEGROUP = "ALLOW_AUTO_CREATE_TABLEGROUP";
    public static final String INSTANCE_READ_ONLY = "INSTANCE_READ_ONLY";
    public static final String SUPER_WRITE = "SUPER_WRITE";
    public static final String ENABLE_EXTRACT_STREAM_NAME_FROM_USER = "ENABLE_EXTRACT_STREAM_NAME_FROM_USER";

    public static final String SNAPSHOT_TS = "SNAPSHOT_TS";

    public static final String SKIP_CHECK_CCI_TASK = "SKIP_CHECK_CCI_TASK";

    public static final String ENABLE_1PC_OPT = "ENABLE_1PC_OPT";

    public static final String FORCE_CCI_VISIBLE = "FORCE_CCI_VISIBLE";

    public static final String ENABLE_OSS_DELETED_SCAN = "ENABLE_OSS_DELETED_SCAN";

    public static final String ENABLE_ORC_RAW_TYPE_BLOCK = "ENABLE_ORC_RAW_TYPE_BLOCK";

    public static final String READ_CSV_ONLY = "READ_CSV_ONLY";

    public static final String READ_ORC_ONLY = "READ_ORC_ONLY";

    public static final String READ_SPECIFIED_COLUMNAR_FILES = "READ_SPECIFIED_COLUMNAR_FILES";

    public static final String CCI_INCREMENTAL_CHECK = "CCI_INCREMENTAL_CHECK";

    public static final String ENABLE_CCI_FAST_CHECKER = "ENABLE_CCI_FAST_CHECKER";

    public static final String ENABLE_CCI_NAIVE_CHECK_IF_FAST_CHECKER_FAILED =
        "ENABLE_CCI_NAIVE_CHECK_IF_FAST_CHECKER_FAILED";

    public static final String ENABLE_FAST_PARSE_ORC_RAW_TYPE = "ENABLE_FAST_PARSE_ORC_RAW_TYPE";

    public static final String ENABLE_ACCURATE_REL_TYPE_TO_DATA_TYPE = "ENABLE_ACCURATE_REL_TYPE_TO_DATA_TYPE";

    public static final String CHECK_CCI_TASK_CHECKPOINT_LIMIT = "CHECK_CCI_TASK_CHECKPOINT_LIMIT";

    public static final String SKIP_CHECK_CCI_SCHEDULE_JOB = "SKIP_CHECK_CCI_SCHEDULE_JOB";

    public static final String FORBID_AUTO_COMMIT_TRX = "FORBID_AUTO_COMMIT_TRX";

    public static final String FORCE_2PC_DURING_CCI_CHECK = "FORCE_2PC_DURING_CCI_CHECK";

    public static final String ENABLE_ACCURATE_INFO_SCHEMA_TABLES = "ENABLE_ACCURATE_INFO_SCHEMA_TABLES";
    public static final String ENABLE_FLOATING_TYPE_PRECISION = "ENABLE_FLOATING_TYPE_PRECISION";

    public static final String ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION = "ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION";

    /**
     * Enable sync point function in CN.
     */
    public static final String ENABLE_SYNC_POINT = "ENABLE_SYNC_POINT";

    public static final String PRINT_MORE_INFO_FOR_DEADLOCK_DETECTION = "PRINT_MORE_INFO_FOR_DEADLOCK_DETECTION";

    /**
     * Mark this trx as sync point trx, for inner usage.
     */
    public static final String MARK_SYNC_POINT = "MARK_SYNC_POINT";

    public static final String SYNC_POINT_TASK_INTERVAL = "SYNC_POINT_TASK_INTERVAL";

    public static final String KILL_PHYSICAL_CONNECTION_DELAY = "KILL_PHYSICAL_CONNECTION_DELAY";

    public static final String DISABLE_LEGACY_VARIABLE = "DISABLE_LEGACY_VARIABLE";

    public static final String SHOW_COLUMNAR_STATUS_USE_SUB_QUERY = "SHOW_COLUMNAR_STATUS_USE_SUB_QUERY";

    public static final String ENABLE_SHARE_READVIEW_IN_RC = "ENABLE_SHARE_READVIEW_IN_RC";

    /**
     * ================ Param keys for ttl job begin ================
     */

    /**
     * ===========================
     * The following Config Params for polardbx-inst-level
     * ===========================
     */

    /**
     * The global worker count of select task of all ttl-jobs, default is 0, auto decided by dn count
     */
    public static final String TTL_GLOBAL_SELECT_WORKER_COUNT = "TTL_GLOBAL_SELECT_WORKER_COUNT";

    /**
     * The global worker count of delete task of all ttl-jobs, default is 0, auto decided by dn count
     */
    public static final String TTL_GLOBAL_DELETE_WORKER_COUNT = "TTL_GLOBAL_DELETE_WORKER_COUNT";

    /**
     * The max data length of each one ttl tmp table
     */
    public static final String TTL_TMP_TBL_MAX_DATA_LENGTH = "TTL_TMP_TBL_MAX_DATA_LENGTH";

    /**
     * The max percent of (data_free * 100 /  data_length) of ttl-table to perform optimize-table operation,
     * the default value if 60% , unit: %
     */
    public static final String TTL_TBL_MAX_DATA_FREE_PERCENT = "TTL_TBL_MAX_DATA_FREE_PERCENT";

    /**
     * The max wait time for interrupting a running ttl intra-task, unit: ms, default is 10s
     * Unit: ms
     */
    public static final String TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME = "TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME";

    /**
     * The wait time of each round for the manager of intra-tasks
     * Unit: ms
     */
    public static final String TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME =
        "TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME";

    /**
     * The max wait time for waiting a ddl-job from pause to running, unit: ms, default value is  30s
     */
    public static final String TTL_WAIT_TIME_OF_DDL_JOB_FROM_PAUSED_TO_RUNNING =
        "TTL_WAIT_TIME_OF_DDL_JOB_FROM_PAUSED_TO_RUNNING";

    /**
     * The global switch that label if using auto-optimize table in ttl job
     */
    public static final String TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB = "TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB";

    /**
     * Label if auto perform the optimize-table operation for the ttl-table after archiving
     */
    public static final String TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING =
        "TTL_ENABLE_AUTO_EXEC_OPTIMIZE_TABLE_AFTER_ARCHIVING";

    /**
     * The max parallelism for the scheduled job of all ttl tables
     */
    public static final String TTL_SCHEDULED_JOB_MAX_PARALLELISM = "TTL_SCHEDULED_JOB_MAX_PARALLELISM";

    /**
     * Label if perform ttl-job one by one the tables which ar arc by part
     */
    public static final String TTL_SCHEDULE_JOB_ARCHIVED_BY_PARTITION_ONE_BY_ONE =
        "TTL_SCHEDULE_JOB_ARCHIVED_BY_PARTITION_ONE_BY_ONE";

    /**
     * A local debug option, use create gsi sql instead of cci sql for create
     * columnar arc table of ttl table, just for debug, session level
     */
    public static final String TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL = "TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL";

    /**
     * A local debug option of ttl20, specify the skip ddl tasks of cci creation of ttl tbl,
     * such as : SKIP_DDL_TASKS="WaitColumnarTableCreationTask"
     */
    public static final String TTL_DEBUG_CCI_SKIP_DDL_TASKS = "TTL_DEBUG_CCI_SKIP_DDL_TASKS";

    /**
     * Label if enable split cci partition from the nearest existing range partition
     */
    public static final String TTL_ENABLE_CCI_SPLIT_FROM_NEAREST_PART =
        "TTL_ENABLE_CCI_SPLIT_FROM_NEAREST_PART";

    /**
     * The gap count (in units of arc-part-interval) beyond current time that identifies
     * user-reserved range partitions which are NOT managed by TTL.
     * A range partition whose bound is more than N part-intervals ahead of the current time
     * is treated as a user-reserved partition (transparent to TTL, only usable as a split target).
     * Default -1 means infinity (no reserved zone, fully backward-compatible).
     */
    public static final String TTL_CCI_RESERVED_PART_GAP_COUNT =
        "TTL_CCI_RESERVED_PART_GAP_COUNT";

    /**
     * The default batch size of dml of ttl-job
     */
    public static final String TTL_JOB_DEFAULT_BATCH_SIZE = "TTL_JOB_DEFAULT_BATCH_SIZE";

    /**
     * The interval count for computing minCleanupBound base on lowerBound(normalized minVal of ttl_col),
     * that means the delta = ttlMinCleanupBoundIntervalCount * ttlUnit, is the delta interval between
     * minCleanupBound and the lowerBound, default is 1
     */
    public static final String TTL_CLEANUP_BOUND_INTERVAL_COUNT = "TTL_CLEANUP_BOUND_INTERVAL_COUNT";

    /**
     * Stop ttl-job scheduling for all ttl tables, used for handling critical situation
     */
    public static final String TTL_STOP_ALL_JOB_SCHEDULING = "TTL_STOP_ALL_JOB_SCHEDULING";

    /**
     * label if use archive trans policy for all dml trans of  ttl-job
     * <pre>
     *      usage:
     *           set transaction_policy = archive;
     *           begin;
     *           ...
     *           delete from ttl_tbl where ...
     *           commit;
     *
     *  </pre>
     */
    public static final String TTL_USE_ARCHIVE_TRANS_POLICY = "TTL_USE_ARCHIVE_TRANS_POLICY";

    /**
     * The default merge_union_size for the select sql of fetch ttl-col lower bound
     */
    public static final String TTL_SELECT_MERGE_UNION_SIZE = "TTL_SELECT_MERGE_UNION_SIZE";

    /**
     * The label if use merge_concurrent for the select sql of fetch ttl-col lower bound
     */
    public static final String TTL_SELECT_MERGE_CONCURRENT = "TTL_SELECT_MERGE_CONCURRENT";

    /**
     * The query hint for the select stmt of fetch ttl-col lower bound,
     * which use to control the concurrent policy
     */
    public static final String TTL_SELECT_STMT_HINT = "TTL_SELECT_STMT_HINT";

    /**
     * The query hint for delete stmt of deleting expired data
     */
    public static final String TTL_DELETE_STMT_HINT = "TTL_DELETE_STMT_HINT";

    /**
     * The query hint for insert-select stmt of preparing expired data
     */
    public static final String TTL_INSERT_STMT_HINT = "TTL_INSERT_STMT_HINT";

    /**
     * The query hint for optimize table stmt of ttl-table
     */
    public static final String TTL_OPTIMIZE_TABLE_STMT_HINT = "TTL_OPTIMIZE_TABLE_STMT_HINT";

    /**
     * The query hint for alter table add parts stmt of cci of ttl-table or arctmp of ttl-table
     */
    public static final String TTL_ALTER_ADD_PART_STMT_HINT = "TTL_ALTER_ADD_PART_STMT_HINT";

    /**
     * The extras params for exec adding add_part_stmt of ttl20
     */
    public static final String TTL_ALTER_ADD_PART_STMT_EXTRA_PARAMS = "TTL_ALTER_ADD_PART_STMT_EXTRA_PARAMS";

    /**
     * The query hint for alter table drop parts stmt of cci of ttl-table or arctmp of ttl-table
     */
    public static final String TTL_ALTER_DROP_PART_STMT_HINT = "TTL_ALTER_DROP_PART_STMT_HINT";

    /**
     * The extras params for exec drop_part_stmt of ttl20
     */
    public static final String TTL_ALTER_DROP_PART_STMT_EXTRA_PARAMS = "TTL_ALTER_DROP_PART_STMT_EXTRA_PARAMS";

    /**
     * The default group_parallelism of conn of select stmt, 0 means use the default val of inst_config
     */
    public static final String TTL_GROUP_PARALLELISM_ON_DQL_CONN = "TTL_GROUP_PARALLELISM_ON_DQL_CONN";

    /**
     * The default group_parallelism of conn of delete/insert stmt,0 means use the default val of inst_config
     */
    public static final String TTL_GROUP_PARALLELISM_ON_DML_CONN = "TTL_GROUP_PARALLELISM_ON_DML_CONN";

    /**
     * Label if auto add a maxvalue into the range parts of cci of art-tbl
     */
    public static final String TTL_ADD_MAXVAL_PART_ON_CCI_CREATING = "TTL_ADD_MAXVAL_PART_ON_CCI_CREATING";

    /**
     * The max periods of try waiting to acquire the rate permits, unit: ms
     */
    public static final String TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS = "TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS";

    /**
     * The default rowsSpeed limit for each dn, unit: rows/sec
     */
    public static final String TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN = "TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN";

    /**
     * Label if need limit the cleanup rows speed for each dn
     */
    public static final String TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT = "TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT";

    /**
     * Label if ignore maintain window in ttl ddl job
     */
    public static final String TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB = "TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB";

    /**
     * Label if use TTL_JOB_MAINTENANCE_WINDOW, default val is true
     */
    public static final String TTL_JOB_MAINTENANCE_ENABLE = "TTL_JOB_MAINTENANCE_ENABLE";

    /**
     * Label the independent maintain start time of ttl_job
     */
    public static final String TTL_JOB_MAINTENANCE_TIME_START = "TTL_JOB_MAINTENANCE_TIME_START";

    /**
     * Label the independent maintain end time of ttl_job
     */
    public static final String TTL_JOB_MAINTENANCE_TIME_END = "TTL_JOB_MAINTENANCE_TIME_END";

    /**
     * The retry time for fired-scheudled-Ttl-Job to restart paused the ddl job of cleanup expired data
     */
    public static final String TTL_MAX_RETRY_TIME_FOR_PAUSED_CLEANUP_DDL_JOB =
        "TTL_MAX_RETRY_TIME_FOR_PAUSED_CLEANUP_DDL_JOB";

    /**
     * The wait time before exec each retry ddl-stmt of cleanup expired data. Unit: ms
     */
    public static final String TTL_WAIT_TIME_BEFORE_EACH_DDL_STMT_RETRY =
        "TTL_WAIT_TIME_BEFORE_EACH_DDL_STMT_RETRY";

    /**
     * a debug properties for ttl scheduled job to interrupt ttl-job ignore if out of maintain windows
     */
    public static final String TTL_JOB_INTERRUPT_IGNORE_MAINTAIN_WINDOWS = "TTL_JOB_INTERRUPT_IGNORE_MAINTAIN_WINDOWS";

    /**
     * When this hint is set to true in a manually-triggered
     * "ALTER TABLE xxx CLEANUP EXPIRED DATA" statement, the DDL job will respect the TTL
     * maintenance-window constraint just like a scheduler-triggered job:
     * if the current time falls outside the window, the cleanup batch loop will stop
     * and the job will be paused.
     * Usage: /*+TDDL:TTL_JOB_FOLLOW_MAINTAIN_WINDOW=true*&#47; ALTER TABLE ...
     * Default is false (manual jobs are unrestricted by the maintenance window).
     */
    public static final String TTL_JOB_FOLLOW_MAINTAIN_WINDOW = "TTL_JOB_FOLLOW_MAINTAIN_WINDOW";

    /**
     * The ratio of global-worker / rw-dn-count, default is 2
     */
    public static final String TTL_GLOBAL_WORKER_DN_RATIO = "TTL_GLOBAL_WORKER_DN_RATIO";

    /**
     * Label if enable batch-resubmit schedule for TTL cleanup intra tasks.
     * When enabled, each DataCleaningUpIntraTask runs only one batch per scheduling slot
     * and resubmits itself to the thread pool if more data remains, so workers are never
     * bound to a single partition and long-tail idle workers are eliminated.
     * Default is false (keep the original partition-hold-thread behaviour for rollback).
     */
    public static final String TTL_ENABLE_BATCH_RESUBMIT_SCHEDULE = "TTL_ENABLE_BATCH_RESUBMIT_SCHEDULE";

    /**
     * The max concurrent worker count allowed on each DN when batch-resubmit schedule is enabled.
     * 0 means auto-decide as (totalWorkers / dnCount), capped to avoid overloading a single DN.
     */
    public static final String TTL_MAX_WORKER_COUNT_EACH_DN = "TTL_MAX_WORKER_COUNT_EACH_DN";

    /**
     * Label if enable per-partition intra-task info logging for TTL cleanup jobs.
     * When true, each batch completion and the per-partition batch-round summary are written to the TTL task log.
     * Replaces the static TtlConfigUtil.enableCleanupIntraTaskInfoLog field with a dynamically-readable param.
     * Default is false.
     */
    public static final String TTL_ENABLE_INTRA_TASK_INFO_LOG = "TTL_ENABLE_INTRA_TASK_INFO_LOG";

    /**
     * The default allocate part count for pre building of futrue of arc cci
     */
    public static final String TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT = "TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT";

    /**
     * The default allocate part count for post building of past of arc cci
     */
    public static final String TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT = "TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT";

    /**
     * Label if enable auto add partitoins for arc cci
     */
    public static final String TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI = "TTL_ENABLE_AUTO_ADD_PARTS_FOR_ARC_CCI";

    /**
     * Label if enable scan add parts warning
     */
    public static final String TTL_ENABLE_SCAN_ADD_PARTS_WARNING = "TTL_ENABLE_SCAN_ADD_PARTS_WARNING";

    /**
     * The scan interval of warning scan for add parts
     */
    public static final String TTL_ADD_PARTS_WARNING_SCAN_INTERVAL_SECONDS =
        "TTL_ADD_PARTS_WARNING_SCAN_INTERVAL_SECONDS";

    /**
     * Label if only warning for the last part of ttl table arc by parts
     */
    public static final String TTL_ONLY_WARNING_FOR_THE_LAST_PART = "TTL_ONLY_WARNING_FOR_THE_LAST_PART";

    /**
     * Label if force using archive type of columnar_options for arc cci, default is false, inst level params
     */
    public static final String TTL_ARC_CCI_FORCE_USING_ARCHIVE_TYPE = "TTL_ARC_CCI_FORCE_USING_ARCHIVE_TYPE";
    /**
     * The default charset of trans conn of ttl-job when exec sql
     */
    public static final String TTL_DEFAULT_CHARSET_ON_CONN = "TTL_DEFAULT_CHARSET_ON_CONN";

    /**
     * The default sql mode of trans conn of ttl-job when exec sql
     */
    public static final String TTL_DEFAULT_SQL_MODE_ON_CONN = "TTL_DEFAULT_SQL_MODE_ON_CONN";

    /**
     * The parallelism of alter table ttl_tbl optimize partitions xxx
     */
    public static final String TTL_OPTIMIZE_TABLE_PARALLELISM = "TTL_OPTIMIZE_TABLE_PARALLELISM";

    /**
     * The current datetime value of debug, using for testcases
     */
    public static final String TTL_DEBUG_CURRENT_DATETIME = "TTL_DEBUG_CURRENT_DATETIME";

    /**
     * label of forbid drop ttl-defined table with archive table cci
     */
    public static final String TTL_FORBID_DROP_TTL_TBL_WITH_ARC_CCI = "TTL_FORBID_DROP_TTL_TBL_WITH_ARC_CCI";

    /**
     * Label if need mark the drop partition as archive cleanup for cdc
     */
    public static final String TTL_MARK_DROP_PARTITION_AS_ARCHIVE_CLEANUP_FOR_CDC =
        "TTL_MARK_DROP_PARTITION_AS_ARCHIVE_CLEANUP_FOR_CDC";

    /**
     * Label if need validate the encoder/decoder of ttl_col, default is true
     */
    public static final String ENABLE_TTL_COL_ENCODER_DECODER_VALIDATION =
        "ENABLE_TTL_COL_ENCODER_DECODER_VALIDATION";

    /**
     * The value used by the validation the encoder/decoder of ttl_col, default is '1970-01-01 00:00:00'
     */
    public static final String TTL_COL_ENCODER_DECODER_VALIDATION_VALUE =
        "TTL_COL_ENCODER_DECODER_VALIDATION_VALUE";

    public static final String REBUILD_TABLE_KEEP_FILTER =
        "REBUILD_TABLE_KEEP_FILTER";

    /**
     * Allow REBUILD CLEANUP to rebuild the primary table and all published GSIs.
     */
    public static final String FORCE_REBUILD_CLEANUP_WITH_GSI =
        "FORCE_REBUILD_CLEANUP_WITH_GSI";

    /**
     * Skip the CDC mark task for REBUILD CLEANUP when CDC has not been upgraded to support the syntax.
     */
    public static final String REBUILD_CLEANUP_SKIP_CDC_TASK =
        "REBUILD_CLEANUP_SKIP_CDC_TASK";

    /**
     * The value labeled if ttl-job allowed cleanup data
     */
    public static final String TTL_STOP_CLEANUP_DATA = "TTL_STOP_CLEANUP_DATA";

    /**
     * The value labeled if ttl-job skip preparing cleanup interval, like fetch min val of ttl_col
     */
    public static final String TTL_SKIP_PREPARING_CLEANUP_INTERVAL = "TTL_SKIP_PREPARING_CLEANUP_INTERVAL";

    /**
     *
     * ===========================
     * The following Config Params for polardbx-stmt-level
     * ===========================
     */

    /**
     * Label if allowed force drop the cci of the archive table
     */
    public static final String TTL_FORCE_DROP_ARCHIVE_CCI = "TTL_FORCE_DROP_ARCHIVE_CCI";

    /**
     * Label if allowed force drop the view of the archive table view of cci
     */
    public static final String TTL_FORCE_DROP_ARCHIVE_CCI_VIEW = "TTL_FORCE_DROP_ARCHIVE_CCI_VIEW";

    /**
     * auto hide ttl definition in Show create table
     */
    public static final String HIDE_TTL_DEFINITION_IN_SHOW_CREATE_TABLE = "HIDE_TTL_DEFINITION_IN_SHOW_CREATE_TABLE";

    /**
     * Label if only allow cleaning up not-null values rows of ttl_col, default is true
     */
    public static final String TTL_ONLY_CLEANUP_NOT_NULL_ROWS = "TTL_ONLY_CLEANUP_NOT_NULL_ROWS";

    /**
     * The base datetime used by normalizer, default is '1970-01-01 00:00:00'
     */
    public static final String TTL_NORMALIZER_BASE_DATETIME = "TTL_NORMALIZER_BASE_DATETIME";

    /**
     * Label if use rebuild physical tables to finish cleaning up expired data
     */
    public static final String TTL_FORCE_USE_REBUILD_POLICY_FOR_CLEANUP_EXPIRED_DATA =
        "TTL_FORCE_USE_REBUILD_POLICY_FOR_CLEANUP_EXPIRED_DATA";

    /**
     * The skip of split partition for rebuild table
     */
    public static final String TTL_SKIP_SPLIT_PARTITION_FOR_REBUILD_TABLE =
        "TTL_SKIP_SPLIT_PARTITION_FOR_REBUILD_TABLE";

    /**
     * Label if enable cleaning up expired data by omc rebuild policy
     */
    public static final String TTL_ENABLE_CLEANUP_EXPIRED_DATA_BY_REBUILD_POLICY =
        "TTL_ENABLE_CLEANUP_EXPIRED_DATA_BY_REBUILD_POLICY";

    /**
     * The expired data percent of the whole ttl table for auto using rebuild policy, unit: %
     */
    public static final String TTL_EXPIRED_DATA_PERCENT_FOR_AUTO_USING_REBUILD_POLICY =
        "TTL_EXPIRED_DATA_PERCENT_FOR_AUTO_USING_REBUILD_POLICY";

    /**
     * The min row count of the whole ttl table for auto using rebuild policy, default is 500000
     */
    public static final String TTL_MIN_ROW_COUNT_FOR_AUTO_USING_REBUILD_POLICY =
        "TTL_MIN_ROW_COUNT_FOR_AUTO_USING_REBUILD_POLICY";

    /**
     * ================ Param keys for ttl job end ================
     */

    public static final String ENABLE_PARAM_TYPE_CHANGE = "ENABLE_PARAM_TYPE_CHANGE";

    public static final String COLUMNAR_CLUSTER_MAXIMUM_QPS = "COLUMNAR_CLUSTER_MAXIMUM_QPS";

    public static final String COLUMNAR_CLUSTER_MAXIMUM_CONCURRENCY = "COLUMNAR_CLUSTER_MAXIMUM_CONCURRENCY";

    public static final String COLUMNAR_QPS_WINDOW_PERIOD = "COLUMNAR_QPS_WINDOW_PERIOD";

    /**
     * All write trx will start a standard 2PC TSO transaction, even in auto-commit mode.
     */
    public static final String ENABLE_EXTERNAL_CONSISTENCY_FOR_WRITE_TRX = "ENABLE_EXTERNAL_CONSISTENCY_FOR_WRITE_TRX";
    public static final String ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT = "ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT";
    public static final String ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL = "ENABLE_CLOSE_CONNECTION_WHEN_TRX_FATAL";
    public static final String ENABLE_TRX_FATAL_ON_ANY_ERROR = "ENABLE_TRX_FATAL_ON_ANY_ERROR";
    public static final String CCI_INCREMENTAL_CHECK_PARALLELISM = "CCI_INCREMENTAL_CHECK_PARALLELISM";
    public static final String CCI_INCREMENTAL_CHECK_BATCH_SIZE = "CCI_INCREMENTAL_CHECK_BATCH_SIZE";
    public static final String ENABLE_COLUMNAR_DEBUG = "ENABLE_COLUMNAR_DEBUG";
    public static final String ENABLE_COLUMNAR_SNAPSHOT_AUTO_POSITION = "ENABLE_COLUMNAR_SNAPSHOT_AUTO_POSITION";
    public static final String ENABLE_COLUMNAR_READ_INSTANCE_AUTO_GENERATE_SNAPSHOT =
        "ENABLE_COLUMNAR_READ_INSTANCE_AUTO_GENERATE_SNAPSHOT";

    public static final String ENABLE_READ_DELTA_FROM_COLUMNAR = "ENABLE_READ_DELTA_FROM_COLUMNAR";

    public static final String MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS = "MPP_QUERY_RESULT_MAX_WAIT_IN_MILLIS";

    public static final String ENABLE_MPP_SERIALIZED_CHUNK_COMPRESSION = "ENABLE_MPP_SERIALIZED_CHUNK_COMPRESSION";

    public static final String WAIT_FOR_COLUMNAR_COMMIT_MS = "WAIT_FOR_COLUMNAR_COMMIT_MS";

    public static final String COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES = "COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES";

    public static final String COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT = "COLUMNAR_SNAPSHOT_SPILL_MEMORY_LIMIT";

    public static final String ENABLE_PARALLEL_TOP_N = "ENABLE_PARALLEL_TOP_N";

    public static final String CN_DIV_PRECISION_INCREMENT = "CN_DIV_PRECISION_INCREMENT";

    public static final String ENABLE_DRDS_TYPE_SYSTEM = "ENABLE_DRDS_TYPE_SYSTEM";
    public static final String DISABLE_PARTITION_BEFORE_DROP = "DISABLE_PARTITION_BEFORE_DROP";

    public static final String STATISTIC_PARALLELISM = "STATISTIC_PARALLELISM";

    public static final String ENABLE_TSO_OPT = "ENABLE_TSO_OPT";

    public static final String DN_HLL_STATISTIC_PARALLELISM = "DN_HLL_STATISTIC_PARALLELISM";

    public static final String DDL_JOB_ID = "DDL_JOB_ID";

    public static final String DDL_PLAN_ID = "DDL_PLAN_ID";

    public static final String BACKFILL_ASYNC_LOG = "BACKFILL_ASYNC_LOG";

    public static final String OMC_THREAD_POOL_SIZE = "OMC_THREAD_POOL_SIZE";

    public static final String OMC_CHECKER_THREAD_POOL_SIZE = "OMC_CHECKER_THREAD_POOL_SIZE";

    public static final String CONSTANT_FOLD_BLACKLIST = "CONSTANT_FOLD_BLACKLIST";
    public static final String CONSTANT_FOLD_WHITELIST = "CONSTANT_FOLD_WHITELIST";

    public static final String ENABLE_PARALLEL_SORT_WINDOW = "ENABLE_PARALLEL_SORT_WINDOW";

    public static final String ENABLE_PUSH_TOPN_AND_AGG = "ENABLE_PUSH_TOPN_AND_AGG";

    public static final String ENABLE_SQL_AUDIT = "ENABLE_SQL_AUDIT";

    public static final String WAIT_FOR_NOT_FULL_MS = "WAIT_FOR_NOT_FULL_MS";

    public static final String WAIT_FOR_NOT_EMPTY_MS = "WAIT_FOR_NOT_EMPTY_MS";

    public static final String WAIT_FOR_EXCHANGE_CLIENT_MS = "WAIT_FOR_EXCHANGE_CLIENT_MS";

    public static final String ENABLE_SHUFFLE_JOIN = "ENABLE_SHUFFLE_JOIN";

    public static final String ENABLE_EARLY_STOP_TOP_K = "ENABLE_EARLY_STOP_TOP_K";

    public static final String MPP_WAIT_QUERY_INFO_TIME_IN_MILLIS = "MPP_WAIT_QUERY_INFO_TIME_IN_MILLIS";

    public static final String ENABLE_PARALLEL_PREHEAT_FILE_META = "ENABLE_PARALLEL_PREHEAT_FILE_META";
    public static final String ENABLE_WARMUP_SCHEDULE = "ENABLE_WARMUP_SCHEDULE";

    public static final String ENABLE_IN_VALUE_LIST_REWRITE = "ENABLE_IN_VALUE_LIST_REWRITE";

    public static final String FULL_SCAN_TABLE_BLACK_LIST = "FULL_SCAN_TABLE_BLACK_LIST";

    /*
     * ENABLE_RECYCLEBIN is used to control logical table dropping
     * ENABLE_PHY_RECYCLEBIN is used t0 control physical table dropping
     * */
    public static final String ENABLE_PHY_RECYCLEBIN = "ENABLE_PHY_RECYCLEBIN";
    public static final String MAX_PHY_RECYCLEBIN_RETENTION_MINUTES = "MAX_PHY_RECYCLEBIN_RETENTION_MINUTES";
    public static final String PURGE_PHY_RECYCLEBIN_CRON_EXPR = "PURGE_PHY_RECYCLEBIN_CRON_EXPR";
    public static final String PURGE_PHY_RECYCLEBIN_MAINTENANCE_ENABLE = "PURGE_PHY_RECYCLEBIN_MAINTENANCE_ENABLE";
    public static final String PURGE_PHY_RECYCLEBIN_MAINTENANCE_TIME_START =
        "PURGE_PHY_RECYCLEBIN_MAINTENANCE_TIME_START";
    public static final String PURGE_PHY_RECYCLEBIN_MAINTENANCE_TIME_END = "PURGE_PHY_RECYCLEBIN_MAINTENANCE_TIME_END";

    public static final String SQL_AUDIT_RULE = "SQL_AUDIT_RULE";

    public static final String ENABLE_DEEP_PAGE_OPTIMIZER = "ENABLE_DEEP_PAGE_OPTIMIZER";

    public static final String DEFAULT_COLLATION_FOR_UTF8MB4 = "DEFAULT_COLLATION_FOR_UTF8MB4";

    /**
     * if enable push down auto increment ,auto increment columns will be local auto incremented by each dn
     */
    public static final String ENABLE_PUSH_DOWN_AUTO_INCREMENT = "ENABLE_PUSH_DOWN_AUTO_INCREMENT";

    public static final String ENABLE_NEW_EXPLAIN_EXECUTE = "ENABLE_NEW_EXPLAIN_EXECUTE";

    public static final String EXPLAIN_EXECUTE_PHYTB_LEVEL = "EXPLAIN_EXECUTE_PHYTB_LEVEL";

    /**
     * Label if force ignore bad-value type cast status during routing one tuple, default is true
     */
    public static final String ROUTE_TUPLE_IGNORE_BAD_VALUE_TYPE_CAST = "ROUTE_TUPLE_IGNORE_BAD_VALUE_TYPE_CAST";

    /**
     * Label if need perform pre-check for the query value by using udf_func(such dble func), default is false
     */
    public static final String ROUTE_TUPLE_USE_PRECHECK_BY_UDF_FUNC = "ROUTE_TUPLE_USE_PRECHECK_BY_UDF_FUNC";

    /**
     * Label if need auto check the partitionCount of (sub)partitionBy match the routing algorithm of dble_hash like dble/date, default is true
     */
    public static final String AUTO_CHECK_PARTITION_COUNT_IF_MATCH_DBLE_HASH =
        "AUTO_CHECK_PARTITION_COUNT_IF_MATCH_DBLE_HASH";

    /**
     * Label if the last partition of udf_hash use the catch-all bound value during bound value generation, only supported global level, default is true
     */
    public static final String LAST_UDF_HASH_PARTITION_USE_CATCH_ALL_BOUND_VALUE =
        "LAST_UDF_HASH_PARTITION_USE_CATCH_ALL_BOUND_VALUE";

    /**
     * Label if need do datanode checking for dble routing, only supported global level , default is true
     * <pre>
     *     in dble routing, its routing result is the index of datanode,
     *     so the valid datanode index should be 0<= datanode < partitionCount
     *     ,which partitionCount is the partitions number of (sub)partitionBy
     * </pre>
     */
    public static final String ENABLE_DBLE_CHECK_DATANODE_INDEX_ROUTING = "ENABLE_DBLE_CHECK_DATANODE_INDEX_ROUTING";

    /**
     * Label if part_route func ignore any exception, default is false
     */
    public static final String PART_ROUTE_IGNORE_EXCEPTION = "PART_ROUTE_IGNORE_EXCEPTION";

    /**
     * Lable if enable dble routeResult(nodeIndex) value check
     */
    public static final String ENABLE_DBLE_ROUTE_RESULT_CHECK = "ENABLE_DBLE_ROUTE_RESULT_CHECK";

    /**
     * Label if enable push replicas tables join base on common group
     */
    public static final String ENABLE_PUSH_REPLICAS_TABLES_JOIN_BASE_ON_COMMON_GROUP =
        "ENABLE_PUSH_REPLICAS_TABLES_JOIN_BASE_ON_COMMON_GROUP";

    /**
     * The table routing meta used by check table routing cmd
     */
    public static final String CHECK_ROUTING_TABLE_META = "CHECK_ROUTING_TABLE_META";

    /**
     * Force treat remove partitioning as single, default is true
     */
    public static final String FORCE_REMOVE_PARTITIONING_AS_SINGLE = "FORCE_REMOVE_PARTITIONING_AS_SINGLE";

    public static final String ENABLE_ZONE_MAP_PRUNE = "ENABLE_ZONE_MAP_PRUNE";

    public static final String OSS_TRANSFER_POOL_SIZE = "OSS_TRANSFER_POOL_SIZE";

    public static final String EXPLAIN_EXECUTE_PHYTB_PATTERN = "EXPLAIN_EXECUTE_PHYTB_PATTERN";

    public static final String IO_STATUS_BOUND_SIZE = "IO_STATUS_BOUND_SIZE";

    public static final String IO_STATUS_IS_FULL_MAX_WAIT_MS = "IO_STATUS_IS_FULL_MAX_WAIT_MS";

    public static final String DRIVER_MEMORY_ADJUST_FREQUENCY = "DRIVER_MEMORY_ADJUST_FREQUENCY";

    public static final String ENABLE_QUERY_MEMORY_TRACKER = "ENABLE_QUERY_MEMORY_TRACKER";

    public static final String OPERATOR_MEMORY_PAGE_SIZE = "OPERATOR_MEMORY_PAGE_SIZE";

    public static final String DRIVER_MEMORY_PAGE_SIZE = "DRIVER_MEMORY_PAGE_SIZE";

    public static final String PIPELINE_MEMORY_PAGE_SIZE = "PIPELINE_MEMORY_PAGE_SIZE";

    public static final String QUERY_MEMORY_PAGE_SIZE = "QUERY_MEMORY_PAGE_SIZE";

    public static final String TOTAL_QUERY_MEMORY_QUATO_RATIO = "TOTAL_QUERY_MEMORY_QUATO_RATIO";

    public static final String FORCE_CHANGE_ROLE = "FORCE_CHANGE_ROLE";

    public static final String META_DB_PROPS = "META_DB_PROPS";

    /**
     * Repartitioning is supported for DBLE table if this option is enabled.
     */
    public static final String ENABLE_DBLE_TABLE_REPARTITION = "ENABLE_DBLE_TABLE_REPARTITION";

    /**
     * if enable create table without partition definition in dble db
     */
    public static final String ENABLE_DBLE_WITHOUT_PARTITION_DEF = "ENABLE_DBLE_WITHOUT_PARTITION_DEF";

    /**
     * Label if enable the partition hint pruning in PostPlanner
     */
    public static final String ENABLE_POST_PLANNER_PARTITION_HINT_PRUNING =
        "ENABLE_POST_PLANNER_PARTITION_HINT_PRUNING";

    /**
     * Label if ignore the invalid exception during getting topology in planner/postplaner, only used for unit-test of optimizer test
     */
    public static final String IGNORE_INVALID_TOPOLOGY_IN_POST_PLANNER = "IGNORE_INVALID_TOPOLOGY_IN_POST_PLANNER";

    public static final String ENABLE_DML_FOR_NO_PARTITION_KEY_TABLE = "ENABLE_DML_FOR_NO_PARTITION_KEY_TABLE";

    /**
     * Label if enable java udf functions
     */
    public static final String ENABLE_JAVA_UDF = "ENABLE_JAVA_UDF";

    public static final String DRDS_TO_AUTO_DB_PARTITIONS_DEFAULT = "DRDS_TO_AUTO_DB_PARTITIONS_DEFAULT";

    public static final String EXECUTE_AFTER_DRDS_AUTO_MODE_CONVERSION = "EXECUTE_AFTER_DRDS_AUTO_MODE_CONVERSION";

    public static final String GROUP_SEQ_AS_DEFAULT = "GROUP_SEQ_AS_DEFAULT";

    public static final String ENABLE_COLUMNAR_SCAN_COST = "ENABLE_COLUMNAR_SCAN_COST";

    public static final String CCI_ADVISOR_DEFAULT_PARTITIONS = "CCI_ADVISOR_DEFAULT_PARTITIONS";

    public static final String CCI_ADVISOR_FAST_ENUMERATION = "CCI_ADVISOR_FAST_ENUMERATION";

    public static final String CCI_ADVISOR_PREFER_PARTITION_WISE = "CCI_ADVISOR_PREFER_PARTITION_WISE";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_ENABLE = "ASYNC_LOAD_GDN_DDL_SQL_ENABLE";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_STATUS = "ASYNC_LOAD_GDN_DDL_SQL_STATUS";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_INTERVAL_MS = "ASYNC_LOAD_GDN_DDL_SQL_INTERVAL_MS";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_AUTO_INIT_CHECKPOINT_ENABLE =
        "ASYNC_LOAD_GDN_DDL_SQL_AUTO_INIT_CHECKPOINT_ENABLE";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_BATCH_SIZE = "ASYNC_LOAD_GDN_DDL_SQL_BATCH_SIZE";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_ID = "ASYNC_LOAD_GDN_DDL_SQL_ID";

    public static final String ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META = "ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META";

    public static final String ASYNC_LOAD_GDN_DDL_INJECT_DUPLICATE_TROUBLE_ENABLE =
        "ASYNC_LOAD_GDN_DDL_INJECT_DUPLICATE_TROUBLE_ENABLE";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS =
        "ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_WAIT_ALIGN_TIMEOUT_SECONDS =
        "ASYNC_LOAD_GDN_DDL_SQL_WAIT_ALIGN_TIMEOUT_SECONDS";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE =
        "ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE";

    public static final String ASYNC_LOAD_GDN_DDL_SQL_CONNECTION_INIT_SQLS =
        "ASYNC_LOAD_GDN_DDL_SQL_CONNECTION_INIT_SQLS";

    public static final String CDC_DDL_MARK_WITH_DETAIL_META_ENABLE = "CDC_DDL_MARK_WITH_DETAIL_META_ENABLE";
    public static final String ENABLE_CHECK_TABLE_META_VERSION = "ENABLE_CHECK_TABLE_META_VERSION";

    public static final String CHECK_PRIVILEGE_IN_PREPARE_MODE = "CHECK_PRIVILEGE_IN_PREPARE_MODE";

    /**
     * filter ttl delete event in binlog dump, default false.
     */
    public static final String BINLOG_DUMP_ARCHIVE_IGNORE_ENABLED = "BINLOG_DUMP_ARCHIVE_IGNORE_ENABLED";

    /**
     * allow tables in binlog dump
     */
    public static final String BINLOG_DUMP_DO_TABLE = "BINLOG_DUMP_DO_TABLE";

    /**
     * if set this. the tables of BINLOG_DUMP_TABLE will be black name list.
     */
    public static final String BINLOG_DUMP_IGNORE_TABLE = "BINLOG_DUMP_IGNORE_TABLE";

    /**
     * filter rows query event in binlog dump, default false.
     */
    public static final String BINLOG_DUMP_ROWS_QUERY_IGNORE_ENABLED = "BINLOG_DUMP_ROWS_QUERY_IGNORE_ENABLED";

    /**
     * binlog dump filter by set event flag or just don't send event
     */
    public static final String BINLOG_DUMP_IGNORE_BY_SET_FLAG = "BINLOG_DUMP_IGNORE_BY_SET_FLAG";

    /**
     * config params for users one by one, for example:
     * set global BINLOG_DUMP_FILTER_USER_CONFIG = '{"user":{"binlog_dump_rows_query_ignore_enabled":"false"}}';
     */
    public static final String BINLOG_DUMP_FILTER_USER_CONFIG = "BINLOG_DUMP_FILTER_USER_CONFIG";

    public static final String BINLOG_GET_DUMPER_SOCKET_TIME_MILLISECOND = "BINLOG_GET_DUMPER_SOCKET_TIME_MILLISECOND";

    public static final String ENABLE_SHOW_CREATE_TABLE_FOR_EXPORT = "ENABLE_SHOW_CREATE_TABLE_FOR_EXPORT";

    public static final String ENABLE_USERNAME_PUSHDOWN = "ENABLE_USERNAME_PUSHDOWN";

    public static final String ENABLE_DRDS_REX_ROUTE = "ENABLE_DRDS_REX_ROUTE";

    public static final String ENABLE_DRDS_OPTIMIZE_REX_ROUTE = "ENABLE_DRDS_OPTIMIZE_REX_ROUTE";

    public static final String ENABLE_OUTPUT_STORAGE_LABEL = "ENABLE_OUTPUT_STORAGE_LABEL";

    public static final String LOGIN_ERROR_DEFAULT_MAX_COUNT = "LOGIN_ERROR_DEFAULT_MAX_COUNT";
    public static final String LOGIN_ERROR_DEFAULT_EXPIRE_SECONDS = "LOGIN_ERROR_DEFAULT_EXPIRE_SECONDS";

    public static final String WAIT_TIMEOUT = "WAIT_TIMEOUT";

    public static final String MAX_USER_CONNECTIONS = "MAX_USER_CONNECTIONS";
    public static final String ENABLE_TRANSPARENT_TTL = "ENABLE_TRANSPARENT_TTL";

    public static final String USE_REDUNDANT_META_DATA = "USE_REDUNDANT_META_DATA";
    public static final String USE_BINARY_META_DATA = "USE_BINARY_META_DATA";

    public static final String ENABLE_PREHEAT_MEMORY_PRECISE_COUNT = "ENABLE_PREHEAT_MEMORY_PRECISE_COUNT";
    public static final String ENABLE_DECIMAL_128 = "ENABLE_DECIMAL_128";
    public static final String ENABLE_INPLACE_BACKFILL = "ENABLE_INPLACE_BACKFILL";
    public static final String INPLACE_BACKFILL_BATCH_SIZE_MAX = "INPLACE_BACKFILL_BATCH_SIZE_MAX";
    public static final String INPLACE_BACKFILL_BEFORE_FIRST_CATCHUP_SUSPEND_DEBUG =
        "INPLACE_BACKFILL_BEFORE_FIRST_CATCHUP_SUSPEND_DEBUG";
    public static final String SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL = "SUPPORT_DATATYPE_FOR_INPLACE_BACKFILL";
    public static final String ENABLE_HASH_RANGE_CHECK = "ENABLE_HASH_RANGE_CHECK";

    public static final String ENABLE_MERGE_LIMIT_SORT = "ENABLE_MERGE_LIMIT_SORT";

    public static final String ENABLE_MULTI_TABLE_UPDATE_MODIFY_GSI_SHARDING_KEY =
        "ENABLE_MULTI_TABLE_UPDATE_MODIFY_GSI_SHARDING_KEY";

    public static final String ENABLE_CCL_DETECT = "ENABLE_CCL_DETECT";
    public static final String CCL_DETECT_INTERVAL = "CCL_DETECT_INTERVAL";

    public static final String CCL_DETECT_LEVEL = "CCL_DETECT_LEVEL";

    public static final String CCL_DETECT_CONNECTION_LIMIT = "CCL_DETECT_CONNECTION_LIMIT";

    public static final String CCL_DETECT_DN_DELAY_INTERVAL = "CCL_DETECT_DN_DELAY_INTERVAL";

    public static final String CCL_DETECT_KILL_BATCH = "CCL_DETECT_KILL_BATCH";

    public static final String CCL_DETECT_SLOW_THRESHOLD = "CCL_DETECT_SLOW_THRESHOLD";
    public static final String CCL_DETECT_MAX_THRESHOLD = "CCL_DETECT_MAX_THRESHOLD";

    public static final String CCL_DETECT_CONCURRENCY_THRESHOLD = "CCL_DETECT_CONCURRENCY_THRESHOLD";

    public static final String CCL_DETECT_KILL_MIN_CONCURRENCY = "CCL_DETECT_KILL_MIN_CONCURRENCY";

    public static final String CCL_DETECT_ROOT_COLUMN = "CCL_DETECT_ROOT_COLUMN";

    public static final String CCL_DETECT_DN_RULE_EXPIRE_TIME = "CCL_DETECT_DN_RULE_EXPIRE_TIME";
    public static final String CCL_DETECT_DRY_RUN = "CCL_DETECT_DRY_RUN";

    public static final String LIMIT_TDDL_LOG_SQL_PARAMS_LENGTH = "LIMIT_TDDL_LOG_SQL_PARAMS_LENGTH";

    //just for testcase, please don't set this param in production
    public static final String PHYSICAL_BACKFILL_TASK_INJECT_FAIL_TIME =
        "PHYSICAL_BACKFILL_TASK_INJECT_FAIL_TIME";

    //just for testcase, please don't set this param in production
    public static final String FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP =
        "FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP";

    public static final String ENABLE_MOVE_PARTITIONGROUP_CONCURRENTLY =
        "ENABLE_MOVE_PARTITIONGROUP_CONCURRENTLY";

    public static final String ALLOW_USING_TIMESTAMP_IN_RANGE_LIST_PARTITION =
        "ALLOW_USING_TIMESTAMP_IN_RANGE_LIST_PARTITION";

    public static final String ALLOW_USING_SPECIFY_PARTITIONS_ON_SINGLE_TABLE =
        "ALLOW_USING_SPECIFY_PARTITIONS_ON_SINGLE_TABLE";

    public static final String ENABLE_COLLECT_CARDINALITY_FROM_DN = "ENABLE_COLLECT_CARDINALITY_FROM_DN";

    public static final String ENABLE_COLLECT_CARDINALITY_FROM_DN_FOR_GSI =
        "ENABLE_COLLECT_CARDINALITY_FROM_DN_FOR_GSI";

    public static final String TTL_SCHEDULED_JOB_USE_DEBUG_FIRE_TIME = "TTL_SCHEDULED_JOB_USE_DEBUG_FIRE_TIME";

    public static final String ENABLE_RANDOM_PARTITION_PLACEMENT = "ENABLE_RANDOM_PARTITION_PLACEMENT";

    /**
     * Label if show partitions in logicalview of all autodb-tables for show create table
     * <pre>
     *     only used for test cases
     * </pre>
     */
    public static final String SHOW_PARTITIONS_IN_LOGICALVIEW_FOR_SHOW_CREATE_TABLE =
        "SHOW_PARTITIONS_IN_LOGICALVIEW_FOR_SHOW_CREATE_TABLE";

    public static final String TTL_ONLY_SCHEDULED_WARNING_SCANNER_TASK = "TTL_ONLY_SCHEDULED_WARNING_SCANNER_TASK";

    /**
     * The delay time in seconds after the ttl definition is modified, default is 24*3600 , unit: second
     */
    public static final String TTL_WARNING_DELAY_SECONDS_AFTER_TTL_MODIFIED =
        "TTL_WARNING_DELAY_SECONDS_AFTER_TTL_MODIFIED";

    /**
     * The debug time of ttl definition is modified which used to run cases only
     */
    public static final String TTL_DEBUG_WARNING_SCAN_TTLINFO_MODIFIED_TIME =
        "TTL_DEBUG_WARNING_SCAN_TTLINFO_MODIFIED_TIME";

    /**
     * The debug target table schema which used to run cases only for ttl warning scanner task
     */
    public static final String TTL_DEBUG_WARNING_SCAN_TARGET_SCHEMA = "TTL_DEBUG_WARNING_SCAN_TARGET_SCHEMA";

    /**
     * The debug target table name which used to run cases only for ttl warning scanner task
     */
    public static final String TTL_DEBUG_WARNING_SCAN_TARGET_TABLE = "TTL_DEBUG_WARNING_SCAN_TARGET_TABLE";

    public static final String TTL_QUERY_TYPE = "TTL_QUERY_TYPE";

    public static final String ENABLE_TTL_HYBRID_SCHEDULE = "ENABLE_TTL_HYBRID_SCHEDULE";

    public static final String FORCE_TTL_COL_NOT_NULL = "FORCE_TTL_COL_NOT_NULL";

    public static final String FORCE_TTL_TRANSPARENT_QUERY_WORKLOAD = "FORCE_TTL_TRANSPARENT_QUERY_WORKLOAD";

    public static final String IGNORE_TTL_COL_NULLABLE = "IGNORE_TTL_COL_NULLABLE";

    public static final String ALLOW_TTL_HYBRID_SCHEDULE_WITHOUT_COLUMNAR_NODE =
        "ALLOW_TTL_HYBRID_SCHEDULE_WITHOUT_COLUMNAR_NODE";

    public static final String TTL_HYBRID_AUTO_CREATE_ARCHIVE_CCI = "TTL_HYBRID_AUTO_CREATE_ARCHIVE_CCI";

    public static final String TTL_ENABLE_PLAN_PRUNER = "TTL_ENABLE_PLAN_PRUNER";
    public static final String ENCDB_ENABLE_KMS_MODE = "ENCDB_ENABLE_KMS_MODE";

    public static final String ENCJDBC_KMS_MIN_VERSION = "ENCJDBC_KMS_MIN_VERSION";

    public static final String ENCDB_ENABLE_RANDOM_MEK = "ENCDB_ENABLE_RANDOM_MEK";

    /**
     * Reload DDL parameter
     */
    public static final String RELOAD_DDL_PARAMETER = "RELOAD_DDL_PARAMETER";

    public static final String COLUMNAR_SCAN_MAXIMUM_MEMORY_PERMITS_RATIO =
        "COLUMNAR_SCAN_MAXIMUM_MEMORY_PERMITS_RATIO";

    public static final String COLUMNAR_SCAN_GRANULARITY_REDUCTION_SCALE = "COLUMNAR_SCAN_GRANULARITY_REDUCTION_SCALE";

    public static final String COLUMNAR_SCAN_THREAD_LIMIT_REDUCTION_FACTOR =
        "COLUMNAR_SCAN_THREAD_LIMIT_REDUCTION_FACTOR";

    public static final String ADAPTIVE_COLUMNAR_SCAN_MONITOR_MAXIMUM_SIZE =
        "ADAPTIVE_COLUMNAR_SCAN_MONITOR_MAXIMUM_SIZE";

    public static final String COLUMNAR_SCAN_MAX_BUFFER_SIZE = "COLUMNAR_SCAN_MAX_BUFFER_SIZE";

    public static final String COLUMNAR_SCAN_MAX_DISK_RANGE_SIZE = "COLUMNAR_SCAN_MAX_DISK_RANGE_SIZE";

    public static final String ENABLE_MOCK_CONNECTOR = "ENABLE_MOCK_CONNECTOR";

    /**
     * Enable natural language Agent. When enabled, unrecognized input (SQL parse failure)
     * from users with NL2SQL privilege will be sent to LLM Agent for processing.
     */
    public static final String ENABLE_NL2SQL = "ENABLE_NL2SQL";

    /**
     * Model name for NL2SQL Agent. Empty string means use the default LLM model.
     * The model must use OpenAI-compatible endpoint for function calling support.
     */
    public static final String NL2SQL_MODEL_NAME = "NL2SQL_MODEL_NAME";

    /**
     * Maximum conversation history turns to keep in Agent session.
     */
    public static final String NL2SQL_MAX_HISTORY = "NL2SQL_MAX_HISTORY";

    /**
     * Maximum iterations (tool call rounds) per Agent request.
     */
    public static final String NL2SQL_MAX_ITERATIONS = "NL2SQL_MAX_ITERATIONS";

    /**
     * Allowed SQL types for NL Agent execution.
     * Values: "READ_ONLY" (default), "READ_WRITE", "ALL"
     */
    public static final String NL2SQL_ALLOWED_SQL_TYPES = "NL2SQL_ALLOWED_SQL_TYPES";

    /**
     * Maximum execution time (ms) for a single SQL executed by NL2SQL Agent.
     * Queries exceeding this limit will be killed automatically.
     */
    public static final String NL2SQL_QUERY_TIMEOUT_MS = "NL2SQL_QUERY_TIMEOUT_MS";

    /**
     * Maximum estimated scan rows for NL2SQL Agent SELECT queries (EXPLAIN pre-check).
     * SELECT queries with estimated scan exceeding this limit will be rejected before execution.
     */
    public static final String NL2SQL_MAX_SCAN_ROWS = "NL2SQL_MAX_SCAN_ROWS";

    /**
     * Whether NL2SQL Agent query_sql tool routes read queries to master.
     * true (default): always use master for reads (avoids replica-unavailable errors).
     * false: use LOW_DELAY_SLAVE_ONLY for reads (production with healthy replicas).
     */
    public static final String NL2SQL_READ_USE_MASTER = "NL2SQL_READ_USE_MASTER";

    /**
     * Whether to show intermediate steps (SQL actions, results, plans) in NL2SQL Agent output.
     * true (default): show all intermediate steps as separate result sets.
     * false: only return the final answer result set, suppressing SQL_ACTION, SQL_RESULT, and PLAN.
     */
    public static final String NL2SQL_SHOW_STEPS = "NL2SQL_SHOW_STEPS";

    /**
     * Maximum context tokens for NL2SQL Agent LLM requests.
     * When estimated total tokens exceed 80% of this limit, the agent will
     * compress historical messages into a summary to avoid exceeding the
     * model's context window.
     * Can be overridden per-model via model_params.context_length.
     */
    public static final String NL2SQL_MAX_CONTEXT_TOKENS = "NL2SQL_MAX_CONTEXT_TOKENS";

    /**
     * Enable fix for stale SchemaConfig caused by missed DROP DATABASE sync between CN nodes.
     * When true: apply fix logic (force unload stale SchemaConfig + master-mode fallback sweep).
     * When false: keep original behavior and print diagnostic warn logs only.
     */
    public static final String ENABLE_FIX_STALE_SCHEMA_CONFIG = "ENABLE_FIX_STALE_SCHEMA_CONFIG";

    /**
     * Whether to count transaction control statements (BEGIN/COMMIT/ROLLBACK) in QPS statistics.
     * Default: false (off), to maintain backward compatibility of QPS metrics.
     */
    public static final String ENABLE_TRANSACTION_QPS_COUNT = "ENABLE_TRANSACTION_QPS_COUNT";

    /**
     * Enable/disable the active purge in PurgeTsoTimerTask.
     * When set to false, the timer task keeps running but skips the actual purge logic.
     * Default: true.
     */
    public static final String ENABLE_TSO_PURGE_TASK = "ENABLE_TSO_PURGE_TASK";
}

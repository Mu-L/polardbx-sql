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

package com.alibaba.polardbx.executor.utils.failpoint;

/**
 * fail point injection key
 * use set command to enable specific FailPoint
 */
public interface FailPointKey {

    /**
     * set @FP_ASSERT='true';
     * 注入assert
     * 默认开启，default=true
     */
    String FP_ASSERT = "FP_ASSERT";

    /**
     * set @FP_RANDOM_FAIL='30';
     * 注入随机概率异常
     */
    String FP_RANDOM_FAIL = "FP_RANDOM_FAIL";

    /**
     * set @FP_RANDOM_SUSPEND='30,3000';
     * 注入随机概率停顿，单位是ms
     */
    String FP_RANDOM_SUSPEND = "FP_RANDOM_SUSPEND";

    /**
     * set @FP_RANDOM_HANG='30';
     * 注入随机概率死循环
     */
    String FP_RANDOM_HANG = "FP_RANDOM_HANG";

    /**
     * set @FP_RANDOM_CRASH='30';
     * 注入随机概率crash
     */
    String FP_RANDOM_CRASH = "FP_RANDOM_CRASH";

    String FP_OVERRIDE_NOW = "FP_OVERRIDE_NOW";

    String FP_FORBID_REUSE_WRITE_CONNECTION = "FP_FORBID_REUSE_WRITE_CONNECTION";

    /**
     * set @FP_RANDOM_PHYSICAL_DDL_EXCEPTION='30';
     * inject exception during executing physical DDL
     * 执行物理DDL时随机失败，可指定失败概率
     */
    String FP_RANDOM_PHYSICAL_DDL_EXCEPTION = "FP_RANDOM_PHYSICAL_DDL_EXCEPTION";

    /**
     * set @FP_BEFORE_PHYSICAL_DDL_EXCEPTION='true';
     * 执行物理DDL之前全部分片失败
     */
    String FP_BEFORE_PHYSICAL_DDL_EXCEPTION = "FP_BEFORE_PHYSICAL_DDL_EXCEPTION";

    /**
     * set @FP_AFTER_PHYSICAL_DDL_EXCEPTION='true';
     * 执行物理DDL之后全部分片失败
     */
    String FP_AFTER_PHYSICAL_DDL_EXCEPTION = "FP_AFTER_PHYSICAL_DDL_EXCEPTION";

    /**
     * set @FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION='true';
     * 执行物理DDL之前部分分片失败
     */
    String FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION = "FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION";

    /**
     * set @FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION='true';
     * 执行物理DDL之后部分分片失败
     */
    String FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION = "FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION";

    /**
     * set @FP_PHYSICAL_DDL_INTERRUPTED='true';
     * 执行物理DDL期间中断
     */
    String FP_PHYSICAL_DDL_INTERRUPTED = "FP_PHYSICAL_DDL_INTERRUPTED";

    /**
     * set @FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION='true';
     * 创建New Seq的底层Seq后立即失败
     */
    String FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION = "FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION";

    /**
     * set @FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION='tableName';
     * 指定表执行 DROP TABLE 物理 DDL 异常
     */
    String FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION = "FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION";

    /**
     * set @FP_PHYSICAL_DDL_TIMEOUT='1000';
     * 执行物理DDL时超时（毫秒）
     */
    String FP_PHYSICAL_DDL_TIMEOUT = "FP_PHYSICAL_DDL_TIMEOUT";

    /**
     * changeset catchup task sleep time (ms)
     */
    String FP_CATCHUP_TASK_SUSPEND = "FP_CATCHUP_TASK_SUSPEND";

    /**
     * set @FP_RANDOM_BACKFILL_EXCEPTION='30';
     * Backfill时随机失败，可指定失败概率
     */
    String FP_RANDOM_BACKFILL_EXCEPTION = "FP_RANDOM_BACKFILL_EXCEPTION";

    /**
     * set @FP_FAIL_ON_DDL_TASK_NAME='GsiInsertMetaTask';
     * 根据taskName注入异常
     */
    String FP_FAIL_ON_DDL_TASK_NAME = "FP_FAIL_ON_DDL_TASK_NAME";

    /**
     * set @FP_PAUSE_AFTER_DDL_TASK_EXECUTION='GsiInsertMetaTask';
     * 根据taskName注入异常
     */
    String FP_PAUSE_AFTER_DDL_TASK_EXECUTION = "FP_PAUSE_AFTER_DDL_TASK_EXECUTION";

    /**
     * set @FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION='FP_FAIL_AFTER_DDL_TASK_EXECUTION';
     * 根据taskName注入异常
     */
    String FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION = "FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION";

    /**
     * set @FP_HIJACK_DDL_JOB='15,6,30';
     * 劫持所有的JOB, 替换为MockDdlJob
     */
    String FP_HIJACK_DDL_JOB = "FP_HIJACK_DDL_JOB";

    /**
     * set @FP_HIJACK_DDL_JOB_FORMAT='RANDOM';
     * set @FP_HIJACK_DDL_JOB_FORMAT='SEQUELTIAL';
     */
    String FP_HIJACK_DDL_JOB_FORMAT = "FP_HIJACK_DDL_JOB_FORMAT";

    /**
     * set @FP_INJECT_SUBJOB='true';
     * Inject subjob into MockDdl
     */
    String FP_INJECT_SUBJOB = "FP_INJECT_SUBJOB";

    /**
     * set @FP_PAUSE_DDL_JOB_ONCE_CREATED='true';
     * 创建完DDL之后立刻暂停
     */
    String FP_PAUSE_DDL_JOB_ONCE_CREATED = "FP_PAUSE_DDL_JOB_ONCE_CREATED";

    /**
     * set @FP_TRUNCATE_CUTOVER_FAIL='true';
     * 在 Truncate Table with GSI 的 CutOver 时失败
     */
    String FP_TRUNCATE_CUTOVER_FAILED = "FP_TRUNCATE_CUTOVER_FAIL";

    /**
     * set @FP_TRUNCATE_SYNC_FAIL='true';
     * 在 Truncate Table with GSI 的 Sync 时失败
     */
    String FP_TRUNCATE_SYNC_FAILED = "FP_TRUNCATE_SYNC_FAIL";

    /**
     * set @FP_MOCK_TASK_RANDOM_FAIL='30';
     * 设置mock task有30%概率失败
     */
    String FP_MOCK_TASK_RANDOM_FAIL = "FP_MOCK_TASK_RANDOM_FAIL";

    /**
     * set @FP_MOCK_TASK_RANDOM_ROLLBACK_POLOCY='50';
     * 设置mock task有30%概率失败
     */
    String FP_MOCK_TASK_RANDOM_ROLLBACK_POLOCY = "FP_MOCK_TASK_RANDOM_ROLLBACK_POLOCY";

    /**
     * set @fp_ddl_internal_max_parallelism='5';
     * 设置DDL引擎最大并行度
     */
    String FP_DDL_INTERNAL_MAX_PARALLELISM = "FP_DDL_INTERNAL_MAX_PARALLELISM";

    /**
     * set @fp_ddl_restore_job_suspend='20000';
     * 设置DDL引擎反序列化Job时的延迟，单位是ms
     */
    String FP_DDL_RESTORE_JOB_SUSPEND = "FP_DDL_RESTORE_JOB_SUSPEND";

    /**
     * set @FP_EACH_DDL_TASK_FAIL_ONCE='true';
     * 让每个task失败1次。不影响BaseValidateTask。
     */
    String FP_EACH_DDL_TASK_FAIL_ONCE = "FP_EACH_DDL_TASK_FAIL_ONCE";

    /**
     * set @FP_EACH_DDL_TASK_EXECUTE_TWICE='true';
     * 让每个task执行2次
     */
    String FP_EACH_DDL_TASK_EXECUTE_TWICE = "FP_EACH_DDL_TASK_EXECUTE_TWICE";

    /**
     * set @FP_EACH_DDL_TASK_BACK_AND_FORTH='true';
     * 让每个Task先执行execute、再执行rollback、再执行execute
     */
    String FP_EACH_DDL_TASK_BACK_AND_FORTH = "FP_EACH_DDL_TASK_BACK_AND_FORTH";

    /**
     * set @FP_SKIP_TASK_EXECUTION_BY_NAMES='TaskName1,TaskName2,TaskName3';
     * 跳过task的执行
     */
    String FP_SKIP_TASK_EXECUTION_BY_NAMES = "FP_SKIP_TASK_EXECUTION_BY_NAMES";

    /**
     * set @FP_DDL_TASK_SUSPEND_WHEN_FAILED='5000';
     * 当task失败时，sleep一段时间。
     * 当JOB按照DAG图并发执行时，这个注入点可以制造task并发失败的场景。
     * 可以用于测试并发TASK失败时，引擎调度的正确性。
     * 期望：只有一个task能修改Job的失败策略
     */
    String FP_DDL_TASK_SUSPEND_WHEN_FAILED = "FP_DDL_TASK_SUSPEND_WHEN_FAILED";

    /**
     * set @FP_DDL_GSI_CHECK_FAILED='true';
     * 注入gsi check失败
     */
    String FP_DDL_GSI_CHECK_FAILED = "FP_DDL_GSI_CHECK_FAILED";

    /**
     * set @FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_BEFORE_DO='true';
     * 在老引擎的beforeDo()之前注入失败
     */
    String FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_BEFORE_DO = "FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_BEFORE_DO";

    /**
     * set @FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_DO_HANDLE='true';
     * 在老引擎的doHandle()之前注入失败
     */
    String FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_DO_HANDLE = "FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_DO_HANDLE";

    /**
     * set @FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_AFTER_DO='true';
     * 在老引擎的afterDo()之前注入失败
     */
    String FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_AFTER_DO = "FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_AFTER_DO";

    /**
     * set @FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP='true';
     * 在cdc系统库新增Group之后注入失败
     */
    String FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP = "FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP";

    /**
     * set @FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP='true';
     * 在cdc系统库新增Group之后注入失败
     */
    String FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP = "FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP";

    /**
     * set @FP_INJECT_INTERRUPTED_TO_SCHEDULE_JOB='true';
     */
    String FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB =
        "FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB";

    String FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL = "FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL";
    String FP_INJECT_IGNORE_SAMPLE_TASK_EXCEPTION = "FP_INJECT_IGNORE_SAMPLE_TASK_EXCEPTION";
    String FP_INJECT_IGNORE_STATISTIC_COLLECT_FROM_DN_EXCEPTION =
        "FP_INJECT_IGNORE_STATISTIC_COLLECT_FROM_DN_EXCEPTION";
    String FP_INJECT_IGNORE_STATISTIC_SAMPLE_SKETCH = "FP_INJECT_IGNORE_STATISTIC_SAMPLE_SKETCH";
    String FP_INJECT_IGNORE_HLL_TASK_EXCEPTION = "FP_INJECT_IGNORE_HLL_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_ROWCOUNT_TASK_EXCEPTION = "FP_INJECT_IGNORE_ROWCOUNT_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION = "FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_SYNC_TASK_EXCEPTION = "FP_INJECT_IGNORE_SYNC_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_PERSIST_TABLE_STATISTIC = "FP_INJECT_IGNORE_PERSIST_TABLE_STATISTIC";

    String FP_INJECT_IGNORE_PERSIST_COLUMN_STATISTIC = "FP_INJECT_IGNORE_PERSIST_COLUMN_STATISTIC";

    String FP_INJECT_IGNORE_PERSIST_NDV_STATISTIC = "FP_INJECT_IGNORE_PERSIST_NDV_STATISTIC";

    /**
     * set @FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION='true';
     * 注入统计信息定时采集任务的超时失败
     */
    String FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION =
        "FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION";

    /**
     * set @FP_INJECT_STATISTIC_HLL_ON_COLUMNAR_EXCEPTION='true';
     * 注入统计信息hll on cci的超时失败
     */
    String FP_INJECT_STATISTIC_HLL_ON_COLUMNAR_EXCEPTION =
        "FP_INJECT_STATISTIC_HLL_ON_COLUMNAR_EXCEPTION";

    /**
     * set @FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB='true';
     * 忽略 expire local partition 定时任务的中断
     */
    String FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB =
        "FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB";

    /**
     * set @FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION='true';
     * 忽略 local partition 子任务之间的中断
     */
    String FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION =
        "FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION";

    /**
     * set @FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR='true';
     * 向 expire local partition 定时任务注入中断
     */
    String FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR = "FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR";

    /**
     * set @FP_TTL_PAUSE='20'
     * 调整expire local partition的超时时间，单位是秒
     */
    String FP_TTL_PAUSE = "FP_TTL_PAUSE";

    /**
     * Fail before creating tmp tables at status 0.
     * No tmp table is created.
     */
    String FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP = "FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP";

    /**
     * Fail during creating tmp tables at status 0.
     * At least one DN finishes creating tmp table.
     * Status is still 0.
     */
    String FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP = "FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP";

    /**
     * Fail before switching tables at status 1.
     */
    String FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE = "FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE";

    /**
     * Fail during switching tables at status 1.
     * At least one DN finishes switching tables.
     * Status is still 1.
     */
    String FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE = "FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE";

    /**
     * Fail before dropping archive tables at status 2.
     */
    String FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE = "FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE";

    /**
     * Fail during switching tables at status 2.
     * At least one DN finishes dropping archive tables.
     * Status is still 2.
     */
    String FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE = "FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE";

    /**
     * Fail before table sync task.
     */
    String FP_FAILED_TABLE_SYNC = "FP_FAILED_TABLE_SYNC";

    /**
     * set @FP_SYNC_LOCAL_VIA_MANAGER='true';
     * Do not skip the local node in the cluster sync remote loop, so that the sync action is
     * also delivered to the local node through the real JDBC manager-port path.
     */
    String FP_SYNC_LOCAL_VIA_MANAGER = "FP_SYNC_LOCAL_VIA_MANAGER";

    /**
     * set @FP_GMS_SYNC_CONN_FAIL_TIMES='5';
     * Make the first N connection establishments of GmsSyncDataSource fail with a transient
     * SQLException, simulating network jitter between CN nodes.
     */
    String FP_GMS_SYNC_CONN_FAIL_TIMES = "FP_GMS_SYNC_CONN_FAIL_TIMES";

    /**
     * Fail in sync table_group task.
     */
    String FP_FAILED_TABLE_GROUP_SYNC = "FP_FAILED_TABLE_GROUP_SYNC";

    /**
     * Simulate the OptimizerContext-unavailable branch in TableGroupSyncAction.sync().
     */
    String FP_TABLE_GROUP_SYNC_CONTEXT_NULL = "FP_TABLE_GROUP_SYNC_CONTEXT_NULL";

    /**
     * Suspend before tables sync task.
     */
    String FP_TABLES_SYNC_TASK_SUSPEND = "FP_TABLES_SYNC_TASK_SUSPEND";

    /**
     * Fail before tables sync task
     */
    String FP_FAILED_TABLES_SYNC = "FP_FAILED_TABLES_SYNC";

    /**
     * Suspend before set table group change meta task.
     */
    String FP_SUSPEND_ON_SET_TABLE_GROUP_CHANGE_META = "FP_SUSPEND_ON_SET_TABLE_GROUP_CHANGE_META";

    /**
     * Fail before set table group change meta task.
     */
    String FP_FAIL_ON_SET_TABLE_GROUP_CHANGE_META = "FP_FAIL_ON_SET_TABLE_GROUP_CHANGE_META";

    String FP_UPDATE_TABLES_VERSION_ERROR = "FP_UPDATE_TABLES_VERSION_ERROR";

    String FP_FASTCHECKER_IDLE_QUERY_SLEEP = "FP_FASTCHECKER_IDLE_QUERY_SLEEP";

    String FP_FASTCHECKER_RESEND_SNAPSHOT_EXCEPTION = "FP_FASTCHECKER_RESEND_SNAPSHOT_EXCEPTION";

    String FP_FAIL_DURING_CAL_PRIMARY_HASH = "FP_FAIL_DURING_CAL_PRIMARY_HASH";

    String FP_FAIL_DURING_CAL_COLUMNAR_HASH = "FP_FAIL_DURING_CAL_COLUMNAR_HASH";

    String FP_FAIL_INNER_CONN_CLOSE = "FP_FAIL_INNER_CONN_CLOSE";

    String FP_FAIL_DELETE_CHECKSUM = "FP_FAIL_DELETE_CHECKSUM";

    String FP_FAIL_CSV_CHECKSUM = "FP_FAIL_CSV_CHECKSUM";

    String FP_FORCE_CAL_IN_NAIVE_METHOD = "FP_FORCE_CAL_IN_NAIVE_METHOD";

    String FP_SLEEP_DURING_CHECK_CCI = "FP_SLEEP_DURING_CHECK_CCI";

    String FB_CHECK_IN_BACK_FILL = "FB_CHECK_IN_BACK_FILL";

    String FP_PHYSICAL_BACKFILL_TASK_SUSPEND = "FP_PHYSICAL_BACKFILL_TASK_SUSPEND";
    String FP_PHYSICAL_BACKFILL_TASK_RANDOM_FAIL = "FP_PHYSICAL_BACKFILL_TASK_RANDOM_FAIL";
    String FP_CLONE_TABLE_DATA_FILE_TASK_SUSPEND = "FP_CLONE_TABLE_DATA_FILE_TASK_SUSPEND";
    String FP_CLONE_TABLE_DATA_FILE_TASK_RANDOM_FAIL = "FP_CLONE_TABLE_DATA_FILE_TASK_RANDOM_FAIL";

    String FP_TTL_JOB_FAILED_ON_PREPARE_CURR_DATETIME = "FP_TTL_JOB_FAILED_ON_PREPARE_CURR_DATETIME";
    String FP_TTL_JOB_FAILED_ON_PREPARE_CLEANUP_INTERVAL = "FP_TTL_JOB_FAILED_ON_PREPARE_CLEANUP_INTERVAL";
    String FP_TTL_JOB_FAILED_ON_CREATE_ARC_CCI = "FP_TTL_JOB_FAILED_ON_CREATE_ARC_CCI";
    String FP_TTL_JOB_FAILED_ON_ADD_ARC_CCI_PART = "FP_TTL_JOB_FAILED_ON_ADD_ARC_CCI_PART";
    String FP_TTL_JOB_FAILED_ON_ADD_PRIM_PART = "FP_TTL_JOB_FAILED_ON_ADD_PRIM_PART";
    String FP_TTL_JOB_FAILED_ON_DELETE_ROW = "FP_TTL_JOB_FAILED_ON_DELETE_ROW";
    String FP_TTL_JOB_FAILED_ON_DROP_PART = "FP_TTL_JOB_FAILED_ON_DROP_PART";
    String FP_TTL_JOB_FAILED_ON_OPTI_TBL = "FP_TTL_JOB_FAILED_ON_OPTI_TBL";
    String FP_TTL_JOB_FAILED_ON_LOG_TASK = "FP_TTL_JOB_FAILED_ON_LOG_TASK";

    String FP_TTL_JOB_SUSPEND_TIME_ON_DELETE_ROW = "FP_TTL_JOB_SUSPEND_TIME_ON_DELETE_ROW";
    String FP_TTL_JOB_SUSPEND_TIME_ON_CREATE_ARC_CCI = "FP_TTL_JOB_SUSPEND_TIME_ON_CREATE_ARC_CCI";
    String FP_TTL_JOB_SUSPEND_TIME_ON_ADD_PRIM_PART = "FP_TTL_JOB_SUSPEND_TIME_ON_ADD_PRIM_PART";
    String FP_TTL_JOB_SUSPEND_TIME_ON_ADD_CCI_PART = "FP_TTL_JOB_SUSPEND_TIME_ON_ADD_CCI_PART";
    String FP_TTL_JOB_SUSPEND_TIME_ON_OPTI_TBL = "FP_TTL_JOB_SUSPEND_TIME_ON_OPTI_TBL";

    String FP_TTL_SCHEDULE_JOB_SUSPEND_TIME_ON_WAIT_FINISH_RUNNING =
        "FP_TTL_SCHEDULE_JOB_SUSPEND_TIME_ON_WAIT_FINISH_RUNNING";

    // ****** OMC 3.0 *****
    String FP_OMC_INIT_TASK_FAILED = "FP_OMC_INIT_TASK_FAILED";
    String FP_OMC_BEFORE_EXECUTE_TASK_FAILED = "FP_OMC_BEFORE_EXECUTE_TASK_FAILED";
    String FP_OMC_AFTER_EXECUTE_TASK_FAILED = "FP_OMC_AFTER_EXECUTE_TASK_FAILED";
    String FP_OMC_PHYSICAL_DDL_INTERRUPTED = "FP_OMC_PHYSICAL_DDL_INTERRUPTED";
    String FP_OMC_FAILED_ON_OMC_STATUS = "FP_OMC_FAILED_ON_OMC_STATUS";
    String FP_OMC_FAILED_ON_LOCK_TABLE_CHECK = "FP_OMC_FAILED_ON_LOCK_TABLE_CHECK";
    String FP_OMC_FAILED_ON_CHECKER = "FP_OMC_FAILED_ON_CHECKER";
    String FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_1 = "FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_1";
    String FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_2 = "FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_2";
    String FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_3 = "FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_3";
    String FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_4 = "FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_4";
    String FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_5 = "FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_5";
    String FP_OMC_FAILED_BEFORE_GET_SESSION_ID = "FP_OMC_FAILED_BEFORE_GET_SESSION_ID";
    String FP_OMC_FAILED_BEFORE_RENAME = "FP_OMC_FAILED_BEFORE_RENAME";
    String FP_OMC_FAILED_AFTER_RENAME = "FP_OMC_FAILED_AFTER_RENAME";
    String FP_OMC_FAILED_ON_HANDLE_EXCEPTION = "FP_OMC_FAILED_ON_HANDLE_EXCEPTION";

    String FP_OMC_FAILED_ON_BACK_FILL = "FP_OMC_FAILED_ON_BACK_FILL";
    String FP_OMC_SKIP_BACK_FILL = "FP_OMC_SKIP_BACK_FILL";
    String FP_OMC_FAILED_ON_APPLY_CHANGESET = "FP_OMC_FAILED_ON_APPLY_CHANGESET";
    String FP_OMC_SKIP_APPLY_CHANGESET = "FP_OMC_SKIP_APPLY_CHANGESET";
    String FP_OMC_BEFORE_CUTOVER_SUSPEND = "FP_OMC_BEFORE_CUTOVER_SUSPEND";
    String FP_OMC_APPLY_CHANGESET_SUSPEND = "FP_OMC_APPLY_CHANGESET_SUSPEND";
    String FP_OMC_CHECK_WITH_TSO_SUSPEND = "FP_OMC_CHECK_WITH_TSO_SUSPEND";

    String FP_LOGICAL_BACK_FILL_SUSPEND = "FP_LOGICAL_BACK_FILL_SUSPEND";
    String FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND = "FP_SPLIT_BEFORE_FIRST_CATCHUP_TASK_SUSPEND";
    String FP_SPLIT_AFTER_TABLE_READONLY_TASK_SUSPEND = "FP_ALTER_TABLE_READONLY_TASK_SUSPEND";
    String FP_ALTER_TABLE_READONLY_TASK_SUSPEND = "FP_ALTER_TABLE_READONLY_TASK_SUSPEND";
    String FP_SPLIT_FAILED_BEFORE_READONLY_TASK = "FP_SPLIT_FAILED_BEFORE_READONLY_TASK";
    String FP_SPLIT_FAILED_AFTER_READONLY_TASK = "FP_SPLIT_FAILED_AFTER_READONLY_TASK";
    String FP_SPLIT_FAILED_BEFORE_UNSET_READONLY_TASK = "FP_SPLIT_FAILED_BEFORE_UNSET_READONLY_TASK";

    /**
     * set @FP_SUSPEND_BEFORE_DDL_JOB_CREATED='5000';
     * Suspend before DdlContext is created in handler, creating a window where
     * isDdlStatement=true but getDdlJobId()=null (for testing KILL QUERY TOCTOU fix)
     */
    String FP_SUSPEND_BEFORE_DDL_JOB_CREATED = "FP_SUSPEND_BEFORE_DDL_JOB_CREATED";

    /**
     * MDL lock leak reproduction: pause inside acquireLock compute lambda (stamp acquired, entry not yet written)
     * Value format: "tableName,sleepMs" e.g. "t1,40000" means sleep 40s when acquiring MDL for table t1
     * Only matches request.getKey().getTableName(), tableGroupDigest will not match
     * Sleep is interrupt-resistant to survive Path A's f.cancel(true)
     */
    String FP_MDL_ACQUIRE_INSIDE_COMPUTE = "FP_MDL_ACQUIRE_INSIDE_COMPUTE";

    /**
     * set @FP_BLOB_WRITE_FAIL='true';
     * Inject a synchronous failure before the Blob write. Value 'async' returns a structurally
     * valid write receipt whose durability future completes exceptionally at the physical barrier.
     */
    String FP_BLOB_WRITE_FAIL = "FP_BLOB_WRITE_FAIL";

    /**
     * set @FP_BLOB_READ_FAIL='true';
     * Inject failure in FetchBlob.compute() before reading blob from OSS/cache.
     */
    String FP_BLOB_READ_FAIL = "FP_BLOB_READ_FAIL";

    /**
     * set @FP_BLOB_CACHE_READ_FAIL='true';
     * Inject a cache read failure so the blob reader falls back to direct OSS.
     */
    String FP_BLOB_CACHE_READ_FAIL = "FP_BLOB_CACHE_READ_FAIL";

    /**
     * set @FP_BLOB_CACHE_READ_SUSPEND='5000';
     * Suspend the high-watermark cache branch before reading GeneralCache.
     */
    String FP_BLOB_CACHE_READ_SUSPEND = "FP_BLOB_CACHE_READ_SUSPEND";

    /**
     * Suspend an OSS-path FETCH_BLOB worker for the configured milliseconds.
     * set @FP_BLOB_OSS_READ_SUSPEND='5000';
     */
    String FP_BLOB_OSS_READ_SUSPEND = "FP_BLOB_OSS_READ_SUSPEND";

    /**
     * set @FP_STAGING_READ_SUSPEND='5000';
     * Suspend the high-watermark staging branch before resolving/reading the DN.
     */
    String FP_STAGING_READ_SUSPEND = "FP_STAGING_READ_SUSPEND";

    /**
     * set @FP_BLOB_FLUSH_TIMEOUT='true';
     * Inject timeout in BlobWriteTracker.awaitAll() simulating stuck blob upload.
     */
    String FP_BLOB_FLUSH_TIMEOUT = "FP_BLOB_FLUSH_TIMEOUT";

    /**
     * set @FP_BLOB_UPLOAD_HANG='true';
     * Returns a never-completing future from the BlobWriter batch upload path, triggering
     * real BlobWriteTracker.awaitAll() timeout + EventLogger.
     */
    String FP_BLOB_UPLOAD_HANG = "FP_BLOB_UPLOAD_HANG";

    /**
     * set @FP_MCE_REMOTE_UPLOAD_DELAY='3000';
     * Delay completion of an MCE remote-only upload after the OSS write has completed.
     */
    String FP_MCE_REMOTE_UPLOAD_DELAY = "FP_MCE_REMOTE_UPLOAD_DELAY";

    /**
     * set @FP_GMS_TABLE_META_MCE_STATE_LOAD_FAIL='true';
     * Inject a failure when loading mce_column_state records from MetaDB.
     */
    String FP_GMS_TABLE_META_MCE_STATE_LOAD_FAIL = "FP_GMS_TABLE_META_MCE_STATE_LOAD_FAIL";

    /**
     * set @FP_GMS_TABLE_META_MCE_STATE_RESOLVE_FAIL='true';
     * Inject a failure when resolving mce_column_state records into TableMeta state.
     */
    String FP_GMS_TABLE_META_MCE_STATE_RESOLVE_FAIL = "FP_GMS_TABLE_META_MCE_STATE_RESOLVE_FAIL";

    /**
     * set @FP_BLOB_DELETE_FAIL='true';
     * Inject failure in blob delete (rollback cleanup), verifying
     * that delete errors are non-fatal.
     */
    String FP_BLOB_DELETE_FAIL = "FP_BLOB_DELETE_FAIL";

    /**
     * Inject a statement-scoped failure after an MCE/externalized UPDATE pushdown has completed all blob uploads,
     * but before any physical UPDATE is dispatched. Use through cmd_extra so parallel tests do not share JVM state.
     */
    String FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE = "FP_MCE_DML_FAIL_BEFORE_PHYSICAL_WRITE";

    /**
     * set @FP_STAGING_CACHE_MISS='true';
     * Force a seqDnCache miss in resolveDn, triggering full MetaDB refresh path.
     */
    String FP_STAGING_CACHE_MISS = "FP_STAGING_CACHE_MISS";

    /**
     * set @FP_STAGING_ENSURE_DN_TABLE_FAIL='true';
     * Force StagingTableManager.ensureDnTable to throw, simulating physical
     * DB/table creation failure so the seq stays in / rolls back from CREATING.
     */
    String FP_STAGING_ENSURE_DN_TABLE_FAIL = "FP_STAGING_ENSURE_DN_TABLE_FAIL";

    /**
     * set @FP_STAGING_SKIP_CREATING_ROLLBACK='true';
     * Skip the rollback-delete of a CREATING seq after ensureDnTable fails,
     * leaving a stuck CREATING row to exercise cleanupTimedOutCreating.
     */
    String FP_STAGING_SKIP_CREATING_ROLLBACK = "FP_STAGING_SKIP_CREATING_ROLLBACK";

    /**
     * set @FP_STAGING_SKIP_BACKGROUND_DRAIN='true';
     * Skip only the scheduled SEALED drain; synchronous procedures remain enabled.
     */
    String FP_STAGING_SKIP_BACKGROUND_DRAIN = "FP_STAGING_SKIP_BACKGROUND_DRAIN";

    /**
     * set @FP_STAGING_FLUSH_UPLOAD_FAIL='true';
     * set @FP_STAGING_FLUSH_UPLOAD_FAIL='&lt;seqId&gt;';
     * Complete staging's remote-only upload future exceptionally so the flush
     * retry and in-flight cancellation path can be verified without breaking OSS.
     * A seqId value targets one per-DN staging fixture; true retains the legacy
     * fail-every-seq behavior.
     */
    String FP_STAGING_FLUSH_UPLOAD_FAIL = "FP_STAGING_FLUSH_UPLOAD_FAIL";

    /**
     * set @FP_STAGING_MARK_FLUSHED_FAIL='true';
     * set @FP_STAGING_MARK_FLUSHED_FAIL='&lt;seqId&gt;';
     * Fail after deleting the staging MetaDB row but before committing the
     * transaction, verifying that metadata and the DN table remain retryable.
     * A seqId value targets one per-DN staging fixture.
     */
    String FP_STAGING_MARK_FLUSHED_FAIL = "FP_STAGING_MARK_FLUSHED_FAIL";

    /**
     * set @FP_STAGING_DROP_TABLE_FAIL='true';
     * set @FP_STAGING_DROP_TABLE_FAIL='&lt;seqId&gt;';
     * Fail after the MetaDB flush commit but before dropping the physical staging table,
     * leaving a safe metadata-free orphan for the cleanup path to sweep.
     * A seqId value targets one per-DN staging fixture.
     */
    String FP_STAGING_DROP_TABLE_FAIL = "FP_STAGING_DROP_TABLE_FAIL";

    /**
     * set @FP_STAGING_ORPHAN_DROP_FAIL='true';
     * Fail the physical DROP performed by orphan cleanup so best-effort and strict
     * drain recovery behavior can be verified.
     */
    String FP_STAGING_ORPHAN_DROP_FAIL = "FP_STAGING_ORPHAN_DROP_FAIL";

    /**
     * set @FP_STAGING_FORCE_FLUSH_CLAIM_LOST='<seqId>';
     * Simulate an exact force-flush snapshot entry losing its SEALED-to-FLUSHING CAS.
     * The legacy value 'true' still applies to every snapshot entry.
     */
    String FP_STAGING_FORCE_FLUSH_CLAIM_LOST = "FP_STAGING_FORCE_FLUSH_CLAIM_LOST";

    /**
     * set @FP_EXT_MAPPING_MIGRATION_INIT_FAIL='true';
     * Fail external-column mapping initialization before accessing MetaDB.
     */
    String FP_EXT_MAPPING_MIGRATION_INIT_FAIL = "FP_EXT_MAPPING_MIGRATION_INIT_FAIL";

    /**
     * set @FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL='true';
     * Fail after copying legacy mappings but before committing their migration transaction.
     */
    String FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL = "FP_EXT_MAPPING_MIGRATION_COMMIT_FAIL";

    /**
     * set @FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL='true';
     * Fail named-lock release after migration commit so the MetaDB connection-discard path is exercised.
     */
    String FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL = "FP_EXT_MAPPING_MIGRATION_RELEASE_LOCK_FAIL";

    /**
     * Fail one staging flush by exact seq id without affecting concurrent seqs.
     * set @FP_STAGING_FLUSH_FAIL_SEQ_ID='123';
     */
    String FP_STAGING_FLUSH_FAIL_SEQ_ID = "FP_STAGING_FLUSH_FAIL_SEQ_ID";

    /**
     * Inject failure while marking external-column mappings during DROP DATABASE.
     */
    String FP_DROP_DATABASE_EXT_MAPPING_FAIL = "FP_DROP_DATABASE_EXT_MAPPING_FAIL";

    String FP_DDL_BEFORE_JOB_ID_GEN = "FP_DDL_BEFORE_JOB_ID_GEN";
    String FP_DDL_BEFORE_PHASE_1_LOCK = "FP_DDL_BEFORE_PHASE_1_LOCK";
    String FP_DDL_AFTER_PHASE_1_LOCK = "FP_DDL_AFTER_PHASE_1_LOCK";
    String FP_DDL_BEFORE_PHASE_2_LOCK = "FP_DDL_BEFORE_PHASE_2_LOCK";
    String FP_DDL_AFTER_STORE_JOB = "FP_DDL_AFTER_STORE_JOB";

}

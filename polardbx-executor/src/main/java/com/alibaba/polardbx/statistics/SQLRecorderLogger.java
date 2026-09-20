package com.alibaba.polardbx.statistics;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.text.MessageFormat;

public class SQLRecorderLogger {

    // sql#group#dbKey#totalTime#sqlTime#connectionWaitTime#connectionCreateTime#param#traceid
    public final static MessageFormat physicalLogFormat = new MessageFormat("{0}#{1}#{2}#{3}#{4}#{5}#{6}#{7}#{8}");

    // sql#time#affectrow#traceid
    public final static MessageFormat slowLogFormat = new MessageFormat("{0}#{1}#{2}#{3}");
    /**
     * sql$#time#affectrow#type#traceid type为二进制表示 type = hasScanWholeTable
     * low-bit hasUnpushedJoin hasTempTable high-bit
     */
    public final static MessageFormat ddlLogFormat = new MessageFormat("{0}#{1}#{2}#{3}");

    // sql#parameterizedSqlId#sqlCount#appName
    public final static MessageFormat parameterizedSqlLogFormat = new MessageFormat("{0}#{1}#{2}#{3}");

    //逻辑慢SQL日志详细，和SQL日志一样的指标
    public final static Logger slowDetailLogger = LoggerFactory.getLogger("slow_detail", true);

    // 物理慢SQL
    public final static Logger physicalSlowLogger = LoggerFactory.getLogger("physical_slow");

    // DRDS普通 SQL日志
    public final static Logger sqlLogger = LoggerFactory.getLogger("sql", true);

    // DDL日志
    public final static Logger ddlLogger = LoggerFactory.getLogger("ddl");

    public final static Logger ddlEngineLogger = LoggerFactory.getLogger("DDL_ENGINE_LOG");

    public final static Logger ddlMetaLogger = LoggerFactory.getLogger("DDL_META_LOG");

    // DDL Statistics Log
    public final static Logger ddlStatsLogger = LoggerFactory.getLogger("ddl");

    // SCALE OUT SQL日志
    public final static Logger scaleOutSqlLogger = LoggerFactory.getLogger("ddl");

    // 参数化后的SQL的日志
    public final static Logger parameterizedSqlLogger = LoggerFactory.getLogger("PARAMETERIZED_SQL");

    // SCALE OUT 任务日志
    public final static Logger scaleOutTaskLogger = LoggerFactory.getLogger("ddl");

    // TTL 任务日志
    public final static Logger ttlTaskLogger = LoggerFactory.getLogger("ddl");

    // Slow trans log
    public final static Logger slowTransLogger = LoggerFactory.getLogger("slow_trans");

    /**
     * 0:sql#<br>
     * 1:success(0 for success, 1 for fail)#<br>
     * 2:time_cost
     * 3:trace_id
     * 4:update_count
     */
    public final static MessageFormat innerSqlFormat = new MessageFormat("{0}#{1}#{2}#{3}#{4}");

    public final static Logger cdcLogger = LoggerFactory.getLogger(SQLRecorderLogger.class);

    // format_version#trace_id#trx_type#start_time#finish_time#duration_time#status#active_time#idle_time
    // #write_time#read_time#write_affect_rows#read_return_rows#mdl_wait_time#get_tso_time#prepare_time
    // #trx_log_time#commit_time#rollback_time#sql_count#rw_sql_count#trx_template_id#trx_log_type#
    public final static MessageFormat slowTransFormat =
        new MessageFormat(
            "{0}#{1}#{2}#{3}#{4}#{5}#{6}#{7}#{8}#{9}#{10}#{11}#{12}#{13}#{14}#{15}#{16}#{17}#{18}#{19}#{20}#{21}#{22}#");

}

package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;

public class InnerProcedureUtils {
    public static final String POLARDBX_INNER_PROCEDURE = "polardbx";

    //忽略大小写
    public static final String TRIGGER_SYNC_POINT_TRX = "trigger_sync_point_trx";
    public static final String COLUMNAR_FLUSH = "columnar_flush";
    public static final String COLUMNAR_BACKUP = "columnar_backup";
    public static final String COLUMNAR_SNAPSHOT_FILES = "columnar_snapshot_files";
    public static final String COLUMNAR_SET_CONFIG = "columnar_set_config";
    public static final String COLUMNAR_ROLLBACK = "columnar_rollback";
    public static final String COLUMNAR_GENERATE_SNAPSHOTS = "columnar_generate_snapshots";
    public static final String COLUMNAR_IGNORE = "columnar_ignore";
    public static final String COLUMNAR_UNIGNORE = "columnar_unignore";
    public static final String RECORD_JDBC_URL = "record_jdbc_url";
    public static final String ADD_DN_CCL_RULE = "add_dn_ccl_rule";
    public static final String SHOW_JDBC_URL = "show_jdbc_url";

    public static final String CLEAR_ALL_DN_CCL_RULES = "clear_all_dn_ccl_rules";

    public static final String COLUMNAR_AUTO_SNAPSHOT_CONFIG =
        TddlConstants.COLUMNAR_AUTO_SNAPSHOT_CONFIG.toLowerCase();
    public static final String CHECK_CHAIN_HIST = "check_chain_hist";
    public static final String CHECK_CHAIN_GLOBAL = "check_chain_global";
    public static final String CHECK_CHAIN_VALID = "check_chain_valid";
    public static final String CHECK_CHAIN_ALL = "check_chain_all";

    public static final String CHAIN_HIST_ARCHIVE = "chain_hist_archive";
    public static final String CHAIN_GLOBAL_ARCHIVE = "chain_global_archive";

    public static final String DRAIN_HANGING_TRX = "drain_hanging_trx";
    public static final String FIX_GDN_TRX_POLICY = "fix_gdn_trx_policy";
    public static final String EXT_STAGING_DRAIN_SIMULATE = "ext_staging_drain_simulate";
    public static final String FORCE_ROTATE_STAGING = "force_rotate_staging";
    public static final String FORCE_FLUSH_STAGING = "force_flush_staging";
    public static final String FORCE_EXT_COLUMN_MAPPING_MIGRATION = "force_ext_column_mapping_migration";

    public static boolean isInnerProcedure(SQLCallStatement stmt) {
        SQLName procedureName = stmt.getProcedureName();
        if (procedureName instanceof SQLPropertyExpr) {
            String owner = ((SQLPropertyExpr) procedureName).getOwnerName();
            return POLARDBX_INNER_PROCEDURE.equalsIgnoreCase(owner);
        }
        return false;
    }
}

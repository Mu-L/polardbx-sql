package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import lombok.Data;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

public class FixGdnTrxPolicyProcedure extends BaseInnerProcedure {
    private static final Logger logger = LoggerFactory.getLogger(FixGdnTrxPolicyProcedure.class);

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("RESULT", DataTypes.StringType);
        cursor.addColumn("MESSAGE", DataTypes.StringType);
        List<SQLExpr> params = statement.getParameters();
        if (params.size() != 1) {
            cursor.addRow(new Object[] {"FAIL", "fix_gdn_trx_policy() requires exactly one parameter"});
            return;
        }
        String action = params.get(0).toString();
        String currentPolicy = InstConfUtil.getOriginVal(ConnectionParams.TRANSACTION_POLICY);
        String statusStr = null;
        try {
            InstConfigRecord record = MetaDbUtil.getGlobal(ConnectionProperties.GDN_TRX_POLICY_STATUS);
            if (record != null) {
                logger.warn(
                    "[GDN] get trx policy status: " + record.paramVal + ", gmt_modified: " + record.gmtModified);
                statusStr = record.paramVal;
            } else {
                logger.warn("[GDN] get empty trx policy status.");
                statusStr = "";
            }
        } catch (Throwable t) {
            handleError(t, cursor);
            return;
        }
        logger.warn("[GDN] start processing fix gdn trx policy"
            + ", action: " + action
            + ", currentPolicy: " + currentPolicy
            + ", statusStr: " + statusStr);
        if ("become_slave".equalsIgnoreCase(action)) {
            if (becomeSlave(cursor, currentPolicy, statusStr)) {
                return;
            }
        } else if ("become_master".equalsIgnoreCase(action)) {
            if (becomeMaster(cursor, statusStr)) {
                return;
            }
        } else {
            logger.error("[GDN] got unknown action: " + action);
            cursor.addRow(new Object[] {"FAIL", "parameter choices: become_slave, become_master"});
            return;
        }

        cursor.addRow(new Object[] {"OK", ""});
    }

    /**
     * @return true if error
     */
    private static boolean becomeSlave(ArrayResultCursor cursor, String currentPolicy, String statusStr) {
        logger.warn("[GDN] start changing status to " + statusStr);
        Status status = JSON.parseObject(statusStr, Status.class);
        if (null != status && "slave".equalsIgnoreCase(status.role)) {
            // ignore slave to slave
            logger.warn("[GDN] change to slave when already in slave role, ignore.");
            return false;
        }
        if ("".equalsIgnoreCase(currentPolicy) || "tso".equalsIgnoreCase(currentPolicy)) {
            // change tso to xa
            logger.warn("[GDN] change transaction policy from " + currentPolicy + " to XA");
            try {
                Properties properties = new Properties();
                properties.setProperty(ConnectionProperties.TRANSACTION_POLICY, "XA");
                MetaDbUtil.setGlobal(properties);

                InstConfigRecord record = MetaDbUtil.getGlobal(ConnectionProperties.TRANSACTION_POLICY);
                if (null == record) {
                    return handleError(new RuntimeException("fail to set transaction policy"), cursor);
                }
                logger.warn("[GDN] already changed to " + record.paramVal
                    + ", gmt_modified: " + record.gmtModified);
                properties = new Properties();
                Status newStatus = new Status("slave", currentPolicy, "XA", record.gmtCreated, record.gmtModified);
                String newStatusStr = JSON.toJSONString(newStatus);
                logger.warn("[GDN] change status to " + newStatusStr);
                properties.setProperty(ConnectionProperties.GDN_TRX_POLICY_STATUS, newStatusStr);
                MetaDbUtil.setGlobal(properties);
            } catch (Exception e) {
                return handleError(e, cursor);
            }
        } else {
            // no need to change
            logger.warn("[GDN] no need to change transaction policy.");
            try {
                Properties properties = new Properties();
                logger.warn("[GDN] clear status for slave.");
                properties.setProperty(ConnectionProperties.GDN_TRX_POLICY_STATUS,
                    JSON.toJSONString(new Status("slave", null, null, null, null)));
                MetaDbUtil.setGlobal(properties);
            } catch (Exception e) {
                return handleError(e, cursor);
            }
        }
        logger.warn("[GDN] succeed changing status to " + statusStr);
        return false;
    }

    private static boolean becomeMaster(ArrayResultCursor cursor, String statusStr) {
        logger.warn("[GDN] start changing status to " + statusStr);
        Status status = JSON.parseObject(statusStr, Status.class);
        if (null != status && "master".equalsIgnoreCase(status.role)) {
            // ignore master to master
            logger.warn("[GDN] change to master when already in master role, ignore.");
            return false;
        }
        try {
            InstConfigRecord record = MetaDbUtil.getGlobal(ConnectionProperties.TRANSACTION_POLICY);
            if (null == status || null == record || !record.gmtModified.equals(status.gmtModified)) {
                // already change to other policy, do nothing
                logger.warn("[GDN] transaction policy is already changed when in slave role, do nothing."
                    + " record gmtModified: " + (record == null ? "null" : record.gmtModified)
                    + " status gmtModified: " + (status == null ? "null" : status.gmtModified));
            } else {
                // restore transaction policy to the old one
                ITransactionPolicy.of(status.oldPolicy);
                logger.warn("[GDN] restore transaction policy to " + status.oldPolicy);
                Properties properties = new Properties();
                properties.setProperty(ConnectionProperties.TRANSACTION_POLICY, status.oldPolicy);
                MetaDbUtil.setGlobal(properties);
            }
            Properties properties = new Properties();
            logger.warn("[GDN] clear status for master.");
            properties.setProperty(ConnectionProperties.GDN_TRX_POLICY_STATUS,
                JSON.toJSONString(new Status("master", null, null, null, null)));
            MetaDbUtil.setGlobal(properties);
        } catch (Throwable t) {
            return handleError(t, cursor);
        }
        logger.warn("[GDN] succeed changing status to " + statusStr);
        return false;
    }

    private static boolean handleError(Throwable t, ArrayResultCursor cursor) {
        logger.warn(t);
        cursor.addRow(new Object[] {"FAIL", t.getMessage()});
        return true;
    }

    @Data
    public static class Status {
        public String role;
        public String oldPolicy;
        public String newPolicy;
        public Timestamp gmtCreated;
        public Timestamp gmtModified;

        public Status(String role, String oldPolicy, String newPolicy, Timestamp gmtCreated, Timestamp gmtModified) {
            this.role = role;
            this.oldPolicy = oldPolicy;
            this.newPolicy = newPolicy;
            this.gmtCreated = gmtCreated;
            this.gmtModified = gmtModified;
        }
    }
}

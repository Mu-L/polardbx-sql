package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Sync action to add DN CCL rule from leader node
 *
 * @author liugaoji
 */
public class AddDnCclRuleSyncAction implements IGmsSyncAction, Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(AddDnCclRuleSyncAction.class);

    private String targetStorageInstId;
    private String sqlType;
    private long concurrency;
    private String keywords;

    public AddDnCclRuleSyncAction() {

    }

    public AddDnCclRuleSyncAction(String targetStorageInstId, String sqlType, long concurrency, String keywords) {
        this.targetStorageInstId = targetStorageInstId;
        this.sqlType = sqlType;
        this.concurrency = concurrency;
        this.keywords = keywords;
    }

    @Override
    public Object sync() {
        ArrayResultCursor result = new ArrayResultCursor("ADD_DN_CCL_RULE");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("STORAGE_INST_ID", DataTypes.StringType);
        result.addColumn("GENERATED_SQL", DataTypes.StringType);
        result.addColumn("MESSAGE", DataTypes.StringType);

        // Check if current node is leader
        if (!ConfigDataMode.isMasterMode() || !LeaderStatusBridge.getInstance().hasLeadership()) {
            // Non-leader nodes return empty result
            return result;
        }

        boolean success = true;
        String generatedSql = "";
        StringBuilder messageBuilder = new StringBuilder();

        try {
            // Validate input parameters
            if (targetStorageInstId == null || targetStorageInstId.trim().isEmpty()) {
                messageBuilder.append("Invalid storage instance ID: ").append(targetStorageInstId);
                result.addRow(new Object[] {
                    TddlNode.getHost() + ":" + TddlNode.getPort(),
                    "FAILED",
                    targetStorageInstId,
                    generatedSql,
                    messageBuilder.toString()
                });
                return result;
            }

            // Find target storage instance
            StorageInstHaContext targetInstance = null;
            Iterator<StorageInstHaContext> iterator = CCLDetectManager.getDnMasterIterator();
            while (iterator.hasNext()) {
                StorageInstHaContext instHaContext = iterator.next();
                if (instHaContext != null && targetStorageInstId.equals(instHaContext.getStorageInstId())) {
                    targetInstance = instHaContext;
                    break;
                }
            }

            if (targetInstance == null) {
                success = false;
                messageBuilder.append("Storage instance not found: ").append(targetStorageInstId);
            } else {
                // Create CCL rule
                CCLDetectDnActor cclDetectDnActor = new CCLDetectDnActor();
                StorageInstHaContext finalTargetInstance = targetInstance;

                generatedSql = cclDetectDnActor.getGenSql(
                    () -> DbTopologyManager.getConnectionForStorage(finalTargetInstance),
                    concurrency,
                    keywords
                );

                Long ruleId = cclDetectDnActor.generateCcl(
                    () -> DbTopologyManager.getConnectionForStorage(finalTargetInstance),
                    generatedSql
                );

                messageBuilder.append("Successfully created CCL rule with ID: ").append(ruleId);
            }
        } catch (Exception e) {
            success = false;
            messageBuilder.append("Error creating CCL rule: ").append(e.getMessage());
            logger.error("Error creating CCL rule for storage instance: " + targetStorageInstId, e);
        }

        // Add result row
        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            success ? "SUCCESS" : "FAILED",
            targetStorageInstId,
            generatedSql,
            messageBuilder.toString()
        });

        return result;
    }

    // Getter and Setter methods
    public String getTargetStorageInstId() {
        return targetStorageInstId;
    }

    public void setTargetStorageInstId(String targetStorageInstId) {
        this.targetStorageInstId = targetStorageInstId;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public long getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(long concurrency) {
        this.concurrency = concurrency;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

}
package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.ConcurrentHashSet;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

/**
 * @author chenghui.lch
 */
public class FullScanTableBlackListManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(FullScanTableBlackListManager.class);

    /**
     * Key format: (#db.#tb).to_lower_case
     */
    protected volatile Set<String> forbidFullTableNameSet = new ConcurrentHashSet<>();
    private static FullScanTableBlackListManager instance = null;

    static {
        instance = new FullScanTableBlackListManager();
    }

    private FullScanTableBlackListManager() {
    }

    public static FullScanTableBlackListManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    public static String buildFullTableNameKey(String dbName, String tbName) {
        String fullTblName = String.format("%s.%s", dbName, tbName);
        return fullTblName.toLowerCase();
    }

    protected boolean checkIfAllowFullScan(String dbName, String tbName) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(dbName)) {
            /**
             * Only use for new-part of auto db
             */
            return true;
        }
        if (forbidFullTableNameSet.isEmpty()) {
            return true;
        }
        String fullTblNameKey = FullScanTableBlackListManager.buildFullTableNameKey(dbName, tbName);
        return !forbidFullTableNameSet.contains(fullTblNameKey);
    }

    private Set<String> buildNewFullScanTblBlackList(String fullScanTbBlackList) {
        Set<String> newFullTableNameSet = new ConcurrentHashSet<>();
        if (StringUtils.isEmpty(fullScanTbBlackList)) {
            return newFullTableNameSet;
        }
        String[] fullTbNameStrArr = fullScanTbBlackList.toLowerCase().split(",");
        if (fullTbNameStrArr != null && fullTbNameStrArr.length == 0) {
            return newFullTableNameSet;
        }
        for (int i = 0; i < fullTbNameStrArr.length; i++) {
            String fullTblName = fullTbNameStrArr[i];
            if (newFullTableNameSet.contains(fullTblName)) {
                continue;
            }
            newFullTableNameSet.add(fullTblName);
        }
        return newFullTableNameSet;

    }

    public synchronized void reload(String fullScanTbBlackList) {
        try {
            Set<String> newForbidFullTableNameSet = buildNewFullScanTblBlackList(fullScanTbBlackList);
            this.forbidFullTableNameSet = newForbidFullTableNameSet;
        } catch (Throwable ex) {
            logger.warn(String.format("Failed to reload full-scan table while list, erro is %s", ex.getMessage()), ex);
        }
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    /**
     * Throw if the given table is not allowed to perform a full table scan.
     *
     * <p>The caller is responsible for determining whether the actual pruning
     * result really constitutes a full physical scan (e.g. by checking that
     * the pruned physical-partition BitSet covers all physical partitions).
     * This method only consults the per-session ALLOW_FULL_TABLE_SCAN switch
     * and the table-level black list, then throws if disallowed.
     */
    public void throwIfNotAllowFullScan(PartitionInfo partInfo, ExecutionContext ec) {
        /**
         * Allow current session to bypass the full-scan blacklist check
         * via session/global variable ALLOW_FULL_TABLE_SCAN.
         */
        if (ec != null && ec.getParamManager() != null
            && ec.getParamManager().getBoolean(ConnectionParams.ALLOW_FULL_TABLE_SCAN)) {
            return;
        }

        String tblSchema = partInfo.getTableSchema();
        String tblName = partInfo.getTableName();
        boolean allowFullScan = checkIfAllowFullScan(tblSchema, tblName);
        if (!allowFullScan || partInfo.isBlockFullTableScan()) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_EXECUTOR,
                String.format("%s.%s is not allowed to perform full table scan", tblSchema, tblName));
        }
    }
}

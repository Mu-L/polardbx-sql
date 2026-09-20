package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

/**
 * @author wumu
 */
@Getter
public class OmcStorageInfo {
    private static final int XPORT_RETRY_TIMES = 10;
    private static final long XPORT_RETRY_INTERVAL_MS = 200L;

    public String storageId;
    public String host;
    public int port;
    public String username;
    public String password;
    public String defaultDb;

    public long jobId;

    public OmcStorageInfo(String storageId, String host, int port, String username, String password, String defaultDb,
                          long jobId) {
        this.storageId = storageId;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.defaultDb = defaultDb;
        this.jobId = jobId;
    }

    public static OmcStorageInfo fromStorageInstId(String storageInstId, String physicalDb, long jobId) {
        HaSwitchParams haSwitchParams = getStorageHaSwitchParamsWithAvailableXPort(storageInstId, physicalDb, jobId);
        Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String ip = nodeIpPort.getKey();
        int xPort = getStorageXPort(haSwitchParams);
        return new OmcStorageInfo(storageInstId, ip, xPort, haSwitchParams.userName,
            PasswdUtil.decrypt(haSwitchParams.passwdEnc), physicalDb, jobId);
    }

    private static HaSwitchParams getStorageHaSwitchParamsWithAvailableXPort(String storageInstId, String physicalDb,
                                                                             long jobId) {
        HaSwitchParams haSwitchParams = null;
        int xPort = -1;
        for (int i = 1; i <= XPORT_RETRY_TIMES; i++) {
            haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
            if (haSwitchParams == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("no found the storage inst for %s", storageInstId));
            }
            xPort = getStorageXPort(haSwitchParams);
            if (xPort > 0 || XConnectionManager.getInstance().getStorageDbPort() < 0) {
                break;
            }
            if (i < XPORT_RETRY_TIMES) {
                SQLRecorderLogger.ddlLogger.warn(String.format(
                    "Storage xport is unavailable, retry get storage ha params. storageInstId=%s, physicalDb=%s, "
                        + "availableAddr=%s, xPort=%s, storageDbPort=%s, jobId=%s, attempt=%s/%s",
                    storageInstId, physicalDb, haSwitchParams.curAvailableAddr, xPort,
                    XConnectionManager.getInstance().getStorageDbPort(), jobId, i, XPORT_RETRY_TIMES));
                sleepBeforeRetry(storageInstId, physicalDb, jobId);
            }
        }
        if (xPort <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("storage xport is unavailable after retry. storageInstId=%s, physicalDb=%s, "
                        + "availableAddr=%s, xPort=%s, storageDbPort=%s, jobId=%s",
                    storageInstId, physicalDb, haSwitchParams == null ? null : haSwitchParams.curAvailableAddr, xPort,
                    XConnectionManager.getInstance().getStorageDbPort(), jobId));
        }
        return haSwitchParams;
    }

    private static int getStorageXPort(HaSwitchParams haSwitchParams) {
        int xPort = haSwitchParams.xport;
        if (XConnectionManager.getInstance().getStorageDbPort() != 0) {
            xPort = XConnectionManager.getInstance().getStorageDbPort();
        }
        return xPort;
    }

    private static void sleepBeforeRetry(String storageInstId, String physicalDb, long jobId) {
        try {
            Thread.sleep(XPORT_RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("interrupted while waiting for storage xport. storageInstId=%s, physicalDb=%s, jobId=%s",
                    storageInstId, physicalDb, jobId), e);
        }
    }

    @Override
    public String toString() {
        return String.format("host:%s,port:%d,u:%s,mask:%s,defaultDB:%s", host,
            port, username, password.substring(0, Math.min(3, password.length())) + "****", defaultDb);
    }

    public String getDigest() {
        return username + "@" + host + ":" + port + ":" + defaultDb;
    }
}

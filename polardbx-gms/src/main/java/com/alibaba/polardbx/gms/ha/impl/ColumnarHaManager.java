package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.ha.ColumnarHaSwitchParams;
import com.alibaba.polardbx.gms.ha.ColumnarHaSwitcher;
import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnarHaManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarHaManager.class);

    @Getter
    @Setter
    private volatile ColumnarHaContext columnarHaContext = null;

    private final static ColumnarHaManager instance = new ColumnarHaManager();

    private final Set<ColumnarHaSwitcher> haSwitcherSet = new HashSet<>();

    private Thread haWatchDogThread;

    public static ColumnarHaManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    @Override
    protected void doInit() {
        logger.info("ColumnarHaManager init");

        columnarHaContext = buildColumnarHaContextFromMetaDb();
        haWatchDogThread = new Thread(() -> {
            columnarHaWatchdog(this);
        }, "columnar-ha-watchdog-thread");
        haWatchDogThread.setDaemon(true);
        haWatchDogThread.setPriority(Thread.MAX_PRIORITY);
        haWatchDogThread.start();
    }

    @Override
    protected void doDestroy() {
        if (haWatchDogThread != null) {
            haWatchDogThread.interrupt();
            haWatchDogThread = null;
        }
    }

    public void registerHaSwitcher(@NotNull ColumnarHaSwitcher haSwitcher) {
        haSwitcherSet.add(haSwitcher);
    }

    public void doHaSwitch(ColumnarHaSwitchParams haSwitchParams) {
        for (ColumnarHaSwitcher haSwitcher : haSwitcherSet) {
            haSwitcher.doHaSwitch(haSwitchParams);
        }
    }

    public static ColumnarHaContext buildColumnarHaContextFromMetaDb() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setNetworkTimeout(null, 1000);
            ColumnarLeaseAccessor accessor = new ColumnarLeaseAccessor();
            accessor.setConnection(conn);
            List<ColumnarLeaseRecord> records = accessor.getAllNodes();
            if (records.isEmpty()) {
                return null;
            }
            //find leader
            ColumnarLeaseRecord leader = records.stream().filter(r -> r.id == 1).findFirst().orElse(null);
            if (leader == null) {
                // 没找到leader，不正常
                return null;
            }
            String leaderIp = leader.owner.split("@")[0];
            Map<String, ColumnarHaInfo> allColumnarHaInfoMap = new HashMap<>();
            for (ColumnarLeaseRecord record : records) {
                if (record.id == 1) {
                    // leader
                    continue;
                }
                String[] infos = record.owner.split(":");
                if (infos.length < 3) {
                    //缺少信息，可能是旧版本，忽略,正确的格式 ip:restPort:grpcPort
                    continue;
                }

                ColumnarHaInfo haInfo = new ColumnarHaInfo(infos[0],
                    infos[0].equalsIgnoreCase(leaderIp) ? ColumnarRole.MASTER : ColumnarRole.STANBY,
                    true,
                    Integer.parseInt(infos[2]));
                allColumnarHaInfoMap.put(infos[0], haInfo);
            }
            if (allColumnarHaInfoMap.isEmpty() || !allColumnarHaInfoMap.containsKey(leaderIp)) {
                //无节点，不包含leader节点信息，异常
                return null;
            }
            ColumnarHaContext columnarHaContext =
                new ColumnarHaContext(leaderIp, allColumnarHaInfoMap.get(leaderIp).getRpcPort(), allColumnarHaInfoMap);
            columnarHaContext.setLeaseTime(leader.lease);
            return columnarHaContext;
        } catch (Exception e) {
            logger.error("buildColumnarHaContextFromMetaDb error", e);
            return null;
        }
    }

    public static void columnarHaWatchdog(ColumnarHaManager ha) {
        while (true) {
            try {
                long now = System.currentTimeMillis();
                long leaseTime = ha.columnarHaContext == null ? 0 : ha.getColumnarHaContext().getLeaseTime();
                if (leaseTime > (now + 2000)) {
                    // lease时间过大，等后面再更新
                    Thread.sleep(leaseTime - now - 2000);
                    continue;
                } else {
                    ColumnarHaContext newContext = buildColumnarHaContextFromMetaDb();
                    refreshColumnarHaContext(ha, newContext);

                    //正常情况，等待1s, 防止metadb信息残留，列存租约不更新，一直读取metadb
                    //异常情况，等待1s后重试
                    Thread.sleep(1000);
                }

            } catch (InterruptedException interruptedException) {
                break;
            } catch (Exception e) {
                logger.error("columnarHaWatchdog error", e);
                try {
                    //报错，3s后重试
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    break;
                }
            }
        }
    }

    public static void refreshColumnarHaContext(ColumnarHaManager ha, ColumnarHaContext newHaContext) {
        if (newHaContext == null) {
            return;
        }

        //正常情况
        if (ha.columnarHaContext == null) {
            ha.columnarHaContext = newHaContext;
            ColumnarHaSwitchParams haSwitchParams = new ColumnarHaSwitchParams();
            haSwitchParams.curAvailableAddr = newHaContext.getCurrAvailableNodeAddr();
            haSwitchParams.rpcPort = newHaContext.getCurrRpcPort();
            ha.doHaSwitch(haSwitchParams);
        } else {
            if (newHaContext.getCurrAvailableNodeAddr()
                .equalsIgnoreCase(ha.getColumnarHaContext().getCurrAvailableNodeAddr())
                && newHaContext.getCurrRpcPort() == ha.getColumnarHaContext().getCurrRpcPort()) {
                //节点信息未变化,更新租约
                ha.columnarHaContext.setLeaseTime(newHaContext.getLeaseTime());
            } else {
                //节点信息变化
                logger.info(
                    String.format("ColumnarHaWatchDog set new leader: [%s:%d], old leader is [%s:%d]",
                        newHaContext.getCurrAvailableNodeAddr(), newHaContext.getCurrRpcPort(),
                        ha.getColumnarHaContext().getCurrAvailableNodeAddr(),
                        ha.getColumnarHaContext().getCurrRpcPort()));
                ha.columnarHaContext = newHaContext;
                ColumnarHaSwitchParams haSwitchParams = new ColumnarHaSwitchParams();
                haSwitchParams.curAvailableAddr = newHaContext.getCurrAvailableNodeAddr();
                haSwitchParams.rpcPort = newHaContext.getCurrRpcPort();
                ha.doHaSwitch(haSwitchParams);
            }
        }
    }
}

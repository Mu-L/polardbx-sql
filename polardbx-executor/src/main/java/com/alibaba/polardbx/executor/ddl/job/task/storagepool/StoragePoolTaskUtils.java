package com.alibaba.polardbx.executor.ddl.job.task.storagepool;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.handler.LogicalRebalanceHandler;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoExtraFieldJSON;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.locality.StoragePoolInfo;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StoragePoolTaskUtils {
    public static List<StorageInfoRecord> getStorageInfoRecordsByDnIds(StorageInfoAccessor storageInfoAccessor,
                                                                       String instId, Collection<String> dnIds) {
        List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
        List<StorageInfoRecord> filteredStorageInfo =
            storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        return filteredStorageInfo;
    }

    public static String constructPlanIdHint(Long planId) {
        if (LogicalRebalanceHandler.isValidPlanId(planId)) {
            return String.format("/*+TDDL:cmd_extra(%s=%d)*/", ConnectionProperties.DDL_PLAN_ID, planId);
        } else {
            return "";
        }
    }

    public static String constructPlanIdOption(Long planId) {
        if (LogicalRebalanceHandler.isValidPlanId(planId)) {
            return String.format(" %s=%s", SqlRebalance.OPTION_DDL_PLAN_ID, planId.toString());
        } else {
            return "";
        }
    }

    public static String constructRebalanceSql(String storagePoolName, Long planId) {
        String option = constructPlanIdOption(planId);
        String rebalanceSql = "SCHEDULE REBALANCE TENANT " + storagePoolName + " POLICY='data_balance'";
        return rebalanceSql + option;
    }

    public static String constructRebalanceSqlForDrainNode(String storagePoolName, Collection<String> dnIds,
                                                           Long planId) {
        String option = constructPlanIdOption(planId);
        String rebalanceSql = String.format("SCHEDULE REBALANCE TENANT %s drain_node='%s'", storagePoolName,
            StringUtils.join(dnIds, ","));
        return rebalanceSql + option;
    }

    public static void validateStoragePoolNameDuplicate(String storagePoolName) {
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        if (storagePoolManager.storagePoolCacheByName.containsKey(storagePoolName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("duplicate storage pool name '%s' found! ", storagePoolName));
        }
    }

    public static void validateDnIdsInCluster(Collection<String> dnIds) {
        Map<String, StorageInfoRecord> storageInfoMap =
            DbTopologyManager.getStorageInfoMap(InstIdUtil.getInstId());
        if (!storageInfoMap.keySet().containsAll(dnIds)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                "The storage insts appended contains illegal storage inst id!"
                    + StringUtils.join(dnIds, ","));
        }
    }

    public static void validateStoragePoolNameExists(String storagePoolName) {
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        if (!storagePoolManager.storagePoolCacheByName.containsKey(storagePoolName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("storage pool doesn't exist: '%s'", storagePoolName));
        }
    }

    public static void validateDnIdsInStoragePool(String storagePoolName, Collection<String> dnIds) {
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        StoragePoolInfo storagePoolInfo = storagePoolManager.getStoragePoolInfo(storagePoolName);
        if (!new HashSet<>(storagePoolInfo.getDnLists()).containsAll(dnIds)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("storage pool '%s' doesn't contains all of storage inst: '%s'", storagePoolName,
                    StringUtils.join(dnIds, ",")));
        }

    }

    /**
     * 校验storage pool name和dn ids是否合法
     */
    public static void validateStoragePoolNameExistsAndDnIdsInStoragePool(String storagePoolName, List<String> dnIds) {
        validateStoragePoolNameExists(storagePoolName);
        validateDnIdsInStoragePool(storagePoolName, dnIds);
    }

    public static void updateStoragePoolName(StorageInfoAccessor storageInfoAccessor,
                                             List<StorageInfoRecord> storageInfoRecords, String storagePoolName) {
        for (StorageInfoRecord record : storageInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            extras.setStoragePoolName(storagePoolName);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
        }
    }

    public static void updateStorageStatus(StorageInfoAccessor storageInfoAccessor, List<String> dnIds, int status) {
        for (String dnId : dnIds) {
            storageInfoAccessor.updateStorageStatus(dnId, status);
        }
    }

//    public static String findUndeletableDnIds(Collection<String> dnIds, List<String> singleGroupStorageInstList) {
//        String firstDnId = "";
//        for (String dnId : dnIds) {
//            if (StringUtils.isEmpty(firstDnId)) {
//                firstDnId = dnId;
//            }
//            if (singleGroupStorageInstList.contains(dnId)) {
//                return dnId;
//            }
//        }
//        return firstDnId;
//    }
}
